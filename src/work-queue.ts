import { RetryBackoff, systemClock, type BackoffOptions, type RetryClock } from './retry-backoff.js'

export interface WorkMessage<T> {
  readonly id: number
  readonly payload: T
  readonly deliveryCount: number
}

export interface Delivery<T> {
  readonly message: WorkMessage<T>
  readonly deliveryTag: number
  ack(): void
  nack(options?: { requeue?: boolean }): void
}

export type ConsumerHandler<T> = (delivery: Delivery<T>) => void
export type Unsubscribe = () => void

export interface WorkQueueOptions {
  maxDeliveryCount?: number
  retryBackoff?: BackoffOptions | false
  clock?: RetryClock
}

interface PendingMessage<T> {
  id: number
  payload: T
  deliveryCount: number
  lastRetryDelayMs: number
}

interface DelayedRetry<T> {
  pending: PendingMessage<T>
  availableAt: number
}

interface InFlightEntry<T> {
  pending: PendingMessage<T>
  consumerId: number
  settled: boolean
}

interface ConsumerState<T> {
  id: number
  handler: ConsumerHandler<T>
  prefetch: number
  inFlight: number
  active: boolean
}

const DEFAULT_MAX_DELIVERY_COUNT = 10

export class WorkQueue<T> {
  private readonly ready: PendingMessage<T>[] = []
  private readonly delayed: DelayedRetry<T>[] = []
  private readonly consumers: ConsumerState<T>[] = []
  private readonly inFlight = new Map<number, InFlightEntry<T>>()
  private readonly maxDeliveryCount: number
  private readonly backoff: RetryBackoff | null
  private readonly clock: RetryClock
  private cancelRetryTimer: (() => void) | undefined
  private nextMessageId = 1
  private nextDeliveryTag = 1
  private nextConsumerId = 1
  private nextConsumerIndex = 0
  private pumping = false

  constructor(options?: WorkQueueOptions) {
    const max = options?.maxDeliveryCount ?? DEFAULT_MAX_DELIVERY_COUNT
    if (!Number.isInteger(max) || max < 1) {
      throw new Error(`maxDeliveryCount must be a positive integer, got ${max}`)
    }
    this.maxDeliveryCount = max
    this.backoff = options?.retryBackoff === false ? null : new RetryBackoff(options?.retryBackoff)
    this.clock = options?.clock ?? systemClock()
  }

  enqueue(payload: T): number {
    const id = this.nextMessageId++
    this.ready.push({ id, payload, deliveryCount: 0, lastRetryDelayMs: 0 })
    this.pump()
    return id
  }

  consume(handler: ConsumerHandler<T>, options?: { prefetch?: number }): Unsubscribe {
    const prefetch = options?.prefetch ?? 1
    if (!Number.isInteger(prefetch) || prefetch < 1) {
      throw new Error(`prefetch must be a positive integer, got ${prefetch}`)
    }

    const consumer: ConsumerState<T> = {
      id: this.nextConsumerId++,
      handler,
      prefetch,
      inFlight: 0,
      active: true,
    }
    this.consumers.push(consumer)
    this.pump()

    return () => {
      if (!consumer.active) return
      consumer.active = false

      const toRequeue: PendingMessage<T>[] = []
      for (const [tag, entry] of this.inFlight) {
        if (entry.consumerId !== consumer.id || entry.settled) continue
        entry.settled = true
        this.inFlight.delete(tag)
        toRequeue.push(entry.pending)
      }
      this.ready.unshift(...toRequeue)
      consumer.inFlight = 0

      const i = this.consumers.indexOf(consumer)
      if (i !== -1) this.consumers.splice(i, 1)
      this.pump()
    }
  }

  readyCount(): number {
    return this.ready.length
  }

  delayedCount(): number {
    return this.delayed.length
  }

  nextRetryAt(): number | undefined {
    if (this.delayed.length === 0) return undefined
    let soonest = this.delayed[0]!.availableAt
    for (const item of this.delayed) {
      if (item.availableAt < soonest) soonest = item.availableAt
    }
    return soonest
  }

  inFlightCount(): number {
    return this.inFlight.size
  }

  consumerCount(): number {
    return this.consumers.length
  }

  private hasConsumerCapacity(): boolean {
    for (const c of this.consumers) {
      if (c.active && c.inFlight < c.prefetch) return true
    }
    return false
  }

  private pump(): void {
    if (this.pumping) return
    this.pumping = true
    try {
      this.releaseDueRetries()
      while (this.ready.length > 0 && this.hasConsumerCapacity()) {
        // Same id at most once per round: a sync nack cannot busy-spin enqueue.
        const deliveredThisRound = new Set<number>()
        let deliveredAny = false

        while (this.ready.length > 0 && this.consumers.length > 0) {
          const head = this.ready[0]
          if (!head || deliveredThisRound.has(head.id)) break

          const n = this.consumers.length
          const start = this.nextConsumerIndex % n
          let target: ConsumerState<T> | undefined
          let targetIndex = -1
          for (let offset = 0; offset < n; offset++) {
            const i = (start + offset) % n
            const consumer = this.consumers[i]
            if (!consumer || !consumer.active || consumer.inFlight >= consumer.prefetch) {
              continue
            }
            target = consumer
            targetIndex = i
            break
          }
          if (!target || targetIndex < 0) break

          const pending = this.ready.shift()
          if (!pending) break
          deliveredThisRound.add(pending.id)
          this.deliver(target, pending)
          const len = this.consumers.length
          this.nextConsumerIndex = len > 0 ? (targetIndex + 1) % len : 0
          deliveredAny = true
        }

        if (!deliveredAny) break
      }
    } finally {
      this.pumping = false
    }
    this.armRetryTimer()
  }

  private deliver(consumer: ConsumerState<T>, pending: PendingMessage<T>): void {
    pending.deliveryCount += 1
    const deliveryTag = this.nextDeliveryTag++
    const entry: InFlightEntry<T> = {
      pending,
      consumerId: consumer.id,
      settled: false,
    }
    this.inFlight.set(deliveryTag, entry)
    consumer.inFlight += 1

    const delivery: Delivery<T> = {
      message: {
        id: pending.id,
        payload: pending.payload,
        deliveryCount: pending.deliveryCount,
      },
      deliveryTag,
      ack: () => this.settle(deliveryTag, false),
      nack: (options) => this.settle(deliveryTag, options?.requeue !== false),
    }

    try {
      consumer.handler(delivery)
    } catch {
      if (!entry.settled) this.settle(deliveryTag, true)
    }
  }

  private settle(deliveryTag: number, requeue: boolean): void {
    const entry = this.inFlight.get(deliveryTag)
    if (!entry || entry.settled) return
    entry.settled = true
    this.inFlight.delete(deliveryTag)

    const consumer = this.consumers.find((c) => c.id === entry.consumerId)
    if (consumer?.active) consumer.inFlight -= 1

    if (requeue && entry.pending.deliveryCount < this.maxDeliveryCount) {
      this.scheduleRetry(entry.pending)
    }
    this.pump()
  }

  private scheduleRetry(pending: PendingMessage<T>): void {
    if (!this.backoff) {
      this.ready.push(pending)
      return
    }
    const delay = this.backoff.delayMs(pending.deliveryCount, pending.lastRetryDelayMs)
    pending.lastRetryDelayMs = delay
    if (delay === 0) {
      this.ready.push(pending)
      return
    }
    this.delayed.push({ pending, availableAt: this.clock.now() + delay })
  }

  private releaseDueRetries(): void {
    if (this.delayed.length === 0) return
    const now = this.clock.now()
    const still: DelayedRetry<T>[] = []
    for (const item of this.delayed) {
      if (item.availableAt <= now) this.ready.push(item.pending)
      else still.push(item)
    }
    this.delayed.length = 0
    this.delayed.push(...still)
  }

  private armRetryTimer(): void {
    this.cancelRetryTimer?.()
    this.cancelRetryTimer = undefined
    const soonest = this.nextRetryAt()
    if (soonest === undefined) return
    const wait = Math.max(0, soonest - this.clock.now())
    this.cancelRetryTimer = this.clock.schedule(() => {
      this.cancelRetryTimer = undefined
      this.pump()
    }, wait)
  }
}
