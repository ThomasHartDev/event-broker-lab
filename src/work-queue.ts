import {
  QueueFullError,
  WatermarkGate,
  resolveQueueBounds,
  type BackpressureEvent,
  type BackpressureListener,
  type FlowState,
  type QueueBoundOptions,
} from './backpressure.js'

export { QueueFullError } from './backpressure.js'
export type { BackpressureEvent, BackpressureListener, FlowState } from './backpressure.js'

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

export type EnqueueResult = { readonly accepted: true; readonly id: number } | { readonly accepted: false }

export interface WorkQueueOptions extends QueueBoundOptions {
  maxDeliveryCount?: number
}

interface PendingMessage<T> {
  id: number
  payload: T
  deliveryCount: number
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
  readonly capacity: number
  readonly highWatermark: number
  readonly lowWatermark: number
  private readonly ready: PendingMessage<T>[] = []
  private readonly consumers: ConsumerState<T>[] = []
  private readonly inFlight = new Map<number, InFlightEntry<T>>()
  private readonly listeners = new Set<BackpressureListener>()
  private readonly gate: WatermarkGate | undefined
  private readonly maxDeliveryCount: number
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
    const bounds = resolveQueueBounds(options)
    this.capacity = bounds.capacity
    this.highWatermark = bounds.highWatermark
    this.lowWatermark = bounds.lowWatermark
    this.gate =
      bounds.capacity === Number.POSITIVE_INFINITY
        ? undefined
        : new WatermarkGate(bounds.highWatermark, bounds.lowWatermark)
  }

  enqueue(payload: T): number {
    const result = this.tryEnqueue(payload)
    if (!result.accepted) throw new QueueFullError(this.capacity)
    return result.id
  }

  tryEnqueue(payload: T): EnqueueResult {
    if (this.ready.length >= this.capacity) return { accepted: false }
    const id = this.nextMessageId++
    this.ready.push({ id, payload, deliveryCount: 0 })
    this.pump()
    this.applyFlow()
    return { accepted: true, id }
  }

  backpressure(): FlowState {
    return this.gate?.state ?? 'open'
  }

  onBackpressure(listener: BackpressureListener): Unsubscribe {
    this.listeners.add(listener)
    let active = true
    return () => {
      if (!active) return
      active = false
      this.listeners.delete(listener)
    }
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
    this.applyFlow()

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
      this.applyFlow()
    }
  }

  readyCount(): number {
    return this.ready.length
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
      // Cursor RR. Same id at most once per round so a sync nack cannot busy-spin.
      while (this.ready.length > 0 && this.hasConsumerCapacity()) {
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
      // Throw ≡ nack({ requeue: true }) so uncaught handler errors do not strand work.
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

    // Tail, not head: a poison nack must not starve work already on ready.
    if (requeue && entry.pending.deliveryCount < this.maxDeliveryCount) {
      this.ready.push(entry.pending)
    }
    this.pump()
    this.applyFlow()
  }

  private applyFlow(): void {
    if (!this.gate) return
    const next = this.gate.observe(this.ready.length)
    if (next === undefined) return
    this.emit({ state: next, occupancy: this.ready.length, capacity: this.capacity })
  }

  private emit(event: BackpressureEvent): void {
    const snapshot = [...this.listeners]
    let firstError: unknown
    for (const listener of snapshot) {
      try {
        listener(event)
      } catch (error) {
        if (firstError === undefined) firstError = error
      }
    }
    if (firstError !== undefined) throw firstError
  }
}
