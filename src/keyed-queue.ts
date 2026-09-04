export interface KeyedMessage<T> {
  readonly id: number
  readonly key: string
  readonly payload: T
  readonly deliveryCount: number
}

export interface KeyedDelivery<T> {
  readonly message: KeyedMessage<T>
  readonly deliveryTag: number
  ack(): void
  nack(options?: { requeue?: boolean }): void
}

export type KeyedHandler<T> = (delivery: KeyedDelivery<T>) => void
export type Unsubscribe = () => void

export interface KeyedWorkQueueOptions {
  maxDeliveryCount?: number
}

interface Pending<T> {
  id: number
  key: string
  payload: T
  deliveryCount: number
}

interface Group<T> {
  pending: Pending<T>[]
  inFlight: boolean
}

interface InFlightEntry<T> {
  pending: Pending<T>
  consumerId: number
  settled: boolean
}

interface ConsumerState<T> {
  id: number
  handler: KeyedHandler<T>
  prefetch: number
  inFlight: number
  active: boolean
}

const DEFAULT_MAX_DELIVERY_COUNT = 10

export class KeyedWorkQueue<T> {
  private readonly groups = new Map<string, Group<T>>()
  private readonly runnable: string[] = []
  private readonly queuedKeys = new Set<string>()
  private readonly consumers: ConsumerState<T>[] = []
  private readonly inFlight = new Map<number, InFlightEntry<T>>()
  private readonly maxDeliveryCount: number
  private nextMessageId = 1
  private nextDeliveryTag = 1
  private nextConsumerId = 1
  private nextConsumerIndex = 0
  private pumping = false

  constructor(options?: KeyedWorkQueueOptions) {
    const max = options?.maxDeliveryCount ?? DEFAULT_MAX_DELIVERY_COUNT
    if (!Number.isInteger(max) || max < 1) {
      throw new Error(`maxDeliveryCount must be a positive integer, got ${max}`)
    }
    this.maxDeliveryCount = max
  }

  enqueue(key: string, payload: T): number {
    if (key.length === 0) throw new Error('key must be a non-empty string')
    const id = this.nextMessageId++
    let group = this.groups.get(key)
    if (!group) {
      group = { pending: [], inFlight: false }
      this.groups.set(key, group)
    }
    group.pending.push({ id, key, payload, deliveryCount: 0 })
    this.schedule(key)
    this.pump()
    return id
  }

  consume(handler: KeyedHandler<T>, options?: { prefetch?: number }): Unsubscribe {
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
      for (const [tag, entry] of this.inFlight) {
        if (entry.consumerId !== consumer.id || entry.settled) continue
        entry.settled = true
        this.inFlight.delete(tag)
        const group = this.groups.get(entry.pending.key)
        if (!group) continue
        group.inFlight = false
        group.pending.unshift(entry.pending)
        this.schedule(entry.pending.key)
      }
      consumer.inFlight = 0
      const i = this.consumers.indexOf(consumer)
      if (i !== -1) this.consumers.splice(i, 1)
      this.pump()
    }
  }

  readyCount(): number {
    let n = 0
    for (const group of this.groups.values()) n += group.pending.length
    return n
  }

  inFlightCount(): number {
    return this.inFlight.size
  }

  consumerCount(): number {
    return this.consumers.length
  }

  private schedule(key: string): void {
    const group = this.groups.get(key)
    if (!group || group.inFlight || group.pending.length === 0) return
    if (this.queuedKeys.has(key)) return
    this.queuedKeys.add(key)
    this.runnable.push(key)
  }

  private pickConsumer(): { consumer: ConsumerState<T>; index: number } | undefined {
    const n = this.consumers.length
    if (n === 0) return undefined
    const start = this.nextConsumerIndex % n
    for (let offset = 0; offset < n; offset++) {
      const i = (start + offset) % n
      const consumer = this.consumers[i]
      if (!consumer || !consumer.active || consumer.inFlight >= consumer.prefetch) continue
      return { consumer, index: i }
    }
    return undefined
  }

  private pump(): void {
    if (this.pumping) return
    this.pumping = true
    try {
      while (this.runnable.length > 0) {
        const deliveredThisRound = new Set<string>()
        let deliveredAny = false
        while (this.runnable.length > 0) {
          const key = this.runnable[0]
          if (!key || deliveredThisRound.has(key)) break
          const picked = this.pickConsumer()
          if (!picked) break
          this.runnable.shift()
          this.queuedKeys.delete(key)
          const group = this.groups.get(key)
          if (!group || group.inFlight || group.pending.length === 0) continue
          const pending = group.pending.shift()
          if (!pending) continue
          group.inFlight = true
          deliveredThisRound.add(key)
          this.deliver(picked.consumer, pending)
          const len = this.consumers.length
          this.nextConsumerIndex = len > 0 ? (picked.index + 1) % len : 0
          deliveredAny = true
        }
        if (!deliveredAny) break
      }
    } finally {
      this.pumping = false
    }
  }

  private deliver(consumer: ConsumerState<T>, pending: Pending<T>): void {
    pending.deliveryCount += 1
    const deliveryTag = this.nextDeliveryTag++
    const entry: InFlightEntry<T> = {
      pending,
      consumerId: consumer.id,
      settled: false,
    }
    this.inFlight.set(deliveryTag, entry)
    consumer.inFlight += 1
    const delivery: KeyedDelivery<T> = {
      message: {
        id: pending.id,
        key: pending.key,
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
    const key = entry.pending.key
    const group = this.groups.get(key)
    if (group) group.inFlight = false
    if (requeue && entry.pending.deliveryCount < this.maxDeliveryCount) {
      // Head of this key so a later message cannot overtake a nack.
      group?.pending.unshift(entry.pending)
    }
    if (group && group.pending.length === 0 && !group.inFlight) {
      this.groups.delete(key)
      this.queuedKeys.delete(key)
    } else if (group) {
      this.schedule(key)
    }
    this.pump()
  }
}
