import type { Unsubscribe } from './broker.js'

export interface ReliableMessage<T> {
  readonly id: number
  readonly topic: string
  readonly payload: T
  readonly deliveryCount: number
  readonly redelivered: boolean
}

export interface ReliableDelivery<T> {
  readonly message: ReliableMessage<T>
  readonly deliveryTag: number
  ack(): void
  nack(options?: { requeue?: boolean }): void
}

export type ReliableHandler<T> = (delivery: ReliableDelivery<T>) => void

export interface ReliableBrokerOptions {
  maxDeliveryCount?: number
}

export interface ReliableSubscribeOptions {
  prefetch?: number
}

interface PendingCopy<T> {
  id: number
  topic: string
  payload: T
  deliveryCount: number
}

interface InFlightCopy<T> {
  copy: PendingCopy<T>
  subscriberId: number
  settled: boolean
}

interface Subscriber<T> {
  id: number
  topic: string
  handler: ReliableHandler<T>
  prefetch: number | undefined
  ready: PendingCopy<T>[]
  inFlight: number
  active: boolean
}

const DEFAULT_MAX_DELIVERY_COUNT = 10

export class ReliableBroker<T> {
  private readonly subscribers: Subscriber<T>[] = []
  private readonly inFlight = new Map<number, InFlightCopy<T>>()
  private readonly maxDeliveryCount: number
  private nextMessageId = 1
  private nextDeliveryTag = 1
  private nextSubscriberId = 1
  private pumping = false

  constructor(options?: ReliableBrokerOptions) {
    const max = options?.maxDeliveryCount ?? DEFAULT_MAX_DELIVERY_COUNT
    if (!Number.isInteger(max) || max < 1) {
      throw new Error(`maxDeliveryCount must be a positive integer, got ${max}`)
    }
    this.maxDeliveryCount = max
  }

  subscribe(
    topic: string,
    handler: ReliableHandler<T>,
    options?: ReliableSubscribeOptions,
  ): Unsubscribe {
    const prefetch = options?.prefetch
    if (prefetch !== undefined && (!Number.isInteger(prefetch) || prefetch < 1)) {
      throw new Error(`prefetch must be a positive integer, got ${prefetch}`)
    }

    const subscriber: Subscriber<T> = {
      id: this.nextSubscriberId++,
      topic,
      handler,
      prefetch,
      ready: [],
      inFlight: 0,
      active: true,
    }
    this.subscribers.push(subscriber)

    return () => {
      if (!subscriber.active) return
      subscriber.active = false
      for (const [tag, entry] of this.inFlight) {
        if (entry.subscriberId !== subscriber.id || entry.settled) continue
        entry.settled = true
        this.inFlight.delete(tag)
      }
      subscriber.ready.length = 0
      subscriber.inFlight = 0
      const i = this.subscribers.indexOf(subscriber)
      if (i !== -1) this.subscribers.splice(i, 1)
    }
  }

  publish(topic: string, payload: T): number {
    const targets = this.subscribers.filter((s) => s.active && s.topic === topic)
    if (targets.length === 0) return 0
    const id = this.nextMessageId++
    for (const subscriber of targets) {
      subscriber.ready.push({ id, topic, payload, deliveryCount: 0 })
    }
    this.pump()
    return targets.length
  }

  readyCount(): number {
    return this.subscribers.reduce((n, s) => n + s.ready.length, 0)
  }

  inFlightCount(): number {
    return this.inFlight.size
  }

  subscriberCount(): number {
    return this.subscribers.length
  }

  private hasCapacity(subscriber: Subscriber<T>): boolean {
    return subscriber.prefetch === undefined || subscriber.inFlight < subscriber.prefetch
  }

  private pump(): void {
    if (this.pumping) return
    this.pumping = true
    try {
      let progressed = true
      while (progressed) {
        progressed = false
        for (const subscriber of this.subscribers) {
          if (!subscriber.active) continue
          const seenThisRound = new Set<number>()
          while (subscriber.ready.length > 0 && this.hasCapacity(subscriber)) {
            const head = subscriber.ready[0]
            // sync nack requeues the same id; skip it until the next pump round
            if (!head || seenThisRound.has(head.id)) break
            subscriber.ready.shift()
            seenThisRound.add(head.id)
            this.deliver(subscriber, head)
            progressed = true
          }
        }
      }
    } finally {
      this.pumping = false
    }
  }

  private deliver(subscriber: Subscriber<T>, copy: PendingCopy<T>): void {
    copy.deliveryCount += 1
    const deliveryTag = this.nextDeliveryTag++
    const entry: InFlightCopy<T> = { copy, subscriberId: subscriber.id, settled: false }
    this.inFlight.set(deliveryTag, entry)
    subscriber.inFlight += 1

    const delivery: ReliableDelivery<T> = {
      message: {
        id: copy.id,
        topic: copy.topic,
        payload: copy.payload,
        deliveryCount: copy.deliveryCount,
        redelivered: copy.deliveryCount > 1,
      },
      deliveryTag,
      ack: () => this.settle(deliveryTag, false),
      nack: (options) => this.settle(deliveryTag, options?.requeue !== false),
    }

    try {
      subscriber.handler(delivery)
    } catch {
      if (!entry.settled) this.settle(deliveryTag, true)
    }
  }

  private settle(deliveryTag: number, requeue: boolean): void {
    const entry = this.inFlight.get(deliveryTag)
    if (!entry || entry.settled) return
    entry.settled = true
    this.inFlight.delete(deliveryTag)

    const subscriber = this.subscribers.find((s) => s.id === entry.subscriberId)
    if (subscriber?.active) subscriber.inFlight -= 1

    if (requeue && subscriber?.active && entry.copy.deliveryCount < this.maxDeliveryCount) {
      subscriber.ready.push(entry.copy)
    }
    this.pump()
  }
}
