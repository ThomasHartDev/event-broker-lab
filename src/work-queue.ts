/**
 * Competing-consumer work queue with explicit acknowledgements.
 *
 * Unlike topic fan-out, each enqueued message is delivered to exactly one
 * consumer. Consumers ack on success or nack to requeue; unacked work held by
 * a leaving consumer is requeued so peers can take it (RabbitMQ / SQS model).
 */

export interface WorkMessage<T> {
  readonly id: number
  readonly payload: T
  /** Times this message has been handed to a consumer (1 on first delivery). */
  readonly deliveryCount: number
}

export interface Delivery<T> {
  readonly message: WorkMessage<T>
  readonly deliveryTag: number
  ack(): void
  /** Reject the delivery. `requeue` defaults to true. */
  nack(options?: { requeue?: boolean }): void
}

export type ConsumerHandler<T> = (delivery: Delivery<T>) => void
export type Unsubscribe = () => void

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

export class WorkQueue<T> {
  private readonly ready: PendingMessage<T>[] = []
  private readonly consumers: ConsumerState<T>[] = []
  private readonly inFlight = new Map<number, InFlightEntry<T>>()
  private nextMessageId = 1
  private nextDeliveryTag = 1
  private nextConsumerId = 1
  private nextConsumerIndex = 0
  private pumping = false

  /** Enqueue a payload. Dispatches if a consumer has spare capacity. Returns message id. */
  enqueue(payload: T): number {
    const id = this.nextMessageId++
    this.ready.push({ id, payload, deliveryCount: 0 })
    this.pump()
    return id
  }

  /**
   * Register a competing consumer. Default prefetch is 1. Unsubscribe requeues
   * any still-unacked deliveries held by this consumer.
   */
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

  inFlightCount(): number {
    return this.inFlight.size
  }

  consumerCount(): number {
    return this.consumers.length
  }

  private pump(): void {
    if (this.pumping) return
    this.pumping = true
    try {
      // Cursor-based RR so sequential enqueues share fairly, not only batched pumps.
      while (this.ready.length > 0 && this.consumers.length > 0) {
        const n = this.consumers.length
        const start = this.nextConsumerIndex % n
        let delivered = false
        for (let offset = 0; offset < n; offset++) {
          const i = (start + offset) % n
          const consumer = this.consumers[i]
          if (!consumer || !consumer.active || consumer.inFlight >= consumer.prefetch) {
            continue
          }
          const pending = this.ready.shift()
          if (!pending) return
          this.deliver(consumer, pending)
          this.nextConsumerIndex = (i + 1) % n
          delivered = true
          break
        }
        if (!delivered) break
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
      // Uncaught handler error must not leave the message stranded in-flight.
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

    // Tail, not head: avoids a poison message tight-looping on one consumer.
    if (requeue) this.ready.push(entry.pending)
    this.pump()
  }
}
