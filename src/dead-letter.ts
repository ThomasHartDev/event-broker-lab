import type { Handler, Message } from './broker.js'

type Delivery<T> = Pick<Message<T>, 'topic' | 'payload' | 'id'>

export class DeadLetterFullError extends Error {
  readonly capacity: number

  constructor(capacity: number) {
    super(`dead-letter queue is full (capacity ${capacity})`)
    this.name = 'DeadLetterFullError'
    this.capacity = capacity
  }
}

export interface DeadLetterEnvelope<T> {
  readonly id: number
  readonly sourceTopic: string
  readonly payload: T
  readonly originalId: number
  readonly attempts: number
  readonly error: string
  readonly enqueuedAt: number
}

export type FailStatus = 'retry' | 'dead_lettered'

export interface FailResult<T> {
  readonly status: FailStatus
  readonly attempts: number
  readonly envelope?: DeadLetterEnvelope<T>
}

export interface DeadLetterQueueOptions {
  readonly maxAttempts?: number
  readonly capacity?: number
  readonly now?: () => number
}

export class DeadLetterQueue<T> {
  readonly maxAttempts: number
  readonly capacity: number
  private readonly now: () => number
  private nextId = 1
  private readonly items = new Map<number, DeadLetterEnvelope<T>>()
  private readonly byOriginal = new Map<number, number>()
  private readonly attempts = new Map<number, number>()

  constructor(options: DeadLetterQueueOptions = {}) {
    const maxAttempts = options.maxAttempts ?? 3
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new Error(`maxAttempts must be a positive integer, got ${maxAttempts}`)
    }
    const capacity = options.capacity ?? Number.POSITIVE_INFINITY
    if (capacity !== Number.POSITIVE_INFINITY && (!Number.isInteger(capacity) || capacity < 1)) {
      throw new Error(`capacity must be a positive integer, got ${capacity}`)
    }
    this.maxAttempts = maxAttempts
    this.capacity = capacity
    this.now = options.now ?? Date.now
  }

  fail(message: Delivery<T>, error: unknown): FailResult<T> {
    const parked = this.parked(message.id)
    if (parked) return { status: 'dead_lettered', attempts: parked.attempts, envelope: parked }

    let attempts = this.attempts.get(message.id) ?? 0
    if (attempts < this.maxAttempts) {
      attempts += 1
      this.attempts.set(message.id, attempts)
      if (attempts < this.maxAttempts) return { status: 'retry', attempts }
    }

    const envelope = this.enqueue(message, error, attempts)
    return { status: 'dead_lettered', attempts, envelope }
  }

  deadLetter(message: Delivery<T>, error: unknown): DeadLetterEnvelope<T> {
    const parked = this.parked(message.id)
    if (parked) return parked
    const attempts = Math.max(this.attempts.get(message.id) ?? 0, 1)
    this.attempts.set(message.id, attempts)
    return this.enqueue(message, error, attempts)
  }

  succeed(originalId: number): void {
    if (!this.byOriginal.has(originalId)) this.attempts.delete(originalId)
  }

  attemptCount(originalId: number): number {
    return this.attempts.get(originalId) ?? 0
  }

  peek(): readonly DeadLetterEnvelope<T>[] {
    return [...this.items.values()]
  }

  size(): number {
    return this.items.size
  }

  drop(id: number): boolean {
    const envelope = this.items.get(id)
    if (!envelope) return false
    this.forget(envelope)
    return true
  }

  purge(): number {
    const n = this.items.size
    for (const envelope of [...this.items.values()]) this.forget(envelope)
    return n
  }

  redrive(id: number, publish: (topic: string, payload: T) => void): DeadLetterEnvelope<T> | undefined {
    const envelope = this.items.get(id)
    if (!envelope) return undefined
    publish(envelope.sourceTopic, envelope.payload)
    this.forget(envelope)
    return envelope
  }

  redriveAll(publish: (topic: string, payload: T) => void): DeadLetterEnvelope<T>[] {
    const moved: DeadLetterEnvelope<T>[] = []
    for (const envelope of [...this.items.values()]) {
      const result = this.redrive(envelope.id, publish)
      if (result) moved.push(result)
    }
    return moved
  }

  private parked(originalId: number): DeadLetterEnvelope<T> | undefined {
    const id = this.byOriginal.get(originalId)
    return id === undefined ? undefined : this.items.get(id)
  }

  private forget(envelope: DeadLetterEnvelope<T>): void {
    this.items.delete(envelope.id)
    this.byOriginal.delete(envelope.originalId)
    this.attempts.delete(envelope.originalId)
  }

  private enqueue(message: Delivery<T>, error: unknown, attempts: number): DeadLetterEnvelope<T> {
    if (this.items.size >= this.capacity) throw new DeadLetterFullError(this.capacity)
    const envelope: DeadLetterEnvelope<T> = {
      id: this.nextId++,
      sourceTopic: message.topic,
      payload: message.payload,
      originalId: message.id,
      attempts,
      error: error instanceof Error ? error.message : String(error),
      enqueuedAt: this.now(),
    }
    this.items.set(envelope.id, envelope)
    this.byOriginal.set(message.id, envelope.id)
    return envelope
  }
}

export function withDeadLetter<T>(dlq: DeadLetterQueue<T>, handler: Handler<T>): Handler<T> {
  return (message) => {
    for (;;) {
      try {
        handler(message)
        dlq.succeed(message.id)
        return
      } catch (error) {
        if (dlq.fail(message, error).status === 'dead_lettered') return
      }
    }
  }
}
