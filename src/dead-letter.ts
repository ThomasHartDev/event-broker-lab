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
  private readonly byOriginal = new Map<string, number>()
  private readonly attempts = new Map<string, number>()
  private readonly ledgerOf = new Map<number, string>()

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

  fail(message: Delivery<T>, error: unknown, subscription?: string): FailResult<T> {
    const parked = this.parked(message.id, subscription)
    if (parked) return { status: 'dead_lettered', attempts: parked.attempts, envelope: parked }

    const key = this.ledgerKey(message.id, subscription)
    let attempts = this.attempts.get(key) ?? 0
    if (attempts < this.maxAttempts) {
      attempts += 1
      this.attempts.set(key, attempts)
      if (attempts < this.maxAttempts) return { status: 'retry', attempts }
    }

    const envelope = this.enqueue(message, error, attempts, key)
    return { status: 'dead_lettered', attempts, envelope }
  }

  deadLetter(message: Delivery<T>, error: unknown, subscription?: string): DeadLetterEnvelope<T> {
    const parked = this.parked(message.id, subscription)
    if (parked) return parked
    const key = this.ledgerKey(message.id, subscription)
    const attempts = Math.max(this.attempts.get(key) ?? 0, 1)
    this.attempts.set(key, attempts)
    return this.enqueue(message, error, attempts, key)
  }

  succeed(originalId: number, subscription?: string): void {
    const key = this.ledgerKey(originalId, subscription)
    if (!this.byOriginal.has(key)) this.attempts.delete(key)
  }

  attemptCount(originalId: number, subscription?: string): number {
    return this.attempts.get(this.ledgerKey(originalId, subscription)) ?? 0
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

  private ledgerKey(originalId: number, subscription?: string): string {
    return `${subscription ?? ''}:${originalId}`
  }

  private parked(originalId: number, subscription?: string): DeadLetterEnvelope<T> | undefined {
    const id = this.byOriginal.get(this.ledgerKey(originalId, subscription))
    return id === undefined ? undefined : this.items.get(id)
  }

  private forget(envelope: DeadLetterEnvelope<T>): void {
    const key = this.ledgerOf.get(envelope.id)
    this.items.delete(envelope.id)
    this.ledgerOf.delete(envelope.id)
    if (key === undefined) return
    this.byOriginal.delete(key)
    this.attempts.delete(key)
  }

  private enqueue(
    message: Delivery<T>,
    error: unknown,
    attempts: number,
    key: string,
  ): DeadLetterEnvelope<T> {
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
    this.byOriginal.set(key, envelope.id)
    this.ledgerOf.set(envelope.id, key)
    return envelope
  }
}

let nextWrapper = 1

export function withDeadLetter<T>(dlq: DeadLetterQueue<T>, handler: Handler<T>): Handler<T> {
  const subscription = `w${nextWrapper++}`
  return (message) => {
    for (;;) {
      try {
        handler(message)
        dlq.succeed(message.id, subscription)
        return
      } catch (error) {
        if (dlq.fail(message, error, subscription).status === 'dead_lettered') return
      }
    }
  }
}
