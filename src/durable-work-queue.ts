import { WriteAheadLog, type WriteAheadLogOptions } from './wal.js'
import {
  WorkQueue,
  type ConsumerHandler,
  type Delivery,
  type Unsubscribe,
  type WorkQueueOptions,
} from './work-queue.js'

const DEFAULT_MAX = 10

type Inner<T> = { walId: number; body: T }

type Op<T> =
  | { op: 'enqueue'; id: number; payload: T }
  | { op: 'ack'; id: number }
  | { op: 'drop'; id: number }

export interface DurableWorkQueueOptions extends WorkQueueOptions, WriteAheadLogOptions {}

function encode<T>(op: Op<T>): Uint8Array {
  if (op.op === 'enqueue' && op.payload === undefined) {
    throw new Error('enqueue payload must be JSON-serializable')
  }
  return new TextEncoder().encode(JSON.stringify(op))
}

function decode<T>(bytes: Uint8Array): Op<T> {
  const value = JSON.parse(new TextDecoder().decode(bytes)) as Op<T>
  if (value.op !== 'enqueue' && value.op !== 'ack' && value.op !== 'drop') {
    throw new Error('invalid wal record')
  }
  if (!Number.isInteger(value.id) || value.id < 1) throw new Error('invalid wal record')
  return value
}

export class DurableWorkQueue<T> {
  private constructor(
    private readonly log: WriteAheadLog,
    private readonly inner: WorkQueue<Inner<T>>,
    private readonly live: Map<number, T>,
    private nextId: number,
    private readonly maxDeliveryCount: number,
  ) {}

  static open<T>(path: string, options?: DurableWorkQueueOptions): DurableWorkQueue<T> {
    return DurableWorkQueue.fromLog(WriteAheadLog.open(path, options), options)
  }

  static memory<T>(options?: DurableWorkQueueOptions): DurableWorkQueue<T> {
    return DurableWorkQueue.fromLog(WriteAheadLog.memory(options), options)
  }

  private static fromLog<T>(
    log: WriteAheadLog,
    options?: DurableWorkQueueOptions,
  ): DurableWorkQueue<T> {
    const maxDeliveryCount = options?.maxDeliveryCount ?? DEFAULT_MAX
    if (!Number.isInteger(maxDeliveryCount) || maxDeliveryCount < 1) {
      throw new Error(`maxDeliveryCount must be a positive integer, got ${maxDeliveryCount}`)
    }
    const inner = new WorkQueue<Inner<T>>({ maxDeliveryCount })
    const live = new Map<number, T>()
    let nextId = 1
    try {
      for (const bytes of log.replay()) {
        const rec = decode<T>(bytes)
        if (rec.id >= nextId) nextId = rec.id + 1
        if (rec.op === 'enqueue') live.set(rec.id, rec.payload)
        else live.delete(rec.id)
      }
    } catch (error) {
      log.close()
      throw error
    }
    const queue = new DurableWorkQueue(log, inner, live, nextId, maxDeliveryCount)
    for (const [id, payload] of live) inner.enqueue({ walId: id, body: payload })
    return queue
  }

  enqueue(payload: T): number {
    const id = this.nextId
    this.log.append(encode({ op: 'enqueue', id, payload }))
    this.nextId += 1
    this.live.set(id, payload)
    this.inner.enqueue({ walId: id, body: payload })
    return id
  }

  consume(handler: ConsumerHandler<T>, options?: { prefetch?: number }): Unsubscribe {
    const inflight = new Set<{ settled: boolean }>()
    const off = this.inner.consume((delivery) => {
      const gate = { settled: false }
      inflight.add(gate)
      const wrapped = this.wrap(delivery, gate)
      try {
        handler(wrapped)
      } catch {
        wrapped.nack()
      } finally {
        if (gate.settled) inflight.delete(gate)
      }
    }, options)
    return () => {
      for (const gate of inflight) gate.settled = true
      inflight.clear()
      off()
    }
  }

  checkpoint(): void {
    const kept: Uint8Array[] = []
    for (const [id, payload] of this.live) kept.push(encode({ op: 'enqueue', id, payload }))
    this.log.rewrite(kept)
  }

  close(): void {
    this.log.close()
  }

  readyCount(): number {
    return this.inner.readyCount()
  }

  private wrap(delivery: Delivery<Inner<T>>, gate: { settled: boolean }): Delivery<T> {
    const walId = delivery.message.payload.walId
    const finish = (fn: () => void) => {
      if (gate.settled) return
      gate.settled = true
      fn()
    }
    return {
      message: {
        id: walId,
        payload: delivery.message.payload.body,
        deliveryCount: delivery.message.deliveryCount,
      },
      deliveryTag: delivery.deliveryTag,
      ack: () =>
        finish(() => {
          this.commit('ack', walId)
          delivery.ack()
        }),
      nack: (opts) =>
        finish(() => {
          if (opts?.requeue === false || delivery.message.deliveryCount >= this.maxDeliveryCount) {
            this.commit('drop', walId)
          }
          delivery.nack(opts)
        }),
    }
  }

  private commit(op: 'ack' | 'drop', id: number): void {
    if (!this.live.has(id)) return
    this.log.append(encode({ op, id }))
    this.live.delete(id)
  }
}
