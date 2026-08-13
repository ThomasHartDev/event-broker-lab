import { describe, it, expect, vi } from 'vitest'
import {
  Broker,
  DeadLetterFullError,
  DeadLetterQueue,
  withDeadLetter,
} from '../src/index.js'

const msg = (id: number, payload = 'x', topic = 'orders') => ({ topic, payload, id })

describe('DeadLetterQueue.fail', () => {
  it('retries until maxAttempts then parks the payload', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 3, now: () => 42 })
    expect(dlq.fail(msg(1), new Error('boom'))).toEqual({ status: 'retry', attempts: 1 })
    expect(dlq.fail(msg(1), new Error('boom'))).toEqual({ status: 'retry', attempts: 2 })
    const last = dlq.fail(msg(1), new Error('boom'))
    expect(last).toMatchObject({
      status: 'dead_lettered',
      attempts: 3,
      envelope: {
        id: 1,
        sourceTopic: 'orders',
        payload: 'x',
        originalId: 1,
        attempts: 3,
        error: 'boom',
        enqueuedAt: 42,
      },
    })
    expect(dlq.peek()).toEqual([last.envelope])
  })

  it('dead-letters on the first fail when maxAttempts is 1', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    const result = dlq.fail(msg(7, 'poison'), 'bad schema')
    expect(result.status).toBe('dead_lettered')
    expect(result.envelope).toMatchObject({ attempts: 1, error: 'bad schema' })
  })

  it('is idempotent once the original id is already parked', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    const first = dlq.fail(msg(1), new Error('a'))
    expect(dlq.fail(msg(1), new Error('b')).envelope).toBe(first.envelope)
    expect(dlq.peek()[0]?.error).toBe('a')
  })

  it('tracks attempts per original id and stringifies non-Error throws', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 5 })
    dlq.fail(msg(1), 'a')
    dlq.fail(msg(2), { code: 9 })
    dlq.fail(msg(1), 'a')
    expect(dlq.attemptCount(1)).toBe(2)
    expect(dlq.attemptCount(2)).toBe(1)
    expect(dlq.attemptCount(99)).toBe(0)
    const parked = new DeadLetterQueue<string>({ maxAttempts: 1 })
    expect(parked.fail(msg(1), { code: 9 }).envelope?.error).toBe('[object Object]')
    expect(parked.fail(msg(2), 0).envelope?.error).toBe('0')
  })
})

describe('succeed and explicit deadLetter', () => {
  it('clears the ledger so the next fail starts at attempt 1', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 3 })
    dlq.fail(msg(1), 'x')
    dlq.fail(msg(1), 'x')
    dlq.succeed(1)
    expect(dlq.attemptCount(1)).toBe(0)
    expect(dlq.fail(msg(1), 'x').attempts).toBe(1)
    expect(dlq.size()).toBe(0)
    expect(() => dlq.succeed(99)).not.toThrow()
  })

  it('does not clear a message already on the DLQ', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    dlq.fail(msg(1), 'x')
    dlq.succeed(1)
    expect(dlq.size()).toBe(1)
    expect(dlq.attemptCount(1)).toBe(1)
  })

  it('deadLetter parks immediately without waiting for maxAttempts', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 8 })
    const envelope = dlq.deadLetter(msg(3, 'nope'), new Error('rejected'))
    expect(envelope).toMatchObject({ attempts: 1, error: 'rejected' })
    expect(dlq.deadLetter(msg(3, 'nope'), new Error('again'))).toBe(envelope)
  })
})

describe('capacity, drop, purge, redrive', () => {
  it('refuses a new poison payload when the queue is full', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1, capacity: 1 })
    dlq.fail(msg(1), 'first')
    expect(() => dlq.fail(msg(2), 'second')).toThrow(DeadLetterFullError)
    expect(dlq.peek()[0]?.originalId).toBe(1)
  })

  it('rejects non-positive options', () => {
    expect(() => new DeadLetterQueue({ maxAttempts: 0 })).toThrow(/maxAttempts/)
    expect(() => new DeadLetterQueue({ maxAttempts: 1.5 })).toThrow(/maxAttempts/)
    expect(() => new DeadLetterQueue({ capacity: 0 })).toThrow(/capacity/)
    expect(() => new DeadLetterQueue({ capacity: -1 })).toThrow(/capacity/)
  })

  it('drop and purge remove envelopes and forget their ledgers', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    expect(dlq.purge()).toBe(0)
    const parked = dlq.fail(msg(4), 'x').envelope!
    expect(dlq.drop(parked.id)).toBe(true)
    expect(dlq.drop(parked.id)).toBe(false)
    expect(dlq.attemptCount(4)).toBe(0)
    dlq.fail(msg(1), 'a')
    dlq.fail(msg(2), 'b')
    expect(dlq.purge()).toBe(2)
    expect(dlq.peek()).toEqual([])
  })

  it('redrive publishes then removes, and leaves the envelope if publish throws', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    const parked = dlq.fail(msg(9, 'job'), 'x').envelope!
    expect(() =>
      dlq.redrive(parked.id, () => {
        throw new Error('broker down')
      }),
    ).toThrow(/broker down/)
    expect(dlq.size()).toBe(1)
    const seen: Array<[string, string]> = []
    expect(dlq.redrive(parked.id, (topic, payload) => seen.push([topic, payload]))).toEqual(parked)
    expect(seen).toEqual([['orders', 'job']])
    expect(dlq.redrive(parked.id, () => {})).toBeUndefined()
  })

  it('redriveAll walks insertion order and stops if a publish throws', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    dlq.fail(msg(1, 'a'), 'x')
    dlq.fail(msg(2, 'b'), 'x')
    dlq.fail(msg(3, 'c'), 'x')
    const seen: string[] = []
    expect(() =>
      dlq.redriveAll((_topic, payload) => {
        if (payload === 'b') throw new Error('stop')
        seen.push(payload)
      }),
    ).toThrow(/stop/)
    expect(seen).toEqual(['a'])
    expect(dlq.peek().map((e) => e.payload)).toEqual(['b', 'c'])
  })
})

describe('withDeadLetter', () => {
  it('retries the same message then parks a permanent failure', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 3 })
    const handler = vi.fn(() => {
      throw new Error('poison')
    })
    expect(() => withDeadLetter(dlq, handler)({ topic: 'jobs', payload: 'bad', id: 11 })).not.toThrow()
    expect(handler).toHaveBeenCalledTimes(3)
    expect(dlq.peek()[0]).toMatchObject({
      originalId: 11,
      sourceTopic: 'jobs',
      payload: 'bad',
      attempts: 3,
      error: 'poison',
    })
  })

  it('does not park a message that succeeds on a later attempt', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 4 })
    let n = 0
    withDeadLetter(dlq, () => {
      n += 1
      if (n < 3) throw new Error('flaky')
    })({ topic: 'jobs', payload: 'ok', id: 1 })
    expect(n).toBe(3)
    expect(dlq.size()).toBe(0)
    expect(dlq.attemptCount(1)).toBe(0)
  })

  it('keeps fan-out going when one subscriber throws', () => {
    const broker = new Broker<string>()
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 2 })
    const healthy = vi.fn()
    broker.subscribe(
      'orders',
      withDeadLetter(dlq, () => {
        throw new Error('poison')
      }),
    )
    broker.subscribe('orders', healthy)
    expect(broker.publish('orders', 'A-1')).toBe(2)
    expect(healthy).toHaveBeenCalledOnce()
    expect(dlq.peek()[0]?.payload).toBe('A-1')
  })

  it('redrives a parked payload back through the broker as a new publish', () => {
    const broker = new Broker<string>()
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1 })
    const seen: string[] = []
    let failNext = true
    broker.subscribe(
      'orders',
      withDeadLetter(dlq, (m) => {
        if (failNext) throw new Error('first pass')
        seen.push(m.payload)
      }),
    )
    broker.publish('orders', 'A-1')
    failNext = false
    dlq.redrive(dlq.peek()[0]!.id, (topic, payload) => broker.publish(topic, payload))
    expect(seen).toEqual(['A-1'])
    expect(dlq.size()).toBe(0)
  })

  it('surfaces DeadLetterFullError instead of parking a new poison payload', () => {
    const dlq = new DeadLetterQueue<string>({ maxAttempts: 1, capacity: 1 })
    const wrapped = withDeadLetter(dlq, () => {
      throw new Error('nope')
    })
    wrapped({ topic: 't', payload: 'one', id: 1 })
    expect(() => wrapped({ topic: 't', payload: 'two', id: 2 })).toThrow(DeadLetterFullError)
    expect(dlq.peek()[0]?.payload).toBe('one')
  })
})
