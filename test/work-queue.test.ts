import { describe, it, expect } from 'vitest'
import { ManualClock, WorkQueue, type Delivery } from '../src/index.js'

const immediate = { retryBackoff: false as const }

describe('WorkQueue competing consumers', () => {
  it('throws on invalid maxDeliveryCount', () => {
    expect(() => new WorkQueue({ ...immediate, maxDeliveryCount: 0 })).toThrow(/maxDeliveryCount/)
    expect(() => new WorkQueue({ ...immediate, maxDeliveryCount: 1.5 })).toThrow(/maxDeliveryCount/)
  })

  it('delivers each message to exactly one consumer and round-robins', () => {
    const queue = new WorkQueue<string>(immediate)
    const a: string[] = []
    const b: string[] = []
    queue.consume((d) => {
      a.push(d.message.payload)
      d.ack()
    })
    queue.consume((d) => {
      b.push(d.message.payload)
      d.ack()
    })

    for (const p of ['one', 'two', 'three', 'four']) queue.enqueue(p)

    expect([...a, ...b].sort()).toEqual(['four', 'one', 'three', 'two'])
    expect(a).toEqual(['one', 'three'])
    expect(b).toEqual(['two', 'four'])
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(0)
  })

  it('buffers when no consumer is registered, then drains on consume', () => {
    const queue = new WorkQueue<number>(immediate)
    expect(queue.enqueue(1)).toBe(1)
    expect(queue.enqueue(2)).toBe(2)
    expect(queue.readyCount()).toBe(2)

    const seen: number[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      d.ack()
    })
    expect(seen).toEqual([1, 2])
  })

  it('assigns monotonic message ids starting at 1', () => {
    const queue = new WorkQueue<string>(immediate)
    const ids: number[] = []
    queue.consume((d) => {
      ids.push(d.message.id)
      d.ack()
    })
    queue.enqueue('a')
    queue.enqueue('b')
    expect(ids).toEqual([1, 2])
  })

  it('respects prefetch: unacked work blocks further delivery to that consumer', () => {
    const queue = new WorkQueue<string>(immediate)
    const held: Delivery<string>[] = []
    queue.consume((d) => held.push(d))
    queue.enqueue('first')
    queue.enqueue('second')
    expect(held).toHaveLength(1)
    expect(held[0]?.message.payload).toBe('first')
    expect(queue.readyCount()).toBe(1)

    held[0]?.ack()
    expect(held).toHaveLength(2)
    expect(held[1]?.message.payload).toBe('second')

    const multi: Delivery<string>[] = []
    const q2 = new WorkQueue<string>(immediate)
    q2.consume((d) => multi.push(d), { prefetch: 3 })
    q2.enqueue('a')
    q2.enqueue('b')
    q2.enqueue('c')
    q2.enqueue('d')
    expect(multi).toHaveLength(3)
    expect(q2.readyCount()).toBe(1)
    multi[0]?.ack()
    expect(multi).toHaveLength(4)
  })

  it('throws on invalid prefetch', () => {
    const queue = new WorkQueue<string>(immediate)
    expect(() => queue.consume(() => {}, { prefetch: 0 })).toThrow(/prefetch/)
    expect(() => queue.consume(() => {}, { prefetch: 1.5 })).toThrow(/prefetch/)
  })

  it('shares work when one consumer is blocked at prefetch', () => {
    const queue = new WorkQueue<number>(immediate)
    const aHeld: Delivery<number>[] = []
    const bSeen: number[] = []
    queue.consume((d) => aHeld.push(d))
    queue.consume((d) => {
      bSeen.push(d.message.payload)
      d.ack()
    })
    queue.enqueue(1)
    queue.enqueue(2)
    queue.enqueue(3)
    expect(aHeld[0]?.message.payload).toBe(1)
    expect(bSeen).toEqual([2, 3])
    aHeld[0]?.ack()
    expect(queue.inFlightCount()).toBe(0)
  })
})

describe('ack and nack', () => {
  it('ack settles permanently; nack requeues with rising deliveryCount', () => {
    const queue = new WorkQueue<string>(immediate)
    const counts: number[] = []
    queue.consume((d) => {
      counts.push(d.message.deliveryCount)
      if (d.message.deliveryCount < 3) d.nack()
      else d.ack()
    })
    queue.enqueue('retry')
    expect(counts).toEqual([1, 2, 3])
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(0)
  })

  it('nack({ requeue: false }) drops the message', () => {
    const queue = new WorkQueue<string>(immediate)
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      d.nack({ requeue: false })
    })
    queue.enqueue('poison')
    expect(seen).toEqual(['poison'])
    expect(queue.readyCount()).toBe(0)
  })

  it('double settle is a no-op', () => {
    const queue = new WorkQueue<string>(immediate)
    let first: Delivery<string> | undefined
    let count = 0
    queue.consume((d) => {
      count += 1
      if (count === 1) {
        first = d
        d.nack()
        return
      }
      d.ack()
    })
    queue.enqueue('y')
    expect(() => first?.ack()).not.toThrow()
    expect(() => first?.nack()).not.toThrow()
    expect(count).toBe(2)
  })

  it('requeues on handler throw so work is not stranded', () => {
    const queue = new WorkQueue<string>(immediate)
    let throws = true
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (throws) {
        throws = false
        throw new Error('boom')
      }
      d.ack()
    })
    queue.enqueue('recover')
    expect(seen).toEqual(['recover', 'recover'])
    expect(queue.inFlightCount()).toBe(0)
  })

  it('keeps delivery tags unique across redeliveries', () => {
    const queue = new WorkQueue<string>(immediate)
    const tags: number[] = []
    queue.consume((d) => {
      tags.push(d.deliveryTag)
      if (tags.length < 3) d.nack()
      else d.ack()
    })
    queue.enqueue('z')
    expect(tags).toEqual([1, 2, 3])
  })

  it('nack requeues to the tail when other messages are already ready', () => {
    const queue = new WorkQueue<string>(immediate)
    const order: string[] = []
    let holdA: Delivery<string> | undefined
    queue.consume((d) => {
      if (d.message.payload === 'A' && !holdA) {
        holdA = d
        return
      }
      order.push(d.message.payload)
      d.ack()
    })
    queue.enqueue('A')
    expect(holdA).toBeDefined()
    expect(queue.readyCount()).toBe(0)
    queue.enqueue('B')
    expect(queue.readyCount()).toBe(1)
    holdA!.nack()
    expect(order).toEqual(['B', 'A'])
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(0)
  })

  it('always-nack does not hang: enqueue returns and deliveries stay bounded', () => {
    const maxDeliveryCount = 5
    const queue = new WorkQueue<string>({ ...immediate, maxDeliveryCount })
    let deliveries = 0
    queue.consume((d) => {
      deliveries += 1
      d.nack()
    })
    const id = queue.enqueue('spin')
    expect(id).toBe(1)
    expect(deliveries).toBe(maxDeliveryCount)
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(0)
    expect(queue.enqueue('still-responsive')).toBe(2)
  })

  it('always-throw / poison parse does not hang: bounded redelivery, enqueue returns', () => {
    const maxDeliveryCount = 4
    const queue = new WorkQueue<string>({ ...immediate, maxDeliveryCount })
    let deliveries = 0
    const seen: string[] = []
    queue.consume((d) => {
      deliveries += 1
      seen.push(d.message.payload)
      // Permanent poison: throw every time on this payload (e.g. bad JSON).
      if (d.message.payload === 'not-json') {
        JSON.parse(d.message.payload)
      }
      d.ack()
    })
    const id = queue.enqueue('not-json')
    expect(id).toBe(1)
    expect(deliveries).toBe(maxDeliveryCount)
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(0)
    expect(queue.enqueue('ok-after')).toBe(2)
    expect(seen).toContain('ok-after')
    expect(queue.inFlightCount()).toBe(0)
  })
})

describe('consumer lifecycle', () => {
  it('requeues unacked work on unsubscribe; stale ack is harmless', () => {
    const queue = new WorkQueue<string>(immediate)
    let stolen: Delivery<string> | undefined
    const off = queue.consume((d) => {
      stolen = d
    })
    queue.enqueue('held')
    expect(queue.inFlightCount()).toBe(1)

    off()
    expect(() => off()).not.toThrow()
    expect(queue.consumerCount()).toBe(0)
    expect(queue.readyCount()).toBe(1)

    const recovered: string[] = []
    queue.consume((d) => {
      recovered.push(d.message.payload)
      expect(d.message.deliveryCount).toBe(2)
      d.ack()
    })
    expect(recovered).toEqual(['held'])
    expect(() => stolen?.ack()).not.toThrow()
  })

  it('handles enqueue-from-handler without dropping messages', () => {
    const queue = new WorkQueue<string>(immediate)
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (d.message.payload === 'seed') queue.enqueue('child')
      d.ack()
    })
    queue.enqueue('seed')
    expect(seen).toEqual(['seed', 'child'])
  })

  it('round-robins evenly across three consumers', () => {
    const queue = new WorkQueue<number>(immediate)
    const counts = [0, 0, 0]
    for (let i = 0; i < 3; i++) {
      const slot = i
      queue.consume((d) => {
        counts[slot] = (counts[slot] ?? 0) + 1
        d.ack()
      })
    }
    for (let n = 0; n < 9; n++) queue.enqueue(n)
    expect(counts).toEqual([3, 3, 3])
  })
})

describe('exponential backoff on retry', () => {
  it('holds a nack until the exponential delay elapses', () => {
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      retryBackoff: { baseDelayMs: 100, maxDelayMs: 10_000, jitter: 'none' },
    })
    const counts: number[] = []
    queue.consume((d) => {
      counts.push(d.message.deliveryCount)
      if (d.message.deliveryCount < 3) d.nack()
      else d.ack()
    })
    queue.enqueue('job')
    expect(counts).toEqual([1])
    expect(queue.nextRetryAt()).toBe(100)
    clock.advance(99)
    expect(counts).toEqual([1])
    clock.advance(1)
    expect(counts).toEqual([1, 2])
    expect(queue.nextRetryAt()).toBe(300)
    clock.advance(200)
    expect(counts).toEqual([1, 2, 3])
    expect(queue.delayedCount()).toBe(0)
  })

  it('does not stall other ready work behind a delayed retry', () => {
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      retryBackoff: { baseDelayMs: 50, jitter: 'none' },
    })
    const order: string[] = []
    let holdA: Delivery<string> | undefined
    queue.consume((d) => {
      if (d.message.payload === 'A' && !holdA) {
        holdA = d
        return
      }
      order.push(d.message.payload)
      d.ack()
    })
    queue.enqueue('A')
    queue.enqueue('B')
    holdA!.nack()
    expect(order).toEqual(['B'])
    clock.advance(50)
    expect(order).toEqual(['B', 'A'])
  })

  it('spreads two full-jitter nacks of the same attempt', () => {
    const draws = [0.1, 0.9]
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      retryBackoff: { baseDelayMs: 100, jitter: 'full', random: () => draws.shift() ?? 0 },
    })
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(`${d.message.payload}:${d.message.deliveryCount}`)
      if (d.message.deliveryCount === 1) d.nack()
      else d.ack()
    }, { prefetch: 2 })
    queue.enqueue('x')
    queue.enqueue('y')
    expect(seen).toEqual(['x:1', 'y:1'])
    clock.advance(10)
    expect(seen).toEqual(['x:1', 'y:1', 'x:2'])
    clock.advance(80)
    expect(seen).toEqual(['x:1', 'y:1', 'x:2', 'y:2'])
  })

  it('drops after maxDeliveryCount even when retries are delayed', () => {
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      maxDeliveryCount: 3,
      retryBackoff: { baseDelayMs: 10, jitter: 'none' },
    })
    let deliveries = 0
    queue.consume((d) => {
      deliveries += 1
      d.nack()
    })
    queue.enqueue('poison')
    clock.advance(10)
    clock.advance(20)
    clock.advance(80)
    expect(deliveries).toBe(3)
    expect(queue.delayedCount()).toBe(0)
  })

  it('delays a handler throw the same way as a requeueing nack', () => {
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      retryBackoff: { baseDelayMs: 25, jitter: 'none' },
    })
    let throws = true
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (throws) {
        throws = false
        throw new Error('boom')
      }
      d.ack()
    })
    queue.enqueue('recover')
    expect(seen).toEqual(['recover'])
    clock.advance(25)
    expect(seen).toEqual(['recover', 'recover'])
  })

  it('keeps delayed retries when the holding consumer unsubscribes', () => {
    const clock = new ManualClock()
    const queue = new WorkQueue<string>({
      clock,
      retryBackoff: { baseDelayMs: 40, jitter: 'none' },
    })
    const off = queue.consume((d) => d.nack())
    queue.enqueue('held')
    off()
    expect(queue.delayedCount()).toBe(1)
    const recovered: string[] = []
    queue.consume((d) => {
      recovered.push(d.message.payload)
      d.ack()
    })
    clock.advance(40)
    expect(recovered).toEqual(['held'])
  })
})

