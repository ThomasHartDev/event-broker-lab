import { describe, it, expect } from 'vitest'
import { KeyedWorkQueue, type KeyedDelivery } from '../src/index.js'

describe('KeyedWorkQueue per-key order', () => {
  it('throws on empty key and invalid options', () => {
    const queue = new KeyedWorkQueue<string>()
    expect(() => queue.enqueue('', 'x')).toThrow(/non-empty/)
    expect(() => new KeyedWorkQueue({ maxDeliveryCount: 0 })).toThrow(/maxDeliveryCount/)
    expect(() => queue.consume(() => {}, { prefetch: 0 })).toThrow(/prefetch/)
  })

  it('delivers the same key in enqueue order with monotonic ids', () => {
    const queue = new KeyedWorkQueue<number>()
    const seen: { id: number; payload: number }[] = []
    for (let i = 0; i < 2; i++) {
      queue.consume((d) => {
        seen.push({ id: d.message.id, payload: d.message.payload })
        d.ack()
      })
    }
    queue.enqueue('order-1', 1)
    queue.enqueue('order-1', 2)
    queue.enqueue('order-1', 3)
    expect(seen.map((s) => s.payload)).toEqual([1, 2, 3])
    expect(seen.map((s) => s.id)).toEqual([1, 2, 3])
  })

  it('blocks a key tail until the head acks, while another key is in flight', () => {
    const queue = new KeyedWorkQueue<string>()
    const held: KeyedDelivery<string>[] = []
    queue.consume((d) => held.push(d))
    queue.consume((d) => held.push(d))
    queue.enqueue('A', 'A1')
    queue.enqueue('A', 'A2')
    queue.enqueue('B', 'B1')
    expect(held.map((d) => `${d.message.key}:${d.message.payload}`).sort()).toEqual([
      'A:A1',
      'B:B1',
    ])
    expect(queue.readyCount()).toBe(1)
    expect(queue.inFlightCount()).toBe(2)
    held.find((d) => d.message.payload === 'A1')?.ack()
    expect(held.map((d) => d.message.payload)).toContain('A2')
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(2)
  })

  it('prefetch cannot pull two messages of the same key', () => {
    const queue = new KeyedWorkQueue<string>()
    const held: KeyedDelivery<string>[] = []
    queue.consume((d) => held.push(d), { prefetch: 4 })
    queue.enqueue('A', '1')
    queue.enqueue('A', '2')
    queue.enqueue('B', '1')
    expect(held.map((d) => d.message.key).sort()).toEqual(['A', 'B'])
    expect(queue.readyCount()).toBe(1)
  })

  it('buffers with no consumer, then drains per key', () => {
    const queue = new KeyedWorkQueue<number>()
    queue.enqueue('A', 1)
    queue.enqueue('B', 10)
    queue.enqueue('A', 2)
    expect(queue.readyCount()).toBe(3)
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(`${d.message.key}:${d.message.payload}`)
      d.ack()
    })
    expect(seen).toEqual(['A:1', 'B:10', 'A:2'])
  })
})

describe('nack, drop, and cancel keep per-key order', () => {
  it('redelivers a nacked head before the next message of that key', () => {
    const queue = new KeyedWorkQueue<string>()
    const seen: string[] = []
    queue.enqueue('A', 'A1')
    queue.enqueue('A', 'A2')
    queue.consume((d) => {
      seen.push(`${d.message.payload}#${d.message.deliveryCount}`)
      if (d.message.payload === 'A1' && d.message.deliveryCount === 1) d.nack()
      else d.ack()
    })
    expect(seen).toEqual(['A1#1', 'A1#2', 'A2#1'])
  })

  it('nack({ requeue: false }) drops the head and releases the tail', () => {
    const queue = new KeyedWorkQueue<string>()
    const seen: string[] = []
    queue.enqueue('A', 'A1')
    queue.enqueue('A', 'A2')
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (d.message.payload === 'A1') d.nack({ requeue: false })
      else d.ack()
    })
    expect(seen).toEqual(['A1', 'A2'])
    expect(queue.readyCount()).toBe(0)
  })

  it('handler throw requeues the head of that key', () => {
    const queue = new KeyedWorkQueue<string>()
    let throws = true
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (throws && d.message.payload === 'A1') {
        throws = false
        throw new Error('boom')
      }
      d.ack()
    })
    queue.enqueue('A', 'A1')
    queue.enqueue('A', 'A2')
    expect(seen).toEqual(['A1', 'A1', 'A2'])
  })

  it('cancel requeues the in-flight head ahead of the same-key tail', () => {
    const queue = new KeyedWorkQueue<string>()
    let stolen: KeyedDelivery<string> | undefined
    const off = queue.consume((d) => {
      stolen = d
    })
    queue.enqueue('A', 'A1')
    queue.enqueue('A', 'A2')
    expect(queue.inFlightCount()).toBe(1)
    off()
    expect(() => off()).not.toThrow()
    expect(queue.consumerCount()).toBe(0)
    expect(queue.readyCount()).toBe(2)
    const recovered: string[] = []
    queue.consume((d) => {
      recovered.push(d.message.payload)
      d.ack()
    })
    expect(recovered).toEqual(['A1', 'A2'])
    expect(() => stolen?.ack()).not.toThrow()
  })

  it('double settle is a no-op', () => {
    const queue = new KeyedWorkQueue<string>()
    let first: KeyedDelivery<string> | undefined
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
    queue.enqueue('k', 'x')
    expect(() => first?.ack()).not.toThrow()
    expect(() => first?.nack()).not.toThrow()
    expect(count).toBe(2)
  })

  it('always-nack stays bounded and does not starve another key', () => {
    const maxDeliveryCount = 4
    const queue = new KeyedWorkQueue<string>({ maxDeliveryCount })
    const seen: string[] = []
    queue.enqueue('poison', 'spin')
    queue.enqueue('ok', 'live')
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (d.message.key === 'poison') d.nack()
      else d.ack()
    })
    expect(seen.filter((p) => p === 'spin')).toHaveLength(maxDeliveryCount)
    expect(seen.indexOf('live')).toBeLessThan(seen.lastIndexOf('spin'))
    expect(queue.readyCount()).toBe(0)
    expect(queue.enqueue('ok', 'after')).toBe(3)
  })

  it('enqueue from a handler preserves that key order', () => {
    const queue = new KeyedWorkQueue<string>()
    const seen: string[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      if (d.message.payload === 'seed') queue.enqueue('A', 'child')
      d.ack()
    })
    queue.enqueue('A', 'seed')
    queue.enqueue('A', 'after')
    expect(seen).toEqual(['seed', 'child', 'after'])
  })
})
