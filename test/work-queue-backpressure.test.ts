import { describe, it, expect } from 'vitest'
import { QueueFullError, WorkQueue, type BackpressureEvent, type Delivery } from '../src/index.js'

describe('WorkQueue bounded ready backlog', () => {
  it('throws QueueFullError once ready depth hits capacity', () => {
    const queue = new WorkQueue<string>({ capacity: 2 })
    expect(queue.enqueue('a')).toBe(1)
    expect(queue.enqueue('b')).toBe(2)
    expect(queue.readyCount()).toBe(2)
    expect(() => queue.enqueue('c')).toThrow(QueueFullError)
    expect(() => queue.enqueue('c')).toThrow(/capacity 2/)
    expect(queue.tryEnqueue('c')).toEqual({ accepted: false })
    expect(queue.readyCount()).toBe(2)
  })

  it('does not spend a message id on a rejected enqueue', () => {
    const queue = new WorkQueue<string>({ capacity: 1 })
    expect(queue.enqueue('held')).toBe(1)
    expect(queue.tryEnqueue('nope')).toEqual({ accepted: false })
    const seen: number[] = []
    queue.consume((d) => {
      seen.push(d.message.id)
      d.ack()
    })
    expect(seen).toEqual([1])
    expect(queue.enqueue('after')).toBe(2)
  })

  it('accepts again after consumers drain below capacity', () => {
    const queue = new WorkQueue<number>({ capacity: 1 })
    queue.enqueue(1)
    expect(queue.tryEnqueue(2).accepted).toBe(false)
    const seen: number[] = []
    queue.consume((d) => {
      seen.push(d.message.payload)
      d.ack()
    })
    expect(seen).toEqual([1])
    expect(queue.enqueue(2)).toBe(2)
    expect(seen).toEqual([1, 2])
  })

  it('counts only ready depth, not in-flight deliveries', () => {
    const queue = new WorkQueue<string>({ capacity: 1 })
    const held: Delivery<string>[] = []
    queue.consume((d) => held.push(d))
    expect(queue.enqueue('in-flight')).toBe(1)
    expect(queue.readyCount()).toBe(0)
    expect(queue.inFlightCount()).toBe(1)
    expect(queue.enqueue('ready')).toBe(2)
    expect(queue.readyCount()).toBe(1)
    expect(queue.tryEnqueue('over')).toEqual({ accepted: false })
    held[0]?.ack()
    expect(queue.readyCount()).toBe(0)
    expect(queue.enqueue('after-ack')).toBe(3)
  })

  it('lets redelivery exceed capacity so accepted work is not dropped', () => {
    const queue = new WorkQueue<string>({ capacity: 1 })
    const held: Delivery<string>[] = []
    const off = queue.consume((d) => held.push(d), { prefetch: 2 })
    queue.enqueue('a')
    queue.enqueue('b')
    expect(queue.inFlightCount()).toBe(2)
    expect(queue.readyCount()).toBe(0)
    off()
    expect(queue.readyCount()).toBe(2)
    expect(queue.readyCount()).toBeGreaterThan(queue.capacity)
    expect(queue.tryEnqueue('c')).toEqual({ accepted: false })
    expect(queue.backpressure()).toBe('paused')
  })
})

describe('WorkQueue backpressure signaling', () => {
  it('stays open when a consumer drains as fast as we enqueue', () => {
    const queue = new WorkQueue<string>({ capacity: 2, highWatermark: 2, lowWatermark: 1 })
    const events: BackpressureEvent[] = []
    queue.onBackpressure((event) => events.push(event))
    queue.consume((d) => d.ack())
    queue.enqueue('a')
    queue.enqueue('b')
    queue.enqueue('c')
    expect(queue.backpressure()).toBe('open')
    expect(events).toEqual([])
  })

  it('pauses at high watermark and resumes at or below low, without flapping in the band', () => {
    const queue = new WorkQueue<string>({ capacity: 4, highWatermark: 3, lowWatermark: 1 })
    const events: Pick<BackpressureEvent, 'state' | 'occupancy'>[] = []
    queue.onBackpressure((event) => events.push({ state: event.state, occupancy: event.occupancy }))

    queue.enqueue('1')
    queue.enqueue('2')
    expect(queue.backpressure()).toBe('open')
    expect(events).toEqual([])

    queue.enqueue('3')
    expect(queue.backpressure()).toBe('paused')
    expect(events).toEqual([{ state: 'paused', occupancy: 3 }])

    queue.enqueue('4')
    expect(queue.tryEnqueue('5')).toEqual({ accepted: false })
    expect(events).toEqual([{ state: 'paused', occupancy: 3 }])

    const held: Delivery<string>[] = []
    queue.consume((d) => held.push(d))
    expect(queue.readyCount()).toBe(3)
    expect(queue.backpressure()).toBe('paused')

    held[0]?.ack()
    expect(queue.readyCount()).toBe(2)
    expect(queue.backpressure()).toBe('paused')
    held[1]?.ack()
    expect(queue.readyCount()).toBe(1)
    expect(queue.backpressure()).toBe('open')
    expect(events).toEqual([
      { state: 'paused', occupancy: 3 },
      { state: 'open', occupancy: 1 },
    ])
  })

  it('unsubscribing a listener stops further events', () => {
    const queue = new WorkQueue<string>({ capacity: 2 })
    const seen: string[] = []
    const off = queue.onBackpressure((event) => seen.push(event.state))
    queue.enqueue('a')
    queue.enqueue('b')
    expect(seen).toEqual(['paused'])
    off()
    off()
    const next: string[] = []
    queue.onBackpressure((event) => next.push(event.state))
    queue.consume((d) => d.ack())
    expect(seen).toEqual(['paused'])
    expect(next).toEqual(['open'])
  })

  it('delivers the first listener error after the rest of the snapshot runs', () => {
    const queue = new WorkQueue<string>({ capacity: 1 })
    const order: string[] = []
    queue.onBackpressure(() => {
      order.push('a')
      throw new Error('listener-a')
    })
    queue.onBackpressure(() => {
      order.push('b')
    })
    expect(() => queue.enqueue('full')).toThrow(/listener-a/)
    expect(order).toEqual(['a', 'b'])
    expect(queue.readyCount()).toBe(1)
    expect(queue.backpressure()).toBe('paused')
  })

  it('does not reject a nack requeue when ready is already at capacity', () => {
    const queue = new WorkQueue<string>({ capacity: 1 })
    const events: string[] = []
    queue.onBackpressure((event) => events.push(event.state))
    let first: Delivery<string> | undefined
    const seen: string[] = []
    queue.consume((d) => {
      if (!first) {
        first = d
        return
      }
      seen.push(d.message.payload)
      d.ack()
    })
    queue.enqueue('held')
    queue.enqueue('waiting')
    expect(queue.readyCount()).toBe(1)
    expect(queue.backpressure()).toBe('paused')
    expect(() => first!.nack()).not.toThrow()
    expect(seen).toEqual(['waiting', 'held'])
    expect(queue.readyCount()).toBe(0)
    expect(queue.backpressure()).toBe('open')
    expect(events).toEqual(['paused', 'open'])
  })
})
