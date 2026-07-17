import { describe, it, expect, vi } from 'vitest'
import { Broker, type Message } from '../src/index.js'

describe('Broker fan-out', () => {
  it('delivers a message to every subscriber on the topic', () => {
    const broker = new Broker<string>()
    const a = vi.fn()
    const b = vi.fn()
    broker.subscribe('orders', a)
    broker.subscribe('orders', b)

    const delivered = broker.publish('orders', 'hello')

    expect(delivered).toBe(2)
    expect(a).toHaveBeenCalledOnce()
    expect(b).toHaveBeenCalledOnce()
    expect(a.mock.calls[0]?.[0]).toMatchObject({ topic: 'orders', payload: 'hello', id: 1 })
  })

  it('only delivers to subscribers of the published topic', () => {
    const broker = new Broker<number>()
    const orders = vi.fn()
    const shipments = vi.fn()
    broker.subscribe('orders', orders)
    broker.subscribe('shipments', shipments)

    broker.publish('orders', 42)

    expect(orders).toHaveBeenCalledOnce()
    expect(shipments).not.toHaveBeenCalled()
  })

  it('returns 0 and does nothing when a topic has no subscribers', () => {
    const broker = new Broker<string>()
    expect(broker.publish('empty', 'x')).toBe(0)
  })

  it('assigns monotonic ids across topics starting at 1', () => {
    const broker = new Broker<string>()
    const seen: number[] = []
    broker.subscribe('a', (m) => seen.push(m.id))
    broker.subscribe('b', (m) => seen.push(m.id))

    broker.publish('a', 'one')
    broker.publish('b', 'two')
    broker.publish('a', 'three')

    expect(seen).toEqual([1, 2, 3])
  })

  it('preserves registration order during fan-out', () => {
    const broker = new Broker<string>()
    const order: string[] = []
    broker.subscribe('t', () => order.push('first'))
    broker.subscribe('t', () => order.push('second'))
    broker.subscribe('t', () => order.push('third'))

    broker.publish('t', 'x')

    expect(order).toEqual(['first', 'second', 'third'])
  })
})

describe('subscribe / unsubscribe', () => {
  it('stops delivery after unsubscribe', () => {
    const broker = new Broker<string>()
    const handler = vi.fn()
    const off = broker.subscribe('t', handler)

    broker.publish('t', 'one')
    off()
    broker.publish('t', 'two')

    expect(handler).toHaveBeenCalledOnce()
    expect(broker.subscriberCount('t')).toBe(0)
  })

  it('is idempotent: calling unsubscribe twice is safe', () => {
    const broker = new Broker<string>()
    const handler = vi.fn()
    const off = broker.subscribe('t', handler)
    off()
    expect(() => off()).not.toThrow()
    expect(broker.subscriberCount('t')).toBe(0)
  })

  it('deduplicates the same handler on one topic', () => {
    const broker = new Broker<string>()
    const handler = vi.fn()
    broker.subscribe('t', handler)
    broker.subscribe('t', handler)

    expect(broker.subscriberCount('t')).toBe(1)
    expect(broker.publish('t', 'x')).toBe(1)
    expect(handler).toHaveBeenCalledOnce()
  })

  it('drops a topic from topicNames once its last subscriber leaves', () => {
    const broker = new Broker<string>()
    const off = broker.subscribe('t', () => {})
    expect(broker.topicNames()).toEqual(['t'])
    off()
    expect(broker.topicNames()).toEqual([])
  })
})

describe('delivery snapshot semantics', () => {
  it('does not deliver to a handler that subscribes during dispatch', () => {
    const broker = new Broker<string>()
    const late = vi.fn()
    broker.subscribe('t', () => {
      broker.subscribe('t', late)
    })

    broker.publish('t', 'first')
    expect(late).not.toHaveBeenCalled()

    broker.publish('t', 'second')
    expect(late).toHaveBeenCalledOnce()
  })

  it('still delivers this message to a handler that unsubscribes a peer mid-dispatch', () => {
    const broker = new Broker<string>()
    const received: string[] = []
    let offB = () => {}
    broker.subscribe('t', () => {
      received.push('a')
      offB()
    })
    offB = broker.subscribe('t', (m: Message<string>) => {
      received.push('b:' + m.payload)
    })

    broker.publish('t', 'one')
    expect(received).toEqual(['a', 'b:one'])

    broker.publish('t', 'two')
    expect(received).toEqual(['a', 'b:one', 'a'])
  })
})

describe('pattern subscriptions', () => {
  it('delivers to a pattern subscriber whose pattern matches the topic', () => {
    const broker = new Broker<string>()
    const seen: string[] = []
    broker.subscribePattern('orders.#', (m) => seen.push(m.topic))

    expect(broker.publish('orders.created', 'a')).toBe(1)
    expect(broker.publish('orders.created.us', 'b')).toBe(1)
    expect(broker.publish('shipments.created', 'c')).toBe(0)

    expect(seen).toEqual(['orders.created', 'orders.created.us'])
  })

  it('fans out to both exact and pattern subscribers, exact first', () => {
    const broker = new Broker<string>()
    const order: string[] = []
    broker.subscribe('orders.created', () => order.push('exact'))
    broker.subscribePattern('orders.*', () => order.push('pattern'))

    expect(broker.publish('orders.created', 'x')).toBe(2)
    expect(order).toEqual(['exact', 'pattern'])
  })

  it('invokes a handler once per matching pattern binding', () => {
    const broker = new Broker<string>()
    const seen: string[] = []
    broker.subscribePattern('orders.*', () => seen.push('star'))
    broker.subscribePattern('orders.#', () => seen.push('hash'))

    expect(broker.publish('orders.created', 'x')).toBe(2)
    expect(seen).toEqual(['star', 'hash'])
  })

  it('stops delivery after the pattern is unsubscribed', () => {
    const broker = new Broker<string>()
    const handler = vi.fn()
    const off = broker.subscribePattern('orders.#', handler)

    broker.publish('orders.created', 'a')
    off()
    broker.publish('orders.created', 'b')

    expect(handler).toHaveBeenCalledOnce()
  })

  it('throws on an invalid pattern', () => {
    const broker = new Broker<string>()
    expect(() => broker.subscribePattern('', () => {})).toThrow(/invalid topic pattern/)
    expect(() => broker.subscribePattern('orders..*', () => {})).toThrow()
  })

  it('does not consume a message id when nothing matches', () => {
    const broker = new Broker<string>()
    const ids: number[] = []
    broker.subscribePattern('orders.#', (m) => ids.push(m.id))

    broker.publish('shipments.created', 'skip') // no match, no id burned
    broker.publish('orders.created', 'hit')

    expect(ids).toEqual([1])
  })
})
