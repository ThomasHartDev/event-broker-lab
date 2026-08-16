import { describe, it, expect } from 'vitest'
import { ReliableBroker, type ReliableDelivery } from '../src/index.js'

describe('ReliableBroker fan-out', () => {
  it('throws on invalid maxDeliveryCount', () => {
    expect(() => new ReliableBroker({ maxDeliveryCount: 0 })).toThrow(/maxDeliveryCount/)
    expect(() => new ReliableBroker({ maxDeliveryCount: 1.5 })).toThrow(/maxDeliveryCount/)
  })

  it('delivers a copy to every matching subscriber and skips other topics', () => {
    const broker = new ReliableBroker<string>()
    const a: string[] = []
    const b: string[] = []
    broker.subscribe('orders', (d) => {
      a.push(d.message.payload)
      d.ack()
    })
    broker.subscribe('orders', (d) => {
      b.push(d.message.payload)
      d.ack()
    })
    broker.subscribe('shipments', (d) => {
      a.push(`ship:${d.message.payload}`)
      d.ack()
    })
    expect(broker.publish('orders', 'A-1')).toBe(2)
    expect(a).toEqual(['A-1'])
    expect(b).toEqual(['A-1'])
    expect(broker.publish('empty', 'x')).toBe(0)
    expect(broker.inFlightCount()).toBe(0)
  })

  it('shares one message id across copies and ignores publishes before subscribe', () => {
    const broker = new ReliableBroker<string>()
    expect(broker.publish('t', 'late')).toBe(0)
    const ids: number[] = []
    const seen: string[] = []
    broker.subscribe('t', (d) => {
      ids.push(d.message.id)
      seen.push(d.message.payload)
      d.ack()
    })
    broker.subscribe('t', (d) => {
      ids.push(d.message.id)
      d.ack()
    })
    broker.publish('t', 'one')
    broker.publish('t', 'two')
    expect(ids).toEqual([1, 1, 2, 2])
    expect(seen).toEqual(['one', 'two'])
  })
})

describe('ack and nack redelivery', () => {
  it('redelivers only to the subscriber that nacked', () => {
    const broker = new ReliableBroker<string>()
    const a: number[] = []
    const b: number[] = []
    const flags: boolean[] = []
    const tags: number[] = []
    broker.subscribe('t', (d) => {
      a.push(d.message.deliveryCount)
      flags.push(d.message.redelivered)
      tags.push(d.deliveryTag)
      if (d.message.deliveryCount < 3) d.nack()
      else d.ack()
    })
    broker.subscribe('t', (d) => {
      b.push(d.message.deliveryCount)
      d.ack()
    })
    broker.publish('t', 'retry')
    expect(a).toEqual([1, 2, 3])
    expect(b).toEqual([1])
    expect(flags).toEqual([false, true, true])
    expect(tags).toEqual([1, 3, 4])
    expect(broker.readyCount()).toBe(0)
    expect(broker.inFlightCount()).toBe(0)
  })

  it('nack({ requeue: false }) drops only that subscriber copy', () => {
    const broker = new ReliableBroker<string>()
    const seen: string[] = []
    broker.subscribe('t', (d) => {
      seen.push(`drop:${d.message.payload}`)
      d.nack({ requeue: false })
    })
    broker.subscribe('t', (d) => {
      seen.push(`keep:${d.message.payload}`)
      d.ack()
    })
    broker.publish('t', 'poison')
    expect(seen).toEqual(['drop:poison', 'keep:poison'])
    expect(broker.readyCount()).toBe(0)
    expect(broker.inFlightCount()).toBe(0)
  })

  it('double settle is a no-op; handler throw requeues', () => {
    const broker = new ReliableBroker<string>()
    let first: ReliableDelivery<string> | undefined
    let throws = true
    const seen: string[] = []
    broker.subscribe('t', (d) => {
      seen.push(d.message.payload)
      if (throws) {
        first = d
        throws = false
        d.nack()
        return
      }
      d.ack()
    })
    broker.publish('t', 'y')
    expect(() => first?.ack()).not.toThrow()
    expect(() => first?.nack()).not.toThrow()
    expect(seen).toEqual(['y', 'y'])

    const recover = new ReliableBroker<string>()
    let boom = true
    const recovered: string[] = []
    recover.subscribe('t', (d) => {
      recovered.push(d.message.payload)
      if (boom) {
        boom = false
        throw new Error('boom')
      }
      d.ack()
    })
    recover.publish('t', 'recover')
    expect(recovered).toEqual(['recover', 'recover'])
    expect(recover.inFlightCount()).toBe(0)
  })

  it('always-nack stays bounded and publish still returns', () => {
    const maxDeliveryCount = 5
    const broker = new ReliableBroker<string>({ maxDeliveryCount })
    let deliveries = 0
    broker.subscribe('t', (d) => {
      deliveries += 1
      d.nack()
    })
    expect(broker.publish('t', 'spin')).toBe(1)
    expect(deliveries).toBe(maxDeliveryCount)
    expect(broker.readyCount()).toBe(0)
    expect(broker.inFlightCount()).toBe(0)
    expect(broker.publish('t', 'next')).toBe(1)
  })
})

describe('prefetch, isolation, unsubscribe', () => {
  it('throws on invalid prefetch', () => {
    const broker = new ReliableBroker<string>()
    expect(() => broker.subscribe('t', () => {}, { prefetch: 0 })).toThrow(/prefetch/)
    expect(() => broker.subscribe('t', () => {}, { prefetch: 1.5 })).toThrow(/prefetch/)
  })

  it('holds later copies at prefetch while peers still receive', () => {
    const broker = new ReliableBroker<string>()
    const held: ReliableDelivery<string>[] = []
    const peer: string[] = []
    broker.subscribe('t', (d) => held.push(d), { prefetch: 1 })
    broker.subscribe('t', (d) => {
      peer.push(d.message.payload)
      d.ack()
    })
    broker.publish('t', 'first')
    broker.publish('t', 'second')
    expect(held).toHaveLength(1)
    expect(held[0]?.message.payload).toBe('first')
    expect(peer).toEqual(['first', 'second'])
    expect(broker.readyCount()).toBe(1)
    held[0]?.ack()
    expect(held[1]?.message.payload).toBe('second')
    expect(broker.readyCount()).toBe(0)
  })

  it('publish from a handler does not drop the new copy', () => {
    const broker = new ReliableBroker<string>()
    const seen: string[] = []
    broker.subscribe('t', (d) => {
      seen.push(d.message.payload)
      if (d.message.payload === 'seed') broker.publish('t', 'child')
      d.ack()
    })
    broker.publish('t', 'seed')
    expect(seen).toEqual(['seed', 'child'])
  })

  it('unsubscribe drops only that inbox; stale settle is harmless', () => {
    const broker = new ReliableBroker<string>()
    let stolen: ReliableDelivery<string> | undefined
    const kept: string[] = []
    const off = broker.subscribe('t', (d) => {
      stolen = d
    })
    broker.subscribe('t', (d) => {
      kept.push(d.message.payload)
      d.ack()
    })
    broker.publish('t', 'held')
    expect(broker.inFlightCount()).toBe(1)
    off()
    expect(() => off()).not.toThrow()
    expect(broker.subscriberCount()).toBe(1)
    expect(broker.inFlightCount()).toBe(0)
    expect(() => stolen?.ack()).not.toThrow()
    expect(() => stolen?.nack()).not.toThrow()
    broker.publish('t', 'solo')
    expect(kept).toEqual(['held', 'solo'])
  })
})
