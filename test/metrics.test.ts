import { describe, it, expect } from 'vitest'
import { TopicMetrics, DEFAULT_CONSUMER } from '../src/index.js'

function clock(start = 0) {
  const t = { now: start }
  const metrics = (windowMs = 1000, bucketMs = 100) =>
    new TopicMetrics({ windowMs, bucketMs, now: () => t.now })
  return { t, metrics }
}

describe('TopicMetrics construction', () => {
  it('rejects bad windows, empty names, and a non-finite clock', () => {
    expect(() => new TopicMetrics({ windowMs: 0 })).toThrow(/windowMs/)
    expect(() => new TopicMetrics({ windowMs: 1.5 })).toThrow(/windowMs/)
    expect(() => new TopicMetrics({ bucketMs: 0 })).toThrow(/bucketMs/)
    expect(() => new TopicMetrics({ windowMs: 1000, bucketMs: 300 })).toThrow(/multiple/)
    expect(() => new TopicMetrics({ windowMs: 2000, bucketMs: 1 })).toThrow(/<= 1000/)
    const metrics = new TopicMetrics({ now: () => Number.NaN })
    expect(() => metrics.produced('')).toThrow(/topic/)
    expect(() => new TopicMetrics().consumed('orders', '')).toThrow(/consumer/)
    expect(() => metrics.produced('orders')).toThrow(/finite/)
  })
})

describe('offset lag', () => {
  it('assigns 0-based offsets and reports lag as log end minus committed', () => {
    const metrics = new TopicMetrics({ now: () => 0 })
    expect(metrics.produced('orders')).toBe(0)
    expect(metrics.produced('orders')).toBe(1)
    expect(metrics.produced('orders')).toBe(2)
    expect(metrics.snapshot('orders')).toMatchObject({
      topic: 'orders',
      consumer: DEFAULT_CONSUMER,
      logEndOffset: 3,
      committedOffset: 0,
      lag: 3,
      oldestAgeMs: 0,
    })
  })

  it('drops lag on FIFO consume and is a no-op once caught up', () => {
    const metrics = new TopicMetrics({ now: () => 0 })
    metrics.produced('orders')
    metrics.produced('orders')
    expect(metrics.consumed('orders')).toBe(true)
    expect(metrics.snapshot('orders').lag).toBe(1)
    expect(metrics.consumed('orders')).toBe(true)
    expect(metrics.snapshot('orders')).toMatchObject({
      logEndOffset: 2,
      committedOffset: 2,
      lag: 0,
      oldestAgeMs: 0,
    })
    expect(metrics.consumed('orders')).toBe(false)
    expect(metrics.snapshot('orders').committedOffset).toBe(2)
  })

  it('does not consume or materialize a topic that has never been produced', () => {
    const metrics = new TopicMetrics()
    expect(metrics.consumed('ghost')).toBe(false)
    expect(metrics.snapshot('ghost')).toEqual({
      topic: 'ghost',
      consumer: DEFAULT_CONSUMER,
      logEndOffset: 0,
      committedOffset: 0,
      lag: 0,
      produceRate: 0,
      consumeRate: 0,
      oldestAgeMs: 0,
    })
    metrics.snapshot('orders')
    expect(metrics.snapshots()).toEqual([])
  })

  it('isolates offsets across topics', () => {
    const metrics = new TopicMetrics({ now: () => 0 })
    metrics.produced('orders')
    metrics.produced('orders')
    metrics.produced('shipments')
    metrics.consumed('orders')
    expect(metrics.snapshot('orders')).toMatchObject({ lag: 1, logEndOffset: 2 })
    expect(metrics.snapshot('shipments')).toMatchObject({ lag: 1, logEndOffset: 1 })
  })

  it('tracks independent committed offsets per consumer on the same topic', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    t.now = 10
    m.produced('orders')
    t.now = 20
    m.produced('orders')
    t.now = 50
    expect(m.consumed('orders', 'billing')).toBe(true)
    expect(m.consumed('orders', 'billing')).toBe(true)
    expect(m.snapshot('orders', 'billing')).toMatchObject({
      committedOffset: 2,
      lag: 1,
      oldestAgeMs: 30,
    })
    expect(m.snapshot('orders', 'search')).toMatchObject({
      committedOffset: 0,
      lag: 3,
      oldestAgeMs: 50,
    })
    expect(m.consumed('orders', 'search')).toBe(true)
    expect(m.snapshot('orders', 'search').oldestAgeMs).toBe(40)
    expect(m.snapshot('orders', 'billing').lag).toBe(1)
  })
})

describe('time lag', () => {
  it('ages the oldest unconsumed produce and resets after catch-up', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    t.now = 40
    m.produced('orders')
    t.now = 90
    expect(m.snapshot('orders').oldestAgeMs).toBe(90)
    m.consumed('orders')
    expect(m.snapshot('orders').oldestAgeMs).toBe(50)
    m.consumed('orders')
    expect(m.snapshot('orders').oldestAgeMs).toBe(0)
    t.now = 10
    expect(m.snapshot('orders').oldestAgeMs).toBe(0)
  })
})

describe('sliding-window throughput', () => {
  it('reports produce rate as window sum over window seconds', () => {
    const { t, metrics } = clock()
    const wide = metrics(10_000, 1_000)
    for (let i = 0; i < 10; i++) wide.produced('orders')
    expect(wide.snapshot('orders').produceRate).toBe(1)
    expect(wide.snapshot('orders').consumeRate).toBe(0)
    const burst = metrics()
    for (let i = 0; i < 5; i++) burst.produced('shipments')
    expect(burst.snapshot('shipments').produceRate).toBe(5)
    t.now = 400
    burst.produced('shipments')
    expect(burst.snapshot('shipments').produceRate).toBe(6)
  })

  it('expires produces that have left the window and keeps later ones', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    t.now = 800
    m.produced('orders')
    m.produced('orders')
    t.now = 1500
    expect(m.snapshot('orders')).toMatchObject({ produceRate: 2, logEndOffset: 3, lag: 3 })
  })

  it('records consume rate only for successful commits', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    m.produced('orders')
    expect(m.consumed('orders')).toBe(true)
    expect(m.consumed('orders')).toBe(true)
    expect(m.consumed('orders')).toBe(false)
    expect(m.snapshot('orders').consumeRate).toBe(2)
    t.now = 1500
    expect(m.snapshot('orders').consumeRate).toBe(0)
    expect(m.snapshot('orders').committedOffset).toBe(2)
  })

  it('zeros rates after a jump larger than the window, without resetting offsets', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    m.produced('orders')
    m.consumed('orders')
    t.now = 50_000
    expect(m.snapshot('orders')).toMatchObject({
      produceRate: 0,
      consumeRate: 0,
      logEndOffset: 2,
      committedOffset: 1,
      lag: 1,
    })
  })

  it('keeps independent consume rates per consumer', () => {
    const { t, metrics } = clock()
    const m = metrics()
    m.produced('orders')
    m.produced('orders')
    m.produced('orders')
    m.consumed('orders', 'billing')
    t.now = 200
    m.consumed('orders', 'billing')
    m.consumed('orders', 'search')
    expect(m.snapshot('orders', 'billing').consumeRate).toBe(2)
    expect(m.snapshot('orders', 'search').consumeRate).toBe(1)
    expect(m.snapshot('orders', 'billing').produceRate).toBe(
      m.snapshot('orders', 'search').produceRate,
    )
  })
})

describe('snapshots listing', () => {
  it('lists produced topics with the default consumer until a named one commits', () => {
    const metrics = new TopicMetrics({ now: () => 0 })
    metrics.produced('zeta')
    metrics.produced('alpha')
    expect(metrics.snapshots().map((s) => [s.topic, s.consumer, s.lag])).toEqual([
      ['alpha', DEFAULT_CONSUMER, 1],
      ['zeta', DEFAULT_CONSUMER, 1],
    ])
    metrics.consumed('alpha', 'billing')
    metrics.consumed('alpha', 'search')
    expect(metrics.snapshots().map((s) => [s.topic, s.consumer, s.lag])).toEqual([
      ['alpha', 'billing', 0],
      ['alpha', 'search', 0],
      ['zeta', DEFAULT_CONSUMER, 1],
    ])
  })
})
