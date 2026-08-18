import { describe, it, expect } from 'vitest'
import { WatermarkGate, resolveQueueBounds, QueueFullError } from '../src/index.js'

describe('resolveQueueBounds', () => {
  it('defaults to an unbounded ready queue', () => {
    expect(resolveQueueBounds()).toEqual({
      capacity: Number.POSITIVE_INFINITY,
      highWatermark: Number.POSITIVE_INFINITY,
      lowWatermark: 0,
    })
  })

  it('defaults high to capacity and low to half, with hysteresis when possible', () => {
    expect(resolveQueueBounds({ capacity: 8 })).toEqual({
      capacity: 8,
      highWatermark: 8,
      lowWatermark: 4,
    })
    expect(resolveQueueBounds({ capacity: 1 })).toEqual({
      capacity: 1,
      highWatermark: 1,
      lowWatermark: 0,
    })
    expect(resolveQueueBounds({ capacity: 10, highWatermark: 3 })).toEqual({
      capacity: 10,
      highWatermark: 3,
      lowWatermark: 2,
    })
  })

  it('rejects invalid capacity and watermark combinations', () => {
    expect(() => resolveQueueBounds({ capacity: 0 })).toThrow(/capacity/)
    expect(() => resolveQueueBounds({ capacity: 1.5 })).toThrow(/capacity/)
    expect(() => resolveQueueBounds({ highWatermark: 1 })).toThrow(/require a finite capacity/)
    expect(() => resolveQueueBounds({ capacity: 4, highWatermark: 5 })).toThrow(/highWatermark/)
    expect(() => resolveQueueBounds({ capacity: 4, highWatermark: 2, lowWatermark: 3 })).toThrow(
      /lowWatermark/,
    )
    expect(() => resolveQueueBounds({ capacity: 4, lowWatermark: -1 })).toThrow(/lowWatermark/)
  })
})

describe('WatermarkGate hysteresis', () => {
  it('starts open and only emits on transitions', () => {
    const gate = new WatermarkGate(3, 1)
    expect(gate.state).toBe('open')
    expect(gate.observe(0)).toBeUndefined()
    expect(gate.observe(2)).toBeUndefined()
    expect(gate.observe(3)).toBe('paused')
    expect(gate.observe(3)).toBeUndefined()
    expect(gate.observe(2)).toBeUndefined()
    expect(gate.observe(1)).toBe('open')
    expect(gate.observe(1)).toBeUndefined()
  })

  it('holds the current state inside the band between low and high', () => {
    const gate = new WatermarkGate(4, 1)
    expect(gate.observe(4)).toBe('paused')
    expect(gate.observe(3)).toBeUndefined()
    expect(gate.observe(2)).toBeUndefined()
    expect(gate.state).toBe('paused')
    expect(gate.observe(1)).toBe('open')
    expect(gate.observe(2)).toBeUndefined()
    expect(gate.observe(3)).toBeUndefined()
    expect(gate.state).toBe('open')
  })

  it('does not flap when high equals low', () => {
    const gate = new WatermarkGate(2, 2)
    expect(gate.observe(2)).toBe('paused')
    expect(gate.observe(2)).toBeUndefined()
    expect(gate.observe(1)).toBe('open')
    expect(gate.observe(1)).toBeUndefined()
    expect(gate.observe(2)).toBe('paused')
  })

  it('handles occupancy jumps that skip the band', () => {
    const gate = new WatermarkGate(5, 2)
    expect(gate.observe(9)).toBe('paused')
    expect(gate.observe(0)).toBe('open')
  })

  it('rejects inverted watermarks and negative occupancy', () => {
    expect(() => new WatermarkGate(0, 0)).toThrow(/high watermark/)
    expect(() => new WatermarkGate(2, 3)).toThrow(/low watermark/)
    const gate = new WatermarkGate(2, 1)
    expect(() => gate.observe(-1)).toThrow(/occupancy/)
    expect(() => gate.observe(Number.NaN)).toThrow(/occupancy/)
  })
})

describe('QueueFullError', () => {
  it('names the capacity that rejected the producer', () => {
    const error = new QueueFullError(4)
    expect(error).toBeInstanceOf(Error)
    expect(error.name).toBe('QueueFullError')
    expect(error.capacity).toBe(4)
    expect(error.message).toMatch(/capacity 4/)
  })
})
