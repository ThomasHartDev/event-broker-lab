import { describe, it, expect } from 'vitest'
import { ManualClock, RetryBackoff } from '../src/index.js'

describe('RetryBackoff', () => {
  it('rejects invalid options, attempts, lastDelayMs, and random()', () => {
    expect(() => new RetryBackoff({ baseDelayMs: -1 })).toThrow(/baseDelayMs/)
    expect(() => new RetryBackoff({ baseDelayMs: 1.5 })).toThrow(/baseDelayMs/)
    expect(() => new RetryBackoff({ maxDelayMs: -1 })).toThrow(/maxDelayMs/)
    expect(() => new RetryBackoff({ jitter: 'maybe' as 'full' })).toThrow(/jitter/)
    const backoff = new RetryBackoff({ jitter: 'none' })
    expect(() => backoff.delayMs(0)).toThrow(/attempt/)
    expect(() => backoff.delayMs(1.2)).toThrow(/attempt/)
    expect(() => backoff.delayMs(1, -1)).toThrow(/lastDelayMs/)
    const badRng = new RetryBackoff({ jitter: 'full', random: () => 1 })
    expect(() => badRng.delayMs(1)).toThrow(/random/)
  })

  it('doubles until the cap, including overflowed exponents', () => {
    const backoff = new RetryBackoff({ baseDelayMs: 100, maxDelayMs: 250, jitter: 'none' })
    expect([1, 2, 3, 40].map((n) => backoff.delayMs(n))).toEqual([100, 200, 250, 250])
    const overflow = new RetryBackoff({
      baseDelayMs: Number.MAX_SAFE_INTEGER,
      maxDelayMs: 7,
      jitter: 'none',
    })
    expect(overflow.cappedExponential(54)).toBe(7)
    const zero = new RetryBackoff({ baseDelayMs: 0, jitter: 'none' })
    expect(zero.delayMs(8)).toBe(0)
  })

  it('applies full, equal, and decorrelated jitter from a supplied random', () => {
    const full = (u: number) =>
      new RetryBackoff({ baseDelayMs: 100, jitter: 'full', random: () => u })
    expect(full(0).delayMs(1)).toBe(0)
    expect(full(0.4).delayMs(1)).toBe(40)
    expect(full(0.4).delayMs(2)).toBe(80)
    expect(full(0.999).delayMs(1)).toBe(99)

    const equal = (u: number) =>
      new RetryBackoff({ baseDelayMs: 100, jitter: 'equal', random: () => u })
    expect(equal(0).delayMs(1)).toBe(50)
    expect(equal(0.999).delayMs(1)).toBe(99)
    expect(equal(0).delayMs(2)).toBe(100)
    expect(equal(0.999).delayMs(2)).toBe(199)

    const deco = new RetryBackoff({
      baseDelayMs: 100,
      maxDelayMs: 1000,
      jitter: 'decorrelated',
      random: () => 0.5,
    })
    expect(deco.delayMs(1, 0)).toBe(200)
    expect(deco.delayMs(2, 200)).toBe(350)
    const capped = new RetryBackoff({
      baseDelayMs: 100,
      maxDelayMs: 250,
      jitter: 'decorrelated',
      random: () => 0.999,
    })
    expect(capped.delayMs(1, 200)).toBe(249)
  })
})

describe('ManualClock', () => {
  it('fires due timers in order, skips cancelled ones, and runs nested schedules', () => {
    const clock = new ManualClock()
    const order: string[] = []
    clock.schedule(() => order.push('late'), 30)
    const cancel = clock.schedule(() => order.push('gone'), 10)
    clock.schedule(() => order.push('soon'), 10)
    clock.schedule(() => {
      order.push(`t${clock.now()}`)
      clock.schedule(() => order.push(`t${clock.now()}`), 5)
    }, 10)
    cancel()
    clock.advance(9)
    expect(order).toEqual([])
    clock.advance(21)
    expect(order).toEqual(['soon', 't10', 't15', 'late'])
    expect(clock.now()).toBe(30)
    expect(clock.pendingCount()).toBe(0)
    expect(() => clock.advance(-1)).toThrow(/advance/)
  })
})
