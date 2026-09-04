export type JitterStrategy = 'none' | 'full' | 'equal' | 'decorrelated'

export interface RetryClock {
  now(): number
  schedule(fn: () => void, delayMs: number): () => void
}

export interface BackoffOptions {
  readonly baseDelayMs?: number
  readonly maxDelayMs?: number
  readonly jitter?: JitterStrategy
  readonly random?: () => number
}

const DEFAULT_BASE_DELAY_MS = 100
const DEFAULT_MAX_DELAY_MS = 30_000
const JITTER: ReadonlySet<JitterStrategy> = new Set([
  'none',
  'full',
  'equal',
  'decorrelated',
])

export function systemClock(): RetryClock {
  return {
    now: () => Date.now(),
    schedule(fn, delayMs) {
      const handle = setTimeout(fn, delayMs)
      return () => clearTimeout(handle)
    },
  }
}

export class ManualClock implements RetryClock {
  private current = 0
  private nextId = 1
  private readonly timers = new Map<number, { at: number; fn: () => void }>()

  now(): number {
    return this.current
  }

  schedule(fn: () => void, delayMs: number): () => void {
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      throw new Error(`delayMs must be a non-negative finite number, got ${delayMs}`)
    }
    const id = this.nextId++
    this.timers.set(id, { at: this.current + delayMs, fn })
    return () => {
      this.timers.delete(id)
    }
  }

  pendingCount(): number {
    return this.timers.size
  }

  advance(ms: number): void {
    if (!Number.isFinite(ms) || ms < 0) {
      throw new Error(`advance must be a non-negative finite number, got ${ms}`)
    }
    const target = this.current + ms
    for (;;) {
      let next: { id: number; at: number; fn: () => void } | undefined
      for (const [id, timer] of this.timers) {
        if (timer.at > target) continue
        if (!next || timer.at < next.at || (timer.at === next.at && id < next.id)) {
          next = { id, at: timer.at, fn: timer.fn }
        }
      }
      if (!next) {
        this.current = target
        return
      }
      this.current = next.at
      this.timers.delete(next.id)
      next.fn()
    }
  }
}

export class RetryBackoff {
  readonly baseDelayMs: number
  readonly maxDelayMs: number
  readonly jitter: JitterStrategy
  private readonly random: () => number

  constructor(options: BackoffOptions = {}) {
    const base = options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS
    const cap = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS
    const jitter = options.jitter ?? 'full'
    if (!Number.isInteger(base) || base < 0) {
      throw new Error(`baseDelayMs must be a non-negative integer, got ${base}`)
    }
    if (!Number.isInteger(cap) || cap < 0) {
      throw new Error(`maxDelayMs must be a non-negative integer, got ${cap}`)
    }
    if (!JITTER.has(jitter)) {
      throw new Error(`jitter must be none, full, equal, or decorrelated, got ${String(jitter)}`)
    }
    this.baseDelayMs = base
    this.maxDelayMs = cap
    this.jitter = jitter
    this.random = options.random ?? Math.random
  }

  cappedExponential(attempt: number): number {
    assertAttempt(attempt)
    const shift = attempt - 1
    // 2^53 is the last integer power Number can represent exactly.
    if (shift >= 53) return this.maxDelayMs
    const raw = this.baseDelayMs * 2 ** shift
    if (!Number.isFinite(raw)) return this.maxDelayMs
    return Math.min(this.maxDelayMs, raw)
  }

  delayMs(attempt: number, lastDelayMs = 0): number {
    assertAttempt(attempt)
    if (lastDelayMs < 0 || !Number.isFinite(lastDelayMs)) {
      throw new Error(`lastDelayMs must be a non-negative finite number, got ${lastDelayMs}`)
    }
    const exp = this.cappedExponential(attempt)
    switch (this.jitter) {
      case 'none':
        return exp
      case 'full':
        return Math.floor(this.unit() * exp)
      case 'equal':
        return Math.floor(exp / 2 + this.unit() * (exp / 2))
      case 'decorrelated': {
        const seed = lastDelayMs > 0 ? lastDelayMs : this.baseDelayMs
        const hi = Math.min(this.maxDelayMs, seed * 3)
        const lo = Math.min(this.baseDelayMs, hi)
        return Math.floor(lo + this.unit() * (hi - lo))
      }
    }
  }

  private unit(): number {
    const u = this.random()
    if (!Number.isFinite(u) || u < 0 || u >= 1) {
      throw new Error(`random() must return a number in [0, 1), got ${u}`)
    }
    return u
  }
}

function assertAttempt(attempt: number): void {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new Error(`attempt must be a positive integer, got ${attempt}`)
  }
}
