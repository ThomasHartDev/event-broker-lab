export const DEFAULT_CONSUMER = 'default'

export interface TopicSnapshot {
  readonly topic: string
  readonly consumer: string
  readonly logEndOffset: number
  readonly committedOffset: number
  readonly lag: number
  readonly produceRate: number
  readonly consumeRate: number
  readonly oldestAgeMs: number
}

export interface TopicMetricsOptions {
  readonly windowMs?: number
  readonly bucketMs?: number
  readonly now?: () => number
}

const DEFAULT_WINDOW_MS = 10_000
const DEFAULT_BUCKET_MS = 1_000
const MAX_BUCKETS = 1_000

class SlidingWindow {
  private readonly counts: number[]
  private readonly bucketMs: number
  private readonly size: number
  private head = 0
  private cursor = 0
  private primed = false

  constructor(windowMs: number, bucketMs: number) {
    this.bucketMs = bucketMs
    this.size = windowMs / bucketMs
    this.counts = new Array<number>(this.size).fill(0)
  }

  add(now: number, n: number): void {
    this.advance(now)
    const t = now < this.cursor ? this.cursor : now
    const offset = Math.min(this.size - 1, Math.floor((t - this.cursor) / this.bucketMs))
    const i = (this.head + offset) % this.size
    const current = this.counts[i]
    this.counts[i] = (current ?? 0) + n
  }

  sum(now: number): number {
    this.advance(now)
    let total = 0
    for (const count of this.counts) total += count
    return total
  }

  private advance(now: number): void {
    if (!this.primed) {
      this.cursor = now - (now % this.bucketMs)
      this.primed = true
      return
    }
    if (now < this.cursor) return
    const offset = Math.floor((now - this.cursor) / this.bucketMs)
    if (offset < this.size) return
    const shift = offset - this.size + 1
    if (shift >= this.size) {
      // A jump past one full window would walk the ring more than once; reset instead.
      this.counts.fill(0)
      this.head = 0
      this.cursor = now - (now % this.bucketMs) - (this.size - 1) * this.bucketMs
      return
    }
    for (let s = 0; s < shift; s++) {
      this.counts[this.head] = 0
      this.head = (this.head + 1) % this.size
      this.cursor += this.bucketMs
    }
  }
}

interface ConsumerState {
  committedOffset: number
  readonly consumeWindow: SlidingWindow
}

interface TopicState {
  logEndOffset: number
  readonly produceTimes: number[]
  readonly produceWindow: SlidingWindow
  readonly consumers: Map<string, ConsumerState>
}

export class TopicMetrics {
  readonly windowMs: number
  readonly bucketMs: number
  private readonly now: () => number
  private readonly topics = new Map<string, TopicState>()

  constructor(options: TopicMetricsOptions = {}) {
    const windowMs = options.windowMs ?? DEFAULT_WINDOW_MS
    const bucketMs = options.bucketMs ?? DEFAULT_BUCKET_MS
    if (!Number.isInteger(windowMs) || windowMs < 1) {
      throw new Error(`windowMs must be a positive integer, got ${windowMs}`)
    }
    if (!Number.isInteger(bucketMs) || bucketMs < 1) {
      throw new Error(`bucketMs must be a positive integer, got ${bucketMs}`)
    }
    if (windowMs % bucketMs !== 0) {
      throw new Error(`windowMs (${windowMs}) must be a multiple of bucketMs (${bucketMs})`)
    }
    const buckets = windowMs / bucketMs
    if (buckets > MAX_BUCKETS) {
      throw new Error(`windowMs / bucketMs must be <= ${MAX_BUCKETS}, got ${buckets}`)
    }
    this.windowMs = windowMs
    this.bucketMs = bucketMs
    this.now = options.now ?? Date.now
  }

  produced(topic: string): number {
    this.assertName(topic, 'topic')
    const t = this.timestamp()
    const state = this.topicState(topic)
    const offset = state.logEndOffset
    state.logEndOffset += 1
    state.produceTimes.push(t)
    state.produceWindow.add(t, 1)
    return offset
  }

  consumed(topic: string, consumer: string = DEFAULT_CONSUMER): boolean {
    this.assertName(topic, 'topic')
    this.assertName(consumer, 'consumer')
    const state = this.topics.get(topic)
    if (!state) return false
    const group = this.consumerState(state, consumer)
    if (group.committedOffset >= state.logEndOffset) return false
    group.committedOffset += 1
    group.consumeWindow.add(this.timestamp(), 1)
    return true
  }

  snapshot(topic: string, consumer: string = DEFAULT_CONSUMER): TopicSnapshot {
    this.assertName(topic, 'topic')
    this.assertName(consumer, 'consumer')
    const state = this.topics.get(topic)
    if (!state) {
      return {
        topic,
        consumer,
        logEndOffset: 0,
        committedOffset: 0,
        lag: 0,
        produceRate: 0,
        consumeRate: 0,
        oldestAgeMs: 0,
      }
    }
    return this.capture(topic, state, consumer)
  }

  snapshots(): TopicSnapshot[] {
    const rows: TopicSnapshot[] = []
    for (const [topic, state] of this.topics) {
      if (state.consumers.size === 0) {
        rows.push(this.capture(topic, state, DEFAULT_CONSUMER))
        continue
      }
      for (const consumer of state.consumers.keys()) {
        rows.push(this.capture(topic, state, consumer))
      }
    }
    rows.sort((a, b) => a.topic.localeCompare(b.topic) || a.consumer.localeCompare(b.consumer))
    return rows
  }

  private capture(topic: string, state: TopicState, consumer: string): TopicSnapshot {
    const t = this.timestamp()
    const group = state.consumers.get(consumer)
    const committedOffset = group?.committedOffset ?? 0
    const lag = state.logEndOffset - committedOffset
    const producedAt = state.produceTimes[committedOffset]
    const oldestAgeMs =
      lag > 0 && producedAt !== undefined ? Math.max(0, t - producedAt) : 0
    return {
      topic,
      consumer,
      logEndOffset: state.logEndOffset,
      committedOffset,
      lag,
      produceRate: state.produceWindow.sum(t) * (1000 / this.windowMs),
      consumeRate: group ? group.consumeWindow.sum(t) * (1000 / this.windowMs) : 0,
      oldestAgeMs,
    }
  }

  private topicState(topic: string): TopicState {
    let state = this.topics.get(topic)
    if (!state) {
      state = {
        logEndOffset: 0,
        produceTimes: [],
        produceWindow: new SlidingWindow(this.windowMs, this.bucketMs),
        consumers: new Map(),
      }
      this.topics.set(topic, state)
    }
    return state
  }

  private consumerState(state: TopicState, consumer: string): ConsumerState {
    let group = state.consumers.get(consumer)
    if (!group) {
      group = {
        committedOffset: 0,
        consumeWindow: new SlidingWindow(this.windowMs, this.bucketMs),
      }
      state.consumers.set(consumer, group)
    }
    return group
  }

  private timestamp(): number {
    const t = this.now()
    if (!Number.isFinite(t)) {
      throw new Error(`now() must return a finite number, got ${t}`)
    }
    return t
  }

  private assertName(value: string, label: string): void {
    if (value.length === 0) {
      throw new Error(`${label} must be a non-empty string`)
    }
  }
}
