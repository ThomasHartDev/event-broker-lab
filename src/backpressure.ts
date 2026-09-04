export type FlowState = 'open' | 'paused'

export interface BackpressureEvent {
  readonly state: FlowState
  readonly occupancy: number
  readonly capacity: number
}

export type BackpressureListener = (event: BackpressureEvent) => void

export class QueueFullError extends Error {
  readonly capacity: number

  constructor(capacity: number) {
    super(`queue is full (capacity ${capacity})`)
    this.name = 'QueueFullError'
    this.capacity = capacity
  }
}

export interface QueueBounds {
  readonly capacity: number
  readonly highWatermark: number
  readonly lowWatermark: number
}

export interface QueueBoundOptions {
  readonly capacity?: number
  readonly highWatermark?: number
  readonly lowWatermark?: number
}

export function resolveQueueBounds(options: QueueBoundOptions = {}): QueueBounds {
  const capacity = options.capacity ?? Number.POSITIVE_INFINITY
  if (capacity !== Number.POSITIVE_INFINITY && (!Number.isInteger(capacity) || capacity < 1)) {
    throw new Error(`capacity must be a positive integer, got ${capacity}`)
  }

  if (capacity === Number.POSITIVE_INFINITY) {
    if (options.highWatermark !== undefined || options.lowWatermark !== undefined) {
      throw new Error('highWatermark and lowWatermark require a finite capacity')
    }
    return {
      capacity,
      highWatermark: Number.POSITIVE_INFINITY,
      lowWatermark: 0,
    }
  }

  const high = options.highWatermark ?? capacity
  if (!Number.isInteger(high) || high < 1 || high > capacity) {
    throw new Error(`highWatermark must be an integer in 1..capacity, got ${high}`)
  }

  const low = options.lowWatermark ?? Math.min(Math.floor(capacity / 2), Math.max(0, high - 1))
  if (!Number.isInteger(low) || low < 0 || low > high) {
    throw new Error(`lowWatermark must be an integer in 0..highWatermark, got ${low}`)
  }

  return { capacity, highWatermark: high, lowWatermark: low }
}

export class WatermarkGate {
  readonly high: number
  readonly low: number
  private current: FlowState = 'open'

  constructor(high: number, low: number) {
    if (!Number.isFinite(high) || high < 1) {
      throw new Error(`high watermark must be >= 1, got ${high}`)
    }
    if (!Number.isFinite(low) || low < 0 || low > high) {
      throw new Error(`low watermark must be in 0..high, got ${low}`)
    }
    this.high = high
    this.low = low
  }

  get state(): FlowState {
    return this.current
  }

  observe(occupancy: number): FlowState | undefined {
    if (!Number.isFinite(occupancy) || occupancy < 0) {
      throw new Error(`occupancy must be a finite number >= 0, got ${occupancy}`)
    }
    const before = this.current
    if (occupancy >= this.high) this.current = 'paused'
    else if (occupancy <= this.low) this.current = 'open'
    return this.current !== before ? this.current : undefined
  }
}
