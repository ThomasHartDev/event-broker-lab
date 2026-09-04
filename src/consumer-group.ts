export type AssignmentStrategy = 'range' | 'round-robin'

export interface LogRecord<T> {
  readonly partition: number
  readonly offset: number
  readonly key: string
  readonly payload: T
}

export type RecordHandler<T> = (record: LogRecord<T>) => void

export interface GroupMember {
  readonly id: number
  assignment(): readonly number[]
  leave(): void
}

export function partitionForKey(key: string, partitionCount: number): number {
  if (!Number.isInteger(partitionCount) || partitionCount < 1) {
    throw new Error(`partitionCount must be a positive integer, got ${partitionCount}`)
  }
  let hash = 2166136261
  for (let i = 0; i < key.length; i++) {
    hash ^= key.charCodeAt(i)
    hash = Math.imul(hash, 16777619)
  }
  return (hash >>> 0) % partitionCount
}

export function rangeAssign(
  memberIds: readonly number[],
  partitionCount: number,
): Map<number, number[]> {
  const ids = [...memberIds].sort((a, b) => a - b)
  const assignment = new Map<number, number[]>()
  for (const id of ids) assignment.set(id, [])
  if (ids.length === 0 || partitionCount < 1) return assignment
  const n = ids.length
  const base = Math.floor(partitionCount / n)
  const extra = partitionCount % n
  let partition = 0
  for (let i = 0; i < n; i++) {
    const count = base + (i < extra ? 1 : 0)
    const owned = assignment.get(ids[i]!)!
    for (let j = 0; j < count; j++) owned.push(partition++)
  }
  return assignment
}

export function roundRobinAssign(
  memberIds: readonly number[],
  partitionCount: number,
): Map<number, number[]> {
  const ids = [...memberIds].sort((a, b) => a - b)
  const assignment = new Map<number, number[]>()
  for (const id of ids) assignment.set(id, [])
  if (ids.length === 0) return assignment
  for (let p = 0; p < partitionCount; p++) {
    assignment.get(ids[p % ids.length]!)!.push(p)
  }
  return assignment
}

export class PartitionedTopic<T> {
  readonly partitionCount: number
  private readonly partitions: LogRecord<T>[][]
  private readonly listeners = new Set<() => void>()

  constructor(partitionCount: number) {
    if (!Number.isInteger(partitionCount) || partitionCount < 1) {
      throw new Error(`partitionCount must be a positive integer, got ${partitionCount}`)
    }
    this.partitionCount = partitionCount
    this.partitions = Array.from({ length: partitionCount }, () => [])
  }

  partitionFor(key: string): number {
    return partitionForKey(key, this.partitionCount)
  }

  produce(key: string, payload: T): LogRecord<T> {
    const partition = this.partitionFor(key)
    const bucket = this.partitions[partition]!
    const record: LogRecord<T> = {
      partition,
      offset: bucket.length,
      key,
      payload,
    }
    bucket.push(record)
    for (const listener of [...this.listeners]) listener()
    return record
  }

  log(partition: number): readonly LogRecord<T>[] {
    const bucket = this.partitions[partition]
    if (!bucket) {
      throw new Error(`partition ${partition} out of range (0..${this.partitionCount - 1})`)
    }
    return bucket
  }

  endOffset(partition: number): number {
    return this.log(partition).length
  }

  watch(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }
}

interface MemberState<T> {
  readonly id: number
  readonly handler: RecordHandler<T>
  active: boolean
  partitions: number[]
}

export class ConsumerGroup<T> {
  private readonly topic: PartitionedTopic<T>
  private readonly strategy: AssignmentStrategy
  private readonly members: MemberState<T>[] = []
  private readonly committed: number[]
  private nextMemberId = 1
  private pumping = false
  private pendingPump = false

  constructor(topic: PartitionedTopic<T>, options?: { strategy?: AssignmentStrategy }) {
    this.topic = topic
    this.strategy = options?.strategy ?? 'range'
    this.committed = Array.from({ length: topic.partitionCount }, () => 0)
    topic.watch(() => this.pump())
  }

  join(handler: RecordHandler<T>): GroupMember {
    const member: MemberState<T> = {
      id: this.nextMemberId++,
      handler,
      active: true,
      partitions: [],
    }
    this.members.push(member)
    this.rebalance()
    this.pump()

    return {
      id: member.id,
      assignment: () => member.partitions.slice(),
      leave: () => this.leave(member),
    }
  }

  memberCount(): number {
    return this.members.length
  }

  committedOffset(partition: number): number {
    const offset = this.committed[partition]
    if (offset === undefined) {
      throw new Error(`partition ${partition} out of range (0..${this.topic.partitionCount - 1})`)
    }
    return offset
  }

  private leave(member: MemberState<T>): void {
    if (!member.active) return
    member.active = false
    member.partitions = []
    const i = this.members.indexOf(member)
    if (i !== -1) this.members.splice(i, 1)
    this.rebalance()
    this.pump()
  }

  private rebalance(): void {
    const ids = this.members.map((m) => m.id)
    const assigned =
      this.strategy === 'round-robin'
        ? roundRobinAssign(ids, this.topic.partitionCount)
        : rangeAssign(ids, this.topic.partitionCount)
    for (const member of this.members) {
      member.partitions = assigned.get(member.id) ?? []
    }
  }

  private pump(): void {
    if (this.pumping) {
      this.pendingPump = true
      return
    }
    this.pumping = true
    try {
      do {
        this.pendingPump = false
        for (const member of [...this.members]) {
          if (!member.active) continue
          for (const partition of [...member.partitions]) {
            this.drain(member, partition)
          }
        }
      } while (this.pendingPump)
    } finally {
      this.pumping = false
    }
  }

  private drain(member: MemberState<T>, partition: number): void {
    const log = this.topic.log(partition)
    while (member.active && member.partitions.includes(partition)) {
      const offset = this.committed[partition]
      if (offset === undefined || offset >= log.length) return
      const record = log[offset]
      if (!record) return
      try {
        member.handler(record)
      } catch {
        return
      }
      if (this.committed[partition] === offset) {
        this.committed[partition] = offset + 1
      }
    }
  }
}
