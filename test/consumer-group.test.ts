import { describe, it, expect } from 'vitest'
import {
  ConsumerGroup,
  PartitionedTopic,
  partitionForKey,
  rangeAssign,
  roundRobinAssign,
  type LogRecord,
} from '../src/index.js'

describe('partitionForKey', () => {
  it('throws on a non-positive partition count', () => {
    expect(() => partitionForKey('k', 0)).toThrow(/partitionCount/)
    expect(() => partitionForKey('k', 1.5)).toThrow(/partitionCount/)
  })

  it('is stable and maps the same key to the same partition', () => {
    const n = 8
    const a = partitionForKey('user-42', n)
    expect(a).toBe(partitionForKey('user-42', n))
    expect(a).toBeGreaterThanOrEqual(0)
    expect(a).toBeLessThan(n)
    expect(partitionForKey('', n)).toBe(partitionForKey('', n))
  })

  it('spreads distinct keys across partitions', () => {
    const seen = new Set<number>()
    for (let i = 0; i < 40; i++) seen.add(partitionForKey(`k-${i}`, 4))
    expect(seen.size).toBe(4)
  })
})

describe('rangeAssign and roundRobinAssign', () => {
  it('gives consecutive ranges and dumps remainder on the first members', () => {
    expect(rangeAssign([2, 1], 6)).toEqual(
      new Map([
        [1, [0, 1, 2]],
        [2, [3, 4, 5]],
      ]),
    )
    expect(rangeAssign([1, 2, 3], 5)).toEqual(
      new Map([
        [1, [0, 1]],
        [2, [2, 3]],
        [3, [4]],
      ]),
    )
  })

  it('leaves extra members idle when there are more members than partitions', () => {
    expect(rangeAssign([1, 2, 3, 4, 5], 3)).toEqual(
      new Map([
        [1, [0]],
        [2, [1]],
        [3, [2]],
        [4, []],
        [5, []],
      ]),
    )
  })

  it('interleaves partitions for round-robin and sorts members by id', () => {
    expect(roundRobinAssign([3, 1], 6)).toEqual(
      new Map([
        [1, [0, 2, 4]],
        [3, [1, 3, 5]],
      ]),
    )
    expect(rangeAssign([], 4).size).toBe(0)
    expect(roundRobinAssign([], 4).size).toBe(0)
  })
})

describe('PartitionedTopic', () => {
  it('rejects a bad partition count', () => {
    expect(() => new PartitionedTopic(0)).toThrow(/partitionCount/)
    expect(() => new PartitionedTopic(-1)).toThrow(/partitionCount/)
  })

  it('appends monotonic offsets per partition and preserves per-key order', () => {
    const topic = new PartitionedTopic<string>(4)
    const key = 'acct-7'
    const p = topic.partitionFor(key)
    const first = topic.produce(key, 'a')
    const second = topic.produce(key, 'b')
    expect(first.partition).toBe(p)
    expect(second.partition).toBe(p)
    expect(first.offset).toBe(0)
    expect(second.offset).toBe(1)
    expect(topic.log(p).map((r) => r.payload)).toEqual(['a', 'b'])
    expect(topic.endOffset(p)).toBe(2)
    expect(() => topic.log(4)).toThrow(/out of range/)
  })
})

describe('ConsumerGroup', () => {
  it('delivers each record to exactly one member of the group', () => {
    const topic = new PartitionedTopic<number>(4)
    const group = new ConsumerGroup<number>(topic)
    const a: number[] = []
    const b: number[] = []
    group.join((r) => a.push(r.payload))
    group.join((r) => b.push(r.payload))

    for (let i = 0; i < 12; i++) topic.produce(`k-${i}`, i)

    const all = [...a, ...b].sort((x, y) => x - y)
    expect(all).toEqual([...Array(12).keys()])
    expect(a.length).toBeGreaterThan(0)
    expect(b.length).toBeGreaterThan(0)
    expect(new Set(all).size).toBe(12)
  })

  it('gives every record to each independent group', () => {
    const topic = new PartitionedTopic<string>(3)
    const g1 = new ConsumerGroup<string>(topic)
    const g2 = new ConsumerGroup<string>(topic)
    const one: string[] = []
    const two: string[] = []
    g1.join((r) => one.push(r.payload))
    g2.join((r) => two.push(r.payload))
    topic.produce('a', 'x')
    topic.produce('b', 'y')
    expect(one.sort()).toEqual(['x', 'y'])
    expect(two.sort()).toEqual(['x', 'y'])
  })

  it('applies range and round-robin strategies on join', () => {
    const rangeTopic = new PartitionedTopic<string>(6)
    const range = new ConsumerGroup<string>(rangeTopic)
    const r1 = range.join(() => {})
    const r2 = range.join(() => {})
    expect(r1.assignment()).toEqual([0, 1, 2])
    expect(r2.assignment()).toEqual([3, 4, 5])

    const rr = new ConsumerGroup<string>(new PartitionedTopic<string>(6), {
      strategy: 'round-robin',
    })
    const a = rr.join(() => {})
    const b = rr.join(() => {})
    expect(a.assignment()).toEqual([0, 2, 4])
    expect(b.assignment()).toEqual([1, 3, 5])
  })

  it('rebalances on join and leave, continuing from the group commit', () => {
    const topic = new PartitionedTopic<string>(2)
    const group = new ConsumerGroup<string>(topic)
    const seen: string[] = []
    const hold: LogRecord<string>[] = []
    const first = group.join((r) => {
      seen.push(`1:${r.payload}`)
      hold.push(r)
    })
    expect(first.assignment()).toEqual([0, 1])

    topic.produce('p0-only', 'a')
    expect(seen).toEqual(['1:a'])
    expect(group.committedOffset(hold[0]!.partition)).toBe(1)

    const secondSeen: string[] = []
    const second = group.join((r) => secondSeen.push(r.payload))
    expect(first.assignment().length + second.assignment().length).toBe(2)
    expect(new Set([...first.assignment(), ...second.assignment()]).size).toBe(2)

    first.leave()
    expect(group.memberCount()).toBe(1)
    expect(second.assignment()).toEqual([0, 1])

    topic.produce('p0-only', 'b')
    topic.produce('other', 'c')
    expect(secondSeen).toContain('b')
    expect(secondSeen).toContain('c')
    expect(seen).toEqual(['1:a'])
  })

  it('catches up a late joiner from offset zero', () => {
    const topic = new PartitionedTopic<string>(2)
    topic.produce('k', 'old')
    const group = new ConsumerGroup<string>(topic)
    const seen: string[] = []
    group.join((r) => seen.push(r.payload))
    expect(seen).toEqual(['old'])
  })

  it('stalls a partition on handler throw and retries on the next pump', () => {
    const topic = new PartitionedTopic<string>(1)
    const group = new ConsumerGroup<string>(topic)
    let fail = true
    const seen: string[] = []
    group.join((r) => {
      if (fail) throw new Error('boom')
      seen.push(r.payload)
    })
    topic.produce('k', 'one')
    expect(group.committedOffset(0)).toBe(0)
    expect(seen).toEqual([])

    fail = false
    topic.produce('k', 'two')
    expect(seen).toEqual(['one', 'two'])
    expect(group.committedOffset(0)).toBe(2)
  })

  it('keeps per-key order on the member that owns the partition', () => {
    const topic = new PartitionedTopic<number>(8)
    const group = new ConsumerGroup<number>(topic)
    const byKey = new Map<string, number[]>()
    const take = (r: LogRecord<number>) => {
      const list = byKey.get(r.key) ?? []
      list.push(r.payload)
      byKey.set(r.key, list)
    }
    group.join(take)
    group.join(take)
    for (let i = 0; i < 5; i++) topic.produce('alpha', i)
    expect(byKey.get('alpha')).toEqual([0, 1, 2, 3, 4])
  })

  it('leave is idempotent and extra members stay idle', () => {
    const topic = new PartitionedTopic<string>(1)
    const group = new ConsumerGroup<string>(topic)
    const owner = group.join(() => {})
    const idle = group.join(() => {})
    expect(owner.assignment()).toEqual([0])
    expect(idle.assignment()).toEqual([])
    owner.leave()
    owner.leave()
    expect(group.memberCount()).toBe(1)
    expect(idle.assignment()).toEqual([0])
    expect(() => group.committedOffset(1)).toThrow(/out of range/)
  })
})
