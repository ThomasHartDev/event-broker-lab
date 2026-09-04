import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { crc32, DurableWorkQueue, WriteAheadLog, type Delivery } from '../src/index.js'

const enc = (s: string) => Buffer.from(s)
const payloads = (log: WriteAheadLog) => [...log.replay()].map((b) => Buffer.from(b).toString())

const cleanups: Array<() => void> = []
afterEach(() => {
  while (cleanups.length > 0) cleanups.pop()?.()
})

function tmpFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ebl-wal-'))
  cleanups.push(() => fs.rmSync(dir, { recursive: true, force: true }))
  return path.join(dir, 'log')
}

function drain<T>(q: DurableWorkQueue<T>): T[] {
  const seen: T[] = []
  q.consume((d) => {
    seen.push(d.message.payload)
    d.ack()
  })
  return seen
}

describe('WriteAheadLog', () => {
  it('checksums, fsyncs on append, and preserves order including empty payloads', () => {
    expect(crc32(new Uint8Array())).toBe(0)
    expect(crc32(Buffer.alloc(4))).not.toBe(0)
    expect(crc32(enc('123456789'))).toBe(0xcbf43926)
    let syncs = 0
    const log = WriteAheadLog.memory({ sync: () => { syncs += 1 } })
    expect(payloads(log)).toEqual([])
    expect(log.append(enc('a'))).toBe(0)
    expect(log.append(enc('b'))).toBe(1)
    expect(log.append(enc(''))).toBe(2)
    expect(payloads(log)).toEqual(['a', 'b', ''])
    expect(syncs).toBe(4)
    let skipped = 0
    WriteAheadLog.memory({ fsync: false, sync: () => { skipped += 1 } }).append(enc('y'))
    expect(skipped).toBe(0)
    log.close()
    expect(() => log.append(enc('x'))).toThrow(/closed/)
  })

  it('survives reopen, truncates a torn or corrupt tail, and rewrite keeps the suffix', () => {
    const file = tmpFile()
    const log = WriteAheadLog.open(file)
    log.append(enc('one'))
    log.append(enc('two'))
    log.close()

    const torn = tmpFile()
    fs.copyFileSync(file, torn)
    fs.appendFileSync(torn, Buffer.from([8, 0, 0, 0, 1, 2, 3]))
    const afterTorn = WriteAheadLog.open(torn)
    cleanups.push(() => afterTorn.close())
    expect(payloads(afterTorn)).toEqual(['one', 'two'])
    afterTorn.append(enc('three'))
    expect(payloads(afterTorn)).toEqual(['one', 'two', 'three'])

    const buf = fs.readFileSync(file)
    const at = buf.lastIndexOf(Buffer.from('two'))
    buf[at] = (buf[at] ?? 0) ^ 0xff
    fs.writeFileSync(file, buf)
    const afterCrc = WriteAheadLog.open(file)
    cleanups.push(() => afterCrc.close())
    expect(payloads(afterCrc)).toEqual(['one'])

    const bad = tmpFile()
    fs.writeFileSync(bad, 'XXXX')
    expect(() => WriteAheadLog.open(bad)).toThrow(/magic/)
    const stub = tmpFile()
    fs.writeFileSync(stub, 'EB')
    const reset = WriteAheadLog.open(stub)
    cleanups.push(() => reset.close())
    expect(payloads(reset)).toEqual([])

    const compacted = tmpFile()
    const src = WriteAheadLog.open(compacted)
    src.append(enc('a'))
    src.append(enc('b'))
    src.append(enc('c'))
    src.rewrite([enc('b'), enc('c')])
    src.close()
    const reopened = WriteAheadLog.open(compacted)
    cleanups.push(() => reopened.close())
    expect(payloads(reopened)).toEqual(['b', 'c'])
  })

  it('truncates an eight-byte zero tail and keeps only real payloads', () => {
    const file = tmpFile()
    const log = WriteAheadLog.open(file)
    log.append(enc('one'))
    log.append(enc('two'))
    log.close()
    const size = fs.statSync(file).size
    fs.appendFileSync(file, Buffer.alloc(8))
    const reopened = WriteAheadLog.open(file)
    cleanups.push(() => reopened.close())
    expect(payloads(reopened)).toEqual(['one', 'two'])
    expect(fs.statSync(file).size).toBe(size)
  })

  it('does not replay a well-formed record after an eight-byte zero hole', () => {
    const prefix = tmpFile()
    const first = WriteAheadLog.open(prefix)
    first.append(enc('one'))
    first.close()
    const prefixSize = fs.statSync(prefix).size

    const full = tmpFile()
    const second = WriteAheadLog.open(full)
    second.append(enc('one'))
    second.append(enc('sneak'))
    second.close()
    const sneak = fs.readFileSync(full).subarray(prefixSize)

    const file = tmpFile()
    fs.copyFileSync(prefix, file)
    fs.appendFileSync(file, Buffer.alloc(8))
    fs.appendFileSync(file, sneak)
    const reopened = WriteAheadLog.open(file)
    cleanups.push(() => reopened.close())
    expect(payloads(reopened)).toEqual(['one'])
    expect(fs.statSync(file).size).toBe(prefixSize)
  })

  it('keeps a record appended after torn-tail recovery across reopen', () => {
    const file = tmpFile()
    const log = WriteAheadLog.open(file)
    log.append(enc('one'))
    log.append(enc('two'))
    log.close()
    fs.appendFileSync(file, Buffer.from([8, 0, 0, 0, 1, 2, 3]))
    const recovered = WriteAheadLog.open(file)
    recovered.append(enc('three'))
    recovered.close()
    const reopened = WriteAheadLog.open(file)
    cleanups.push(() => reopened.close())
    expect(payloads(reopened)).toEqual(['one', 'two', 'three'])
  })
})

describe('DurableWorkQueue crash recovery', () => {
  it('redelivers unacked work with stable ids', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<string>(file)
    expect(q.enqueue('a')).toBe(1)
    expect(q.enqueue('b')).toBe(2)
    const held: string[] = []
    q.consume((d) => held.push(d.message.payload))
    expect(held).toEqual(['a'])
    q.close()
    const recovered = DurableWorkQueue.open<string>(file)
    cleanups.push(() => recovered.close())
    const seen: Array<{ id: number; payload: string }> = []
    recovered.consume((d) => {
      seen.push({ id: d.message.id, payload: d.message.payload })
      d.ack()
    })
    expect(seen).toEqual([
      { id: 1, payload: 'a' },
      { id: 2, payload: 'b' },
    ])
    expect(recovered.enqueue('c')).toBe(3)
  })

  it('does not resurrect acked, dropped, retried-then-acked, or max-delivery work', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<string>(file, { maxDeliveryCount: 2 })
    let threw = false
    q.consume((d) => {
      if (d.message.payload === 'drop-me') d.nack({ requeue: false })
      else if (d.message.payload === 'poison') d.nack()
      else if (d.message.payload === 'boom' && !threw) {
        threw = true
        throw new Error('boom')
      } else d.ack()
    })
    q.enqueue('done')
    q.enqueue('drop-me')
    q.enqueue('poison')
    q.enqueue('boom')
    q.close()
    const recovered = DurableWorkQueue.open<string>(file)
    cleanups.push(() => recovered.close())
    expect(drain(recovered)).toEqual([])
    expect(() => DurableWorkQueue.memory().enqueue(undefined)).toThrow(/JSON-serializable/)
    expect(() => DurableWorkQueue.memory({ maxDeliveryCount: 0 })).toThrow(/maxDeliveryCount/)
  })

  it('checkpoint rewrites to live enqueue records only', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<{ job: string }>(file)
    q.enqueue({ job: 'a' })
    q.enqueue({ job: 'b' })
    q.consume((d) => {
      if (d.message.payload.job === 'a') d.ack()
    })
    q.checkpoint()
    q.close()
    const recovered = DurableWorkQueue.open<{ job: string }>(file)
    cleanups.push(() => recovered.close())
    expect(drain(recovered).map((m) => m.job)).toEqual(['b'])
  })

  it('redelivers a prefix enqueue when the log tail is eight zeros', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<string>(file)
    q.enqueue('keep-me')
    q.close()
    fs.appendFileSync(file, Buffer.alloc(8))
    const recovered = DurableWorkQueue.open<string>(file)
    cleanups.push(() => recovered.close())
    expect(drain(recovered)).toEqual(['keep-me'])
  })

  it('stale ack after unsubscribe does not drop requeued work from the log', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<string>(file)
    let stolen: Delivery<string> | undefined
    const off = q.consume((d) => {
      stolen = d
    })
    q.enqueue('held')
    off()
    expect(q.readyCount()).toBe(1)
    stolen!.ack()
    expect(q.readyCount()).toBe(1)
    q.close()
    const recovered = DurableWorkQueue.open<string>(file)
    cleanups.push(() => recovered.close())
    expect(drain(recovered)).toEqual(['held'])
  })

  it('stale drop-nack after unsubscribe does not drop requeued work from the log', () => {
    const file = tmpFile()
    const q = DurableWorkQueue.open<string>(file)
    let stolen: Delivery<string> | undefined
    const off = q.consume((d) => {
      stolen = d
    })
    q.enqueue('held')
    off()
    stolen!.nack({ requeue: false })
    expect(q.readyCount()).toBe(1)
    q.close()
    const recovered = DurableWorkQueue.open<string>(file)
    cleanups.push(() => recovered.close())
    expect(drain(recovered)).toEqual(['held'])
  })
})
