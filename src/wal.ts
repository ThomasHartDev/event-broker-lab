import fs from 'node:fs'

const MAGIC = Buffer.from('EBL1')
const MAX_PAYLOAD = 16 * 1024 * 1024

const CRC_TABLE = new Uint32Array(256)
for (let n = 0; n < 256; n++) {
  let c = n
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
  CRC_TABLE[n] = c >>> 0
}

export function crc32(bytes: Uint8Array): number {
  let c = 0xffffffff
  for (const b of bytes) c = CRC_TABLE[(c ^ b) & 0xff]! ^ (c >>> 8)
  return (c ^ 0xffffffff) >>> 0
}

export interface WriteAheadLogOptions {
  readonly fsync?: boolean
  readonly sync?: () => void
}

function crc32LengthAndPayload(length: number, payload: Uint8Array): number {
  const body = Buffer.allocUnsafe(4 + payload.byteLength)
  body.writeUInt32LE(length, 0)
  Buffer.from(payload).copy(body, 4)
  return crc32(body)
}

function frame(payload: Uint8Array): Buffer {
  const out = Buffer.allocUnsafe(8 + payload.byteLength)
  out.writeUInt32LE(payload.byteLength, 0)
  out.writeUInt32LE(crc32LengthAndPayload(payload.byteLength, payload), 4)
  Buffer.from(payload).copy(out, 8)
  return out
}

function parse(buf: Buffer): { payloads: Buffer[]; consumed: number } {
  if (buf.length < 4 || !buf.subarray(0, 4).equals(MAGIC)) throw new Error('invalid wal magic')
  const payloads: Buffer[] = []
  let offset = 4
  while (offset + 8 <= buf.length) {
    const length = buf.readUInt32LE(offset)
    const expected = buf.readUInt32LE(offset + 4)
    const start = offset + 8
    const end = start + length
    if (length > MAX_PAYLOAD || end > buf.length) break
    const payload = buf.subarray(start, end)
    // First bad CRC or incomplete header is a torn tail. Nothing after it is trusted.
    if (crc32LengthAndPayload(length, payload) !== expected) break
    payloads.push(Buffer.from(payload))
    offset = end
  }
  return { payloads, consumed: offset }
}

function writeFully(fd: number, bytes: Buffer, position: number): number {
  let offset = 0
  while (offset < bytes.length) {
    const n = fs.writeSync(fd, bytes, offset, bytes.length - offset, position + offset)
    if (n <= 0) throw new Error('short write')
    offset += n
  }
  return offset
}

function readFully(fd: number, size: number): Buffer {
  if (size <= 0) return Buffer.alloc(0)
  const buf = Buffer.allocUnsafe(size)
  let offset = 0
  while (offset < size) {
    const n = fs.readSync(fd, buf, offset, size - offset, offset)
    if (n <= 0) return buf.subarray(0, offset)
    offset += n
  }
  return buf
}

export class WriteAheadLog {
  private payloads: Buffer[] = []
  private mem = Buffer.alloc(0)
  private fd: number | undefined
  private path = ''
  private size = 0
  private closed = false

  private constructor(
    private readonly doSync: boolean,
    private readonly injectedSync: (() => void) | undefined,
  ) {}

  static open(path: string, options?: WriteAheadLogOptions): WriteAheadLog {
    const fd = fs.existsSync(path) ? fs.openSync(path, 'r+') : fs.openSync(path, 'w+')
    const log = new WriteAheadLog(options?.fsync !== false, options?.sync)
    log.fd = fd
    log.path = path
    log.size = fs.fstatSync(fd).size
    return log.boot()
  }

  static memory(options?: WriteAheadLogOptions): WriteAheadLog {
    return new WriteAheadLog(options?.fsync !== false, options?.sync).boot()
  }

  append(payload: Uint8Array): number {
    this.assertOpen()
    if (payload.byteLength > MAX_PAYLOAD) throw new Error(`payload exceeds ${MAX_PAYLOAD} bytes`)
    this.write(frame(payload))
    this.flush()
    this.payloads.push(Buffer.from(payload))
    return this.payloads.length - 1
  }

  replay(): readonly Uint8Array[] {
    this.assertOpen()
    return this.payloads
  }

  rewrite(payloads: Uint8Array[]): void {
    this.assertOpen()
    const kept = payloads.map((p) => Buffer.from(p))
    this.replace(Buffer.concat([MAGIC, ...kept.map(frame)]))
    this.flush()
    this.payloads = kept
  }

  close(): void {
    if (this.closed) return
    this.closed = true
    if (this.fd !== undefined) fs.closeSync(this.fd)
  }

  private boot(): this {
    try {
      this.recover()
      return this
    } catch (error) {
      this.close()
      throw error
    }
  }

  private recover(): void {
    const buf = this.readAll()
    if (buf.length < 4) {
      this.replace(MAGIC)
      this.flush()
      return
    }
    const { payloads, consumed } = parse(buf)
    this.payloads = payloads
    if (consumed < buf.length) this.truncate(consumed)
  }

  private readAll(): Buffer {
    if (this.fd === undefined) return this.mem
    return readFully(this.fd, this.size)
  }

  private write(bytes: Buffer): void {
    if (this.fd === undefined) {
      this.mem = Buffer.concat([this.mem, bytes])
      return
    }
    let offset = 0
    while (offset < bytes.length) {
      const n = fs.writeSync(this.fd, bytes, offset, bytes.length - offset, this.size)
      if (n <= 0) throw new Error('short write')
      this.size += n
      offset += n
    }
  }

  private replace(bytes: Buffer): void {
    if (this.fd === undefined) {
      this.mem = Buffer.from(bytes)
      return
    }
    const tmp = `${this.path}.tmp`
    const tfd = fs.openSync(tmp, 'w')
    try {
      writeFully(tfd, bytes, 0)
      fs.fsyncSync(tfd)
    } finally {
      fs.closeSync(tfd)
    }
    fs.closeSync(this.fd)
    fs.renameSync(tmp, this.path)
    this.fd = fs.openSync(this.path, 'r+')
    this.size = bytes.length
  }

  private truncate(length: number): void {
    if (this.fd === undefined) this.mem = this.mem.subarray(0, length)
    else {
      fs.ftruncateSync(this.fd, length)
      this.size = length
    }
  }

  private flush(): void {
    if (!this.doSync) return
    if (this.injectedSync) this.injectedSync()
    else if (this.fd !== undefined) fs.fsyncSync(this.fd)
  }

  private assertOpen(): void {
    if (this.closed) throw new Error('write-ahead log is closed')
  }
}
