# event-broker-lab

A from-scratch in-memory message broker exploring pub/sub, work queues, delivery guarantees, and backpressure: the concepts behind Kafka, RabbitMQ, and SQS, built small enough to read.

## What this demonstrates

Message brokers hide a lot of machinery behind `publish` and `subscribe`. This repo pulls that machinery apart one piece at a time and keeps each piece small enough to read in a sitting. The topics it works through include topic-based fan-out, wildcard routing, competing-consumer work queues with acknowledgements, at-least-once redelivery on nack, dead-letter handling, and later ordering, durability, and backpressure. Everything runs in a single process with no dependencies, so the focus stays on the semantics rather than the transport.

## Concepts demonstrated

- **Topic-based publish/subscribe** with fan-out delivery
- **Snapshot-consistent dispatch** (subscribe/unsubscribe mid-delivery does not change the in-flight recipient set)
- **AMQP-style topic patterns** (`*` one segment, `#` zero or more) with a small dynamic program for matching
- **Competing consumers** (work queue): each message is delivered to exactly one consumer
- **Acknowledgements** (`ack` / `nack`) and **at-least-once** redelivery on nack or consumer cancel
- **Per-subscriber inbox** on reliable fan-out: each subscriber gets its own copy, settles independently, and a nack redelivers only that copy
- **Redelivered flag** and **delivery count** so handlers can tell first delivery from a retry
- **Prefetch** as a per-consumer in-flight limit for fair sharing
- **Redelivery counting**, **bounded redelivery** (`maxDeliveryCount`, default 10; excess drops), and **tail requeue** on nack so other ready work is not starved
- **Dead-letter queue** for poison messages after a bounded number of failed attempts

- **Bounded buffers** with a finite ready-queue `capacity` and **reject-on-full** (`QueueFullError` / `tryEnqueue`)
- **High/low watermark backpressure** (hysteresis / Schmitt trigger) so producers pause before the wall and resume after the queue drains, without flapping in the band
- **Producer-facing occupancy**: capacity is ready depth. Prefetch already caps in-flight. Redelivery of accepted work is allowed to sit above capacity so a nack cannot drop a message the queue already took.
- **Per-key FIFO ordering**: at most one in-flight delivery per key (SQS FIFO message group / Kafka key), a partial order across keys, and head-of-line blocking only within a key

- **Write-ahead logging**: append-only log, **length-prefixed framing**, **CRC32 checksums**, and **fsync** before an append returns
- **Torn-write recovery**: a truncated or checksum-mismatched tail is dropped, not replayed
- **Crash recovery by replay**: enqueue / ack / drop records rebuild the ready set
- **Log truncation (checkpoint)**: rewrite the file to live enqueue records so the log cannot grow without bound
- **Consumer lag** as `logEndOffset - committedOffset`, tracked per topic and per consumer
- **Time lag** as the age of the oldest unconsumed produce
- **Sliding-window throughput** (circular time buckets) for produce and consume rates, so a quiet period after a burst reports ~0

- **Partitioned log** with **key-based partitioning** (FNV-1a) so a key is sticky to one partition
- **Consumer groups** with **range** and **round-robin partition assignment**, **eager rebalance** on join/leave, and **group-level committed offsets**
- **Per-key ordering**: records for one key append in order on one partition, and only the assigned member reads them
- **Exponential backoff** on retry: `min(cap, base * 2^(attempt-1))`
- **Jitter** (full, equal, decorrelated) so concurrent retries do not align
- **Delayed retry queue** with an injectable clock, so tests can advance time without sleeping

## What's implemented

- **Topic-based publish/subscribe with fan-out.** A `Broker` where subscribers register handlers against a topic and every publish fans out to all matching subscribers, with monotonic message ids, idempotent unsubscribe, and snapshot-consistent delivery (subscribing or unsubscribing during dispatch never changes who receives the in-flight message).
- **Wildcard topic subscriptions.** AMQP-style pattern bindings where `*` matches exactly one segment and `#` matches zero or more, so one handler can receive a whole family of topics (`orders.#`, `*.created.us`). Matching uses a `#`-aware dynamic program, and a published message fans out to exact subscribers first, then every matching pattern.
- **Competing-consumer work queue with acks.** A `WorkQueue` where each enqueued message goes to exactly one consumer. Consumers `ack` to settle or `nack` to requeue (or drop). Handler throw is treated as a requeueing nack. Prefetch defaults to 1 so peers share fairly; cancelling a consumer requeues its unacked deliveries at the head of ready. A nack requeues at the tail. Redelivery is bounded by `maxDeliveryCount` (default 10) so a permanent poison payload cannot busy-spin inside `enqueue`.
- **Dead-letter queue for poison messages.** A `DeadLetterQueue` that counts attempts per original message (and optional subscription), parks a failed payload once `maxAttempts` is exhausted, and supports peek, drop, purge, and redrive back onto a publisher.
- **At-least-once delivery with redelivery on nack.** A `ReliableBroker` where publish still fans out, but each subscriber owns a private inbox. `ack` settles that copy. `nack` (or a handler throw) redelivers it to the same subscriber with `deliveryCount` incremented and `redelivered` set. Peers who already acked are not retried. `nack({ requeue: false })` drops only that subscriber's copy. Retries stop at `maxDeliveryCount`. Optional per-subscriber prefetch isolates a slow consumer without stalling everyone else.

- **Bounded buffers** with a finite ready-queue `capacity` and **reject-on-full** (`QueueFullError` / `tryEnqueue`)
- **High/low watermark backpressure** (hysteresis / Schmitt trigger) so producers pause before the wall and resume after the queue drains, without flapping in the band
- **Producer-facing occupancy**: capacity is ready depth. Prefetch already caps in-flight. Redelivery of accepted work is allowed to sit above capacity so a nack cannot drop a message the queue already took.
- **Bounded queues with backpressure signaling.** Give `WorkQueue` a finite `capacity`. New publishes that would grow the ready backlog past that bound are rejected (`enqueue` throws `QueueFullError`, `tryEnqueue` returns `{ accepted: false }`). A `WatermarkGate` watches ready depth: occupancy at or above `highWatermark` emits `paused`, occupancy at or below `lowWatermark` emits `open`, and values in between hold the last state. Subscribe with `onBackpressure` or poll `backpressure()`. Defaults: high equals capacity, low is half the capacity (or `high - 1` when high is small). Unbounded queues stay the default so existing callers do not change.
- **Per-key FIFO ordering**: at most one in-flight delivery per key (SQS FIFO message group / Kafka key), a partial order across keys, and head-of-line blocking only within a key
- **Per-key ordering guarantees.** A `KeyedWorkQueue` where each message carries a group key. The head of a key is exclusive: the tail stays queued until that head is acked, nacked-and-dropped, or cancelled. Different keys can be in flight on competing consumers at the same time. A nack requeues at the head of that key so a later message of the same key cannot overtake. Prefetch can hold several keys, never two messages of one key.
- **Write-ahead logging**: append-only log, **length-prefixed framing**, **CRC32 checksums**, and **fsync** before an append returns
- **Torn-write recovery**: a truncated or checksum-mismatched tail is dropped, not replayed
- **Crash recovery by replay**: enqueue / ack / drop records rebuild the ready set
- **Log truncation (checkpoint)**: rewrite the file to live enqueue records so the log cannot grow without bound
- **Write-ahead log for crash durability.** A `WriteAheadLog` stores length-prefixed records (`u32le` length, `u32le` CRC32, payload) and fsyncs before `append` returns. On open it replays complete records and truncates a torn or corrupt tail. `DurableWorkQueue` logs enqueue, ack, and drop *before* the in-memory mutation, so a restart redelivers unacked work and does not resurrect acked work. `checkpoint()` rewrites the file to the live enqueue records (temp file, fsync, atomic rename).
- **Consumer lag** as `logEndOffset - committedOffset`, tracked per topic and per consumer
- **Time lag** as the age of the oldest unconsumed produce
- **Sliding-window throughput** (circular time buckets) for produce and consume rates, so a quiet period after a burst reports ~0
- **Throughput and lag metrics per topic.** `TopicMetrics` records `produced` / `consumed` per topic. Lag is `logEndOffset - committedOffset` (Kafka's formula), time lag is the age of the oldest unconsumed record, and produce/consume rates use a sliding window of circular time buckets. Two consumers on one topic keep independent commits and rates.
- **Partitioned log** with **key-based partitioning** (FNV-1a) so a key is sticky to one partition
- **Consumer groups** with **range** and **round-robin partition assignment**, **eager rebalance** on join/leave, and **group-level committed offsets**
- **Per-key ordering**: records for one key append in order on one partition, and only the assigned member reads them
- **Partitioned log** with **key-based partitioning** (FNV-1a) so a key is sticky to one partition
- **Consumer groups** with **range** and **round-robin partition assignment**, **eager rebalance** on join/leave, and **group-level committed offsets**
- **Per-key ordering**: records for one key append in order on one partition, and only the assigned member reads them
- **Consumer groups with partition assignment.** A `PartitionedTopic` is an append-only log split into N partitions. `produce(key, payload)` hashes the key with FNV-1a so that key always lands on the same partition. A `ConsumerGroup` assigns each partition to at most one member using Kafka's range assignor (consecutive slices, remainder on the first members) or round-robin (interleaved). Independent groups each see the full log. Offsets are stored on the group, so a rebalance hands a partition to a peer at the last committed offset instead of replaying from zero. A thrown handler stalls that partition until the next pump.
- **Exponential backoff** on retry: `min(cap, base * 2^(attempt-1))`
- **Jitter** (full, equal, decorrelated) so concurrent retries do not align
- **Delayed retry queue** with an injectable clock, so tests can advance time without sleeping
- **Exponential backoff with jitter on retry.** A nack or handler throw no longer redelivers in the same tick by default. The wait is exponential, then jittered (`full` by default, also `equal` and `decorrelated`). Ready work is not blocked behind a delayed retry. Pass `retryBackoff: false` for immediate requeue.
## Usage

```ts
import { Broker } from 'event-broker-lab'

const broker = new Broker<{ orderId: string }>()

const off = broker.subscribe('orders.created', (msg) => {
  console.log(`handler A saw order ${msg.payload.orderId} (id ${msg.id})`)
})
broker.subscribe('orders.created', (msg) => {
  console.log(`handler B saw order ${msg.payload.orderId}`)
})

const delivered = broker.publish('orders.created', { orderId: 'A-1' })
console.log(`${delivered} subscribers received it`) // 2

off() // handler A stops receiving
```

Bind a handler to a family of topics with a pattern:

```ts
// `#` matches zero or more trailing segments
broker.subscribePattern('orders.#', (msg) => {
  console.log(`audit log saw ${msg.topic}`)
})

broker.publish('orders.created', { orderId: 'A-1' })     // matches
broker.publish('orders.created.us', { orderId: 'A-2' })  // matches
broker.publish('shipments.created', { orderId: 'B-1' })  // does not match
```

Competing consumers on a work queue (one message, one worker):

```ts
import { WorkQueue } from 'event-broker-lab'

const queue = new WorkQueue<{ jobId: string }>()

queue.consume((delivery) => {
  const { jobId } = delivery.message.payload
  try {
    // do the work
    delivery.ack()
  } catch {
    delivery.nack() // requeue for another consumer
  }
})

queue.consume((delivery) => {
  // second worker competes for the same queue
  delivery.ack()
})

queue.enqueue({ jobId: 'job-1' }) // only one of the two consumers receives it
```

At-least-once fan-out: every subscriber gets a copy, and a nack retries only that copy.

```ts
import { ReliableBroker } from 'event-broker-lab'

const reliable = new ReliableBroker<{ orderId: string }>()

reliable.subscribe('orders.created', (delivery) => {
  if (delivery.message.redelivered) {
    // already tried once
  }
  try {
    // handle the order
    delivery.ack()
  } catch {
    delivery.nack()
  }
})

reliable.subscribe('orders.created', (delivery) => {
  delivery.ack() // this copy settles even if the other subscriber nacks
})

reliable.publish('orders.created', { orderId: 'A-1' })
```

## Running the tests

```sh
pnpm install
pnpm test
```

Type-check with `pnpm run typecheck`.
