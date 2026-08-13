# event-broker-lab

A from-scratch in-memory message broker exploring pub/sub, work queues, delivery guarantees, dead-letter handling, and backpressure: the concepts behind Kafka, RabbitMQ, and SQS, built small enough to read.

## What this demonstrates

Message brokers hide a lot of machinery behind `publish` and `subscribe`. This repo pulls that machinery apart one piece at a time and keeps each piece small enough to read in a sitting. The topics it works through include topic-based fan-out, subscriber lifecycle and snapshot-consistent delivery, wildcard routing, dead-letter handling for poison messages, and later work queues, acknowledgements, and backpressure. Everything runs in a single process with no dependencies, so the focus stays on the semantics rather than the transport.

## Concepts demonstrated

- **Topic-based publish/subscribe** with fan-out delivery
- **Snapshot-consistent dispatch** (subscribe/unsubscribe mid-delivery does not change the in-flight recipient set)
- **AMQP-style topic patterns** (`*` one segment, `#` zero or more) with a small dynamic program for matching
- **Poison-message isolation** so one throwing subscriber does not abort the rest of a fan-out
- **Redrive policy** (`maxAttempts`) that retries a failed handler immediately on the same publish, then gives up
- **Dead-letter queue** that parks the original payload with source topic, attempt count, and last error
- **Bounded failure queue** (explicit `capacity`) that refuses new entries instead of dropping evidence
- **Redrive / replay** back onto the original topic, including a publish-then-remove so a failed replay leaves the envelope in place

## What's implemented

- **Topic-based publish/subscribe with fan-out.** A `Broker` where subscribers register handlers against a topic and every publish fans out to all matching subscribers, with monotonic message ids, idempotent unsubscribe, and snapshot-consistent delivery (subscribing or unsubscribing during dispatch never changes who receives the in-flight message).
- **Wildcard topic subscriptions.** AMQP-style pattern bindings where `*` matches exactly one segment and `#` matches zero or more, so one handler can receive a whole family of topics (`orders.#`, `*.created.us`). Matching uses a `#`-aware dynamic program, and a published message fans out to exact subscribers first, then every matching pattern.
- **Dead-letter queue for poison messages.** A `DeadLetterQueue` plus `withDeadLetter` wrapper. Each failed delivery increments a per-subscription attempt ledger (two wrappers on one queue do not share retries or hide each other's last error). After `maxAttempts` the payload is parked with its source topic, attempt count, and last error. `Broker.publish` finishes the subscriber snapshot even if a handler throws, then rethrows the first error. Operators can inspect, `drop`, `purge`, or `redrive` back onto the original topic. A full DLQ throws `DeadLetterFullError` rather than silently discarding the poison payload.

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

Park a poison payload after it exhausts retries, then redrive it:

```ts
import { Broker, DeadLetterQueue, withDeadLetter } from 'event-broker-lab'

const broker = new Broker<string>()
const dlq = new DeadLetterQueue<string>({ maxAttempts: 3 })

const off = broker.subscribe(
  'orders',
  withDeadLetter(dlq, (msg) => {
    if (msg.payload === 'poison') throw new Error('bad payload')
    console.log(msg.payload)
  }),
)

broker.publish('orders', 'poison')
console.log(dlq.size()) // 1
console.log(dlq.peek()[0]?.error) // bad payload

off() // unsubscribe first, otherwise redrive hits the same handler and re-parks
dlq.redriveAll((topic, payload) => {
  broker.publish(topic, payload)
})
```

## Running the tests

```sh
pnpm install
pnpm test
```

Type-check with `pnpm run typecheck`.
