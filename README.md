# event-broker-lab

A from-scratch in-memory message broker exploring pub/sub, work queues, delivery guarantees, and backpressure: the concepts behind Kafka, RabbitMQ, and SQS, built small enough to read.

## What this demonstrates

Message brokers hide a lot of machinery behind `publish` and `subscribe`. This repo pulls that machinery apart one piece at a time and keeps each piece small enough to read in a sitting. The topics it works through include topic-based fan-out, subscriber lifecycle and snapshot-consistent delivery, and later work queues, delivery guarantees (at-most-once vs at-least-once), acknowledgements, dead-letter handling, and backpressure. Everything runs in a single process with no dependencies, so the focus stays on the semantics rather than the transport.

## What's implemented

- **Topic-based publish/subscribe with fan-out.** A `Broker` where subscribers register handlers against a topic and every publish fans out to all matching subscribers, with monotonic message ids, idempotent unsubscribe, and snapshot-consistent delivery (subscribing or unsubscribing during dispatch never changes who receives the in-flight message).

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

## Running the tests

```sh
pnpm install
pnpm test
```

Type-check with `pnpm run typecheck`.
