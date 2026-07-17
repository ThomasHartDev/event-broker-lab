/**
 * A tiny in-memory topic broker with fan-out delivery.
 *
 * Every message is published to a topic string. Each subscriber registers a
 * handler against a topic and receives every message published to that exact
 * topic. One publish fans out to all matching subscribers, which is the same
 * model behind a Kafka topic or a RabbitMQ fanout exchange, minus the network.
 *
 * Subscribers can also register against a pattern (`orders.*`, `orders.#`) so
 * one handler receives a whole family of topics, the way a RabbitMQ topic
 * exchange routes on binding keys.
 */

import { matchTopic, isValidPattern } from './topic-match.js'

export interface Message<T> {
  /** The topic this message was published to. */
  readonly topic: string
  /** The payload the publisher sent. */
  readonly payload: T
  /** Monotonic id assigned by the broker at publish time, starting at 1. */
  readonly id: number
}

export type Handler<T> = (message: Message<T>) => void

/** Returned by subscribe. Call it once to stop receiving messages. */
export type Unsubscribe = () => void

interface PatternSub<T> {
  readonly pattern: string
  readonly handler: Handler<T>
}

export class Broker<T> {
  private readonly topics = new Map<string, Set<Handler<T>>>()
  private readonly patterns: PatternSub<T>[] = []
  private nextId = 1

  /**
   * Register a handler for an exact topic. Returns an unsubscribe function.
   * Subscribing the same handler twice to one topic is a no-op: the handler
   * set is deduplicated, so it still fires once per publish.
   */
  subscribe(topic: string, handler: Handler<T>): Unsubscribe {
    let handlers = this.topics.get(topic)
    if (!handlers) {
      handlers = new Set()
      this.topics.set(topic, handlers)
    }
    handlers.add(handler)

    let active = true
    return () => {
      if (!active) return
      active = false
      const current = this.topics.get(topic)
      if (!current) return
      current.delete(handler)
      if (current.size === 0) this.topics.delete(topic)
    }
  }

  /**
   * Register a handler against a pattern (`orders.*`, `orders.#`, `#`). The
   * handler receives every message whose topic matches the pattern. Returns an
   * unsubscribe function. Throws on an invalid pattern so binding mistakes fail
   * loudly instead of silently matching nothing.
   */
  subscribePattern(pattern: string, handler: Handler<T>): Unsubscribe {
    if (!isValidPattern(pattern)) {
      throw new Error(`invalid topic pattern: ${JSON.stringify(pattern)}`)
    }
    const sub: PatternSub<T> = { pattern, handler }
    this.patterns.push(sub)

    let active = true
    return () => {
      if (!active) return
      active = false
      const i = this.patterns.indexOf(sub)
      if (i !== -1) this.patterns.splice(i, 1)
    }
  }

  /**
   * Publish a payload to a topic. Every exact subscriber on that topic and
   * every pattern subscriber whose pattern matches is invoked synchronously,
   * exact subscribers first then patterns in registration order. Returns the
   * number of handlers that received the message.
   *
   * The recipient list is snapshotted before dispatch, so a handler that
   * subscribes or unsubscribes during delivery does not change who gets this
   * particular message.
   */
  publish(topic: string, payload: T): number {
    const exact = this.topics.get(topic)
    const snapshot: Handler<T>[] = exact ? [...exact] : []
    for (const sub of this.patterns) {
      if (matchTopic(sub.pattern, topic)) snapshot.push(sub.handler)
    }
    if (snapshot.length === 0) return 0

    const message: Message<T> = { topic, payload, id: this.nextId++ }
    for (const handler of snapshot) {
      handler(message)
    }
    return snapshot.length
  }

  /** Number of active subscribers on a topic. */
  subscriberCount(topic: string): number {
    return this.topics.get(topic)?.size ?? 0
  }

  /** Topics that currently have at least one subscriber. */
  topicNames(): string[] {
    return [...this.topics.keys()]
  }
}
