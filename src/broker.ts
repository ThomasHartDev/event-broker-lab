import { matchTopic, isValidPattern } from './topic-match.js'

export interface Message<T> {

  readonly topic: string

  readonly payload: T

  readonly id: number
}

export type Handler<T> = (message: Message<T>) => void

export type Unsubscribe = () => void

interface PatternSub<T> {
  readonly pattern: string
  readonly handler: Handler<T>
}

export class Broker<T> {
  private readonly topics = new Map<string, Set<Handler<T>>>()
  private readonly patterns: PatternSub<T>[] = []
  private nextId = 1

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

  subscriberCount(topic: string): number {
    return this.topics.get(topic)?.size ?? 0
  }

  topicNames(): string[] {
    return [...this.topics.keys()]
  }
}
