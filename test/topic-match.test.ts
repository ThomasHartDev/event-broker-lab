import { describe, it, expect } from 'vitest'
import { matchTopic, isValidTopic, isValidPattern } from '../src/index.js'

describe('matchTopic', () => {
  it('matches identical literal topics', () => {
    expect(matchTopic('orders.created', 'orders.created')).toBe(true)
    expect(matchTopic('orders.created', 'orders.shipped')).toBe(false)
  })

  it('treats * as exactly one segment', () => {
    expect(matchTopic('orders.*', 'orders.created')).toBe(true)
    expect(matchTopic('orders.*', 'orders.created.us')).toBe(false)
    expect(matchTopic('orders.*', 'orders')).toBe(false)
    expect(matchTopic('*.created.*', 'orders.created.us')).toBe(true)
    expect(matchTopic('*.created.*', 'orders.created')).toBe(false)
  })

  it('treats # as zero or more trailing segments', () => {
    expect(matchTopic('orders.#', 'orders')).toBe(true)
    expect(matchTopic('orders.#', 'orders.created')).toBe(true)
    expect(matchTopic('orders.#', 'orders.created.us.west')).toBe(true)
    expect(matchTopic('orders.#', 'shipments.created')).toBe(false)
  })

  it('handles # in the middle and matches a variable span', () => {
    expect(matchTopic('orders.#.us', 'orders.us')).toBe(true)
    expect(matchTopic('orders.#.us', 'orders.created.us')).toBe(true)
    expect(matchTopic('orders.#.us', 'orders.created.eu.us')).toBe(true)
    expect(matchTopic('orders.#.us', 'orders.created.eu')).toBe(false)
  })

  it('matches everything with a lone #', () => {
    expect(matchTopic('#', 'a')).toBe(true)
    expect(matchTopic('#', 'a.b.c.d.e')).toBe(true)
  })

  it('combines * and # together', () => {
    expect(matchTopic('*.#', 'orders')).toBe(true)
    expect(matchTopic('*.#', 'orders.created.us')).toBe(true)
    expect(matchTopic('#.us', 'a.b.us')).toBe(true)
    expect(matchTopic('#.us', 'us')).toBe(true)
  })

  it('does not match when literal segments differ under wildcards', () => {
    expect(matchTopic('orders.*.us', 'orders.created.eu')).toBe(false)
  })

  it('never throws and returns false on invalid input', () => {
    expect(matchTopic('', 'orders')).toBe(false)
    expect(matchTopic('orders.*', '')).toBe(false)
    expect(matchTopic('orders.*', 'orders.*')).toBe(false) // topic can't contain wildcards
    expect(matchTopic('orders..created', 'orders.created')).toBe(false)
  })
})

describe('validators', () => {
  it('isValidTopic rejects wildcards, empties, and empty segments', () => {
    expect(isValidTopic('orders.created')).toBe(true)
    expect(isValidTopic('orders')).toBe(true)
    expect(isValidTopic('')).toBe(false)
    expect(isValidTopic('orders.*')).toBe(false)
    expect(isValidTopic('orders.#')).toBe(false)
    expect(isValidTopic('orders..created')).toBe(false)
    expect(isValidTopic('.orders')).toBe(false)
  })

  it('isValidPattern accepts wildcards but rejects empties', () => {
    expect(isValidPattern('orders.*')).toBe(true)
    expect(isValidPattern('orders.#')).toBe(true)
    expect(isValidPattern('#')).toBe(true)
    expect(isValidPattern('*.created.#')).toBe(true)
    expect(isValidPattern('')).toBe(false)
    expect(isValidPattern('orders..*')).toBe(false)
  })
})
