/**
 * AMQP-style topic pattern matching over dot-separated topics.
 *
 * A topic is a non-empty string of segments joined by dots, e.g.
 * `orders.created.us`. A pattern is the same shape with two wildcards:
 *
 *   `*`  matches exactly one segment.
 *   `#`  matches zero or more segments.
 *
 * Examples:
 *   `orders.*`        matches `orders.created`      not `orders.created.us`
 *   `orders.#`        matches `orders`, `orders.created`, `orders.created.us`
 *   `*.created.*`     matches `orders.created.us`   not `orders.created`
 *   `#`               matches every topic
 *
 * The `#` case is what makes this more than a glob: it can absorb a variable
 * number of segments, so matching walks both strings with a small dynamic
 * program rather than a single left-to-right pass.
 */

const SEGMENT = /^[^.#*]+$/

/** True if a string is a valid topic (segments split by dots, no wildcards). */
export function isValidTopic(topic: string): boolean {
  if (topic.length === 0) return false
  return topic.split('.').every((seg) => SEGMENT.test(seg))
}

/** True if a string is a valid pattern (segments may be `*`, `#`, or a literal). */
export function isValidPattern(pattern: string): boolean {
  if (pattern.length === 0) return false
  return pattern.split('.').every((seg) => seg === '*' || seg === '#' || SEGMENT.test(seg))
}

/**
 * Match a topic against a pattern. Both are split into segments and compared
 * with a `#`-aware matcher. Invalid input never throws: a malformed topic or
 * pattern simply does not match.
 */
export function matchTopic(pattern: string, topic: string): boolean {
  if (!isValidPattern(pattern) || !isValidTopic(topic)) return false
  return matchSegments(pattern.split('.'), topic.split('.'))
}

function matchSegments(pat: string[], topic: string[]): boolean {
  // dp[t] = can pattern[pi..] match topic[t..]? Rebuilt right-to-left per row.
  const n = topic.length
  // Base row (pattern exhausted): matches only when topic is also exhausted.
  let dp: boolean[] = new Array(n + 1).fill(false)
  dp[n] = true

  for (let pi = pat.length - 1; pi >= 0; pi--) {
    const seg = pat[pi]
    const next: boolean[] = new Array(n + 1).fill(false)
    for (let t = n; t >= 0; t--) {
      if (seg === '#') {
        // Absorb zero segments (dp at same t) or one more (next at t+1).
        next[t] = dp[t] === true || (t < n && next[t + 1] === true)
      } else if (t < n && (seg === '*' || seg === topic[t])) {
        // Consume exactly one segment.
        next[t] = dp[t + 1] === true
      }
    }
    dp = next
  }

  return dp[0] === true
}
