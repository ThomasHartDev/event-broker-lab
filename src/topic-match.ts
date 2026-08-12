const SEGMENT = /^[^.#*]+$/

export function isValidTopic(topic: string): boolean {
  if (topic.length === 0) return false
  return topic.split('.').every((seg) => SEGMENT.test(seg))
}

export function isValidPattern(pattern: string): boolean {
  if (pattern.length === 0) return false
  return pattern.split('.').every((seg) => seg === '*' || seg === '#' || SEGMENT.test(seg))
}

export function matchTopic(pattern: string, topic: string): boolean {
  if (!isValidPattern(pattern) || !isValidTopic(topic)) return false
  return matchSegments(pattern.split('.'), topic.split('.'))
}

function matchSegments(pat: string[], topic: string[]): boolean {

  const n = topic.length
  // pattern exhausted matches only if topic is exhausted too
  let dp: boolean[] = new Array(n + 1).fill(false)
  dp[n] = true

  for (let pi = pat.length - 1; pi >= 0; pi--) {
    const seg = pat[pi]
    const next: boolean[] = new Array(n + 1).fill(false)
    for (let t = n; t >= 0; t--) {
      if (seg === '#') {

        next[t] = dp[t] === true || (t < n && next[t + 1] === true)
      } else if (t < n && (seg === '*' || seg === topic[t])) {

        next[t] = dp[t + 1] === true
      }
    }
    dp = next
  }

  return dp[0] === true
}
