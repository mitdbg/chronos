# LiteLLM #34399: multi-deployment retries ignore Retry-After

- Status at capture: open
- Upstream: https://github.com/BerriAI/litellm/issues/34399
- Pinned source: `litellm@8f86c87f8e065343af6e74e0823ab8a8276b9528`

`Router._time_to_sleep_before_retry` honors an upstream `Retry-After` header
for a single deployment but returns zero immediately when another healthy
deployment exists. That assumes deployments have independent limits. In
practice, several deployments can share one upstream quota, so instant retries
hammer the throttled service and exhaust the retry budget before its backoff
window ends.

Acceptance criteria:

1. A focused test represents two deployments sharing a rate-limited upstream.
2. An explicit upstream backoff is not silently discarded merely because a
   sibling deployment is healthy.
3. Immediate failover remains possible when no upstream backoff applies.
4. The change states the precedence between provider backoff and router retry
   policy.
