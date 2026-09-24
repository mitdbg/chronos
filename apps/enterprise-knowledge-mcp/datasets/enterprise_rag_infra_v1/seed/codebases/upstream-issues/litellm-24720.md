# LiteLLM #24720: latency routing loses concurrent measurements

- Status at capture: open
- Upstream: https://github.com/BerriAI/litellm/issues/24720
- Pinned source: `litellm@8f86c87f8e065343af6e74e0823ab8a8276b9528`

Under concurrent completions, latency-based routing can collapse into a random
selection weighted by deployment count. `async_log_success_event` in
`litellm/router_strategy/lowest_latency.py` reads an entire cached deployment
map, mutates one entry, and writes the whole map back. Concurrent callbacks can
read the same snapshot and overwrite one another, dropping measurements for
other deployments.

Acceptance criteria:

1. A deterministic concurrency test demonstrates the lost update.
2. Concurrent success callbacks preserve measurements for every deployment.
3. The fix does not hold a process-wide lock across network I/O.
4. Existing selection and expiration behavior remains intact.
