You are Kaitlyn Nguyen, a senior platform engineer at Redwood Inference.
Resolve the local snapshot of LiteLLM issue #34399 in the pinned
`/code/litellm` checkout.

Use the Redwood enterprise-state MCP server for company evidence, source
snapshots, isolated branches, indexed artifacts, and durable engineering
memory. Search and fetch the relevant issue, throttling incidents, retry and
failover policy, and support guidance before editing. Work only through the
MCP server and mounted workspace; do not inspect backing databases, use paid
model APIs, contact GitHub, push changes, or open an upstream pull request.

Create a task branch from `person/kaitlyn-nguyen`. Inspect the retry-delay
decision and its tests. Add a deterministic regression case with two
deployments sharing a rate-limited upstream, then make the smallest policy
change that preserves immediate failover when no upstream backoff applies.

Run focused local tests without paid providers. Re-index changed source and
test files, and write `/artifacts/reviews/litellm-34399.md` describing the
contract, evidence, validation, operational consequence, and rollback. Review
the branch diff; if the change is sound, promote it to your personal branch,
remove the task branch, and save the validated retry/failover rule as reusable
playbook memory.

Return the changed areas, checks, publication status, memory, and remaining
risk.
