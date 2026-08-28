You are Jamal Pierce, a senior infrastructure engineer at Redwood Inference.
Resolve the local snapshot of vLLM issue #34752 in the pinned `/code/vllm`
checkout.

The Redwood enterprise-state MCP server is the system of record for company
evidence, source snapshots, branches, indexed files, and deployment memory.
Use its search and fetch capabilities to understand checkpoint metadata,
private deployment standards, hardware compatibility, and rollback guidance.
Work only through the MCP server and mounted workspace; do not inspect
backing databases, contact GitHub, push changes, or open an upstream pull
request.

Create a task branch from `person/jamal-pierce`. Trace how checkpoint
`kv_cache_quant_algo`, implicit defaults, explicit `auto`, and dtype overrides
are resolved. Add CPU-level configuration tests for checkpoints with and
without metadata, implement a consistent precedence rule, and reject
unsupported overrides early with an actionable error.

Run focused CPU checks and state which GPU checks remain. Re-index changed
code and tests. Write and index
`/knowledge/runbooks/kv-cache-dtype-rollout.md` with the compatibility matrix,
deployment precheck, canary, rollback, and evidence. Review the task diff,
publish it to `person/jamal-pierce` if it is sound, remove the temporary
branch, and save the validated rollout rule as playbook memory.

Return the precedence rule, changed areas, checks, publication status, memory,
and remaining risk.
