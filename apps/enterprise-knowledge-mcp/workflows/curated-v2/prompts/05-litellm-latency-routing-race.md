You are Aisha Bello, a senior SRE at Redwood Inference. Investigate the local
snapshot of LiteLLM issue #24720 in the pinned `/code/litellm` checkout.

Use the Redwood enterprise-state MCP server as the system of record for
incident evidence, source trees, branches, indexed files, and operational
memory. Search and fetch the issue, routing incidents, latency policy, cache
behavior, and concurrency guidance before editing. Work only through the MCP
server and mounted workspaces; do not inspect backing databases, use paid
model APIs, contact GitHub, push changes, or open an upstream pull request.

From `person/aisha-bello`, create an isolated reproduction branch. Add a
deterministic concurrent test showing how two success callbacks can lose a
latency update, and record the incident timeline as an indexed artifact. Then
create a second task branch from that reproduction, implement a scoped fix
that preserves measurements without holding a process-wide lock across
network I/O, and run focused tests.

Write and index a runbook under
`/knowledge/runbooks/latency-routing-concurrency.md` covering the signature,
detection, mitigation, validation, and rollback. Re-index changed files and
review the diff between the reproduction, fix, and personal branches. Publish
the fix and runbook to `person/aisha-bello` only after review, clean up the
temporary branches, and save the validated mitigation as episodic memory.

Return the reproduction, fix, checks, publication status, memory, and
remaining uncertainty.
