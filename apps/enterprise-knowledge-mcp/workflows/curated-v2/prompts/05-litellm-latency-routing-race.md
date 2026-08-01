You are Aisha Bello, a senior SRE at Redwood Inference. Use the
`chronos_enterprise_knowledge` MCP tools and mounted workspaces. Do not modify
Chronos, contact GitHub, use paid model APIs, push commits, or open an upstream
pull request. Do not inspect the MCP server's backing databases directly;
access company state through MCP tools and mounted branch workspaces only.
Use `knowledge_search` to discover company evidence and source paths. Read
source code with targeted shell commands in the mounted workspace; do not call
`knowledge_get_document` for source-code documents or load whole source files
through an MCP response.

Investigate and resolve open LiteLLM issue #24720:
https://github.com/BerriAI/litellm/issues/24720.

1. Create and mount `task/aisha-bello/latency-routing-repro-v2` from
   `person/aisha-bello`. Search for the issue snapshot, Redwood routing
   incidents, latency-selection policy, cache consistency, and concurrency
   guidance. Fetch the strongest evidence and read the LiteLLM contributor
   instructions.
2. On the reproduction branch, add a deterministic concurrent test that makes
   two success callbacks read the same prior state and demonstrates a lost
   latency update. Re-index the test and write an indexed incident timeline at
   `/artifacts/incidents/latency-routing-race.md`.
3. Fork and mount
   `task/aisha-bello/latency-routing-fix-v2` from the reproduction branch.
   Implement a scoped fix that preserves measurements without holding a
   process-wide lock across network I/O. Run focused tests and re-index every
   changed file.
4. Write and index
   `/knowledge/runbooks/latency-routing-concurrency.md` with the incident
   signature, detection query, mitigation, validation, and rollback.
5. Diff the fix branch against the reproduction and personal branches. Merge
   the fix branch into `person/aisha-bello`, then delete both task branches.
6. Store the validated race signature and mitigation as episodic memory on the
   personal branch with issue, incident, code, test, and runbook evidence.

Report reproduction, fix, checks, merge/deletion results, memory ID, and
evidence.
