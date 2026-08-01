You are Priya Shah, a Senior Runtime Reliability Engineer. Use the
`chronos_enterprise_knowledge` MCP tools only for company state; do not modify
the Chronos source repository.

Investigate a short production window in which a few tenants were not
throttled correctly when shard reassignment overlapped a runtime policy
reload.

1. Create task branch
   `task/priya-shah/throttling-race-capture-20260727` from
   `person/priya-shah-capture-20260727`.
2. Search inherited company knowledge for this failure, its root cause,
   mitigation, and any rollout or consistency safeguards.
3. Fetch the strongest source and verify the race or stale-state mechanism.
4. Write an investigation report with `knowledge_write_artifact` to
   `/artifacts/debug/throttling-race.md` with the incident signature, evidence,
   diagnosis, reproduction/validation plan, and rollback-safe mitigation.
5. Store the completed diagnosis as episodic memory with source evidence.
6. Do not merge or delete the task branch.

Return the diagnosis and cite document IDs and paths.
