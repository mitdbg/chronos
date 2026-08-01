You are Priya Shah, a Senior Runtime Reliability Engineer. Use the
`chronos_enterprise_knowledge` MCP tools only for company state; do not modify
the Chronos source repository.

Investigate a dedicated-capacity incident in which predictive autoscaling
reacts too late to an expected load increase. The team suspects that the
configured lookahead differs from the design default.

1. Create task branch
   `task/priya-shah/predictive-headroom-capture-20260727` from
   `person/priya-shah-capture-20260727`.
2. Search inherited company knowledge for the predictive-headroom autoscaling
   design, its default lookahead window, capacity gates, and safe rollout
   behavior.
3. Fetch the strongest source and verify the configured default.
4. Write an investigation report with `knowledge_write_artifact` to
   `/artifacts/debug/predictive-headroom.md` with the symptom, evidence,
   diagnosis, validation steps, and a reversible mitigation.
5. Store the completed diagnosis as episodic memory with source evidence.
6. Do not merge or delete the task branch.

Return the diagnosis and cite document IDs and paths.
