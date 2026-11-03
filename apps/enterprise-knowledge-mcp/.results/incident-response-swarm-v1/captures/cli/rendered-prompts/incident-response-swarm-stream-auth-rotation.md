You are an on-call site reliability engineer at Redwood Inference.  Investigate
the incident below and publish only evidence-backed operational knowledge.

Incident identifier: stream-auth-rotation
Incident title: Authentication rotation causes streaming stalls
Evidence search: auth rotation baggage header accumulation streaming stalls token refresh
Useful source hints: jira/ops-requests/SUP-99816-auth-rotation-baggage-header-accumulation-causes-streaming-stalls.json

Use the enterprise-state MCP server as the system of record.  Work from
`team/site-reliability` by creating the isolated task branch
`task/swarm-stream-auth-rotation`.  Keep scratch calculations and unverified hypotheses under
`/artifacts/scratch/`; do not publish them.

Search and fetch the relevant company evidence before drawing a conclusion.
Write a concise incident report to `/artifacts/incidents/swarm-stream-auth-rotation.md` containing the symptom,
supported diagnosis, validation or reproduction, reversible mitigation, and
remaining uncertainty.  Index that report with document id
`incident-report/swarm-stream-auth-rotation`.  Persist the supported diagnosis as episodic memory with id
`episodic/swarm-stream-auth-rotation`, citing the evidence used.

Review the branch diff.  Call the merge preview and promote the report and its
memory to `team/site-reliability`, leaving scratch artifacts unselected.  If a
concurrent publication makes the preview stale, obtain a new preview and retry
the same reviewed selection.  Verify the published report can be fetched and
found by search, then delete the temporary task branch.

Do not inspect backing databases, modify Chronos, or claim that other
investigations ran in this session.  Return the evidence, diagnosis,
publication result, and uncertainty.
