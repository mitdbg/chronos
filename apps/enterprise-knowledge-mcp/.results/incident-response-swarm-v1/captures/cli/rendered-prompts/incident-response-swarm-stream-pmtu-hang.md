You are an on-call site reliability engineer at Redwood Inference.  Investigate
the incident below and publish only evidence-backed operational knowledge.

Incident identifier: stream-pmtu-hang
Incident title: Large HTTP/2 streams hang after an egress PMTU response
Evidence search: large HTTP/2 shard streams hang egress provider PMTU fragmentation frag-needed inference streaming
Useful source hints: jira/ops-requests/SUP-912345-egress-provider-blocks-pmtu-frag-needed-causing-large-http2-shard-streams-to-hang.json

Use the enterprise-state MCP server as the system of record.  Work from
`team/site-reliability` by creating the isolated task branch
`task/swarm-stream-pmtu-hang`.  Keep scratch calculations and unverified hypotheses under
`/artifacts/scratch/`; do not publish them.

Search and fetch the relevant company evidence before drawing a conclusion.
Write a concise incident report to `/artifacts/incidents/swarm-stream-pmtu-hang.md` containing the symptom,
supported diagnosis, validation or reproduction, reversible mitigation, and
remaining uncertainty.  Index that report with document id
`incident-report/swarm-stream-pmtu-hang`.  Persist the supported diagnosis as episodic memory with id
`episodic/swarm-stream-pmtu-hang`, citing the evidence used.

Review the branch diff.  Call the merge preview and promote the report and its
memory to `team/site-reliability`, leaving scratch artifacts unselected.  If a
concurrent publication makes the preview stale, obtain a new preview and retry
the same reviewed selection.  Verify the published report can be fetched and
found by search, then delete the temporary task branch.

Do not inspect backing databases, modify Chronos, or claim that other
investigations ran in this session.  Return the evidence, diagnosis,
publication result, and uncertainty.
