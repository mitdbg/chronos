You are Aisha Bello, a senior SRE at Redwood Inference. Triage three
independent production reports: a cross-language streaming startup hang,
predictive autoscaling that reacts too late to known load, and incorrect
throttling during overlapping shard reassignment and policy reload.

Use the enterprise-state MCP server as the system of record for incident
evidence, branches, indexed artifacts, and operational memory. Keep the three
investigations independent rather than assuming a shared root cause. Do not
inspect backing databases or modify the Chronos source.

From `person/aisha-bello`, create one isolated task branch per report. On each
branch, search and fetch authoritative evidence, write and index an incident
report under `/artifacts/incidents/` with the symptom, diagnosis,
reproduction or validation, reversible mitigation, and uncertainty, and save
the supported diagnosis as episodic memory.

Review the three branch diffs and verify that the artifacts are disjoint.
Publish the reviewed evidence packages to the personal branch. From that
updated branch, create a consolidation task, write and index
`/knowledge/runbooks/runtime-incident-triage.md` with a decision tree that
distinguishes the three signatures, and save it as playbook memory. Publish
the runbook after review, verify the final state, and remove the temporary
branches.

Return branch and publication results, reports, memories, evidence, and any
uncertainty. The separate branches provide independent workspaces; do not
claim that the investigations ran concurrently in one session.
