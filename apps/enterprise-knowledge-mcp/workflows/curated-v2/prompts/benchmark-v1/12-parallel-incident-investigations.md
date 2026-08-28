You are Aisha Bello, a senior SRE at Redwood Inference. Use the
`chronos_enterprise_knowledge` MCP tools and mounted branch workspaces. Do not
modify the Chronos source repository. Do not inspect the MCP server's backing
databases directly; access company state through MCP tools and mounted branch
workspaces only.

Triage three independent production reports in parallel: a cross-language
streaming startup hang, predictive autoscaling that reacts too late to a known
load increase, and incorrect throttling during overlapping shard reassignment
and policy reload.

1. From `person/aisha-bello`, create and mount three sibling branches:
   `task/aisha-bello/streaming-handshake-v2`,
   `task/aisha-bello/predictive-headroom-v2`, and
   `task/aisha-bello/throttling-race-v2`.
2. On each branch, independently search and fetch authoritative company
   evidence for that report. Write and index one report under
   `/artifacts/incidents/` with the symptom, exact evidence, diagnosis,
   reproduction or validation, reversible mitigation, and uncertainty. Store
   the supported diagnosis as episodic memory on that branch.
3. Diff all three branches against `person/aisha-bello`. Verify that their
   artifacts are disjoint, then merge all three reviewed evidence packages
   into the personal branch.
4. From the updated personal branch, create and mount
   `task/aisha-bello/incident-consolidation-v2`. Write and index
   `/knowledge/runbooks/runtime-incident-triage.md`, which distinguishes the
   three signatures and gives a decision tree for choosing the correct
   validation path. Cite all three reports and their underlying sources.
5. Store the reviewed decision tree as playbook memory on the consolidation
   branch. Merge it into `person/aisha-bello`, verify the final diff, and
   delete the consolidation branch and all three sibling task branches.

Do not claim the incidents share one root cause. Report branch create, diff,
merge, and delete results; documents; memory IDs; and evidence.
