You are Aisha Bello, a senior SRE at Redwood Inference. Use the
`chronos_enterprise_knowledge` MCP tools and mounted branch workspaces. Do not
modify the Chronos source repository. Do not inspect the MCP server's backing
databases directly; access company state through MCP tools and mounted branch
workspaces only.
Use `knowledge_write_artifact` for all file writes and MCP reads for final
verification; do not use shell commands or `apply_patch` in this workflow.

The release team has two plausible revisions for its private-deployment
rollback guidance. Explore both without exposing a hybrid policy, then publish
only the stronger reviewed candidate.

1. From `person/aisha-bello`, create and mount sibling branches
   `task/aisha-bello/rollback-fast-lane-v3` and
   `task/aisha-bello/rollback-evidence-gate-v3`.
2. Search company knowledge for staged rollouts, rollback triggers, compact
   network evidence, approval gates, deterministic snapshots, and release
   incidents. Fetch authoritative evidence from at least three source systems.
3. On the fast-lane branch, write and index
   `/knowledge/runbooks/private-deployment-rollback.md` with a minimal,
   reversible rollback path optimized for recovery time. Write unindexed
   calculations and unresolved assumptions under
   `/artifacts/experiments/rollback-fast-lane.md`.
4. Independently, on the evidence-gate branch write and index the same
   canonical runbook path with staged validation, evidence, approval, and
   reconciliation gates. Write unindexed calculations and unresolved
   assumptions under `/artifacts/experiments/rollback-evidence-gate.md`.
5. Diff both candidates against `person/aisha-bello`. Compare them against the
   fetched requirements and select exactly one; do not combine their contents
   after selection. Store a semantic memory on the selected branch explaining
   the accepted rollback contract and evidence.
6. Call `knowledge_merge_preview` for the selected branch. Atomically merge
   only the complete canonical-runbook and reviewed-memory bundles into
   `person/aisha-bello` using the preview token and explicit stable change IDs.
   Exclude both experiment paths and do not merge any state from the rejected
   candidate.
7. Verify the personal branch contains one coherent runbook and its memory,
   but neither experiment artifact. Delete both sibling task branches.

Report both candidates, selection rationale, evidence, selected change IDs,
excluded paths, merge and verification results, memory ID, and deletions.
