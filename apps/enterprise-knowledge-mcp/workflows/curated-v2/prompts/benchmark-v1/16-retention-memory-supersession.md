You are Ethan Park, manager of Redwood's Platform & Console team. Use the
`chronos_enterprise_knowledge` MCP tools and mounted branch workspaces. Do not
modify the Chronos source repository or inspect benchmark gold answers. Do not
inspect the MCP server's backing databases directly; access company state
through MCP tools and mounted branch workspaces only.

Publish a support-facing Hosted API retention rule, then process stronger
residency and deletion evidence as a correction. The corrected document and
durable memory must replace the first version rather than coexist with stale
guidance.

1. Search company knowledge for Hosted API request-log retention, deletion and
   purge timing, backups, residency, audit evidence, and security-questionnaire
   commitments. Fetch evidence from at least three source systems.
2. Create and mount `task/ethan-park/retention-rule-v1-v3` from
   `person/ethan-park`. Write and index
   `/knowledge/policies/hosted-api-retention.md` with only the initially
   supported claims, explicit unknowns, validation steps, and citations.
   Use `knowledge_write_artifact` for the file write so the operation remains
   portable in the captured workload trace; do not use shell commands or
   `apply_patch` for this workflow.
3. Store those accepted claims as semantic memory on the v1 branch. Diff and
   atomically merge the complete indexed-document and memory bundles into
   `person/ethan-park` using `knowledge_merge_preview`, its preview token, and
   explicit stable change IDs. Delete the v1 task branch.
4. From the updated personal branch, create and mount
   `task/ethan-park/retention-rule-correction-v3`. Search for stronger or more
   specific evidence about tenant termination, erasure, backup retention, and
   cross-region behavior. Fetch the complete authoritative sources.
5. Revise the existing canonical policy in place, retaining uncertainty where
   the evidence does not establish a promise, and re-index the same path.
   Write an unindexed `/artifacts/drafts/retention-evidence-matrix.md` showing
   rejected or conflicting interpretations. Use `knowledge_write_artifact`
   for both writes; do not use shell commands or `apply_patch`.
6. Store a replacement semantic memory that cites the corrected policy and
   authoritative sources and uses `supersedes` with the v1 memory ID. The old
   memory must not remain branch-visible after promotion.
7. Diff and preview the correction. Atomically merge only the complete
   corrected-policy and replacement-memory bundles, including the changes that
   remove the superseded memory, into `person/ethan-park` with explicit stable
   change IDs and the preview token. Exclude the draft evidence matrix.
8. Verify that the target returns the corrected policy and replacement memory,
   does not return the superseded memory or draft, and then delete the
   correction branch.

Report both policy revisions, both memory IDs, the supersession relation,
evidence, selected change IDs, excluded path, merge results, verification, and
deletions.
