You are Ethan Park, manager of Redwood's Platform & Console team. Use the
`chronos_enterprise_knowledge` MCP tools and a mounted workspace. Do not
modify the Chronos source repository or inspect benchmark gold answers. Do
not inspect the MCP server's backing databases directly; access company state
through MCP tools and the mounted branch workspace only.

Prepare a reviewable launch decision for a planned Hosted API capability. The
decision document must evolve as evidence from different functions arrives;
do not create one document per phase.

1. Create and mount `task/ethan-park/launch-readiness-v2` from
   `person/ethan-park`.
2. Search and fetch the current product requirement, delivery status, and
   design evidence. Write the first version of
   `/artifacts/product/launch-readiness.md` with the proposed capability,
   satisfied product gates, unresolved delivery items, owners, and citations.
   Index the workspace file.
3. Search and fetch SRE, security, release, and rollback requirements. Revise
   the existing file in place to reconcile this evidence, add operational
   gates and blockers, and re-index the same path.
4. Search and fetch customer, support, and rollout-cohort evidence. Revise the
   same file a final time with the supported launch scope, remaining
   approvals, evidence-backed next steps, and an explicit go, conditional-go,
   or no-go recommendation. Do not invent a date. Re-index the same path.
5. Store the final decision and accepted launch criteria as semantic memory,
   citing the final artifact and authoritative sources from at least three
   source systems.
6. Diff against `person/ethan-park` and leave the branch isolated for review;
   do not merge or delete it.

Report all three indexed revisions, the final recommendation, blockers,
memory ID, diff, and evidence.
