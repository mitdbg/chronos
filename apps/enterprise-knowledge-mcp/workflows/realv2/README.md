# Enterprise knowledge real-v2 workflows

These prompts are curated from the Redwood Inference corpus in
`EnterpriseRAG-Bench/generated_data`. They isolate the three application
behaviors the evaluation should measure instead of mixing all of them into
every workflow.

| Prompt | Enterprise use case | Knowledge operations stressed | Expected durable effect |
|---|---|---|---|
| `01-enterprise-rag-qa.md` | Support and internal company Q&A | Search and full-document fetch across connectors | None |
| `02-fast-tier-canary-speculation.md` | Runtime team evaluates two mutually exclusive responses to a regressing canary | Sibling branch creation, branch-local document/vector writes, isolation checks, diff, merge, and discard | Only the selected decision is promoted |
| `03-residency-error-contract-update.md` | Documentation team corrects a customer-facing support article after reconciling policy, implementation, SDK, and incident evidence | Repeated update of one document ID, embedding replacement, branch-local retrieval validation, diff, and merge | One reviewed article replaces its draft |

## Expected trace shape

These are behavioral invariants rather than exact search-call counts:

- `01`: zero mutating MCP calls; at least one independent search per question
  and evidence-bearing final answers.
- `02`: two sibling checkouts; divergent writes to the same document ID; at
  least three diffs; exactly one accepted merge; both temporary branches
  deleted; rejected candidate state absent from the parent.
- `03`: one checkout; exactly two updates to the same document ID; the second
  update has a different content hash; one diff and one merge; successful
  retrieval before merge only on the task branch and after merge on the team
  branch.

## Curation principles

- Prompts name the business symptom and the types of evidence to reconcile, but
  do not reveal benchmark answers or expected document IDs.
- A branch exists only when the work is provisional or mutually exclusive.
  Read-only Q&A therefore runs directly against an existing team branch.
- Speculative branches write different versions of the same logical indexed
  document. This makes isolation meaningful: accepting one candidate must not
  leak the rejected candidate's document or embeddings.
- The update workflow calls `knowledge_update_document` twice with the same
  explicit `document_id`. The second write must replace the document,
  chunks, and embeddings rather than create a duplicate.
- Every workflow includes observable correctness checks, not merely a request
  to call the desired operations.

## Corpus grounding

The read-only questions are retained from
`real/prompts/06-enterprise-rag-qa.md`.

The speculative canary workflow is grounded in the Fast-tier tail-latency
evidence cluster spanning the SLO definition, batching rollout brief,
runtime-investigation thread, observability specifications, admission-control
implementation, dashboards, and engineering follow-ups.

The document-update workflow is grounded in the data-residency error-contract
cluster spanning ADR-022, the product and engineering requirements, the
gateway implementation, the Python SDK change, the merged documentation work,
and the resolved streaming support incident.

## Execution assumptions

Run against a hierarchy initialized by `init-hierarchy`. The expected parent
branches are:

- `team/technical-support`
- `team/runtime-scheduling`
- `team/technical-documentation`

The two mutating prompts delete their temporary task branches after a
successful merge. Use a fresh benchmark state, or remove only their exact task
branches before recapturing a run that previously failed.
