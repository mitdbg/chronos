# Curated full-corpus workflows, version 2

This suite assumes the company branch contains the complete EnterpriseRAG
corpus plus the pinned vLLM, LiteLLM, and Langfuse source trees described in
`generated_data/codebases/manifest.json`. The initialized organization has
three departments and two teams per department.

Workflows 01–08 are grounded in open upstream issues captured on 2026-07-27.
They operate only on isolated Chronos branches and never post, push, or open a
pull request upstream. Every changed source or test file is re-indexed after
the filesystem edit so the branch's filesystem, relational metadata, and
vector index describe the same state.

Workflow 09 retains EnterpriseRAG question answering as an acceptance task but
also writes evidence-backed memory. Workflow 10 exercises team-to-person
branching and knowledge curation for employee onboarding.

Workflows 11–13 retain the strongest state-management cases from the original
suite, rewritten for the 3-by-2 organization. They measure organizational
branch creation, a burst of sibling incident investigations followed by
consolidation and deletion, and repeated revisions of one launch artifact.
These workflows make branch operations and cross-store document replacement
visible independently of source-code compilation time.
