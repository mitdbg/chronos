You are the platform operations lead at Redwood Inference. Perform this task
using the `chronos_enterprise_knowledge` MCP tools; do not modify the Chronos
source repository.

Redwood is establishing a Reliability Engineering department and a Runtime
Diagnostics team. This is an operational task, not a hypothetical proposal.

1. Inspect the available branches and company knowledge.
2. Search for company evidence about runtime reliability, incident response,
   regional failover, release safety, and customer-facing availability.
3. Create branch
   `department/reliability-engineering-capture-20260727` from `main`.
4. The checkout must be mounted. On that department branch, create a concise
   department charter through the returned `workspace_path` at
   `/knowledge/departments/reliability-engineering/charter.md`. It must state
   the department mission, responsibilities, interfaces with SRE and Runtime,
   and evidence-backed first-quarter priorities. Cite document IDs and paths.
   Then make that existing workspace file searchable with
   `knowledge_index_workspace_file`.
5. Store the department's validated mission and operating scope as semantic
   memory on the department branch, with the source documents as evidence.
6. Create branch `team/runtime-diagnostics-capture-20260727` from the new
   department branch.
7. On the mounted team checkout, create an operating plan through the returned
   `workspace_path` at
   `/knowledge/teams/runtime-diagnostics/operating-plan.md` covering intake,
   investigation artifacts, validation, escalation, and promotion of durable
   findings. Then index that existing workspace file with
   `knowledge_index_workspace_file`.
8. Store the validated investigation-and-promotion procedure as a team
   playbook memory, citing the department charter and company evidence.
9. Do not merge or delete either branch.

Report the created branches, documents, memories, and evidence used.
