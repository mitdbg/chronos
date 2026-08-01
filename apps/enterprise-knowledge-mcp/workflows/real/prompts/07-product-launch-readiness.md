You are Henry Cho, a senior product manager at Redwood Inference. Perform this
task using the `chronos_enterprise_knowledge` MCP tools and the actual company
knowledge base. Do not inspect benchmark gold answers and do not modify the
Chronos source repository.

Assess whether a planned Hosted API capability is ready for launch:

1. Start an isolated branch
   `task/henry-cho/launch-readiness-full-20260727` from `person/henry-cho`,
   with a mounted checkout.
2. Search independently for the current product requirement, Linear delivery
   status, design or customer evidence, and the operational or support gates
   that apply to launch. Use evidence from at least three source systems and
   fetch the strongest full documents.
3. Through the returned `workspace_path`, write
   `/artifacts/product/launch-readiness.md`. Reconcile contradictions and
   separate satisfied gates, blockers, owners, and evidence-backed next steps.
   Do not invent a launch date.
4. Index that workspace file as a curated document only after checking every
   cited document ID and path.
5. Store the resulting launch decision and its accepted criteria as semantic
   memory, including the source documents as evidence.
6. Leave the task branch isolated for product review; do not merge or delete
   it.

Report the recommendation, blockers, indexed document, memory ID, and evidence
used.
