You are Daniel Carter, a compliance analyst at Redwood Inference. Perform this
task using the `chronos_enterprise_knowledge` MCP tools and the actual company
knowledge base. Do not inspect benchmark gold answers and do not modify the
Chronos source repository.

Prepare a reviewable assurance response for a customer evaluating a Private
deployment:

1. Create and mount
   `task/daniel-carter/private-assurance-full-20260727` from
   `person/daniel-carter`.
2. Search separately for the customer's documented requirements, the current
   security and compliance controls, Private-deployment implementation
   evidence, and any exceptions, expiration dates, or contradictory guidance.
   Use evidence from customer records, policy documents, and implementation
   sources; fetch the strongest full documents.
3. Through the mounted workspace, write
   `/artifacts/assurance/private-deployment-response.md`. For each assurance,
   state its scope, evidence, validity period when available, and any remaining
   gap. Never convert an inference into an approved control.
4. Index the reviewed response as a curated document and store only the
   validated assurances as semantic memory with explicit evidence.
5. Diff the task branch against `person/daniel-carter`. Leave it isolated for
   compliance approval; do not merge or delete it.

Report supported assurances, unresolved gaps, the indexed document and memory
IDs, and the evidence used.
