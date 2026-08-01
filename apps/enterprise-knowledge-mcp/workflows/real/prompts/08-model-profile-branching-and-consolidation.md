You are Olivia Grant, a senior applied-ML engineer at Redwood Inference.
Perform this task using the `chronos_enterprise_knowledge` MCP tools and the
actual company knowledge base. Do not inspect benchmark gold answers and do
not modify the Chronos source repository.

Evaluate two plausible model-optimization profiles for a mixed Hosted and
Dedicated workload while preserving quality, tail latency, and deployment
compatibility:

1. Create sibling branches
   `task/olivia-grant/profile-latency-full-20260727` and
   `task/olivia-grant/profile-quality-full-20260727` from
   `person/olivia-grant`, mounting both checkouts.
2. On the latency branch, gather current benchmark, runtime, and rollout
   evidence. Write and index a candidate report under
   `/artifacts/model-profile/latency-candidate.md`, then store its validated
   findings as episodic memory.
3. On the quality branch, independently gather quality-guardrail, regression,
   product, and compatibility evidence. Write and index a candidate report
   under `/artifacts/model-profile/quality-candidate.md`, then store its
   validated findings as episodic memory.
4. Diff both candidates against `person/olivia-grant`. Fetch the evidence
   needed to reconcile any disagreement. Merge both reviewed evidence packages
   into `person/olivia-grant`; do not silently discard a valid constraint.
5. Create
   `task/olivia-grant/profile-consolidation-full-20260727` from the updated
   personal branch. Write a final recommendation under
   `/artifacts/model-profile/final-recommendation.md`. Store one consolidated
   semantic memory that cites the source evidence and supersedes the two
   candidate episodic memories.
6. Merge the consolidation branch into `person/olivia-grant`, verify the final
   diff, and delete all three task branches.

Report the selected profile, rejected alternatives, candidate and consolidated
memory IDs, merge results, and evidence used.
