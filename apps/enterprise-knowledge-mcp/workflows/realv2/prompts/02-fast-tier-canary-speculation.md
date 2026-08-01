You are Marcus Reed, a senior engineer on Redwood's Runtime Scheduling team.
Use the `chronos_enterprise_knowledge` MCP tools and the actual company
knowledge base. Do not inspect benchmark gold answers or modify the Chronos
source repository.

A Fast-tier batching canary has a material p99 latency regression. The team
must decide between two mutually exclusive responses:

- **Rollback candidate:** abort the canary and return to the last known-good
  batching behavior.
- **Guarded-tuning candidate:** keep a narrowly scoped canary running while
  changing admission-control or requeue behavior within documented limits.

Both are plausible until the current SLO, abort criteria, rollout evidence,
dashboards, reason codes, and rollback procedure are reconciled.

1. Inspect available branches. Create and mount these sibling branches from
   `team/runtime-scheduling`:

   - `task/runtime-scheduling/fast-tier-rollback-realv2`
   - `task/runtime-scheduling/fast-tier-guarded-tuning-realv2`

   If either exact task branch already exists, stop and report the dirty
   starting state rather than reusing or deleting it.

2. Search the parent branch for the Fast-tier latency SLOs and error budget,
   the batching-default rollout checklist and launch brief, the live p99
   regression investigation, the batching observability specification, the
   admission-control implementation, the tiered dashboards, and the incident
   runbook follow-up. Fetch the strongest full documents. Separate normative
   thresholds from work-in-progress proposals and chat hypotheses.

3. On the rollback branch, use `knowledge_update_document` to write an
   evidence-backed version of:

   - path:
     `/knowledge/curated/runtime-scheduling/fast-tier-canary-decision.md`
   - document ID: `curated_fast_tier_canary_decision`
   - title: `Fast-tier canary response decision`
   - source: `runtime-scheduling-curation`
   - kind: `curated`

   It must contain the measured symptom, applicable SLO and abort threshold,
   exact rollback sequence, validation dashboards and reason codes, owner,
   and conditions required before retrying the canary. Clearly label this
   version `Candidate: rollback`.

4. On the guarded-tuning branch, independently evaluate the evidence and write
   a different version at the same path and with the same document ID. It must
   state the maximum safe scope and duration of continued canary exposure,
   the specific admission-control or requeue change, stop conditions,
   dashboards and reason codes, owner, and rollback trigger. Clearly label
   this version `Candidate: guarded tuning`.

5. Validate isolation through retrieval:

   - Search each branch for language distinctive to its own candidate and
     confirm its version is returned.
   - Search the sibling and parent for the same distinctive language and
     confirm the candidate document is not visible there.
   - Fetch `curated_fast_tier_canary_decision` from both task branches and
     confirm that the document contents and hashes differ.

6. Diff each candidate against `team/runtime-scheduling`, then diff the two
   candidates against each other. Select the candidate supported by the
   strongest current evidence. A documented SLO abort condition cannot be
   waived merely to preserve a canary.

7. Before promotion, update the selected candidate document once more, using
   the same path and document ID, to add a short decision record explaining
   why the other candidate was rejected and citing the decisive evidence.
   Store the accepted decision as semantic memory with the same evidence.

8. Merge only the selected branch into `team/runtime-scheduling`. Verify that
   the parent branch retrieves the accepted document and memory, while text
   unique to the rejected candidate is absent from that document. Delete both
   temporary task branches after successful verification.

Report the selected response, rejected response, evidence, document hashes,
isolation checks, diffs, merge result, memory ID, and branch deletions.
