You are Wei Chen, a senior technical writer on Redwood's Technical
Documentation team. Use the `chronos_enterprise_knowledge` MCP tools and the
actual company knowledge base. Do not inspect benchmark gold answers or modify
the Chronos source repository.

Support reports that the customer-facing data-residency troubleshooting
guidance contains terminology and response examples that do not consistently
match the approved contract and deployed behavior. Produce and promote one
corrected, searchable support article.

1. Inspect available branches. Create and mount
   `task/technical-documentation/residency-error-contract-realv2` from
   `team/technical-documentation`. If that exact task branch already exists,
   stop and report the dirty starting state rather than reusing or deleting
   it.

2. First-pass research: search for and fetch the normative data-residency
   error-contract ADR and the product requirements for policy-block messaging.
   Identify the required HTTP status, stable code and subcodes, required
   fields, streaming behavior, retry semantics, privacy restrictions, and
   customer remediation.

3. Using only that normative evidence, call `knowledge_update_document` to
   create a review draft with:

   - path:
     `/knowledge/curated/technical-documentation/residency-policy-violation.md`
   - document ID: `curated_residency_policy_violation_v2`
   - title: `Data residency policy violation: support and SDK contract`
   - source: `technical-documentation-curation`
   - kind: `curated`

   Record the returned draft hash and chunk count.

4. Implementation and incident review: independently search for and fetch the
   merged gateway implementation, engineering delivery ticket, SDK behavior,
   merged documentation change, and the resolved customer incident in which
   streaming returned a generic server error. Reconcile conflicts by
   distinguishing:

   - normative contract,
   - deployed behavior confirmed by resolution or tests,
   - obsolete or transitional examples,
   - internal-only fields that must not appear in customer guidance.

5. Call `knowledge_update_document` a second time with the exact same path and
   document ID. Replace the draft with the final article; do not create a
   second document. The final article must include:

   - a canonical non-streaming response example,
   - pre-stream behavior and what Support should collect,
   - SDK handling and retry guidance,
   - a short customer remediation checklist,
   - a clearly labeled internal Support note,
   - evidence citations by document ID and path,
   - a `Reconciled inconsistencies` section identifying obsolete terms or
     shapes without presenting them as valid alternatives.

   Record the final hash and chunk count and confirm that its hash differs from
   the draft hash.

6. Validate the replacement and embedding isolation:

   - Fetch `curated_residency_policy_violation_v2` and confirm there is only
     one document with that ID and that it contains the final article.
   - Search the task branch using at least three customer phrasings that do not
     repeat the title, covering the policy block, streaming symptom, and SDK
     handling. Confirm the updated article is retrieved.
   - Run the same searches on `team/technical-documentation` before merge and
     confirm the new article is not visible there.
   - Diff the task branch against the team branch and report the document and
     file changes.

7. Merge the reviewed task branch into `team/technical-documentation`.
   Repeat the three retrieval checks on the team branch and fetch the document
   to verify the final hash. Delete the temporary task branch only after all
   checks pass.

Do not store a separate memory for the article; this workflow is intended to
measure document and embedding replacement. Report the evidence, reconciled
contract, draft and final hashes, chunk counts, retrieval checks, diff, merge
result, and branch deletion.
