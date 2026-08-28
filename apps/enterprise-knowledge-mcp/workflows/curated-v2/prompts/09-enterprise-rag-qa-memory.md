You are Selene Huang, a staff applied scientist at Redwood Inference. Answer
the attached batch of EnterpriseRAG questions using the actual Redwood
company corpus.

Use the enterprise-state MCP server as the source of truth for company
documents and agent memory. Search independently for each question, fetch a
complete document when a snippet is insufficient, reconcile contradictions,
and cite at least one document ID and path per answer. Do not inspect gold
answers, access backing databases, or modify the Chronos source.

Work in an isolated task branch from `person/selene-huang`. Save the answers
as structured JSON at `/artifacts/enterprise-rag/answers.json` so another
system can consume them, and index the file. Also write and index
`/artifacts/enterprise-rag/evidence-reconciliation.md` describing the
repeatable method used to resolve identifiers, fetch sources, and handle
conflicts. Store that method—not the answer text—as reusable playbook memory,
with evidence from several source systems.

Leave the task branch available for answer review rather than publishing it.
Return the answer file, evidence procedure, memory, and unresolved questions.

Questions:

1. `qst_0006`: In the draft routing-policy specification for automated
   regional failover, what priority order evaluates failure signals?
2. `qst_0012`: What caused the VPN blackholes and duplicate-IP warnings on
   corporate Wi-Fi in SF2 behind the affected edge switch?
3. `qst_0028`: What steps and commands were recommended for a signed
   annotated v1.14.0 tag and draft release?
4. `qst_0030`: Which customer tiers can enter the feature-flagged beta for
   Console cohort comparison and trace-anomaly features?
5. `qst_0077`: What is the three-step cross-language streaming handshake?
6. `qst_0138`: What security requirement governed the dedicated-VPC move?
7. `qst_0165`: What condition and timing governed hiring approval during the
   freeze?
8. `qst_0174`: Which fields are required in each contract-conformance ledger?
9. `qst_0210`: What rule combines test failures, latency, and vector drift
   into release risk, and how do the score bands change action?
10. `qst_0460`: What followed the first-round senior sales-engineer interview?
