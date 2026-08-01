You are Selene Huang, a staff applied scientist at Redwood Inference. Use the
`chronos_enterprise_knowledge` MCP tools and the actual company corpus. Do not
inspect benchmark gold answers and do not modify the Chronos source. Do not
inspect the MCP server's backing databases directly; access company state
through MCP tools and the mounted branch workspace only.

1. Create and mount `task/selene-huang/enterprise-rag-qa-v2` from
   `person/selene-huang`.
2. Search independently for every question below. Fetch full documents when a
   snippet is insufficient, reconcile conflicting sources, and cite at least
   one document ID and path per answer.

Questions:

1. `qst_0006`: In the draft spec about extending a routing policy engine for
   automated regional failover, what is the proposed priority order for
   evaluating different failure signals?
2. `qst_0012`: What caused intermittent VPN blackholes and duplicate-IP
   warnings on corporate Wi-Fi in SF2 behind the affected edge switch?
3. `qst_0028`: What steps and commands were recommended for a signed annotated
   v1.14.0 Git tag and draft GitHub release?
4. `qst_0030`: Which customer tiers can enter the feature-flagged beta for the
   Console cohort comparison and trace-anomaly feature?
5. `qst_0077`: What is the three-step cross-language streaming startup
   handshake?
6. `qst_0138`: What was the CTO's non-negotiable security requirement for the
   analytics SaaS customer moving to a dedicated VPC deployment?
7. `qst_0165`: What condition and timing governed approval of the additional
   senior infrastructure engineer under the hiring freeze?
8. `qst_0174`: Which fields are required in each contract-conformance ledger
   entry?
9. `qst_0210`: What numeric rule combines test failures, latency, and vector
   drift into a 0–100 release-risk score, and how do score bands affect action?
10. `qst_0460`: What next steps followed the first-round senior sales engineer
    interview?

3. Write the exact answer array as JSON through the mounted workspace at
   `/artifacts/enterprise-rag/answers.json`, then index it.
4. Write and index
   `/artifacts/enterprise-rag/evidence-reconciliation.md` describing the
   reusable procedure used to resolve identifiers, fetch full sources, and
   handle contradictions.
5. Store that validated procedure as playbook memory with evidence from at
   least three source systems. Do not store the answer text as durable memory.
6. Leave the task branch isolated for answer evaluation; do not merge or
   delete it.

Return the answer array, artifact paths, memory ID, and evidence.
