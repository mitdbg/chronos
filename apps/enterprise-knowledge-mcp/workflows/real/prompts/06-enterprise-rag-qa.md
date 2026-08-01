You are a Redwood internal knowledge analyst. Answer the ten company questions
below using the `chronos_enterprise_knowledge` MCP tools and the actual company
knowledge base. Do not use general knowledge, do not inspect the benchmark's
gold answers, and do not modify the Chronos source repository.

Create branch `task/enterprise-rag-qa-capture-20260727` from
`team/runtime-diagnostics-capture-20260727`. Search independently for every
question. Fetch a full document when the search snippet is insufficient.

For every answer provide:

- `question_id`
- `answer`
- `evidence`, containing at least one document ID and path

Questions:

1. `qst_0006`: In the draft spec about extending a routing policy engine for
   automated regional failover, what is the proposed priority order for
   evaluating different failure signals when deciding whether to shift traffic
   or fail over?
2. `qst_0012`: What was identified as the root cause of the intermittent VPN
   connectivity blackholes and duplicate IP warnings for users on the
   corporate Wi-Fi in the SF2 building behind a specific edge switch?
3. `qst_0028`: What steps and commands were recommended for creating and
   pushing a signed annotated Git tag and then making a draft GitHub release
   for the v1.14.0 release?
4. `qst_0030`: In the Console feature that compares canary or A-B cohorts and
   links metric anomalies to request traces, what customer tiers are allowed
   into the beta via a feature flag gate?
5. `qst_0077`: What is the three-step message sequence used for the new
   cross-language streaming startup handshake in the SDKs?
6. `qst_0138`: What was the CTO's non-negotiable security requirement
   mentioned on the early-2026 demo call for the analytics SaaS customer
   moving from hosted API to a dedicated VPC deployment?
7. `qst_0165`: What was the agreed condition and timing for approving the
   additional senior infrastructure engineering hire under the hiring-freeze
   plan?
8. `qst_0174`: What are the required fields that each entry in the contract
   conformance results ledger must include?
9. `qst_0210`: What numeric rule combines automated test-slice failures,
   latency, and vector drift into a 0--100 release-risk score, and how do the
   score bands determine proceed, slow down, pause, or revert?
10. `qst_0460`: In a first-round technical interview for a senior sales
    engineer candidate, what were the agreed next steps after the call?

After answering, write the exact answer array as JSON to
`/artifacts/enterprise-rag/answers.json` using `knowledge_write_artifact`.
Do not merge or delete the branch.
Return the same JSON array in your final response.
