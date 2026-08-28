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

The historical `atomic-updates.json` add-on defines workflows 14–16:
correctness-focused document and memory updates. They exercise selective
promotion that excludes provisional artifacts, competing sibling revisions
where only one candidate is accepted, and a published rule whose indexed
document and durable semantic memory are later superseded. Its archived
captures retain the explicit merge-selection bookkeeping used by the earlier
performance suite; the active prompts express the same business situations in
agent-facing terms.

The reference Codex captures for workflows 14–16 are under
`runs/20260801-atomic-updates-v1`, with normalized traces under
`traces/atomic-updates-v1`. They were curated against a deterministic,
connector-stratified 600-document sample of the real generated corpus
(`sample_fraction=0.02`, `sample_seed=chronos-atomic-updates-v1`, capped at
600 documents; selection digest
`7c2c59c0d0bfb2560bf51053526b489b75827ed2b959011a6afb9ab2f6eb2cf9`).
The capture manifest records the exact rollout and trace hashes. Timed
full-corpus evaluation should replay these operations against the normal
curated-v2 benchmark base; this smaller capture state is provenance for agent
decisions, not a retrieval-quality evaluation corpus.

The active agent-facing prompts have a fresh, rollout-backed capture under
`runs/20260814-realistic-v2`, with normalized traces under
`traces/realistic-v2-20260814`. Each trace records the Codex rollout hash, the
logical MCP/shell/patch operations, and `llm_timing`: the wall-clock interval
from each user or tool result to the next model-emitted operation or final
answer, excluding tool execution. A nonzero `codex_exit_code` is preserved in
the trace metadata when Codex's safety filter or an unavailable dependency
ends a session; the corresponding persisted rollout remains the source of
truth rather than being replaced by a synthetic workload. The capture
manifest records all 16 workflow sessions and their prompt/rollout hashes.

The active prompts in `prompts/` are the agent-facing versions: they describe
the business outcome, evidence boundary, isolated work, and publication intent
without prescribing benchmark bookkeeping or backend internals. The former
instrumented prompts are preserved in `prompts/benchmark-v1/`. A new capture
can select another prompt and output version with `ENTERPRISE_PROMPTS_DIR`,
`ENTERPRISE_CAPTURE_RUN_ID`, `ENTERPRISE_CAPTURE_TRACE_ID`, and
`ENTERPRISE_CAPTURE_TAG` while retaining the same JSONL trace format.

The separate `concurrency-v1/incident-response-swarm/` experiment uses one
prompt template with independent incident inputs. Its process-based runner
replays those traces against one shared team branch and accepts up to 128
workers; it is intentionally kept outside the numbered sequential suite.

Run workflows 14–16 across all three backends with the controlled Qdrant
deployment:

```bash
apps/enterprise-knowledge-mcp/scripts/run_atomic_updates_pipeline.sh
```

The wrapper uses the same digest-pinned Docker Qdrant image, memory limit,
workers, shards, optimizer settings, HNSW settings, and gRPC transport for
Chronos, native branching, and application-managed state. Each backend gets
fresh service storage to prevent cache and state carryover. The runner rejects
embedded Qdrant, records the configuration in `qdrant-deployment.json`, and
Chronos verifies or recreates its interval-visibility payload indexes whenever
it opens an existing collection.
