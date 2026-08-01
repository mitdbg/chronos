# Recorded Codex rollout workloads

`curated-v2/` is the main benchmark suite. Its `suite.json` declares thirteen
workflows over the complete EnterpriseRAG corpus and pinned vLLM, LiteLLM, and
Langfuse checkouts. Eight workflows resolve real open upstream issues; five
exercise company QA and memory, onboarding, organizational branch creation,
parallel incident branches, and repeated document replacement. The persistent
pipeline records a real Codex session for every prompt before any backend
benchmark runs.

`real/` is the earlier historical suite. Its six source sessions perform these
workflows:

1. Create a reliability-engineering department and runtime-diagnostics team,
   curate their starting knowledge, and write durable department and team
   memory.
2. Add Priya Shah to the team, create her personal branch, and record
   onboarding memory.
3. Run three debugging investigations concurrently, one branch per
   investigation. Each agent retrieves company knowledge, creates branch-local
   artifacts, updates an indexed workspace file, and records episodic memory.
4. Create a QA branch and answer ten EnterpriseRAG questions with cited
   evidence.

`real/prompts/` contains the exact task prompts, `real/runs/20260727/` contains
the Codex event streams and final responses, and `real/traces/` contains the
portable traces extracted from the corresponding session rollouts. The
combined `enterprise-knowledge-real-workflow.jsonl` preserves timestamp order
across the three parallel debugging sessions. `real/PROVENANCE.md` maps each
trace to its source Codex session and rollout hash.

Both suites treat the MCP boundary, shell commands, and file patches as the
application workload. Storage-engine calls below those interfaces remain
opaque. Main-suite prompts explicitly prohibit direct access to backend
SQLite, Qdrant, and ChronosFS state. Shell and patch replay is disabled unless
`--allow-shell` is supplied for a trusted trace.
