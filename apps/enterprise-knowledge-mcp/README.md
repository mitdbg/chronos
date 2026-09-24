# Chronos Enterprise Knowledge MCP

This application gives a coding agent one branch-aware company workspace backed
by three ordinary systems:

- PostgreSQL stores the document/chunk catalog, provenance, and memory
  metadata for the Chronos service and benchmarks.
- ChronosFS stores the original EnterpriseRAG documents, curated knowledge,
  durable memory, and generated task artifacts.
- Qdrant stores chunk text, dense embeddings, and BM25 sparse vectors.

Each fact has one authoritative home. The relational catalog contains
identifiers, paths, hashes, ordinals, and Qdrant point IDs, but does not
duplicate document or chunk text. ChronosFS is authoritative for complete
files. Qdrant is authoritative for retrieval units and their search indexes.
`knowledge_search` fuses dense and BM25 rankings with Qdrant RRF.

Chronos creates and coordinates the same branch across all three systems. A
department inherits company knowledge, a team inherits its department, a person
inherits the team, and a task inherits the person's knowledge and memory.
Changes made in one task stay private until they are merged.

The user-facing model is only **branch → search/read/update → diff/merge or
discard**.

`chronos` is the default backend. Three experiment-only backends implement the
identical logical API:

- `app-managed` stores a version history for branch-local deltas, records the
  parent revision at every fork, and reconstructs visibility by walking
  ancestry in application code.
- `physical-clone` duplicates every visible document, file, and vector when a
  branch is created.
- `doltgres-qdrant-btrfs` composes three independent native mechanisms:
  Doltgres branches the catalog, Qdrant evaluates branch-aware hybrid-search
  filters, and writable Btrfs snapshots isolate files.

They are not additional MCP protocols. They exist so one recorded agent task
can quantify the application complexity, query work, storage amplification,
and cross-store coordination avoided by Chronos.

All backends expose merge previews, stable change IDs, complete indexed-document
bundles, filesystem-path groups, preview-token validation, and explicit subset
selection. The comparison backends report `atomic: false`: after validating the
same high-level request, they apply the selected relational, vector, and file
changes through their existing application-managed write paths. They therefore
match successful-run semantics without acquiring Chronos's single atomic
multi-store publication point.

The concurrency microbenchmark also includes `app-managed-big-lock` and
`doltgres-qdrant-btrfs-big-lock`. These are benchmark-only controls: a single
process lock surrounds the complete `merge_preview` plus `merge` call while
leaving the underlying backend, stores, and configuration unchanged. They
measure how much throughput a coarse application-level serialization policy
can recover, rather than adding a new storage implementation.

## Why this memory model

The design follows recurring practices in agent-memory research and deployed
agent frameworks:

- Keep current-session history separate from durable cross-session knowledge.
  Agent frameworks commonly persist session history and compact it when it
  grows, rather than treating every turn as a long-term fact
  ([OpenAI Agents SDK sessions](https://openai.github.io/openai-agents-js/guides/sessions/)).
- Separate stable facts, completed experiences, and reusable procedures. This
  semantic/episodic/procedural distinction is used by cognitive architectures
  for language agents
  ([CoALA](https://arxiv.org/abs/2309.02427)).
- Write selectively and maintain memories after insertion. Production-oriented
  systems extract and consolidate salient information
  ([Mem0](https://arxiv.org/abs/2504.19413)), while agentic-memory research
  links new evidence to prior memories and lets memory evolve
  ([A-MEM](https://arxiv.org/abs/2502.12110)).
- Preserve provenance. This implementation stores evidence and confidence with
  durable facts and explicitly supersedes stale entries; raw turns and
  unsupported hypotheses remain task-local.
- Put document identity and source context into every chunk, then combine
  lexical and dense retrieval with rank fusion
  ([Anthropic contextual retrieval](https://www.anthropic.com/engineering/contextual-retrieval)).
- Keep durable memory inspectable as files while indexing it for retrieval.
  The company, department, team, person, and task hierarchy scopes which
  inherited knowledge an agent can read and update.

`knowledge_remember` therefore accepts only:

| Kind | Retained content |
|---|---|
| `semantic_memory` | A validated fact or decision, with evidence |
| `episodic_memory` | The outcome and useful retrospective of a completed task |
| `playbook` | A reusable, validated procedure |

Every memory is also a ChronosFS document, relational record, and Qdrant
embedding.
Replacing or deleting it updates all three stores in the same branch. Memory
consolidation uses the same write path: the agent writes the reviewed
replacement and lists the old memory IDs in `supersedes`; Chronos removes the
stale entries from that branch.

## Install

From the `chronos` repository:

```bash
.venv/bin/uv sync --all-packages --all-extras
```

### Native-component comparison backend

The native-component baseline follows
[Qdrant's public branch-aware search design](https://qdrant.tech/documentation/tutorials-search-engineering/branch-aware-search/).
Each vector carries its creating branch and branch-local sequence; an update
adds a branch-specific supersession marker to the old point. Search selects
points from the current branch and its ancestors, caps each ancestor at the
sequence visible when its child was created, and excludes superseded points.
Relational rows use
[Doltgres branches](https://docs.doltgres.com/concepts/git/branch), while each
file checkout is a writable
[Btrfs subvolume snapshot](https://btrfs.readthedocs.io/en/latest/btrfs-subvolume.html).

This composition provides native copy-on-write inside each component, but the
application still has to maintain ancestry and update ordering across all
three. It cannot make a Doltgres commit, Qdrant payload update, and Btrfs file
change one atomic transaction. That coordination gap is an intentional
property of the baseline.

Start the pinned Doltgres and Qdrant images:

```bash
cd apps/enterprise-knowledge-mcp/infra/native-branching
mkdir -p data/doltgres data/qdrant
chmod u+rwx data/doltgres data/qdrant
docker compose up -d
```

`CHRONOS_BTRFS_ROOT` must reside on a Btrfs filesystem. The mount must include
`user_subvol_rm_allowed` when the MCP runs without root privileges; fork bases
are read-only snapshots and are made writable immediately before deletion.
For example, mount a dedicated experiment device with:

```bash
sudo mount -t btrfs \
  -o user_subvol_rm_allowed,compress=zstd \
  /dev/EXPERIMENT_DEVICE /mnt/enterprise-knowledge-btrfs
sudo chown "$USER" /mnt/enterprise-knowledge-btrfs
```

Create a dedicated Doltgres database for a directly served MCP:

```bash
PGPASSWORD=password createdb \
  -h 127.0.0.1 -p 55439 -U postgres enterprise_knowledge_native

export CHRONOS_DOLTGRES_DSN=\
postgresql://postgres:password@127.0.0.1:55439/enterprise_knowledge_native
export CHRONOS_BTRFS_ROOT=/mnt/enterprise-knowledge-btrfs
export QDRANT_URL=http://127.0.0.1:6339

.venv/bin/chronos-enterprise-knowledge \
  --state-dir /data/enterprise-native-mcp \
  --backend doltgres-qdrant-btrfs \
  serve
```

For `benchmark-rollouts`, pass an administrative DSN naming a persistent
database such as `postgres`. The benchmark creates a separate Doltgres
database, Qdrant collection, and Btrfs namespace for every trace, backend, and
repetition.

Run Qdrant as a service for the full corpus. Embedded Qdrant is convenient for
tests, but its per-point local persistence is not intended for a
500K-document bootstrap.

The production default is the local Hugging Face model
`sentence-transformers/all-MiniLM-L6-v2`, which emits 384-dimensional vectors.
The model is downloaded on first use, then reused from the Hugging Face cache.
Snapshot preparation uses the model's WordPiece tokenizer to keep chunks
within its 256-token context, with a 240-token target and 24-token overlap.
SentenceTransformers' process-pool encoder parallelizes generation while
preserving input order. Use `--embedding-batch-size`,
`--embedding-chunk-size`, and `--embedding-workers` to tune throughput, and
`--embedding-device` to select `cpu`, `cuda`, or `mps`. Snapshot preparation
also uses two tokenizer workers by default; the measured experiment host
showed no further gain from a third. `--offline-hash-embeddings` exists only
for deterministic tests and smoke runs.

OpenRouter remains available as an explicit compatibility option:

```bash
export OPENROUTER_API_KEY=...
.venv/bin/chronos-enterprise-knowledge \
  --embedding-provider openrouter \
  --embedding-model openai/text-embedding-3-small \
  --dimensions 1536 \
  serve
```

## Prepare and ingest EnterpriseRAG

The paper uses an augmented EnterpriseRAG-Bench corpus containing the original
company records, pinned vLLM/LiteLLM/Langfuse source trees, cutoff-filtered
public GitHub history, and synthetic internal maintenance records. The complete
provenance and one-command rebuild recipe are checked in under
[`datasets/enterprise_rag_infra_v1`](datasets/enterprise_rag_infra_v1/README.md).
The examples below also work with the unaugmented upstream corpus.

```bash
.venv/bin/chronos-enterprise-knowledge \
  prepare-snapshot \
  /path/to/EnterpriseRAG-Bench/generated_data \
  /data/enterprise-rag-full-minilm/snapshot \
  --sample-fraction 1.0
```

The prepared snapshot is a reusable artifact containing every selected
original document, its final chunks, and its embeddings. Text is compressed
and normalized vectors are stored as float16 to keep the complete corpus
artifact practical; ingestion expands vectors to float32 for Qdrant.
Sampling, when requested, is deterministic and stratified by source connector.
The manifest records the selection digest and all chunking, storage, and
embedding parameters. Preparation is resume-safe at the document level, so
completed documents are neither chunked nor embedded again. The snapshot
itself is the reusable embedding artifact; local preparation does not create a
second full embedding cache unless `--cache-snapshot-embeddings` is requested.

When corpus preparation is on the critical path, stage the documents and
chunks first with implicit zero vectors:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --embedding-backend onnx \
  --embedding-model-file onnx/model_qint8_avx512.onnx \
  --placeholder-zero-embeddings \
  prepare-snapshot \
  /path/to/EnterpriseRAG-Bench/generated_data \
  /data/enterprise-rag-full-staged/snapshot \
  --sample-fraction 1.0
```

The placeholder snapshot stores no 384-value zero blobs. It can immediately
exercise full-corpus ingestion, ChronosFS access, branch creation, isolation,
updates, and lexical retrieval. It must not be used to report semantic
retrieval quality. Backfill the same artifact later without reading or
chunking the source documents again:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --embedding-backend onnx \
  --embedding-model-file onnx/model_qint8_avx512.onnx \
  backfill-snapshot-embeddings \
  /data/enterprise-rag-full-staged/snapshot \
  --follow-preparation
```

With `--follow-preparation`, the encoder fills new chunks while the zero-vector
preparation process is still running. If a prior compatible snapshot already
contains completed vectors, reuse them first with
`import-snapshot-embeddings TARGET SOURCE`; chunk IDs and content hashes must
both match before a vector is copied.

Load that artifact into the company branch without contacting the embedding
provider:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --state-dir /data/redwood-knowledge \
  --qdrant-url http://127.0.0.1:6333 \
  ingest-snapshot /data/enterprise-rag-full-minilm/snapshot \
  --branch main --batch-size 2048 --resume
```

For the staged path, add `--follow-preparation` to ingest committed zero-vector
batches while the snapshot builder is still running. Chronos checkpoints the
last ingested relative path after every successful batch. Placeholder and
complete-vector ingestion use distinct checkpoints. A placeholder load still
installs chunk text, BM25 vectors, and full-text indexes in Qdrant; it omits
only the all-zero dense vector. The later hydration pass revisits every
document and adds its dense vector without rechunking.

Together, preparation and loading:

1. preserves every original source file under `/knowledge/company`;
2. extracts structured text from JSON, YAML, Markdown, and source files;
3. chunks on natural blocks using the embedding model's tokenizer;
4. prepends document title, connector, workspace, and path context;
5. generates normalized embeddings in parallel and checkpoints them once;
6. writes the file, relational metadata, and Qdrant points through the public
   Chronos workspace API.

The same path can prepare the pinned source repositories without rescanning
or rechunking the company-document snapshot:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --dimensions 384 \
  --placeholder-zero-embeddings \
  prepare-snapshot \
  /path/to/EnterpriseRAG-Bench/generated_data \
  /data/redwood-code-snapshot \
  --sample-fraction 1.0 \
  --connector codebase
```

Code files are mounted at `/code/<repository>`. Git metadata, dependency
caches, generated output, lockfiles, files over 1 MiB, and separately licensed
enterprise-only directories are excluded. The company-document and code
snapshots remain separate reusable artifacts, but both are ingested into
`main` before creating the organizational hierarchy.

For a cheap validation run:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --offline-hash-embeddings \
  --dimensions 64 \
  prepare-snapshot \
  /path/to/EnterpriseRAG-Bench/generated_data \
  /tmp/redwood-snapshot-smoke \
  --sample-fraction 0.10 --max-documents 100
```

After full ingestion, measure whether `knowledge_search` retrieves the
benchmark's expected evidence:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --state-dir /data/redwood-knowledge \
  --qdrant-url http://127.0.0.1:6333 \
  evaluate-retrieval /path/to/EnterpriseRAG-Bench/questions.jsonl \
  --branch main --top-k 20
```

This reports document Recall@20 and mean reciprocal rank. Questions without
gold document identifiers are reported as unscored rather than counted as
retrieval misses; they require answer-quality evaluation by an agent. The JSON
also records whether the run used semantic vectors, offline test vectors, or
the lexical placeholder path. The command tests the same service method exposed
to Codex, without using a separate retrieval path.

## Create the experiment hierarchy

```bash
.venv/bin/chronos-enterprise-knowledge \
  --state-dir /data/redwood-knowledge \
  --qdrant-url http://127.0.0.1:6333 \
  init-hierarchy --snapshot /data/enterprise-rag-full-minilm/snapshot
```

The curated fixture focuses on three technical departments—Engineering,
Infrastructure & SRE, and Applied ML / Research—with two teams per department
and nineteen named employees drawn from `employee_directory.yaml`. Each node
receives a starting knowledge brief; department and team briefs cite
representative documents that actually occur in the prepared corpus. Source
scopes in those briefs are retrieval hints, not ACL filters. The ten curated
role tasks can be inspected with:

```bash
.venv/bin/chronos-enterprise-knowledge tasks
```

## Main recorded-workflow benchmark

`workflows/curated-v2/suite.json` defines the main benchmark suite. It contains
eight coding-agent tasks grounded in captured open upstream issues, one
full-corpus EnterpriseRAG question-answering and memory task, one employee
onboarding task, and three state-management workloads retained from the
original suite. The latter explicitly stress organizational branch creation,
sibling incident branches with merge and deletion, and repeated replacement
of one indexed document.

Benchmark inputs are never handwritten storage traces. For each prompt,
`scripts/capture_codex_workflows.py` runs Codex against the MCP, locates the
persisted rollout under its isolated `CODEX_HOME`, verifies that its tool calls
are replayable, and records the raw event stream, final response, session ID,
rollout hash, and normalized trace. `scripts/run_full_workflow_benchmarks.py`
then replays only those recorded traces against identically prepared backend
roots and reports each workflow separately.

The persistent end-to-end driver is:

```bash
apps/enterprise-knowledge-mcp/scripts/run_curated_v2_pipeline.sh
```

The driver reads the input from `ENTERPRISE_CORPUS`; without it, the portable
default is `.enterprise-knowledge/datasets/enterprise-rag-infra-v1` under the
repository root. Set the variable to the output of the checked-in dataset
recipe. `ENTERPRISE_DOCUMENT_SNAPSHOT` can select a separate prepared snapshot;
otherwise the derivative corpus is prepared under the curated artifact
directory.

It layers the complete EnterpriseRAG snapshot and pinned-code snapshot,
creates the 3-by-2 hierarchy, captures all thirteen real Codex workflows, and
uses those traces as the benchmark workload.

## Connect Codex

Codex supports local STDIO MCP servers and reads server-wide MCP instructions.
Add this project to `~/.codex/config.toml` or a trusted project's
`.codex/config.toml`:

```toml
[mcp_servers.chronos_enterprise_knowledge]
command = "/absolute/path/to/chronos/.venv/bin/chronos-enterprise-knowledge"
args = [
  "--state-dir", "/data/redwood-knowledge",
  "--qdrant-url", "http://127.0.0.1:6333",
  "serve",
]
env_vars = ["QDRANT_API_KEY"]
startup_timeout_sec = 30
tool_timeout_sec = 300
```

Restart Codex after adding the server. The Codex CLI and IDE extension share
this MCP configuration; `/mcp` shows the connected tools. See the
[official Codex MCP configuration](https://developers.openai.com/codex/mcp/).

The equivalent CLI form is:

```bash
codex mcp add chronos-enterprise-knowledge \
  -- /absolute/path/to/chronos/.venv/bin/chronos-enterprise-knowledge \
  --state-dir /data/redwood-knowledge \
  --qdrant-url http://127.0.0.1:6333 \
  serve
```

## Agent workflow

Each concurrent Codex session uses its own task branch.

1. Call `knowledge_checkout` with a unique task branch and a personal or team
   parent. The returned `workspace_path` is a mounted ChronosFS folder.
2. Call `knowledge_search` for company facts and cite returned document IDs and
   paths.
3. Read inherited source files or create task artifacts under `/artifacts`.
4. Use `knowledge_index_workspace_file` only for material that should become
   searchable.
5. Save a validated fact, task outcome, or procedure with
   `knowledge_remember`.
6. Call `knowledge_merge_preview` to review stable change IDs across the
   relational store, ChronosFS, and Qdrant.
7. Call `knowledge_merge` with the preview token and only the approved change
   IDs, or omit the allow-list to promote everything. An empty allow-list is a
   no-op. The server rejects incomplete indexed-document bundles, so document
   metadata, chunks, embeddings, and the authoritative file cannot diverge.
8. Delete the task branch after promotion or rejection.

All MCP calls carry an explicit `branch_id`; the server never relies on
process-global checkout state. This keeps concurrent agent sessions isolated.
The Chronos backend publishes a selected merge with one native branch-head
change: readers see either the old shared interval head or the continuation
head containing the selected changes. Generated reports and scratch files
remain private unless their filesystem change IDs are explicitly selected.
The PostgreSQL database named by `CHRONOS_POSTGRES_DSN` is the single Chronos
metadata plane and relational application store. Relational rows, ChronosFS
rows, and Qdrant interval payloads share its branch head and segment
allocation. The application creates no workspace manifest, concurrency
generation, or separate branch catalog.
`knowledge_merge` first performs a non-lazy unmount of the source and target
workspaces so external tools cannot race the reviewed filesystem state. Native
Chronos writes are also rejected while the branch transaction reservation is
active. If a process still holds either mount busy, the merge fails closed;
finish the tool process and retry.

## Evaluation hooks

Pass `--trace-dir` and `--trace-id` to `serve` to record content-addressed
logical operations. The trace stores branch creation, document and artifact
updates, searches, memory writes, diffs, and merges independently of the
storage implementation.

Replay mutations against a fresh backend and report final logical state:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --state-dir /tmp/replay-app-managed \
  --dimensions 384 \
  --backend app-managed \
  replay-trace /path/to/trace \
  --skip-reads \
  --verify-branch task/alice/incident

.venv/bin/chronos-enterprise-knowledge \
  --state-dir /tmp/replay-physical-clone \
  --dimensions 384 \
  --backend physical-clone \
  replay-trace /path/to/trace \
  --skip-reads \
  --verify-branch task/alice/incident
```

`--skip-reads` verifies that mutations produce identical final state even when
the backends use different search implementations. Omit it to replay queries;
use `--no-verify-results` when the experiment should measure each backend's
native ranking rather than require byte-identical scores.

Useful non-latency outcomes include:

- answer correctness and evidence recall on EnterpriseRAG questions;
- cross-store consistency after document replacement, deletion, and failure;
- sibling-branch leakage rate under concurrent agents;
- successful recovery after destructive tool actions;
- storage amplification and bytes copied per branch;
- application code and recovery logic required by each backend;
- rate of stale or contradictory memories after task consolidation.

## Analyze and replay Codex rollouts

Codex session JSONL records can be converted into a compact, portable workload:

```bash
.venv/bin/chronos-enterprise-knowledge \
  analyze-rollout \
  ~/.codex/sessions/YYYY/MM/DD/rollout-....jsonl \
  --output /tmp/codex-workload.jsonl
```

The analyzer extracts completed enterprise-knowledge MCP calls and
`exec_command` calls in their original order. Checkout paths and the original
working directory become `{{workspace:BRANCH}}` and `{{repo}}` placeholders,
so a fresh backend receives the same logical actions without reusing paths
from the recorded run. The trace also records normalized result digests and
marks shell output that Codex truncated. If the rollout contains an
unsupported tool such as an interactive shell continuation, the analyzer marks
the trace incomplete and replay refuses it rather than silently omitting the
action.

Replay is open-loop: it reproduces the actions chosen by Codex, but does not
rerun the language model or inspect storage-engine calls beneath MCP. Shell
commands are disabled unless the operator explicitly trusts the trace:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --state-dir /tmp/replay-state \
  --backend chronos \
  replay-rollout /tmp/codex-workload.jsonl \
  --repo-dir /path/to/chronos \
  --embedding-cache /data/redwood-knowledge/embedding-cache.sqlite \
  --allow-shell
```

When `--embedding-cache` is supplied, replay is cache-only: a missing query
embedding fails the event instead of invoking any embedding provider. The
report always shows whether normalized outputs match the recording; add
`--require-result-match` to make a mismatch fail the command.

## Cross-backend rollout benchmark

The initial workload in `workflows/real` comes from six actual Codex
sessions rather than hand-authored tool sequences. Codex creates a reliability
department and runtime-diagnostics team, onboards a team member, runs three
debugging investigations concurrently in separate branches, and answers ten
EnterpriseRAG questions. The five non-QA workflows write durable team,
onboarding, or episodic memory. The normalized traces preserve completed MCP
calls, shell commands, and file patches in their original order while removing
backend-specific checkout paths.

That workload remains a smoke trace. Full-corpus evaluation also captures
branch lifecycle and sibling-isolation, memory correction, product launch,
security assurance, customer-success, and broader EnterpriseRAG reasoning
workflows. Every evaluated trace must retain its source Codex rollout and hash;
prompt files alone are not benchmark traces.

The benchmark creates a fresh state directory for every trace, backend, and
repetition. It ingests the same prepared documents and vectors, creates the
same hierarchy, replays the same high-level actions, and records:

- per-operation and end-to-end latency;
- storage consumed after the workflow;
- normalized read-result differences;
- logical state digests before and after replay; and
- cross-backend equivalence of all branches named by the workflow.

Replay the combined real workflow against Chronos, application-managed
copy-on-write state, physical clones, and the native-component composition:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --qdrant-url http://127.0.0.1:6339 \
  --doltgres-dsn \
    postgresql://postgres:password@127.0.0.1:55439/postgres \
  --btrfs-root /mnt/enterprise-knowledge-btrfs \
  --doltgres-data-dir \
    apps/enterprise-knowledge-mcp/infra/native-branching/data/doltgres \
  --qdrant-storage-dir \
    apps/enterprise-knowledge-mcp/infra/native-branching/data/qdrant \
  benchmark-rollouts \
  apps/enterprise-knowledge-mcp/workflows/real/traces/enterprise-knowledge-real-workflow.jsonl \
  --snapshot /data/enterprise-rag-full-minilm/snapshot \
  --embedding-cache /data/redwood-knowledge/embedding-cache.sqlite \
  --output-dir /data/rollout-benchmark/run-001 \
  --repo-dir /path/to/chronos \
  --max-documents 64 \
  --repetitions 3 \
  --benchmark-backend chronos \
  --benchmark-backend app-managed \
  --benchmark-backend physical-clone \
  --benchmark-backend doltgres-qdrant-btrfs \
  --allow-shell \
  --discard-states
```

The document bound selects a deterministic prefix and always adds documents
explicitly required by a workflow. Use `--all-documents` for the complete
prepared snapshot. The state-division experiment loads the same complete
company-document and source-code snapshots into Chronos, the native
Doltgres–Qdrant–Btrfs composition, and the app-managed overlay in sequence.
For this protocol Chronos uses a pinned PostgreSQL 16 service for its shared
relational metadata and application/ChronosFS state; the comparison backends
keep their own specified relational stores.
Each backend finishes ingestion and all recorded workflows before the next
backend starts. The physical-clone baseline remains excluded at this scale
because it eagerly duplicates every inherited file and retrieval point. The
benchmark never invokes an embedding provider.
`results.json` distinguishes state-transition equivalence from native search
ranking differences. Add `--require-result-match` when result identity should
also determine benchmark success.

Run that sequential protocol with:

```bash
apps/enterprise-knowledge-mcp/scripts/run_state_division_v2_pipeline.sh
```

To validate branch operations and storage before dense-vector backfill
completes, add the global `--placeholder-zero-embeddings` option before
`benchmark-rollouts`. This mode omits dense vectors while retaining the same
Qdrant BM25 and full-text retrieval path on every backend, and records
`forced-zero` in every run. It never reads the query cache or contacts an
embedding provider. These runs validate lexical retrieval, execution,
final-state equivalence, and storage amplification, but do not measure
semantic retrieval quality.

Storage statistics include the Qdrant directory for each run's collection,
the PostgreSQL data directory for Chronos, and (when supplied) the
corresponding Doltgres database directory. The benchmark counts allocated
filesystem blocks rather than the logical lengths of sparse database files.

Generate the paper-style comparison figure and CSV summaries with:

```bash
pip install -e 'apps/enterprise-knowledge-mcp[experiment]'
.venv/bin/python \
  apps/enterprise-knowledge-mcp/workflows/real/summarize_results.py \
  /data/rollout-benchmark/run-001/results.json
```

The script refuses to summarize a run unless every replay succeeds and the
backends reach equivalent logical state.

### Concurrent merge experiments

`scripts/run_concurrent_polystore_experiments.py` runs four small, repeatable
concurrency scenarios: disjoint promotions, competing revisions with
selective promotion, recursive fan-out/fan-in, and a process crash during
publication.  A dedicated verifier process continuously cross-checks each
visible document's relational row, file bytes, and Qdrant payload while the
workers write and merge.  It records publication anomalies separately from
expected branch-local write windows, so verification is not serialized by the
Python GIL.  The driver waits for the verifier's ready signal before releasing
the workers, and records the verifier PID and observed overlap in each result.

Use a single Docker Qdrant endpoint for all backends (and the same Doltgres and
Btrfs services for the native backend):

```bash
uv run python apps/enterprise-knowledge-mcp/scripts/run_concurrent_polystore_experiments.py \
  --backend chronos --backend app-managed \
  --backend doltgres-qdrant-btrfs \
  --qdrant-url http://127.0.0.1:6333 \
  --doltgres-dsn postgresql://root@127.0.0.1:5433/knowledge \
  --scenario all --output /data/concurrent-polystore
```

The embedded Qdrant client is supported only as an explicitly requested
single-process smoke test (`--allow-local-qdrant`); it cannot be shared by the
worker, verifier, and crash-child processes.
