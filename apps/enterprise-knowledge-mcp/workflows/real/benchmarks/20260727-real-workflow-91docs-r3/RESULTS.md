# Real Codex workflow: backend comparison

## Method

This experiment replays 151 high-level actions captured from six real Codex
sessions: 141 enterprise-knowledge MCP calls, seven shell commands, and three
file patches. The workflow creates a department, a team, and a personal
workspace; runs three debugging investigations in separate branches; writes
six durable memories; and answers ten EnterpriseRAG questions. It adds seven
branches in total.

Every backend starts from the same 91 documents, 702 chunks, and 969,203 bytes
of document content. The set consists of a deterministic 64-document prefix
plus every document that the captured sessions directly accessed. Embeddings
come from the prepared cache; replay does not invoke an embedding provider or
language model.

The three backends expose the same knowledge interface:

- **Chronos** branches the relational, filesystem, and vector state through the
  common Chronos abstraction.
- **App-managed** stores branch-local overlays and resolves inherited data by
  walking ancestry in application code.
- **Physical clone** eagerly materializes every visible relational record,
  file, and vector when creating a branch.

The table reports the median of three fresh-state repetitions. Replay time
excludes initial corpus ingestion and final state verification. Storage is the
on-disk SQLite and Qdrant state; transient mounted checkout directories are
excluded for every backend.

## Results

| Backend | Replay (s) | Slowdown | Checkout p50 (ms) | Search p50 (ms) | Baseline (MiB) | Added (MiB) | Final (MiB) | Final / baseline |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Chronos | 20.35 | 1.00x | 62.65 | 114.21 | 22.14 | 3.01 | 25.15 | 1.136x |
| App-managed | 59.65 | 2.93x | 39.62 | 507.64 | 33.22 | 1.54 | 34.76 | 1.046x |
| Physical clone | 241.41 | 11.86x | 25,849.22 | 542.81 | 33.22 | 249.21 | 282.44 | 8.502x |

Chronos completes the captured workflow 2.93x faster than the app-managed
overlay and 11.86x faster than physical cloning. The app-managed baseline
creates a branch slightly faster than Chronos, but its application-level
ancestry resolution makes search 4.45x slower. Physical cloning is dominated
by branch creation: its median checkout takes 25.85 seconds, about 413x the
Chronos median.

The app-managed overlay has the lowest incremental storage because it stores
only local changes, but this saving comes with application-specific branch and
ancestry logic and slower reads and writes. Chronos adds 3.01 MiB across seven
branches, or 13.6% over its baseline state. Physical cloning adds 249.21 MiB,
82.9x the Chronos increment, and grows state by 8.50x.

All nine replays completed and produced identical final logical-state digests
for every branch across all three backends. Exact search-result digests are not
required because the backends use different native ranking implementations;
the open-loop trace preserves the same Codex-selected action sequence.

This is a validation-scale run over the documents exercised by the captured
tasks, not yet a corpus-scale result. The prepared 10% EnterpriseRAG snapshot
contains 51,205 documents and 246,920 chunks. A paper-facing scalability claim
should add a document-count sweep; eager cloning is expected to become much
more expensive as the inherited knowledge base grows.
