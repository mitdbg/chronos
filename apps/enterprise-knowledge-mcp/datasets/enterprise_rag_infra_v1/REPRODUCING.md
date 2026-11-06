# Reproduce the EnterpriseRAG infrastructure corpus

This directory contains the source recipe for the enterprise agent-state
experiment in the Chronos paper. It answers two separate questions:

1. Which data did the experiment use?
2. How can a reader rebuild that data without access to an author's machine?

This directory provides both the retained paper data as **Git LFS archives**
and its preparation recipe. Ordinary Git history contains small pointers,
not hundreds of thousands of raw files. The complete augmentation builder,
tests, authored records and pinned revisions remain browsable.
`paper_artifact.json` records the evaluated corpus's counts and digests.

## Download the retained paper data (Git LFS)

This is the recommended replay path: it does not depend on today's GitHub API
responses, tokenizer versions, or new embeddings. From the repository root,
with Git LFS, GNU tar and `zstd` installed:

```bash
git lfs install
git lfs pull --include='apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/artifacts/**'
python3 apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/artifact.py verify
python3 apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/artifact.py restore \
  --dataset snapshot --output /data/paper-data
```

The snapshot is restored into `/data/paper-data/document-snapshot`: 628,081
documents and 10,704,651 chunks, ready for `ingest-snapshot`. Runtime service
initialization/ingestion is still required. This SQLite file is a portable
ingestion artifact, not a claim that Chronos used SQLite instead of PostgreSQL
for runtime state. Author paths in the manifest are historical provenance;
loaders use the local snapshot directory.

To also restore source documents, pinned code trees and crawl provenance:

```bash
python3 apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/artifact.py restore \
  --dataset source --output /data/paper-data
.venv/bin/python apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/verify.py \
  /data/paper-data/generated_data_infra_v1
```

The compressed source is 2.13 GiB and the snapshot is 7.05 GiB (9.18 GiB
combined). Allow at least 30 GiB free **in addition to the archives** for both
datasets. Archives are split into at most 1 GiB LFS objects. The manifest records
ordered parts, byte counts, SHA-256s, and the uncompressed SQLite hash. Restore
checks hashes and refuses nonempty destinations, streaming parts without an
extra joined archive. Restore only trusted release archives. The source archive
excludes Git internals and separately licensed Langfuse enterprise directories,
not indexed documents. Upstream license/notice files are retained; see
[NOTICE](NOTICE.md).

`artifact.py verify --deep` also tests decompression and checks the original
SQLite hash without writing another 15 GiB file. It takes longer than checking
compressed-part hashes alone.

**Embedding caveat:** the snapshot has zero ready dense embeddings
(`embeddings_complete=false`), matching the captures' `forced-zero` mode.
BM25 text indexing is built during ingestion. The model name identifies the
chunking/embedding configuration, not evidence that dense embeddings were
generated. Do not backfill it when reproducing these measurements.

See the [incident-response guide](../../.results/incident-response-swarm-v1/REPRODUCING.md)
for released sessions, results and replay commands. Offline plot reproduction
does not require downloading this much larger dataset.

## What was constructed

The corpus starts from EnterpriseRAG-Bench's synthetic Redwood Inference
company data. We augmented it in three ways:

- We added clean checkouts of vLLM, LiteLLM, and Langfuse at the exact commits
  in `seed/codebases/manifest.json`. These supply the shared source trees used
  by the coding-agent workflows.
- We crawled public GitHub issues, pull requests, releases, comments, reviews,
  and relevant history for those projects, then reconstructed records as of
  `2026-07-27T23:59:59Z`. This produced 97,336 normalized public GitHub
  documents. The crawl covers the repositories, not only the issues named in
  the workflow prompts.
- We added 200 Redwood maintenance records across Slack, Linear, Gmail,
  GitHub, Confluence, Google Drive, Jira, and Fireflies. They connect the public
  upstream work to realistic internal decisions, incidents, reviews, and
  follow-up work. The evaluated artifact used the builder's deterministic
  `template-fallback` mode; it did not use an LLM to author these records.

The resulting source corpus contains 609,498 structured enterprise records
plus 18,583 indexable source-code files: 628,081 documents and 10,704,651
chunks. Initializing the experiment hierarchy adds one company-knowledge root
record, which explains the 628,082-document and 10,704,652-chunk counts
reported for the ingested main branch.

The code repositories are source inputs, not vendored Chronos dependencies.
The corpus reader excludes Git internals, dependency caches, build products,
large lockfiles, and separately licensed Langfuse enterprise directories. See
the checked-in indexing policy in `seed/codebases/manifest.json` and its
implementation in `../../src/chronos_enterprise_knowledge/enterprise_rag.py`.

## Rebuild the evaluated corpus

Requirements:

- Git, Python 3.11 or newer, and GNU `cp`;
- an authenticated GitHub token in `GITHUB_TOKEN` or `GH_TOKEN`;
- approximately 35 GiB of free disk space for the base checkout, crawl
  provenance, and derivative (copy-on-write filesystems can use less);
- enough GitHub API quota for a large, resumable crawl. The paper artifact made
  12,237 requests and archived about 4.23 GB of API responses.

From the Chronos repository root:

```bash
export GITHUB_TOKEN=github_pat_...
python apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/prepare.py \
  --workspace /data/chronos-enterprise-rag
```

This command:

1. checks out EnterpriseRAG-Bench at
   `d36685e273713975ee20299bbf1ab64165575b3c`;
2. adds the three pinned source trees and the authored LiteLLM issue briefs to
   its `generated_data/codebases` directory;
3. copies the base corpus into a derivative without modifying the base;
4. crawls and archives the public GitHub history;
5. normalizes records at the fixed cutoff, generates the 200 deterministic
   internal records, rebuilds indexes, and validates provenance and references.

The result is:

```text
/data/chronos-enterprise-rag/EnterpriseRAG-Bench/generated_data_infra_v1
```

All phases are resumable. Re-run the same command after interruption, or run an
individual phase with `--phase init`, `crawl`, `normalize`,
`generate-internal`, or `validate`. `--phase source` stops after preparing the
pinned base and source repositories. An existing EnterpriseRAG-Bench checkout
can be supplied with `--enterprise-rag-checkout`; the entry point verifies its
commit before changing `generated_data/codebases`.

The optional `--llm-generated-internal` flag uses EnterpriseRAG-Bench's model
configuration instead of deterministic records. That creates a different
corpus and is not the mode used by the paper.

## Prepare the Chronos ingestion snapshot

Set the corpus explicitly when running the experiment driver:

```bash
export ENTERPRISE_CORPUS=/data/chronos-enterprise-rag/EnterpriseRAG-Bench/generated_data_infra_v1
apps/enterprise-knowledge-mcp/scripts/run_curated_v2_pipeline.sh
```

For dataset preparation alone, the same application command used by the
pipeline is:

```bash
.venv/bin/chronos-enterprise-knowledge \
  --dimensions 384 \
  --placeholder-zero-embeddings \
  prepare-snapshot "$ENTERPRISE_CORPUS" /data/enterprise-rag-snapshot \
  --sample-fraction 1 \
  --sample-seed chronos-enterprise-infra-v1 \
  --batch-size 256 \
  --chunk-workers 2 \
  --progress-every 1000
```

Some sequential experiment configurations separate company records and code
into reusable snapshots. The retained incident-response snapshot instead holds
the complete corpus in one artifact; do not apply the older pipeline's split
or reduced selection when reproducing it.

## Verify the corpus

Use the retained LFS archives for the paper input; a fresh GitHub crawl can
change as public records change. Crawl provenance is included in the source
archive's `provenance/` directory.

To compare the indexed records and selected source paths—not incidental Git
pack files, absolute paths, or generation timestamps—with the paper artifact:

```bash
.venv/bin/python \
  apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1/verify.py \
  "$ENTERPRISE_CORPUS"
```

## Test the builder

The unit tests use only temporary fixtures and do not download the corpus:

```bash
python -m unittest discover \
  -s apps/enterprise-knowledge-mcp/datasets/enterprise_rag_infra_v1 \
  -p 'test_*.py'
```
