# EnterpriseRAG infrastructure corpus

This directory contains the source recipe for the enterprise agent-state
experiment in the Chronos paper. It answers two separate questions:

1. Which data did the experiment use?
2. How can a reader rebuild that data without access to an author's machine?

The generated corpus is about 11 GiB and contains hundreds of thousands of
files, so it is intentionally not duplicated in the Chronos Git repository.
Instead, this directory checks in the complete augmentation builder, its tests,
the small authored source records, all repository revisions, and a one-command
preparation entry point. `paper_artifact.json` records the observed counts and
digests of the artifact used to capture the paper workloads.

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

The experiment separated the structured company records and the `codebase`
connector into reusable snapshots before loading both into each backend. The
pipeline script is the authoritative end-to-end command for that process.

## Provenance and reproducibility limits

`build_infra_dataset.py` stores compressed GitHub responses, response headers,
ETags, checkpoints, request metrics, a base-file lineage manifest, normalized
records, and validation output below the derivative's `provenance/` directory.
Credentials are read only from the environment and are never archived.

The recipe reproduces the selection rules, pinned code, cutoff, deterministic
internal records, and validation contract. A new public crawl is not guaranteed
to be byte-for-byte identical to the paper artifact: GitHub records can be
deleted or become unavailable after the cutoff, and generation timestamps and
Git pack layouts can differ. Compare a rebuild with `paper_artifact.json`; for
archival byte identity, the original compressed crawl responses must also be
published as a separate data artifact.

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
