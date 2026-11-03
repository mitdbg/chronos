# EnterpriseRAG infrastructure corpus

The paper dataset combines EnterpriseRAG-Bench company documents with pinned
vLLM, LiteLLM and Langfuse source trees, public GitHub issues/PRs, and synthetic
internal maintenance records: **628,081 documents and 10,704,651 chunks**.

- `artifacts/`: retained source corpus and prepared ingestion snapshot in Git LFS.
- `paper_artifact.json`: document counts, revisions and content digests.
- `seed/`: authored records and pinned repository manifest.
- `prepare.py`: source-corpus preparation entry point.
- `artifact.py`: archive verification and restoration.

See [REPRODUCING.md](REPRODUCING.md) for download, preparation and verification
commands, and [NOTICE.md](NOTICE.md) for attribution.
