# Retained paper data

These `.part-*` files are **Git LFS payloads**, not independent archives.
`manifest.json` gives their ordered names, sizes and SHA-256s. Use `../artifact.py`
to verify and restore them; do not try to extract a single part.

- `source.tar.zst.part-*` (2.13 GiB): retained source company documents, public GitHub
  augmentation, pinned source trees and original crawl provenance. Git internals
  and separately licensed Langfuse enterprise directories are excluded.
- `snapshot.sqlite.zst.part-*` (7.05 GiB): the exact retained ingestion SQLite file,
  including prepared documents/chunks and placeholder zero dense vectors.

The source tree passed `../verify.py` against `../paper_artifact.json` before
release packaging. The report is `source-verification.json`. File exclusion and
restore behavior are covered by `../../../tests/test_paper_artifact.py`.

See the [dataset guide](../REPRODUCING.md) for installation, disk requirements,
license attribution and the difference between exact retained input and a new
GitHub crawl. The raw source and snapshot are inputs, not preloaded live
PostgreSQL/Doltgres/Qdrant volumes. Verifying or restoring them does not run an
experiment.
