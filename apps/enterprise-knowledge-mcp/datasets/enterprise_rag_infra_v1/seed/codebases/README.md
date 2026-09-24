# Curated inference-company codebases

This directory adds real source code to Redwood Inference's synthetic
enterprise corpus. The repositories are shallow checkouts pinned in
`manifest.json`; they are shared at the company branch and inherited by
department, team, person, and task branches.

The selection covers three complementary parts of an inference platform:

- `vllm` provides the serving runtime.
- `litellm` provides the inference gateway, routing, and administrative path.
- `langfuse` provides an operations and observability frontend plus its
  supporting services.

Chronos indexes text source files and places them under `/code/<repository>` in
each mounted workspace. Dependency caches, generated outputs, large lockfiles,
Git metadata, and separately licensed Langfuse enterprise directories are not
indexed. The upstream checkouts remain unchanged so their licenses, contributor
guides, and exact pinned revisions are available for provenance.
