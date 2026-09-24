# Curated inference-company codebases

This directory adds real source code to the synthetic Redwood Inference
enterprise corpus. The repositories are pinned in `manifest.json` and serve as
shared company source state for the coding-agent workflows.

- `vllm` provides the serving runtime.
- `litellm` provides the inference gateway, routing, and administrative path.
- `langfuse` provides observability, evaluation, and operations tooling.

The Chronos corpus reader indexes text source files and exposes them under
`/code/<repository>`. It excludes Git metadata, dependency caches, generated
outputs, large lockfiles, and the separately licensed Langfuse enterprise
directories listed in the manifest. The upstream worktrees themselves remain
unchanged so their licenses and exact revisions remain available for
provenance.
