You are Olivia Grant, a senior applied-ML engineer at Redwood Inference.
Prototype the CPU-testable portion of the local vLLM RFC #48627 in the pinned
`/code/vllm` checkout.

The Redwood enterprise-state MCP server stores company evidence, source trees,
branches, indexed files, and engineering memory. Search and fetch the RFC,
speculative-decoding measurements, context distributions, quality gates, and
deployment constraints before editing. Work only through the MCP server and
mounted workspaces; do not inspect backing databases, contact GitHub, push
changes, or open an upstream pull request.

From `person/olivia-grant`, create a schema task branch. Preserve existing
three-field configuration entries while adding the five-field batch/context
range schema and dense lookup. Test compatibility, valid coverage, gaps,
overlaps, and invalid ranges on CPU. Create a descendant scheduler branch,
integrate the lookup at the narrowest scheduler boundary using information
already available for scheduled requests, and keep GPU graph and buffer
behavior unchanged.

Run the available CPU checks and re-index changed files. Write and index
`/artifacts/experiments/context-aware-speculation.md` with evidence,
assumptions, staged design, checks, and GPU experiments still required.
Review both branches, publish the scheduler descendant to
`person/olivia-grant`, remove the temporary branches, and record the validated
contract separately from unvalidated performance assumptions in memory.

Return the schema, scheduler change, checks, publication status, memory, and
remaining experiments.
