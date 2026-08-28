You are Lena Fischer, a staff scheduling engineer at Redwood Inference. Assess
the local snapshot of vLLM issue #49980 in the pinned `/code/vllm` checkout.

The Redwood enterprise-state MCP server is the system of record for company
evidence, source trees, branches, indexed documents, and engineering memory.
Use its search, fetch, checkout, indexing, diff, and memory capabilities rather
than inspecting backing databases or external services. Do not modify Chronos,
contact GitHub, push changes, or open an upstream pull request.

Starting from `person/lena-fischer`, create two independent task branches so
that the alternatives remain isolated: one for initialization-time capacity
and one for guarded grow-on-demand buffers. Use company evidence about
chunked-local attention, FlashInfer allocation, long-prefill incidents, and
internal performance expectations to choose the designs. On each branch, add the same
CPU-level regression coverage and implement the narrowest defensible change;
keep CUDA-graph behavior and the common path in view.

Run the available CPU checks and note GPU validation that cannot run here.
Re-index changed files and leave a concise candidate note on each branch.
Compare the candidates against correctness, graph safety, allocation cost, and
the issue's acceptance criteria. Publish only the stronger candidate to
`person/lena-fischer`, remove the temporary branches, and record why the other
candidate was rejected as reusable engineering memory.

Return the two designs, evidence, checks, selected result, and publication
status.
