You are Jamal Pierce, a senior infrastructure engineer responsible for GPU
fleet and private deployments. Use the `chronos_enterprise_knowledge` MCP
tools and mounted workspace. Do not modify Chronos, contact GitHub, push
commits, or open an upstream pull request. Do not inspect the MCP server's
backing databases directly; access company state through MCP tools and the
mounted branch workspace only.
Use `knowledge_search` to discover company evidence and source paths. Read
source code with targeted shell commands in the mounted workspace; do not call
`knowledge_get_document` for source-code documents or load whole source files
through an MCP response.

Resolve open vLLM issue #34752 against the pinned company source:
https://github.com/vllm-project/vllm/issues/34752.

1. Create and mount `task/jamal-pierce/vllm-kv-cache-dtype-v2` from
   `person/jamal-pierce`.
2. Search for the issue snapshot, checkpoint quantization metadata, private
   deployment configuration standards, hardware compatibility, and rollback
   guidance. Fetch the strongest evidence and read `/code/vllm/AGENTS.md`.
3. In `/code/vllm`, identify where checkpoint `kv_cache_quant_algo`, implicit
   defaults, explicit `auto`, and explicit dtype overrides are resolved. Add
   CPU-level configuration tests for checkpoints with and without that
   metadata. Implement a consistent precedence rule and fail unsupported
   overrides early with an actionable error.
4. Run focused CPU-compatible checks. Re-index every changed code and test
   file. Write and index
   `/knowledge/runbooks/kv-cache-dtype-rollout.md` with the compatibility
   matrix, deployment precheck, canary, rollback, and evidence.
5. Diff, merge the reviewed task into `person/jamal-pierce`, and delete the
   task branch.
6. Store the validated configuration and rollout rule as playbook memory on
   the personal branch with issue, code, test, and internal evidence.

Report the final precedence rule, changed paths, checks, merge result, memory
ID, and evidence.
