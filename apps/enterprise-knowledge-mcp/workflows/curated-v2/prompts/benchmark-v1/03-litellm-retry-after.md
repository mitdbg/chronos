You are Kaitlyn Nguyen, a senior platform engineer at Redwood Inference. Use
the `chronos_enterprise_knowledge` MCP tools and the mounted workspace. Do not
modify Chronos, contact GitHub, use paid model APIs, push commits, or open an
upstream pull request. Do not inspect the MCP server's backing databases
directly; access company state through MCP tools and the mounted branch
workspace only.
Use `knowledge_search` to discover company evidence and source paths. Read
source code with targeted shell commands in the mounted workspace; do not call
`knowledge_get_document` for source-code documents or load whole source files
through an MCP response.

Resolve open LiteLLM issue #34399 against the pinned company source:
https://github.com/BerriAI/litellm/issues/34399.

1. Create and mount `task/kaitlyn-nguyen/litellm-retry-after-v2` from
   `person/kaitlyn-nguyen`.
2. Search for `LiteLLM #34399`, provider throttling, retry policy, failover,
   and Redwood incident or support evidence. Fetch the issue snapshot and
   strongest internal sources.
3. Read `/code/litellm/AGENTS.md` and its referenced contributor guidance.
   Inspect `Router._time_to_sleep_before_retry` and mapped tests. Add a
   deterministic regression case with two deployments sharing one
   rate-limited upstream, then implement the smallest policy fix that preserves
   immediate failover when no upstream backoff applies.
4. Run focused local tests without paid provider calls. Re-index every changed
   source and test file. Write and index
   `/artifacts/reviews/litellm-34399.md` documenting the retry precedence,
   checks, operational consequence, and rollback.
5. Diff, merge the reviewed task into `person/kaitlyn-nguyen`, and delete the
   task branch.
6. Store the validated retry/failover contract as playbook memory on the
   personal branch with issue, code, test, and internal evidence.

Report the changed paths, checks, merge result, memory ID, and evidence.
