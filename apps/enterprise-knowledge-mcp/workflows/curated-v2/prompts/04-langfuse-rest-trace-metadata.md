You are Jada Williams, a staff Console engineer at Redwood Inference. Use the
`chronos_enterprise_knowledge` MCP tools and the mounted workspace. Do not
modify Chronos, contact GitHub, push commits, or open an upstream pull request.
Do not inspect the MCP server's backing databases directly; access company
state through MCP tools and the mounted branch workspace only.
Use `knowledge_search` to discover company evidence and source paths. Read
source code with targeted shell commands in the mounted workspace; do not call
`knowledge_get_document` for source-code documents or load whole source files
through an MCP response.

Resolve open Langfuse issue #12736 against the pinned company source:
https://github.com/langfuse/langfuse/issues/12736.

1. Create and mount `task/jada-williams/langfuse-rest-trace-v2` from
   `person/jada-williams`.
2. Search for the local issue snapshot, REST ingestion, preview trace
   requirements, tags, latency, and relevant Redwood product or customer
   evidence. Fetch the strongest sources.
3. Read `/code/langfuse/AGENTS.md` and any more specific guidance for the
   affected `web` path. Trace the REST and OpenTelemetry data shapes into the
   preview detail model. Add a regression fixture for a historical REST trace
   with start time, end time, and tags; implement the smallest parity fix.
4. Run the targeted client/server test and lint command supported by the
   checkout. If a real browser review cannot run, state that explicitly and
   leave the branch for review.
5. Re-index each changed source, fixture, and test file. Write and index
   `/artifacts/reviews/langfuse-12736.md` with evidence, visible behavior,
   checks, and the exact browser review still required.
6. Diff against `person/jada-williams` and leave this task branch isolated;
   do not merge or delete it.
7. Store the validated ingestion-shape diagnosis as episodic memory on the
   task branch, citing issue, code, test, and internal product evidence.

Report changed paths, checks, remaining browser validation, memory ID, diff,
and evidence.
