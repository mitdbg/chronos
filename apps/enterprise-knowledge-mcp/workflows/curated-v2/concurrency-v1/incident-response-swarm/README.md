# Incident-response swarm

This is one concurrency workflow, rather than another member of the sixteen
sequential replay workflows.  Independent on-call agents investigate separate
production reports and publish reviewed incident packages to the shared
`team/site-reliability` branch.

Each package contains a searchable incident report and durable operational
memory.  The task branch may also contain scratch notes, which remain private.
The worker traces are captured from Codex sessions using the prompt template
and the incident inputs in this directory.  The replay driver can run the
captured sessions with any worker count from one through 128.  Counts larger
than the number of captured inputs use an explicitly namespaced replay of an
input; the report records that reuse so it is not confused with a run using
distinct incidents.

The driver starts one operating-system process per worker.  All workers for a
trial connect to the same prepared relational, filesystem, and Qdrant state;
only their task branches are private.  The replay can reproduce the
model-response delays recorded with each trace, but does not inject publication
delays.  A separate process runs a busy-loop verifier and checks
that every published report has a matching relational row, file, and vector
result, while allowing the complete package to be absent before publication.

The primary comparison is Chronos versus native branching.  The runner reports
worker overlap, replay failures, publication violations, and final bundle
completeness for each backend.

To measure the conventional coarse-grained coordination alternative, run the
native backend with `--native-big-lock`.  This adds one process-shared
exclusive lock around each complete replayed agent execution, including its
recorded model delays and every state access; Chronos
and the unlocked native run are unchanged.  The result is labeled
`native-branching-lock` so it cannot be confused with native branching without
coordination.

For a stress run beyond the eight distinct captured incidents, explicitly
enable namespaced input reuse:

```bash
uv run python apps/enterprise-knowledge-mcp/scripts/run_incident_response_swarm.py \
  --traces-dir apps/enterprise-knowledge-mcp/workflows/curated-v2/traces/incident-response-swarm-v1 \
  --inputs apps/enterprise-knowledge-mcp/workflows/curated-v2/concurrency-v1/incident-response-swarm/incident-inputs.jsonl \
  --state-dir chronos=/prepared/chronos \
  --state-dir doltgres-qdrant-btrfs=/prepared/native \
  --qdrant-url http://127.0.0.1:6333 \
  --workers 128 --allow-trace-reuse \
  --dimensions 384 --repo-dir /home/ubuntu/TAR-OS/chronos \
  --output .enterprise-knowledge/incident-response-swarm-v1/workers-128
```

Use a fresh prepared state for each worker-count trial; the shared team branch
retains accepted incident packages by design.
