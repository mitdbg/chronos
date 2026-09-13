# Local experimental candidate, 2026-09-09

This candidate remains local. No repository was pushed, no tag or public release
was created, and no Docker image was uploaded. The maintained branching documents
and PDFs are preserved. The Markdown documentation was reviewed against the
0.2.0a1 interfaces on 2026-09-12. Four obsolete pre-release documents describing
the removed TAR, TMCP, and LangChain implementation were deleted rather than
published as current guidance.

## Source changes

The old agent harness, framework wrappers, and separate transaction package are
removed from the working tree. The branching library, reference backends, and
branching tests and enterprise knowledge benchmark app remain. Installation now supports a relational-only build without
FUSE, DuckDB, or S3 dependencies. Optional compiled features are explicit, and
the MCP dependency is restricted to its supported 1.x API.

The software development tutorial tests a code-and-data fix and accepts it with
an atomic shared-metadata merge. The RL tutorial uses verl's actual agent loop
and tool interfaces. Each rollout owns a database branch across tool calls and
is scored from its final database state before cleanup.

Testing led to four core corrections. Repeated checkpoint forks now reserve
disjoint intervals without moving the checkpoint's read point. Cached sessions
recognize a transaction completed through their shared store. PostgreSQL point
updates recheck a logical key when a concurrent sibling replaces its inherited
physical tuple. Session metadata initialization avoids unnecessary index DDL
that could wait on a stopped client's heartbeat transaction.

## Artifacts

The CPython 3.10 Linux x86-64 wheels and package source distributions are in
`/tmp/chronos-release-minimal` and `/tmp/chronos-release-full`. The full directory
also contains a source-tree archive with the tutorials, without Git history.
That archive predates restoration of the enterprise knowledge benchmark app;
it is not a complete archive of the current source tree.
The earlier CPython 3.14 wheel is an intermediate build, not the current candidate.
Raw wheels still depend on compatible system libraries; they are not repaired
manylinux distributions.

The PostgreSQL image is available locally as
`chronos-postgres:experimental-local`. Its image ID is
`sha256:47829d83ebca2e544c56ff8263df43b815890b31e1ac32fb2f3f04f05980a4ed`.
Only its Docker documentation changed during this preparation; the engine was
built from revision `25aa05805457f6630933538f2779d87858664512`.

Chronos was built from an uncommitted working tree based on
`a0ecf2e87e0b82335e724041cee0fbde8fca768c`.
Do not treat that commit alone as the source of these wheels. The candidate
includes source changes that predated this cleanup.

## Test evidence

The complete library run before the last startup-lock correction reported
659 passes, 94 skips, and two failures. One was the stopped-client initialization
problem subsequently fixed; the other is the Qdrant allocation-capacity case
below. The log is `/tmp/chronos-release-pytest-final.log`.

The verl integration passed ten consecutive runs of six cases across SQLite and
PostgreSQL. It uses upstream verl revision
`1252cc71aa5bd82e5604322064d69bfe6454c660`. These are CPU integration tests of the
real agent loop and tool dispatcher with scripted model generation. They do not
validate GPU training, distributed Ray execution, or learning quality. Logs are
`/tmp/chronos-verl-repeat.log` and `/tmp/chronos-verl-final.log`.

The relational-only artifact passed 30 checks with one disabled-driver skip;
three PostgreSQL cases were excluded from that specific command. The full
artifact passed all seven executable examples, including the optional FUSE
control example. The locally built PostgreSQL image passed its initialization,
branch isolation, schema change, metadata hiding, and restart-persistence smoke
test. This was not a new run of the full upstream PostgreSQL suite.

During the 2026-09-12 documentation review, the six non-FUSE examples passed.
The FUSE control example passed when run alone but failed its data-read assertion
when run last through `examples/run_all.py`. Treat the aggregate example run as
unresolved until this ordering-sensitive failure is fixed.

Focused regression tests after the last correction passed 48 cases with two
disabled-feature skips, including the deterministic initialization-lock test and
the stopped-process recovery test. Results are recorded in
`/tmp/chronos-release-final-regressions.log`. Test commands and disposable-database
requirements are in [CONTRIBUTING.md](CONTRIBUTING.md).

## Remaining release work

`test_qdrant_store_nested_branches_preserve_visibility` still exhausts the
default interval domain before completing its unhinted 23-level chain. It has
not been disabled or changed to conceal that result. The Qdrant adapter does not
currently expose the relational context's fanout hint. The finite-depth behavior
and adapter API need to be resolved together; this candidate is not an all-green
release.

Gitleaks 8.30.1 scanned all locally reachable history in the non-shallow clone,
reporting 71 scanned commits and approximately 268 MB of content. Git lists 72
reachable commits. It found 359 generic-token matches across 95 historical
experiment-trace files, all beneath `apps/enterprise-knowledge-mcp`. These matches
are not confirmed credentials. Their redacted report is
`/tmp/chronos-release-audit/history.json`. They need review, as do historical
datasets and traces that secret scanning cannot classify. Git history is
unchanged.

Before public distribution, complete dependency-license and binary-portability
review, test clean-container wheel installation, and resolve the retained
history. See [RELEASING.md](RELEASING.md).
