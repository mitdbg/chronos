# Branching Backend Benchmarks

This folder contains benchmarks for the bolt-on branching physical backends and
the native Doltgres branching baseline.

Run a quick smoke benchmark:

```bash
bench/run_branching_experiments.sh postgres --quick
```

Run the branch-local schema-change benchmark:

```bash
bench/run_schema_branching_experiments.sh postgres-doltgres --quick
```

Run the single-threaded branching-transaction benchmark:

```bash
bench/run_branching_transaction_experiments.sh postgres-doltgres --quick
```

The branching-transaction benchmark compares three ways to execute one logical
agent transaction:

- Chronos interval branch transaction on PostgreSQL: fork from `main`, run
  branch-local SQL, and merge clean row-level changes back to `main`.
- Native Doltgres branch transaction: create a Doltgres branch, run SQL, commit
  the branch, and merge it into `main`.
- Plain PostgreSQL transaction: run the same SQL directly in one database
  transaction. This is the upper-bound reference for a non-branching store.

The benchmark is intentionally single-threaded. Each branch transaction forks
from the current `main` state and then merges back, so `main` maintains the same
serial order as native transactions. The default workload is YCSB-like: each
transaction runs point reads and primary-key point updates. It avoids range DML
so the benchmark measures branch transaction overhead rather than a backend's
set-based range update implementation. Chronos uses `interval_child_width=2` by
default so each temporary branch consumes a fixed amount of interval space
instead of geometrically shrinking `main` through percentage allocation. Results
are streamed to `branching_transaction_details.csv` and
`branching_transaction_summary.csv`; figures are written after all requested
configs finish. Passing `--delete-branches` also deletes each Chronos and
Doltgres branch after merge and includes `branch_delete` latency.
The cross-backend `merge_apply` phase includes whatever validation the backend
requires to commit a branch transaction: Chronos performs row-level merge
validation inside `merge_apply`, while Doltgres performs its native merge logic.

The schema benchmark compares Chronos interval, Chronos copy, and native
Doltgres branches for `ALTER TABLE products ADD COLUMN ...` on a child branch.
It runs both `ADD COLUMN bench_no_default INTEGER` and
`ADD COLUMN bench_default INTEGER DEFAULT 7` so engines with instant/default
DDL can be compared with engines that copy or rewrite branch-local storage.
Unsupported cases are recorded in `results.csv` with `status=unsupported`
instead of aborting the run.

The schema benchmark keeps `first_point_read_new_column` as the cold first read
after the schema change. Before the later steady-state phases, it runs
`--warmup-ops` unmeasured operations on the same branch and schema state. The
default wrapper passes `--warmup-ops 1`, warming point read, range read, count,
update, and insert phases without timing the warmup operation.

By default, `run_branching_experiments.sh` runs PostgreSQL. If
`CHRONOS_BRANCH_POSTGRES_DSN` or `CHRONOS_BRANCH_DATABASE_URL` is not set, it starts
a temporary `postgres:16-alpine` Docker container, waits for readiness, runs the
benchmark, and stops the container.

When the runner starts PostgreSQL and Doltgres containers, it uses the same
resource budget for both:

```bash
CHRONOS_BENCH_DB_MEMORY=10g \
CHRONOS_BENCH_DB_BUFFER=5g \
bench/run_branching_experiments.sh all
```

`CHRONOS_BENCH_DB_MEMORY` is passed as the Docker memory limit for both
containers. `CHRONOS_BENCH_DB_BUFFER` is passed to PostgreSQL as
`shared_buffers` and to Doltgres as `GOMEMLIMIT`. PostgreSQL also gets
`--shm-size`, defaulting to `CHRONOS_BENCH_DB_MEMORY`, because large
`shared_buffers` values require a larger Docker `/dev/shm` than the default
64MB. The runner accepts shorthand values such as `5g` and normalizes them to
PostgreSQL's expected unit spelling such as `5GB`. Doltgres does not expose a
PostgreSQL-style page-buffer setting, so the benchmark constrains its Go heap
with the same buffer budget and constrains both containers with the same total
memory cap.

Run the broader default benchmark:

```bash
bench/run_branching_experiments.sh
```

Run SQLite instead:

```bash
bench/run_branching_experiments.sh sqlite
```

Run both SQLite and PostgreSQL:

```bash
bench/run_branching_experiments.sh both
```

Run the native Doltgres baseline:

```bash
bench/run_branching_experiments.sh doltgres --backends doltgres
```

Run PostgreSQL and Doltgres in one command:

```bash
bench/run_branching_experiments.sh postgres-doltgres --backends copy,interval,doltgres
```

`all` is an alias for `postgres-doltgres`. SQLite only runs when you ask for
`sqlite` or `both`.

The default benchmark runs two separate branch shapes. It does not run
depth-by-width combinations.

For each dataset size and depth, the depth benchmark first loads the data,
registers tables, and creates indexes outside the timed section. It then
measures branch creation while building one branch chain of the requested depth.
After each branch is created, the benchmark applies branch-local updates,
inserts, and deletes before creating the next child branch. Reads, updates,
inserts, and deletes are then measured on the terminal branch of that mutated
chain.

For each dataset size and width, the width benchmark loads a fresh copy of the
same data, creates all child branches from `main`, applies the same mutation
pattern to each child branch, and measures reads, updates, inserts, and deletes
across every child branch. Branch deletion is measured by deleting the fan-out
branches in reverse.

Read metrics include point reads and primary-key range reads. Point reads,
updates, and deletes use seeded random primary keys so backends see the same
workload for a given dataset, branch shape, and branch id. `--read-ops` controls
point reads and optional join aggregates. `--range-read-ops` controls range
scans independently and defaults to `100` in the runner.

The copy backend is capped at depth and width `8` in the benchmark. Larger
requested depth or width values are skipped for copy because full-table copy
branching is only a correctness baseline and does not scale to larger branch
topologies.

The Doltgres baseline uses Doltgres' native branch functions directly. It loads
the same relational schema, commits the initial dataset, creates branches with
`dolt_checkout('-b', ...)`, commits setup mutations between depth levels so
children inherit parent state, and measures normal SQL reads/writes on the
checked-out terminal branch.

The default runner uses `--warmup-ops 100`. Write metrics are not warmed with
write operations because that would mutate the measured branch state.

The default runner also uses `--post-branch-warmup auto`. This runs after branch
creation and setup mutations, but before timed reads and writes. On PostgreSQL
Chronos backends, the benchmark tries `pg_prewarm` for the measured physical
tables and indexes and falls back to sequential scans if the extension is not
available. On Doltgres, the benchmark checks out each measured branch and runs
sequential scans over the logical benchmark tables. Use `--post-branch-warmup
off` to measure cold post-branch access instead.

The benchmark writes:

- `results.csv`
- `results.json`
- `summary.png`
- depth-prefixed and width-prefixed matplotlib PNG charts per benchmark metric
- `README.md` linking the generated plots

By default, output goes under `.benchmarks/branching-<timestamp>/`. Multi-engine
runs put each engine's raw output in a subdirectory such as
`postgres-branching/` or `doltgres-branching/`, then write merged top-level
`results.csv`, `results.json`, and plots in the same run directory.
