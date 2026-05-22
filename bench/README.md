# Branching Backend Benchmarks

This folder contains benchmarks for the bolt-on branching physical backends.

Run a quick smoke benchmark:

```bash
bench/run_branching_experiments.sh postgres --quick
```

By default, `run_branching_experiments.sh` runs PostgreSQL. If
`JANUS_BRANCH_POSTGRES_DSN` or `JANUS_BRANCH_DATABASE_URL` is not set, it starts
a temporary `postgres:16-alpine` Docker container, waits for readiness, runs the
benchmark, and stops the container.

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

For each dataset size and depth, the benchmark first loads the data, registers
tables, and creates indexes outside the timed section. It then measures branch
creation while building one branch chain of the requested depth. Reads,
creation while building one branch chain of the requested depth. After each
branch is created, the benchmark applies branch-local updates, inserts, and
deletes before creating the next child branch. Reads, updates, inserts, and
deletes are then measured on the terminal branch of that mutated chain. Finally,
branch deletion is measured by deleting the chain in reverse.

The benchmark writes:

- `results.csv`
- `results.json`
- `summary.png`
- one matplotlib PNG chart per benchmark metric
- `README.md` linking the generated plots

By default, output goes under `.benchmarks/branching-<timestamp>/`.
