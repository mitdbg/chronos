# Chronos

Chronos gives application state named, writable branches. Fork a dataset, run an
experiment, inspect its changes, and merge or discard the result. Forks change
interval metadata without copying existing records.

This repository contains the Python library and C++ implementation for SQLite,
PostgreSQL, DuckDB, ChronosFS, Qdrant, and the S3 gateway. The separate PostgreSQL
source tree implements the same versioning approach inside the database engine.

Version **0.2.0a1** is an experimental release candidate prepared locally.
These instructions do not assume a public package upload.

## Install

The base source build supports SQLite and PostgreSQL. On Ubuntu:

```sh
sudo apt-get install build-essential python3-dev libsqlite3-dev libpq-dev libboost-dev
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./packages/chronos-core
```

Pip installs Python build dependencies. The build downloads pinned C++ dependencies
and needs internet access. A compatible wheel avoids compilation.
See [installation](docs/installation.md) for optional filesystem, DuckDB, and S3 builds.

## Branch some data

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:")
try:
    ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, quantity INTEGER)")
    ctx.db.execute("INSERT INTO items VALUES (1, 10)")
    ctx.db.commit()
    ctx.register_table("items", ["id"])
    ctx.create_branch("trial", from_branch="main")
    with ctx.checkout("trial") as trial:
        trial.execute("UPDATE items SET quantity = :n WHERE id = :id",
                      {"n": 7, "id": 1})
        assert trial.query("SELECT quantity FROM items") == [{"quantity": 7}]
    with ctx.checkout("main") as main:
        assert main.query("SELECT quantity FROM items") == [{"quantity": 10}]
    ctx.merge_apply("trial", "main", policy="snapshot_isolation")
    ctx.delete_branch("trial")
finally:
    ctx.close()
```

Registration imports existing rows into a physical version table. After registration,
access managed data through branch sessions. Direct access to the original table
does not participate in branching.

## Learn and integrate

- [Documentation](docs/README.md) and [existing applications](docs/integration.md).
- [Software development](docs/tutorials/software-development.md): isolate code and
  data, reproduce a failure, test a fix, and merge both.
- [verl database sandbox](docs/tutorials/rl-data-sandbox.md): one isolated
  database branch per multi-turn rollout, final-state rewards, and cleanup.
- [Atomic multi-store merge](docs/multi-store-branching.md).
- [Implementation](docs/bolt-on-branching.md) and [compatibility](docs/compatibility.md).

Single-store merge uses `merge_apply`. With shared metadata across stores, use
`merge_atomic_preview` and `merge_atomic`. Independent metadata stores have
separate commits and no atomic cross-store visibility guarantee.

See [CONTRIBUTING.md](CONTRIBUTING.md) for tests and [RELEASING.md](RELEASING.md)
for local artifacts. Reference backends and small branching benchmarks remain for
testing. The old agent harness, transaction shims, and framework wrappers are removed.

Chronos isolates managed state. Use a container or another execution sandbox
for untrusted code and restrict external services during speculative work.
