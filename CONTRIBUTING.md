# Contributing

Chronos is experimental. Reproductions should identify the Python package and
compiled extension being used, the database engine, and whether the application
shares metadata across stores. Include a small program with assertions when
reporting a visibility or transaction failure.

## Build and test

Follow [installation](docs/installation.md), then install test dependencies in
the same virtual environment. A full-feature build is required for the complete
suite.

```sh
python -m pip install './packages/chronos-core[test]' matplotlib pytest-timeout \
  -Ccmake.define.CHRONOS_WITH_FILESYSTEM=ON \
  -Ccmake.define.CHRONOS_WITH_DUCKDB=ON \
  -Ccmake.define.CHRONOS_WITH_S3=ON
python -c 'import chronos_core._native_interval as n; print(n.__file__)'
python -m pytest tests -q --timeout=120
python examples/run_all.py
```

PostgreSQL tests reset the `public` schema. Set `CHRONOS_POSTGRES_DSN` only to a
disposable test database. Without it, the current test helper starts its own
`postgres:16-alpine` Docker container on port 55434. Do not point that helper at
an application container with the same name. Tests that require unavailable
services or mount permissions must be reported as skips, not passes.

The [verl tests](examples/verl/README.md) run separately in a verl environment.
Set `CHRONOS_VERL_TEST_POSTGRES_DSN` to another disposable database to include
their PostgreSQL cases. Do not run suites that reset the same database in parallel.

Changes to interval allocation need tests with siblings, descendants,
checkpoints, deletion, and concurrent connections. Changes to DML need isolation
and rollback tests, including an existing session used after a fork. Keep the
reference backends available for comparisons, but identify failures by backend
instead of treating them as evidence about another implementation.

Keep tutorials executable and preserve the design documents. Correct a stale
claim at its source rather than replacing a technical explanation with a shorter
summary. Do not add generated traces, database files, or build directories to Git.
