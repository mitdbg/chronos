# chronos-core

Experimental interval-based branching for application state. The base build
supports SQLite and PostgreSQL. Optional C++ features add ChronosFS, DuckDB, and
an S3 gateway; Qdrant is an optional Python dependency.

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:")
try:
    ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)")
    ctx.db.commit()
    ctx.register_table("items", ["id"])
    ctx.create_branch("trial")
    with ctx.checkout("trial") as session:
        session.upsert_rows("items", [{"id": 1, "value": "candidate"}])
    ctx.merge_apply("trial", "main", policy="snapshot_isolation")
finally:
    ctx.close()
```

Source builds require C++17, Python/SQLite/libpq headers, Boost headers, and make.
Optional builds use `-Ccmake.define.CHRONOS_WITH_FILESYSTEM=ON`,
`-Ccmake.define.CHRONOS_WITH_DUCKDB=ON`, and
`-Ccmake.define.CHRONOS_WITH_S3=ON`. ChronosFS requires Linux and libfuse3.
Python extras do not change compiled capabilities.

See the repository's docs/installation.md, docs/integration.md, and docs/tutorials.
This is a Python library, not a PostgreSQL-wire proxy. Managed access must pass
through branch sessions. Version 0.2.0a1 is an experimental release candidate.
