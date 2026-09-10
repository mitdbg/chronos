# Integrating Chronos

Use ChronosBranchContext for one relational store. Keep it open while its sessions
are in use. Reuse a checked-out session for named-parameter queries and short
database transactions.

## Adopt existing data

Provision a PostgreSQL test database, connect using its URL, and register each
managed table with its stable logical key. Registration copies existing data into
an interval table. It does not make the original table transparently branch-aware.

Pause writers during adoption and route subsequent access through branch sessions.
Direct ORM or psycopg queries against original tables bypass Chronos.

```python
import os
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect(os.environ["APP_DATABASE_URL"])
try:
    ctx.register_table("items", ["id"])  # existing table
    ctx.create_branch("trial")
    with ctx.checkout("trial") as session:
        with session.transaction():
            session.execute("UPDATE items SET quantity = :n WHERE id = :id",
                            {"n": 5, "id": 1})
finally:
    ctx.close()
```

Give concurrent workers separate contexts/connections; do not share mutable
sessions across threads. PostgreSQL is preferable for concurrent writers.
SQLite serializes writes. Stop workers and close sessions before contexts.

## Client compatibility

BranchSession is not a DB-API cursor or SQLAlchemy engine. Queries materialize
lists of dictionaries and DML supports a defined subset. There is no generic
SQLAlchemy/Django adapter or PostgreSQL-wire proxy. A future adapter must preserve
parameters, transactions, row counts, errors, and reconnect behavior. A proxy must
also implement authentication, protocol messages, types, prepared statements,
cancellation, and COPY. Unsupported writes must not bypass Chronos.

For normal PostgreSQL clients, use the separate PostgreSQL implementation. Pin
connections with a startup option such as `options=-c%20branch%3Dtrial` in a libpq
URL, verify SHOW branch after reconnects, and select a branch before BEGIN.
That implementation has broader SQL support but no branch merge.

## Export

Read one branch and write its user columns to ordinary tables in a separate
destination database. Recreate constraints and indexes explicitly. Verify counts,
keys, and application queries before cutover. Page large exports by stable keys
while writers are paused or read a checkpoint. Each query result is materialized;
there is no streaming export API or automatic catalog restoration.
