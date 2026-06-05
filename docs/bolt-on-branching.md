# Bolt-On Branching for Relational Data

**Status:** Draft

## Summary

This document proposes a bolt-on branching layer for relational databases. The database continues to provide transactions, isolation, concurrency control, durability, indexing, and SQL execution. The branching layer adds mutable named branches, cheap forks, branch-local writes, branch isolation, diff, merge, and checkpointing.

Applications keep using SQL against logical relational tables:

```sql
SELECT * FROM products WHERE sku = 'abc';

UPDATE products
SET price = 19.99
WHERE sku = 'abc';
```

A checked-out branch determines which physical rows are visible to a branch-bound session across the database. A normal database `COMMIT` publishes changes to the current branch. There is no branch commit required for each transaction.

The interval branching model can be understood with one number-line model:

- each branch owns a monotonically shrinking segment `[lo, hi)` in an integer
  space
- each branch also has one read point inside its current segment
- each physical row version owns a visibility interval `[live_lo, live_hi)`
- a new row version gets its interval from the current segment of the branch
  that writes it
- the same physical row version can be shared by many branches when its
  interval contains their read points

Branch visibility is tested with a constant-shape predicate:

```sql
live_lo <= current_branch_point()
AND current_branch_point() < live_hi
AND deleted = false
```

Chronos adds this predicate to every user query over a registered logical table.

The primary storage model is **write-time interval maintenance**. Branch creation
only partitions branch segment metadata; it does not copy user rows. Writes
create row versions over the writer branch's current segment. Updates and
deletes preserve correctness by splitting overlapping old row intervals, so
reads stay predicate-based and SQL predicates can be evaluated against a
normal-looking branch view efficiently.

At a high level:

```text
1. Branches own monotonically shrinking interval segments.
2. Forking a branch splits the parent's current segment into a parent
   continuation, a child segment, and an immutable fork-base segment.
3. Rows written by a branch receive that branch's current segment as their
   visibility interval.
4. Reads use one point inside the branch segment to filter visible rows.
5. Updates/deletes splice old row intervals so that each branch point sees at
   most one live physical row for each logical key.
```

## Goals

- Preserve SQL and the relational model for application data.
- Support fast branch creation through metadata-only forks.
- Support fast branch-local queries with a simple branch visibility predicate.
- Support fast writes on the current mutable branch segment.
- Support flexible branch tree topology, including arbitrary width and deep branch chains.
- Keep user-visible branches mutable after they are forked.
- Let many database transactions mutate the same branch.
- Provide branch isolation after fork.
- Avoid copying whole tables or graphs at branch creation time.
- Support branch-level time travel to fork points and explicit checkpoints.
- Keep the branch visibility predicate independent of branch depth.

## Operational Limits

- The base implementation branches row contents under a shared schema.
- Branch-local schema changes should be added with the hybrid schema-version
  design in this document: DDL creates a new interval-backed physical table
  version for the affected logical table and branch-visible table bindings
  choose the active schema version.
- Fixed-width interval spaces require sparse allocation, relabeling, or explicit depth limits.
- Hot keys can accumulate many physical rows.
- Multi-row writes evaluate the current branch view first, then splice matched keys.
- Branch-local DDL initially copies visible rows for the affected table into
  the new schema version. Later optimizations can add lazy migration or
  superset schemas for additive changes.
- The log-table representation needs a current-state projection for consistently fast arbitrary SQL reads.
- Long-lived branches require retaining historical log records or physical rows until all dependent branches, checkpoints, and retention policies release them.

## User-Facing API

Agents interact with a Python branch-layer API. Branch management is not exposed as SQL functions. SQL is still the data language, but SQL statements are submitted through a branch-bound session object.

### Context

Create a branching context over an existing database connection or connection pool:

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("postgresql://app@localhost/okg")
```

The context owns branch metadata, table registration, checkout, diff, merge, and SQL rewriting/execution.

### API Surface

```python
class ChronosBranchContext:
    @classmethod
    def connect(cls, database_url: str) -> "ChronosBranchContext": ...

    def register_table(self, table: str, primary_key: list[str]) -> None: ...

    def create_branch(self, branch_id: str, from_branch: str) -> None: ...
    def create_branch_from_checkpoint(
        self,
        branch_id: str,
        checkpoint: str,
    ) -> None: ...
    def delete_branch(self, branch_id: str) -> None: ...
    def list_branches(self) -> list["BranchInfo"]: ...
    def get_branch(self, branch_id: str) -> "BranchInfo": ...

    def checkout(self, branch_id: str) -> "BranchSession": ...
    def checkout_checkpoint(self, checkpoint: str) -> "BranchSession": ...
    def checkout_at(self, branch: str, lsn: int) -> "BranchSession": ...

    def create_checkpoint(self, checkpoint: str, branch: str) -> None: ...

    def diff(self, left: str, right: str) -> "BranchDiff": ...
    def diff_rows(self, left: str, right: str, table: str) -> list["RowDiff"]: ...

    def merge_preview(self, source: str, target: str) -> "MergePreview": ...
    def merge_apply(
        self,
        source: str,
        target: str,
        resolution: "MergeResolution",
    ) -> "MergeResult": ...


class BranchSession:
    branch_id: str

    def query(self, sql: str, params: dict | None = None) -> list[dict]: ...
    def execute(self, sql: str, params: dict | None = None) -> "ExecuteResult": ...
    def transaction(self) -> "TransactionContext": ...
    def branch_info(self) -> "BranchInfo": ...
```

### Table Registration

All user/application tables are branchable data. A deployment registers the tables that belong to the branchable application state:

```python
ctx.register_table("products", primary_key=["sku"])
ctx.register_table("orders", primary_key=["order_id"])
ctx.register_table("nodes", primary_key=["node_id"])
ctx.register_table("edges", primary_key=["edge_id"])
```

After registration, agents continue to use the logical table names in SQL. They do not refer to physical branched tables, segment tables, or interval columns.

### Branch Lifecycle

Create a branch:

```python
ctx.create_branch("exp_pricing", from_branch="main")
```

Delete a branch:

```python
ctx.delete_branch("exp_pricing")
```

List and inspect branches:

```python
branches = ctx.list_branches()
info = ctx.get_branch("exp_pricing")
```

### Branch Sessions

Checkout returns a branch-bound session:

```python
session = ctx.checkout("exp_pricing")
```

All SQL executed through the session runs against that branch's database-wide view:

```python
rows = session.query(
    "SELECT * FROM products WHERE sku = :sku",
    {"sku": "abc"},
)

neighbors = session.query(
    "SELECT * FROM edges WHERE src_id = :node_id",
    {"node_id": "n42"},
)
```

Writes also go through the branch session:

```python
with session.transaction():
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 19.99, "sku": "abc"},
    )
    session.execute(
        """
        INSERT INTO orders (order_id, sku, quantity)
        VALUES (:order_id, :sku, :quantity)
        """,
        {"order_id": "o1", "sku": "abc", "quantity": 2},
    )
```

The underlying database transaction commits both writes atomically to the checked-out branch. There is no branch commit API.

### Current Branch

The session exposes its branch context directly:

```python
session.branch_id
session.branch_info()
```

A transaction pins the branch context at transaction start. Agents should check out the intended branch before opening a transaction:

```python
session = ctx.checkout("exp_pricing")
with session.transaction():
    session.execute("UPDATE products SET price = 10 WHERE sku = 'abc'")
```

### Checkpoints

Create a stable time-travel point for the current state of a branch:

```python
ctx.create_checkpoint("before_discount", branch="exp_pricing")
```

Open a read-only session on a checkpoint:

```python
checkpoint = ctx.checkout_checkpoint("before_discount")
rows = checkpoint.query("SELECT * FROM products WHERE sku = 'abc'")
```

Promote a checkpoint to a new mutable branch:

```python
ctx.create_branch_from_checkpoint(
    "restore_before_discount",
    checkpoint="before_discount",
)
```

### Diff

Diff compares two database-wide branch states:

```python
diff = ctx.diff("main", "exp_pricing")
```

The result is grouped by table and primary key:

```python
diff.changes
# [
#   {"table": "products", "key": {"sku": "abc"}, "change": "modified"},
#   {"table": "products", "key": {"sku": "def"}, "change": "added"},
#   {"table": "orders", "key": {"order_id": "o9"}, "change": "deleted"},
#   {"table": "nodes", "key": {"node_id": "n42"}, "change": "modified"},
# ]
```

Detailed before/after rows can be requested for changed keys:

```python
rows = ctx.diff_rows("main", "exp_pricing", table="products")
```

### Merge

A merge computes changes from a source branch and applies them to a target branch:

```python
preview = ctx.merge_preview(source="exp_pricing", target="main")
ctx.merge_apply(source="exp_pricing", target="main", resolution=preview.resolution)
```

`merge_preview` reports additions, deletions, modifications, and conflicts. `merge_apply` runs in a normal database transaction and updates the target branch if conflicts are resolved.

## System Model

The design has two layers:

```text
Database transaction layer
  - BEGIN / COMMIT / ROLLBACK
  - isolation and concurrency control
  - row locks, indexes, constraints
  - crash recovery

Branching layer
  - branch checkout
  - branch creation
  - segment allocation
  - branch-local writes
  - checkpoints
  - diff and merge metadata
```

The branch layer does not replace database transactions. A database transaction opened through a branch session runs on one checked-out branch. If the transaction commits, its changes become visible on that branch. If it rolls back, no branch state changes.

## Branch Scope

A branch represents the logical state of the application database, not one table. All user/application data tables are branched together under the same checked-out branch.

For a branch `exp_a`, the branch view includes:

```text
products at exp_a
orders at exp_a
customers at exp_a
nodes at exp_a
edges at exp_a
```

The branch point is database-wide. Every branched table uses the same checked-out branch context:

```text
current_branch_id
current_segment_id
current_branch_point
current_segment interval
```

So this transaction mutates one coherent branch state:

```python
session = ctx.checkout("exp_a")

with session.transaction():
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 19.99, "sku": "abc"},
    )
    session.execute(
        """
        INSERT INTO orders (order_id, sku, quantity)
        VALUES (:order_id, :sku, :quantity)
        """,
        {"order_id": "o1", "sku": "abc", "quantity": 2},
    )
```

Both writes commit to the same branch. A later `ctx.create_branch(...)` forks the combined state of all branched tables.

The branch layer manages all user/application data by default:

- user data tables
- graph node and edge tables
- application-owned relational tables
- table-level indexes generated for those branched tables

The branch layer does not branch its own global metadata tables:

- `branches`
- `segments`
- checkpoint metadata
- garbage-collection metadata

In the base implementation, system catalogs and database schema are shared.
Branch-local DDL is added by versioning logical table schema descriptors and
binding branch intervals to schema versions. Chronos metadata remains global;
user table schemas become branch-visible data.

## Core Concepts

### User Branch

A user branch is a mutable named workspace:

```text
main
exp_a
agent_17
```

Users create, check out, mutate, and compare branches.

### Segment

A segment is the part of the integer space currently managed by a branch. A
branch's segment shrinks monotonically: every time the branch forks a child,
Chronos partitions the current segment and gives the branch a smaller
continuation segment. The branch's `branch_point` is an interior read point of
the current segment, and all registered tables use that point for visibility
checks. The segment is the write/inheritance scope; the point is the read
identity.

When a branch is forked, Chronos recursively partitions the source branch's
current segment into three new segments:

- one continuation segment for the source branch
- one initial segment for the new child branch
- one immutable `fork_base` segment that represents the source branch's state
  at fork time

This is not allocation from a global append-only counter. Each fork splits the
parent branch's current interval.

```text
Before:

  x -> x_s1

After `ctx.create_branch("y", from_branch="x")`:

            x_s2   <- x continues here
          /
  x_s1 -> z_s1     <- immutable fork base
          \
            y_s1   <- y starts here
```

Future writes to `x` go to `x_s2`. Future writes to `y` go to `y_s1`. The
segment owned by `x` has shrunk from `x_s1` to `x_s2`. Both branches inherit
rows whose intervals covered the old `x_s1` segment. The `z_s1` fork-base
segment is readable but not writable or branchable, so later writes to either
branch cannot change the state used as their merge base.

### Interval Encoding

Each segment receives a range and a branch point. The branch point is not the
split point. It is a representative point inside the segment used to evaluate
row visibility. A branch's point can change when the branch is forked because
the branch moves to a new continuation segment.

```text
segment  interval          point
main_s1  [0, 1000000)      500000
x_s1     [100000,200000)   150000
x_s2     [120000,140000)   130000
y_s1     [160000,180000)   170000
```

Child intervals are nested inside parent intervals:

```text
parent_lo < child_lo < child_hi <= parent_hi
```

A physical row version is visible on a branch when the row's interval contains
that branch's point:

```text
row.live_lo <= branch.branch_point < row.live_hi
AND row.deleted = false
```

This keeps reads independent of branch depth. A read does not walk from a branch
to its parent, then grandparent, and so on. It evaluates one point against row
intervals.

The branch point is deliberately not used as the write scope. Writes use the
entire current segment, so future descendants can inherit those writes until
they override them.

## Metadata Tables

```sql
CREATE TABLE branches (
  branch_id TEXT PRIMARY KEY,
  current_segment_id INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

```sql
CREATE TABLE segments (
  segment_id INTEGER PRIMARY KEY,
  parent_segment_id INTEGER NULL,
  owner_branch_id TEXT NULL,
  segment_kind TEXT NOT NULL DEFAULT 'mutable',
  live_lo NUMERIC(78, 0) NOT NULL,
  live_hi NUMERIC(78, 0) NOT NULL,
  branch_point NUMERIC(78, 0) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CHECK (live_lo <= branch_point),
  CHECK (branch_point < live_hi)
);
```

`branches.current_segment_id` points to the branch's mutable segment.
`segments.parent_segment_id` supports management, visualization, diff, merge,
and interval allocation. It is not on the hot read path.

`segment_kind` distinguishes normal mutable branch segments from immutable
internal refs:

```text
mutable    writable segment owned by a branch head
checkpoint read-only user checkpoint
fork_base  read-only internal merge base created during branch creation
```

Only `mutable` segments are writable and branchable. `checkpoint` and
`fork_base` segments are readable snapshots. A `fork_base` segment is hidden
from the public branch API; users name source and target branches, and Chronos
infers the merge base from metadata.

Mutable segments are allocated with enough width to keep an interior branch
point. Fork-base segments are one-unit intervals `[x, x+1)` and use
`branch_point = x`.

## Branched User Tables

For each logical table, the system stores physical row versions with
branch-visible live intervals. A single logical row can therefore have multiple
physical rows, each visible to a different region of branch space.

The most important rule is how a physical row gets its interval:

```text
on insert/update in branch b:
  new_row.live_lo = b.current_segment.live_lo
  new_row.live_hi = b.current_segment.live_hi
  new_row.writer_segment_id = b.current_segment_id
```

That write scope is what makes inheritance work. A row written before a fork
has an interval that covers both resulting branch points after the fork. A row
written after a fork is restricted to the writer's continuation segment, so
sibling branches do not see it.

`writer_segment_id` records provenance. It identifies the segment that produced
the logical user effect. Inserts, updates, and delete tombstones use the current
segment id of the writing branch. Preservation rows created while splicing an
old row keep the old row's writer segment id; they are not attributed to the
branch that caused the splice. This distinction lets Chronos find candidate
branch changes from row-version provenance without a separate side table.

Logical table:

```sql
CREATE TABLE products (
  sku TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  price NUMERIC NOT NULL
);
```

Physical branched table:

```sql
CREATE TABLE products_b (
  sku TEXT NOT NULL,
  name TEXT NULL,
  price NUMERIC NULL,
  live_lo NUMERIC(78, 0) NOT NULL,
  live_hi NUMERIC(78, 0) NOT NULL,
  writer_segment_id INTEGER NOT NULL,
  deleted BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (sku, live_lo),
  CHECK (live_lo < live_hi)
);
```

The key invariant is:

```text
For a given logical key K and branch b, there is at most one physical row r
such that:

  r.key = K
  r.live_lo <= b.branch_point < r.live_hi
  r.deleted = false
```

In other words, a branch point sees at most one live physical row per logical
key. This is the property that makes the branch-visible relation behave like an
ordinary SQL table.

Deletes are tombstones:

```text
deleted = true
```

A tombstone hides inherited rows inside its live interval.

## Branch-Local Schema Changes

Chronos should support branch-local DDL with a hybrid schema-version design.
The key rule is that a table with a divergent schema must remain branchable by
the interval backend. DDL must not turn the table into a dead-end physical copy
that future forks cannot share cheaply.

The relational store therefore versions **logical table schemas** and maps each
branch interval to the schema version visible at that branch point.

### Schema Versions

Each logical table can have multiple schema versions:

```text
logical table: products

schema version v1:
  physical table: _chronos_b_interval_products_v1
  columns: sku, name, price
  primary key: sku
  row storage: interval rows

schema version v2:
  physical table: _chronos_b_interval_products_v2
  columns: sku, name, price, score
  primary key: sku
  row storage: interval rows
```

Both physical tables are interval-backed. They include `live_lo`, `live_hi`,
and `deleted`, and they use the same row-branching rules as the base interval
design.

### Branch-Visible Table Bindings

The active schema version for a logical table is itself branch-visible:

```text
_chronos_table_schema_versions
  table_name
  schema_version_id
  parent_schema_version_id
  physical_table
  columns
  primary_key
  ddl_op
  created_at
  metadata

_chronos_table_bindings
  table_name
  schema_version_id
  live_lo
  live_hi
```

Resolving a table during checkout or query planning becomes:

```text
schema_version =
  visible binding for table_name where
    live_lo <= current_branch_point < live_hi

physical_table = schema_version.physical_table
```

This is the schema-level analogue of row visibility. A branch point sees one
active schema version for each logical table.

### DDL Flow for an Inherited Table

Given:

```sql
ALTER TABLE products ADD COLUMN score DOUBLE PRECISION;
```

and a branch `exp` currently seeing `products@v1`, Chronos performs:

1. Resolve the current branch segment and branch point.
2. Resolve the visible schema version: `products@v1`.
3. Create a new schema version: `products@v2`.
4. Create a new physical interval table for `v2` with the new columns.
5. Copy rows visible to `exp` from `v1` into `v2`.
6. Assign copied rows visibility over `exp`'s current segment.
7. Splice `_chronos_table_bindings` so `exp` and descendants see `v2`.
8. Keep all other branches bound to `v1`.
9. Route future SQL for `products` in `exp` to the `v2` physical table.

The copied rows remain interval rows:

```text
new_v2_row.live_lo = exp.current_segment.live_lo
new_v2_row.live_hi = exp.current_segment.live_hi
new_v2_row.writer_segment_id = source_row.writer_segment_id
```

Future writes in `exp` and future forks from `exp` continue to use interval
splicing inside `_chronos_b_interval_products_v2`.

### Forking After Divergence

After schema divergence:

```text
main -> exp -> child_a
            -> child_b
```

`child_a` and `child_b` inherit the table binding:

```text
products -> v2
```

Branch creation is still metadata-only. It does not copy the `products@v2`
rows. The children continue sharing inherited `v2` row intervals until one
child writes an override.

This is the central requirement of the hybrid design: divergent schema tables
must stay first-class branchable tables.

### CREATE TABLE

For branch-local `CREATE TABLE`, Chronos creates:

```text
schema version v1 for the new logical table
physical interval table for v1
binding visible only over the current branch segment
```

Other branches see the table as absent because they have no visible binding for
that logical table.

### DROP TABLE

For branch-local `DROP TABLE`, Chronos creates a tombstone table binding for
the current branch segment rather than dropping physical storage:

```text
table_name = products
schema_version_id = NULL
tombstone = true
live_lo/live_hi = current branch segment
```

Branches outside that interval keep seeing the previous schema version.
Descendants of the dropping branch inherit table absence until a later DDL
recreates the table.

### Repeated DDL

DDL on an already-diverged table creates another schema version:

```text
products@v1 -> products@v2 -> products@v3
```

Each transition uses the same mechanics: create a new interval-backed physical
table, copy the branch-visible rows from the previous version, and splice the
table binding over the current branch segment.

### Tradeoffs

This hybrid design copies visible rows for the affected table at DDL time. That
is the cost paid to preserve correct SQL semantics without implementing a full
logical catalog immediately.

The benefit is that:

- ordinary DML remains interval-backed
- branch creation after DDL remains cheap
- unaffected tables remain on their existing interval physical tables
- branches that did not perform the DDL keep seeing the old schema
- implementation can fall back to normal physical SQL table definitions for
  complex DDL

Later optimizations can reduce DDL copy cost:

- lazy row migration from old schema versions to new schema versions
- superset physical schemas for additive `ADD COLUMN`
- schema-version compaction after old branches and checkpoints are collected

## Read Path

A checked-out session has branch constants:

```text
current_segment_id
current_branch_point()
current_segment_lo()
current_segment_hi()
```

The read path is a **predicate rewrite layer**. User SQL refers to logical table
names. Chronos rewrites those table references to physical interval tables plus
the branch visibility predicate.

Logical point lookup:

```sql
SELECT sku, name, price
FROM products
WHERE sku = 'abc';
```

Rewritten physical query:

```sql
SELECT sku, name, price
FROM products_b
WHERE sku = 'abc'
  AND live_lo <= current_branch_point()
  AND current_branch_point() < live_hi
  AND deleted = false;
```

Logical scan:

```sql
SELECT sku, name, price
FROM products
WHERE price > 100;
```

Rewritten physical query:

```sql
SELECT sku, name, price
FROM products_b
WHERE live_lo <= current_branch_point()
  AND current_branch_point() < live_hi
  AND deleted = false
  AND price > 100;
```

The important property is that the added predicate has constant shape:

```text
live_lo <= current_branch_point()
AND current_branch_point() < live_hi
AND deleted = false
```

It does not grow with branch depth or branch width. The read path is simple
because the write path maintains the interval invariant.

## Write Path

Writes assign intervals to new physical row versions and maintain the interval
map for old versions. They target the current mutable segment of the checked-out
branch:

```text
write scope = [current_segment_lo(), current_segment_hi())
```

Stable fork-prefix segments and checkpoints are read-only. When a branch is forked, the source branch moves to a new continuation segment. Future writes go to that continuation segment.

For a new logical row, the write path is simple: insert one physical row whose
`live_lo` and `live_hi` equal the current segment bounds.

```text
current segment: [20, 40)
INSERT sku = 'abc', price = 15

physical row:
  abc = 15  [20, 40)
```

For an update/delete, the new physical row also gets the current segment. The
extra work is removing that segment from any old physical row intervals for the
same logical key.

When a branch updates or deletes a logical row, Chronos finds physical rows for
the same logical key whose intervals overlap the branch's current segment. It
then replaces each overlap with up to three pieces:

```text
old value before the branch segment
new branch-local value inside the branch segment
old value after the branch segment
```

This preserves the invariant that each branch point sees at most one live
version of each logical row.

The rule is therefore:

```text
row intervals are assigned on write from the writer's current segment;
old intervals are split only when they overlap that write segment.
```

### Update

User SQL:

```sql
UPDATE products
SET price = 19.99
WHERE sku = 'abc';
```

The system splices the current segment's interval into the live interval map for `sku = 'abc'`.

Find and lock overlapping physical rows:

```sql
SELECT *
FROM products_b
WHERE sku = 'abc'
  AND live_lo < current_segment_hi()
  AND current_segment_lo() < live_hi
FOR UPDATE;
```

For each overlapping physical row:

```text
old physical row: [a, b)
write scope:   [u_lo, u_hi)
overlap:       [max(a,u_lo), min(b,u_hi))

left remainder:   [a, u_lo) if a < u_lo
replacement:      [max(a,u_lo), min(b,u_hi)) with new payload
right remainder:  [u_hi, b) if u_hi < b
```

Example:

```text
old:
abc = 10  [0,1000000)

write in y_s1 [160000,180000):
abc = 10  [0,160000)
abc = 15  [160000,180000)
abc = 10  [180000,1000000)
```

This is the central tradeoff of the interval model: reads are simple because
writes split row intervals when needed.

If the current segment already has the exact live interval for the key, repeated updates can modify that row in place:

```sql
UPDATE products_b
SET price = 19.99,
    deleted = false
WHERE sku = 'abc'
  AND live_lo = current_segment_lo()
  AND live_hi = current_segment_hi();
```

### Delete

Delete uses the same splice operation, but the replacement is a tombstone:

```text
old:
abc = 10  [0,1000000)

delete in y_s1 [160000,180000):
abc = 10       [0,160000)
abc = deleted  [160000,180000)
abc = 10       [180000,1000000)
```

The tombstone is part of the logical state. It prevents inherited rows from
reappearing inside the deleted branch interval.

### Insert

Insert first checks the row visible at the current branch point:

```sql
SELECT *
FROM products_b
WHERE sku = 'abc'
  AND live_lo <= current_branch_point()
  AND current_branch_point() < live_hi
FOR UPDATE;
```

If a non-deleted row is visible, the logical primary key already exists. If no row is visible, insert the new row over the current segment interval. If a tombstone is visible, `UPSERT` semantics can replace it by using the same splice operation as update.

### Multi-Row Writes

For a predicate update:

```sql
UPDATE products
SET price = price * 0.9
WHERE price > 100;
```

the system first identifies matching logical keys in the current branch view:

```sql
SELECT sku
FROM products_b
WHERE live_lo <= current_branch_point()
  AND current_branch_point() < live_hi
  AND deleted = false
  AND price > 100;
```

Then it splices each matched key. Batch execution should lock keys in deterministic primary-key order.

## Branch Creation Protocol

`ctx.create_branch("y", from_branch="x")` runs in one database transaction:

1. Lock branch `x`.
2. Read `x.current_segment_id`, called `S`.
3. Split `S` into three child intervals:
   - one immutable fork-base segment `Z`
   - one continuation segment for `x`
   - one initial segment for `y`
4. Insert all three segments.
5. Set the parent of both mutable segments to `Z`.
6. Set the parent of `Z` to the old segment `S`.
7. Update `x.current_segment_id` to the continuation segment.
8. Insert branch `y` pointing to its initial segment.
9. Record fork metadata linking `(x, y)` to `Z`.
10. Commit.

No user rows are copied.

Existing live intervals continue to cover the parent continuation, child, and
fork-base branch points until one mutable branch writes an override. Writes only
target the current mutable segment, so the fork-base point remains a stable
readable snapshot.

## Correctness Conditions

The system is correct when these conditions hold:

- For each logical key and branch point, at most one non-deleted physical row is visible.
- Branch creation only writes metadata.
- User writes target the current mutable segment.
- Fork-base segments and checkpoints are immutable.
- Only mutable segments can be written or used as the source of a new branch.
- Deletes create tombstone intervals.
- Splices are atomic database transactions.
- Concurrent writers to the same key and overlapping interval are serialized.
- Logical SQL operates on the branch-visible relation, not directly on internal metadata tables.

With these conditions, a branch read is equivalent to reading an ordinary table containing the branch's current state.

## Indexing

Point reads:

```sql
CREATE INDEX products_b_point_read
ON products_b (sku, live_lo)
INCLUDE (live_hi, deleted, name, price);
```

Scans:

```sql
CREATE INDEX products_b_scan
ON products_b (live_lo, live_hi)
INCLUDE (deleted, sku, name, price);
```

Overlap maintenance:

```sql
CREATE INDEX products_b_overlap
ON products_b (sku, live_lo, live_hi);
```

For range-aware databases, store a generated range:

```sql
live_span numrange
  GENERATED ALWAYS AS (
    numrange(live_lo, live_hi, '[)')
  ) STORED
```

and use:

```sql
CREATE INDEX products_b_live_span
ON products_b USING gist (live_span);
```

In PostgreSQL, the non-overlap invariant can be enforced with an exclusion constraint:

```sql
EXCLUDE USING gist (
  sku WITH =,
  live_span WITH &&
);
```

Databases without exclusion constraints can enforce the invariant with serializable transactions, per-key locks, or trigger-based overlap checks.

## Interval Allocation

The initial interval space should be much larger than 64 bits:

```text
root: [0, 10^78)
```

Children are created by splitting the parent segment into sub-intervals.

Fixed-width intervals have a depth/fanout tradeoff:

```text
max_depth ~= W / log2(fanout)
```

Approximate binary-chain depth:

```text
64-bit:       ~64 levels
128-bit:      ~128 levels
256-bit:      ~256 levels
NUMERIC(78):  ~259 bits
```

The implementation should not rely on 64-bit intervals for correctness.

Recommended first implementation:

- use `NUMERIC(78,0)` or 256-bit integer intervals
- allocate sparsely
- reject branch creation when a local interval lacks space
- include a subtree relabeling maintenance path

For arbitrary adversarial depth, use variable-length lexicographic intervals:

```text
main_s1 label: 80
x_s1    label: 80.40
y_s1    label: 80.40.90

main_s1 range: [80, 81)
x_s1    range: [80.40, 80.41)
y_s1    range: [80.40.90, 80.40.91)
```

The read predicate stays the same shape:

```sql
live_lo <= current_branch_label()
AND current_branch_label() < live_hi
```

Variable-length labels support unbounded depth at the cost of wider labels and indexes.

## Efficiency

Let:

```text
D = branch depth
W = branch width / number of sibling branches
K = number of physical row intervals for the logical keys touched by a write
```

```text
branch checkout:   O(1) with respect to D and W
branch creation:   O(1) metadata writes with respect to D and W
point read:        O(1) branch overhead; indexed branch-point predicate
scan:              O(1) branch overhead; branch-point predicate over live intervals
keyed update:      O(K) overlap lookup plus interval splice
delete:            O(K) splice with tombstone replacement
storage growth:    physical rows for changed keys
```

Reads do not walk branch ancestry and do not scan sibling branches. They bind
one `branch_point` and evaluate the same visibility predicate for every
registered table.

With leaf-scoped writes, a keyed update normally overlaps one live physical row
for that key. The expensive case is a heavily fragmented hot key or an
administrative rewrite over a broad interval.

Branch creation does not copy user rows. Storage grows when data changes, not when branches are created.

## Alternative Representation: Branch Log Tables

The same `ChronosBranchContext` API can be implemented with append-only log
tables instead of write-time interval maintenance. In this representation, a
branch is a log timeline. Branch creation records a fork point and shares the
parent log prefix. Writes append new log records to the branch's own timeline.

This is closer to a WAL-timeline model:

```text
main: L1 -> L2 -> L3

ctx.create_branch("exp", from_branch="main")

main: L1 -> L2 -> L3 -> L4_main
exp:  L1 -> L2 -> L3 -> L4_exp -> L5_exp
```

The physical log is stored in ordinary relational tables.

### Timeline Metadata

```sql
CREATE TABLE branch_timelines (
  branch_id TEXT PRIMARY KEY,
  parent_branch_id TEXT NULL,
  fork_lsn BIGINT NULL,
  head_lsn BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

`fork_lsn` is the parent's head at branch creation time. `head_lsn` is the latest committed log sequence number on this branch.

Transactions are recorded explicitly:

```sql
CREATE TABLE branch_txns (
  txn_id BIGINT PRIMARY KEY,
  branch_id TEXT NOT NULL,
  begin_lsn BIGINT NOT NULL,
  commit_lsn BIGINT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

All row changes in a database transaction share a `txn_id` and commit atomically with the database transaction.

### Log Tables

Each user table gets an append-only log table. For `products`:

```sql
CREATE TABLE products_log (
  log_id BIGINT PRIMARY KEY,
  branch_id TEXT NOT NULL,
  txn_id BIGINT NOT NULL,
  lsn BIGINT NOT NULL,
  op TEXT NOT NULL, -- insert, update, delete

  sku TEXT NOT NULL,
  name TEXT NULL,
  price NUMERIC NULL,

  row_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL
);
```

For graph data:

```sql
CREATE TABLE nodes_log (
  log_id BIGINT PRIMARY KEY,
  branch_id TEXT NOT NULL,
  txn_id BIGINT NOT NULL,
  lsn BIGINT NOT NULL,
  op TEXT NOT NULL,

  node_id TEXT NOT NULL,
  label TEXT NULL,
  properties JSONB NULL,

  row_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL
);
```

```sql
CREATE TABLE edges_log (
  log_id BIGINT PRIMARY KEY,
  branch_id TEXT NOT NULL,
  txn_id BIGINT NOT NULL,
  lsn BIGINT NOT NULL,
  op TEXT NOT NULL,

  edge_id TEXT NOT NULL,
  src_id TEXT NOT NULL,
  dst_id TEXT NOT NULL,
  label TEXT NULL,
  properties JSONB NULL,

  row_hash TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL
);
```

Deletes are log records with `op = 'delete'`. The key is retained, and payload columns may be null.

### Branch Creation

Branch creation is metadata-only:

```python
ctx.create_branch("exp", from_branch="main")
```

If `main.head_lsn = 100`, the new branch starts as:

```text
branch_id  parent_branch_id  fork_lsn  head_lsn
main       null              null      100
exp        main              100       100
```

No user rows and no log rows are copied.

### Write Path

Writes append log records to the checked-out branch.

```python
session = ctx.checkout("exp")

with session.transaction():
    session.execute(
        "UPDATE products SET price = :price WHERE sku = :sku",
        {"price": 19.99, "sku": "abc"},
    )
```

The branch layer rewrites this into an append to `products_log`:

```sql
INSERT INTO products_log (
  log_id,
  branch_id,
  txn_id,
  lsn,
  op,
  sku,
  name,
  price,
  row_hash,
  committed_at
)
VALUES (
  :log_id,
  'exp',
  :txn_id,
  :lsn,
  'update',
  'abc',
  'Widget',
  19.99,
  :row_hash,
  now()
);
```

`INSERT` and `DELETE` are also append-only:

```sql
-- insert
INSERT INTO products_log (..., op, sku, name, price, row_hash)
VALUES (..., 'insert', 'def', 'New Item', 4.99, :row_hash);

-- delete
INSERT INTO products_log (..., op, sku, row_hash)
VALUES (..., 'delete', 'abc', :delete_hash);
```

The log table is the history source. It records every committed row mutation.

### Visible Timeline Ranges

To read branch `exp`, the system needs the ordered timeline ranges inherited by that branch.

For a shallow branch:

```text
exp sees:
  main [0, 100]
  exp  [101, exp.head_lsn]
```

For a deep branch:

```text
main -> a -> b -> c

c sees:
  main [0, fork_a]
  a    [fork_a + 1, fork_b]
  b    [fork_b + 1, fork_c]
  c    [fork_c + 1, c.head_lsn]
```

The branch context can materialize this at checkout:

```python
session = ctx.checkout("c")
session.visible_ranges
```

Example:

```text
[
  ("main", 0, 100),
  ("a", 101, 150),
  ("b", 151, 180),
  ("c", 181, 220),
]
```

The SQL rewriter injects these ranges as constants. It does not expose them to agents.

### Point Query

Agent query:

```python
session.query(
    "SELECT * FROM products WHERE sku = :sku",
    {"sku": "abc"},
)
```

Physical query for a shallow branch:

```sql
SELECT sku, name, price, op
FROM products_log
WHERE sku = :sku
  AND (
    (branch_id = 'exp' AND lsn <= :exp_head_lsn)
    OR
    (branch_id = 'main' AND lsn <= :exp_fork_lsn)
  )
ORDER BY lsn DESC
LIMIT 1;
```

If the latest row has `op = 'delete'`, the logical row is absent. Otherwise the payload is returned.

For deep branches, the generated predicate uses the session's visible ranges:

```sql
SELECT sku, name, price, op
FROM products_log
WHERE sku = :sku
  AND (
    (branch_id = 'main' AND lsn BETWEEN 0 AND 100)
    OR (branch_id = 'a' AND lsn BETWEEN 101 AND 150)
    OR (branch_id = 'b' AND lsn BETWEEN 151 AND 180)
    OR (branch_id = 'c' AND lsn BETWEEN 181 AND 220)
  )
ORDER BY lsn DESC
LIMIT 1;
```

This is correct, but read cost grows with timeline depth unless the branch context caches visible ranges and the database has supporting indexes.

### Full Table Query

Agent query:

```python
session.query("SELECT * FROM products WHERE price > 100")
```

The branch layer reconstructs the current logical table first, then applies the user predicate:

```sql
WITH visible_log AS (
  SELECT *
  FROM products_log
  WHERE
    (branch_id = 'exp' AND lsn <= :exp_head_lsn)
    OR
    (branch_id = 'main' AND lsn <= :exp_fork_lsn)
),
current_products AS (
  SELECT DISTINCT ON (sku)
    sku,
    name,
    price,
    op
  FROM visible_log
  ORDER BY sku, lsn DESC
)
SELECT sku, name, price
FROM current_products
WHERE op <> 'delete'
  AND price > 100;
```

The order of operations matters:

```text
1. reconstruct the latest row per logical key for the branch
2. remove tombstones
3. apply the user's SQL predicate
```

This preserves normal SQL semantics.

### Indexes

Point reads:

```sql
CREATE INDEX products_log_point
ON products_log (sku, branch_id, lsn DESC)
INCLUDE (op, name, price, row_hash);
```

Branch-range scans:

```sql
CREATE INDEX products_log_branch_lsn
ON products_log (branch_id, lsn)
INCLUDE (sku, op, name, price, row_hash);
```

Graph adjacency:

```sql
CREATE INDEX edges_log_src
ON edges_log (src_id, branch_id, lsn DESC)
INCLUDE (edge_id, dst_id, op, label, properties, row_hash);

CREATE INDEX edges_log_dst
ON edges_log (dst_id, branch_id, lsn DESC)
INCLUDE (edge_id, src_id, op, label, properties, row_hash);
```

### Current-State Projection

The log-table representation should maintain an optional current-state projection for fast SQL reads:

```text
log tables                source of truth, history, time travel
current-state projection  optimized branch query path
```

Projection options:

- interval live-range tables, as described in the primary representation
- branch-head tables for selected hot branches
- materialized current views for analytical workloads
- cached reconstructed pages or row groups

The projection is maintained in the same database transaction as the log append. The log remains the authoritative history; the projection is a derived read model.

Without a projection, broad SQL queries reconstruct current rows from the log and can be expensive. With a projection, ordinary branch queries use the projection while history, audit, time travel, and replay use the log.

### Diff

Diff can use the log when branches share a known prefix:

```text
main and exp share prefix through lsn 100
exp changes after 100 are candidate differences
main changes after 100 are candidate differences
```

For exact state diff, the system compares reconstructed current rows at the two branch points:

```text
left_current(table, key)
right_current(table, key)
```

`row_hash` avoids fetching full payloads for unchanged rows. If exact comparison is required, the implementation can fetch full rows for keys whose hashes differ.

### Efficiency

```text
branch creation: O(1) timeline metadata
writes:          append-only log records
time travel:     natural by branch and LSN
point read:      latest visible log record for key
full scan:       reconstruct current row per key unless projected
storage:         one log row per logical mutation
```

This representation is strongest when history, auditability, replay, and append-only writes matter. It needs a projection layer for consistently fast arbitrary SQL reads.

## Time Travel and Checkpoints

The interval model supports time travel to structural states:

- current branch heads
- fork boundaries
- explicit checkpoints

Repeated updates inside the same current segment retain only the latest value for that segment. To preserve a state, create a checkpoint before further mutation:

```python
ctx.create_checkpoint("before_discount", branch="exp_pricing")
```

Checkpointing creates a stable segment boundary. It does not require copying user tables.

The log-table representation can additionally support time travel by LSN because every row mutation is retained in the append-only log:

```python
historical = ctx.checkout_at(branch="exp_pricing", lsn=120)
rows = historical.query("SELECT * FROM products WHERE sku = 'abc'")
```

This requires retaining the relevant log records.

## Branch Diff

Diff compares two database-wide branch states:

```text
left_branch_point
right_branch_point
```

The system computes diff table by table for every branched user table. A row is compared by logical primary key. Each result is one of:

```text
added      key is absent on left and present on right
deleted    key is present on left and absent on right
modified   key is present on both sides but row contents differ
unchanged  key is present on both sides and row contents match
```

The physical table stores writer provenance and may store a row hash to make
final comparison cheap:

```sql
CREATE TABLE products_b (
  sku TEXT NOT NULL,
  name TEXT NULL,
  price NUMERIC NULL,
  live_lo NUMERIC(78, 0) NOT NULL,
  live_hi NUMERIC(78, 0) NOT NULL,
  writer_segment_id INTEGER NOT NULL,
  deleted BOOLEAN NOT NULL DEFAULT false,
  row_hash TEXT NULL,
  PRIMARY KEY (sku, live_lo),
  CHECK (live_lo < live_hi)
);
```

`row_hash` is computed from the logical row payload, excluding branch metadata:

```text
hash(name, price)
```

Tombstones participate in diff as absence. A deleted row is not present in that branch state.

### Candidate Keys From Writer Segments

For two current branch segments, Chronos first finds their nearest shared
immutable fork-base segment. Mutable writer segments on the left path after
that base are left-only writers. Mutable writer segments on the right path
after that base are right-only writers.

```text
left_segments  = mutable_ancestors(left.current_segment) after base_segment
right_segments = mutable_ancestors(right.current_segment) after base_segment
candidate_writer_segments = left_segments union right_segments
```

Rows whose writer segment is on only one side are the only rows that can cause
the two branch states to differ. Shared-prefix writes are inherited by both
branches unless a later one-sided write overrides them. Preservation rows do not
create false authorship because they keep the writer segment id of the row they
preserve.

For a single table, collect candidate keys from writer provenance:

```sql
SELECT DISTINCT sku
FROM products_b
WHERE writer_segment_id = ANY(:candidate_writer_segments);
```

Then reconstruct only those keys at the two branch points:

```sql
SELECT sku, row_hash, deleted
FROM products_b
WHERE sku = :sku
  AND live_lo <= :left_point
  AND :left_point < live_hi;
```

```sql
SELECT sku, row_hash, deleted
FROM products_b
WHERE sku = :sku
  AND live_lo <= :right_point
  AND :right_point < live_hi;
```

For batch execution, use the candidate key relation instead of one key at a
time:

```sql
WITH candidate_keys AS (
  SELECT DISTINCT sku
  FROM products_b
  WHERE writer_segment_id = ANY(:candidate_writer_segments)
),
left_visible AS (
  SELECT p.*
  FROM products_b p
  JOIN candidate_keys c USING (sku)
  WHERE p.live_lo <= :left_point
    AND :left_point < p.live_hi
),
right_visible AS (
  SELECT p.*
  FROM products_b p
  JOIN candidate_keys c USING (sku)
  WHERE p.live_lo <= :right_point
    AND :right_point < p.live_hi
)
SELECT c.sku,
       l.deleted AS left_deleted,
       r.deleted AS right_deleted,
       l.row_hash AS left_hash,
       r.row_hash AS right_hash
FROM candidate_keys c
LEFT JOIN left_visible l USING (sku)
LEFT JOIN right_visible r USING (sku);
```

The non-overlap invariant guarantees that each branch point contributes at most
one row per key. Tombstones are treated as absence during classification.

Classify each candidate row:

```text
left absent and right present      -> added
left present and right absent      -> deleted
left present and right present
  and left_hash <> right_hash      -> modified
otherwise                         -> unchanged
```

Most callers should suppress unchanged rows. Unchanged candidates are possible:
a branch may update a row and later restore the same value, or a divergent
writer may touch a key without changing the final logical state.

### Fallback Snapshot Diff

If writer provenance is unavailable, the backend can fall back to a visible
snapshot diff. This path collects rows visible at either branch point and
collapses by logical key:

```sql
SELECT
  sku,
  bool_or(live_lo <= :left_point AND :left_point < live_hi AND deleted = false) AS present_left,
  bool_or(live_lo <= :right_point AND :right_point < live_hi AND deleted = false) AS present_right,
  max(row_hash) FILTER (WHERE live_lo <= :left_point AND :left_point < live_hi AND deleted = false) AS left_hash,
  max(row_hash) FILTER (WHERE live_lo <= :right_point AND :right_point < live_hi AND deleted = false) AS right_hash
FROM products_b
WHERE (live_lo <= :left_point AND :left_point < live_hi)
   OR (live_lo <= :right_point AND :right_point < live_hi)
GROUP BY sku;
```

### Diff Output

The diff API returns database-wide results grouped by table:

```text
ctx.diff(left_branch_id, right_branch_id)
  products
    added:    [sku=def]
    deleted:  [sku=old]
    modified: [sku=abc]
  orders
    added:    [order_id=o1]
  nodes
    modified: [node_id=n42]
  edges
    deleted:  [edge_id=e9]
```

For detailed diffs, fetch before/after rows for changed keys:

```sql
SELECT *
FROM products_b
WHERE sku = :sku
  AND live_lo <= :left_point
  AND :left_point < live_hi;
```

```sql
SELECT *
FROM products_b
WHERE sku = :sku
  AND live_lo <= :right_point
  AND :right_point < live_hi;
```

### Diff Efficiency

With writer provenance, diff cost is proportional to the two segment ancestry
paths plus the row versions written after divergence:

```text
O(depth(left) + depth(right) + W_delta + K_delta * point_lookup)
```

`W_delta` is the number of physical row versions and tombstones whose writer
segment is on one side of the branch divergence. `K_delta` is the distinct key
count among those rows. The final point lookup uses the normal visibility
predicate, so correctness does not depend on a separate diff representation.

Indexes for provenance-based diff:

```sql
CREATE INDEX products_b_writer_segment
ON products_b (writer_segment_id, sku);

CREATE INDEX products_b_point_lookup
ON products_b (sku, live_hi)
INCLUDE (live_lo, deleted, row_hash);
```

The snapshot fallback can use a range index:

```sql
CREATE INDEX products_b_diff_span
ON products_b USING gist (live_span);
```

Large database-wide diffs should stream table-by-table and key-by-key.

## Merge

Merge is the promotion of one speculative branch state into another branch. The
user provides source and target branches; Chronos infers the base:

```text
base   = immutable fork-base segment shared by source and target
target = visible state of target branch
source = visible state of source branch
```

The base must be a readable state, not merely the id of a mutable common
ancestor. A mutable branch segment can receive later writes, so using it as the
base would make merge depend on changes that happened after the source branch
forked. Chronos therefore creates an immutable fork-base segment during branch
creation and uses that segment as the merge base.

For the common case:

```text
main forks agent_attempt
agent_attempt merges back into main
```

the fork-base segment created at fork time is the merge base. If branches are
deeper, Chronos walks segment ancestry and picks the nearest shared immutable
fork-base segment.

### Merge Classification

Merge compares each candidate logical key in three states:

```text
B = row visible at base fork-base point
T = row visible at target branch point
S = row visible at source branch point
```

Classification:

```text
S == B and T != B              keep target
T == B and S != B              apply source
S == T                         already converged
S != B and T != B and S != T   conflict
source deleted, target same    apply delete
target deleted, source same    keep target delete
delete vs update               conflict
```

The default relational conflict granularity is row-level. Column-level merge can
be added later as an explicit policy, but the core model should not silently
combine independently updated columns because application invariants may span
columns.

### Candidate Keys

Chronos should not scan the whole table for merge preview. It can reuse
writer-segment provenance:

```text
source_writer_segments = mutable writer segments on source path after base
target_writer_segments = mutable writer segments on target path after base
candidate_writer_segments = source_writer_segments union target_writer_segments
```

Then:

```sql
SELECT DISTINCT sku
FROM products_b
WHERE writer_segment_id = ANY(:candidate_writer_segments);
```

For each candidate key, reconstruct the row at the base, source, and target
branch points using the normal visibility predicate. This gives merge preview
cost proportional to ancestry plus changed keys:

```text
O(depth(source) + depth(target) + W_delta + K_delta * point_lookup)
```

The result is a `MergePreview` containing clean changes, conflicts, and the
base segment id used for the preview. `merge_apply` must verify that the target
branch still points to the same segment observed by the preview, or recompute
the preview before applying.

### Applying a Clean Merge

For clean source-only changes, `merge_apply` writes the source result into the
target branch using the same interval write path as ordinary DML. It does not
modify the source branch and does not mutate the fork-base segment.

The merge result can optionally be recorded as metadata:

```text
merge_source_branch
merge_target_branch
merge_base_segment_id
merge_source_segment_id
merge_target_segment_id_before_apply
```

This metadata is useful for audit, visualization, and later garbage collection.

### Schema-Aware Merge

Schema merge runs before row merge. The conservative first policy is:

```text
source adds table/column, target unchanged from base       apply
source drops table/column, target unchanged from base      apply if dependencies allow
both make identical schema change                         clean
both change schema differently                            conflict
type change vs target row updates                         conflict
constraint/index changes differ                           conflict unless identical
```

If a source branch has a divergent schema version, row merge for that logical
table can proceed only after Chronos resolves how the target schema should
change. If source and target physical schema tables differ and no safe schema
resolution is available, merge preview must report a schema conflict rather
than falling back to precedence.

### Compatibility With Interval Optimizations

Fork-base segments are internal readers. Every optimization that assumes a
schema version or physical table is private must treat fork bases the same way
it treats user checkpoints.

In particular, in-place DDL on a private schema-version table is safe only when
no other live branch, checkpoint, or fork-base segment can resolve to that
schema version. Otherwise Chronos must create a new schema-version physical
table, copy the visible rows, and splice table bindings over the current
mutable segment.

The same rule applies to row fast paths. An optimization that updates a private
physical table in place is valid only when no branch, checkpoint, or fork base
can observe the old state. If a fork base can observe it, the operation must use
interval splicing so the fork-base point keeps seeing the pre-write value.

### Orpheus-Style Precedence Merge

Chronos may expose a separate precedence merge mode for compatibility with
OrpheusDB-style dataset versioning:

```text
materialize source branches in listed order
first row for a primary key wins
commit result as a new branch/version with multiple parents
```

This is not the default agentic merge model. It can silently discard one side's
change, so it should be explicit and should not be used for promotion of agent
execution results unless the caller chooses that policy.

## Garbage Collection

Segments and row intervals can be collected when they are not reachable from:

- live branch heads
- explicit checkpoints
- retained merge bases
- active transactions
- retention policies

Garbage collection must preserve the non-overlap invariant for remaining live branch points.

## Minimal Implementation Plan

For the interval live-range representation:

1. Add `branches` and `segments`.
2. Implement checkout, branch creation, and checkpoint creation.
3. Generate physical branched tables with `live_lo`, `live_hi`, and `deleted`.
4. Implement branch-visible reads with injected branch constants.
5. Implement keyed update, delete, insert, and interval splice.
6. Add non-overlap enforcement.
7. Add sparse interval allocation and depth checks.
8. Add diff, merge, and garbage collection.
9. Add branch-local schema versions:
   - `_chronos_table_schema_versions`
   - `_chronos_table_bindings`
   - table resolution from `(logical table, branch point)` to physical table
   - DDL flow that creates a new interval-backed physical table version
   - row copy from the previous visible schema version into the new branch
     segment
   - binding splices for `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE`

For the log-table representation:

1. Add `branch_timelines` and `branch_txns`.
2. Generate per-table append-only log tables.
3. Implement branch creation as timeline metadata.
4. Implement branch session checkout with visible timeline ranges.
5. Rewrite writes into append-only log records.
6. Rewrite reads to reconstruct current rows from visible logs.
7. Add indexes for point reads, adjacency reads, and branch-range scans.
8. Add a maintained current-state projection for fast arbitrary SQL.
9. Add log-aware diff, time travel by LSN, merge, and garbage collection.
