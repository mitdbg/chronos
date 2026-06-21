# Chronos Multi-Store Branching Design

**Status:** Draft, aligned with the ChronosFS direction

## Summary

Chronos exposes named mutable branches for relational data through
`ChronosBranchContext` and `BranchSession`. Multi-store branching extends the
same branch lifecycle to a workspace that can contain relational stores and a
ChronosFS filesystem store.

ChronosFS is the filesystem branch store. It stores filesystem state in Chronos
interval tables and exposes that state through a FUSE mount when normal POSIX
access is needed. It does not use an external filesystem copy-on-write layer.
Chronos interval row versioning is the filesystem copy-on-write mechanism.

A workspace branch is a shared branch name across stores:

```text
branch_id -> {
  postgresql: interval branch head for OLTP relational data,
  duckdb:     interval branch head for OLAP relational data,
  filesystem: ChronosFS interval branch head,
}
```

Applications that only need one relational store continue to use the existing
API:

```python
ctx = ChronosBranchContext.connect(dsn, backend="interval")
session = ctx.checkout("main")
rows = session.query("SELECT * FROM graph_nodes")
```

Applications that need files and multiple stores use a workspace API:

```python
from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace import ChronosPostgresStore, ChronosWorkspaceContext
from chronos_core.workspace.chronosfs import ChronosFSStore

postgresql = ChronosPostgresStore("postgresql://postgres:postgres@localhost/app")
fs_context = ChronosBranchContext.connect(
    "postgresql://postgres:postgres@localhost/app_fs",
    backend="interval",
)
filesystem = ChronosFSStore(fs_context)
filesystem.ensure()

workspace = ChronosWorkspaceContext(
    postgresql=postgresql,
    filesystem=filesystem,
)

workspace.create_branch("agent_run_1", from_branch="main")
branch = workspace.checkout("agent_run_1")

branch.postgresql.execute(
    "UPDATE graph_nodes SET score = :score WHERE node_id = :node_id",
    {"score": 0.9, "node_id": "n1"},
)
filesystem.write_file("agent_run_1", "/reports/summary.md", "# Summary\n", parents=True)
```

For subprocess execution, mount the ChronosFS branch and run tools inside that
mount:

```python
from chronos_core.workspace.chronosfs import mount_chronosfs

mount_chronosfs(filesystem, "/mnt/chronosfs-agent", branch_id="agent_run_1")
```

## Goals

- Preserve `ChronosBranchContext` and `BranchSession` as stable relational APIs.
- Let one Chronos branch include relational state and filesystem state.
- Use ChronosFS as the filesystem store.
- Store filesystem state in branchable interval tables: inodes, directory
  entries, and fixed-size file block rows.
- Provide POSIX access through ChronosFS FUSE mounts so arbitrary tools can run
  inside a branch-local filesystem.
- Keep branch creation metadata-only for both relational stores and ChronosFS.
- Support checkpoints, diff, merge apply, and branch deletion across all
  enrolled stores.
- Keep speculative agent writes private until `merge_apply`.

## Non-Goals

- Do not change existing relational-only call sites that use
  `ChronosBranchContext`.
- Do not make `ChronosBranchContext.checkout()` return a multi-store object.
- Do not require agent code to call a Chronos-specific file API when POSIX
  access is needed; tools should operate under a ChronosFS mount.
- Do not promise transparent branch switching for already-running arbitrary
  processes. Processes may hold cwd handles, file descriptors, locks, and
  mmaps.
- Do not copy entire filesystems or entire relational databases to create
  branches.
- Do not treat filesystem branching as a complete hostile-code sandbox.

## Compatibility Contract

The existing relational API remains the public contract for relational-only
users:

```python
class ChronosBranchContext:
    @classmethod
    def connect(cls, database_url: str, ...) -> "ChronosBranchContext": ...

    @classmethod
    def from_database_adapter(cls, db, ...) -> "ChronosBranchContext": ...

    def register_table(self, table: str, primary_key: list[str]) -> None: ...
    def create_branch(self, branch_id: str, from_branch: str = "main", ...) -> None: ...
    def create_checkpoint(self, checkpoint: str, branch: str = "main", ...) -> CheckpointInfo: ...
    def checkout(self, branch_id: str) -> "BranchSession": ...
    def diff(self, left: str, right: str) -> BranchDiff: ...
    def merge_apply(self, source: str, target: str, ...) -> MergeResult: ...


class BranchSession:
    branch_id: str
    current_ref: str

    def query(self, sql: str, params: dict | None = None) -> list[dict]: ...
    def execute(self, sql: str, params: dict | None = None) -> ExecuteResult: ...
    def upsert_rows(self, table: str, rows: list[dict]) -> None: ...
    def delete_keys(self, table: str, keys: list[dict]) -> None: ...
    def transaction(self): ...
```

Multi-store branching is additive:

```python
class ChronosWorkspaceContext:
    def __init__(
        self,
        filesystem: ChronosFSStore | None = None,
        stores: dict[str, BranchStore] | None = None,
        **named_stores: BranchStore,
    ): ...

    def create_branch(self, branch_id: str, from_branch: str = "main", ...) -> None: ...
    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None: ...
    def delete_branch(self, branch_id: str) -> None: ...
    def checkout(self, branch_id: str = "main") -> "WorkspaceBranchSession": ...
    def checkout_checkpoint(self, checkpoint: str) -> "WorkspaceBranchSession": ...
    def create_checkpoint(self, checkpoint: str, branch: str = "main", ...) -> dict[str, Any]: ...
    def diff(self, left: str, right: str) -> dict[str, Any]: ...
    def merge_apply(self, source: str, target: str) -> dict[str, Any]: ...
```

`WorkspaceBranchSession` exposes relational stores by system name. ChronosFS
operations are branch-id explicit at the store layer and can be exposed through
a workspace adapter or a mounted POSIX path:

```python
branch = workspace.checkout("agent")
branch.postgresql.query("SELECT ...")
branch.duckdb.query("SELECT ...")
filesystem.write_file("agent", "/artifact.txt", "...")
```

This keeps old relational code stable while giving new agent runtimes an
explicit place to access filesystem and additional store state.

## Architecture

```text
                  Agent / Application
                         |
                         v
             ChronosWorkspaceContext
                         |
        +----------------+----------------+
        |                |                |
        v                v                v
 ChronosPostgresStore  ChronosDuckDBStore  ChronosFSStore
        |                |                |
        v                v                v
 interval data plane   interval data plane interval filesystem tables
 PostgreSQL rows       DuckDB rows         inodes / dirents / blocks
        |                |                |
        +----------------+----------------+
                         |
                         v
                Chronos branch lifecycle
             create / checkout / diff / merge
```

The workspace context orchestrates branch lifecycle calls. Each store owns its
physical implementation and exposes a branch-bound session. ChronosFS uses the
same interval visibility model as relational interval stores, but its logical
records are filesystem metadata and file blocks.

## Store Driver Interface

Every multi-store participant implements the same lifecycle contract:

```python
class BranchStore:
    def create_branch(
        self,
        branch_id: str,
        from_branch: str = "main",
        metadata: dict[str, Any] | None = None,
    ) -> None: ...

    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None: ...
    def delete_branch(self, branch_id: str) -> None: ...
    def checkout(self, branch_id: str = "main") -> Any: ...
    def checkout_checkpoint(self, checkpoint: str) -> Any: ...
    def create_checkpoint(self, checkpoint: str, branch: str = "main", ...) -> Any: ...
    def diff(self, left: str, right: str) -> Any: ...
    def merge_apply(self, source: str, target: str) -> Any: ...
    def close(self) -> None: ...
```

`ChronosPostgresStore` and `ChronosDuckDBStore` fit this shape directly.
`ChronosFSStore` supplies the same branch lifecycle and filesystem operations;
workspace integration can wrap it so checkout returns a branch-bound filesystem
session or mount descriptor. Existing `ChronosBranchContext` instances can also
be used as named relational stores for compatibility.

## Relational Stores

Relational stores use `ChronosBranchContext` and `BranchSession`.

PostgreSQL remains the OLTP row-store path:

```python
postgresql = ChronosPostgresStore(
    "postgresql://postgres:postgres@localhost/app",
)
```

DuckDB is the OLAP path with transactional row-store metadata:

```python
duckdb = ChronosDuckDBStore(
    data_url="duckdb:///analytics.duckdb",
    metadata_url="postgresql://postgres:postgres@localhost/chronos_meta",
)
```

Both use interval branching for fixed-schema data operations. PostgreSQL also
supports opt-in branch-local schema branching through the existing interval
backend configuration.
DuckDB stores only physical user tables with interval visibility columns; branch
lifecycle, segment allocation, checkpoints, and registries live in the
PostgreSQL or SQLite metadata store.

## ChronosFS Store

ChronosFS is the filesystem store for multi-store branching.

It creates and registers three branchable logical tables:

```text
chronosfs_inodes
  inode_id
  kind
  mode
  uid
  gid
  size
  nlink
  symlink_target
  atime
  mtime
  ctime

chronosfs_dirents
  parent_inode_id
  name
  inode_id
  created_at

chronosfs_file_blocks
  inode_id
  block_index
  data
  valid_length
```

The inode allocator is intentionally not branchable. Inode ids are globally
unique across branches so independently-created files cannot collide during
merge.

File content is stored as fixed-size logical block rows. The default block size
is `3072` bytes:

```text
block_index = floor(offset / block_size)
block_offset = offset % block_size
```

The copy-on-write unit is a logical row:

```text
chronosfs_file_blocks(inode_id, block_index)
```

When a branch modifies one block, Chronos writes a new physical interval row
for that logical key in the writer branch segment. Parent and sibling branches
continue to see the old block row through their branch points. Unchanged blocks
remain shared by interval visibility. There is no separate filesystem layer
stack, content-addressed block store, or file-level copy-up mechanism in v1.

### POSIX Access

`ChronosFSStore` is the durable store. `mount_chronosfs(...)` is the POSIX
adapter:

```python
from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

fs = ChronosFSStore.connect(
    "postgresql://postgres:postgres@localhost/app_fs",
    backend="interval",
)
fs.ensure()
fs.create_branch("agent", from_branch="main")

mount_chronosfs(fs, "/mnt/agent", branch_id="agent")
```

Ordinary tools can operate under `/mnt/agent`. FUSE handlers call
`ChronosFSStore` methods directly; writes become Chronos interval writes in the
mounted branch.

The mount has one active branch id. Parallel agent execution should use one
mount point per active branch:

```text
/mnt/chronosfs/run_1  -> branch run_1
/mnt/chronosfs/run_2  -> branch run_2
/mnt/chronosfs/run_3  -> branch run_3
```

Switching a long-running process in place is not a core semantic. If a stable
path is needed for a process, use a mount namespace or container at process
launch:

```text
host:    /mnt/chronosfs/run_42
process: /workspace -> bind mount of run_42
```

### Direct Store API

Agents that do not need POSIX can use the store API directly:

```python
fs.write_file("agent", "/notes/todo.txt", "hello\n", parents=True)
fs.write_at("agent", "/notes/todo.txt", 0, "HELLO")
text = fs.read_text("agent", "/notes/todo.txt")
fs.rename("agent", "/notes/todo.txt", "/notes/done.txt")
```

Important operations:

- `mkdir(branch, path, parents=False)`
- `write_file(branch, path, data, parents=False)`
- `write_at(branch, path, offset, data)`
- `read_file(branch, path)` / `read_text(branch, path)`
- `truncate(branch, path, size)`
- `unlink(branch, path)` / `rmdir(branch, path)`
- `rename(branch, old_path, new_path)`
- `symlink(branch, target, link_path)` / `readlink(branch, path)`
- `manifest(branch)`
- `diff(left, right)` / `merge_apply(source, target)`

## Sandboxed Agent Execution

The main application is safe agent execution over code, files, and relational
data.

Example flow:

```python
workspace.create_branch("run_42", from_branch="main")
run = workspace.checkout("run_42")

run.postgresql.execute(
    "UPDATE graph_nodes SET quality_score = :score WHERE node_id = :node_id",
    {"score": 0.5, "node_id": "n1"},
)

# Either direct ChronosFS store operations:
filesystem.write_file("run_42", "/reports/summary.md", "# Summary\n", parents=True)

# Or POSIX execution inside a mounted ChronosFS branch:
mount_chronosfs(filesystem, "/mnt/run_42", branch_id="run_42")
subprocess.run(["python", "scripts/build_report.py"], cwd="/mnt/run_42", check=True)

preview = workspace.diff("main", "run_42")

if policy_allows(preview):
    workspace.merge_apply("run_42", "main")
else:
    workspace.delete_branch("run_42")
```

The agent can:

- Edit files.
- Generate temporary or durable artifacts.
- Run compilers, tests, linters, scripts, and data processing jobs.
- Modify relational rows.
- Modify relational schema when the relational store supports schema branching.

All effects remain branch-local until a workspace merge publishes them.

## Transactions and Atomicity

Multi-store branch operations use a staged-publish pattern. The guiding rule is:
write new store state first, then publish one transactional branch reference.

Chronos assumes that a polystore workspace has one transactional row store
owning the branch metadata plane. That store is usually PostgreSQL, but SQLite
is also valid for local deployments. Additional stores such as DuckDB,
ChronosFS, vector indexes, or search indexes store only physical user data with
Chronos visibility columns. They do not own branch lifecycle state. The
metadata store owns:

- branch heads
- interval segments
- checkpoints
- table and index registries
- staged publish records

This keeps the commit decision in one transactional database. Chronos does not
need two-phase commit across stores because additional stores never independently
publish branch visibility. They only write durable rows that remain unreachable
until the metadata store advances the target branch head.

For `create_branch`:

1. Ask each store to create branch-local metadata from the same parent branch.
2. If every store succeeds, the workspace branch exists.
3. If one store fails, best-effort cleanup deletes branches created in earlier
   stores.

For `create_checkpoint`:

1. Ask each store to create a checkpoint for the same branch.
2. Return a grouped checkpoint result keyed by store name.

For `merge_apply`:

1. Resolve a target validation token from the metadata store.
2. Compute diffs with a three-way comparison over source, target, and fork base.
3. Run deterministic conflict checks and optional application/LLM-assisted
   resolution outside the metadata publish transaction.
4. Reserve exactly one successor segment for the target branch in a short
   metadata transaction. This revalidates the target token, verifies that no
   active staged successor already exists for the target, allocates a fresh
   successor interval, and inserts the staged-publish row.
5. Run the policy-specific post-reservation validation. If the target changed
   during preview, the configured policy decides whether to abort, retry,
   rebase, apply source-over-target, or re-run LLM reconciliation against the
   new target state.
6. Write the resolved merge output into the successor segment in every
   participating store. Stores use the ordinary branch write path with the
   explicit successor segment as the destination. If a store supports
   transactions, staged writes for that store should run inside a physical store
   transaction to minimize cleanup after failures.
7. Open the final metadata publish transaction, lock the target branch row,
   revalidate the target token and staged successor row, atomically update the
   target head to `successor_segment_id`, delete the staged-publish row, and
   commit.
8. Return a grouped merge result keyed by store name.

The successor segment does not need a special segment kind. It is an ordinary
`mutable` segment that is unpublished until a branch head points at it. Publish
state is derived only from metadata reachability:

- Unpublished: staged row exists and target branch still points at the old
  segment.
- Published: target branch points at the successor segment and the staged row
  was deleted by the publish transaction.
- Abandoned: target branch moved somewhere else before publish; the staged row
  and successor rows are garbage-collection candidates.

The staged-publish table is intentionally small:

```sql
CREATE TABLE _chronos_branch_staged_publish (
  successor_segment_id INTEGER PRIMARY KEY,
  target_branch_id TEXT NOT NULL UNIQUE,
  old_target_segment_id INTEGER NOT NULL,
  old_target_revision INTEGER NOT NULL,
  source_branch_id TEXT,
  participant_stores TEXT NOT NULL,
  created_at TEXT NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}'
);
```

There is no separate `staging_id`, heartbeat, or status column. The successor
segment id identifies the attempt. The row is either present because an
unpublished attempt may need cleanup, or absent because publish completed.

Only one staged successor may exist for a target branch at a time. The
`UNIQUE(target_branch_id)` constraint makes the staged row a target-branch
publish lock. Concurrent preview, diff, and LLM reconciliation can still run
outside the lock, but only one merge enters the staged apply/publish pipeline
for a given target. This preserves the fast interval visibility predicate: a
reader does not need a reachability join to filter out multiple sibling staged
segments for the same target.

`old_target_revision` is required if the target branch can accept in-place
writes to its current segment. Every Chronos-mediated write that can change the
target branch's visible contents must advance this revision in the metadata
store. If an implementation instead makes every visible branch update publish a
new immutable segment head, then `old_target_segment_id` alone is a sufficient
validation token. Chronos' general polystore protocol should use the pair
`(old_target_segment_id, old_target_revision)` so it is correct for both modes.

The publish transaction is the only visibility commit:

```sql
BEGIN;

SELECT current_segment_id, content_revision
  FROM _chronos_branch_interval_branches
 WHERE branch_id = :target_branch_id
 FOR UPDATE;

UPDATE _chronos_branch_interval_branches
   SET current_segment_id = :successor_segment_id,
       content_revision = content_revision + 1
 WHERE branch_id = :target_branch_id
   AND current_segment_id = :old_target_segment_id
   AND content_revision = :old_target_revision;

DELETE FROM _chronos_branch_staged_publish
 WHERE successor_segment_id = :successor_segment_id;

COMMIT;
```

SQLite uses the same logical protocol inside a write transaction instead of
`FOR UPDATE`. If the conditional update affects zero rows, the target moved or
its current segment was modified. The merge attempt is then handled according
to the configured merge policy: strict snapshot isolation rejects it, while
weaker or semantic policies may retry, rebase, or re-run reconciliation against
the new head.

The row-count check is mandatory. A publish implementation must never do
`SELECT current_segment_id`, observe that it changed, and then unconditionally
overwrite the branch head with the staged successor. It also must not ignore a
revision change on the same head segment. The conditional update is the
commit-validation step. A successful publish requires exactly one updated branch
row. Any other result means the merge candidate was validated against a stale
target and cannot be exposed.

Old readers that acquired their branch read point before publish continue to
see the old segment. New readers resolve the branch after publish and see the
successor segment. This gives all participating stores atomic visibility as
long as their staged writes are durable before the metadata publish commits.
If a store-local staged write transaction fails before publish, the successor
remains invisible and GC can later remove any rows that escaped rollback.

### Concurrent Merge Control

Diff, policy checks, and LLM-assisted reconciliation must be decoupled from the
publish transaction. Holding the target branch row lock while waiting for an LLM
or external tool would recreate the long transaction problem Chronos is trying
to avoid.

Chronos instead uses optimistic concurrency control for preview and a serialized
reservation for staged apply/publish:

1. Merge preview reads `(source, target, fork_base)` at a stable target head.
2. Conflict detection and optional LLM reconciliation produce a resolved merge
   candidate for that specific target token:
   `(old_target_segment_id, old_target_revision)`.
3. A short reservation transaction locks the target branch row, checks that the
   target token still matches, checks that no staged successor exists for the
   target, allocates a successor interval, and inserts the staged row.
4. After reservation, Chronos runs any policy-required validation against the
   current target state. Under `snapshot_isolation`, a changed target usually
   means abort/retry. Under weaker or semantic policies, Chronos may rebase,
   apply source-over-target, or re-run the application/LLM resolver to minimize
   wasted work.
5. While the staged row exists, all target-visible writes for that branch must
   wait, fail, or participate in the same reserved successor protocol. The
   reservation is the publish lock for the target branch.
6. The final publish transaction revalidates the target token and staged row,
   then flips the branch head.

This handles the race where two concurrent previews both pass diff validation
against the same target head. The first merge that reserves the successor slot
enters the commit pipeline. The second merge cannot reserve while the staged
row exists. After the first merge publishes or aborts, the second merge must
consult its configured policy against the current target state before it can
reserve a new successor.

The same validation handles concurrent ordinary writes to the target branch.
Those writes may not allocate a new segment, but they must advance the target
branch content revision or be blocked by an active staged row. A merge staged
against an old revision then fails validation instead of silently overwriting
data that appeared after diff validation. Direct writes that bypass Chronos and
do not update the metadata revision are outside the protocol.

## Workspace Transactions

A workspace checkout may expose a transaction context:

```python
with branch.transaction():
    branch.postgresql.execute("...")
    branch.duckdb.execute("...")
```

This groups store-local physical transactions for branch-session writes. It is
not a global transaction across arbitrary subprocesses. Strong application
publish semantics should use branch lifecycle operations:

```python
workspace.create_checkpoint("before_agent_run", branch="main")
workspace.create_branch("agent_run", from_branch="main")
...
workspace.merge_apply("agent_run", "main")
```

## Diff

Workspace diff returns grouped store diffs:

```python
{
    "postgresql": BranchDiff(...),
    "duckdb": BranchDiff(...),
    "filesystem": ChronosFSDiff(...),
}
```

ChronosFS diff is path-based over branch-visible manifests:

```text
added       reports/summary.md
modified    scripts/build_report.py
deleted     tmp/old-output.json
mode-change scripts/run.sh
symlink     current -> reports/summary.md
```

Filesystem diff metadata should include enough information for merge and
policy:

- Path.
- Change type.
- Content hash before and after.
- File type.
- Mode.
- Symlink target.
- Optional size and mtime for display.

## Merge

Workspace merge is a store-wise merge.

Relational merge:

- Uses existing `diff` and `merge_apply` semantics for rows.
- For schema branching, requires schema-aware diff and conflict detection.

ChronosFS merge:

- Compares source and target branch-visible manifests.
- Applies accepted path changes through normal ChronosFS write/delete/rename
  operations on the target branch.
- Uses the interval backend's staged-publish behavior so target-branch readers
  do not observe partial merge writes inside one store.

Workspace conflict reporting should preserve store identity:

```text
filesystem:scripts/build_report.py modified on both branches
postgresql:graph_nodes[node_id=n1] modified on both branches
postgresql:schema graph_nodes column quality_score differs
```

The v1 ChronosFS merge is conservative and path-oriented. Rich text merge and
binary conflict-resolution policies can be layered above the store diff.

## Security and Isolation

Filesystem branching is not a complete security sandbox by itself. It isolates
filesystem writes under the mounted ChronosFS root, but arbitrary code can still
perform network calls, access other host paths if permitted, consume resources,
or call external services.

For agent execution, combine ChronosFS branching with:

- Process sandboxing: namespaces, containers, seccomp, cgroups, or a dedicated
  sandbox provider.
- Environment filtering.
- Network policy.
- Secrets isolation.
- Explicit allowed mount points.
- Timeout and resource limits.

Chronos multi-store branching provides state rollback and branch isolation. It
should not claim to be a complete hostile-code security boundary unless paired
with those controls.

## Garbage Collection

ChronosFS rows and relational row versions must be retained while reachable
from any branch or checkpoint.

GC roots:

- Workspace branches.
- Workspace checkpoints.
- Active checkouts and mounted branches.
- Staged publish rows in `_chronos_branch_staged_publish`.

ChronosFS GC should remove unreachable inode, dirent, and block row versions
only after no branch/checkpoint/read point can see them. Relational GC can use
the same interval reachability logic.

For staged publish cleanup, GC must preserve any successor segment referenced by
a staged-publish row until the system decides the attempt is abandoned. A
staged row whose target branch still matches
`(old_target_segment_id, old_target_revision)` can be retried or explicitly
aborted. A staged row whose target branch points somewhere else, or whose
content revision has advanced, is stale and can be collected after the retention
policy allows it.

## Failure Modes

### Store create succeeds, later store create fails

The workspace context attempts best-effort cleanup for stores that already
created the branch. If cleanup fails, the orphaned store branch is unreachable
from the intended workspace branch and can be collected later.

### ChronosFS mount fails

Checkout of the store can still succeed, but POSIX execution through that mount
fails. Agent runtimes that require POSIX filesystem access should fail closed.

### Process keeps files open while branch is deleted

Deletion should mark the branch deleted and defer mount shutdown or row GC until
active checkout references are released or a timeout expires.

### Staged writes succeed, metadata publish fails

The target branch still points at the old segment, so staged rows are invisible.
The staged-publish row remains as a cleanup root. The merge can either retry the
same publish if the target head is unchanged, or abort and let GC remove the
successor later.

### Target branch moves before publish

The publish transaction validates `current_segment_id = old_target_segment_id`.
With Chronos-managed writes, the active staged row should prevent another merge
or ordinary write from advancing the target during staged apply. If the target
still moves because of recovery, timeout, manual intervention, or a write path
outside the reservation protocol, validation fails and the transaction rolls
back. The stale successor must not be published. The next action is selected by
the merge policy: strict snapshot isolation rejects or retries, while semantic
policies may rebase or re-run conflict resolution against the new target head.

### Metadata publish succeeds, process crashes before returning

The branch head already points at the successor and the staged-publish row was
deleted in the same transaction. Recovery treats the merge as committed. A
client that did not receive the response can safely inspect the branch head or
use an idempotency key in higher-level orchestration.

## Open Questions

- Should the workspace catalog live inside the relational Chronos metadata
  database permanently, or should it be pluggable?
- Should ChronosFS support a highly optimized local interval data plane in
  addition to SQL-backed interval tables?
- What block size should be the default for agent code execution workloads?
- How should ChronosFS expose conflict-aware text merges without turning the
  filesystem store into a source-control system?
- Should workspace `merge_apply()` be allowed when one store has conflicts and
  another does not, or should all store conflicts block the entire merge?
- What retention policy should decide when abandoned staged successors are
  cleaned?

## Proposed Incremental Plan

1. Keep `ChronosWorkspaceContext` as an additive API. Do not change
   `ChronosBranchContext`.
2. Use system names for store sessions: `branch.postgresql`, `branch.duckdb`,
   and `branch.fs`.
3. Use `ChronosFSStore` as the filesystem store. Store inode, dirent, and block
   rows in Chronos interval tables.
4. Use `mount_chronosfs(...)` for POSIX execution inside a branch.
5. Add workspace diff that groups relational and ChronosFS changes.
6. Publish multi-store `merge_apply` through the transactional metadata-store
   branch-head swap.
7. Add richer ChronosFS merge preview and conflict reporting.
8. Add schema-branching support behind the PostgreSQL relational store.
9. Add GC and compaction for ChronosFS row versions and old relational versions.
10. Explore a high-performance local ChronosFS data plane that still supports
    interval visibility predicates and secondary indexes.
