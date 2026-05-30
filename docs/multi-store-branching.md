# Chronos Multi-Store Branching Design

**Status:** Draft

## Summary

Chronos currently exposes named mutable branches for relational data through
`ChronosBranchContext` and `BranchSession`. OKG depends on that API for graph
storage, so the relational API must remain stable.

This document proposes a new multi-store branch layer that keeps the existing
relational API intact while adding filesystem state to the same branch
abstraction. A branch becomes a workspace-wide state vector:

```text
branch_id -> {
  relational: relational branch head,
  filesystem: filesystem layer head,
}
```

Applications can continue using the existing relational API:

```python
ctx = ChronosBranchContext.connect(dsn)
session = ctx.checkout("default")
rows = session.query("SELECT * FROM okg.graph_nodes")
```

New sandboxed agent execution can opt into a workspace API:

```python
workspace = ChronosWorkspaceContext(
    relational=ChronosBranchContext.connect(dsn),
    filesystem=ChronosFilesystemStore(root="/repo"),
)

workspace.create_branch("agent_run_1", from_branch="default")
branch = workspace.checkout("agent_run_1")

branch.sql.execute("ALTER TABLE okg.graph_nodes ADD COLUMN score DOUBLE PRECISION")
branch.fs.run(["pytest", "-q"], cwd=".")
```

The relational backend defaults to the interval backend. Multi-store callers do
not need to choose a relational branch backend unless a non-default backend is
explicitly required.

## Goals

- Preserve `ChronosBranchContext` and `BranchSession` as stable relational APIs.
- Let one Chronos branch include both relational state and filesystem state.
- Provide SQL access for relational data, including branch-local schema changes.
- Provide POSIX access for filesystem state so arbitrary code can run against a
  branch-local filesystem view.
- Use `fuse-overlayfs` as the default filesystem copy-on-write primitive.
- Support sandboxed agent execution where file edits, generated artifacts,
  subprocess side effects, and relational mutations can be discarded or merged.
- Keep branch creation cheap for both stores.
- Make branch checkout cheap by returning a branch-bound SQL session and a
  mounted filesystem path.
- Support checkpoints, rollback, diff, merge preview, merge apply, and branch
  deletion across all enrolled stores.

## Non-Goals

- Do not change existing OKG call sites that use `ChronosBranchContext`.
- Do not make `ChronosBranchContext.checkout()` return a multi-store object.
- Do not require agent code to use a Chronos-specific file API. Filesystem
  access should be normal POSIX access through a mounted path.
- Do not promise transparent branch switching for already-running arbitrary
  processes. Processes may hold cwd handles, open file descriptors, locks, and
  mmaps.
- Do not make cross-store commit depend on copying entire filesystems or
  entire relational databases.

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
    def transaction(self): ...
```

Multi-store branching is additive:

```python
class ChronosWorkspaceContext:
    def __init__(
        self,
        relational: ChronosBranchContext,
        filesystem: ChronosFilesystemStore | None = None,
        *,
        stores: list[ChronosStore] | None = None,
    ): ...

    def create_branch(self, branch_id: str, from_branch: str = "main", ...) -> None: ...
    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> None: ...
    def delete_branch(self, branch_id: str) -> None: ...
    def list_branches(self) -> list[WorkspaceBranchInfo]: ...
    def get_branch(self, branch_id: str) -> WorkspaceBranchInfo: ...

    def checkout(self, branch_id: str) -> "WorkspaceBranchSession": ...
    def checkout_checkpoint(self, checkpoint: str) -> "WorkspaceBranchSession": ...

    def create_checkpoint(self, checkpoint: str, branch: str = "main", ...) -> WorkspaceCheckpointInfo: ...
    def diff(self, left: str, right: str) -> WorkspaceDiff: ...
    def merge_preview(self, source: str, target: str) -> WorkspaceMergePreview: ...
    def merge_apply(self, source: str, target: str, resolution: WorkspaceMergeResolution | None = None) -> WorkspaceMergeResult: ...
```

`WorkspaceBranchSession` composes store-specific access APIs:

```python
class WorkspaceBranchSession:
    branch_id: str

    @property
    def sql(self) -> BranchSession: ...

    @property
    def fs(self) -> FilesystemBranchSession: ...
```

This keeps existing relational code stable while giving new sandboxing code an
explicit place to ask for filesystem access.

## Architecture

```text
                  Agent / Application
                         |
                         v
             ChronosWorkspaceContext
                         |
        +----------------+----------------+
        |                                 |
        v                                 v
 ChronosBranchContext              ChronosFilesystemStore
 existing relational API           new POSIX branch store
        |                                 |
        v                                 v
 interval relational backend        fuse-overlayfs layers
 PostgreSQL / SQLite                repo/workspace filesystem
```

The workspace context owns branch lifecycle orchestration. Each store owns its
own physical branching implementation and exposes a store-specific checked-out
session.

## Store Driver Interface

Every multi-store participant implements the same lifecycle surface:

```python
class ChronosStore:
    store_id: str

    def ensure(self) -> None: ...

    def create_branch(self, branch_id: str, from_branch: str, metadata: dict | None) -> StoreRef: ...
    def create_branch_from_checkpoint(self, branch_id: str, checkpoint: str) -> StoreRef: ...
    def delete_branch(self, branch_id: str) -> None: ...

    def checkout(self, branch_id: str) -> StoreSession: ...
    def checkout_checkpoint(self, checkpoint: str) -> StoreSession: ...

    def create_checkpoint(self, checkpoint: str, branch: str, metadata: dict | None) -> StoreCheckpointInfo: ...

    def diff(self, left: str, right: str) -> StoreDiff: ...
    def merge_preview(self, source: str, target: str) -> StoreMergePreview: ...
    def merge_apply(self, source: str, target: str, resolution: StoreMergeResolution | None) -> StoreMergeResult: ...
```

The existing `ChronosBranchContext` can be adapted as a relational store driver
without changing its public API.

## Branch Catalog

The workspace layer needs durable metadata that maps one logical branch to one
store ref per store:

```text
_chronos_workspace_branches
  branch_id          text primary key
  created_at         text
  metadata           json/text

_chronos_workspace_branch_store_refs
  branch_id          text
  store_id           text
  current_ref        text
  metadata           json/text
  primary key (branch_id, store_id)

_chronos_workspace_checkpoints
  checkpoint_id      text primary key
  branch_id          text
  created_at         text
  metadata           json/text

_chronos_workspace_checkpoint_store_refs
  checkpoint_id      text
  store_id           text
  ref                text
  metadata           json/text
  primary key (checkpoint_id, store_id)
```

Initially this catalog can live in the same relational database as Chronos
metadata. That keeps branch names and checkpoint names authoritative in one
place. Filesystem metadata points to layer IDs and mount roots, not to copied
workspace contents.

## Relational Store

The relational store remains `ChronosBranchContext`.

The default relational backend is interval. Callers can keep using:

```python
ctx = ChronosBranchContext.connect(dsn)
```

The multi-store wrapper should not force callers to pass `backend="interval"`.

### Schema Changes

Agent sandboxes need to safely modify relational data and schema. There are two
phases:

1. Preserve the existing row-branching behavior for registered tables.
2. Add branch-local schema support behind the same relational SQL API.

Schema branching should be represented in the relational store, not in the
workspace layer. The workspace only coordinates lifecycle. The relational store
should make schema-visible SQL correct for a checked-out branch.

Possible implementation directions:

- Branch-visible logical catalog. Store table, column, index, and constraint
  definitions with branch visibility metadata, then rewrite SQL to physical
  storage.
- Per-branch physical schemas for DDL-heavy sandboxes. This is simpler for DDL
  correctness but can be more expensive.
- Hybrid model. Use interval row branching for stable registered tables and
  per-branch physical objects for newly-created or structurally-divergent
  tables.

The design requirement is API-level, not implementation-specific:

```python
branch.sql.execute("ALTER TABLE nodes ADD COLUMN score DOUBLE PRECISION")
branch.sql.query("SELECT node_id, score FROM nodes")
```

Those changes must be branch-local until merged.

## Filesystem Store

The filesystem store provides a POSIX-visible branch view using
`fuse-overlayfs` by default.

```python
class ChronosFilesystemStore:
    def __init__(
        self,
        root: str | Path,
        state_dir: str | Path | None = None,
        mount_strategy: Literal["fuse-overlayfs", "kernel-overlayfs"] = "fuse-overlayfs",
    ): ...
```

Checkout returns:

```python
class FilesystemBranchSession:
    branch_id: str
    path: Path

    def run(
        self,
        argv: list[str],
        *,
        cwd: str | Path = ".",
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> CompletedProcess: ...
```

Agents and subprocesses should interact with files through `session.fs.path` or
through `session.fs.run(...)`.

The filesystem API is the control plane, not the data plane. Once a branch is
checked out and mounted, file reads and writes do not have to pass through a
Chronos file API. Any POSIX-compatible tool that operates under the mounted
branch path reads and writes branch-local state.

For MCP servers and coding agents, expose the mounted branch path as the tool
root:

```python
branch = workspace.checkout("agent_run_1")

mcp_file_server(root=branch.fs.path)
mcp_shell_server(cwd=branch.fs.path)
```

In that setup, Claude, Codex, or another client can use ordinary file read,
file write, and shell tools. Writes under `branch.fs.path` go to the branch
`upperdir`; reads see the branch's merged view. Writes to the original base
repository path bypass Chronos and must be blocked by the MCP server or process
sandbox.

### Layer Model

The filesystem should use persistent layers rather than committing by copying
an upperdir into the root directory.

```text
root snapshot / imported base layer
  + immutable committed layers for branch lineage
  + mutable writable upperdir for current branch head
  = merged checkout path
```

Branch metadata stores the current filesystem layer head:

```text
branch_id -> fs_ref = layer_head_id
checkpoint_id -> fs_ref = immutable layer_head_id
```

Creating a branch records the parent's current immutable layer head as the
fork point and creates an empty writable upperdir for the child. It does not
copy repository contents.

Checkpointing seals the current writable upperdir as an immutable layer and
starts a new empty writable upperdir:

```text
before:
  lower = base + L1 + L2
  upper = U3

checkpoint:
  L3 = seal(U3)
  checkpoint.ref = L3
  upper = empty U4
```

This gives checkpoints stable semantics even if the parent branch is mutated
later.

`fuse-overlayfs` supports this model through multiple lower directories and one
writable upper directory:

```text
lowerdir = L3:L2:L1:base
upperdir = U4
merged   = mounts/parent
```

The newest lower layer is listed first. A child branch created from the parent
checkpoint mounts the same immutable lower stack with its own upperdir:

```text
child lowerdir = L3:L2:L1:base
child upperdir = child_U1
child merged   = mounts/child
```

Unmodified files remain shared because they are absent from `L3` and are read
from older layers or `base`. Modified files, created files, whiteouts for
deletions, and opaque directory markers live in the sealed layer.

### Checkpoint Mechanics

A filesystem checkpoint seals the branch's current `upperdir` as a new
immutable delta layer. It does not copy the full tree.

```text
Initial:
  base/
    a.txt
    b.txt
    c.txt

Parent writes:
  modify b.txt
  delete c.txt
  create d.txt

Parent upper contains only:
  b.txt          # modified copy
  c.txt whiteout # delete marker, representation depends on overlay mode
  d.txt          # new file
```

Checkpointing turns that upperdir into a layer:

```text
layers/L3/
  b.txt
  c.txt whiteout
  d.txt
```

Then a child mounted with `lowerdir=L3:L2:L1:base` sees:

```text
a.txt from base or an older layer
b.txt from L3
c.txt hidden by the L3 whiteout
d.txt from L3
```

The checkpoint algorithm is:

```text
1. Acquire a write lease for the branch.
2. Stop or wait for active subprocesses and MCP file operations.
3. sync filesystem state.
4. Unmount the branch merged path.
5. Rename current upperdir to a new layer directory.
6. Record the new layer ref in filesystem metadata.
7. Create a new empty upperdir and workdir.
8. Remount the branch with lowerdir = new_layer:old_layers:base.
9. Release the write lease.
```

The implementation should preserve overlay metadata exactly. Prefer
`rename(upperdir, layer_dir)` after unmounting. If copying is unavoidable, use a
metadata-preserving copy/archive path that preserves whiteouts, opaque directory
markers, symlinks, modes, and xattrs.

Do not make sealed layers immutable by recursively changing file modes, for
example with `chmod -R a-w`. That changes user-visible permissions when the
layer is later used as a lowerdir and can break copy-up writes in future
overlays. Layer immutability should be enforced by Chronos metadata, private
state directory permissions, not exposing layer paths to agents, and never
using a sealed layer as an `upperdir`. If stronger enforcement is needed, use
ownership, mount namespaces, or read-only bind mounts without changing the file
modes stored inside the layer.

### Mounts and Checkout

Each active checkout gets its own mounted path:

```text
$CHRONOS_STATE/fs/mounts/default
$CHRONOS_STATE/fs/mounts/agent_run_1
$CHRONOS_STATE/fs/mounts/agent_run_2
```

Switching branches means selecting a different mounted path or running a
process with a different cwd. It does not require remounting a global
`/workspace` path.

For stronger sandbox ergonomics, `fs.run(...)` may execute in a private mount
namespace and bind the selected branch mount to a stable path such as
`/workspace` inside that process. That should be a process launch behavior, not
a mutation of the host process's global filesystem view.

### Out-of-Band Access

Filesystem branching intentionally supports out-of-band reads and writes. A
tool does not need a Chronos client library if it is rooted at the branch mount.

```text
base repo:
  /srv/repos/project

branch mount:
  /var/lib/chronos/fs/mounts/agent_run_1

MCP file root:
  /var/lib/chronos/fs/mounts/agent_run_1
```

The MCP server should treat the branch mount as the only allowed project root.
The base repo path and Chronos state internals should not be exposed. Otherwise
an agent could bypass the branch by writing directly to `/srv/repos/project` or
by mutating layer directories.

Open file descriptors are also out-of-band. If a process opened files before a
checkpoint, unmount, or branch deletion, the filesystem store must either wait
for that process to finish or fail/defer the operation.

### Why Not Parent Merged As Child Lowerdir

Overlaying a child branch over a live parent merged directory is acceptable for
short-lived nested transactions, but it is not correct for long-lived named
branches. If the parent branch later changes, those changes can leak into the
child through the child's lowerdir.

Copy-on-write only means writes through the child mount are copied into the
child `upperdir`; it does not freeze the child's `lowerdir`. If the child uses
the parent's live merged mount as its lowerdir, later parent writes change that
lowerdir:

```text
parent:
  lowerdir = base
  upperdir = parent_upper
  merged   = mounts/parent

child:
  lowerdir = mounts/parent
  upperdir = child_upper
  merged   = mounts/child
```

If the parent later creates `e.txt`, `mounts/parent/e.txt` appears. The child
has no override for `e.txt`, so lookup falls through to `mounts/parent` and the
child sees a file that did not exist at branch creation time.

Named branches should fork from immutable parent layer heads or checkpoints.
That preserves snapshot isolation:

```text
create_branch(child, from_branch=parent)
  child.fork_ref = parent.current_immutable_layer_head
```

## Sandboxed Agent Execution

The main application is safe agent execution over code, files, and relational
data.

Example flow:

```python
workspace.create_branch("run_42", from_branch="default")
run = workspace.checkout("run_42")

with run.transaction():
    run.sql.execute("ALTER TABLE okg.graph_nodes ADD COLUMN quality_score DOUBLE PRECISION")
    run.sql.execute("UPDATE okg.graph_nodes SET quality_score = 0.5")
    result = run.fs.run(["python", "scripts/build_report.py"], cwd=".")

preview = workspace.diff("default", "run_42")

if result.returncode == 0 and policy_allows(preview):
    workspace.merge_apply("run_42", "default")
else:
    workspace.delete_branch("run_42")
```

The agent can:

- Edit files.
- Generate temporary or durable artifacts.
- Run compilers, tests, linters, scripts, and data processing jobs.
- Modify relational rows.
- Modify relational schema.

All effects remain branch-local until a workspace merge publishes them.

## Transactions and Atomicity

Multi-store branch operations need an orchestration protocol. The guiding rule
is: write new store state first, then publish branch refs.

For `create_branch`:

1. Begin workspace metadata transaction.
2. Ask each store to create branch-local metadata.
3. Insert store refs into workspace metadata.
4. Commit workspace metadata transaction.

For `create_checkpoint`:

1. Ask each store to produce an immutable checkpoint ref.
2. Insert all checkpoint refs into workspace metadata.
3. Commit metadata.

For `merge_apply`:

1. Compute merge preview for each store.
2. Validate policy and conflict resolution.
3. Ask each store to prepare immutable post-merge state for the target branch.
4. Publish all new target store refs in one workspace metadata transaction.
5. Garbage collect orphaned prepared refs asynchronously.

This avoids requiring filesystem copy operations to be part of a relational
database transaction. The filesystem store prepares immutable layers, and the
workspace catalog controls which layers are visible.

If any store fails before the metadata publish, no branch refs change. If the
metadata publish succeeds but cleanup fails, the branch is still correct and
cleanup can be retried.

## Workspace Transactions

A workspace checkout may expose a transaction context:

```python
with branch.transaction():
    branch.sql.execute("...")
    branch.fs.run(["python", "generate.py"])
```

This should be treated as a convenience for grouping operations and running
store-local rollback on exception. It should not be confused with a single
kernel-level or database-level transaction across arbitrary subprocesses.

For strong publish semantics, use branch-level checkpoint and merge:

```python
workspace.create_checkpoint("before_agent_run", branch="default")
workspace.create_branch("agent_run", from_branch="default")
...
workspace.merge_apply("agent_run", "default")
```

## Diff

Workspace diff returns grouped store diffs:

```python
WorkspaceDiff(
    left="default",
    right="agent_run",
    stores={
        "relational": BranchDiff(...),
        "filesystem": FilesystemDiff(...),
    },
)
```

Filesystem diff should be path-based:

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

Workspace merge is a store-wise merge followed by a single metadata publish.

Relational merge:

- Uses existing `diff` and `merge_apply` semantics for rows.
- For schema branching, requires schema-aware diff and conflict detection.

Filesystem merge:

- Uses a three-way merge with common fork ref:

```text
base = common ancestor layer head
source = source branch layer head
target = target branch layer head
```

- Non-conflicting file additions, modifications, deletes, mode changes, and
  symlink changes can be applied automatically.
- Text file conflicts may optionally produce conflict markers or require an
  explicit resolution.
- Binary conflicts require explicit source/target choice.

Workspace conflict reporting should preserve store identity:

```text
filesystem:scripts/build_report.py modified on both branches
relational:okg.graph_nodes[node_id=n1] modified on both branches
relational:schema okg.graph_nodes column quality_score differs
```

## Branch Switching Semantics

Relational switching is already cheap:

```python
session = ctx.checkout("branch")
```

Filesystem switching should be:

```python
branch = workspace.checkout("branch")
subprocess.run(["pytest"], cwd=branch.fs.path)
```

or:

```python
branch.fs.run(["pytest"], cwd=".")
```

Switching a long-running process in place is not a core semantic. If a stable
path is needed for a process, use a mount namespace at process launch:

```text
host:    /chronos/state/fs/mounts/run_42
process: /workspace -> bind mount of run_42
```

## Security and Isolation

Filesystem branching is not a complete security sandbox by itself. It isolates
filesystem writes under the mounted root, but arbitrary code can still perform
network calls, access other host paths if permitted, consume resources, or call
external services.

For agent execution, combine filesystem branching with:

- Process sandboxing: namespaces, containers, seccomp, cgroups, or a dedicated
  sandbox provider.
- Environment filtering.
- Network policy.
- Secrets isolation.
- Explicit allowed mount points.
- Timeout and resource limits.

Chronos multi-store branching should provide state rollback. It should not
claim to be a complete hostile-code security boundary unless paired with those
controls.

## Garbage Collection

Filesystem layers and relational row versions must be retained while reachable
from any branch or checkpoint.

GC roots:

- Workspace branches.
- Workspace checkpoints.
- Active checkouts.
- In-progress prepared merge refs.

Filesystem GC can delete unreferenced immutable layers and abandoned upperdirs
after a grace period. Relational GC can use existing Chronos retention logic for
old row versions and schema versions.

## Failure Modes

### Store create succeeds, metadata publish fails

The store ref is orphaned. It is safe to collect later because no workspace
branch points to it.

### Filesystem mount fails

Checkout fails for the filesystem store. Relational sessions remain valid only
if the caller requested relational-only access. A full workspace checkout should
fail closed.

### Process keeps files open while branch is deleted

Deletion should mark the branch deleted and defer unmount/layer deletion until
active checkout references are released or a timeout expires.

### Merge partially prepares stores, then one store fails

No workspace branch ref is published. Prepared refs from successful stores are
orphaned and GC handles them.

### Metadata publish succeeds, cleanup fails

The branch state is correct. Cleanup is retried asynchronously.

## Open Questions

- Should the workspace catalog live inside the relational Chronos metadata
  database permanently, or should it be pluggable?
- How much schema branching should be implemented before the filesystem store
  ships?
- Should filesystem checkpoints be explicit only, or should every successful
  `fs.run(...)` optionally seal a layer?
- What is the maximum overlay lowerdir stack depth we want to support before
  mandatory compaction?
- Should `FilesystemBranchSession.run()` use a container provider by default in
  agent-facing packages?
- Should workspace `merge_apply()` be allowed when one store has conflicts and
  another does not, or should all store conflicts block the entire merge?

## Proposed Incremental Plan

1. Add `ChronosWorkspaceContext` as an additive API. Do not change
   `ChronosBranchContext`.
2. Implement a relational store adapter around existing `ChronosBranchContext`.
   Use interval as the default relational backend.
3. Implement `ChronosFilesystemStore` with `fuse-overlayfs`, mounted checkouts,
   path-based diff, explicit checkpointing, and branch deletion.
4. Add workspace branch catalog tables and metadata-publish orchestration.
5. Support sandboxed `fs.run(...)` with cwd rooted in the branch checkout.
6. Add workspace diff that groups relational and filesystem changes.
7. Add filesystem three-way merge for simple path changes.
8. Add schema-branching support behind the relational store.
9. Add cross-store merge preview and merge apply with conflict reporting.
10. Add GC and compaction for filesystem layers and old relational versions.

The first filesystem implementation should include an integration test that
mounts a parent branch, seals the parent upperdir as a layer, mounts a child
from that sealed layer plus base, mutates the parent again, and verifies that
the child continues to see the checkpoint state rather than later parent
writes. A second test should seal a child layer and mount a grandchild from
`child_layer:parent_layer:base` to validate branch-on-branch behavior.
