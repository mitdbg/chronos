# ChronosFS Implementation

ChronosFS stores filesystem state in Chronos interval tables and exposes that
state through a Linux FUSE adapter. There is no worktree adapter: ordinary Unix
tools interact with a mounted filesystem, and the FUSE handlers call the
SQL-backed store directly.

## Storage Tables

`ChronosFSStore.ensure()` creates and registers three branchable logical tables:

- `chronosfs_inodes`: numeric inode metadata. Root inode is `1`.
- `chronosfs_dirents`: `(parent_inode_id, name) -> inode_id`.
- `chronosfs_file_blocks`: fixed-size file blocks keyed by
  `(inode_id, block_index)`.

The default block size is `4096` bytes. Tests can use a smaller block size to
force multi-block behavior.

The inode allocator table, `_chronosfs_inode_allocator`, is intentionally not
branchable. Inode ids are globally unique across branches; gaps are allowed.

## Copy-On-Write Model

ChronosFS does not implement its own CoW layer. It writes normal logical rows
through `ChronosBranchContext` and lets the interval backend version those rows.

For file content, the CoW unit is a logical block row:

```text
chronosfs_file_blocks(inode_id, block_index) -> data
```

When a branch rewrites one block, Chronos interval-splices the old row and adds
a new row for the branch segment. Parent and sibling branches continue to see
the old row through their branch points. There is no content-addressed block
table, extent tree, refcount table, or filesystem-level block allocator.

## FUSE Path

`mount_chronosfs(store, mountpoint, branch_id="main")` starts a blocking native
libfuse mount. By default, same-machine mounts for the same
`(database_url, block_size)` are routed through one local ChronosFS daemon
process. The daemon owns one native branch store and a branch-session cache
for each mounted branch. Mounts of the same branch share that branch state;
different branches have independent caches, locks, and SQLite connections.
Ordinary requests take a shared topology lock plus only their branch lock, so a
write on one branch does not block reads and cache operations on another.
SQLite still serializes write transactions, while WAL readers on other branch
connections can proceed concurrently. Branch creation, checkout, and merge use
the exclusive topology lock and invalidate affected in-process caches.

`start_chronosfs_mount()` adds a mount point without starting another daemon.
Run-scoped integrations can unmount their mount points and then call
`shutdown_chronosfs_daemon()` for deterministic cleanup instead of waiting for
the daemon's idle timeout.

The current adapter supports:

- `lookup`, `getattr`, `readdir`
- `open`, `create`, `read`, `write`, `flush`, `fsync`, `release`
- `mkdir`, `unlink`, `rmdir`, `rename`
- `setattr` for truncate, chmod, and utimens
- `symlink`, `readlink`

Regular file reads are range reads: the FUSE `read()` handler computes the
overlapping logical block indexes for the requested byte range and fetches only
those block rows. Regular handle writes are accumulated in a per-handle dirty
block map. Reads through that handle overlay dirty bytes on the currently
persisted blocks. `flush`, `fsync`, or `release` rereads the latest block values,
merges only bytes dirtied by that handle, and publishes all affected blocks in
one branch transaction. This avoids one SQLite commit per FUSE `write()` while
preserving nonoverlapping updates from other handles. The mount wrapper also
sets FUSE `entry_timeout=0`,
`attr_timeout=0`, and `negative_timeout=0` unless callers override them, so
kernel dentry/attribute caches do not hide updates between local mount points.

## Hard Parts

### Inode Allocation

Inode ids are allocated from `_chronosfs_inode_allocator`, which is not a
branchable table. If sibling branches allocated inode ids independently, two
different files could receive the same inode id and later collide during merge.
Global allocation gives every file identity one numeric id across the whole
ChronosFS instance.

### Partial Block Writes

Partial writes read the currently visible logical block, patch bytes in memory,
and upsert the whole block row. Chronos interval splicing is the only CoW
mechanism. Sparse regions are represented by missing block rows and read back as
zero bytes.

### Branch Switching

The FUSE adapter has one active branch id per mount. Writing an existing branch
name to `.chronos/current` switches that mount to the branch. The current v1
implementation does not yet pin already-open regular file handles to the branch
that opened them; callers should switch branches when no user file handles are
active.

### Rename

`rename()` is implemented as a SQL transaction over dirent rows and parent inode
timestamps. If the destination already exists, its inode tree is logically
deleted through Chronos interval rows before the old dirent is inserted at the
new name.

## `.chronos` Control Plane

The FUSE adapter exposes virtual control paths. They are not persisted as user
inodes.

```text
.chronos/current
.chronos/branches/
.chronos/merge-preview/
.chronos/merge-apply/
```

Implemented operations:

```bash
cat .chronos/current
mkdir .chronos/branches/agent
printf 'agent\n' > .chronos/current
cat .chronos/merge-preview/agent..main.json
cat > .chronos/merge-apply/agent..main <<'JSON'
{"policy":"weak_snapshot_isolation"}
JSON
```

`merge-preview` reports file-content conflicts as
`chronosfs_file_range` entries with `path`, byte range, and a text unified diff
when the block content is textual. Internal `inode_id` and `block_index` values
are not exposed through the control plane. `merge-apply` accepts
`abort_on_conflict`, `snapshot_isolation`, `weak_snapshot_isolation`,
`source_wins`, `target_wins`, and `manual_review` policies.

## Python API

```python
from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

ctx = ChronosBranchContext.connect("sqlite:///chronosfs.sqlite", backend="interval")
fs = ChronosFSStore(ctx)
fs.ensure()

fs.write_file("main", "/notes/todo.txt", "hello\n", parents=True)
fs.create_branch("agent", from_branch="main")
fs.write_at("agent", "/notes/todo.txt", 0, "HELLO")

mount_chronosfs(fs, "/mnt/chronosfs", branch_id="agent")
```

Important direct store operations:

- `mkdir(branch, path, parents=False)`
- `write_file(branch, path, data, parents=False)`
- `write_at(branch, path, offset, data)`
- `read_file(branch, path)` / `read_text(branch, path)`
- `truncate(branch, path, size)`
- `unlink(branch, path)` / `rmdir(branch, path)`
- `rename(branch, old_path, new_path)`
- `symlink(branch, target, link_path)` / `readlink(branch, path)`
- `diff(left, right)`
- `merge_preview(source, target, policy=...)`
- `merge_apply(source, target, resolution=None, policy=...)`

## Current Limits

- Linux FUSE support requires FUSE 3 development/runtime libraries and
  `/dev/fuse`.
- POSIX support is practical but incomplete: hardlinks, xattrs, device files,
  full permissions enforcement, writable `mmap` correctness, file locks, and
  branch-pinned open handles are not complete.
- Merge preview reports file conflicts by public path and byte range. Textual
  block conflicts include a unified diff; binary conflicts include hex payloads.
- The FUSE adapter currently shares one branch id across a mount, so branch
  checkout should be treated as a mount-level operation.
- Same-machine mount points share a daemon by default. Separate machines, or
  callers that pass `shared_daemon=False`, still need cross-daemon/cache
  invalidation before concurrent same-branch writes can be treated as a fully
  coherent distributed filesystem.
- The test suite covers both SQLite and PostgreSQL-backed stores, including a
  PostgreSQL-backed FUSE mount exercised through normal shell tools.
