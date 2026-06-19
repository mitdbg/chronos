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

`mount_chronosfs(store, mountpoint, branch_id="main")` starts a blocking
`pyfuse3` mount. The current adapter supports:

- `lookup`, `getattr`, `readdir`
- `open`, `create`, `mknod`, `read`, `write`, `flush`, `fsync`, `release`
- `mkdir`, `unlink`, `rmdir`, `rename`
- `setattr` for truncate and chmod
- `symlink`, `readlink`
- `access`, `statfs`

Regular file reads are range reads: the FUSE `read()` handler computes the
overlapping logical block indexes for the requested byte range and fetches only
those block rows. Regular file writes are synchronous at the FUSE write-handler
boundary: `write()` patches the affected logical blocks and upserts them through
the current Chronos branch. `flush()` and `fsync()` are therefore mostly
durability barriers for the underlying SQL transaction path today. The only
buffered writes are `.chronos` control-file writes, because those need complete
command text.

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
.chronos/status
.chronos/ctl
.chronos/branches/
.chronos/checkpoints/
.chronos/diff/
.chronos/merge-preview/
```

Implemented operations:

```bash
cat .chronos/current
cat .chronos/status
mkdir .chronos/branches/agent
printf 'agent\n' > .chronos/current
printf 'create-branch exp from main\n' > .chronos/ctl
printf 'create-checkpoint before-edit\n' > .chronos/ctl
printf 'delete-branch exp\n' > .chronos/ctl
printf 'merge exp into main\n' > .chronos/ctl
```

`.chronos/checkpoints`, `.chronos/diff`, and `.chronos/merge-preview` are
reserved virtual directories for richer read-only views.

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
- `diff(left, right)` / `merge_apply(source, target)`

## Current Limits

- Linux FUSE support requires `pyfuse3`, `trio`, FUSE 3 development/runtime
  libraries, and `/dev/fuse`.
- POSIX support is practical but incomplete: hardlinks, xattrs, device files,
  full permissions enforcement, writable `mmap` correctness, file locks, and
  branch-pinned open handles are not complete.
- The v1 merge implementation is path-oriented and conservative; conflict-aware
  merge preview remains future work.
- The FUSE adapter currently shares one branch id across a mount, so branch
  checkout should be treated as a mount-level operation.
- The test suite covers both SQLite and PostgreSQL-backed stores, including a
  PostgreSQL-backed FUSE mount exercised through normal shell tools.
