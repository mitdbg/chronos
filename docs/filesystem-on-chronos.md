# Filesystem on Chronos

**Status:** Current behavior in Chronos 0.2.0a1

## Summary

ChronosFS is a SQL-backed virtual filesystem whose persistent state is stored in
Chronos interval tables. It does not implement a second filesystem-level
copy-on-write layer. Chronos row interval versioning is the copy-on-write
mechanism.

The implementation provides:

- A storage layer, `ChronosFSStore`, that registers filesystem tables with the
  Chronos interval backend.
- A native Linux FUSE adapter, `mount_chronosfs`, implemented with libfuse.
- A small POSIX control plane under `.chronos/` for branch operations using
  ordinary file reads, writes, and mkdir.

There is no worktree adapter in ChronosFS. Ordinary tools such as `bash`, `cp`,
`mv`, `rm`, `truncate`, `dd`, `sha256sum`, and `find` operate on a mounted FUSE
filesystem. FUSE handlers call the Chronos SQL-backed store directly.

TigerFS is the main local reference for the adapter surface: Linux FUSE,
dot-directory control paths, and database-backed file operations. ChronosFS
intentionally differs in storage. TigerFS models workspace files at a more
file-first layer, while ChronosFS maps file data into small branchable block
rows and lets Chronos intervals version those rows.

## Goals

- Provide a filesystem path that ordinary tools and agents can use.
- Keep all durable filesystem state in Chronos-backed SQL tables.
- Use one branch API across relational data and filesystem data.
- Make branch creation metadata-only: no file tree, file row, or block payload
  is copied at fork time.
- Support block-level branch isolation by choosing small logical filesystem
  records and letting Chronos interval row versioning handle writes.
- Keep filesystem branching on the same native interval backend as relational
  stores instead of implementing a second branch algorithm.

## Non-Goals

- Do not build an independent CoW filesystem inside Chronos.
- Do not add content-addressed blocks, extent trees, refcounts, or filesystem
  block garbage collection in the current implementation.
- Do not implement a materialized worktree import/export adapter.
- Do not target complete POSIX behavior in the current implementation. Hardlinks, writable `mmap`
  correctness, device files, mandatory locks, quotas, xattrs, and full Unix ACLs
  are out of scope.

## Storage Model

ChronosFS stores filesystem objects as ordinary logical tables registered with
the Chronos interval backend. The physical interval tables receive the standard
Chronos visibility columns such as branch segment bounds and tombstone state.

### Inodes

Inode ids are numeric, not UUIDs. Root inode is `1`. New inodes are allocated
from a small global allocator table. That table is intentionally not branchable:
branch-local allocation could assign the same numeric inode to unrelated files
in sibling branches and later collide during merge.

```sql
CREATE TABLE chronosfs_inodes (
  inode_id BIGINT PRIMARY KEY,
  kind TEXT NOT NULL,
  mode INTEGER NOT NULL,
  uid INTEGER NOT NULL,
  gid INTEGER NOT NULL,
  size BIGINT NOT NULL,
  nlink INTEGER NOT NULL,
  symlink_target TEXT,
  atime TEXT NOT NULL,
  mtime TEXT NOT NULL,
  ctime TEXT NOT NULL
);
```

### Directory Entries

Directory entries map a visible parent directory and name to a visible child
inode.

```sql
CREATE TABLE chronosfs_dirents (
  parent_inode_id BIGINT NOT NULL,
  name TEXT NOT NULL,
  inode_id BIGINT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (parent_inode_id, name)
);
```

Path lookup walks visible `chronosfs_dirents` rows under the current branch
point. Rename updates dirent rows and parent inode timestamps inside one SQL
transaction.

### File Blocks

File contents are stored in fixed-size logical blocks. The default block size is
`4096` bytes.

```sql
CREATE TABLE chronosfs_file_blocks (
  inode_id BIGINT NOT NULL,
  block_index BIGINT NOT NULL,
  data BYTEA NOT NULL,
  valid_length INTEGER NOT NULL,
  PRIMARY KEY (inode_id, block_index)
);
```

The logical mapping is:

```text
block_index = floor(offset / 4096)
block_offset = offset % 4096
```

The block table is a Chronos interval table. Updating a block means upserting
the logical key `(inode_id, block_index)` through a checked-out Chronos branch
session. Chronos writes a new physical row version over the writer branch
segment and interval-splices older physical rows. Parent and sibling branches
continue to see their previous block rows through their branch points.

This is the current copy-on-write mechanism. Blocks are not content-addressed,
not refcounted, and not shared through a separate filesystem allocator. Sharing
comes from unchanged Chronos row versions remaining visible across branches.

## Branch Semantics

ChronosFS reuses the existing interval branch lifecycle:

- `create_branch(child, from_branch=parent)` creates branch metadata and copies
  no filesystem rows.
- `create_checkpoint(name, branch)` records a stable read point.
- `create_branch_from_checkpoint(branch, checkpoint)` creates a branch view from
  a checkpoint.
- `diff(left, right)` compares visible path manifests.
- `merge_preview(source, target, policy=...)` reports row/file conflicts.
- `merge_apply(source, target, ...)` commits accepted source changes through the
  native branch transaction commit path.

Reads do not walk ancestry manually. A Chronos branch session supplies one
branch point, and table reads use the standard interval visibility predicate.

## FUSE Adapter

`mount_chronosfs(store, mountpoint, branch_id="main")` starts a blocking FUSE
mount. The adapter has one active branch id per mount.

Implemented operations:

- `lookup`, `getattr`, `readdir`
- `open`, `create`, `mknod`, `read`, `write`, `flush`, `fsync`, `release`
- `mkdir`, `unlink`, `rmdir`, `rename`
- `setattr` for truncate and chmod
- `symlink`, `readlink`
- `access`, `statfs`

Regular file reads are range reads. The FUSE `read()` handler computes the
overlapping logical block indexes for the requested byte range and fetches only
those block rows, filling sparse gaps with zeroes. Regular file writes are
synchronous at the FUSE write-handler boundary. A write patches affected logical
blocks and upserts them through Chronos immediately. `flush()` and `fsync()`
currently have little buffered regular-file work to do, but they are
implemented so tools that call them receive normal responses.

## POSIX Semantics

The current target is a practical agent workspace filesystem, not a complete
general-purpose POSIX filesystem.

Implemented or partially implemented:

- Atomic path `rename` at SQL transaction commit.
- Open, create, write, truncate, unlink, rmdir, mkdir, symlink, readlink.
- Sparse reads as zeroes for missing block rows.
- `chmod` through `setattr`.
- Basic `statfs` and permissive `access`.

Known limits:

- No hardlinks yet.
- No xattrs, device files, quotas, or full ACL enforcement.
- Writable `mmap` correctness is not guaranteed.
- File locks are not implemented.
- The mount has one active branch id. Branch switching should be done when no
  user file handles are active; branch-pinned handles are future work.
- `chown` and timestamp-setting through `setattr` are not complete.

## `.chronos` Control Plane

ChronosFS exposes a proc-like virtual control plane under `.chronos/`. These
paths are not persisted as user inodes.

```text
.chronos/current
.chronos/branches/
.chronos/merge-preview/
.chronos/merge-apply/
```

Implemented workflows:

```bash
# Read the currently checked-out branch for this mount.
cat .chronos/current

# Create a branch from the current branch.
mkdir .chronos/branches/agent_run_1

# Checkout a branch into this mount.
printf 'agent_run_1\n' > .chronos/current

# List known branches.
ls .chronos/branches/

# Preview source-into-target merge.
cat .chronos/merge-preview/agent_run_1..main.json

# Commit accepted changes with a policy.
cat > .chronos/merge-apply/agent_run_1..main <<'JSON'
{"policy":"weak_snapshot_isolation"}
JSON
```

Preview files are virtual JSON files named `<source>..<target>.json`. Apply
files accept JSON written to `.chronos/merge-apply/<source>..<target>` or
`.chronos/merge-apply/<source>..<target>.json`. The preview/apply paths are not
tied to the mount's current branch, so operators usually run them from a
main-branch mount while worker mounts remain checked out to private branches.

ChronosFS file conflicts are reported as public file-range conflicts:

```json
{
  "table": "chronosfs_file_range",
  "key": {
    "path": "/src/solution.py",
    "byte_range": {"start": 4096, "end": 8192}
  },
  "before": {"encoding": "utf-8", "text": "target text"},
  "after": {
    "encoding": "utf-8",
    "text": "source text",
    "unified_diff": "--- target:/src/solution.py@bytes:4096-8192\n+++ source:/src/solution.py@bytes:4096-8192\n..."
  },
  "conflict_id": "..."
}
```

The control plane hides internal `inode_id` and `block_index` values. Agents
see paths, byte ranges, and text diffs when content is textual. Manual
resolution writes choices keyed by the preview's `conflict_id`:

```bash
cat > .chronos/merge-apply/agent..main <<'JSON'
{
  "policy": "manual_review",
  "conflicts": {
    "CONFLICT_ID_FROM_PREVIEW": "source"
  }
}
JSON
```

Supported choices are `source`/`theirs` and `target`/`ours`/`skip`. Supported
policies are `abort_on_conflict`, `snapshot_isolation`,
`weak_snapshot_isolation`, `source_wins`, `target_wins`, and `manual_review`.

## Python API

Filesystem-only setup:

```python
from chronos_core.branching import ChronosBranchContext
from chronos_core.workspace.chronosfs import ChronosFSStore, mount_chronosfs

ctx = ChronosBranchContext.connect(
    "postgresql://postgres:postgres@localhost:5432/chronos",
    backend="interval",
)

fs = ChronosFSStore(ctx)
fs.ensure()

fs.create_branch("agent_run_1", from_branch="main")
mount_chronosfs(fs, "/mnt/chronosfs-agent-run-1", branch_id="agent_run_1")
```

Direct store methods are used by tests and by the FUSE adapter:

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

Existing relational-only users continue to use `ChronosBranchContext` directly.

## Diff and Merge

The current diff is path-oriented and built from visible branch manifests:

- Added path: visible on right only.
- Deleted path: visible on left only.
- Metadata change: mode, size, symlink target, or kind differs.
- Content change: file hash differs.

The merge path uses the same interval merge machinery as relational stores.
For file-content conflicts, the public preview reports paths and byte ranges,
while internally mapping choices back to block rows. `weak_snapshot_isolation`
and `source_wins` choose source content for conflicts; `target_wins` skips
source conflict rows; `manual_review` requires explicit choices by
`conflict_id`.

## Performance Notes

The `4096`-byte default block size is intentionally small enough to make branch
isolation fine-grained while keeping row payloads modest. The block size is
stored as filesystem metadata and should be treated as fixed for a filesystem
instance.

Important logical access patterns:

- Lookup by `(parent_inode_id, name)`.
- Directory listing by `parent_inode_id`.
- File read by `(inode_id, block_index)` range.
- Block upsert by `(inode_id, block_index)`.

Physical index DDL should be generated by the Chronos interval registration
layer where possible, because physical interval table names and visibility
columns are backend-owned.

## Reliability and Isolation

- SQL transactions provide operation atomicity and durability.
- Chronos interval predicates provide branch isolation.
- All filesystem state must be accessed through branch-bound Chronos sessions.
- Direct writes to physical ChronosFS tables outside Chronos are unsupported and
  can break branch invariants.
- The mounted path is not a complete security sandbox. Agent execution still
  needs process sandboxing, mount restrictions, environment filtering, secrets
  policy, and resource limits.

## Implemented Tests

The test suite includes:

- Direct branch isolation for files and manifests.
- Fixed-size block behavior across branches.
- Truncate and sparse reads.
- Checkpoint restore via Chronos branch APIs.
- Rename, symlink, and basic merge apply.
- Real FUSE mount tests using `bash`, `mkdir`, `printf`, `cp`, `mv`, `rm`,
  `truncate`, `dd`, `ln`, and `cat`.
- `.chronos` branch creation and checkout through POSIX paths.
- A larger FUSE test that creates 200 files and writes/reads a 2 MiB file
  through normal Unix tools.
- PostgreSQL-backed direct block-interval tests and PostgreSQL-backed FUSE
  tests using ordinary shell tools.

## Future Work

- Branch-pinned file handles and `EBUSY` on unsafe checkout.
- Read-only checkpoint mounts.
- Rich `.chronos/diff` and `.chronos/checkpoints` directory views.
- Higher-level whole-file and semantic merge helpers built on top of the
  existing byte-range conflict preview.
- Hardlinks, xattrs, locks, timestamp setting, and stronger permissions.
- PostgreSQL stress tests under concurrent writers.
