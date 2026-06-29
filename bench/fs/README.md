# ChronosFS Benchmarks

This directory benchmarks SQLite-backed ChronosFS against kernel `overlayfs`,
XFS reflink, Btrfs subvolume, and Turso AgentFS baselines.

Run a smoke benchmark:

```bash
bench/fs/run_fs_experiments.sh --quick --no-run-compile
```

By default this runs `chronosfs`, `overlayfs`, `btrfs`, and `turso`.
`overlayfs` uses kernel overlay mounts with `sudo -n mount/umount` and places
its lower/upper/work directories in the benchmark temp workdir, which is
typically on the host ext4 root. The optional `xfs` baseline auto-uses
`/mnt/dbfork-nvme-xfs/chronos-fs-xfs-baseline` when present; run it with
`--backends xfs` or include it in a mixed backend list. Override its location
with `CHRONOS_FS_BENCH_XFS_ROOT=/path/to/xfs/workdir`.
The `btrfs` baseline uses native Btrfs subvolume snapshots and auto-uses
`/mnt/dbfork-btrfs-loop/chronos-fs-btrfs-baseline` when present; override it
with `CHRONOS_FS_BENCH_BTRFS_ROOT=/path/to/btrfs/workdir`. `turso` requires
the `agentfs` CLI.

Run only ChronosFS:

```bash
bench/fs/run_fs_experiments.sh --backends chronosfs
```

Run the XFS reflink baseline:

```bash
bench/fs/run_fs_experiments.sh \
  --backends xfs \
  --xfs-root /path/to/xfs/workdir
```

The `xfs` backend requires an XFS filesystem with reflink support. It creates
branches with `cp -a --reflink=always`, runs the same POSIX workloads inside
the cloned branch directory, and uses filesystem-level used bytes for COW
storage deltas. Set `CHRONOS_FS_BENCH_XFS_ROOT=/path/to/xfs/workdir` to use
that XFS directory for default runs. If unset, the runner auto-detects the
local `/mnt/dbfork-nvme-xfs/chronos-fs-xfs-baseline` workdir when present.

Run the Btrfs subvolume baseline:

```bash
bench/fs/run_fs_experiments.sh \
  --backends btrfs \
  --btrfs-root /path/to/btrfs/workdir
```

The `btrfs` backend requires the `btrfs` CLI and a Btrfs filesystem. It creates
branches with writable `btrfs subvolume snapshot`, deletes branches with
`btrfs subvolume delete`, and uses filesystem-level used bytes for COW storage
deltas. The local benchmark machine uses a Btrfs filesystem mounted from a
sparse loopback image stored on the ext4 root filesystem at
`/mnt/dbfork-btrfs-loop`.

Run the Turso AgentFS baseline:

```bash
bench/fs/run_fs_experiments.sh \
  --backends turso \
  --turso-agentfs-bin agentfs
```

The `turso` backend requires the AgentFS CLI. It creates one AgentFS overlay
database per benchmark branch with `agentfs init --base ...`, mounts that
database with `agentfs mount`, and measures the same POSIX workloads through
the mounted directory. Use `--turso-root /path/to/workdir` to place those
AgentFS databases outside the temporary benchmark directory.

AgentFS uses WAL mode and disables SQLite synchronous mode for normal
filesystem operations internally, temporarily switching to FULL only for fsync.
The benchmark also configures each generated AgentFS database after `agentfs
init` with `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=OFF`,
`PRAGMA fullfsync=OFF`, and `PRAGMA checkpoint_fullfsync=OFF` by default. Use
`--turso-sqlite-synchronous NORMAL` or `FULL` to change the requested post-init
setting.

By default the benchmark runs microbenchmarks, large-file COW cases, and the
Redis compile workload. Add `--no-run-compile` to skip compilation.

The POSIX IO microbenchmarks use `--file-count` as the open-file working set
size. The read phase first performs one untimed sequential full-file pass over
every open file, then runs random reads for `--io-duration-seconds`. The write
phase runs random writes for the same duration. Both phases use `--io-size`
bytes per operation and deterministic random 4 KiB-aligned offsets.
For the Turso AgentFS baseline, each write operation is immediately followed by
`fsync()` and the fsync time is included in the sampled write latency. AgentFS
enables FUSE writeback caching, so this keeps the write metric tied to backend
SQLite commit work instead of kernel write acceptance.

The microbenchmark also runs matching direct-IO read and write phases,
reported as `file read (directio)` and `file write (directio)` in the plots.
Those phases use `O_DIRECT` with aligned `preadv`/`pwritev` buffers. For
ChronosFS and Turso, the direct-IO phases use `PRAGMA synchronous=NORMAL` and a
64 MiB SQLite cache by default. Tune those with
`--directio-sqlite-synchronous` and `--directio-cache-size`.

Direct I/O is requested on the mounted file descriptor. The overlay baseline
uses kernel `overlayfs`, so its direct-IO phases exercise the kernel overlay
implementation. For FUSE-backed systems such as ChronosFS and AgentFS,
`O_DIRECT` on the mounted file does not guarantee that the daemon's backend
storage path also bypasses the kernel page cache. ChronosFS and AgentFS still
access their SQLite databases through SQLite's normal VFS; the benchmark only
bounds SQLite's page cache for those direct-IO phases. Likewise, when Btrfs or
XFS are mounted from loopback images, the runner enables
`losetup --direct-io=on` for the loop device and rejects the backend if it
cannot verify `DIO=1`.

By default this uses 64 open files, 16 MiB file payloads, 4 KiB operations,
and 5 seconds per timed read/write phase.

Run the large-file copy-on-write granularity benchmark with explicit sizes:

```bash
bench/fs/run_fs_experiments.sh \
  --cow-file-size 64M \
  --cow-write-sizes 512,4K,64K,1M,4M
```

ChronosFS benchmark connections use `PRAGMA journal_mode=WAL` and
`PRAGMA synchronous=OFF` by default. Use
`--chronosfs-sqlite-synchronous FULL` when measuring durable SQLite commit
behavior, or `NORMAL` for a weaker but still checkpoint-aware SQLite setting.

Run the Redis compile workload with an existing Redis checkout:

```bash
bench/fs/run_fs_experiments.sh \
  --redis-source /path/to/redis
```

If `--redis-source` is omitted, the runner clones Redis from GitHub using
`--redis-repo-url` and `--redis-ref`.

The benchmark writes:

- `results.csv`: raw per-repeat rows.
- `summary.csv`: grouped medians, averages, latency p10/p50/p99, and p10/p90 columns for other metrics.
- `results.json`: config, raw rows, and summary rows.
- `fs_micro_latency.png`: branch, cached POSIX IO, and direct-IO median latency with one data label above each bar.
- `fs_micro_throughput.png`: cached POSIX IO and direct-IO throughput with p10/p90 whiskers.
- `fs_cow_large_file_latency.png`: large-file overwrite latency by write size with p10/p90 whiskers.
- `fs_cow_large_file_storage.png`: backend storage growth by write size with p10/p90 whiskers.
- `README.md`: generated run summary linking plots.

The COW workload branches from a parent containing one large file and overwrites
varied byte ranges in the child branch. It records backend storage growth so
record/block-level ChronosFS COW can be compared with file-level overlay
copy-up, XFS reflink extent COW, Btrfs subvolume COW, and Turso AgentFS
overlay COW.
The COW write path uses direct I/O. Write sizes below the direct-I/O alignment
are rounded up for the physical write while the requested logical write size is
kept as the plot parameter.
