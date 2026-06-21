# ChronosFS Benchmarks

This directory benchmarks SQLite-backed ChronosFS against the basic
`fuse-overlayfs` branch store baseline.

Run a smoke benchmark:

```bash
bench/fs/run_fs_experiments.sh --quick --no-run-compile
```

Run only ChronosFS:

```bash
bench/fs/run_fs_experiments.sh --backends chronosfs
```

By default the benchmark runs microbenchmarks, large-file COW cases, and the
Redis compile workload. Add `--no-run-compile` to skip compilation.

The POSIX IO microbenchmarks use `--file-count` as the open-file working set
size and issue `--file-ops-per-file` read/write operations per open file. This
keeps a group of files open and cycles operations across that set instead of
opening each file for a single read or write.

By default this uses 64 open files, 32 KiB file payloads, and 4 KiB overwrites
at deterministic random 4 KiB-aligned offsets for the `partial_overwrite`
phase.

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
- `summary.csv`: grouped medians and averages.
- `results.json`: config, raw rows, and summary rows.
- `fs_micro_latency.png`: branch and POSIX operation latency.
- `fs_micro_throughput.png`: read/write throughput.
- `fs_cow_large_file_latency.png`: large-file overwrite latency by write size.
- `fs_cow_large_file_storage.png`: backend storage growth by write size.
- `README.md`: generated run summary linking plots.

The COW workload branches from a parent containing one large file and overwrites
varied byte ranges in the child branch. It records backend storage growth so
record/block-level ChronosFS COW can be compared with file-level overlay
copy-up.
