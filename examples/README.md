# Chronos Examples

Run these from the repository root:

```bash
PYTHONPATH=packages/chronos-core/src python3 examples/run_all.py
```

Each script creates its own temporary state and asserts the expected branch
behavior before printing a success line.

- `single_store_branching.py`: SQL branching with one SQLite store.
- `multi_store_branching.py`: one branch spanning SQLite rows and ChronosFS.
- `branch_apis.py`: lifecycle operations: branch, checkout, checkpoint, merge,
  and delete.
- `chronosfs_direct.py`: ChronosFS direct Python API without FUSE.
- `branch_transactions.py`: branch transaction commit across SQL and ChronosFS.
- `chronosfs_fuse_control.py`: optional `.chronos/` POSIX control-plane example.
  It skips itself when Linux FUSE is unavailable.
