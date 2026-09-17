# Chronos gateway

`chronos-gateway` gives an unmodified runtime sandbox access to one branch of a
Chronos workspace through ordinary PostgreSQL and NFSv4 clients. A trusted
coordinator uses the private HTTP control API to create the branch, choose its
database and filesystem access, and attach it to a sandbox. The gateway keeps
the resulting credentials out of Chronos's public workspace model.

The API follows the existing workspace lifecycle. For example,
`POST /v1/workspaces/training/branches` corresponds to
`workspace.create_branch(...)`; SQL and NFS both check out the returned branch.

The executable contains the control server, PostgreSQL wire frontend,
ChronosFS capability view, and NFS-Ganesha server in one process. Ganesha is
loaded as a library and reads the in-process ChronosFS FUSE mount through its
VFS adapter; no separate Chronos filesystem RPC daemon is used.

Build and test from the repository root:

```sh
cmake -S packages/chronos-gateway -B build/gateway -DCMAKE_BUILD_TYPE=Release
cmake --build build/gateway -j
ctest --test-dir build/gateway --output-on-failure
```

`chronos_gateway_postgres_tests` is a pgwire compatibility matrix. It connects
with libpq and covers joins, CTEs, aggregates, subqueries, set operations,
prepared parameters, transactions and error recovery, DML, and branch-local
DDL. The default run uses temporary SQLite data and metadata stores for a fast
test. Set both `CHRONOS_GATEWAY_TEST_POSTGRES_DATA_URL` and
`CHRONOS_GATEWAY_TEST_POSTGRES_METADATA_URL` to empty PostgreSQL databases to
run the same matrix against PostgreSQL. CI runs both modes.

The test suite includes an opt-in end-to-end test against E2B Embed running on
the same machine. It creates real Firecracker microVMs and exercises both the
PostgreSQL and NFS paths; it never uses E2B Cloud. See the tutorial for the
one-time host setup and the required test command.

The complete setup and E2B walkthrough are in the
[sandbox tutorial](../../docs/tutorials/e2b-sandbox.md). The example runtime
configuration is in
[`deploy/chronos-gateway`](../../deploy/chronos-gateway/chronos-gateway.example.json).
The [`chronos-e2b`](../chronos-e2b) coordinator adapter automates branch
creation, database URL injection, NFS attachment, synchronization, and cleanup.
