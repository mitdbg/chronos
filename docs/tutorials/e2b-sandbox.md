# Give an E2B sandbox a private database and filesystem branch

Chronos complements an execution sandbox. E2B isolates the process and its
runtime; the Chronos gateway gives that process a writable branch of the data it
is allowed to use. The sandbox connects with an ordinary PostgreSQL client and
mounts its files over NFSv4. It never receives the credentials for the backing
databases or the Chronos metadata store.

The gateway integration is experimental and targets E2B Embed on a local host
as well as private or BYOC E2B deployments. Do not expose its plaintext
PostgreSQL or NFS listeners to a public network.

## How the pieces fit together

One `chronos-gateway` process serves three interfaces. It loads NFS-Ganesha as
a library and runs its NFS workers in the same process; there is no separate
filesystem RPC service between Ganesha and ChronosFS.

- a private control API used by your rollout coordinator;
- PostgreSQL wire connections used by applications inside the sandbox; and
- NFSv4 exports used for branch-local files.

The gateway uses Chronos's existing workspace abstraction. A workspace names
the PostgreSQL stores and ChronosFS instance that share one branch history.
The control API calls that workspace's branch lifecycle: it creates a branch
for a rollout, checks out the same branch through the SQL and filesystem
frontends, and deletes it after use. Neither client can select another branch.

```text
rollout coordinator ── control API ──┐
                                     │
E2B sandbox ── PostgreSQL + NFSv4 ───┤ chronos-gateway ── data stores
                                     │
                                     └─ shared Chronos metadata
```

## Build the gateway

On Ubuntu 22.04, install the build dependencies, NFS-Ganesha's VFS module, and
the standard NFS client:

```sh
sudo apt-get install \
  build-essential cmake python3-dev pybind11-dev \
  libsqlite3-dev libpq-dev libboost-dev libboost-system-dev \
  libssl-dev libfuse3-dev nfs-ganesha nfs-ganesha-vfs nfs-common
```

Then build the gateway. If pybind11 came from pip instead of the Ubuntu
package, also pass
`-Dpybind11_DIR="$(python3 -m pybind11 --cmakedir)"` to CMake.

```sh
cmake -S packages/chronos-gateway -B build/gateway \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build/gateway -j
ctest --test-dir build/gateway --output-on-failure
```

Copy the example configuration and replace its private addresses and database
URLs:

```sh
cp deploy/chronos-gateway/chronos-gateway.example.json gateway.json
$EDITOR gateway.json
export CHRONOS_CONTROLLER_TOKEN="$(openssl rand -hex 32)"
export CHRONOS_METADATA_URL='postgresql://...'
export CHRONOS_ORDERS_DATA_URL='postgresql://...'
export CHRONOS_EVENTS_DATA_URL='postgresql://...'
export CHRONOS_FILESYSTEM_DATA_URL='postgresql://...'
sudo --preserve-env=CHRONOS_CONTROLLER_TOKEN,CHRONOS_METADATA_URL,CHRONOS_ORDERS_DATA_URL,CHRONOS_EVENTS_DATA_URL,CHRONOS_FILESYSTEM_DATA_URL \
  build/gateway/chronos-gateway --config gateway.json
```

The example loads `/usr/lib/ganesha/libganesha_nfsd.so` and the VFS module in
`/usr/lib/x86_64-linux-gnu/ganesha`. Use `dpkg -L nfs-ganesha` and
`dpkg -L nfs-ganesha-vfs` if your distribution installs them elsewhere. The
gateway needs permission to mount FUSE and to bind its configured NFS port;
running it as root is the simplest experimental setup.

The relational tables and ChronosFS source branch must already be initialized.
Follow the [multi-store branching guide](../multi-store-branching.md) to create
the shared metadata and register the tables before starting the gateway.
Every gateway store must use that separate metadata database; the gateway
rejects configurations that colocate metadata with application data. Use
least-privilege roles in each data URL, register every application table that a
sandbox may query, and reserve names beginning with `_chronos_` for Chronos.
The SQL frontend rejects that namespace so a sandbox cannot address physical
interval tables directly.

The gateway refuses wildcard listeners in plaintext mode. Its control address
should be reachable only from the rollout coordinator. E2B sandbox subnets need
access only to the configured PostgreSQL and NFS addresses.

Before involving E2B, verify the local protocol paths. The ordinary test suite
mounts the FUSE capability view when `/dev/fuse` is available. The NFSv4 test is
opt-in because it performs a real privileged client mount:

```sh
ctest --test-dir build/gateway --output-on-failure
sudo env \
  CHRONOS_GATEWAY_NFS_LIBRARY=/usr/lib/ganesha/libganesha_nfsd.so \
  CHRONOS_GATEWAY_NFS_PLUGINS=/usr/lib/x86_64-linux-gnu/ganesha \
  build/gateway/chronos_gateway_nfs_integration_tests
```

The NFS test creates two sibling branches, writes through one NFSv4.2 mount,
checks that the other branch cannot see the file, closes the first branch, and
checks that its existing mount can no longer create files.

## Test with E2B locally

[`E2B Embed`](https://github.com/e2b-dev/runtime/tree/main/embed) runs the
complete open-source E2B stack and real Firecracker microVMs on one Linux
machine. It does not use E2B Cloud. The host needs KVM,
Linux 6.8 or newer on x86-64, Docker Engine 27 or newer, at least 20 GiB of free
disk, and enough memory for its huge-page pool. Follow E2B's security guidance:
the evaluation stack binds several internal ports on every host interface and
must not be exposed to an untrusted network.

Install the pinned E2B Embed files outside the Chronos checkout and start the
stack:

```sh
mkdir -p /tmp/chronos-e2b-local
curl -fsSL --remote-name-all \
  --output-dir /tmp/chronos-e2b-local \
  "https://raw.githubusercontent.com/e2b-dev/runtime/main/embed/compose/{compose.yaml,.env}"
docker compose --project-directory /tmp/chronos-e2b-local up -d --wait
docker compose --project-directory /tmp/chronos-e2b-local \
  --profile test run --rm smoke
```

Use the Python environment containing the pinned verl checkout for the complete
integration test, and install the E2B SDK version used by Embed in that same
environment. The selected interpreter must be able to import both `verl` and
`e2b`:

```sh
VERL_PYTHON=/path/to/verl-environment/bin/python
"$VERL_PYTHON" -m pip install 'e2b==2.46.0'
cmake -S packages/chronos-gateway -B build/gateway \
  -DCMAKE_BUILD_TYPE=Release \
  -DCHRONOS_GATEWAY_E2B_PYTHON="$VERL_PYTHON"
cmake --build build/gateway -j
CHRONOS_E2B_REQUIRED=1 \
  ctest --test-dir build/gateway -R e2b_integration --output-on-failure
```

The CTest target reads the API key generated by the local Embed stack, starts a
temporary gateway fixture, and creates actual E2B microVMs. Commands inside each
VM connect through the PostgreSQL frontend and mount the NFSv4 export. The test
mutates one branch, proves that its sibling sees neither the database nor
filesystem change, closes the branch, and verifies immediate revocation. It
then runs a complete scripted verl `ToolAgentLoop` through another microVM,
including tool dispatch, PostgreSQL writes, NFS logging, reward computation,
and cleanup. It rejects non-loopback E2B API URLs, so it cannot silently run
against E2B Cloud.
Set `CHRONOS_E2B_GATEWAY_HOST` only when automatic discovery does not choose a
host address reachable from the microVMs. All fixture files live under `/tmp`.

## Prepare an E2B template

The guest needs only the normal NFS client and your application dependencies.
It does not need Chronos itself.

```dockerfile
FROM e2bdev/base

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nfs-common postgresql-client ripgrep && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir -p /mnt/chronos
```

Build this through E2B's normal template command. There is no E2B runtime or
Firecracker modification.

## Run one rollout

Install the coordinator adapter alongside the E2B SDK. Nothing from Chronos is
installed inside the sandbox.

```sh
python -m pip install ./packages/chronos-e2b
export CHRONOS_CONTROL_URL=http://chronos-control.internal:7400
export CHRONOS_CONTROLLER_TOKEN=...
export E2B_API_KEY=...
```

The adapter owns the complete branch and sandbox lifecycle:

```python
from chronos_e2b import ChronosE2B

chronos = ChronosE2B.from_env()

with chronos.branch(
    template="chronos-agent",
    workspace="training",
    from_branch="checkpoint",
    databases={
        "orders": "ORDERS_DATABASE_URL",
        "events": "EVENTS_DATABASE_URL",
    },
    filesystem="read_write",
    ttl_seconds=3600,
) as rollout:
    result = rollout.run("python agent.py", timeout=0)
    if result.exit_code != 0:
        raise RuntimeError(result.stderr)
```

Inside `agent.py`, the application uses ordinary PostgreSQL and filesystem
interfaces. Existing libraries and Unix tools require no Chronos integration:

```python
import os

import psycopg

with psycopg.connect(os.environ["ORDERS_DATABASE_URL"]) as database:
    database.execute(
        "UPDATE orders SET status = 'placed' WHERE order_id = 42"
    )

with open("/mnt/chronos/result.txt", "w") as result:
    result.write("rollout complete\n")
```

Each key in `databases` is a store alias from the gateway configuration; its
value is the environment variable injected into the sandbox. Use
`filesystem="read_only"` for a read-only mount or `filesystem="none"` when a
rollout needs only databases. Pass ordinary E2B creation options through
`sandbox_kwargs`, for example `sandbox_kwargs={"secure": True}`.

Entering the context performs these operations:

1. Create a Chronos branch from the requested checkpoint.
2. Start the E2B sandbox with branch-scoped database URLs.
3. Attach that sandbox to the branch's NFS export.
4. Mount the export at `/mnt/chronos`.

Leaving the context synchronizes and unmounts the filesystem, kills the E2B
sandbox, and closes the Chronos branch. The same cleanup runs when sandbox
creation, attachment, mounting, or the workload fails. If a create response is
lost, the adapter reconciles the client-generated branch ID instead of blindly
creating a second branch.

The adapter uses only E2B's documented template, environment, command,
metadata, and lifecycle interfaces. The template must permit a root NFS mount;
verify that capability in your E2B deployment before launching a rollout
batch.

Another rollout created from the same checkpoint sees neither the database nor
filesystem changes. The branch belongs to the configured workspace, so both
forms of state follow the same Chronos branch history.

## Run many rollouts

Create a separate branch for every rollout, including parallel samples from the
same prompt. Do not share a branch or database URL between sandboxes. A group
of 128 rollouts therefore has 128 branches and 128 NFS exports, while the
underlying checkpoint data remains shared.

Limit concurrency at the coordinator when either E2B or the gateway reports
capacity pressure. Record the Chronos workspace, branch ID, E2B sandbox ID,
prompt or episode ID, and rollout number together. These identifiers are safe
for logs; database URLs and NFS export paths are not.

## Security boundary and limitations

- Root inside the sandbox can read its branch-scoped database URLs. Their
  embedded credentials are useful only for that branch and database allowlist.
- The NFS export path is a bearer capability. A sandbox that obtains another
  rollout's export path can use it, so deliver it only to the intended sandbox
  over the private control channel. The recorded sandbox IP is not currently
  used as an NFS authentication mechanism.
- Revocation cannot erase bytes already read into guest memory or page cache.
- PostgreSQL and NFS traffic is unencrypted in this deployment model. Add a
  protected transport before crossing a network you do not control.
- Database and filesystem changes belong to the same Chronos branch, but SQL
  and NFS operations do not form one distributed transaction.
- Active branch authorization is currently held in gateway memory. Restarting
  the gateway invalidates issued credentials; authorization recovery is not
  implemented.
- The PostgreSQL frontend covers simple queries, text-format extended queries,
  transactions, and branch-aware SELECT/INSERT/UPDATE/DELETE/DDL supported by
  Chronos. It does not yet implement COPY, replication, LISTEN/NOTIFY, cancel
  requests, or binary-format extended-query values.
- Configure dedicated, least-privilege backing-store roles. SQL functions run
  with the backing role's privileges, so do not grant it administrative or
  host-filesystem functions that a sandbox should not call.
- The control API does not yet implement idempotency keys. `chronos-e2b` uses a
  client-generated branch ID and checks that ID after a lost create response;
  custom coordinators must not retry timed-out creates blindly.
- The NFS adapter uses libfuse's `noforget` mode to provide stable handles to
  Ganesha. Its inode cache can therefore grow for the life of the gateway.
- ChronosFS does not yet implement every Linux filesystem feature. Consult the
  [compatibility guide](../compatibility.md) before running workloads that need
  locks, ACLs, hard links, extended attributes, or writable shared mappings.

The gateway should fail closed when it cannot validate a branch, sandbox IP,
store alias, branch, or operation. Never fall back to a direct backing-store
connection or an ordinary host filesystem.
