# Run verl rollouts in E2B with Chronos

In this example, a verl agent handles a support ticket. It must set the payment
outage ticket (101) to high priority without changing ticket 102. Each rollout
starts from the same database state, changes its own branch, and writes a tool
audit log to its own filesystem branch. The final database state determines the
reward. Chronos closes the branch when the rollout ends; E2B destroys the
microVM.

The [example code](https://github.com/mitdbg/chronos/tree/main/examples/verl)
uses verl's `ToolAgentLoop` and `BaseTool`. The walkthrough below runs a fixed
three-action trajectory through that loop so you can check the full data path
without a GPU. It does **not** update model weights. The last section shows how
to connect the same loop to a verl training job.

## 1. Prepare a local E2B host

Run these commands from the Chronos repository root on an Ubuntu x86-64 host
with KVM, Linux 6.8 or newer, Docker Engine 27 or newer, and at least 20 GiB of
free disk. The local [E2B Embed](https://github.com/e2b-dev/runtime/tree/main/embed)
stack runs real Firecracker microVMs; this example does not use E2B Cloud. Its
evaluation configuration exposes internal ports, so keep the host on a trusted
network.

```sh
mkdir -p /tmp/chronos-e2b-local
curl -fsSL --remote-name-all \
  --output-dir /tmp/chronos-e2b-local \
  "https://raw.githubusercontent.com/e2b-dev/runtime/main/embed/compose/{compose.yaml,.env}"
docker compose --project-directory /tmp/chronos-e2b-local up -d --wait
docker compose --project-directory /tmp/chronos-e2b-local \
  --profile test run --rm smoke
```

Print the local SDK settings and export the three values in your shell before
building the template or running a rollout:

```sh
docker compose --project-directory /tmp/chronos-e2b-local \
  exec -T ready cat /run/e2b/sdk.env
```

Install Chronos and its filesystem feature using the
[installation instructions](../installation.md). Install the pinned
[verl revision](https://github.com/verl-project/verl/tree/1252cc71aa5bd82e5604322064d69bfe6454c660)
and its dependencies in the Python environment used below. Add the E2B SDK and
the coordinator adapter to that environment:

```sh
python -m pip install 'e2b==2.46.0' ./packages/chronos-e2b
export PYTHONPATH="$PWD/examples/verl${PYTHONPATH:+:$PYTHONPATH}"
```

Build the template from the included
[`e2b.Dockerfile`](https://github.com/mitdbg/chronos/blob/main/deploy/chronos-gateway/e2b.Dockerfile):

```sh
python -c 'from e2b import Template; Template.build(Template().from_dockerfile("deploy/chronos-gateway/e2b.Dockerfile"), alias="chronos-agent")'
```

It adds `psql`, the NFS client, and `rg`; it does not install Chronos inside
the microVM.

## 2. Prepare the starting data

Provision three **disposable, empty PostgreSQL databases** reachable from the
gateway host: one for tickets, one for ChronosFS, and a separate one for shared
Chronos metadata. Do not point this tutorial at an application database. Use
the same URLs for preparation and for the gateway.

```sh
export CHRONOS_TICKETS_DATA_URL='postgresql://...'
export CHRONOS_FILESYSTEM_DATA_URL='postgresql://...'
export CHRONOS_METADATA_URL='postgresql://...'
export CHRONOS_VERL_DATA=$(mktemp -d /tmp/chronos-verl-data.XXXXXX)

python -m chronos_verl.prepare \
  --database-url "$CHRONOS_TICKETS_DATA_URL" \
  --metadata-url "$CHRONOS_METADATA_URL" \
  --prompts "$CHRONOS_VERL_DATA/prompts.parquet"
```

This creates two tickets, registers the table with Chronos, records a baseline
checkpoint, and writes one verl prompt. Initialize ChronosFS against the same
metadata database:

```sh
python -c 'import os; from chronos_core.workspace.chronosfs import ChronosFSStore; fs = ChronosFSStore.connect(os.environ["CHRONOS_FILESYSTEM_DATA_URL"], metadata_url=os.environ["CHRONOS_METADATA_URL"], block_size=1048576); fs.ensure(); fs.close()'
```

If your Chronos build does not include ChronosFS, follow the
[filesystem installation instructions](../installation.md) before continuing.

## 3. Start the Chronos gateway

The gateway gives the microVM one branch-scoped PostgreSQL connection and one
NFSv4 mount. Only the trusted rollout worker can call its branch-control API.

```sh
sudo apt-get install \
  build-essential cmake python3-dev pybind11-dev \
  libsqlite3-dev libpq-dev libboost-dev libboost-system-dev \
  libssl-dev libfuse3-dev nfs-ganesha nfs-ganesha-vfs nfs-common
cmake -S packages/chronos-gateway -B build/gateway \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build/gateway -j
```

Copy [`chronos-gateway.example.json`](https://github.com/mitdbg/chronos/blob/main/deploy/chronos-gateway/chronos-gateway.example.json)
to a private location. Set the listener addresses to a host address reachable
from E2B microVMs. In `workspaces.training.postgres`, replace the example
`orders` and `events` entries with one `tickets` entry whose `data_url_env` is
`CHRONOS_TICKETS_DATA_URL`. Keep the example filesystem and metadata entries.
The control listener must be reachable only by the trusted rollout worker;
PostgreSQL and NFS listeners must be reachable by the microVMs. The gateway
rejects plaintext wildcard listeners.

```sh
cp deploy/chronos-gateway/chronos-gateway.example.json /tmp/chronos-gateway.json
$EDITOR /tmp/chronos-gateway.json
export CHRONOS_CONTROLLER_TOKEN="$(openssl rand -hex 32)"
sudo --preserve-env=CHRONOS_CONTROLLER_TOKEN,CHRONOS_METADATA_URL,CHRONOS_TICKETS_DATA_URL,CHRONOS_FILESYSTEM_DATA_URL \
  build/gateway/chronos-gateway --config /tmp/chronos-gateway.json
```

Leave the gateway running in its own terminal. NFS-Ganesha runs inside the
gateway process. The gateway needs FUSE mount permission and permission to bind
its NFS port. Its PostgreSQL and NFS listeners are plaintext; keep them on a
private network.

## 4. Run one trajectory

In another terminal, use the same Python environment and export the gateway
control URL, the token from step 3, the E2B Embed SDK variables, and the E2B
template name:

```sh
export PYTHONPATH="$PWD/examples/verl${PYTHONPATH:+:$PYTHONPATH}"
export CHRONOS_VERL_BACKEND=e2b
export CHRONOS_CONTROL_URL='http://<gateway-control-host>:7400'
export CHRONOS_CONTROLLER_TOKEN='...'
export CHRONOS_E2B_TEMPLATE='chronos-agent'
export CHRONOS_E2B_WORKSPACE=training
export CHRONOS_E2B_DATABASE=tickets
export CHRONOS_E2B_FROM_BRANCH=main
export E2B_API_KEY='...'
export E2B_API_URL='...'
export E2B_SANDBOX_URL='...'

python -m chronos_verl.demo
```

Use the SDK values printed in step 1, not cloud credentials. The command calls
`list`, `set_priority`, and `list` through verl's tool dispatcher. The
tool runs `psql` inside the microVM and writes each call to
`/mnt/chronos/verl-tool-calls.jsonl`. The printed JSON should show
`"reward": 1.0`, ticket 101 at high priority, and ticket 102 still normal.
The branch and microVM are closed after scoring, including when a tool fails.

Each new trajectory gets a new branch from `main`. A parallel rollout can
therefore change the same ticket without seeing this trajectory's write. The
model receives tool results, not the gateway control token or backing-database
credentials.

## 5. Check the real microVM path

The opt-in integration test creates a temporary gateway and data stores,
starts real local E2B microVMs, executes the verl loop, checks SQL and file
isolation, and verifies cleanup. It never falls back to E2B Cloud. Use a Python
environment that can import both `verl` and `e2b`:

```sh
cmake -S packages/chronos-gateway -B build/gateway \
  -DCMAKE_BUILD_TYPE=Release \
  -DCHRONOS_GATEWAY_E2B_PYTHON="$(command -v python)"
cmake --build build/gateway -j
CHRONOS_E2B_REQUIRED=1 \
  ctest --test-dir build/gateway -R e2b_integration --output-on-failure
```

The test reads the local E2B credentials from `/tmp/chronos-e2b-local` when
you have not exported them. It needs permission for privileged NFS mounts and
may need `CHRONOS_E2B_GATEWAY_HOST` if the host address it discovers is not
reachable from the microVMs.

## Connect to a verl training job

Keep the same gateway and E2B environment variables on every trusted rollout
worker. Add these arguments to a working verl GRPO launcher, alongside its
model, GPU, optimizer, and batch-size configuration:

```sh
data.train_files="$CHRONOS_VERL_DATA/prompts.parquet" \
data.val_files="$CHRONOS_VERL_DATA/prompts.parquet" \
data.return_raw_chat=True \
actor_rollout_ref.rollout.mode=async \
actor_rollout_ref.rollout.multi_turn.enable=True \
actor_rollout_ref.rollout.multi_turn.max_assistant_turns=8 \
actor_rollout_ref.rollout.multi_turn.max_parallel_calls=1 \
actor_rollout_ref.rollout.multi_turn.tool_config_path="$PWD/examples/verl/tool_config.yaml" \
actor_rollout_ref.rollout.agent.default_agent_loop=chronos_db_agent \
actor_rollout_ref.rollout.agent.agent_loop_config_path="$PWD/examples/verl/agent_loop.yaml"
```

This is a launcher argument block, not a standalone command. Every sampled
trajectory, including each sample in a GRPO group, receives a separate Chronos
branch and E2B microVM. The loop computes the reward from the final ticket
state before it closes them. The included one-prompt dataset and scripted demo
check the integration; **full GPU training has not been validated** for this
example. Replace the ticket task and verifier with your application before
using it to study learning.

The gateway and [verl example README](https://github.com/mitdbg/chronos/blob/main/examples/verl/README.md)
contain the implementation details. The gateway's SQL and filesystem support
is experimental; consult the [compatibility guide](../compatibility.md) before
using applications that require more than this example exercises.
