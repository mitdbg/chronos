# Database sandboxes for verl

This example gives each verl rollout a Chronos branch. A model uses a
support-ticket tool to inspect two tickets and raise the payment outage's
priority. The reward is one only when ticket 101 is high priority and ticket
102 remains normal. It is computed from the final database state, not the
model's claim that it completed the task.

There are two execution modes:

- `local` creates the database branch directly in the verl worker. It is the
  lightweight default for development and CPU integration tests.
- `e2b` creates an E2B microVM backed by a branch-scoped PostgreSQL connection
  and ChronosFS mount. Tool SQL and file operations execute inside the microVM;
  `chronos-e2b` owns setup and cleanup.

The integration targets verl commit
[`1252cc71aa5bd82e5604322064d69bfe6454c660`](https://github.com/verl-project/verl/tree/1252cc71aa5bd82e5604322064d69bfe6454c660).
At that revision, [`ToolAgentLoop._call_tool`](https://github.com/verl-project/verl/blob/1252cc71aa5bd82e5604322064d69bfe6454c660/verl/experimental/agent_loop/tool_agent_loop.py)
calls `create` and `release` for every tool invocation. The branch therefore
belongs to `ChronosDatabaseAgentLoop.run`, not to an individual tool instance.
This distinction keeps an earlier turn's writes visible to later turns.

## Run the database integration tests

Install Chronos using the [relational build instructions](../../docs/installation.md).
No filesystem, DuckDB, or S3 feature is needed here. Use an environment with the
pinned verl checkout and its dependencies installed, then run from the Chronos root.

```sh
export PYTHONPATH="$PWD/examples/verl${PYTHONPATH:+:$PYTHONPATH}"
python -m pip install pytest pytest-asyncio pytest-timeout
python -m pytest \
  examples/verl/test_e2b_episode.py \
  examples/verl/test_integration.py \
  -q --timeout=60
```

Set `CHRONOS_VERL_TEST_POSTGRES_DSN` to a disposable PostgreSQL database to run
the PostgreSQL cases as well. The test fixture drops its `public` schema. Never
use an application database for that variable.

These CPU tests run verl's actual agent-loop state machine and tool dispatch
against Chronos. They replace model generation and tokenization with scripted
actions. They check parallel rollouts, repeated resets, cross-turn writes,
final-state rewards, model exceptions, cancellation, malformed tool arguments,
the E2B episode lifecycle, and cleanup. They do not launch Ray workers, train a
model, or measure learning.

The gateway's opt-in `chronos_gateway_e2b_integration_tests` target additionally
runs both the episode and the complete scripted verl agent loop in real local
E2B Firecracker microVMs. It checks tool dispatch, database mutation, reward,
ChronosFS audit trail, sibling isolation, and cleanup.

## Prepare data

For a single-node check, choose a new local directory and initialize a database.
The command intentionally fails if the tutorial table already exists.

```sh
export CHRONOS_VERL_DATA=$(mktemp -d /tmp/chronos-verl-data.XXXXXX)
export CHRONOS_DATABASE_URL="sqlite:///$CHRONOS_VERL_DATA/tickets.sqlite"
python -m chronos_verl.prepare \
  --database-url "$CHRONOS_DATABASE_URL" \
  --prompts "$CHRONOS_VERL_DATA/prompts.parquet"
```

For distributed workers, set `CHRONOS_DATABASE_URL` to a dedicated PostgreSQL
database reachable from every worker before running the preparation command.
This example uses the Chronos library on ordinary PostgreSQL; it does not
require the PostgreSQL fork. Do not share a SQLite file across nodes or put it
on a network filesystem. SQLite serializes writers, so PostgreSQL is the useful
next step when evaluating concurrent database workloads.

The seed contains one task to make the integration inspectable. Replace it with
a real task corpus before interpreting training results. Task-specific starting
states can use separate checkpoints selected by trusted rollout configuration.

## Run rollouts in E2B

First configure `chronos-gateway` and build an E2B template as described in the
[verl and E2B tutorial](../../docs/tutorials/rl-data-sandbox.md). The gateway
workspace needs a PostgreSQL store alias named `tickets` and a ChronosFS store.
The data and metadata URLs below must be the same URLs used by that gateway.

Initialize the source data before starting rollouts:

```sh
export CHRONOS_TICKETS_DATA_URL='postgresql://...'
export CHRONOS_METADATA_URL='postgresql://...'
python -m chronos_verl.prepare \
  --database-url "$CHRONOS_TICKETS_DATA_URL" \
  --metadata-url "$CHRONOS_METADATA_URL" \
  --prompts "$CHRONOS_VERL_DATA/prompts.parquet"
```

Install the coordinator adapter on every verl rollout worker and select the E2B
backend:

```sh
python -m pip install ./packages/chronos-e2b
export CHRONOS_VERL_BACKEND=e2b
export CHRONOS_CONTROL_URL=http://chronos-control.internal:7400
export CHRONOS_CONTROLLER_TOKEN=...
export CHRONOS_E2B_TEMPLATE=chronos-agent
export CHRONOS_E2B_WORKSPACE=training
export CHRONOS_E2B_DATABASE=tickets
export CHRONOS_E2B_FROM_BRANCH=main
export E2B_API_KEY=...
```

Run one complete, scripted trajectory through verl's real `ToolAgentLoop` and
tool dispatcher before starting a training job:

```sh
python -m chronos_verl.demo
```

The command prints the final reward, Chronos branch and E2B sandbox identifiers,
and each tool result. It replaces model generation with three fixed actions, but
uses the same agent loop, tool, E2B microVM, gateway, database, filesystem, and
cleanup path as a training rollout.

Use the same verl launcher configuration in the next section. For each
trajectory, `ChronosDatabaseAgentLoop` now creates an E2B sandbox and one
matching Chronos branch. The tool runs `psql` inside the sandbox and records its
calls in `/mnt/chronos/verl-tool-calls.jsonl`. On completion, exception, or
cancellation, the adapter synchronizes the filesystem, destroys the microVM,
and closes the branch.

The verl worker is trusted: it receives the Chronos controller token and creates
branches. The model receives only structured tool results. Its E2B tool sandbox
receives branch-scoped database credentials and one opaque NFS export. Never put
the controller token in the E2B template or sandbox environment.

## Configure verl

Install `chronos_verl` on the Python path of the driver and every rollout worker.
Pass the following overrides to your working verl GRPO launcher, retaining its
model, GPU, optimizer, and batch-size configuration. Paths must be available on
the workers. Local mode requires `CHRONOS_DATABASE_URL`; E2B mode uses the
gateway variables shown above.

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

This block contains launcher arguments, not a standalone shell command. Use a
tool-capable model and its supported parser/chat template. The same tiny file
is used for training and validation above only for an integration check, not
for an accuracy evaluation. Full GPU training has not been validated for this
example yet.

`ChronosDatabaseAgentLoop` sets `AgentLoopOutput.reward_score` before deleting
the branch. The pinned verl worker accepts an existing trajectory reward and
does not send it through the asynchronous reward function again. Tool calls
return zero step reward, so repeated inspections cannot accumulate reward.

## How database state is owned

Each `run` creates a new random branch, including each sample in a GRPO group.
Local mode starts from `tickets_baseline`; E2B mode starts from the configured
gateway source branch. A server-generated token links tool calls to that
episode. The model cannot select a branch through tool parameters. The tool
only offers validated ticket operations; it does not accept arbitrary SQL or
shell commands from the model.

One database connection and executor thread serve each active episode. Blocking
SQL stays off the async rollout thread, and calls within an episode execute in
order. No database transaction remains open during model generation. Independent
episodes can execute concurrently. The configured single parallel call per
turn also makes action ordering explicit to the model.

Normal completion, a model exception, and task cancellation all close the
session and branch. The E2B adapter also destroys the runtime sandbox. A worker
killed without cleanup can leave resources behind until their configured TTLs
expire. Use a dedicated training workspace and an external job supervisor for
reclamation. Never delete branches merely because they look old while workers
may still be using them.

Checkpoint reservations are persistent and disjoint. Deleting an episode does
not reuse its interval, which avoids exposing old tuple versions after reset.
Capacity is finite; the prototype has no checkpoint deletion or interval
compaction API. Use fresh training databases/checkpoints as part of a planned
retention policy rather than assuming indefinite reuse.

To adapt the example, replace the episode's `_execute` and `score` methods with
the application's operations and verifier. Keep branch selection and cleanup in
the agent loop. Local mode does not provide process or network isolation. E2B
mode provides the runtime boundary, while Chronos supplies its database and
filesystem state; neither mode gives model code metadata or controller
credentials.
