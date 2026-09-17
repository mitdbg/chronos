# Isolate database state for verl rollouts

The [verl example](https://github.com/mitdbg/chronos/tree/main/examples/verl)
connects Chronos to verl's multi-turn tool agent. Each rollout gets a writable
branch. Tool calls share that branch across turns; other rollouts and the source
retain their own state. The agent loop evaluates the final database state
before deleting the branch.

The example implements verl's actual `BaseTool` and `ToolAgentLoop` interfaces.
It includes tool and agent-loop configurations, prompt preparation, and CPU
integration tests that exercise the upstream loop and tool dispatcher. Model
generation is scripted in those tests; they do not perform an RL training update.

Start with the example's README for installation and configuration. The
implementation separates database operations from the verl adapters so a
project can replace the support-ticket tool with its own database operations.

For lightweight development, the verl worker can access the branch directly.
For isolated execution, set `CHRONOS_VERL_BACKEND=e2b`. The same agent loop then
uses `chronos-e2b` to create one E2B microVM per trajectory, inject its
branch-scoped PostgreSQL URL, and mount the corresponding ChronosFS branch. The
support-ticket tool executes inside that microVM and writes an audit trail to
the mounted filesystem. Cleanup of the database branch, filesystem branch, and
runtime sandbox follows the trajectory lifecycle.

The E2B mode has a live integration test against E2B Embed. It executes the verl
episode in a real Firecracker microVM and verifies final-state reward, SQL and
filesystem isolation, revocation, and cleanup. See the
[E2B tutorial](e2b-sandbox.md) for gateway and local E2B setup.
