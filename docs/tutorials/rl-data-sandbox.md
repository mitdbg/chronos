# Isolate database state for verl rollouts

The [verl example](../../examples/verl/README.md) connects Chronos to verl's
multi-turn tool agent. Each rollout gets a writable database branch from a
fixed checkpoint. Tool calls share that branch across turns; other rollouts
and the checkpoint retain their own state. The agent loop evaluates the final
database state before deleting the branch.

The example implements verl's actual `BaseTool` and `ToolAgentLoop` interfaces.
It includes tool and agent-loop configurations, prompt preparation, and CPU
integration tests that exercise the upstream loop and tool dispatcher. Model
generation is scripted in those tests; they do not perform an RL training update.

Start with the example's README for installation and configuration. The
implementation separates database operations from the verl adapters so a
project can replace the support-ticket tool with its own database operations.
