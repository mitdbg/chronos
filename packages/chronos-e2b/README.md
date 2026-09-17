# chronos-e2b

`chronos-e2b` starts an E2B sandbox with a private Chronos database and
filesystem branch. It creates the branch through `chronos-gateway`, injects
branch-scoped database URLs, mounts the matching NFSv4 export, and cleans up
both the sandbox and branch.

```python
from chronos_e2b import ChronosE2B

chronos = ChronosE2B.from_env()

with chronos.branch(
    template="chronos-agent",
    workspace="training",
    from_branch="checkpoint",
    databases={"orders": "DATABASE_URL"},
) as rollout:
    result = rollout.run("python agent.py", timeout=0)
```

See the [E2B sandbox tutorial](https://github.com/mitdbg/chronos/blob/main/docs/tutorials/e2b-sandbox.md)
for gateway and template setup. The
[verl example](https://github.com/mitdbg/chronos/tree/main/examples/verl)
uses this adapter to give every agentic RL trajectory its own E2B microVM and
matching database/filesystem branch.
