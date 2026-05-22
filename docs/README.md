# Chronos Quickstart: Reliable Agents with LangChain and LangGraph

This quickstart shows how to use Chronos (Transactional Agent Runtime) to build agents that can safely edit files, run commands, and update structured memory with commit/rollback guarantees.

## What you get

- Isolated execution for file edits, bash commands, and SQLite/vector operations.
- Explicit `commit` / `abort` control for safe deployment.
- Savepoints for speculative "try -> verify -> keep or undo" workflows.

## Prerequisites

- Linux with OverlayFS support.
- Root privileges (`sudo`) for OverlayFS mount/unmount.
- Python 3.10+.
- `OPENAI_API_KEY` set if using `langchain-openai`.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip

pip install -e packages/chronos-core \
  -e packages/chronos-langchain \
  -e packages/chronos-langgraph \
  -e packages/chronos-code
```

## Quickstart 1: App-Controlled Reliability (Recommended)

Use this when your application should decide whether to persist or discard the agent's work.

```python
from pathlib import Path

from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langchain_chronos import ChronosContext

project_dir = Path("./demo_project").resolve()
project_dir.mkdir(parents=True, exist_ok=True)

model = ChatOpenAI(model="gpt-4.1-mini", temperature=0)

with ChronosContext(project_dir, enable_sqlite=True, enable_vectorstore=False) as chronos:
    tools = chronos.get_tools()  # chronos_file_editor, chronos_memory, chronos_bash, chronos_sqlite

    agent = create_agent(
        model=model,
        tools=tools,
        system_prompt=(
            "You are a careful engineering agent. "
            "Make minimal changes, run checks, and explain results briefly."
        ),
    )

    agent.invoke(
        {
            "messages": [
                {
                    "role": "user",
                    "content": "Create README.md with a 3-step setup and run ls to verify files.",
                }
            ]
        }
    )

    print("Pending Chronos changes:", len(chronos.get_changes()))

    # App-level guardrails go here (tests, policy checks, reviewers, etc.)
    checks_passed = True

    if checks_passed:
        chronos.commit()
    else:
        chronos.abort()
```

Run with root privileges:

```bash
sudo -E python quickstart_app_control.py
```

## Quickstart 2: Model-Controlled Savepoints (More Autonomous)

Use this when you want the model to call transaction primitives (`chronos_txn`) directly.

```python
from pathlib import Path

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langchain_chronos import ChronosContext
from langchain_chronos.context import ChronosTransactionControl

project_dir = Path("./demo_project").resolve()
model = ChatOpenAI(model="gpt-4.1-mini", temperature=0)

ctx = ChronosContext(project_dir, enable_sqlite=True, enable_vectorstore=False)
ctx.begin()  # Start once; model controls savepoint/rollback/commit via chronos_txn.

txn_tool = ChronosTransactionControl(chronos_context=ctx)
tools = ctx.get_tools() + [txn_tool]

graph = create_react_agent(
    model,
    tools=tools,
    prompt=(
        "A Chronos transaction is already active. "
        "Before risky edits, call chronos_txn(action='savepoint', name='...'). "
        "If verification fails, call chronos_txn(action='rollback'). "
        "Only call chronos_txn(action='commit') when all checks pass."
    ),
)

graph.invoke(
    {
        "messages": [
            {
                "role": "user",
                "content": "Refactor src/app.py, run tests, and rollback if tests fail.",
            }
        ]
    }
)

# Failsafe: do not leave an uncommitted transaction open.
if ctx.is_active:
    ctx.abort()
```

## Optional: LangGraph Auto-Savepoint Wrapper

If you already manage a `TransactionCoordinator`, you can use LangGraph's transactional helper:

```python
from langgraph.prebuilt.transactional import create_transactional_agent

agent = create_transactional_agent(
    model=model,
    tools=tools,
    coordinator=ctx.coordinator,
    auto_savepoint=True,
    include_transaction_tools=True,
)
```

## Reliability Checklist

- Put a deterministic validation layer between agent run and `commit`.
- Prefer app-controlled commit for production-critical workflows.
- Use savepoints before multi-step or destructive operations.
- Always close active transactions with `commit` or `abort`.
