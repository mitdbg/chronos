---
slug: /
---

# Chronos documentation

These pages describe the Python implementation in this repository. Unless a
page says otherwise, commands and APIs apply to Chronos 0.2.0a1.

## Start here

- [Installation](installation.md) covers the minimal relational build and the
  optional DuckDB, filesystem, and S3 features.
- [Integration](integration.md) explains how to adopt existing tables and route
  application access through Chronos.
- [Compatibility and limits](compatibility.md) states what is supported and
  where direct database access can bypass branch isolation.
- [Branching introduction](branching-introduction.md) explains the data model
  and interval visibility rules without requiring implementation knowledge.

The runnable examples in
[`examples/`](https://github.com/mitdbg/chronos/tree/main/examples) are the
shortest way to exercise the API. Start with the single-store example from the
repository root:

```sh
python examples/single_store_branching.py
```

The other examples require optional database or FUSE support as applicable;
their corresponding guides describe that setup.

## Tutorials

- [Software development](tutorials/software-development.md): isolate code and
  data changes, inspect them, and publish an accepted fix.
- [RL rollouts with verl and E2B](tutorials/rl-data-sandbox.md): run a support
  ticket agent in an E2B microVM with a private database and filesystem branch,
  compute its reward, and clean up the sandbox.

The verl tutorial demonstrates the environment and tool lifecycle with scripted
generation. It does not claim to reproduce a published GPU training run.

## User guides

- [Branch transactions](branching-transaction.md) explains long-running,
  reviewable speculative work and the atomic shared-metadata merge protocol.
- [Multi-store branching](multi-store-branching.md) configures relational stores
  and ChronosFS under one workspace branch.
- [ChronosFS](filesystem-on-chronos.md) covers direct filesystem operations and
  POSIX access through FUSE.
- [MCP integration](mcp-integration.md) configures the agent-facing control
  plane and states its isolation boundary.

## Technical design

- [Bolt-on relational branching](bolt-on-branching.md) is the detailed design
  and implementation guide for interval-versioned relational tables.
- [Multi-store applications](multi-store-branching-applications.md) and the
  [multi-store abstract](multi-store-branching-abstract.md) motivate the broader
  model. They are research material, not API specifications.
- [Related work](related-work.md) is a research reading log.
- [`diagrams/`](https://github.com/mitdbg/chronos/tree/main/docs/diagrams)
  contains the Mermaid sources used by the design documentation.

The two PDFs in this directory are retained project artifacts. The Markdown
pages above are the maintained source for current behavior.

## Two PostgreSQL integration modes

This repository is a bolt-on Python library. Applications explicitly use
`ChronosBranchContext`, `BranchSession`, or `ChronosWorkspaceContext`; it is not
a PostgreSQL wire proxy or a drop-in DB-API driver.

The separate PostgreSQL engine implementation makes a database branch visible
to ordinary PostgreSQL clients and supports a broader SQL surface. It has a
different feature set, including no branch merge today. Do not assume that an
API or limitation documented for one mode applies to the other; the
[compatibility table](compatibility.md) summarizes the distinction.
