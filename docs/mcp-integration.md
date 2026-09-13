# MCP integration

Chronos includes an MCP server that lets an agent create and inspect branches,
run branch-bound SQL, and optionally work in a mounted ChronosFS filesystem. It
is a control-plane integration for MCP clients. It is not a PostgreSQL proxy and
does not intercept database connections opened by other tools.

## Install

Install Chronos with the MCP dependency. Enable the filesystem build only when
the agent needs a POSIX mount:

```sh
python -m pip install './packages/chronos-core[mcp]'
```

For ChronosFS and FUSE build requirements, see [Installation](installation.md).

## Configure a workspace

Create `chronos.toml` in the application directory:

```toml
[workspace]
name = "agent-workspace"
default_branch = "main"
state_dir = ".chronos"
mount_root = ".chronos/mounts"
allowed_source_roots = ["."]

[filesystem]
database_url = "sqlite:///.chronos/chronosfs.sqlite"

[stores.app]
kind = "sqlite"
database_url = "sqlite:///.chronos/app.sqlite"

[[stores.app.tables]]
name = "items"
primary_key = ["id"]
```

Paths in this file are resolved relative to `chronos.toml`. A configured table
must already exist the first time the server opens it. Alternatively, omit the
`tables` entry, create the table with `chronos_sql_execute`, and register it with
`chronos_register_table`.

PostgreSQL uses `kind = "postgresql"` and `data_url`. DuckDB uses a `data_url`
plus a SQLite or PostgreSQL `metadata_url`; see the accepted fields in
[`config.py`](../packages/chronos-core/src/chronos_core/mcp/config.py).

## Run the server

For a local MCP client, start the standard-input transport:

```sh
chronos-mcp --config chronos.toml
```

For development over HTTP:

```sh
chronos-mcp --config chronos.toml --transport streamable-http \
  --host 127.0.0.1 --port 8000
```

Do not expose the HTTP transport to an untrusted network without an
authentication and authorization layer.

The optional helper below creates a starter `chronos.toml` and adds a local
server entry to Codex configuration. It preserves an existing entry unless
`--overwrite` is supplied:

```sh
chronos-mcp-deploy-codex --project /path/to/project
```

The generated starter enables ChronosFS, so it requires a filesystem-enabled
build. For a relational-only installation, remove the `[filesystem]` section
before starting the server.

## Exposed operations

The server exposes tools for status, source import, sandbox creation and
deletion, filesystem mounting, branch-bound SQL query and execution, table
registration, diff, merge preview and apply, and checkpoints. Every data
operation takes an explicit branch identifier.

`chronos_prepare_source` imports a host directory only when it lies under one of
`allowed_source_roots`, and refuses to import into a non-empty filesystem
branch. The allowlist limits source import; it does not sandbox programs running
inside a mount.

## Limits

- MCP SQL has the same supported statement surface as `BranchSession`. DDL is
  executed at the store level and is not made branch-local by the MCP layer.
- The current MCP runtime creates a workspace without shared atomic metadata.
  Its multi-store merge applies stores independently; a later-store failure can
  occur after an earlier store has committed.
- Direct SQL connections, host paths outside a ChronosFS mount, network calls,
  and external services are outside branch isolation.
- FUSE mounts require Linux, `/dev/fuse`, and suitable mount permissions. A
  direct ChronosFS API can be used without mounting.

For application code that needs atomic multi-store publication, construct
`ChronosWorkspaceContext` with `shared_metadata_url` as shown in the
[multi-store guide](multi-store-branching.md).
