# Chronos CLI

The `chronos` command is a one-off wrapper around `ChronosWorkspaceContext` and
its named store APIs. A workspace can contain one store or several. Each command
opens its configured stores, calls the API, prints the returned data, and closes
connections. Stored data persists; the CLI keeps no background connection or
current-branch state.

## Install

The CLI ships with `chronos-core`. Follow [installation](installation.md) for
native build dependencies. From the repository root, install the workspace package:

```sh
uv sync --package chronos-core
uv run --package chronos-core chronos --help
```

You do not need to activate the virtual environment when using `uv run`.
An installed wheel also provides `chronos`; `python -m chronos_core.cli` invokes
the same application.

On macOS with Homebrew Python, build against the same SQLite library Python uses.
If Python uses Homebrew SQLite, explicitly select it when rebuilding:

```sh
brew install sqlite libpq boost
CMAKE_PREFIX_PATH="$(brew --prefix libpq)" \
uv sync --package chronos-core --reinstall-package chronos-core \
  -C "cmake.define.SQLite3_INCLUDE_DIR=$(brew --prefix sqlite)/include" \
  -C "cmake.define.SQLite3_LIBRARY=$(brew --prefix sqlite)/lib/libsqlite3.dylib"
```

Mixing Python's Homebrew SQLite connection with a native extension linked to
macOS's SQLite can crash the process. After a custom build, use
`uv run --no-sync --package chronos-core chronos ...` to run the installed build
without syncing it again.

## Workspaces

Configure named stores in `chronos.toml`. For example, one SQLite store:

```toml
[stores.app]
kind = "sqlite"
database_url = "sqlite:///.chronos/app.sqlite"
```

Save the example as `chronos.toml` and create `.chronos/` first. Local database URLs and Qdrant paths resolve relative
to the TOML file. Configuration precedence is `--config PATH`, then
`CHRONOS_WORKSPACE_CONFIG`, then `chronos.toml` in the current directory. A
workspace name is not required. Connection strings may reference `${ENV_VAR}`;
unset references are errors. `.env` files are not loaded automatically.

From the repository root, run the commands below. If `chronos` is already on
your PATH, you can use it directly in place of the `uv run` invocation.

```sh
mkdir -p .chronos
uv run --package chronos-core chronos table create --store app \
  'CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)'
uv run --package chronos-core chronos table register --store app items --primary-key id
uv run --package chronos-core chronos branch create experiment
uv run --package chronos-core chronos query --store app --branch experiment 'SELECT * FROM items'
```

SQL participants support `sqlite`, `postgresql`, and `duckdb` kinds, with
`database_url` and optional `metadata_url` (required for DuckDB). Qdrant uses
`kind = "qdrant"`, `metadata_url`, and exactly one of `path` or `url`; remote
connections also accept `api_key` and `timeout`. Select vector operations with
`chronos qdrant --store NAME ...`.

Local database URLs use percent-encoded filenames, resolved relative to the
configuration file. For example, `sqlite:///data%23trial.sqlite` names
`data#trial.sqlite`; `%2520` names a literal `%20`. Characters in the config
directory are encoded automatically. DuckDB paths containing `?` are rejected
because the bundled DuckDB version cannot construct their WAL paths correctly.

Branches, checkpoints, diffs, and merges operate across the workspace. With
independent metadata, `merge preview/apply` calls the coordinated API, which
can retain partial changes if a later participant fails. Atomic mode requires
`[workspace].shared_metadata_url` and matching metadata URLs for every store.
Multiple SQL participants sharing metadata require a nonempty `merge_tables`
scope for each store. See the atomic SQL example below. Use
`merge atomic-preview` to inspect the candidate changes and
`merge atomic-apply --operation-id ID` to publish them. Pass the returned
`--preview-token` to reject a changed preview. Ordinary `diff` can fail in
split-data workspaces because the Python API's diff does not use merge scopes.
See the filesystem sections below for OverlayFS and native ChronosFS setup.

## Checked terminal demo

After installation, run `bash examples/cli_workspace_demo.sh --help` from the
repository. The default demo uses PostgreSQL plus embedded Qdrant; set
`CHRONOS_DEMO_POSTGRES_URL` to a fresh demo database, then run:

```sh
bash examples/cli_workspace_demo.sh
```

For SQLite plus embedded Qdrant without servers:

```sh
bash examples/cli_workspace_demo.sh --sqlite
```

The script performs every Chronos operation through the CLI and uses Python
only to check its JSON output. It prints each check, verifies single-store
startup, adding Qdrant, branch isolation, checkpoint restoration, search, and
atomic merging, then removes its branches. It leaves the checkpoint and data
in place and prints the config path for further exploration. Local files go
into a fresh `.chronos/workspace-demo.*` directory; PostgreSQL data remains in
the supplied database. No servers are started or stopped by the script.

## Store operations and output

Prepare existing SQL tables with `table register --store NAME TABLE --primary-key KEY`;
repeat `--primary-key` for composite keys. `table create` runs one ordinary CREATE
TABLE statement before registration; it is not a branch-local schema change.
After registration, use the branch-session `query` and `execute` commands for
managed reads and writes. See [integration](integration.md) before adopting
application data; direct access to the original table bypasses Chronos.

SQL accepts text, `--file PATH`, or `--stdin`. Piped input is read automatically
when no source is supplied. Sources are mutually exclusive; SQL is passed to the
API without splitting scripts. `--params` accepts a JSON object of named values.
Branch defaults are `main`.

```sh
chronos execute --store app --branch experiment \
  'INSERT INTO items (id, value) VALUES (:id, :value)' \
  --params '{"id": 1, "value": "candidate"}'
chronos query --store app --file query.sql
printf 'SELECT * FROM items' | chronos query --store app
```

Qdrant requires `chronos-core[qdrant]`. Register collections, then use point and
search commands on a selected store. Dense vectors and payloads are JSON; point
upserts and searches also accept `--file` and `--stdin`.

```sh
chronos qdrant --store vectors collection register documents --dimensions 3
chronos qdrant --store vectors point upsert documents one \
  '{"vector": [1, 0, 0], "payload": {"title": "Example"}}' --branch experiment
chronos qdrant --store vectors search documents '[1, 0, 0]' --branch experiment
```

Except for `fs run`'s default command-output mode described below, results are
the API's JSON values, including workspace store names and result
field names. `None` prints nothing. Dataclasses become objects, dates use ISO
strings, decimals become strings, and bytes become tagged base64 objects.
Expected operational errors go to stderr with exit code 1; invalid CLI arguments
use exit code 2. Help opens no connections. JSON output can be piped to `jq` or
redirected to a file.
Non-finite floats returned by the API fail clearly because JSON cannot represent
them; query execution and the API's returned values are not modified.

## OverlayFS workspace

The OverlayFS participant wraps `ChronosFilesystemStore` and requires Linux,
`fuse-overlayfs`, `fusermount3` (or `fusermount`), and permission to use `/dev/fuse`.
It can be the only participant or accompany named SQL/Qdrant stores. Configure
an existing base directory and an explicit persistent state directory, preferably
outside the base directory. Relative paths resolve against the TOML file:

```toml
[filesystem]
root = "base"
state_dir = ".chronos/filesystem"
```

Branch, checkpoint, and diff commands include this participant automatically.
Run ordinary programs against a branch using the existing session `run()` API:

```sh
chronos branch create experiment
chronos fs run --branch experiment -- sh -c 'printf "hello\n" > report.txt'
chronos fs run --branch experiment -- cat report.txt
chronos checkpoint create saved --branch experiment
chronos diff main experiment
chronos merge preview experiment main --policy api-default
chronos merge apply experiment main --policy api-default
```

`fs run` defaults to branch `main` and working directory `.`. Options must precede
`-- COMMAND...`. `--cwd` selects a directory within the branch; `--timeout` passes
a timeout in seconds; `--env '{"NAME":"value"}'` replaces the child environment
(the API inherits the current environment when this option is omitted). Commands
are passed as argument lists; shell syntax requires explicitly running a shell.
The mounted working directory is not a security sandbox for the child process.

Output is captured by the API and printed to stdout/stderr after completion.
The CLI exits with the child's exit code (signals use the shell convention
`128 + signal`). `--json` instead prints the API result fields `args`, `returncode`,
`stdout`, and `stderr`, while retaining that exit status. These child-generated
outputs are passed through as-is. Startup failures and timeouts exit with code 1.

`--policy api-default` passes `None` to the workspace API. This is required for
the current filesystem merge implementation, which applies file-level changes
without three-way conflict detection. The existing `abort_on_conflict` default
and explicit policies remain unchanged; the API rejects them for this filesystem.
The CLI's OverlayFS backend does not support shared-metadata atomic workspaces.
These configurations are rejected before opening stores. Coordinated merges do not promise all-store rollback.

Each invocation releases its mounts while preserving state and versioned files.
Use short-lived commands; this interface does not keep a mount alive for later
external use or background processes. For a disposable demo, keep the base,
configuration, and state inside one dedicated directory, and remove that directory
only after commands finish and its mounts are released.

Filesystem integration tests live in `TestWorkspaceFilesystemWorkflows` in
`tests/test_cli_integration.py`. Run them in Linux with the dependencies above:

```sh
CHRONOS_CLI_REQUIRE_FILESYSTEM=ON python -m pytest \
  tests/test_cli_integration.py::TestWorkspaceFilesystemWorkflows -q
```

For Docker, the test container needs `/dev/fuse`, `SYS_ADMIN`, and a mount-permitting
security profile (for example `--device /dev/fuse --cap-add SYS_ADMIN
--security-opt apparmor=unconfined`). Keep test data on the container's Linux
filesystem. The required-test flag prevents missing dependencies from counting
as a successful skipped run.

## Native ChronosFS workspace

Native ChronosFS stores versioned files in Chronos. Use a Linux package built
with `CHRONOS_WITH_FILESYSTEM=ON`. File commands use the Python API directly
and do not need a FUSE mount. Select one filesystem backend per workspace.
Omitting `kind` keeps the existing OverlayFS configuration behavior.

```toml
[filesystem]
kind = "chronosfs"
database_url = "sqlite:///files.sqlite"
```

```sh
chronos branch create experiment
printf 'hello\n' | chronos fs write /notes/report.txt --branch experiment --parents --stdin
chronos fs ls /notes --branch experiment
chronos fs read /notes/report.txt --branch experiment
```

`fs read` writes raw bytes to stdout. `fs write` accepts either `--file PATH`
or `--stdin` and prints the API's result as JSON. `fs run` is available with
the OverlayFS backend.

For an atomic workspace with SQLite, configure the SQL store and native
ChronosFS to use the same database:

```toml
[workspace]
shared_metadata_url = "sqlite:///app.sqlite"

[stores.app]
kind = "sqlite"
database_url = "sqlite:///app.sqlite"

[filesystem]
kind = "chronosfs"
database_url = "sqlite:///app.sqlite"
```

Use the existing `merge atomic-preview` and `merge atomic-apply` commands to
publish SQL and filesystem changes together. The API preview can show the
filesystem changes both as interval rows under the SQL participant and as
file changes under the filesystem participant.

To keep SQL rows and native file data in separate databases, put the SQL
participant's table scope in the workspace TOML:

```toml
[workspace]
shared_metadata_url = "sqlite:///app.sqlite"

[stores.app]
kind = "sqlite"
database_url = "sqlite:///app.sqlite"
merge_tables = ["items"]

[filesystem]
kind = "chronosfs"
database_url = "sqlite:///files.sqlite"
metadata_url = "sqlite:///app.sqlite"
```

Omitting `merge_tables` or setting it to `[]` keeps the API's all-tables
default. Split atomic configurations that require explicit table ownership
need a nonempty list. The CLI passes that list to `set_merge_table_scope()`
when it opens the SQL store. Register tables separately with `chronos table register`.
Update the scope when you want another registered table included in merges.
This setting controls merge scope and does not change workspace `diff`.

On Linux with FUSE access, you can also mount a native branch into an existing
directory:

```sh
mkdir -p ./chronos-mount
chronos fs mount ./chronos-mount --branch experiment
```

The command stays in the foreground. Use the directory from another terminal
and unmount it there with `fusermount3 -u ./chronos-mount` when finished.
Repeat `--option` (or `-o`) to pass individual FUSE options to the Python API.

## Atomic merge across SQL stores

Declare each SQL store in the workspace TOML and point each one at the shared
metadata database. Give each store an explicit `merge_tables` list containing
the tables it should handle during an atomic merge:

```toml
[workspace]
shared_metadata_url = "sqlite:///metadata.sqlite"

[stores.app]
kind = "sqlite"
database_url = "sqlite:///app.sqlite"
metadata_url = "sqlite:///metadata.sqlite"
merge_tables = ["items"]

[stores.audit]
kind = "sqlite"
database_url = "sqlite:///audit.sqlite"
metadata_url = "sqlite:///metadata.sqlite"
merge_tables = ["events"]
```

Register each table in its owning store. Use `merge atomic-preview` to inspect
the changes per store and `merge atomic-apply` to publish them together.
`merge_tables` is forwarded to the Python API's merge scope. To compare a
candidate for publication in this split setup, use atomic preview. The
ordinary workspace `diff` API does not use merge scopes and can fail when it
looks for a table owned by a different data database.
