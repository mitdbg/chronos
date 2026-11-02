# Chronos CLI

The `chronos` command is a one-off wrapper around the public relational and Qdrant
Python APIs. Each invocation opens the selected store, calls the API, prints its
return value, and closes the connection. It does not remember a current branch
or keep a background connection open.

## Install and choose a database

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

Set the database URL in your terminal. This example uses a persistent local file:

```sh
mkdir -p .chronos
export CHRONOS_DATABASE_URL="sqlite:///$PWD/.chronos/demo.sqlite"
```

This keeps the demo in `.chronos/` under your current directory. The absolute URL
continues to select that file if you change directories. Files ending in `.sqlite`
and their SQLite journal files are ignored by this repository's Git configuration.
SQLite creates a missing database file; its parent directory must already exist.
PostgreSQL URLs can also be supplied, for example through an environment variable
populated by your secret-management tooling. No PostgreSQL server is started by
the CLI. A `.env` file is not loaded automatically.

Override the environment variable by placing the shared flag before the command:

```sh
uv run --package chronos-core chronos \
  --database-url "sqlite:///$PWD/.chronos/other.sqlite" branch list
```

Help does not require a database URL and does not open a connection.

## Create and register a table

For a fresh database, create an ordinary table directly through the CLI, then
register it with Chronos:

```sh
uv run --package chronos-core chronos table create \
  'CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)'
uv run --package chronos-core chronos table register items --primary-key id
```

`table create` wraps the public database adapter's `execute()` and `commit()`
methods for initial setup. It accepts one CREATE TABLE statement directly, through
`--file`, or through stdin. This creates an ordinary database table; it is not a
branch-local schema change. Run it once before registration and managed access.
The branch-session `execute` command continues to use the API's default behavior.

If your database already contains the application table, skip `table create` and
register it directly. For composite keys, repeat `--primary-key`. Register each
table once. Registration copies existing data into Chronos-managed storage;
subsequent managed reads and writes must use branch sessions. Direct access to
the original table bypasses Chronos. See [integration](integration.md) before
adopting application data.

## Branch, query, checkpoint, and merge

Run this walkthrough against the newly prepared database:

```sh
uv run --package chronos-core chronos branch list
uv run --package chronos-core chronos branch create experiment --from main

uv run --package chronos-core chronos execute \
  'INSERT INTO items (id, value) VALUES (:id, :value)' \
  --branch experiment --params '{"id": 1, "value": "hello"}'

uv run --package chronos-core chronos query \
  'SELECT * FROM items WHERE id = :id' \
  --branch experiment --params '{"id": 1}'

uv run --package chronos-core chronos checkpoint create saved --branch experiment
uv run --package chronos-core chronos checkpoint list --branch experiment
uv run --package chronos-core chronos diff main experiment
uv run --package chronos-core chronos diff main experiment --table items
uv run --package chronos-core chronos merge preview experiment main
uv run --package chronos-core chronos merge apply experiment main
uv run --package chronos-core chronos query 'SELECT * FROM items'
uv run --package chronos-core chronos branch delete experiment
```

Queries, writes, and checkpoint creation default to branch `main`. Branch creation
defaults to `--from main`. Checkpoint listing without `--branch` lists all
checkpoints. Both merge commands accept `--policy abort_on_conflict` (the default)
or `--policy snapshot_isolation`. These are API policies; snapshot isolation does
not mean force-overwriting conflicts. API validation and conflict behavior apply
unchanged.

## SQL input

Both `query` and `execute` accept one SQL source:

```sh
# Direct text, including multiline text inside shell quotes.
uv run --package chronos-core chronos query 'SELECT * FROM items'

# A UTF-8 file.
uv run --package chronos-core chronos query --file query.sql

# Piped input, automatically detected when no other source is supplied.
printf 'SELECT * FROM items\n' | uv run --package chronos-core chronos query

# Explicit stdin; type SQL and finish with Ctrl-D in an interactive terminal.
uv run --package chronos-core chronos query --stdin
```

Empty SQL and conflicting explicit sources are rejected. With no SQL source in an
interactive terminal, the CLI shows an input error rather than opening an editor.
`--params` must be a JSON object of named parameter values. SQL and parameters are
passed directly to `session.query()` or `session.execute()`; files are not split
into a script of multiple statements. Supported SQL is determined by the API.

## Results and errors

Non-`None` API return values are printed as JSON to stdout:

- Queries produce an array of row objects; no rows produces `[]`.
- Writes produce the API's result fields, such as `{"rowcount": 1}`.
- Lists, checkpoints, diffs, and merges preserve the API result field names.
- Methods returning `None`, including successful branch creation/deletion and
  table registration, produce no output.

Dates and times use ISO strings, decimals use strings to preserve precision, and
bytes use `{"encoding": "base64", "data": "..."}` objects. Unsupported values
produce an error instead of silently changing their representation. No query
results are cached or saved unless you redirect stdout:

```sh
uv run --package chronos-core chronos query 'SELECT * FROM items' > rows.json
uv run --package chronos-core chronos query 'SELECT * FROM items' | python -m json.tool
```

Exit code 0 means the API call and output completed successfully. Expected
operational failures are reported on stderr with exit code 1; invalid command-line
input uses exit code 2. A merge preview may successfully return conflicts as data;
merge application follows the API's rejection behavior. Errors do not emit a JSON
success result.

The CLI uses the default interval backend with one relational data store per
invocation. DuckDB uses a separate metadata store as described below. Workspace
coordination, a persistent interactive shell, advanced connection tuning, and
custom Python merge callbacks are not exposed.

## Qdrant

The `qdrant` command group wraps `ChronosQdrantStore` and its branch sessions.
Install the optional Qdrant dependency; for an existing checkout environment:

```sh
uv pip install 'qdrant-client>=1.18,<2'
mkdir -p .chronos/qdrant-demo
export CHRONOS_QDRANT_PATH="$PWD/.chronos/qdrant-demo/vectors"
export CHRONOS_METADATA_URL="sqlite:///$PWD/.chronos/qdrant-demo/metadata.sqlite"

uv run --no-sync --package chronos-core chronos qdrant collection register documents --dimensions 3
uv run --no-sync --package chronos-core chronos qdrant point upsert documents example \
  '{"vector":[1,0,0],"payload":{"title":"Example"}}'
uv run --no-sync --package chronos-core chronos qdrant search documents '[1,0,0]'
```

Choose persistent local storage with `--path` / `CHRONOS_QDRANT_PATH`, or a server
with `--url` / `CHRONOS_QDRANT_URL`, not both. Server authentication uses
`--api-key` / `CHRONOS_QDRANT_API_KEY`. Metadata requires SQLite or PostgreSQL via
`--metadata-url` / `CHRONOS_METADATA_URL`. Explicit Qdrant options go after
`qdrant` and before the subcommand; flags override their environment variables.

Point upsert and search also accept JSON from `--file`, `--stdin`, or a pipe.
Use `--branch` to select a branch; `main` is the default. The group also exposes
branch, checkpoint, diff, and merge operations through the public store APIs.
Run `chronos qdrant --help` or subcommand help for available options. Results use
the JSON conventions above; search consumes vectors rather than generating them.

The example stores everything under `.chronos/qdrant-demo/`. After closing clients,
remove that folder to delete both its vectors and metadata. Unset the demo
environment variables before switching stores. No storage is created by help.

## DuckDB data with separate metadata

DuckDB is an in-process SQL database designed for analytical queries such as
aggregations and joins. The CLI opens a local DuckDB file; no DuckDB server or
port is required. Chronos stores branch metadata separately in SQLite or
PostgreSQL, using the existing `ChronosBranchContext.connect_split()` API.

The native extension must be built with DuckDB enabled. Installing the Python
`duckdb` package alone does not enable Chronos's native driver. On Linux x86-64,
the build downloads the pinned DuckDB library:

```sh
uv sync --inexact --package chronos-core --reinstall-package chronos-core \
  -Ccmake.define.CHRONOS_WITH_DUCKDB=ON
```

On macOS or other architectures, supply `-Ccmake.define.CHRONOS_DUCKDB_ROOT=PATH`
with a directory containing `duckdb.h` and the compatible `libduckdb` library.
The repository pins DuckDB 1.5.4 for its automatic Linux build. For example, after
extracting the [official release's macOS universal library](https://github.com/duckdb/duckdb/releases/tag/v1.5.4) into
`packages/chronos-core/build/duckdb-sdk`, rebuild with:

```sh
CMAKE_PREFIX_PATH="$(brew --prefix libpq)" \
uv sync --inexact --package chronos-core --reinstall-package chronos-core \
  -C "cmake.define.SQLite3_INCLUDE_DIR=$(brew --prefix sqlite)/include" \
  -C "cmake.define.SQLite3_LIBRARY=$(brew --prefix sqlite)/lib/libsqlite3.dylib" \
  -Ccmake.define.CHRONOS_WITH_DUCKDB=ON \
  -C "cmake.define.CHRONOS_DUCKDB_ROOT=$PWD/packages/chronos-core/build/duckdb-sdk"
```

Use the same table, branch, query, checkpoint, diff, and merge commands with two
connection URLs. Both databases must persist between one-off commands:

```sh
mkdir -p .chronos
export CHRONOS_DATABASE_URL="duckdb:///$PWD/.chronos/analytics.duckdb"
export CHRONOS_METADATA_URL="sqlite:///$PWD/.chronos/analytics-metadata.sqlite"

uv run --no-sync --package chronos-core chronos table create \
  'CREATE TABLE sales (id INTEGER PRIMARY KEY, amount BIGINT)'
uv run --no-sync --package chronos-core chronos table register sales --primary-key id
uv run --no-sync --package chronos-core chronos execute \
  'INSERT INTO sales (id, amount) VALUES (1, 10), (2, 20)'
uv run --no-sync --package chronos-core chronos branch create forecast
uv run --no-sync --package chronos-core chronos execute \
  'UPDATE sales SET amount = amount * 2' --branch forecast
uv run --no-sync --package chronos-core chronos query \
  'SELECT SUM(amount) AS total FROM sales' --branch forecast
```

The forecast totals 60 while `main` still totals 30. You can substitute a
PostgreSQL URL for `CHRONOS_METADATA_URL`; no multi-store workspace is required.
The explicit `--metadata-url` flag precedes the command name and overrides its
environment variable, just like `--database-url`. Missing DuckDB metadata produces
a usage error before connecting. An unavailable native driver produces the API's
build-option error.

Each invocation must use the same data/metadata pair to access the same branches.
Unset `CHRONOS_METADATA_URL` when returning to the default single-database SQLite
or PostgreSQL mode. This wrapper preserves the API's split-store semantics; it
does not add cross-database transaction guarantees. DuckDB's normal file locking
also applies, so use sequential invocations for this one-off CLI workflow.

## Test PostgreSQL locally

The same relational commands work with SQLite, PostgreSQL, and DuckDB. To run the
CLI integration tests, start one disposable PostgreSQL server using Docker:

```sh
docker run --detach --rm --name chronos-cli-postgres-test \
  -e POSTGRES_PASSWORD=chronos_test \
  -p 127.0.0.1:55434:5432 \
  --health-cmd 'pg_isready -U postgres' \
  --health-interval 1s --health-timeout 3s --health-retries 30 \
  postgres:16-alpine

docker inspect --format '{{.State.Health.Status}}' chronos-cli-postgres-test
```

Wait until the health status is `healthy`. Port `55434` on your computer forwards
to PostgreSQL's port `5432` inside the container; it is bound only to localhost.
This port must be free before starting the container. Reuse the existing
container if you already started it; do not start one per test.

With the package and pytest installed in your environment, run from the
repository root:

```sh
CHRONOS_CLI_TEST_POSTGRES_ADMIN_URL=postgresql://postgres:chronos_test@127.0.0.1:55434/postgres \
  uv run --no-sync --package chronos-core python -m pytest \
  tests/test_cli.py tests/test_cli_support.py tests/test_cli_integration.py -v
```

The PostgreSQL account must have permission to create and drop databases. The
test session creates one uniquely named test database and reuses it, clearing its
`public` schema between cases. It drops that database on completion and never
resets the database named in the admin URL. No additional servers are started by
pytest. Without the environment variable, PostgreSQL cases are reported as skips;
an explicitly configured server that cannot be reached causes a test failure.

A DuckDB-enabled build also runs the workflows with SQLite and PostgreSQL
metadata independently. Without its native driver those cases are skipped. Set
`CHRONOS_CLI_REQUIRE_DUCKDB=ON` to fail rather than skip if the driver is missing.

The tests cover CLI-only setup, branch isolation, parameterized writes, SQL files
and stdin, checkpoints, diffs, successful merges, conflicts, and rollback after a
failed write. CI is configured to test the installed wheel on Python 3.10 and 3.12, with
an additional DuckDB-enabled Python 3.12 build.

`test_cli.py` checks command parsing, API arguments, errors, and entry points with
fake connections. `test_cli_support.py` checks JSON conversion and cleanup.
`test_cli_integration.py` runs real CLI processes against SQLite and PostgreSQL.

Stop the disposable server when finished:

```sh
docker stop chronos-cli-postgres-test
```

Because the container uses `--rm`, stopping it removes the container and its
temporary database storage. No Docker volume is retained for this test server.
