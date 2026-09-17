# Chronos

Chronos is a branching abstraction for databases, filesystems, and object stores.
It lets applications create writable branches of their data. Changes made
in a branch are isolated from its parent and other branches. An application can
inspect those changes, merge them back, or delete the branch when it is done.
This is useful for testing changes to code and data, or for giving each agent
training rollout its own database.

You can create a branch without copying the entire dataset. Chronos works with
SQLite, PostgreSQL, and DuckDB, and also supports files through ChronosFS,
Qdrant collections, and S3-compatible storage. This repository provides the
Python library for using Chronos with your existing applications and databases.

If you only need branching within a single PostgreSQL database,
[Chronos for PostgreSQL](https://github.com/mitdbg/postgres_chronos) implements
Chronos's interval-based versioning directly inside PostgreSQL.

Chronos is experimental. See the [compatibility guide](docs/compatibility.md)
for supported operations and current limitations.

The [Chronos paper](https://arxiv.org/abs/2609.14889) explains the
interval-based versioning algorithm, cross-store branching, and evaluation.

## Install

The default build supports SQLite and PostgreSQL. From the repository root on
Ubuntu, install the build dependencies and the Python package:

```sh
sudo apt-get install build-essential python3-dev libsqlite3-dev libpq-dev libboost-dev
python3 -m venv .venv
. .venv/bin/activate
python -m pip install ./packages/chronos-core
```

Installation requires an internet connection to download dependencies.
The [installation guide](docs/installation.md) explains how to enable DuckDB,
filesystem, and S3 support.

## Create your first branch

This example creates a table in SQLite and registers it with Chronos. It then
updates a row in a new branch, checks that the original data is unchanged, and
merges the update back into `main`.

```python
from chronos_core.branching import ChronosBranchContext

ctx = ChronosBranchContext.connect("sqlite:///:memory:")
try:
    ctx.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, quantity INTEGER)")
    ctx.db.execute("INSERT INTO items VALUES (1, 10)")
    ctx.db.commit()
    ctx.register_table("items", ["id"])
    ctx.create_branch("trial", from_branch="main")
    with ctx.checkout("trial") as trial:
        trial.execute("UPDATE items SET quantity = :n WHERE id = :id",
                      {"n": 7, "id": 1})
        assert trial.query("SELECT quantity FROM items") == [{"quantity": 7}]
    with ctx.checkout("main") as main:
        assert main.query("SELECT quantity FROM items") == [{"quantity": 10}]
    ctx.merge_apply("trial", "main", policy="snapshot_isolation")
    ctx.delete_branch("trial")
finally:
    ctx.close()
```

Use `register_table` to choose which tables to branch, then use `checkout` to
read and write data in a particular branch. Accessing the database directly
does not use the selected branch.

## Tutorials and documentation

The [software development tutorial](docs/tutorials/software-development.md)
shows how to reproduce a bug, test a fix in a branch, and merge the changes to
both code and data. The [verl tutorial](docs/tutorials/rl-data-sandbox.md)
shows how to give each training rollout a separate database branch, calculate
its reward from the resulting data, and delete the branch afterward.

To add Chronos to an existing application, start with the
[integration guide](docs/integration.md). Applications that need branches across
more than one store should also read the
[multi-store branching guide](docs/multi-store-branching.md), which explains
how to configure them and when their changes can be merged together atomically.
Agent clients can use the optional [MCP integration](docs/mcp-integration.md)
for explicit branch, SQL, diff, merge, and filesystem tools.

The [documentation index](docs/README.md) links to the API guides and further
reading. If you want to understand how Chronos works, see the
[implementation guide](docs/bolt-on-branching.md).

Chronos isolates changes to the data it manages. It does not sandbox code or
external services. Run untrusted code in a separate execution sandbox and
control which services it can reach.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for build and test instructions.
The [experimental release notes](EXPERIMENTAL_RELEASE.md) describe the tests run
so far and known failures; [RELEASING.md](RELEASING.md) describes the release
process.

## Cite Chronos

```bibtex
@misc{zhou2026chronos,
  title         = {{Chronos}: Efficient Bolt-on Branching Across Data Stores for Stateful Agentic Applications},
  author        = {Xinjing Zhou and Jason Mohoney and Samuel Madden and Michael Stonebraker and Lei Cao},
  year          = {2026},
  eprint        = {2609.14889},
  archivePrefix = {arXiv},
  primaryClass  = {cs.DB},
  url           = {https://arxiv.org/abs/2609.14889}
}
```
