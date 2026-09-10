# Reproduce and fix a bug with its database state

A pricing bug can depend on both the checked-out code and the rows used by a
test. This tutorial forks them together, reproduces a discount calculation
failure, fixes it, and merges the tested code and data in one operation.

Install Chronos with filesystem support using the [installation guide](../installation.md).
Run the complete example from the repository root.

```sh
python examples/software_development.py
```

The script creates a temporary SQLite database and a ChronosFS store in that
same database. It registers the `offers` table, writes `/pricing.py`, and gives
both stores to `ChronosWorkspaceContext` with `shared_metadata_url`. The SQL
store's merge scope contains only `offers`, so it does not also merge the
filesystem's internal tables.

The `fix` branch sets a discount of ten on a price of one hundred. Its test
expects ninety, but the original implementation adds the discount and fails.
The branch then changes the code to subtract the discount and reruns the test.
Assertions verify that `main` still contains the original code and data.

The example passes its preview token to `merge_atomic`, then checks the accepted
state on `main` and deletes the development branch. Shared metadata makes the
branch-head change atomic across the two stores. Two independent database URLs
with ordinary `merge_apply` would give separate commits.

## Connect a real project's tests

The example exports two known files into a temporary test directory. For a
larger project, a ChronosFS FUSE mount lets the test runner use ordinary paths.
Route application database access through the selected branch session as
described in [integration](../integration.md). The original unregistered table
and an ordinary SQL connection do not follow the library's branch selection.

Keep database transactions short. A test suite can run for minutes without
holding one transaction open because its branch persists across tool calls.
After a failed test, retain the branch for inspection or delete it. Do not
merge an attempt merely because its subprocess exited successfully; run the
project's assertions against both the code and data that will be accepted.

Chronos does not isolate processes or network access. Run untrusted generated
code inside an execution sandbox, with credentials and network access restricted
to the intended services. External messages and API calls cannot be undone by
deleting a branch.
