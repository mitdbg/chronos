# Chronos Branching TLA+ Specs

This directory contains small executable TLA+ models for the three Chronos
branching storage schemes:

- `CopyBranching.tla`: branch creation physically copies each registered table.
- `IntervalBranching.tla`: branch creation splits numeric visibility intervals,
  and writes maintain non-overlapping physical rows.
- `LogBranching.tla`: writes append records to per-table logs, and reads replay
  the visible branch lineage.

The specs model a single registered table with a finite set of primary keys and
values. That is enough to check the core branch isolation rule shared by all
tables: after branching, writes and deletes on one branch must not change the
visible rows on another branch unless those changes happened before the fork.

## Running

With `tla2tools.jar` installed:

```bash
java -cp tla2tools.jar tlc2.TLC CopyBranching.cfg
java -cp tla2tools.jar tlc2.TLC IntervalBranching.cfg
java -cp tla2tools.jar tlc2.TLC LogBranching.cfg
```

The included configs use tiny model values so TLC can exhaustively explore
branch creation, upserts, and deletes. Increase `BranchIds`, `Keys`, `Values`,
or `MaxInterval` to explore larger states.

## What Each Spec Checks

`CopyBranching` checks that each branch owns an independent physical copy of the
table after branch creation.

`IntervalBranching` checks that:

- every logical key has at most one visible physical row per branch;
- physical row intervals for the same key never overlap;
- read results from interval predicates match a simple reference branch view.

`LogBranching` checks that:

- branch creation captures the parent branch head as a fork boundary;
- replay only sees log records from the branch lineage up to each fork point;
- nearest/newest visible log records match a simple reference branch view.

These are abstract specs. They do not model SQL parsing, query rewriting,
indexes, transactions, or backend-specific DDL.
