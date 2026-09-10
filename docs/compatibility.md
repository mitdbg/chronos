# Compatibility and limits

| Capability | Python library | PostgreSQL implementation |
| --- | --- | --- |
| Metadata-only data fork | Yes | Yes |
| Ordinary PostgreSQL clients | Explicit library integration | Yes |
| Checkpoints and merge | Yes | Not implemented |
| Branch-local table creation/drop | Opt-in subset | Incomplete catalog semantics |
| Atomic visibility across stores | Shared metadata and merge_atomic | One database |
| Arbitrary SQL/ORM compatibility | No | Broader support; consult fork guide |
| Process/network sandbox | No | No |

The interval library supports SELECT rewriting, INSERT VALUES, supported UPDATE
expressions, DELETE, bulk upserts, and key deletes. Rich reads are delegated to
the engine after rewriting. General DML uses a narrower parser/evaluator.
Triggers, foreign keys, unique secondary indexes, and catalog dependencies are
not automatically preserved by registration and schema copying.

Opt-in schema branching supports table creation/drop and selected column changes.
Index DDL accepts simple column indexes; partial, expression, and unique index
declarations are rejected. DuckDB supports fixed-schema data operations.

Allocation is finite. Hints distribute capacity without guaranteeing unbounded
depth. Sequences and external state are not reset by branch deletion. Checkpoints
retain data, and the current API has no checkpoint-deletion operation.

Snapshot-isolation merge rejects overlapping writes, including equal resulting
values, but does not track arbitrary read predicates. Weak snapshot isolation
applies source changes without write-write validation.

Independent metadata stores have separate commits. Only shared-metadata
merge_atomic coordinates staging through one branch head. Its guarantees require
all managed access to use Chronos and participant writes to be durable and
readable before publication.
