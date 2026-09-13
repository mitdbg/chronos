# Compatibility and limits

This page describes Chronos 0.2.0a1. "Python library" means the bolt-on package
in this repository. "PostgreSQL implementation" means the separate PostgreSQL
engine source tree, where ordinary PostgreSQL sessions select a database branch.

| Capability | Python library | PostgreSQL implementation |
| --- | --- | --- |
| Metadata-only data fork | Yes | Yes |
| Ordinary PostgreSQL clients | Explicit library integration | Yes |
| Checkpoints and merge | Yes | Not implemented |
| Branch-local table creation/drop | Opt-in subset | Incomplete catalog semantics |
| Atomic visibility across stores | Shared metadata and merge_atomic | One database |
| Arbitrary SQL/ORM compatibility | No | Broader support; consult fork guide |
| Process/network sandbox | No | No |

The two implementations share the interval-versioning approach, but they do not
have interchangeable APIs or feature sets. The Python library requires explicit
branch sessions. The PostgreSQL implementation is visible to ordinary clients
but currently lacks checkpoints and merge.

## Relational library

The interval library supports SELECT rewriting, INSERT VALUES, supported UPDATE
expressions, DELETE, bulk upserts, and key deletes. Rich reads are delegated to
the engine after rewriting. General DML uses a narrower parser/evaluator.
Triggers, foreign keys, unique secondary indexes, and catalog dependencies are
not automatically preserved by registration and schema copying.

Opt-in schema branching supports table creation/drop and selected column changes.
Index DDL accepts simple column indexes; partial, expression, and unique index
declarations are rejected. DuckDB supports fixed-schema data operations.

## Other stores

ChronosFS supports direct file operations and a Linux FUSE adapter. It is a
practical agent workspace, not a complete POSIX filesystem: hardlinks, writable
`mmap`, file locks, extended attributes, device files, quotas, and full ACLs are
not supported. See [Filesystem on Chronos](filesystem-on-chronos.md).

Qdrant and S3-compatible adapters provide branch lifecycle and data operations
for their respective stores. They do not turn vendor-specific administration,
external services, or arbitrary client access into branch-local operations.

## Allocation and retention

Allocation is finite. Hints distribute capacity without guaranteeing unbounded
depth. Sequences and external state are not reset by branch deletion. Checkpoints
retain data, and the current API has no checkpoint-deletion operation.

## Merge semantics

Snapshot-isolation merge rejects overlapping writes, including equal resulting
values, but does not track arbitrary read predicates. Weak snapshot isolation
applies source changes without write-write validation.

Independent metadata stores have separate commits. Only shared-metadata
merge_atomic coordinates staging through one branch head. Its guarantees require
all managed access to use Chronos and participant writes to be durable and
readable before publication.

The MCP server currently uses independent workspace merges rather than the
shared-metadata atomic path. See [MCP integration](mcp-integration.md).
