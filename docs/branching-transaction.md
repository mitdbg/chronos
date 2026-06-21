# Branching Transactions for Agentic Workflows

**Status:** Draft

## Summary

Agentic applications often need transaction-like isolation across long-running
workflows. A customer-service agent may read an order, call an LLM, update an
item, call an LLM again, issue a refund, and then email the user. The useful
unit of work is the whole attempt, not each individual database statement.

Traditional ACID transactions are a poor fit for this shape because the
workflow includes slow model calls and external tool execution. Saga-style
transactions avoid holding one large database transaction open, but they expose
intermediate states and rely on compensation logic after failure. Chronos can
offer a third option: treat a branch as a durable private transaction workspace,
validate the final state, then merge approved changes back to the main branch.

## Big Transaction

The most direct design is to keep one database transaction open for the whole
agent run:

```text
BEGIN
  LLM call
  read order
  LLM call
  maybe update item
  LLM call
  maybe issue refund
  LLM call
  maybe email user
COMMIT
```

This gives familiar ACID isolation for database state, but it is operationally
bad for agentic workflows. The transaction stays open while the system waits
for LLM inference and tool execution. That can hold locks, retain MVCC versions,
increase contention, delay vacuum, and reduce concurrency for unrelated
applications. It also does not naturally cover filesystem artifacts or other
state stores touched by the same agent run.

## Saga Transaction

A saga breaks the workflow into smaller committed steps:

```text
BEGIN
  read order
COMMIT

LLM call

BEGIN
  maybe update item
COMMIT

LLM call

BEGIN
  maybe issue refund
COMMIT

LLM call

BEGIN
  maybe email user
COMMIT
```

If the workflow fails midway, the system issues compensation transactions to
undo earlier steps. In agentic systems, those compensations may themselves be
LLM-generated.

This avoids a long-running database transaction, but it exposes intermediate
state to other users, applications, and agents. Other actors may observe the
item update before the refund decision is final. Compensation is also not true
rollback: side effects can escape, compensating logic can be wrong, and later
concurrent changes can make reversal ambiguous.

## Branch Transaction

Chronos can model the whole attempt as a branch:

```text
branch = fork(customer_service_state)

agent performs all tool calls inside branch

system computes DB/file diff
policy checker verifies final branch

merge approved changes
discard rejected branch
```

The branch gives the agent snapshot reads and private writes without holding one
large database transaction open. The agent can take many normal database
transactions inside the branch, and those commits publish only to the branch.
Other applications keep reading the main branch. If the attempt fails, Chronos
deletes the branch. If the attempt succeeds, Chronos computes the branch diff,
runs policy checks, detects merge conflicts, and applies the approved changes
back to the target branch.

In this model, merge is the transaction boundary. Merge validation checks
whether the private branch can still be safely applied to the target state. Merge
apply writes the private state back through Chronos' normal write path. The
combination provides transaction-like commit semantics at branch granularity:

```text
validate final branch state
check conflicts against target branch
apply approved private changes atomically
```

## Relation to Generic Version Control

Yilmaz and Dittrich's GenericVC observes that Git-style branches and
MVCC-style transactions are two forms of the same versioning abstraction. A Git
feature branch is a durable private workspace whose changes become official at
merge time. An MVCC transaction is a private workspace whose versions become
official at commit time. GenericVC frames both as nested transactions with a
configurable commit-validation phase.

Chronos branch transactions follow the same logical model in a bolt-on setting.
Instead of replacing the DBMS with a new unified versioning engine, Chronos
layers branch creation, diff, conflict detection, reconciliation, and merge over
existing state stores. The merge phase is Chronos' commit-validation phase:

```text
candidate changes = diff(branch, target)
conflicts = detect_conflicts(candidate changes, target changes)
resolution = reconcile(conflicts, policy)
stage candidate changes in an unpublished target successor
publish the successor atomically
```

Atomic publish is the critical implementation detail. Chronos must not write
merge results directly into the target branch's currently published interval
segment, because those writes would become visible before every participant in a
multi-store merge has finished. Instead, merge creates a new successor segment
for the target branch, writes the resolved merge changes into that successor
using the same branch-local write path as ordinary DML, and then publishes the
successor with one metadata update:

```text
target before merge: main -> S10

stage:
  create S11 as an unpublished successor of S10
  write resolved merge rows into S11

publish:
  UPDATE branches
     SET current_segment_id = S11
   WHERE branch_id = 'main'
     AND current_segment_id = S10;
```

Readers obtain their branch read point from branch metadata. A reader that
resolved `main -> S10` before publish keeps seeing S10. A reader that resolves
`main -> S11` after publish sees the merged state. The old segment is therefore
sealed by reachability: it remains historical state but is no longer the branch
head. The new segment is ordinary mutable target state after publication.

This framing is important because the validation policy defines the isolation
semantics. Chronos' `snapshot_isolation` policy is first-committer-wins: a merge
commits only if the target has not changed the same rows since the branch forked.
Chronos also exposes `weak_snapshot_isolation`, which keeps branch-local snapshot
reads but skips write-write validation and applies the source branch over the
target at merge time. These policies give snapshot-style behavior but can still
allow read-write anomalies such as write skew. Serializable behavior requires
stronger validation, such as read-set, scan-set, predicate, or
application-specific invariant checks. Chronos should therefore expose merge
validation as a configurable policy rather than treating row-level conflict
detection as the only possible rule.

## Why This Helps

Branch transactions provide stronger isolation than sagas because intermediate
state is private. They avoid the concurrency cost of a big transaction because
LLM calls do not keep a database transaction open. They also fit multi-store
agent execution: the same branch can include relational rows, schema changes,
filesystem edits, generated artifacts, and other application-managed state.

The tradeoff is that Chronos must make branch operations cheap. Fork, checkout,
delete, diff, and merge-preview should be metadata- or change-proportional in
the common case. LLM inference latency gives the system some room, but branch
management still needs to be lightweight enough that agents can speculate
broadly and discard failed attempts freely.

## Research Questions

The central research question is not whether Chronos can beat a native
transaction on a short SQL-only critical path. A native transaction is the right
upper-bound baseline for that case. The question is whether a branch transaction
is a better abstraction when the unit of work is a long-running agent attempt
with private intermediate state, expensive reasoning, and a review step before
commit.

This leads to several focused questions:

- **What does a private branch buy over 2PL or OCC?** Native transactions provide
  concurrency control, but a long agent transaction can hold locks, keep old
  versions alive, or abort after expensive LLM and tool work. A branch
  transaction moves the expensive work into a private workspace and postpones
  validation to merge time. The research problem is to quantify when that
  reduces wasted work, improves concurrency, or avoids MVCC/GC pressure.
- **Can merge be more useful than abort?** Native OCC has a hard outcome: commit
  or retry. Branch transactions can expose a diff, run policy checks, apply
  source-over-target rules, use row or column merge policies, or invoke
  application/LLM-assisted reconciliation. The key question is whether semantic
  merge lowers abort/retry cost for agent workloads.
- **How cheap can the branch primitives be?** A useful branch transaction needs
  fork, checkout, diff, merge-preview, merge-apply, and delete to be stable as
  the number of transactions grows. Chronos' interval backend uses fixed-size
  child intervals for wide transaction streams and `writer_segment_id` to make
  diff proportional to the number of branch-local edits. The benchmark question
  is how close this gets to the native transaction upper bound, and where the
  remaining cost sits.
- **How should conflict detection be parameterized?** Row-level write-write
  conflict detection is a starting point, not the whole semantics. Some
  workflows need column-level merge, predicate validation, invariant checks, or
  policy-selected subsets of the diff. Chronos should make the validation and
  reconciliation policy explicit.
- **How does the API stay usable for agents?** Human users can call
  `create_branch`, `diff`, and `merge` directly. Autonomous agents should not
  need to reason about an entire branch graph. A practical system likely needs a
  higher-level branch-transaction API, database proxy, or MCP tool wrapper that
  presents "run attempt, show diff, approve/merge, discard" as the natural
  workflow.
- **Is relational branching enough to establish the idea?** Relational data is a
  clean first target because conflict detection, row diffs, schema changes, and
  native transaction baselines are well-defined. The same model can later extend
  to filesystems, memory, vector stores, and other state, but the first
  evaluation can stay relational and still answer the core question.

The evaluation should therefore compare Chronos branch transactions with
PostgreSQL native transactions and native branching systems such as Doltgres.
Native transactions show the best possible SQL-only latency. Doltgres shows a
database-native branch baseline. Chronos should be judged on primitive
throughput, scalability with many short-lived branches, change-proportional diff
and merge, and the ability to avoid full retry by offering semantic merge.

## Required Chronos Primitives

For relational state, Chronos needs:

- cheap branch creation from a stable snapshot
- branch-bound SQL sessions with ordinary database transactions inside a branch
- snapshot reads against the branch state
- private writes that do not affect the target branch
- change-proportional diff from branch to target
- merge preview with configurable conflict detection
- optional reconciliation for automatically resolvable conflicts
- branch transaction commit apply for approved changes
- atomic target-head flip after commit writes are durable
- cheap branch deletion after failure or rejection

For multi-store state, Chronos needs equivalent lifecycle operations for each
store plus a coordinator that treats the branch as one unit. A filesystem branch
must expose normal POSIX APIs to agent tools. A relational branch must expose
SQL and schema evolution. The application should not have to reason separately
about each store's native snapshot mechanism.

For multi-store branch transactions, the branch transaction commit protocol is
also how Chronos avoids needing two-phase commit in the common Epoxy-style
case. Each store first writes durable but unpublished versions for the same
logical target successor. The single commit decision is the metadata transaction
that installs the successor as the target branch head. If the metadata commit
never completes, successor versions are unreachable and can be ignored or
garbage-collected. If it commits, all stores resolve the target branch through
the new head.

This relies on one transactional metadata store for the polystore workspace.
PostgreSQL or SQLite owns branch heads, segment allocation, checkpoints, table
registries, and branch transaction commit records. Additional stores such as DuckDB,
ChronosFS, vector stores, or search indexes store only physical branchable data.
They do not independently decide branch visibility.

The branch transaction commit record is keyed by the successor segment id:

```sql
CREATE TABLE _chronos_branch_transaction_commits (
  successor_segment_id INTEGER PRIMARY KEY,
  target_branch_id TEXT NOT NULL UNIQUE,
  old_target_segment_id INTEGER NOT NULL,
  old_target_revision INTEGER NOT NULL,
  source_branch_id TEXT,
  participant_stores TEXT NOT NULL,
  created_at TEXT NOT NULL,
  metadata TEXT NOT NULL DEFAULT '{}'
);
```

The `UNIQUE(target_branch_id)` constraint makes the commit record a
target-branch commit lock. Chronos allows concurrent diff, validation, and
LLM-assisted resolution, but only one merge at a time can reserve a successor
segment and enter the branch transaction commit pipeline for a target branch. This
keeps the interval read path fast: abandoned successors are not competing
sibling intervals for the same target read point.

The successor is a normal `mutable` segment. It does not need a special commit
segment kind. Visibility is determined by the target branch head:

```text
main -> S10        published old state
S11 exists         durable reserved successor, not visible
main -> S11        committed merge
```

The final metadata commit transaction is intentionally short and contains no model calls,
human review, filesystem execution, or expensive diff computation:

```text
BEGIN
  lock target branch row
  verify the commit record still names this successor
  conditionally set current_segment_id = successor_segment_id
    only if the target token still matches
  verify exactly one branch row was updated
  delete branch transaction commit record
COMMIT
```

All conflict detection, policy checks, and optional LLM-assisted reconciliation
run before this transaction using a stable `(source, target, fork_base)` read
point. The final metadata commit transaction only revalidates that the target
branch is still the one that was reviewed.

The full commit pipeline is:

```text
preview:
  diff source/target/base
  run policy checks and optional LLM/manual reconciliation
  produce merge plan for target_token

reserve:
  short metadata transaction
  lock target branch row
  check target_token is still current
  check no active branch transaction commit exists for target
  allocate one successor segment
  insert branch transaction commit record
  commit

commit writes:
  write merge output into the successor segment in each store
  use store-local transactions where available

publish:
  short metadata transaction
  lock target branch row
  check target_token and commit record still match
  flip target branch head to successor
  delete commit record
  commit
```

If two concurrent merge attempts both compute clean diffs against the same
target head, the first one that reserves the commit record enters the commit
pipeline. The second cannot allocate another successor for the same target
while that row exists. After the first merge publishes or aborts, the second
must handle the now-current target according to the configured policy. Under
`snapshot_isolation`, that often means abort or retry. Under
`weak_snapshot_isolation`, source-over-target, application merge, or LLM-based
policies, Chronos may rebase or rerun reconciliation to reduce wasted work.
The target branch row and unique commit record are the serialization points, while
expensive reconciliation work stays outside the lock.

This works only if publish treats the branch-head update as a compare-and-swap.
An implementation that reuses an earlier diff result and unconditionally writes
the target head would be incorrect: two serial metadata transactions could both
commit physically while the second one logically overwrites the first merge.

## Semantics

A branch transaction should provide:

- **Private intermediate state.** Writes inside the branch are invisible to the
  target branch until merge.
- **Snapshot-style reads.** The agent reads from the branch's fork state plus
  its own branch-local writes.
- **Discard.** Rejected or failed attempts can be deleted without compensation
  transactions.
- **Review before promotion.** The system can inspect the final diff before
  exposing it.
- **Conflict-aware commit.** Merge can reject, reconcile, or require manual
  resolution if the target changed incompatibly after the branch forked.
- **Atomic visibility by publish.** Approved changes become visible by a branch
  metadata head flip, not by mutating the previously published target segment in
  place.

The exact isolation level depends on merge validation. Write-write validation is
enough for many speculative workflows, but serializable semantics require
validation against reads, scans, predicates, or explicit application invariants.
This is also not identical to serializable ACID transactions over arbitrary
external side effects. Chronos can isolate state stores it controls, but emails,
payments, webhooks, and other irreversible actions still need explicit effect
policies. A practical pattern is to stage such effects inside the branch as
intent records or outbox entries, then execute them only after merge approval.

## Open Design Questions

- What is the default merge policy: source-over-target, reject on row conflict,
  column-level merge, or application-defined?
- Which validation policies are needed to match common isolation levels, and
  which policies are intentionally weaker but useful for agent workflows?
- Should merge apply support policy-selected subsets of a branch diff?
- What API hides branch graph details from autonomous agents while preserving
  explicit review and merge control for humans?
- How should Chronos represent reserved commit successors for non-interval stores such
  as filesystem, vector, document, or search indexes?
- Which external effects must be represented as staged intents before merge?
- What retention policy keeps enough fork-base state for merge without growing
  metadata indefinitely?
