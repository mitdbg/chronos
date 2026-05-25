---- MODULE LogBranching ----
EXTENDS Naturals, Integers, FiniteSets, TLC

CONSTANTS BranchIds, Keys, Values, Root, Nil, MaxLogEntries

ASSUME BranchIds # {}
ASSUME Keys # {}
ASSUME Values # {}
ASSUME Root \in BranchIds
ASSUME Nil \notin Values
ASSUME MaxLogEntries \in Nat
ASSUME MaxLogEntries > 0

Val == Values \cup {Nil}
Table == [Keys -> Val]
Ops == {"upsert", "delete"}
TxnIds == 0..MaxLogEntries
TxnLimits == (-1)..MaxLogEntries
LogIds == 1..MaxLogEntries

VARIABLES branches, parent, head, limit, depth, log, nextLogId, specView

vars == <<branches, parent, head, limit, depth, log, nextLogId, specView>>

EmptyTable == [k \in Keys |-> Nil]

InitLimit ==
  [b \in BranchIds |-> [a \in BranchIds |-> IF b = Root /\ a = Root THEN 0 ELSE -1]]

LogEntry ==
  [id: LogIds, branch: BranchIds, txn: TxnIds, key: Keys, val: Val, op: Ops]

Init ==
  /\ branches = {Root}
  /\ parent = [b \in BranchIds |-> Root]
  /\ head = [b \in BranchIds |-> 0]
  /\ limit = InitLimit
  /\ depth = [b \in BranchIds |-> 0]
  /\ log = {}
  /\ nextLogId = 1
  /\ specView = [b \in BranchIds |-> EmptyTable]

VisibleEntries(b, k) ==
  {e \in log:
    /\ e.key = k
    /\ limit[b][e.branch] >= e.txn}

\* A branch's visible log entries come from one lineage. Higher depth is nearer
\* to the checked-out branch; within a branch, higher txn/log id is newer.
Dominates(e1, e2) ==
  \/ depth[e1.branch] > depth[e2.branch]
  \/ /\ depth[e1.branch] = depth[e2.branch]
     /\ e1.txn > e2.txn
  \/ /\ depth[e1.branch] = depth[e2.branch]
     /\ e1.txn = e2.txn
     /\ e1.id > e2.id

LatestEntries(b, k) ==
  {e \in VisibleEntries(b, k):
    \A f \in VisibleEntries(b, k): ~Dominates(f, e)}

Read(b, k) ==
  LET latest == LatestEntries(b, k)
  IN IF latest = {}
     THEN Nil
     ELSE LET e == CHOOSE x \in latest: TRUE
          IN IF e.op = "delete" THEN Nil ELSE e.val

CreateBranch(p, b) ==
  /\ p \in branches
  /\ b \in BranchIds \ branches
  /\ branches' = branches \cup {b}
  /\ parent' = [parent EXCEPT ![b] = p]
  /\ head' = [head EXCEPT ![b] = head[p]]
  /\ limit' = [limit EXCEPT ![b] = [a \in BranchIds |-> IF a = b THEN head[p] ELSE limit[p][a]]]
  /\ depth' = [depth EXCEPT ![b] = depth[p] + 1]
  /\ specView' = [specView EXCEPT ![b] = specView[p]]
  /\ UNCHANGED <<log, nextLogId>>

Put(b, k, v) ==
  LET txn == head[b] + 1
      entry == [id |-> nextLogId, branch |-> b, txn |-> txn,
                key |-> k, val |-> v, op |-> "upsert"]
  IN
  /\ b \in branches
  /\ k \in Keys
  /\ v \in Values
  /\ nextLogId <= MaxLogEntries
  /\ log' = log \cup {entry}
  /\ head' = [head EXCEPT ![b] = txn]
  /\ limit' = [limit EXCEPT ![b][b] = txn]
  /\ nextLogId' = nextLogId + 1
  /\ specView' = [specView EXCEPT ![b][k] = v]
  /\ UNCHANGED <<branches, parent, depth>>

Delete(b, k) ==
  LET txn == head[b] + 1
      entry == [id |-> nextLogId, branch |-> b, txn |-> txn,
                key |-> k, val |-> Nil, op |-> "delete"]
  IN
  /\ b \in branches
  /\ k \in Keys
  /\ nextLogId <= MaxLogEntries
  /\ log' = log \cup {entry}
  /\ head' = [head EXCEPT ![b] = txn]
  /\ limit' = [limit EXCEPT ![b][b] = txn]
  /\ nextLogId' = nextLogId + 1
  /\ specView' = [specView EXCEPT ![b][k] = Nil]
  /\ UNCHANGED <<branches, parent, depth>>

Next ==
  \/ \E p \in BranchIds, b \in BranchIds: CreateBranch(p, b)
  \/ \E b \in BranchIds, k \in Keys, v \in Values: Put(b, k, v)
  \/ \E b \in BranchIds, k \in Keys: Delete(b, k)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ branches \subseteq BranchIds
  /\ Root \in branches
  /\ parent \in [BranchIds -> BranchIds]
  /\ head \in [BranchIds -> TxnIds]
  /\ limit \in [BranchIds -> [BranchIds -> TxnLimits]]
  /\ depth \in [BranchIds -> Nat]
  /\ log \subseteq LogEntry
  /\ nextLogId \in 1..(MaxLogEntries + 1)
  /\ specView \in [BranchIds -> Table]

UniqueLatestLogEntry ==
  \A b \in branches, k \in Keys:
    Cardinality(LatestEntries(b, k)) <= 1

NoLogEntryBeyondHead ==
  \A e \in log:
    /\ e.branch \in branches
    /\ e.txn <= head[e.branch]

LogReplayMatchesBranchView ==
  \A b \in branches, k \in Keys:
    Read(b, k) = specView[b][k]

StateBound ==
  Cardinality(branches) <= 3

====
