---- MODULE CopyBranching ----
EXTENDS Naturals, FiniteSets

CONSTANTS BranchIds, Keys, Values, Root, Nil

ASSUME BranchIds # {}
ASSUME Keys # {}
ASSUME Values # {}
ASSUME Root \in BranchIds
ASSUME Nil \notin Values

Val == Values \cup {Nil}

VARIABLES branches, parent, data, forkData

vars == <<branches, parent, data, forkData>>

\* The copy backend stores one complete physical table per active branch.
Table == [Keys -> Val]

EmptyTable == [k \in Keys |-> Nil]

Read(b, k) == data[b][k]

Init ==
  /\ branches = {Root}
  /\ parent = [b \in BranchIds |-> Root]
  /\ data = [b \in BranchIds |-> EmptyTable]
  /\ forkData = [b \in BranchIds |-> EmptyTable]

CreateBranch(p, b) ==
  /\ p \in branches
  /\ b \in BranchIds \ branches
  /\ branches' = branches \cup {b}
  /\ parent' = [parent EXCEPT ![b] = p]
  /\ data' = [data EXCEPT ![b] = data[p]]
  /\ forkData' = [forkData EXCEPT ![b] = data[p]]

Put(b, k, v) ==
  /\ b \in branches
  /\ k \in Keys
  /\ v \in Values
  /\ data' = [data EXCEPT ![b][k] = v]
  /\ UNCHANGED <<branches, parent, forkData>>

Delete(b, k) ==
  /\ b \in branches
  /\ k \in Keys
  /\ data' = [data EXCEPT ![b][k] = Nil]
  /\ UNCHANGED <<branches, parent, forkData>>

Next ==
  \/ \E p \in BranchIds, b \in BranchIds: CreateBranch(p, b)
  \/ \E b \in BranchIds, k \in Keys, v \in Values: Put(b, k, v)
  \/ \E b \in BranchIds, k \in Keys: Delete(b, k)

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ branches \subseteq BranchIds
  /\ Root \in branches
  /\ parent \in [BranchIds -> BranchIds]
  /\ data \in [BranchIds -> Table]
  /\ forkData \in [BranchIds -> Table]

\* Inactive branch tables are irrelevant; active branches always have a full
\* private table, so reading a branch is just reading that branch's copy.
ActiveBranchesHavePrivateTables ==
  \A b \in branches, k \in Keys: Read(b, k) \in Val

StateBound ==
  Cardinality(branches) <= 3

====
