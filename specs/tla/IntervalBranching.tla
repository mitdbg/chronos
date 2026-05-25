---- MODULE IntervalBranching ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BranchIds, Keys, Values, Root, Nil, MaxInterval

ASSUME BranchIds # {}
ASSUME Keys # {}
ASSUME Values # {}
ASSUME Root \in BranchIds
ASSUME Nil \notin Values
ASSUME MaxInterval \in Nat
ASSUME MaxInterval > 4

Val == Values \cup {Nil}
Table == [Keys -> Val]
Points == 0..MaxInterval

MinSplitWidth == 2
ContinuationPercent == 5
PercentDenominator == 100

Max2(a, b) == IF a >= b THEN a ELSE b
Min2(a, b) == IF a <= b THEN a ELSE b

LeftWidth(lo, hi) ==
  LET width == hi - lo
      raw == (width * ContinuationPercent) \div PercentDenominator
  IN Max2(MinSplitWidth, Min2(width - MinSplitWidth, raw))

SplitPoint(lo, hi) == lo + LeftWidth(lo, hi)

Segment == [lo: Points, hi: Points, point: Points]

Row(k, v, lo, hi, deleted) ==
  [key |-> k, val |-> v, lo |-> lo, hi |-> hi, deleted |-> deleted]

VARIABLES branches, parent, seg, rows, specView

vars == <<branches, parent, seg, rows, specView>>

EmptyTable == [k \in Keys |-> Nil]

InitialSegment == [lo |-> 0, hi |-> MaxInterval, point |-> MaxInterval \div 2]

InitialRows ==
  {Row(k, Nil, 0, MaxInterval, TRUE) : k \in Keys}

ValidSegment(s) ==
  /\ s.lo \in Points
  /\ s.hi \in Points
  /\ s.point \in Points
  /\ s.lo < s.point
  /\ s.point < s.hi
  /\ s.hi <= MaxInterval

Init ==
  /\ branches = {Root}
  /\ parent = [b \in BranchIds |-> Root]
  /\ seg = [b \in BranchIds |-> InitialSegment]
  /\ rows = InitialRows
  /\ specView = [b \in BranchIds |-> EmptyTable]

OverlapsBranchInterval(r, b, k) ==
  /\ r.key = k
  /\ r.lo < seg[b].hi
  /\ seg[b].lo < r.hi

VisibleRows(b, k) ==
  {r \in rows:
    /\ r.key = k
    /\ r.lo <= seg[b].point
    /\ seg[b].point < r.hi
    /\ ~r.deleted}

Read(b, k) ==
  LET visible == VisibleRows(b, k)
  IN IF visible = {}
     THEN Nil
     ELSE (CHOOSE r \in visible: TRUE).val

\* Splice all physical rows for a logical key that overlap the current branch
\* segment. The replacement may be an upsert row or a delete tombstone.
SpliceRows(rs, b, k, v, isDeleted) ==
  LET hit == {r \in rs: OverlapsBranchInterval(r, b, k)}
      keep == rs \ hit
      left ==
        {Row(r.key, r.val, r.lo, seg[b].lo, r.deleted):
          r \in {x \in hit: x.lo < seg[b].lo}}
      right ==
        {Row(r.key, r.val, seg[b].hi, r.hi, r.deleted):
          r \in {x \in hit: seg[b].hi < x.hi}}
      middle == {Row(k, v, seg[b].lo, seg[b].hi, isDeleted)}
  IN keep \cup left \cup middle \cup right

CreateBranch(p, b) ==
  LET lo == seg[p].lo
      hi == seg[p].hi
      split == SplitPoint(lo, hi)
      parentSeg == [lo |-> lo, hi |-> split, point |-> lo + ((split - lo) \div 2)]
      childSeg == [lo |-> split, hi |-> hi, point |-> split + ((hi - split) \div 2)]
  IN
  /\ p \in branches
  /\ b \in BranchIds \ branches
  /\ hi - lo >= 2 * MinSplitWidth
  /\ branches' = branches \cup {b}
  /\ parent' = [parent EXCEPT ![b] = p]
  /\ seg' = [seg EXCEPT ![p] = parentSeg, ![b] = childSeg]
  /\ specView' = [specView EXCEPT ![b] = specView[p]]
  /\ UNCHANGED rows

Put(b, k, v) ==
  /\ b \in branches
  /\ k \in Keys
  /\ v \in Values
  /\ rows' = SpliceRows(rows, b, k, v, FALSE)
  /\ specView' = [specView EXCEPT ![b][k] = v]
  /\ UNCHANGED <<branches, parent, seg>>

Delete(b, k) ==
  /\ b \in branches
  /\ k \in Keys
  /\ rows' = SpliceRows(rows, b, k, Nil, TRUE)
  /\ specView' = [specView EXCEPT ![b][k] = Nil]
  /\ UNCHANGED <<branches, parent, seg>>

Next ==
  \/ \E p \in BranchIds, b \in BranchIds: CreateBranch(p, b)
  \/ \E b \in BranchIds, k \in Keys, v \in Values: Put(b, k, v)
  \/ \E b \in BranchIds, k \in Keys: Delete(b, k)

Spec == Init /\ [][Next]_vars

RowTypeOK(r) ==
  /\ r.key \in Keys
  /\ r.val \in Val
  /\ r.lo \in Points
  /\ r.hi \in Points
  /\ r.lo < r.hi
  /\ r.hi <= MaxInterval
  /\ r.deleted \in BOOLEAN

TypeOK ==
  /\ branches \subseteq BranchIds
  /\ Root \in branches
  /\ parent \in [BranchIds -> BranchIds]
  /\ seg \in [BranchIds -> Segment]
  /\ \A b \in branches: ValidSegment(seg[b])
  /\ rows \subseteq {r \in [key: Keys, val: Val, lo: Points, hi: Points, deleted: BOOLEAN]: r.lo < r.hi}
  /\ \A r \in rows: RowTypeOK(r)
  /\ specView \in [BranchIds -> Table]

NoOverlappingPhysicalRowsPerKey ==
  \A r1 \in rows, r2 \in rows:
    (r1 # r2 /\ r1.key = r2.key) =>
      ~(r1.lo < r2.hi /\ r2.lo < r1.hi)

AtMostOneVisibleRow ==
  \A b \in branches, k \in Keys:
    Cardinality(VisibleRows(b, k)) <= 1

PhysicalRowsMatchBranchView ==
  \A b \in branches, k \in Keys:
    Read(b, k) = specView[b][k]

StateBound ==
  Cardinality(branches) <= 3

====
