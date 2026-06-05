---- MODULE IntervalBranching ----
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS BranchIds, SegmentIds, Keys, Values, Root, RootSegment, Nil, MaxInterval

ASSUME BranchIds # {}
ASSUME SegmentIds # {}
ASSUME Keys # {}
ASSUME Values # {}
ASSUME Root \in BranchIds
ASSUME RootSegment \in SegmentIds
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

Row(k, v, lo, hi, deleted, writer) ==
  [ key |-> k,
    val |-> v,
    lo |-> lo,
    hi |-> hi,
    deleted |-> deleted,
    writer |-> writer ]

VARIABLES branches,
          parent,
          seg,
          currentSegment,
          activeSegments,
          segmentInfo,
          segmentParent,
          segmentOwner,
          segmentAncestors,
          rows,
          specView

vars == <<branches,
          parent,
          seg,
          currentSegment,
          activeSegments,
          segmentInfo,
          segmentParent,
          segmentOwner,
          segmentAncestors,
          rows,
          specView>>

EmptyTable == [k \in Keys |-> Nil]

InitialSegment == [lo |-> 0, hi |-> MaxInterval, point |-> MaxInterval \div 2]

InitialRows ==
  {Row(k, Nil, 0, MaxInterval, TRUE, RootSegment) : k \in Keys}

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
  /\ currentSegment = [b \in BranchIds |-> RootSegment]
  /\ activeSegments = {RootSegment}
  /\ segmentInfo = [s \in SegmentIds |-> InitialSegment]
  /\ segmentParent = [s \in SegmentIds |-> RootSegment]
  /\ segmentOwner = [s \in SegmentIds |-> Root]
  /\ segmentAncestors =
       [s \in SegmentIds |->
          IF s = RootSegment THEN {RootSegment} ELSE {}]
  /\ rows = InitialRows
  /\ specView = [b \in BranchIds |-> EmptyTable]

OverlapsBranchInterval(r, b, k) ==
  LET s == segmentInfo[currentSegment[b]]
  IN
  /\ r.key = k
  /\ r.lo < s.hi
  /\ s.lo < r.hi

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
\* segment. Replacement rows are attributed to the current segment. Preservation
\* rows inherit the writer segment id from the row they preserve.
SpliceRows(rs, b, k, v, isDeleted) ==
  LET sid == currentSegment[b]
      s == segmentInfo[sid]
      hit == {r \in rs: OverlapsBranchInterval(r, b, k)}
      keep == rs \ hit
      left ==
        {Row(r.key, r.val, r.lo, s.lo, r.deleted, r.writer):
          r \in {x \in hit: x.lo < s.lo}}
      right ==
        {Row(r.key, r.val, s.hi, r.hi, r.deleted, r.writer):
          r \in {x \in hit: s.hi < x.hi}}
      middle == {Row(k, v, s.lo, s.hi, isDeleted, sid)}
  IN keep \cup left \cup middle \cup right

CreateBranch(p, b) ==
  \E continuationSid \in SegmentIds, childSid \in SegmentIds:
  LET sourceSid == currentSegment[p]
      source == segmentInfo[sourceSid]
      lo == source.lo
      hi == source.hi
      split == SplitPoint(lo, hi)
      parentSeg == [lo |-> lo, hi |-> split, point |-> lo + ((split - lo) \div 2)]
      childSeg == [lo |-> split, hi |-> hi, point |-> split + ((hi - split) \div 2)]
  IN
  /\ p \in branches
  /\ b \in BranchIds \ branches
  /\ continuationSid \in SegmentIds \ activeSegments
  /\ childSid \in SegmentIds \ (activeSegments \cup {continuationSid})
  /\ hi - lo >= 2 * MinSplitWidth
  /\ branches' = branches \cup {b}
  /\ parent' = [parent EXCEPT ![b] = p]
  /\ seg' = [seg EXCEPT ![p] = parentSeg, ![b] = childSeg]
  /\ currentSegment' = [currentSegment EXCEPT ![p] = continuationSid, ![b] = childSid]
  /\ activeSegments' = activeSegments \cup {continuationSid, childSid}
  /\ segmentInfo' = [segmentInfo EXCEPT ![continuationSid] = parentSeg, ![childSid] = childSeg]
  /\ segmentParent' = [segmentParent EXCEPT ![continuationSid] = sourceSid, ![childSid] = sourceSid]
  /\ segmentOwner' = [segmentOwner EXCEPT ![continuationSid] = p, ![childSid] = b]
  /\ segmentAncestors' =
       [segmentAncestors EXCEPT
          ![continuationSid] = segmentAncestors[sourceSid] \cup {continuationSid},
          ![childSid] = segmentAncestors[sourceSid] \cup {childSid}]
  /\ specView' = [specView EXCEPT ![b] = specView[p]]
  /\ UNCHANGED rows

Put(b, k, v) ==
  /\ b \in branches
  /\ k \in Keys
  /\ v \in Values
  /\ rows' = SpliceRows(rows, b, k, v, FALSE)
  /\ specView' = [specView EXCEPT ![b][k] = v]
  /\ UNCHANGED <<branches,
                  parent,
                  seg,
                  currentSegment,
                  activeSegments,
                  segmentInfo,
                  segmentParent,
                  segmentOwner,
                  segmentAncestors>>

Delete(b, k) ==
  /\ b \in branches
  /\ k \in Keys
  /\ rows' = SpliceRows(rows, b, k, Nil, TRUE)
  /\ specView' = [specView EXCEPT ![b][k] = Nil]
  /\ UNCHANGED <<branches,
                  parent,
                  seg,
                  currentSegment,
                  activeSegments,
                  segmentInfo,
                  segmentParent,
                  segmentOwner,
                  segmentAncestors>>

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
  /\ r.writer \in SegmentIds

TypeOK ==
  /\ branches \subseteq BranchIds
  /\ Root \in branches
  /\ parent \in [BranchIds -> BranchIds]
  /\ seg \in [BranchIds -> Segment]
  /\ currentSegment \in [BranchIds -> SegmentIds]
  /\ activeSegments \subseteq SegmentIds
  /\ RootSegment \in activeSegments
  /\ segmentInfo \in [SegmentIds -> Segment]
  /\ segmentParent \in [SegmentIds -> SegmentIds]
  /\ segmentOwner \in [SegmentIds -> BranchIds]
  /\ segmentAncestors \in [SegmentIds -> SUBSET SegmentIds]
  /\ \A b \in branches:
       /\ currentSegment[b] \in activeSegments
       /\ ValidSegment(seg[b])
       /\ seg[b] = segmentInfo[currentSegment[b]]
  /\ \A s \in activeSegments: ValidSegment(segmentInfo[s])
  /\ rows \subseteq
       {r \in [key: Keys, val: Val, lo: Points, hi: Points, deleted: BOOLEAN, writer: SegmentIds]:
          r.lo < r.hi}
  /\ \A r \in rows: RowTypeOK(r)
  /\ specView \in [BranchIds -> Table]

SegmentAncestryOK ==
  /\ segmentAncestors[RootSegment] = {RootSegment}
  /\ \A s \in activeSegments:
       /\ s \in segmentAncestors[s]
       /\ segmentAncestors[s] \subseteq activeSegments
       /\ IF s = RootSegment
          THEN TRUE
          ELSE
            /\ segmentParent[s] \in activeSegments
            /\ segmentAncestors[s] =
                 segmentAncestors[segmentParent[s]] \cup {s}

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

WriterSegmentKnown ==
  \A r \in rows:
    r.writer \in activeSegments

WriterIntervalContainsRow ==
  \A r \in rows:
    /\ segmentInfo[r.writer].lo <= r.lo
    /\ r.hi <= segmentInfo[r.writer].hi

VisibleWriterOnReaderPath ==
  \A b \in branches:
    \A k \in Keys:
      \A r \in VisibleRows(b, k):
        r.writer \in segmentAncestors[currentSegment[b]]

StateBound ==
  Cardinality(branches) <= 3

====
