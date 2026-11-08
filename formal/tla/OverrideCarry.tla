---------------------------- MODULE OverrideCarry ----------------------------
(***************************************************************************)
(* The carry of a DAG schedule override to the shard that owns the DAG.   *)
(*                                                                         *)
(* The design records in docs/design/ are the prose spec: DESIGN-2413.md  *)
(* and DESIGN-2602.md. docs/testing/formal-methods.md says how to run     *)
(* this model. The code is schedule_override::carry_dag_override and      *)
(* scheduler::carry_misplaced_overrides.                                   *)
(*                                                                         *)
(* Each shard holds the override table of one DAG. An operator sets or    *)
(* clears the override on the owner. A routing change moves the owner at  *)
(* any time. Each shard that holds a row but does not own the DAG carries *)
(* the row to the owner. It copies the row on the owner, then deletes the *)
(* row on the source. Either step can fail, and the carry then starts     *)
(* again.                                                                  *)
(*                                                                         *)
(* A carry that starts under one route can finish under the next. So the  *)
(* model also covers two processes with two routers.                      *)
(*                                                                         *)
(* A version is the updated_at of a row. Each shard keeps a floor: the    *)
(* newest version that it retired. A clear or a replace raises the floor. *)
(* A copy skips a version at or below the floor of its destination.       *)
(*                                                                         *)
(* Switches give the design and its broken forms:                         *)
(*   Floor     TRUE: a clear or a replace raises the floor of its shard.  *)
(*   Hlc       TRUE: a set takes a version above each version that its    *)
(*             shard knows. FALSE: any version, as a skewed clock gives.  *)
(*   Newest    TRUE: a newer copy replaces an older row on the            *)
(*             destination. FALSE: the destination row always wins, as   *)
(*             in issue #2413.                                             *)
(*   Serial    TRUE: a carry locks both shards from its read to the end   *)
(*             of its delete.                                              *)
(*   Fenced    TRUE: each carry write runs under the fence of both        *)
(*             shards.                                                     *)
(*   FlipWhen  "settled": a routing change waits until no carry runs and  *)
(*             no shard but the owner holds a row. "idle" waits only for  *)
(*             the carries. "any" does not wait.                          *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Shards,
    MaxVersion,
    MaxFlips,
    Floor,
    Hlc,
    Newest,
    Serial,
    Fenced,
    FlipWhen

ASSUME MaxVersion \in Nat /\ MaxFlips \in Nat
ASSUME Floor \in BOOLEAN /\ Hlc \in BOOLEAN /\ Newest \in BOOLEAN
ASSUME Serial \in BOOLEAN /\ Fenced \in BOOLEAN
ASSUME FlipWhen \in {"settled", "idle", "any"}

\* Version 0 means "no row" or "no floor".
None == 0

Versions == 1..MaxVersion

\* The holder of a free lock.
Free == "free"
ASSUME Free \notin Shards

VARIABLES
    owner,      \* The shard that owns the DAG.
    rows,       \* The override version that each shard holds.
    floor,      \* The newest version that each shard retired.
    holder,     \* The source shard whose carry locks each shard.
    pc,         \* The carry step of each source shard.
    dst,        \* The destination of the carry of each source shard.
    ver,        \* The version that the carry of each source shard moves.
    sets,       \* The operator sets so far.
    flips,      \* The owner moves so far.
    cleared,    \* Ghost: the versions that an operator cleared or replaced,
                \* per shard.
    anyClear,   \* Ghost: an operator cleared a row at some time.
    promoted,   \* The shards that the new region promoted.
    lateWrites  \* Ghost: carry writes after a promotion of their shard.

vars == <<owner, rows, floor, holder, pc, dst, ver, sets, flips, cleared,
          anyClear, promoted, lateWrites>>

First == CHOOSE s \in Shards : TRUE

Max(a, b) == IF a >= b THEN a ELSE b

Init ==
    /\ owner = First
    /\ rows = [s \in Shards |-> None]
    /\ floor = [s \in Shards |-> None]
    /\ holder = [s \in Shards |-> Free]
    /\ pc = [s \in Shards |-> "idle"]
    /\ dst = [s \in Shards |-> First]
    /\ ver = [s \in Shards |-> None]
    /\ sets = 0
    /\ flips = 0
    /\ cleared = [s \in Shards |-> {}]
    /\ anyClear = FALSE
    /\ promoted = {}
    /\ lateWrites = 0

\* Whether the old process may write on shard s. A fenced write fails when
\* the new region promoted s.
MayWrite(s) == ~Fenced \/ s \notin promoted

\* Count a write on shard s that lands after its promotion.
Late(s) == IF s \in promoted THEN lateWrites + 1 ELSE lateWrites

\* Whether no carry locks shard s.
Unlocked(s) == ~Serial \/ holder[s] = Free

\* The floor of shard s after an operator retires its row.
Retired(s) ==
    IF Floor THEN [floor EXCEPT ![s] = Max(@, rows[s])] ELSE floor

\* The ghost record of shard s after an operator retires its row.
Gone(s) ==
    IF rows[s] # None
    THEN [cleared EXCEPT ![s] = @ \union {rows[s]}]
    ELSE cleared

\* The versions that a set on shard s can take.
NewVersions(s) ==
    IF Hlc THEN {v \in Versions : v > Max(rows[s], floor[s])}
    ELSE Versions

\* An operator sets version v on the owner. The old row is replaced. The
\* operator write takes the registration lock, so it waits for a carry.
Set(v) ==
    /\ sets < MaxVersion
    /\ v \in NewVersions(owner)
    /\ Unlocked(owner)
    /\ floor' = Retired(owner)
    /\ cleared' = Gone(owner)
    /\ rows' = [rows EXCEPT ![owner] = v]
    /\ sets' = sets + 1
    /\ UNCHANGED <<owner, holder, pc, dst, ver, flips, anyClear, promoted,
                   lateWrites>>

\* An operator clears the row on the owner. A clear of no row is a no-op,
\* so the model leaves it out.
Clear ==
    /\ rows[owner] # None
    /\ Unlocked(owner)
    /\ floor' = Retired(owner)
    /\ cleared' = Gone(owner)
    /\ rows' = [rows EXCEPT ![owner] = None]
    /\ anyClear' = TRUE
    /\ UNCHANGED <<owner, holder, pc, dst, ver, sets, flips, promoted,
                   lateWrites>>

\* No carry runs.
Idle == \A s \in Shards : pc[s] = "idle"

\* No carry runs, and only the owner holds a row.
Quiet == Idle /\ \A s \in Shards : s # owner => rows[s] = None

\* A routing change moves the DAG to shard t. FlipWhen says when.
Flip(t) ==
    /\ flips < MaxFlips
    /\ t # owner
    /\ \/ FlipWhen = "any"
       \/ FlipWhen = "idle" /\ Idle
       \/ FlipWhen = "settled" /\ Quiet
    /\ owner' = t
    /\ flips' = flips + 1
    /\ UNCHANGED <<rows, floor, holder, pc, dst, ver, sets, cleared,
                   anyClear, promoted, lateWrites>>

\* Source s starts a carry to the owner. With Serial it locks both shards.
\* Ascending lock order and a bounded wait make the two locks one step.
Begin(s) ==
    LET d == owner IN
    /\ pc[s] = "idle"
    /\ s # d
    /\ rows[s] # None
    /\ Unlocked(s) /\ Unlocked(d)
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = s, ![d] = s]
                 ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "locked"]
    /\ dst' = [dst EXCEPT ![s] = d]
    /\ UNCHANGED <<owner, rows, floor, ver, sets, flips, cleared, anyClear,
                   promoted, lateWrites>>

\* The copy transaction of source s commits on its destination d. It reads
\* the source row first, and it frees the lock of d.
\*
\* A copy at or below the floor of d writes nothing. Otherwise it lands
\* when d holds no row. With Newest, it also replaces an older row of d,
\* and that row raises the floor of d.
Copy(s) ==
    LET d == dst[s]
        v == rows[s]
        r == rows[d]
        fresh == v > floor[d]
        lands == fresh /\ (r = None \/ (Newest /\ r < v))
    IN
    /\ pc[s] = "locked"
    /\ v # None
    /\ MayWrite(d)
    /\ rows' = IF lands THEN [rows EXCEPT ![d] = v] ELSE rows
    /\ floor' = IF lands /\ r # None
                THEN [floor EXCEPT ![d] = Max(@, r)] ELSE floor
    /\ holder' = IF Serial THEN [holder EXCEPT ![d] = Free] ELSE holder
    /\ lateWrites' = Late(d)
    /\ pc' = [pc EXCEPT ![s] = "copied"]
    /\ ver' = [ver EXCEPT ![s] = v]
    /\ UNCHANGED <<owner, dst, sets, flips, cleared, anyClear, promoted>>

\* The carry of source s ends before its copy commits. Nothing changes.
Abort(s) ==
    /\ pc[s] = "locked"
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free, ![dst[s]] = Free]
                 ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, rows, floor, dst, ver, sets, flips, cleared,
                   anyClear, promoted, lateWrites>>

\* The delete transaction on source s commits. It matches the copied
\* version, and it frees the lock of the source.
Delete(s) ==
    /\ pc[s] = "copied"
    /\ MayWrite(s)
    /\ rows' = IF rows[s] = ver[s] THEN [rows EXCEPT ![s] = None] ELSE rows
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free] ELSE holder
    /\ lateWrites' = Late(s)
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, floor, dst, ver, sets, flips, cleared, anyClear,
                   promoted>>

\* The delete of source s fails. The next tick starts the carry again.
FailDelete(s) ==
    /\ pc[s] = "copied"
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free] ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, rows, floor, dst, ver, sets, flips, cleared,
                   anyClear, promoted, lateWrites>>

\* The new region promotes shard s. The old process keeps running.
Promote(s) ==
    /\ s \notin promoted
    /\ promoted' = promoted \union {s}
    /\ UNCHANGED <<owner, rows, floor, holder, pc, dst, ver, sets, flips,
                   cleared, anyClear, lateWrites>>

Next ==
    \/ Clear
    \/ \E v \in Versions : Set(v)
    \/ \E s \in Shards :
        \/ Flip(s) \/ Begin(s) \/ Copy(s) \/ Abort(s)
        \/ Delete(s) \/ FailDelete(s) \/ Promote(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* A version that an operator cleared or replaced on a shard never comes
\* back to it.
ClearIsKept == \A s \in Shards : rows[s] \notin cleared[s]

\* With no clear and no promotion, a quiet owner holds an override once an
\* operator set one.
NothingLostWithoutAClear ==
    (Quiet /\ ~anyClear /\ promoted = {} /\ sets > 0)
        => rows[owner] # None

\* The old process writes nothing on a shard after its promotion.
NoWriteAfterPromotion == lateWrites = 0

\* A reachability witness: a shard that does not own the DAG still holds a
\* row that an operator cleared on the owner. TLC must violate it.
NoClearOfACopiedRow ==
    ~\E s \in Shards :
        /\ s # owner
        /\ rows[s] # None
        /\ rows[s] \in cleared[owner]

=============================================================================
