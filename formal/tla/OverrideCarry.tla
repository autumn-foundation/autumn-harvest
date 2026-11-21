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
(* any time. Each shard that holds a row or a floor but does not own the  *)
(* DAG carries both to the owner. It copies them on the owner, then       *)
(* deletes them on the source. Either step can fail, and the carry then   *)
(* starts again.                                                           *)
(*                                                                         *)
(* A carry that starts under one route can finish under the next. So the  *)
(* model also covers two processes with two routers.                      *)
(*                                                                         *)
(* A version is the updated_at of a row. Each shard keeps a floor. A      *)
(* replace raises it to the old row. A clear raises it to a new version,  *)
(* so a clear of no row counts too. A copy skips a version at or below    *)
(* the floor of its destination, and the floor kills an older row there.  *)
(*                                                                         *)
(* Copy is one step. The destination lock makes its statements atomic.    *)
(* Under that lock the delete match and the floor raise on a replace are  *)
(* defence in depth: no config needs them.                                *)
(*                                                                         *)
(* Switches give the design and its broken forms:                         *)
(*   Fresh       TRUE: each operator write takes a version above all      *)
(*               versions so far, as synchronized clocks give. FALSE: it  *)
(*               takes a version that its shard allows, as skewed clocks  *)
(*               give. Version numbers can then repeat.                   *)
(*   Floor       TRUE: a clear or a replace raises the floor.             *)
(*   CarryFloor  TRUE: a carry moves the floor of its source.             *)
(*   Hlc         TRUE: a write on a shard takes a version above all that  *)
(*               the shard knows. FALSE: any version, with Fresh FALSE.   *)
(*   Newest      TRUE: a newer copy replaces an older row on the          *)
(*               destination. FALSE: the destination row always wins, as *)
(*               in issue #2413.                                           *)
(*   Serial      TRUE: a carry locks both shards from its read to the end *)
(*               of its delete.                                            *)
(*   Fenced      TRUE: each carry write runs under the fence of both      *)
(*               shards.                                                   *)
(*   FlipWhen    "settled": a routing change waits until no carry runs    *)
(*               and no shard but the owner holds a row. "idle" waits     *)
(*               only for the carries. "any" does not wait.               *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Shards,
    MaxVersion,
    MaxFlips,
    Fresh,
    Floor,
    CarryFloor,
    Hlc,
    Newest,
    Serial,
    Fenced,
    FlipWhen

ASSUME MaxVersion \in Nat /\ MaxFlips \in Nat
ASSUME Fresh \in BOOLEAN /\ Floor \in BOOLEAN /\ CarryFloor \in BOOLEAN
ASSUME Hlc \in BOOLEAN /\ Newest \in BOOLEAN
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
    floor,      \* The floor of each shard.
    holder,     \* The source shard whose carry locks each shard.
    pc,         \* The carry step of each source shard.
    dst,        \* The destination of the carry of each source shard.
    ver,        \* The version that the carry of each source shard moves.
    writes,     \* The operator writes so far.
    flips,      \* The owner moves so far.
    latest,     \* Ghost: the result of the last operator write.
    cleared,    \* Ghost: the versions that an operator cleared or replaced.
    anyClear,   \* Ghost: an operator cleared at some time.
    promoted,   \* The shards that the new region promoted.
    lateWrites  \* Ghost: carry writes after a promotion of their shard.

vars == <<owner, rows, floor, holder, pc, dst, ver, writes, flips, latest,
          cleared, anyClear, promoted, lateWrites>>

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
    /\ writes = 0
    /\ flips = 0
    /\ latest = None
    /\ cleared = {}
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

\* The versions that an operator write on shard s can take.
NewVersions(s) ==
    IF Fresh THEN {writes + 1}
    ELSE IF Hlc THEN {v \in Versions : v > Max(rows[s], floor[s])}
    ELSE Versions

\* The ghost record after the owner retires its row.
Gone == IF rows[owner] # None THEN cleared \union {rows[owner]} ELSE cleared

\* An operator sets version v on the owner. The old row is replaced, and it
\* raises the floor. The write takes the registration lock, so it waits for
\* a carry.
Set(v) ==
    /\ writes < MaxVersion
    /\ v \in NewVersions(owner)
    /\ Unlocked(owner)
    /\ floor' = IF Floor THEN [floor EXCEPT ![owner] = Max(@, rows[owner])]
                ELSE floor
    /\ cleared' = Gone
    /\ rows' = [rows EXCEPT ![owner] = v]
    /\ latest' = v
    /\ writes' = writes + 1
    /\ UNCHANGED <<owner, holder, pc, dst, ver, flips, anyClear, promoted,
                   lateWrites>>

\* An operator clears the override on the owner. The clear takes version v,
\* and the floor rises to it. A clear of no row counts too.
Clear(v) ==
    /\ writes < MaxVersion
    /\ v \in NewVersions(owner)
    /\ Unlocked(owner)
    /\ floor' = IF Floor THEN [floor EXCEPT ![owner] = Max(@, v)] ELSE floor
    /\ cleared' = Gone
    /\ rows' = [rows EXCEPT ![owner] = None]
    /\ latest' = None
    /\ anyClear' = TRUE
    /\ writes' = writes + 1
    /\ UNCHANGED <<owner, holder, pc, dst, ver, flips, promoted, lateWrites>>

\* No carry runs.
Idle == \A s \in Shards : pc[s] = "idle"

\* No carry runs, and no shard but the owner holds a row.
Quiet == Idle /\ \A s \in Shards : s # owner => rows[s] = None

\* Quiet, and no shard but the owner holds a floor that it must carry.
Settled ==
    /\ Quiet
    /\ \A s \in Shards : s # owner /\ CarryFloor => floor[s] = None

\* A routing change moves the DAG to shard t. FlipWhen says when.
Flip(t) ==
    /\ flips < MaxFlips
    /\ t # owner
    /\ \/ FlipWhen = "any"
       \/ FlipWhen = "idle" /\ Idle
       \/ FlipWhen = "settled" /\ Quiet
    /\ owner' = t
    /\ flips' = flips + 1
    /\ UNCHANGED <<rows, floor, holder, pc, dst, ver, writes, latest,
                   cleared, anyClear, promoted, lateWrites>>

\* Whether source s holds something to carry.
Holds(s) == rows[s] # None \/ (CarryFloor /\ floor[s] # None)

\* Source s starts a carry to the owner. With Serial it locks both shards.
\* Ascending lock order and a bounded wait make the two locks one step.
Begin(s) ==
    LET d == owner IN
    /\ pc[s] = "idle"
    /\ s # d
    /\ Holds(s)
    /\ Unlocked(s) /\ Unlocked(d)
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = s, ![d] = s]
                 ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "locked"]
    /\ dst' = [dst EXCEPT ![s] = d]
    /\ UNCHANGED <<owner, rows, floor, ver, writes, flips, latest, cleared,
                   anyClear, promoted, lateWrites>>

\* The copy transaction of source s commits on its destination d. It reads
\* the source row and floor first, and it frees the lock of d.
\*
\* The source floor raises the floor of d, and that floor kills an older
\* row of d. The source row then lands above the floor when d holds no row.
\* With Newest, it also replaces an older row, which raises the floor.
Copy(s) ==
    LET d == dst[s]
        v == rows[s]
        f == IF CarryFloor THEN Max(floor[d], floor[s]) ELSE floor[d]
        r == IF rows[d] # None /\ rows[d] <= f THEN None ELSE rows[d]
        lands == v > f /\ (r = None \/ (Newest /\ r < v))
    IN
    /\ pc[s] = "locked"
    /\ Holds(s)
    /\ MayWrite(d)
    /\ rows' = [rows EXCEPT ![d] = IF lands THEN v ELSE r]
    /\ floor' = [floor EXCEPT ![d] = IF lands THEN Max(f, r) ELSE f]
    /\ holder' = IF Serial THEN [holder EXCEPT ![d] = Free] ELSE holder
    /\ lateWrites' = Late(d)
    /\ pc' = [pc EXCEPT ![s] = "copied"]
    /\ ver' = [ver EXCEPT ![s] = v]
    /\ UNCHANGED <<owner, dst, writes, flips, latest, cleared, anyClear,
                   promoted>>

\* The carry of source s ends before its copy commits. Nothing changes.
Abort(s) ==
    /\ pc[s] = "locked"
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free, ![dst[s]] = Free]
                 ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, rows, floor, dst, ver, writes, flips, latest,
                   cleared, anyClear, promoted, lateWrites>>

\* The delete transaction on source s commits. It matches the copied
\* version, it drops the carried floor, and it frees the lock of the source.
Delete(s) ==
    /\ pc[s] = "copied"
    /\ MayWrite(s)
    /\ rows' = IF rows[s] = ver[s] THEN [rows EXCEPT ![s] = None] ELSE rows
    /\ floor' = IF CarryFloor THEN [floor EXCEPT ![s] = None] ELSE floor
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free] ELSE holder
    /\ lateWrites' = Late(s)
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, dst, ver, writes, flips, latest, cleared, anyClear,
                   promoted>>

\* The delete of source s fails. The next tick starts the carry again.
FailDelete(s) ==
    /\ pc[s] = "copied"
    /\ holder' = IF Serial THEN [holder EXCEPT ![s] = Free] ELSE holder
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, rows, floor, dst, ver, writes, flips, latest,
                   cleared, anyClear, promoted, lateWrites>>

\* The new region promotes shard s. The old process keeps running.
Promote(s) ==
    /\ s \notin promoted
    /\ promoted' = promoted \union {s}
    /\ UNCHANGED <<owner, rows, floor, holder, pc, dst, ver, writes, flips,
                   latest, cleared, anyClear, lateWrites>>

Next ==
    \/ \E v \in Versions : Set(v) \/ Clear(v)
    \/ \E s \in Shards :
        \/ Flip(s) \/ Begin(s) \/ Copy(s) \/ Abort(s)
        \/ Delete(s) \/ FailDelete(s) \/ Promote(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* A settled owner holds no version that an operator cleared or replaced on
\* any shard. Fresh versions make a version name one write.
ClearIsKept == Settled => rows[owner] \notin cleared

\* With no promotion, a settled owner holds the result of the last operator
\* write. It holds no row after a clear.
LastWriteWins == (Settled /\ promoted = {}) => rows[owner] = latest

\* With no clear and no promotion, a quiet owner holds an override once an
\* operator set one. This holds with skewed clocks too.
NothingLostWithoutAClear ==
    (Quiet /\ ~anyClear /\ promoted = {} /\ latest # None)
        => rows[owner] # None

\* The old process writes nothing on a shard after its promotion.
NoWriteAfterPromotion == lateWrites = 0

\* A reachability witness: a shard that does not own the DAG still holds a
\* row that an operator cleared or replaced. TLC must violate it.
NoClearOfACopiedRow ==
    ~\E s \in Shards :
        /\ s # owner
        /\ rows[s] # None
        /\ rows[s] \in cleared

=============================================================================
