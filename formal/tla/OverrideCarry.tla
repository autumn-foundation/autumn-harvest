---------------------------- MODULE OverrideCarry ----------------------------
(***************************************************************************)
(* The carry of a DAG schedule override to the shard that owns the DAG.   *)
(*                                                                         *)
(* The design record in docs/design/ is the prose spec.                    *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(* The code is schedule_override::carry_dag_override and                   *)
(* scheduler::carry_misplaced_overrides.                                   *)
(*                                                                         *)
(* Each shard holds the override table of one DAG. An operator sets or    *)
(* clears the override on the owner. A routing change moves the owner.    *)
(* Each shard that holds a row but does not own the DAG carries the row   *)
(* to the owner in two transactions. It copies the row on the owner, then *)
(* deletes the row on the source. Either step can fail, and the carry     *)
(* then starts again. Each set makes a new version, as the updated_at of  *)
(* a row does.                                                             *)
(*                                                                         *)
(* Switches give the design and its broken forms:                         *)
(*   Receipt     TRUE: the copy records the version that it delivered,   *)
(*               and skips a delivered version.                            *)
(*   DropSource  TRUE: the delete also removes the receipt of the source. *)
(*   Fenced      TRUE: each carry write runs under the fence of both      *)
(*               shards.                                                   *)
(*   FlipWhen    "settled": a routing change waits until no carry runs    *)
(*               and no shard but the owner holds a row. "idle" waits     *)
(*               only for the carries. "any" does not wait. The two last  *)
(*               values are known limits of the design.                    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Shards,
    MaxVersion,
    MaxFlips,
    Receipt,
    DropSource,
    Fenced,
    FlipWhen

ASSUME MaxVersion \in Nat /\ MaxFlips \in Nat
ASSUME Receipt \in BOOLEAN /\ DropSource \in BOOLEAN /\ Fenced \in BOOLEAN
ASSUME FlipWhen \in {"settled", "idle", "any"}

\* Version 0 means "no row" or "no receipt".
None == 0

VARIABLES
    owner,      \* The shard that owns the DAG.
    rows,       \* The override version that each shard holds.
    receipts,   \* The delivered version that each shard records.
    pc,         \* The carry step of each source shard.
    dst,        \* The destination of the carry of each source shard.
    ver,        \* The version that the carry of each source shard moves.
    next,       \* The version of the next operator set.
    flips,      \* The owner moves so far.
    latest,     \* Ghost: the version of the last operator set.
    cleared,    \* Ghost: the versions that an operator cleared, per shard.
    anyClear,   \* Ghost: an operator cleared a row at some time.
    promoted,   \* The shards that the new region promoted.
    lateWrites  \* Ghost: carry writes after a promotion of their shard.

vars == <<owner, rows, receipts, pc, dst, ver, next, flips, latest, cleared,
          anyClear, promoted, lateWrites>>

First == CHOOSE s \in Shards : TRUE

Init ==
    /\ owner = First
    /\ rows = [s \in Shards |-> None]
    /\ receipts = [s \in Shards |-> None]
    /\ pc = [s \in Shards |-> "idle"]
    /\ dst = [s \in Shards |-> First]
    /\ ver = [s \in Shards |-> None]
    /\ next = 1
    /\ flips = 0
    /\ latest = None
    /\ cleared = [s \in Shards |-> {}]
    /\ anyClear = FALSE
    /\ promoted = {}
    /\ lateWrites = 0

\* Whether the old process may write on shard s. A fenced write fails when
\* the new region promoted s.
MayWrite(s) == ~Fenced \/ s \notin promoted

\* Count a write on shard s that lands after its promotion.
Late(s) == IF s \in promoted THEN lateWrites + 1 ELSE lateWrites

\* An operator sets a new version on the owner.
Set ==
    /\ next <= MaxVersion
    /\ rows' = [rows EXCEPT ![owner] = next]
    /\ latest' = next
    /\ next' = next + 1
    /\ UNCHANGED <<owner, receipts, pc, dst, ver, flips, cleared, anyClear,
                   promoted, lateWrites>>

\* An operator clears the row on the owner. A clear of no row is a no-op,
\* so the model leaves it out.
Clear ==
    /\ rows[owner] # None
    /\ cleared' = [cleared EXCEPT ![owner] = @ \union {rows[owner]}]
    /\ rows' = [rows EXCEPT ![owner] = None]
    /\ anyClear' = TRUE
    /\ UNCHANGED <<owner, receipts, pc, dst, ver, next, flips, latest,
                   promoted, lateWrites>>

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
    /\ UNCHANGED <<rows, receipts, pc, dst, ver, next, latest, cleared,
                   anyClear, promoted, lateWrites>>

\* The copy transaction of source s on the owner. It reads the source row
\* first.
Copy(s) ==
    LET d == owner
        v == rows[s]
        delivered == Receipt /\ receipts[d] = v
    IN
    /\ pc[s] = "idle"
    /\ s # owner
    /\ v # None
    /\ MayWrite(d)
    /\ rows' = IF ~delivered /\ rows[d] = None
               THEN [rows EXCEPT ![d] = v] ELSE rows
    /\ receipts' = IF Receipt THEN [receipts EXCEPT ![d] = v] ELSE receipts
    /\ lateWrites' = Late(d)
    /\ pc' = [pc EXCEPT ![s] = "copied"]
    /\ dst' = [dst EXCEPT ![s] = d]
    /\ ver' = [ver EXCEPT ![s] = v]
    /\ UNCHANGED <<owner, next, flips, latest, cleared, anyClear, promoted>>

\* The delete transaction on source s. It matches the copied version.
Delete(s) ==
    /\ pc[s] = "copied"
    /\ MayWrite(s)
    /\ rows' = IF rows[s] = ver[s] THEN [rows EXCEPT ![s] = None] ELSE rows
    /\ receipts' = IF DropSource
                   THEN [receipts EXCEPT ![s] = None] ELSE receipts
    /\ lateWrites' = Late(s)
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, dst, ver, next, flips, latest, cleared, anyClear,
                   promoted>>

\* The delete of source s fails. The next tick starts the carry again.
FailDelete(s) ==
    /\ pc[s] = "copied"
    /\ pc' = [pc EXCEPT ![s] = "idle"]
    /\ UNCHANGED <<owner, rows, receipts, dst, ver, next, flips, latest,
                   cleared, anyClear, promoted, lateWrites>>

\* The new region promotes shard s. The old process keeps running.
Promote(s) ==
    /\ s \notin promoted
    /\ promoted' = promoted \union {s}
    /\ UNCHANGED <<owner, rows, receipts, pc, dst, ver, next, flips, latest,
                   cleared, anyClear, lateWrites>>

Next ==
    \/ Set \/ Clear
    \/ \E s \in Shards :
        Flip(s) \/ Copy(s) \/ Delete(s) \/ FailDelete(s) \/ Promote(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* A version that an operator cleared on a shard never comes back to it.
ClearIsKept == \A s \in Shards : rows[s] \notin cleared[s]

\* With no clear and no promotion, a quiet owner holds an override once an
\* operator set one. A row that the owner already holds wins over a copy,
\* so the owner can hold an older version.
NothingLostWithoutAClear ==
    (Quiet /\ ~anyClear /\ promoted = {} /\ latest # None)
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
