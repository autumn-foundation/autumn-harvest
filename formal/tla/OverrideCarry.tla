---------------------------- MODULE OverrideCarry ----------------------------
(***************************************************************************)
(* The carry of a DAG schedule override to the shard that owns the DAG.   *)
(*                                                                         *)
(* docs/design/DESIGN-2413.md is the prose spec.                           *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(* The code is schedule_override::carry_dag_override and                   *)
(* scheduler::carry_misplaced_overrides.                                   *)
(*                                                                         *)
(* Two shards hold the override table of one DAG. An operator sets or     *)
(* clears the override on the owner. A routing change flips the owner.    *)
(* The carry moves a row from the other shard to the owner in two         *)
(* transactions: a copy on the owner, then a delete on the source. The    *)
(* delete can fail, and the carry then starts again. Each set makes a new *)
(* version, as the updated_at of a row does.                               *)
(*                                                                         *)
(* Three switches give the design and its broken forms:                   *)
(*   Receipt            TRUE is the design: the copy records the version  *)
(*                      that it delivered, and skips a delivered version. *)
(*   DropSourceReceipt  TRUE is the design: the delete also removes the   *)
(*                      receipt of the source.                             *)
(*   Fenced             TRUE is the design: each carry write checks the   *)
(*                      generation of both shards in its transaction.     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    MaxVersion,
    MaxFlips,
    Receipt,
    DropSourceReceipt,
    Fenced

ASSUME MaxVersion \in Nat /\ MaxFlips \in Nat
ASSUME Receipt \in BOOLEAN /\ DropSourceReceipt \in BOOLEAN /\ Fenced \in BOOLEAN

Shards == {"a", "b"}
Other(s) == IF s = "a" THEN "b" ELSE "a"

\* Version 0 means "no row" or "no receipt".
None == 0

VARIABLES
    owner,      \* The shard that owns the DAG.
    rows,       \* The override version that each shard holds.
    receipts,   \* The delivered version that each shard records.
    pc,         \* "idle", or "copied" between the copy and the delete.
    carried,    \* The version that the pending delete matches.
    source,     \* The shard of the pending delete.
    next,       \* The version of the next operator set.
    flips,      \* The owner flips so far.
    latest,     \* Ghost: the version of the last operator set.
    cleared,    \* Ghost: the versions that an operator cleared, per shard.
    anyClear,   \* Ghost: an operator cleared a row at some time.
    promoted,   \* The shards that the new region promoted.
    lateWrites  \* Ghost: carry writes after a promotion of their shard.

vars == <<owner, rows, receipts, pc, carried, source, next, flips, latest,
          cleared, anyClear, promoted, lateWrites>>

Init ==
    /\ owner = "a"
    /\ rows = [s \in Shards |-> None]
    /\ receipts = [s \in Shards |-> None]
    /\ pc = "idle"
    /\ carried = None
    /\ source = "a"
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
    /\ UNCHANGED <<owner, receipts, pc, carried, source, flips, cleared,
                   anyClear, promoted, lateWrites>>

\* An operator clears the row on the owner. A clear of no row is a no-op,
\* so the model leaves it out.
Clear ==
    /\ rows[owner] # None
    /\ cleared' = [cleared EXCEPT ![owner] = @ \union {rows[owner]}]
    /\ rows' = [rows EXCEPT ![owner] = None]
    /\ anyClear' = TRUE
    /\ UNCHANGED <<owner, receipts, pc, carried, source, next, flips, latest,
                   promoted, lateWrites>>

\* A routing change moves the DAG. It happens only when no carry is
\* pending. See "Limits" in the design record.
Flip ==
    /\ flips < MaxFlips
    /\ pc = "idle"
    /\ rows[Other(owner)] = None
    /\ owner' = Other(owner)
    /\ flips' = flips + 1
    /\ UNCHANGED <<rows, receipts, pc, carried, source, next, latest, cleared,
                   anyClear, promoted, lateWrites>>

\* The copy transaction on the owner. It reads the source row first.
Copy ==
    LET s == Other(owner)
        d == owner
        v == rows[s]
        delivered == Receipt /\ receipts[d] = v
    IN
    /\ pc = "idle"
    /\ v # None
    /\ MayWrite(s) /\ MayWrite(d)
    /\ rows' = IF ~delivered /\ rows[d] = None
               THEN [rows EXCEPT ![d] = v] ELSE rows
    /\ receipts' = IF Receipt THEN [receipts EXCEPT ![d] = v] ELSE receipts
    /\ lateWrites' = Late(d)
    /\ pc' = "copied"
    /\ carried' = v
    /\ source' = s
    /\ UNCHANGED <<owner, next, flips, latest, cleared, anyClear, promoted>>

\* The delete transaction on the source. It matches the copied version.
Delete ==
    /\ pc = "copied"
    /\ MayWrite(source) /\ MayWrite(Other(source))
    /\ rows' = IF rows[source] = carried
               THEN [rows EXCEPT ![source] = None] ELSE rows
    /\ receipts' = IF DropSourceReceipt
                   THEN [receipts EXCEPT ![source] = None] ELSE receipts
    /\ lateWrites' = Late(source)
    /\ pc' = "idle"
    /\ carried' = None
    /\ UNCHANGED <<owner, source, next, flips, latest, cleared, anyClear,
                   promoted>>

\* The delete fails. The next tick starts the carry again.
FailDelete ==
    /\ pc = "copied"
    /\ pc' = "idle"
    /\ carried' = None
    /\ UNCHANGED <<owner, rows, receipts, source, next, flips, latest, cleared,
                   anyClear, promoted, lateWrites>>

\* The new region promotes shard s. The old process keeps running.
Promote(s) ==
    /\ s \notin promoted
    /\ promoted' = promoted \union {s}
    /\ UNCHANGED <<owner, rows, receipts, pc, carried, source, next, flips,
                   latest, cleared, anyClear, lateWrites>>

Next ==
    \/ Set \/ Clear \/ Flip \/ Copy \/ Delete \/ FailDelete
    \/ \E s \in Shards : Promote(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* No carry is pending, and only the owner holds a row.
Quiet == pc = "idle" /\ rows[Other(owner)] = None

\* A version that an operator cleared on a shard never comes back to it.
ClearIsKept == \A s \in Shards : rows[s] \notin cleared[s]

\* With no clear, a quiet owner holds the last set version.
NothingLostWithoutAClear ==
    (Quiet /\ ~anyClear /\ promoted = {}) => rows[owner] = latest

\* The old process writes nothing on a shard after its promotion.
NoWriteAfterPromotion == lateWrites = 0

\* A reachability witness: the source still holds a row that an operator
\* cleared on the owner after its copy. TLC must violate it.
NoClearOfACopiedRow ==
    ~(/\ pc = "idle"
      /\ rows[Other(owner)] # None
      /\ rows[Other(owner)] \in cleared[owner])

=============================================================================
