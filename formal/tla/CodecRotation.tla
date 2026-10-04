---------------------------- MODULE CodecRotation ----------------------------
(***************************************************************************)
(* Codec key re-encryption against PII erasure (issues #948 and #495).     *)
(*                                                                         *)
(* CLAUDE.md names two sanctioned writers of harvest_events.event_data.    *)
(* The rotation sweep reads a payload under the retired key and writes it  *)
(* again under the active key. Erasure writes a tombstone.                 *)
(*                                                                         *)
(* Cas = TRUE models compare_and_swap_event: the write applies only when   *)
(* the row still holds the bytes that the sweep read.                      *)
(* Cas = FALSE models a blind write. TLC then finds the race in which the  *)
(* sweep writes ciphertext over a tombstone and resurrects erased data.    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Rows,      \* The event rows.
    Sweepers,  \* The concurrent sweep workers.
    OldKey,    \* The retired codec key. A model value.
    NewKey,    \* The active codec key. A model value.
    Cas        \* TRUE: compare-and-swap write. FALSE: blind write.

ASSUME Cas \in BOOLEAN /\ OldKey /= NewKey

VARIABLES
    data,        \* data[r]: the payload field of row r.
    erased,      \* The rows that an erasure has tombstoned.
    snap,        \* snap[s]: the row and bytes that sweeper s read, or None.
    unresolved   \* unresolved[s]: the lost writes of sweeper s.

vars == <<data, erased, snap, unresolved>>

\* A payload field is ciphertext under a key, or the erasure tombstone.
\* The plaintext of row r is r itself. Re-encryption never changes it.
Cipher(k, r) == [kind |-> "cipher", key |-> k, pt |-> r]
Tombstone == [kind |-> "erased", key |-> NewKey, pt |-> "none"]
Values == {Cipher(k, r) : k \in {OldKey, NewKey}, r \in Rows} \cup {Tombstone}
None == [row |-> "none", val |-> Tombstone]

TypeOK ==
    /\ data \in [Rows -> Values]
    /\ erased \subseteq Rows
    /\ snap \in [Sweepers -> [row : Rows, val : Values] \cup {None}]
    /\ unresolved \in [Sweepers -> Nat]

Init ==
    /\ data = [r \in Rows |-> Cipher(OldKey, r)]
    /\ erased = {}
    /\ snap = [s \in Sweepers |-> None]
    /\ unresolved = [s \in Sweepers |-> 0]

-----------------------------------------------------------------------------
(* Actions *)

\* The sweep reads a row that is still under the retired key.
Read(s, r) ==
    /\ snap[s] = None
    /\ data[r] = Cipher(OldKey, r)
    /\ snap' = [snap EXCEPT ![s] = [row |-> r, val |-> data[r]]]
    /\ UNCHANGED <<data, erased, unresolved>>

\* The sweep writes the re-encoded bytes. A lost CAS counts as unresolved.
Write(s) ==
    /\ snap[s] /= None
    /\ LET r == snap[s].row
           new == Cipher(NewKey, snap[s].val.pt)
       IN IF ~Cas \/ data[r] = snap[s].val
          THEN /\ data' = [data EXCEPT ![r] = new]
               /\ UNCHANGED unresolved
          ELSE /\ unresolved' = [unresolved EXCEPT ![s] = @ + 1]
               /\ UNCHANGED data
    /\ snap' = [snap EXCEPT ![s] = None]
    /\ UNCHANGED erased

\* erase_workflow_payloads tombstones the payload field.
Erase(r) ==
    /\ r \notin erased
    /\ data' = [data EXCEPT ![r] = Tombstone]
    /\ erased' = erased \cup {r}
    /\ UNCHANGED <<snap, unresolved>>

Next ==
    \/ \E s \in Sweepers, r \in Rows : Read(s, r)
    \/ \E s \in Sweepers : Write(s)
    \/ \E r \in Rows : Erase(r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety invariants *)

\* An erased row stays erased.
ErasureIsFinal == \A r \in erased : data[r] = Tombstone

\* Re-encryption never changes the plaintext.
PlaintextPreserved ==
    \A r \in Rows : data[r].kind = "cipher" => data[r].pt = r

=============================================================================
