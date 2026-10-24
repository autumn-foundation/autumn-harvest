------------------------------ MODULE AgentMesh ------------------------------
(***************************************************************************)
(* The durable peer send of an agent mesh (issue #2269).                   *)
(*                                                                         *)
(* DESIGN-2262.md section 7 is the prose spec.                             *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(*                                                                         *)
(* Agent A sends PeerMessage ops to agent B by workflow id. Both agents    *)
(* are keyed entities. The send key is <from>:<seq>. A durable sender      *)
(* sets from to <agent id>@<incarnation>. An agent_loop sender sets it to  *)
(* its run id. The seq is durable state of A. It moves only when the send  *)
(* commits with the decision of A. The model has one sender, so the seq    *)
(* is the key.                                                             *)
(*                                                                         *)
(* A send takes one of two paths:                                          *)
(*   inline  Same shard. The decision of A inserts the signal row of B in  *)
(*           its own transaction.                                          *)
(*   outbox  Cross shard. The decision of A writes an outbox row. A relay  *)
(*           delivers the row at least once, and in any order.             *)
(*                                                                         *)
(* B dedupes in two layers. Each layer is a switch:                        *)
(*   EngineDedupe  The engine refuses a signal key that it took before.    *)
(*                 "chain" keeps the keys across continue-as-new, as the   *)
(*                 engine does. "run" keeps the keys of one run only.      *)
(*                 "off" keeps none.                                       *)
(*   SeqDedupe     The agent layer of B dedupes by seq.                    *)
(*                 "applied_set" is the design. B keeps the set of the     *)
(*                 seqs that it applied, as floor and above. Each seq up   *)
(*                 to floor is applied, and above holds the applied seqs   *)
(*                 over floor. B skips a seq in that set.                  *)
(*                 "last_seq" keeps the highest applied seq only. B skips  *)
(*                 each seq up to it.                                      *)
(*                 "off" skips nothing.                                    *)
(*                                                                         *)
(* MaxInFlight bounds the sends of A that wait for their delivery. The     *)
(* outbox can reorder two such sends. The "last_seq" layer then skips the  *)
(* earlier one, which it never applied. The "applied_set" layer applies    *)
(* it, so the design allows two sends in flight.                           *)
(*                                                                         *)
(* Each message carries the outbound mandate of A, a set of tools. As      *)
(* Mandate::outbound does, it is the meet of the mandate of A and the      *)
(* tools of the peer entry of B. B runs the message under                  *)
(* Effective(inbound). Accept = "meet" is the design, local.accept.        *)
(* Accept = "join" widens.                                                 *)
(*                                                                         *)
(* The nemeses are always on, within their bounds:                         *)
(*   Crash        A loses its decision in flight. Its durable state stays, *)
(*                so the next decision sends the same seq again.           *)
(*   RelayAgain   The relay loses a delivery outcome. The row stays, so    *)
(*                the relay delivers it again.                             *)
(*   OwnerChange  The owner epoch of B moves. A decision of the old owner  *)
(*                aborts at its commit, and the new owner replays B.       *)
(*   Checkpoint   B continues as new while a message is in flight.         *)
(*                                                                         *)
(* The fence itself is OwnerLease.tla. This model takes it as given: a     *)
(* decision commits only at the epoch that it read.                        *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Tools,          \* The tool names. Model values.
    LocalB,         \* The local mandate of B: the tools of its profile.
    MandateA,       \* The tools of the mandate of A.
    PeerTools,      \* The tools of the peer entry of B in the roster of A.
    MaxSeq,         \* The number of sends of A.
    MaxInFlight,    \* The bound on sends of A that wait for delivery.
    MaxCrash,       \* The bound on crashes of A.
    MaxEpoch,       \* The bound on the owner epoch of B.
    MaxDup,         \* The bound on lost delivery outcomes of the relay.
    MaxCheckpoint,  \* The bound on continue-as-new of B.
    EngineDedupe,   \* "chain", "run" or "off".
    SeqDedupe,      \* "applied_set", "last_seq" or "off".
    Accept          \* "meet" (the design) or "join" (a widening defect).

ASSUME LocalB \subseteq Tools /\ MandateA \subseteq Tools
ASSUME PeerTools \subseteq Tools
ASSUME EngineDedupe \in {"chain", "run", "off"}
ASSUME SeqDedupe \in {"applied_set", "last_seq", "off"}
ASSUME Accept \in {"meet", "join"}
ASSUME {MaxSeq, MaxInFlight, MaxCrash, MaxEpoch, MaxDup, MaxCheckpoint}
         \subseteq Nat
ASSUME MaxEpoch >= 1 /\ MaxInFlight >= 1

VARIABLES
    aSeq,      \* Durable state of A: the seq of its last committed send.
    aDec,      \* The decision of A in flight: {} or one send.
    crashes,   \* The crashes of A so far.
    outbox,    \* The outbox rows that the relay has not resolved.
    dups,      \* The lost delivery outcomes so far.
    keys,      \* The signal keys that the engine took for B.
    inbox,     \* The signal rows of B that no decision consumed, in order.
    acked,     \* The seqs whose delivery the history of A records.
    lastSeq,   \* Durable state of B: the highest seq that it applied.
    floor,     \* Durable state of B: each seq up to floor is applied.
    above,     \* Durable state of B: the applied seqs over floor.
    applied,   \* Ghost: the messages that B applied, from its history.
    epoch,     \* The owner epoch of B.
    bDecs,     \* The decisions of B in flight, of every owner.
    runs,      \* The continue-as-new count of B.
    aborted,   \* Ghost: the seqs that a fenced decision of B held.
    seen       \* Ghost: the nemesis tags that the behaviour reached.

\* The dedupe state of B. Each mode reads its own part. The parts are
\* functions of the applied seqs, so the unused parts add no state.
dedupe == <<lastSeq, floor, above>>

vars == <<aSeq, aDec, crashes, outbox, dups, keys, inbox, acked, dedupe,
          applied, epoch, bDecs, runs, aborted, seen>>

Seqs == 1..MaxSeq
Paths == {"inline", "outbox"}
Msgs == [seq : Seqs, mandate : SUBSET MandateA]
Sends == [seq : Seqs, mandate : SUBSET MandateA, path : Paths]
Applications == [seq : Seqs, inbound : SUBSET Tools, eff : SUBSET Tools]
Decisions == [epoch : 1..MaxEpoch, msg : Msgs]

\* The nemesis tags. The reachability witness must reach all of them.
WitnessTags == {"crash-before-commit", "crash-before-delivery",
                "crash-after-delivery", "relay-duplicate",
                "owner-change-abort", "replay-after-abort",
                "checkpoint-in-flight", "out-of-order-apply"}

\* The mandate that A sends: Mandate::outbound narrows it to the peer.
Outbound == MandateA \cap PeerTools

\* The tools that B may call while it runs a message with this mandate.
Effective(inbound) ==
    IF Accept = "meet" THEN LocalB \cap inbound ELSE LocalB \cup inbound

\* The seqs that the history of A commits.
Committed == 1..aSeq

\* The committed sends whose delivery A has not seen.
Undelivered == Committed \ acked

\* The seqs that B applied.
AppliedSeqs == {applied[i].seq : i \in DOMAIN applied}

\* The seqs of the signal rows that wait in the inbox of B.
Queued == {inbox[i].seq : i \in DOMAIN inbox}

\* True when B consumes the message but does not apply it.
Skips(m) ==
    CASE SeqDedupe = "applied_set" -> m.seq <= floor \/ m.seq \in above
      [] SeqDedupe = "last_seq"    -> m.seq <= lastSeq
      [] OTHER                     -> FALSE

\* The floor after B applies seq s: the end of the run of applied seqs
\* that starts at floor + 1.
FloorAfter(s) ==
    LET set == above \cup {s}
    IN  CHOOSE f \in floor..MaxSeq :
          /\ \A k \in (floor + 1)..f : k \in set
          /\ (f + 1) \notin set

\* The applied seqs over the new floor, after B applies seq s.
AboveAfter(s) == {k \in above \cup {s} : k > FloorAfter(s)}

TypeOK ==
    /\ aSeq \in 0..MaxSeq
    /\ aDec \subseteq Sends /\ Cardinality(aDec) <= 1
    /\ crashes \in 0..MaxCrash
    /\ outbox \subseteq Msgs
    /\ dups \in 0..MaxDup
    /\ keys \subseteq Seqs
    /\ inbox \in Seq(Msgs)
    /\ acked \subseteq Seqs
    /\ lastSeq \in 0..MaxSeq
    /\ floor \in 0..MaxSeq
    /\ above \subseteq Seqs
    /\ applied \in Seq(Applications)
    /\ epoch \in 1..MaxEpoch
    /\ bDecs \subseteq Decisions
    /\ runs \in 0..MaxCheckpoint
    /\ aborted \subseteq Seqs
    /\ seen \subseteq WitnessTags

Init ==
    /\ aSeq = 0
    /\ aDec = {}
    /\ crashes = 0
    /\ outbox = {}
    /\ dups = 0
    /\ keys = {}
    /\ inbox = <<>>
    /\ acked = {}
    /\ lastSeq = 0
    /\ floor = 0
    /\ above = {}
    /\ applied = <<>>
    /\ epoch = 1
    /\ bDecs = {}
    /\ runs = 0
    /\ aborted = {}
    /\ seen = {}

-----------------------------------------------------------------------------
(* Actions of the sender and the relay *)

\* The signal insert into B. The engine refuses a key that it took. A
\* refused insert is still a delivery: the send succeeds.
Insert(m) ==
    IF EngineDedupe # "off" /\ m.seq \in keys
    THEN UNCHANGED <<inbox, keys>>
    ELSE /\ inbox' = Append(inbox, m)
         /\ keys' = keys \cup {m.seq}

\* A decides to send its next message, on either path. It waits while
\* MaxInFlight sends wait for their delivery.
Decide ==
    /\ aDec = {}
    /\ aSeq < MaxSeq
    /\ Cardinality(Undelivered) < MaxInFlight
    /\ \E p \in Paths :
         aDec' = {[seq |-> aSeq + 1, mandate |-> Outbound, path |-> p]}
    /\ UNCHANGED <<aSeq, crashes, outbox, dups, keys, inbox, acked, dedupe,
                   applied, epoch, bDecs, runs, aborted, seen>>

\* The decision of A commits with its send, in one transaction. The inline
\* path inserts the signal row of B in it. The outbox path writes a row.
SendCommit ==
    /\ aDec # {}
    /\ LET d == CHOOSE x \in aDec : TRUE
           m == [seq |-> d.seq, mandate |-> d.mandate]
       IN /\ aSeq' = d.seq
          /\ IF d.path = "inline"
             THEN /\ Insert(m)
                  /\ acked' = acked \cup {d.seq}
                  /\ UNCHANGED outbox
             ELSE /\ outbox' = outbox \cup {m}
                  /\ UNCHANGED <<inbox, keys, acked>>
    /\ aDec' = {}
    /\ UNCHANGED <<crashes, dups, dedupe, applied, epoch, bDecs, runs,
                   aborted, seen>>

\* A crashes. Its decision in flight is lost. Its durable state stays, so a
\* new decision sends the same seq under the same key.
Crash ==
    /\ crashes < MaxCrash
    /\ crashes' = crashes + 1
    /\ aDec' = {}
    /\ seen' = seen
         \cup (IF aDec # {} THEN {"crash-before-commit"} ELSE {})
         \cup (IF outbox # {} THEN {"crash-before-delivery"} ELSE {})
         \cup (IF acked \cap Queued # {} THEN {"crash-after-delivery"}
               ELSE {})
    /\ UNCHANGED <<aSeq, outbox, dups, keys, inbox, acked, dedupe, applied,
                   epoch, bDecs, runs, aborted>>

\* The relay delivers an outbox row. The history of A records the delivery,
\* and the row is resolved.
Relay(r) ==
    /\ r \in outbox
    /\ Insert(r)
    /\ outbox' = outbox \ {r}
    /\ acked' = acked \cup {r.seq}
    /\ UNCHANGED <<aSeq, aDec, crashes, dups, dedupe, applied, epoch, bDecs,
                   runs, aborted, seen>>

\* The relay delivers a row, but it loses the outcome. The row stays, so the
\* relay delivers it again.
RelayAgain(r) ==
    /\ r \in outbox
    /\ dups < MaxDup
    /\ Insert(r)
    /\ dups' = dups + 1
    /\ seen' = seen \cup {"relay-duplicate"}
    /\ UNCHANGED <<aSeq, aDec, crashes, outbox, acked, dedupe, applied,
                   epoch, bDecs, runs, aborted>>

-----------------------------------------------------------------------------
(* Actions of the receiver *)

\* The owner of B begins a decision on the head of the inbox. One owner runs
\* one decision at a time.
Begin ==
    /\ inbox # <<>>
    /\ ~\E d \in bDecs : d.epoch = epoch
    /\ bDecs' = bDecs \cup {[epoch |-> epoch, msg |-> Head(inbox)]}
    /\ UNCHANGED <<aSeq, aDec, crashes, outbox, dups, keys, inbox, acked,
                   dedupe, applied, epoch, runs, aborted, seen>>

\* The decision commits. The fence compares the epoch. A fenced decision
\* aborts, so its state change and its consumption roll back. A decision
\* that passes consumes the message, applies it and updates the dedupe
\* state, in one transaction.
Commit(d) ==
    /\ d \in bDecs
    /\ bDecs' = bDecs \ {d}
    /\ IF d.epoch = epoch
       THEN LET s == d.msg.seq
            IN /\ inbox' = Tail(inbox)
               /\ IF Skips(d.msg)
                  THEN UNCHANGED <<applied, dedupe>>
                  ELSE /\ applied' = Append(applied,
                                      [seq |-> s,
                                       inbound |-> d.msg.mandate,
                                       eff |-> Effective(d.msg.mandate)])
                       /\ lastSeq' = IF s > lastSeq THEN s ELSE lastSeq
                       /\ floor' = FloorAfter(s)
                       /\ above' = AboveAfter(s)
               /\ seen' = seen
                    \cup (IF s \in aborted THEN {"replay-after-abort"}
                          ELSE {})
                    \cup (IF ~Skips(d.msg) /\ s < lastSeq
                          THEN {"out-of-order-apply"} ELSE {})
               /\ UNCHANGED aborted
       ELSE /\ aborted' = aborted \cup {d.msg.seq}
            /\ seen' = seen \cup {"owner-change-abort"}
            /\ UNCHANGED <<inbox, applied, dedupe>>
    /\ UNCHANGED <<aSeq, aDec, crashes, outbox, dups, keys, acked, epoch,
                   runs>>

\* A new owner takes B, so the epoch moves. A decision of the old owner
\* stays in flight until its commit, which the fence refuses.
OwnerChange ==
    /\ epoch < MaxEpoch
    /\ epoch' = epoch + 1
    /\ UNCHANGED <<aSeq, aDec, crashes, outbox, dups, keys, inbox, acked,
                   dedupe, applied, bDecs, runs, aborted, seen>>

\* B continues as new. The new run carries the state and the waiting ops. A
\* "run" dedupe keeps only the keys of the ops that it carries.
Checkpoint ==
    /\ runs < MaxCheckpoint
    /\ ~\E d \in bDecs : d.epoch = epoch
    /\ runs' = runs + 1
    /\ keys' = IF EngineDedupe = "run" THEN Queued ELSE keys
    /\ seen' = IF Queued \cup {r.seq : r \in outbox} # {}
               THEN seen \cup {"checkpoint-in-flight"} ELSE seen
    /\ UNCHANGED <<aSeq, aDec, crashes, outbox, dups, inbox, acked, dedupe,
                   applied, epoch, bDecs, aborted>>

Next ==
    \/ Decide
    \/ SendCommit
    \/ Crash
    \/ \E r \in outbox : Relay(r) \/ RelayAgain(r)
    \/ Begin
    \/ \E d \in bDecs : Commit(d)
    \/ OwnerChange
    \/ Checkpoint

Spec == Init /\ [][Next]_vars

\* The relay, the owner of B and its commits make progress. The nemeses
\* have bounds, so each behaviour runs out of them.
Fairness ==
    /\ WF_vars(SendCommit)
    /\ WF_vars(\E r \in outbox : Relay(r))
    /\ WF_vars(Begin)
    /\ WF_vars(\E d \in bDecs : Commit(d))

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* Safety invariants *)

\* B applies each seq at most once.
AtMostOnce ==
    \A i, j \in DOMAIN applied : i # j => applied[i].seq # applied[j].seq

\* A delivered message is applied, or it still waits in the inbox. A
\* decision consumes a message only at its commit.
NoLostAck == \A s \in acked : s \in AppliedSeqs \cup Queued

\* B runs each message within its local mandate and within the inbound
\* mandate. A replay or a duplicate runs under the same rule. So B never
\* holds a tool that A does not hold, and authority only narrows along the
\* chain.
MandateNarrows ==
    \A i \in DOMAIN applied :
        /\ applied[i].eff \subseteq LocalB
        /\ applied[i].eff \subseteq applied[i].inbound
        /\ applied[i].eff \subseteq MandateA

\* The decision of the current owner holds the head of the inbox, so its
\* commit consumes the message that it read.
ConsumesTheHead ==
    \A d \in bDecs :
        d.epoch = epoch => inbox # <<>> /\ Head(inbox) = d.msg

\* floor and above encode the applied seqs exactly. The encoding is also
\* compact: above holds no seq at floor + 1 or lower, so floor is as high
\* as it can be.
AppliedSetExact ==
    /\ {k \in Seqs : k <= floor} \cup above = AppliedSeqs
    /\ \A k \in above : k > floor + 1

-----------------------------------------------------------------------------
(* Liveness. The pass configs check it under LiveSpec.                     *)

\* B applies each committed send. With AtMostOnce, B applies it exactly
\* once.
EventuallyApplied == \A s \in Seqs : (s \in Committed) ~> (s \in AppliedSeqs)

-----------------------------------------------------------------------------
(* Reachability witness. AgentMeshReach.cfg expects TLC to violate it.     *)
(* The violation is one behaviour of the design that reaches every tag:    *)
(*   crash-before-commit    A crashes with a send that it did not commit.  *)
(*   crash-before-delivery  A crashes with its send in the outbox.         *)
(*   crash-after-delivery   A crashes with its message queued at B.        *)
(*   relay-duplicate        The relay delivers one row twice.              *)
(*   owner-change-abort     The fence aborts a decision of an old owner.   *)
(*   replay-after-abort     The new owner runs the message again.          *)
(*   checkpoint-in-flight   B continues as new with a message in flight.   *)
(*   out-of-order-apply     B applies a seq below one that it applied.     *)

SomeNemesisUnseen == ~(WitnessTags \subseteq seen)

=============================================================================
