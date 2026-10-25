---------------------------- MODULE AgentWorkspace ----------------------------
(***************************************************************************)
(* The workspace head of a durable agent (issue #2274).                    *)
(*                                                                         *)
(* docs/agent-adapter.md, section "Workspaces", is the prose spec.         *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(*                                                                         *)
(* One op runs Calls tool calls in order. Each call can run as several     *)
(* attempts, for example after a lost ack. An attempt loads the head,      *)
(* runs, and writes a snapshot. A stale attempt of an earlier call can     *)
(* still write. History records the result of one attempt of each call.    *)
(* Retention keeps the head and deletes every other snapshot.              *)
(*                                                                         *)
(* Two switches give the design and its broken forms:                     *)
(*   AdoptOn     "record" is the design: a recorded result moves the head. *)
(*               "write" moves the head when a snapshot is written.        *)
(*   RetainWhen  "op_end" is the design: retention runs after the last     *)
(*               call. "any_time" can run while a call runs.               *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Calls,
    Attempts,
    AdoptOn,
    RetainWhen

ASSUME AdoptOn \in {"record", "write"}
ASSUME RetainWhen \in {"op_end", "any_time"}

\* A snapshot is named by the call and the attempt that wrote it.
None == <<0, 0>>

VARIABLES
    call,      \* The call that runs now. Calls + 1 means the op ended.
    blobs,     \* The snapshots in the store.
    written,   \* The snapshots that an attempt wrote.
    recorded,  \* The snapshot of the result that history recorded, per call.
    head       \* The head of the agent.

vars == <<call, blobs, written, recorded, head>>

Init ==
    /\ call = 1
    /\ blobs = {}
    /\ written = {}
    /\ recorded = [c \in 1..Calls |-> None]
    /\ head = None

\* An attempt of call c writes its snapshot. A stale attempt of an earlier
\* call can write too.
Write(c, a) ==
    /\ c <= call
    /\ <<c, a>> \notin written
    /\ written' = written \union {<<c, a>>}
    /\ blobs' = blobs \union {<<c, a>>}
    /\ head' = IF AdoptOn = "write" /\ c = call THEN <<c, a>> ELSE head
    /\ UNCHANGED <<call, recorded>>

\* History records the result of one attempt of the call that runs. The
\* next call starts.
Record(c, a) ==
    /\ c = call
    /\ c <= Calls
    /\ recorded[c] = None
    /\ <<c, a>> \in written
    /\ recorded' = [recorded EXCEPT ![c] = <<c, a>>]
    /\ head' = IF AdoptOn = "record" THEN <<c, a>> ELSE head
    /\ call' = call + 1
    /\ UNCHANGED <<blobs, written>>

\* Retention keeps the head and deletes every other snapshot.
Retain ==
    /\ RetainWhen = "any_time" \/ call = Calls + 1
    /\ blobs' = IF head = None THEN {} ELSE blobs \intersect {head}
    /\ UNCHANGED <<call, written, recorded, head>>

Next ==
    \/ \E c \in 1..Calls, a \in Attempts : Write(c, a) \/ Record(c, a)
    \/ Retain

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* The head is the snapshot of a result that history recorded.
HeadIsRecorded == head = None \/ \E c \in 1..Calls : recorded[c] = head

\* The head is in the store, so the next call can load it.
HeadPresent == head = None \/ head \in blobs

\* A reachability witness: some attempt wrote a snapshot that history never
\* recorded. TLC must violate it.
SomeStaleWriteUnseen ==
    ~\E c \in 1..Calls, a \in Attempts :
        /\ <<c, a>> \in written
        /\ recorded[c] # None
        /\ recorded[c] # <<c, a>>

=============================================================================
