------------------------ MODULE AgentWorkspaceRetention ------------------------
(***************************************************************************)
(* Workspace retention across the ops of a durable agent (issue #2274).    *)
(*                                                                         *)
(* docs/agent-adapter.md, section "Workspaces", is the prose spec.         *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(* AgentWorkspace.tla models the calls of one op. This model has several   *)
(* ops and one call per op.                                                *)
(*                                                                         *)
(* An op commits a snapshot: the tool call writes it, and history records  *)
(* the result, so the snapshot becomes the head. The op then schedules a   *)
(* retention activity with the head as its input. An op can instead reset  *)
(* the workspace, which schedules a reset activity. An attempt of an       *)
(* activity can run at any later time, also after a newer op committed.    *)
(* That is a stale attempt whose lease the engine reclaimed.               *)
(*                                                                         *)
(* One switch gives the design and its broken form:                        *)
(*   KeyBy  "generation" is the design: the key of a snapshot holds the    *)
(*          generation of its op. Retention deletes each key but its input *)
(*          head of that generation or older. A reset deletes the keys     *)
(*          below its bound.                                               *)
(*          "content" keys a snapshot by its content only. Retention then  *)
(*          deletes every key but its input head, and a reset deletes all. *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Ops,
    Contents,
    KeyBy

ASSUME KeyBy \in {"generation", "content"}

\* No head: an empty workspace.
None == <<0, "none">>

\* The key of a snapshot with content v, written by op o.
Key(o, v) == IF KeyBy = "generation" THEN <<o, v>> ELSE <<0, v>>

VARIABLES
    op,        \* The op that runs now. Ops + 1 means that no op is left.
    blobs,     \* The keys in the store.
    head,      \* The head of the agent.
    tasks,     \* The retention and reset activities that ops scheduled.
    staleRan   \* An attempt ran with an input that is not the head now.

vars == <<op, blobs, head, tasks, staleRan>>

Init ==
    /\ op = 1
    /\ blobs = {}
    /\ head = None
    /\ tasks = {}
    /\ staleRan = FALSE

\* The op writes a snapshot with content v, and history records it. The op
\* end schedules a retention of the new head.
Commit(v) ==
    /\ op <= Ops
    /\ blobs' = blobs \union {Key(op, v)}
    /\ head' = Key(op, v)
    /\ tasks' = tasks \union {[kind |-> "retain", keep |-> Key(op, v), at |-> op]}
    /\ op' = op + 1
    /\ UNCHANGED staleRan

\* The op resets the workspace. The bound is the generation of the op, so
\* each later snapshot is at or above it.
Reset ==
    /\ op <= Ops
    /\ head' = None
    /\ tasks' = tasks \union {[kind |-> "reset", keep |-> None, at |-> op]}
    /\ op' = op + 1
    /\ UNCHANGED <<blobs, staleRan>>

\* What one attempt of task t leaves in the store.
After(t) ==
    IF KeyBy = "generation"
    THEN IF t.kind = "retain"
         THEN {b \in blobs : b = t.keep \/ b[1] > t.keep[1]}
         ELSE {b \in blobs : b[1] >= t.at}
    ELSE IF t.kind = "retain"
         THEN blobs \intersect {t.keep}
         ELSE {}

\* An attempt of a scheduled activity runs. It can run again later.
Run(t) ==
    /\ t \in tasks
    /\ blobs' = After(t)
    /\ staleRan' = (staleRan \/ t.at < op - 1)
    /\ UNCHANGED <<op, head, tasks>>

Next ==
    \/ \E v \in Contents : Commit(v)
    \/ Reset
    \/ \E t \in tasks : Run(t)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* The head is in the store, so the next call can load it.
HeadPresent == head = None \/ head \in blobs

\* A reachability witness: an attempt of an older op's activity runs after
\* a newer op committed. TLC must violate it.
NoStaleRun == ~staleRan

=============================================================================
