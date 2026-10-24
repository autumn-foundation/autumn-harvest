------------------------ MODULE AgentWorkspaceRetention ------------------------
(***************************************************************************)
(* Workspace retention across the ops and lives of a durable agent (issue  *)
(* #2274).                                                                 *)
(*                                                                         *)
(* docs/agent-adapter.md, section "Workspaces", is the prose spec.         *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(* AgentWorkspace.tla models the calls of one op. This model has several   *)
(* ops, one call per op, and lives.                                        *)
(*                                                                         *)
(* An op commits a snapshot: the tool call writes it, and history records  *)
(* the result, so the snapshot becomes the head. The op then runs a        *)
(* retention activity with the head as its input. An op can instead reset  *)
(* the workspace, which runs a reset activity. The workflow waits for the  *)
(* first completed attempt of each activity before the next op. An older   *)
(* attempt can still run at any later time, also after a newer op. That is *)
(* a stale attempt whose lease the engine reclaimed.                       *)
(*                                                                         *)
(* A delete ends a life. The next life starts with no head and with its    *)
(* generation at a floor.                                                  *)
(*                                                                         *)
(* Three switches give the design and its broken forms:                    *)
(*   KeyBy    "generation" is the design: the key of a snapshot holds the  *)
(*            generation of its op. Retention deletes each key but its     *)
(*            input head of that generation or older. A reset deletes the  *)
(*            keys below its bound and leaves a marker at the bound.       *)
(*            "content" keys a snapshot by its content only. Retention     *)
(*            then deletes every key but its input head, and a reset       *)
(*            deletes all.                                                 *)
(*   FloorBy  "store" is the design: a new life starts at the highest key  *)
(*            or marker in the store, or at the clock when that is higher. *)
(*            "clock" starts it at the clock alone, which can read low.    *)
(*   MarkReset TRUE is the design: a reset leaves a marker at its bound,   *)
(*            so the store floor still holds after a reset empties it.     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Ops,
    Contents,
    KeyBy,
    FloorBy,
    MarkReset

ASSUME KeyBy \in {"generation", "content"}
ASSUME FloorBy \in {"store", "clock"}
ASSUME MarkReset \in BOOLEAN

\* No head: an empty workspace.
None == <<0, "none">>

\* The content of the marker that a reset leaves.
Marker == "marker"

\* The key of a snapshot with content v at generation g.
Key(g, v) == IF KeyBy = "generation" THEN <<g, v>> ELSE <<0, v>>

Max(a, b) == IF a >= b THEN a ELSE b

VARIABLES
    op,        \* The ops that ran. Ops bounds the behaviours.
    gen,       \* The generation of the last op of this life.
    blobs,     \* The keys in the store.
    head,      \* The head of the agent.
    tasks,     \* The retention and reset activities that ops started.
    pending,   \* The activity of the last op, until its first attempt ends.
    staleRan   \* An attempt ran after a newer op.

vars == <<op, gen, blobs, head, tasks, pending, staleRan>>

\* The highest generation of a key in the store, or 0.
HighestGen ==
    IF blobs = {} THEN 0
    ELSE CHOOSE m \in {b[1] : b \in blobs} : \A b \in blobs : m >= b[1]

Init ==
    /\ op = 0
    /\ gen = 0
    /\ blobs = {}
    /\ head = None
    /\ tasks = {}
    /\ pending = {}
    /\ staleRan = FALSE

\* The op writes a snapshot with content v at the next generation, and
\* history records it. The op end starts a retention of the new head.
Commit(v) ==
    LET g == gen + 1
        t == [kind |-> "retain", keep |-> Key(g, v), bound |-> g, at |-> op + 1]
    IN /\ op < Ops
       /\ pending = {}
       /\ blobs' = blobs \union {Key(g, v)}
       /\ head' = Key(g, v)
       /\ gen' = g
       /\ tasks' = tasks \union {t}
       /\ pending' = {t}
       /\ op' = op + 1
       /\ UNCHANGED staleRan

\* The op resets the workspace at the next generation. Each later snapshot
\* of this life is above the bound.
Reset ==
    LET g == gen + 1
        t == [kind |-> "reset", keep |-> None, bound |-> g, at |-> op + 1]
    IN /\ op < Ops
       /\ pending = {}
       /\ head' = None
       /\ gen' = g
       /\ tasks' = tasks \union {t}
       /\ pending' = {t}
       /\ op' = op + 1
       /\ UNCHANGED <<blobs, staleRan>>

\* A delete ends the life. The new life starts at a floor. The clock of a
\* worker can read any value, so it is a free choice.
NewLife ==
    \E clock \in 0..Ops :
        /\ op < Ops
        /\ pending = {}
        /\ head' = None
        /\ gen' = IF FloorBy = "store" THEN Max(HighestGen, clock) ELSE clock
        /\ op' = op + 1
        /\ UNCHANGED <<blobs, tasks, pending, staleRan>>

\* What one attempt of task t leaves in the store.
After(t) ==
    IF KeyBy = "generation"
    THEN IF t.kind = "retain"
         THEN {b \in blobs : b = t.keep \/ b[1] > t.keep[1]}
         ELSE {b \in blobs : b[1] >= t.bound}
              \union (IF MarkReset THEN {<<t.bound, Marker>>} ELSE {})
    ELSE IF t.kind = "retain"
         THEN blobs \intersect {t.keep}
         ELSE {}

\* The first attempt of the activity of the last op ends.
Complete(t) ==
    /\ t \in pending
    /\ blobs' = After(t)
    /\ pending' = {}
    /\ UNCHANGED <<op, gen, head, tasks, staleRan>>

\* An attempt of an earlier activity runs again, at any time.
Run(t) ==
    /\ t \in tasks
    /\ blobs' = After(t)
    /\ staleRan' = (staleRan \/ t.at < op)
    /\ UNCHANGED <<op, gen, head, tasks, pending>>

Next ==
    \/ \E v \in Contents : Commit(v)
    \/ Reset
    \/ NewLife
    \/ \E t \in pending : Complete(t)
    \/ \E t \in tasks : Run(t)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* The head is in the store, so the next call can load it.
HeadPresent == head = None \/ head \in blobs

\* A reachability witness: an attempt of an older op's activity runs after
\* a newer op. TLC must violate it.
NoStaleRun == ~staleRan

=============================================================================
