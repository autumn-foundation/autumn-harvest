-------------------------- MODULE WorkflowTaskClaim --------------------------
(***************************************************************************)
(* Terminal-write ownership of a workflow task (issues #1184 and #1806).   *)
(*                                                                         *)
(* A decision cycle persists the terminal state of its run only while its  *)
(* claim is current. claim_still_held_for_update is the guard.             *)
(*                                                                         *)
(* ChecksAttempt = TRUE models the guard after #1806:                      *)
(*   state = 'RUNNING' AND worker_id = w AND attempt = a                   *)
(*   AND crash_strikes = s.                                                *)
(* ChecksAttempt = FALSE models the guard before #1806. It has no attempt  *)
(* term. The stuck-running requeue keeps crash_strikes, so the same worker *)
(* can win the row back with the same (worker_id, crash_strikes) pair.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,        \* The worker ids.
    NoWorker,       \* The NULL worker_id. A model value.
    MaxClaims,      \* The bound on claims. It keeps the model finite.
    MaxStrikes,     \* The bound on crash_strikes.
    ChecksAttempt   \* TRUE after #1806. FALSE before #1806.

ASSUME MaxClaims \in Nat /\ MaxStrikes \in Nat /\ ChecksAttempt \in BOOLEAN
ASSUME NoWorker \notin Workers

VARIABLES
    row,       \* The harvest_task_queue row of the workflow task.
    run,       \* The harvest_workflow_executions state: "RUNNING" or "COMPLETED".
    inflight,  \* The decision cycles that are still running.
    events,    \* The terminal events of the run.
    claims     \* The number of claims so far.

vars == <<row, run, inflight, events, claims>>

Ids == [w : Workers \cup {NoWorker}, a : 0..MaxClaims, s : 0..MaxStrikes]
NoClaim == [w |-> NoWorker, a |-> 0, s |-> 0]

Holder ==
    IF row.state = "RUNNING"
    THEN [w |-> row.worker, a |-> row.attempt, s |-> row.strikes]
    ELSE NoClaim

\* claim_still_held_for_update in queue.rs.
Guard(c) ==
    /\ row.state = "RUNNING"
    /\ row.worker = c.w
    /\ row.strikes = c.s
    /\ ChecksAttempt => row.attempt = c.a

TypeOK ==
    /\ row \in [state : {"PENDING", "RUNNING", "COMPLETED"},
                worker : Workers \cup {NoWorker},
                attempt : 0..MaxClaims,
                strikes : 0..MaxStrikes]
    /\ run \in {"RUNNING", "COMPLETED"}
    /\ inflight \subseteq Ids
    /\ events \subseteq [by : Ids, holder : Ids]
    /\ claims \in 0..MaxClaims

Init ==
    /\ row = [state |-> "PENDING", worker |-> NoWorker, attempt |-> 0, strikes |-> 0]
    /\ run = "RUNNING"
    /\ inflight = {}
    /\ events = {}
    /\ claims = 0

-----------------------------------------------------------------------------
(* Actions *)

\* claim_task: add 1 to attempt and keep crash_strikes.
Claim(w) ==
    /\ row.state = "PENDING"
    /\ run = "RUNNING"
    /\ claims < MaxClaims
    /\ row' = [row EXCEPT !.state = "RUNNING", !.worker = w, !.attempt = @ + 1]
    /\ inflight' = inflight \cup {[w |-> w, a |-> row.attempt + 1, s |-> row.strikes]}
    /\ claims' = claims + 1
    /\ UNCHANGED <<run, events>>

\* requeue_stuck_task: the worker can still be alive. It keeps
\* crash_strikes and attempt.
StuckRequeue ==
    /\ row.state = "RUNNING"
    /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker]
    /\ UNCHANGED <<run, inflight, events, claims>>

\* requeue_orphan: the worker's liveness row is stale. It adds a strike.
OrphanReclaim ==
    /\ row.state = "RUNNING"
    /\ row.strikes < MaxStrikes
    /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker, !.strikes = @ + 1]
    /\ UNCHANGED <<run, inflight, events, claims>>

\* release_suspended_workflow_claim: the cycle suspends. It resets
\* crash_strikes.
SuspendRelease(c) ==
    /\ c \in inflight
    /\ IF Guard(c)
       THEN row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker, !.strikes = 0]
       ELSE UNCHANGED row
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<run, events, claims>>

\* The terminal persist: lock the run, check the guard, append the terminal
\* event and close the run and the task in one transaction.
PersistTerminal(c) ==
    /\ c \in inflight
    /\ IF run = "RUNNING" /\ Guard(c)
       THEN /\ events' = events \cup {[by |-> c, holder |-> Holder]}
            /\ run' = "COMPLETED"
            /\ row' = [row EXCEPT !.state = "COMPLETED"]
       ELSE UNCHANGED <<events, run, row>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED claims

Next ==
    \/ \E w \in Workers : Claim(w)
    \/ StuckRequeue
    \/ OrphanReclaim
    \/ \E c \in inflight : SuspendRelease(c) \/ PersistTerminal(c)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety invariants *)

\* At most one decision cycle closes the run.
AtMostOneTerminal == Cardinality(events) <= 1

\* Only the current claim closes the run.
TerminalByCurrentClaim == \A e \in events : e.by = e.holder

=============================================================================
