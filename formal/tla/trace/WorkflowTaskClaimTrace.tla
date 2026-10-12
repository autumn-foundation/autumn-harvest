----------------------- MODULE WorkflowTaskClaimTrace -----------------------
(***************************************************************************)
(* Trace validation of WorkflowTaskClaim (issue #2003).                   *)
(*                                                                         *)
(* Log is one workflow task row's history from a real engine run. Each     *)
(* line is one committed transaction. TraceNext explains a line with one   *)
(* action of WorkflowTaskClaim. The post-state of that action must equal   *)
(* the line.                                                               *)
(*                                                                         *)
(* TLC checks the invariant LogNotConsumed. A violation means that a       *)
(* behavior of the spec matches every line, so the trace is accepted. "No  *)
(* error" means that no behavior matches, so the trace is rejected.        *)
(*                                                                         *)
(* A parked row is RUNNING with no worker_id. The exporter logs it as      *)
(* PENDING, because no claim holds it.                                     *)
(*                                                                         *)
(* docs/testing/formal-methods.md says how to run it.                      *)
(***************************************************************************)
EXTENDS WorkflowTaskClaim, Sequences

CONSTANT Log  \* The trace. scripts/formal_traces.py writes it.

VARIABLE cursor  \* The index of the next line to match.

Line == Log[cursor]

\* A line names its writer when the writer was a known claim of this row.
\* "" means that the writer is unknown, so any writer can explain it.
ActorOK(c) ==
    Line.actor = "" \/ (Line.actor = c.w /\ Line.actorAttempt = c.a)

\* A system action (requeue, reclaim) has no claim, so it has no actor.
SystemOK == Line.actor = ""

\* The post-state equals the logged columns.
Matches ==
    /\ row'.state = Line.state
    /\ row'.worker = IF Line.worker = "" THEN NoWorker ELSE Line.worker
    /\ row'.attempt = Line.attempt
    /\ row'.strikes = Line.strikes
    /\ Cardinality(events') = Line.terminal

\* A change to state, worker_id, attempt, crash_strikes or the terminal
\* events of the run.
WriteStep ==
    \/ \E w \in Workers : Claim(w)
    \/ \E c \in inflight :
        /\ ActorOK(c)
        /\ \/ SuspendRelease(c)
           \/ CapMissRelease(c)
           \/ UnstartedRelease(c)
           \/ PersistTerminal(c)
    \/ SystemOK /\ (StuckRequeue \/ OrphanReclaim)

TraceInit ==
    /\ Init
    /\ Log[1].op = "init"
    /\ row.state = Log[1].state
    /\ row.attempt = Log[1].attempt
    /\ row.strikes = Log[1].strikes
    /\ Log[1].worker = ""
    /\ Log[1].terminal = 0
    /\ cursor = 2

TraceNext ==
    /\ cursor <= Len(Log)
    /\ IF Line.op = "write" THEN WriteStep ELSE FALSE
    /\ Matches
    /\ cursor' = cursor + 1

\* TLC violates this invariant when it matches the whole log.
LogNotConsumed == cursor <= Len(Log)

=============================================================================
