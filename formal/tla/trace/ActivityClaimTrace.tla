------------------------- MODULE ActivityClaimTrace -------------------------
(***************************************************************************)
(* Trace validation of ActivityClaim (issue #2003).                       *)
(*                                                                         *)
(* Log is one task row's history from a real engine run. Each line is one  *)
(* committed transaction. TraceNext explains a line with one action of     *)
(* ActivityClaim. The post-state of that action must equal the line.       *)
(*                                                                         *)
(* TLC checks the invariant LogNotConsumed. A violation means that a       *)
(* behavior of the spec matches every line, so the trace is accepted. "No  *)
(* error" means that no behavior matches, so the trace is rejected.        *)
(*                                                                         *)
(* Unobserved actions change no logged column. They only drop a claim from *)
(* inflight, so TraceNext does not need them.                              *)
(*                                                                         *)
(* docs/testing/formal-methods.md says how to run it.                      *)
(***************************************************************************)
EXTENDS ActivityClaim, Sequences

CONSTANT Log  \* The trace. scripts/formal_traces.py writes it.

VARIABLE cursor  \* The index of the next line to match.

Line == Log[cursor]

\* A line names its writer when the writer was a known claim of this row.
\* "" means that the writer is unknown, so any writer can explain it.
ActorOK(c) ==
    Line.actor = "" \/ (Line.actor = c.w /\ Line.actorAttempt = c.a)

\* A system action (reclaim, sweeper) has no claim, so it has no actor.
SystemOK == Line.actor = ""

\* The post-state equals the logged columns.
Matches ==
    /\ row'.state = Line.state
    /\ row'.worker = IF Line.worker = "" THEN NoWorker ELSE Line.worker
    /\ row'.attempt = Line.attempt
    /\ Cardinality(events') = Line.terminal

\* A change to state, worker_id, attempt or the terminal events.
WriteStep ==
    \/ \E w \in Workers : Claim(w)
    \/ \E c \in inflight :
        /\ ActorOK(c)
        /\ \/ SelfRelease(c)
           \/ RetryRequeue(c)
           \/ Finish(c, "COMPLETED")
           \/ Finish(c, "FAILED")
    \/ SystemOK /\ (OrphanReclaim \/ TimeoutFail)

\* An ActivityStarted append. The start fence accepted the claim. The
\* event names the worker of the claim in Line.by.
StartStep ==
    \E c \in inflight :
        /\ ActorOK(c)
        /\ Line.by \in {"", c.w}
        /\ Start(c)
        /\ [c EXCEPT !.phase = "started"] \in inflight'

\* A last_heartbeat_at write that changed no other logged column.
HeartbeatStep ==
    \E c \in inflight : ActorOK(c) /\ Heartbeat(c) /\ hbBy' = c.seq

TraceInit ==
    /\ Init
    /\ Log[1].op = "init"
    /\ row.state = Log[1].state
    /\ row.attempt = Log[1].attempt
    /\ Log[1].worker = ""
    /\ Log[1].terminal = 0
    /\ cursor = 2

TraceNext ==
    /\ cursor <= Len(Log)
    /\ CASE Line.op = "write"     -> WriteStep
         [] Line.op = "start"     -> StartStep
         [] Line.op = "heartbeat" -> HeartbeatStep
         [] OTHER                 -> FALSE
    /\ Matches
    /\ cursor' = cursor + 1

\* TLC violates this invariant when it matches the whole log.
LogNotConsumed == cursor <= Len(Log)

=============================================================================
