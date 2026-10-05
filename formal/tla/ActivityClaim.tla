---------------------------- MODULE ActivityClaim ----------------------------
(***************************************************************************)
(* The activity claim protocol of issue #1789: claim, start, heartbeat,    *)
(* orphan reclaim, re-claim and the terminal write, on one task row.       *)
(*                                                                         *)
(* docs/architecture.md, design decision 9, is the prose spec.             *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(*                                                                         *)
(* Fenced = TRUE models the code after #1789. Every owner write checks     *)
(* claim_held: state = 'RUNNING' AND worker_id = w AND attempt = a.        *)
(* Fenced = FALSE models the code before #1789. Owner writes check only    *)
(* state = 'RUNNING'. TLC then finds the stale-owner counter-example.      *)
(*                                                                         *)
(* Each claim gets a ghost sequence number, seq. The code has no such      *)
(* column. The invariants compare seq, not (worker_id, attempt), so a      *)
(* reused pair cannot hide a stale write.                                  *)
(*                                                                         *)
(* SelfRelease uses the full claim_held guard. The pause releases run in   *)
(* the claim's own transaction. The capability-miss release runs only on   *)
(* a worker without the handler, which never starts the activity. Both     *)
(* facts make the stronger guard equivalent for this model.                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,    \* The worker ids.
    NoWorker,   \* The NULL worker_id. A model value.
    MaxClaims,  \* The bound on claims of the row. It keeps the model finite.
    Fenced      \* TRUE after #1789. FALSE before #1789.

ASSUME MaxClaims \in Nat /\ Fenced \in BOOLEAN
ASSUME NoWorker \notin Workers

VARIABLES
    row,       \* The harvest_task_queue row.
    inflight,  \* The claims that worker processes still act on.
    events,    \* The terminal events in harvest_events for this task.
    hbBy,      \* The seq of the claim that wrote last_heartbeat_at, or 0.
    writes,    \* Every owner write that took effect: [by |-> seq, holder |-> seq].
    claims     \* The number of claims so far. The last claim has seq = claims.

vars == <<row, inflight, events, hbBy, writes, claims>>

States == {"PENDING", "RUNNING", "COMPLETED", "FAILED"}
Attempts == 0..MaxClaims
Seqs == 0..MaxClaims
Claims == [w : Workers, a : Attempts, seq : Seqs, phase : {"claimed", "started"}]
Sweeper == 0  \* The seq of a write by the timeout sweeper, which is not an owner.

\* The seq of the claim that the row holds now, or 0.
Holder == IF row.state = "RUNNING" THEN row.seq ELSE 0

\* Record an owner write by claim c.
Wrote(c) == writes' = writes \cup {[by |-> c.seq, holder |-> Holder]}

\* claim_held in queue.rs.
Held(c) ==
    /\ row.state = "RUNNING"
    /\ row.worker = c.w
    /\ row.attempt = c.a

\* The predicate that an owner write puts in its WHERE clause.
Accepts(c) == IF Fenced THEN Held(c) ELSE row.state = "RUNNING"

TypeOK ==
    /\ row \in [state : States, worker : Workers \cup {NoWorker},
                attempt : Attempts, seq : Seqs]
    /\ inflight \subseteq Claims
    /\ events \subseteq [by : Seqs, holder : Seqs]
    /\ hbBy \in Seqs
    /\ writes \subseteq [by : Seqs, holder : Seqs]
    /\ claims \in 0..MaxClaims

Init ==
    /\ row = [state |-> "PENDING", worker |-> NoWorker, attempt |-> 0, seq |-> 0]
    /\ inflight = {}
    /\ events = {}
    /\ hbBy = 0
    /\ writes = {}
    /\ claims = 0

-----------------------------------------------------------------------------
(* Actions *)

\* claim_task: set RUNNING and worker_id, and add 1 to attempt.
Claim(w) ==
    /\ row.state = "PENDING"
    /\ claims < MaxClaims
    /\ row' = [state |-> "RUNNING", worker |-> w, attempt |-> row.attempt + 1,
               seq |-> claims + 1]
    /\ inflight' = inflight \cup
         {[w |-> w, a |-> row.attempt + 1, seq |-> claims + 1, phase |-> "claimed"]}
    /\ claims' = claims + 1
    /\ UNCHANGED <<events, hbBy, writes>>

\* The start fence. A lost claim appends no event and stops.
Start(c) ==
    /\ c \in inflight
    /\ c.phase = "claimed"
    /\ IF Accepts(c)
       THEN inflight' = (inflight \ {c}) \cup {[c EXCEPT !.phase = "started"]}
       ELSE inflight' = inflight \ {c}
    /\ UNCHANGED <<row, events, hbBy, writes, claims>>

\* A pause release, capability-miss release, rate-limit deferral or
\* retry-budget deferral. It runs before the handler starts and
\* subtracts 1 from attempt.
SelfRelease(c) ==
    /\ c \in inflight
    /\ c.phase = "claimed"
    /\ Accepts(c)
    /\ Wrote(c)
    /\ row' = [state |-> "PENDING", worker |-> NoWorker, attempt |-> row.attempt - 1,
               seq |-> 0]
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<events, hbBy, claims>>

\* The heartbeat flusher. After #1789 a lost lease cancels the activity.
\* Before #1789 a NotFound result was only logged.
Heartbeat(c) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ hbBy' = c.seq
            /\ Wrote(c)
            /\ UNCHANGED inflight
       ELSE /\ inflight' = IF Fenced THEN inflight \ {c} ELSE inflight
            /\ UNCHANGED <<hbBy, writes>>
    /\ UNCHANGED <<row, events, claims>>

\* requeue_orphan: the claiming worker's liveness row is stale. The worker
\* can still be alive, so this action has no guard on the worker.
\* It keeps attempt.
OrphanReclaim ==
    /\ row.state = "RUNNING"
    /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker, !.seq = 0]
    /\ hbBy' = 0
    /\ UNCHANGED <<inflight, events, writes, claims>>

\* requeue_claimed_task_for_retry: the attempt failed and can retry.
RetryRequeue(c) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ Wrote(c)
            /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker, !.seq = 0]
            /\ hbBy' = 0
       ELSE UNCHANGED <<row, hbBy, writes>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<events, claims>>

\* The finalize path: lock the claim, append the terminal event and write
\* the terminal row state in one transaction. A lost claim writes nothing.
Finish(c, outcome) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ events' = events \cup {[by |-> c.seq, holder |-> Holder]}
            /\ Wrote(c)
            /\ row' = [row EXCEPT !.state = outcome]
       ELSE UNCHANGED <<events, row, writes>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<hbBy, claims>>

\* The timeout sweeper. It is not an owner, so it is not fenced (by design).
TimeoutFail ==
    /\ row.state \in {"PENDING", "RUNNING"}
    /\ events' = events \cup {[by |-> Sweeper, holder |-> Holder]}
    /\ row' = [row EXCEPT !.state = "FAILED"]
    /\ UNCHANGED <<inflight, hbBy, writes, claims>>

Next ==
    \/ \E w \in Workers : Claim(w)
    \/ \E c \in inflight :
        \/ Start(c)
        \/ SelfRelease(c)
        \/ Heartbeat(c)
        \/ RetryRequeue(c)
        \/ Finish(c, "COMPLETED")
        \/ Finish(c, "FAILED")
    \/ OrphanReclaim
    \/ TimeoutFail

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety invariants *)

\* At most one terminal event takes effect.
AtMostOneTerminal == Cardinality(events) <= 1

\* A terminal row state has exactly one terminal event.
TerminalStateHasOneEvent ==
    row.state \in {"COMPLETED", "FAILED"} => Cardinality(events) = 1

\* An owner's terminal event takes effect only while its claim is current.
TerminalByCurrentClaim ==
    \A e \in events : e.by = Sweeper \/ e.by = e.holder

\* Every owner write takes effect only while its claim is current.
OwnerWritesByCurrentClaim == \A x \in writes : x.by = x.holder

\* A heartbeat on a running row comes from the claim that the row holds.
HeartbeatByCurrentClaim ==
    (row.state = "RUNNING" /\ hbBy /= 0) => hbBy = Holder

\* (worker_id, attempt) is a fencing token: no two live claims share it.
ClaimIdsAreUnique ==
    \A c, d \in inflight : (c.w = d.w /\ c.a = d.a) => c.seq = d.seq

-----------------------------------------------------------------------------
(* Reachability witness. ActivityClaimReach.cfg expects TLC to violate it. *)
(* The violation proves that the fenced model reaches the race of #1789:   *)
(* a stale owner still runs after a later claim finished the task.         *)

NoStaleOwnerAfterFinish ==
    ~ \E c \in inflight :
        /\ c.phase = "started"
        /\ row.state = "COMPLETED"
        /\ \E e \in events : e.by /= c.seq

=============================================================================
