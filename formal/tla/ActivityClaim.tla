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
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,    \* The worker ids.
    NoWorker,   \* The NULL worker_id. A model value.
    Sweeper,    \* The timeout sweeper, which is not an owner. A model value.
    MaxClaims,  \* The bound on claims of the row. It keeps the model finite.
    Fenced      \* TRUE after #1789. FALSE before #1789.

ASSUME MaxClaims \in Nat /\ Fenced \in BOOLEAN
ASSUME NoWorker \notin Workers /\ Sweeper \notin Workers

VARIABLES
    row,       \* The harvest_task_queue row.
    inflight,  \* The claims that worker processes still act on.
    events,    \* The terminal events in harvest_events for this task.
    hbBy,      \* The claim that wrote last_heartbeat_at and heartbeat_details.
    claims     \* The number of claims so far.

vars == <<row, inflight, events, hbBy, claims>>

States == {"PENDING", "RUNNING", "COMPLETED", "FAILED"}
Attempts == 0..MaxClaims
Ids == [w : Workers \cup {NoWorker}, a : Attempts]
Claims == [w : Workers, a : Attempts, phase : {"claimed", "started"}]
NoClaim == [w |-> NoWorker, a |-> 0]

\* The (worker_id, attempt) pair of a claim.
Id(c) == [w |-> c.w, a |-> c.a]

\* The claim that the row holds now, or NoClaim.
Holder ==
    IF row.state = "RUNNING"
    THEN [w |-> row.worker, a |-> row.attempt]
    ELSE NoClaim

\* claim_held in queue.rs.
Held(c) ==
    /\ row.state = "RUNNING"
    /\ row.worker = c.w
    /\ row.attempt = c.a

\* The predicate that an owner write puts in its WHERE clause.
Accepts(c) == IF Fenced THEN Held(c) ELSE row.state = "RUNNING"

TypeOK ==
    /\ row \in [state : States, worker : Workers \cup {NoWorker}, attempt : Attempts]
    /\ inflight \subseteq Claims
    /\ events \subseteq [by : Ids \cup [w : {Sweeper}, a : Attempts], holder : Ids]
    /\ hbBy \in Ids
    /\ claims \in 0..MaxClaims

Init ==
    /\ row = [state |-> "PENDING", worker |-> NoWorker, attempt |-> 0]
    /\ inflight = {}
    /\ events = {}
    /\ hbBy = NoClaim
    /\ claims = 0

-----------------------------------------------------------------------------
(* Actions *)

\* claim_task: set RUNNING and worker_id, and add 1 to attempt.
Claim(w) ==
    /\ row.state = "PENDING"
    /\ claims < MaxClaims
    /\ row' = [state |-> "RUNNING", worker |-> w, attempt |-> row.attempt + 1]
    /\ inflight' = inflight \cup {[w |-> w, a |-> row.attempt + 1, phase |-> "claimed"]}
    /\ claims' = claims + 1
    /\ UNCHANGED <<events, hbBy>>

\* The start fence. A lost claim appends no event and stops.
Start(c) ==
    /\ c \in inflight
    /\ c.phase = "claimed"
    /\ IF Accepts(c)
       THEN inflight' = (inflight \ {c}) \cup {[c EXCEPT !.phase = "started"]}
       ELSE inflight' = inflight \ {c}
    /\ UNCHANGED <<row, events, hbBy, claims>>

\* A pause release, capability-miss release or rate-limit deferral.
\* It runs before the handler starts and subtracts 1 from attempt.
SelfRelease(c) ==
    /\ c \in inflight
    /\ c.phase = "claimed"
    /\ Accepts(c)
    /\ row' = [state |-> "PENDING", worker |-> NoWorker, attempt |-> row.attempt - 1]
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<events, hbBy, claims>>

\* The heartbeat flusher. After #1789 a lost lease cancels the activity.
\* Before #1789 a NotFound result was only logged.
Heartbeat(c) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ hbBy' = Id(c)
            /\ UNCHANGED inflight
       ELSE /\ inflight' = IF Fenced THEN inflight \ {c} ELSE inflight
            /\ UNCHANGED hbBy
    /\ UNCHANGED <<row, events, claims>>

\* requeue_orphan: the claiming worker's liveness row is stale. The worker
\* can still be alive, so this action has no guard on the worker.
\* It keeps attempt.
OrphanReclaim ==
    /\ row.state = "RUNNING"
    /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker]
    /\ hbBy' = NoClaim
    /\ UNCHANGED <<inflight, events, claims>>

\* requeue_claimed_task_for_retry: the attempt failed and can retry.
RetryRequeue(c) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker]
            /\ hbBy' = NoClaim
       ELSE UNCHANGED <<row, hbBy>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<events, claims>>

\* The finalize path: lock the claim, append the terminal event and write
\* the terminal row state in one transaction. A lost claim writes nothing.
Finish(c, outcome) ==
    /\ c \in inflight
    /\ c.phase = "started"
    /\ IF Accepts(c)
       THEN /\ events' = events \cup {[by |-> Id(c), holder |-> Holder]}
            /\ row' = [row EXCEPT !.state = outcome]
       ELSE UNCHANGED <<events, row>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<hbBy, claims>>

\* The timeout sweeper. It is not an owner, so it is not fenced (by design).
TimeoutFail ==
    /\ row.state \in {"PENDING", "RUNNING"}
    /\ events' = events \cup {[by |-> [w |-> Sweeper, a |-> row.attempt], holder |-> Holder]}
    /\ row' = [row EXCEPT !.state = "FAILED"]
    /\ UNCHANGED <<inflight, hbBy, claims>>

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
    \A e \in events : e.by.w = Sweeper \/ e.by = e.holder

\* A heartbeat on a running row comes from the claim that the row holds.
HeartbeatByCurrentClaim ==
    (row.state = "RUNNING" /\ hbBy /= NoClaim) => hbBy = Holder

\* (worker_id, attempt) is a fencing token: no two live claims share it.
ClaimIdsAreUnique ==
    \A c, d \in inflight : Id(c) = Id(d) => c = d

-----------------------------------------------------------------------------
(* Reachability witness. ActivityClaimReach.cfg expects TLC to violate it. *)
(* The violation proves that the fenced model reaches the race of #1789:   *)
(* a stale owner still runs after a later claim finished the task.         *)

NoStaleOwnerAfterFinish ==
    ~ \E c \in inflight :
        /\ c.phase = "started"
        /\ row.state = "COMPLETED"
        /\ \E e \in events : e.by /= Id(c)

=============================================================================
