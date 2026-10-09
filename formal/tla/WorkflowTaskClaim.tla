-------------------------- MODULE WorkflowTaskClaim --------------------------
(***************************************************************************)
(* Terminal-write ownership of a workflow task (issues #1184 and #1806).   *)
(*                                                                         *)
(* A decision cycle persists the terminal state of its run only while its  *)
(* claim is current. claim_still_held_for_update is the guard.             *)
(*                                                                         *)
(* ChecksAttempt = TRUE models the guards after #1806:                     *)
(*   state = 'RUNNING' AND worker_id = w AND attempt = a                   *)
(*   AND crash_strikes = s.                                                *)
(* ChecksAttempt = FALSE models the guards before #1806. They have no      *)
(* attempt term. The stuck-running requeue keeps crash_strikes, so the     *)
(* same worker can win the row back with the same pair.                    *)
(*                                                                         *)
(* CapMissGuard sets the guard of the capability-miss release that a       *)
(* cycle takes after its handler starts (DuringHandler, AfterHandler):     *)
(*   "off"     the action is disabled;                                     *)
(*   "strikes" the guard before #1917: worker_id and crash_strikes only;   *)
(*   "epoch"   the guard after #1917, as claim_still_held_for_update.      *)
(*                                                                         *)
(* Each claim gets a ghost sequence number, seq. The code has no such      *)
(* column. The invariants compare seq, so a reused pair cannot hide a      *)
(* stale write.                                                            *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Workers,        \* The worker ids.
    NoWorker,       \* The NULL worker_id. A model value.
    MaxClaims,      \* The bound on claims. It keeps the model finite.
    MaxStrikes,     \* The bound on crash_strikes.
    ChecksAttempt,  \* TRUE after #1806. FALSE before #1806.
    CapMissGuard    \* "off", "strikes" or "epoch".

ASSUME MaxClaims \in Nat /\ MaxStrikes \in Nat /\ ChecksAttempt \in BOOLEAN
ASSUME NoWorker \notin Workers
ASSUME CapMissGuard \in {"off", "strikes", "epoch"}

VARIABLES
    row,       \* The harvest_task_queue row of the workflow task.
    run,       \* The harvest_workflow_executions state: "RUNNING" or "COMPLETED".
    inflight,  \* The decision cycles that are still running.
    events,    \* The terminal events of the run: [by |-> seq, holder |-> seq].
    writes,    \* Every owner write that took effect: [by |-> seq, holder |-> seq].
    claims     \* The number of claims so far. The last claim has seq = claims.

vars == <<row, run, inflight, events, writes, claims>>

Seqs == 0..MaxClaims
Claims == [w : Workers, a : 0..MaxClaims, s : 0..MaxStrikes, seq : Seqs]

\* The seq of the claim that the row holds now, or 0.
Holder == IF row.state = "RUNNING" THEN row.seq ELSE 0

\* The guard without its attempt term.
StrikesGuard(c) ==
    /\ row.state = "RUNNING"
    /\ row.worker = c.w
    /\ row.strikes = c.s

\* The full claim-epoch guard.
EpochGuard(c) == StrikesGuard(c) /\ row.attempt = c.a

\* claim_still_held_for_update and release_suspended_workflow_claim.
Guard(c) == IF ChecksAttempt THEN EpochGuard(c) ELSE StrikesGuard(c)

\* Record an owner write by claim c.
Wrote(c) == writes' = writes \cup {[by |-> c.seq, holder |-> Holder]}

\* Put the row back to PENDING with no worker.
Repend(strikes) ==
    row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker, !.seq = 0,
                       !.strikes = strikes]

TypeOK ==
    /\ row \in [state : {"PENDING", "RUNNING", "COMPLETED"},
                worker : Workers \cup {NoWorker},
                attempt : 0..MaxClaims,
                strikes : 0..MaxStrikes,
                seq : Seqs]
    /\ run \in {"RUNNING", "COMPLETED"}
    /\ inflight \subseteq Claims
    /\ events \subseteq [by : Seqs, holder : Seqs]
    /\ writes \subseteq [by : Seqs, holder : Seqs]
    /\ claims \in 0..MaxClaims

Init ==
    /\ row = [state |-> "PENDING", worker |-> NoWorker, attempt |-> 0,
              strikes |-> 0, seq |-> 0]
    /\ run = "RUNNING"
    /\ inflight = {}
    /\ events = {}
    /\ writes = {}
    /\ claims = 0

-----------------------------------------------------------------------------
(* Actions *)

\* claim_task: add 1 to attempt and keep crash_strikes.
Claim(w) ==
    /\ row.state = "PENDING"
    /\ run = "RUNNING"
    /\ claims < MaxClaims
    /\ row' = [row EXCEPT !.state = "RUNNING", !.worker = w, !.attempt = @ + 1,
                          !.seq = claims + 1]
    /\ inflight' = inflight \cup
         {[w |-> w, a |-> row.attempt + 1, s |-> row.strikes, seq |-> claims + 1]}
    /\ claims' = claims + 1
    /\ UNCHANGED <<run, events, writes>>

\* requeue_stuck_task: the worker can still be alive. It keeps
\* crash_strikes and attempt.
StuckRequeue ==
    /\ row.state = "RUNNING"
    /\ Repend(row.strikes)
    /\ UNCHANGED <<run, inflight, events, writes, claims>>

\* requeue_orphan: the worker's liveness row is stale. It adds a strike.
OrphanReclaim ==
    /\ row.state = "RUNNING"
    /\ row.strikes < MaxStrikes
    /\ Repend(row.strikes + 1)
    /\ UNCHANGED <<run, inflight, events, writes, claims>>

\* release_suspended_workflow_claim: the cycle suspends. It resets
\* crash_strikes.
SuspendRelease(c) ==
    /\ c \in inflight
    /\ IF Guard(c)
       THEN Wrote(c) /\ Repend(0)
       ELSE UNCHANGED <<row, writes>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<run, events, claims>>

\* release_task_for_capability_miss_query after the handler starts.
\* The AfterHandler arm resets crash_strikes. The model keeps them, which
\* gives a stale cycle more chances to match.
CapMissRelease(c) ==
    /\ CapMissGuard /= "off"
    /\ c \in inflight
    /\ IF IF CapMissGuard = "epoch" THEN EpochGuard(c) ELSE StrikesGuard(c)
       THEN Wrote(c) /\ Repend(row.strikes)
       ELSE UNCHANGED <<row, writes>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<run, events, claims>>

\* release_unstarted_claim (issue #1813): a draining worker gives back a
\* claim that never started. Its fence is claim_held, with no strikes term.
\* The release came after #1806, so its guard checks attempt in every config.
\* It subtracts 1 from attempt and keeps crash_strikes. The trace check of
\* issue #2003 found the gap: a chaos trace of #1813 matched no action.
\* The action drops the claim from inflight. That is an assumption: no
\* handler ran, so the claim writes nothing more. A later claim can reuse
\* (worker_id, attempt, crash_strikes), so a stale write after the release
\* would break OwnerWritesByCurrentClaim. The drain calls the release only
\* for a dispatch that never started.
UnstartedRelease(c) ==
    /\ c \in inflight
    /\ IF /\ row.state = "RUNNING"
          /\ row.worker = c.w
          /\ row.attempt = c.a
       THEN /\ Wrote(c)
            /\ row' = [row EXCEPT !.state = "PENDING", !.worker = NoWorker,
                                  !.seq = 0, !.attempt = @ - 1]
       ELSE UNCHANGED <<row, writes>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED <<run, events, claims>>

\* The terminal persist: lock the run, check the guard, append the terminal
\* event and close the run and the task in one transaction.
PersistTerminal(c) ==
    /\ c \in inflight
    /\ IF run = "RUNNING" /\ Guard(c)
       THEN /\ events' = events \cup {[by |-> c.seq, holder |-> Holder]}
            /\ Wrote(c)
            /\ run' = "COMPLETED"
            /\ row' = [row EXCEPT !.state = "COMPLETED"]
       ELSE UNCHANGED <<events, writes, run, row>>
    /\ inflight' = inflight \ {c}
    /\ UNCHANGED claims

Next ==
    \/ \E w \in Workers : Claim(w)
    \/ StuckRequeue
    \/ OrphanReclaim
    \/ \E c \in inflight :
        \/ SuspendRelease(c)
        \/ CapMissRelease(c)
        \/ UnstartedRelease(c)
        \/ PersistTerminal(c)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety invariants *)

\* At most one decision cycle closes the run.
AtMostOneTerminal == Cardinality(events) <= 1

\* Only the current claim closes the run.
TerminalByCurrentClaim == \A e \in events : e.by = e.holder

\* Every owner write takes effect only while its claim is current.
OwnerWritesByCurrentClaim == \A x \in writes : x.by = x.holder

-----------------------------------------------------------------------------
(* Reachability witness. WorkflowTaskClaimReach.cfg expects TLC to violate *)
(* it. The violation proves that the fixed model reaches the race of #1806: *)
(* a stale cycle still runs while the same worker holds a later claim.     *)

NoStaleCycleOfTheHolder ==
    ~ \E c \in inflight :
        /\ row.state = "RUNNING"
        /\ c.w = row.worker
        /\ c.s = row.strikes
        /\ c.seq /= row.seq

=============================================================================
