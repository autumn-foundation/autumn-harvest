----------------------------- MODULE OwnerLease -----------------------------
(***************************************************************************)
(* The fenced owner lease of one decision partition (issue #2250).         *)
(*                                                                         *)
(* docs/adr/0009-fenced-owner-lease.md is the prose spec.                  *)
(* docs/testing/formal-methods.md says how to run this model.              *)
(*                                                                         *)
(* The owner row holds a holder, an epoch and a lease. A worker acquires   *)
(* the row, loads its resident state and runs decisions. Each decision     *)
(* consumes one signal and appends one event to the history of one         *)
(* execution. The commit has two steps: Lock reads the owner row, and      *)
(* Commit writes the event.                                                *)
(*                                                                         *)
(* Defect = "none" is the design. Each other value breaks one rule:        *)
(*   no-fence        The commit does not check the owner row.              *)
(*                   This is golem#3766.                                   *)
(*   unlocked-fence  The check reads the row with no lock.                 *)
(*   holder-fence    The check compares the holder, not the epoch.         *)
(*   keep-cache      An acquisition keeps the old resident state.          *)
(*   pipeline        A tenure begins a decision before its last commits.   *)
(*   renew-bumps     A renewal adds 1 to the epoch.                        *)
(*   memory-ack      The owner acknowledges a signal from memory.          *)
(*                   This is restate#4566.                                 *)
(*                                                                         *)
(* The four nemeses of the issue are always on. Expire is a pause or a     *)
(* clock jump: the lease ends while its holder still runs. An unloaded     *)
(* worker keeps its decisions in flight, as a paused thread does. A new    *)
(* tenure of the same worker can begin a decision beside them.             *)
(*                                                                         *)
(* The model has no event-id backstop. The fence alone keeps the           *)
(* invariants.                                                             *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,   \* The worker ids.
    NoWorker,  \* The NULL holder. A model value.
    Signals,   \* The signal ids that clients send.
    NoSignal,  \* The NULL signal. A model value.
    MaxEpoch,  \* The bound on the epoch. It keeps the model finite.
    Defect     \* "none", or the rule that the model breaks.

Defects == {"none", "no-fence", "unlocked-fence", "holder-fence",
            "keep-cache", "pipeline", "renew-bumps", "memory-ack"}

ASSUME Defect \in Defects
ASSUME MaxEpoch \in Nat
ASSUME NoWorker \notin Workers /\ NoSignal \notin Signals

VARIABLES
    lease,    \* The owner row: holder, epoch, live and a ghost tenure.
    owners,   \* Ghost: owners[e] is the worker that took epoch e.
    pinned,   \* pinned[w]: the epoch that w thinks it holds, or 0.
    held,     \* held[w]: the ghost tenure of w's last acquisition.
    cache,    \* cache[w]: the history length in w's resident state.
    decs,     \* The decisions in flight, of every worker.
    history,  \* The decision events of the execution.
    inbox,    \* The durable signal rows that no decision consumed.
    written,  \* The signals that have a durable write.
    mem,      \* mem[w]: the signals that w holds only in memory.
    acked,    \* The signals that a client saw acknowledged.
    seen      \* Ghost: the nemesis tags that the behaviour reached.

vars == <<lease, owners, pinned, held, cache, decs, history, inbox, written,
          mem, acked, seen>>

Epochs == 0..MaxEpoch
Decisions == [by : Workers, phase : {"read", "locked"}, epoch : Epochs,
              tenure : Epochs, base : Nat, sig : Signals]
Events == [by : Workers, pinned : Epochs, at : Epochs, base : Nat,
           sig : Signals]

\* The nemesis tags. WitnessTags are the four nemeses, and self-fence is
\* a defect.
WitnessTags == {"stale-commit", "flap", "append-after-expiry",
                "renew-in-flight", "ack-before-bump"}
Tags == WitnessTags \cup {"self-fence"}

\* The signals that a decision event consumed.
Consumed == {history[i].sig : i \in DOMAIN history}

\* A takeover finds the row free or its lease ended.
Takeable == lease.holder = NoWorker \/ ~lease.live

\* The decisions that hold the owner row lock. The no-fence and
\* unlocked-fence defects take no lock.
Locked == IF Defect \in {"no-fence", "unlocked-fence"} THEN {}
          ELSE {d \in decs : d.phase = "locked"}

\* A takeover takes the row FOR UPDATE, so it waits for every fence lock.
NoLocks == Locked = {}

TypeOK ==
    /\ lease \in [holder : Workers \cup {NoWorker}, epoch : Epochs,
                  live : BOOLEAN, tenure : Epochs]
    /\ owners \in [Epochs -> Workers \cup {NoWorker}]
    /\ pinned \in [Workers -> Epochs]
    /\ held \in [Workers -> Epochs]
    /\ cache \in [Workers -> Nat]
    /\ decs \subseteq Decisions
    /\ history \in Seq(Events)
    /\ inbox \subseteq Signals
    /\ written \subseteq Signals
    /\ mem \in [Workers -> SUBSET Signals]
    /\ acked \subseteq Signals
    /\ seen \subseteq Tags

Init ==
    /\ lease = [holder |-> NoWorker, epoch |-> 0, live |-> FALSE, tenure |-> 0]
    /\ owners = [e \in Epochs |-> NoWorker]
    /\ pinned = [w \in Workers |-> 0]
    /\ held = [w \in Workers |-> 0]
    /\ cache = [w \in Workers |-> 0]
    /\ decs = {}
    /\ history = <<>>
    /\ inbox = {}
    /\ written = {}
    /\ mem = [w \in Workers |-> {}]
    /\ acked = {}
    /\ seen = {}

-----------------------------------------------------------------------------
(* Actions *)

\* w drops its ownership. Its decisions in flight stay.
Unload(w) ==
    /\ pinned' = [pinned EXCEPT ![w] = 0]
    /\ mem' = [mem EXCEPT ![w] = {}]
    /\ cache' = IF Defect = "keep-cache" THEN cache
                ELSE [cache EXCEPT ![w] = 0]

\* The takeover: SELECT ... FOR UPDATE on the owner row, then the write.
\* Every acquisition adds 1 to the epoch. The owner loads its resident
\* state after the acquisition commits.
Acquire(w) ==
    /\ pinned[w] = 0
    /\ Takeable
    /\ NoLocks
    /\ lease.epoch < MaxEpoch
    /\ lease' = [holder |-> w, epoch |-> lease.epoch + 1, live |-> TRUE,
                 tenure |-> lease.tenure + 1]
    /\ owners' = [owners EXCEPT ![lease.epoch + 1] = w]
    /\ pinned' = [pinned EXCEPT ![w] = lease.epoch + 1]
    /\ held' = [held EXCEPT ![w] = lease.tenure + 1]
    /\ cache' = IF Defect = "keep-cache" THEN cache
                ELSE [cache EXCEPT ![w] = Len(history)]
    /\ seen' = IF acked \cap inbox # {} THEN seen \cup {"ack-before-bump"}
               ELSE seen
    /\ UNCHANGED <<decs, history, inbox, written, mem, acked>>

\* A graceful release: the holder frees the row and unloads.
Release(w) ==
    /\ pinned[w] # 0
    /\ lease.holder = w
    /\ lease.epoch = pinned[w]
    /\ lease' = [lease EXCEPT !.holder = NoWorker, !.live = FALSE]
    /\ Unload(w)
    /\ UNCHANGED <<owners, held, decs, history, inbox, written, acked, seen>>

\* The lease ends while its holder still runs: a pause or a clock jump.
\* A lock does not stop the clock.
Expire ==
    /\ lease.holder # NoWorker
    /\ lease.live
    /\ lease' = [lease EXCEPT !.live = FALSE]
    /\ UNCHANGED <<owners, pinned, held, cache, decs, history, inbox, written,
                   mem, acked, seen>>

\* The renewal UPDATE. It matches the holder and the pinned epoch. It does
\* not wait for a fence lock: the fence takes FOR KEY SHARE, and the
\* renewal changes no key column. A failed renewal unloads the worker.
Renew(w) ==
    /\ pinned[w] # 0
    /\ IF lease.holder = w /\ lease.epoch = pinned[w]
       THEN /\ IF Defect = "renew-bumps"
               THEN /\ lease.epoch < MaxEpoch
                    /\ NoLocks
                    /\ lease' = [lease EXCEPT !.epoch = @ + 1, !.live = TRUE]
                    /\ owners' = [owners EXCEPT ![lease.epoch + 1] = w]
                    /\ pinned' = [pinned EXCEPT ![w] = lease.epoch + 1]
               ELSE /\ lease' = [lease EXCEPT !.live = TRUE]
                    /\ UNCHANGED <<owners, pinned>>
            /\ seen' = IF \E d \in decs : d.by = w /\ d.phase = "locked"
                       THEN seen \cup {"renew-in-flight"} ELSE seen
            /\ UNCHANGED <<cache, mem>>
       ELSE /\ Unload(w)
            /\ UNCHANGED <<lease, owners, seen>>
    /\ UNCHANGED <<held, decs, history, inbox, written, acked>>

\* A client signal. The server writes the harvest_signals row first.
Write(s) ==
    /\ Defect # "memory-ack"
    /\ s \notin written
    /\ written' = written \cup {s}
    /\ inbox' = inbox \cup {s}
    /\ UNCHANGED <<lease, owners, pinned, held, cache, decs, history, mem,
                   acked, seen>>

\* The server acknowledges a signal after its durable write.
Ack(s) ==
    /\ s \in written \ acked
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<lease, owners, pinned, held, cache, decs, history, inbox,
                   written, mem, seen>>

\* memory-ack: the owner keeps the signal in memory and acknowledges it.
\* The durable write comes later, in the decision commit.
Receive(w, s) ==
    /\ Defect = "memory-ack"
    /\ pinned[w] # 0
    /\ s \notin written \cup acked
    /\ mem' = [mem EXCEPT ![w] = @ \cup {s}]
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<lease, owners, pinned, held, cache, decs, history, inbox,
                   written, seen>>

\* A decision reads its resident state and picks a pending signal. It pins
\* the epoch that the owner holds now. A tenure runs one decision at a
\* time. The pipeline defect lets it begin a second one.
Begin(w) ==
    /\ pinned[w] # 0
    /\ Cardinality({d \in decs : d.by = w /\ d.tenure = held[w]})
         < (IF Defect = "pipeline" THEN 2 ELSE 1)
    /\ \E s \in inbox \cup mem[w] :
         decs' = decs \cup {[by |-> w, phase |-> "read", epoch |-> pinned[w],
                             tenure |-> held[w], base |-> cache[w],
                             sig |-> s]}
    /\ UNCHANGED <<lease, owners, pinned, held, cache, history, inbox,
                   written, mem, acked, seen>>

\* The fence predicate of the commit.
Passes(d) ==
    CASE Defect = "no-fence"     -> TRUE
      [] Defect = "holder-fence" -> lease.holder = d.by
      [] OTHER                   -> /\ lease.holder = d.by
                                    /\ lease.epoch = d.epoch

\* The nemesis tag of a fenced commit.
FenceTag(d) ==
    IF lease.holder # d.by THEN "stale-commit"
    ELSE IF d.tenure = lease.tenure THEN "self-fence"
    ELSE "flap"

\* The commit reads the owner row FOR KEY SHARE and checks the fence. A
\* fenced decision appends nothing. It unloads the worker only when the
\* worker still pins the fenced epoch.
Lock(d) ==
    /\ d.phase = "read"
    /\ IF Passes(d)
       THEN /\ decs' = (decs \ {d}) \cup {[d EXCEPT !.phase = "locked"]}
            /\ UNCHANGED <<pinned, cache, mem, seen>>
       ELSE /\ decs' = decs \ {d}
            /\ seen' = seen \cup {FenceTag(d)}
            /\ IF d.epoch = pinned[d.by] THEN Unload(d.by)
               ELSE UNCHANGED <<pinned, cache, mem>>
    /\ UNCHANGED <<lease, owners, held, history, inbox, written, acked>>

\* The commit appends the event, consumes the signal and releases the lock.
\* The owner applies its own event to its resident state.
Commit(d) ==
    /\ d.phase = "locked"
    /\ history' = Append(history, [by |-> d.by, pinned |-> d.epoch,
                                   at |-> lease.epoch, base |-> d.base,
                                   sig |-> d.sig])
    /\ inbox' = inbox \ {d.sig}
    /\ written' = written \cup {d.sig}
    /\ mem' = [mem EXCEPT ![d.by] = @ \ {d.sig}]
    /\ cache' = [cache EXCEPT ![d.by] = Len(history) + 1]
    /\ decs' = decs \ {d}
    /\ seen' = IF lease.live THEN seen ELSE seen \cup {"append-after-expiry"}
    /\ UNCHANGED <<lease, owners, pinned, held, acked>>

Next ==
    \/ \E w \in Workers :
        \/ Acquire(w)
        \/ Release(w)
        \/ Renew(w)
        \/ Begin(w)
        \/ \E s \in Signals : Receive(w, s)
    \/ \E d \in decs : Lock(d) \/ Commit(d)
    \/ \E s \in Signals : Write(s) \/ Ack(s)
    \/ Expire

Spec == Init /\ [][Next]_vars

\* The workers are interchangeable, so TLC checks one order of them.
WorkerSymmetry == Permutations(Workers)

-----------------------------------------------------------------------------
(* Safety invariants *)

\* Each event comes from the worker that took its epoch. So at most one
\* owner commits while the row is at one epoch.
OneOwnerPerEpoch ==
    \A i \in DOMAIN history : history[i].by = owners[history[i].at]

\* A decision commits only at the epoch that it pinned.
CommitAtPinnedEpoch ==
    \A i \in DOMAIN history : history[i].pinned = history[i].at

\* Each decision saw every earlier event, so no event is lost.
DecisionOnFreshHistory ==
    \A i \in DOMAIN history : history[i].base = i - 1

\* No two events consume one signal, so no event is duplicated.
NoDuplicateEvent ==
    \A i, j \in DOMAIN history : i # j => history[i].sig # history[j].sig

\* A signal is acknowledged only after its durable write.
AckAfterDurableWrite == acked \subseteq written

\* An acknowledged signal is pending, consumed, or in a live memory.
NoAckedSignalLost ==
    acked \subseteq inbox \cup Consumed \cup UNION {mem[w] : w \in Workers}

\* Only a takeover fences a commit. The holder never fences itself.
OwnerNeverFencesItself == "self-fence" \notin seen

-----------------------------------------------------------------------------
(* Reachability witness. OwnerLeaseReach.cfg expects TLC to violate it.    *)
(* The violation is one behaviour of the design that reaches all four      *)
(* nemeses:                                                                *)
(*   1. stale-commit         A stale owner commits after a takeover.       *)
(*   2. flap                 The lease flaps under a decision.             *)
(*      append-after-expiry  The lease ends during an append.              *)
(*   3. renew-in-flight      A renewal lands while a commit holds a lock.  *)
(*   4. ack-before-bump      An acked signal waits at an epoch bump.       *)

SomeNemesisUnseen == ~(WitnessTags \subseteq seen)

=============================================================================
