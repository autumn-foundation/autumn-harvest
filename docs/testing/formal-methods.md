# Formal methods: TLA+ models, Kani proofs and Verus proofs

Issue #1819 adds two tools, and a third tool follows it:

- **TLA+ and TLC** check the design of the core protocols. TLC explores every
  interleaving of a small, bounded model and checks safety invariants in each
  state.
- **Kani** proves bounds on pure Rust kernels. It checks every input of the
  real function, not a sample.
- **Verus** proves integer kernels with an SMT solver. It handles the
  128-bit products and the mixer that CBMC cannot finish. See
  [Run the Verus proofs](#run-the-verus-proofs).

No tool replaces the Docker-backed integration tests. A model checks the
design. The integration tests check that the SQL implements it.
[`simulation.md`](simulation.md) covers a seeded harness that checks the
`ActivityClaim` invariants on an oracle. A differential test checks the
oracle against Postgres.
[`stateright.md`](stateright.md) covers Stateright models that visit every
schedule of that oracle within a bound. They must break each TLA+
counter-example on the same invariant.
[`concurrency-model-checking.md`](concurrency-model-checking.md) covers loom
and Shuttle, which check in-process concurrency.

## Run the TLA+ models

The runner needs Java 11 or later.

```bash
cargo xtask check formal-models
```

The command downloads `tla2tools.jar` 1.7.4 and checks its SHA-256. To use a
local jar, set `TLA2TOOLS_JAR=/path/to/tla2tools.jar`. A full run takes about
3 minutes on 4 cores. The `OwnerLease` design config takes about 2 minutes
of it.

To run one config by hand:

```bash
cd formal/tla
java -cp "$TLA2TOOLS_JAR" tlc2.TLC -metadir /tmp/tlc \
  -config ActivityClaimPreFix.cfg ActivityClaim.tla
```

`-metadir` keeps TLC's `states/` directory out of the tree.

The `formal-models` job in `.github/workflows/ci.yml` runs the command. Like
`msrv`, it skips draft PRs and docs-only changes.

### The manifest

`formal/tla/models.txt` lists every config. Each row has three columns:

| Column | Meaning |
|---|---|
| spec | The module, `formal/tla/<spec>.tla`. |
| config | The TLC config in `formal/tla/`. |
| expect | `pass`, or `violation:<Invariant>`. |

A `pass` row must finish with no error. A `violation` row is a
counter-example. TLC must exit with code 12 and name that invariant. A
`violation` config lists only the invariant that it must break, so TLC
cannot stop on another one first.

A counter-example row also guards itself. If a change makes it pass, the
runner fails, so a counter-example cannot rot.

The guard suite `autumn-harvest/tests/integration/formal_models_coverage.rs`
fails in these cases:

- A spec or a config has no row.
- A row names an invariant that its spec does not define.
- This page does not name a spec or a Kani proof.

### The models

| Spec | Protocol | Configs |
|---|---|---|
| `ActivityClaim` | Activity claim epoch (issue #1789) | fixed, pre-fix, reachability |
| `WorkflowTaskClaim` | Workflow-task terminal-write ownership (issues #1184, #1806, #1917) | fixed, pre-fix, reachability, capability-miss pre-fix |
| `CodecRotation` | Codec re-encryption against PII erasure (issues #948, #495) | CAS, blind write, reachability |
| `OwnerLease` | Fenced owner lease of a decision partition (issue #2250) | design, eight defects, reachability |
| `AgentMesh` | Durable peer send between two agents (issue #2269) | design, three single-layer configs, four defects, reachability |
| `AgentWorkspace` | Workspace head of a durable agent (issue #2274) | design, two defects, reachability |
| `AgentWorkspaceRetention` | Workspace retention across the ops and lives of a durable agent (issue #2274) | design, three defects, reachability |
| `OverrideCarry` | The carry of a DAG schedule override to its new shard (issues #2413, #2602) | design, three shards, two routing configs, skew, one limit, six defects, reachability |

**Ghost sequence numbers.** Each claim in the two claim models gets a ghost
`seq`. The code has no such column. An invariant that compared
`(worker_id, attempt)` would restate the guard, so it could not catch a
reused pair. The invariants compare `seq` instead.

**Reachability witnesses.** A `pass` row only says that no bad state is
reachable. A model that is too strict also passes. Each spec therefore has
a `...Reach.cfg` row. It must violate an invariant that says "the race never
happens". The violation proves that the fixed model reaches the race.

#### `ActivityClaim` — model (a)

The prose spec is design decision 9 in
[`architecture.md`](../architecture.md#key-design-decisions). The model has
one task row and these actions:

| Action | Code |
|---|---|
| `Claim` | `queue::claim_task`. Sets `RUNNING` and `worker_id`, adds 1 to `attempt`. |
| `Start` | The start fence, `lock_claim_for_update`. |
| `SelfRelease` | A pause release, capability-miss release, rate-limit deferral or retry-budget deferral. Subtracts 1 from `attempt`. |
| `Heartbeat` | `record_heartbeat`. A lost lease cancels the activity. |
| `OrphanReclaim` | `requeue_orphan`. Keeps `attempt`. Has no guard on the worker, because a live worker can look dead. |
| `RetryRequeue` | `requeue_claimed_task_for_retry`. |
| `Finish` | The finalize paths. Append the terminal event and the row state in one step. |
| `TimeoutFail` | The timeout sweeper. Not an owner, so not fenced. |

`Fenced = TRUE` gives every owner write the `claim_held` predicate.
`Fenced = FALSE` gives it only `state = 'RUNNING'`, as before #1789.

`SelfRelease` uses the full `claim_held` guard, which is stronger than the
pause guards in the code. The pause releases run in the claim's own
transaction, so the stronger guard is equivalent here. The capability-miss
release checks `claim_held` and `crash_strikes` (issue #1917).

Invariants:

- `AtMostOneTerminal`: at most one terminal event.
- `TerminalStateHasOneEvent`: a terminal row state has exactly one event.
- `TerminalByCurrentClaim`: an owner's terminal event takes effect only while
  its claim is current.
- `OwnerWritesByCurrentClaim`: every owner write takes effect only while its
  claim is current.
- `HeartbeatByCurrentClaim`: a heartbeat on a running row comes from the
  claim that the row holds.
- `ClaimIdsAreUnique`: no two live claims share `(worker_id, attempt)`.

The fixed config checks 3 workers and 5 claims (1,324,284 states). It
passes.

`ActivityClaimPreFix.cfg` reproduces the #1789 bug. TLC prints this trace:

1. `w1` claims the row: attempt 1.
2. `w1` starts the activity.
3. The orphan reclaimer requeues the row. `w1` is still alive.
4. `w1` claims the row again: attempt 2.
5. The stale claim `(w1, 1)` finishes. Its write matches
   `state = 'RUNNING'`, so its result wins. `TerminalByCurrentClaim` fails,
   because the row holds `(w1, 2)`.

The pre-fix model also breaks `HeartbeatByCurrentClaim` and
`ClaimIdsAreUnique`. A stale rate-limit deferral can lower `attempt` under a
later claim, so a new claim can reuse a live pair. The pre-fix config lists
only `TerminalByCurrentClaim`, as its manifest row requires.

`ActivityClaimReach.cfg` must violate `NoStaleOwnerAfterFinish`. The
violation shows a stale owner that still runs after a later claim finished
the task.

#### `WorkflowTaskClaim` — model (b)

A decision cycle persists the terminal state of its run only while
`claim_still_held_for_update` holds. The suspension release uses the same
guard. The stuck-running requeue keeps `crash_strikes` and `attempt`. The
orphan reclaim adds a strike. The suspension release resets the strikes
to 0.

`ChecksAttempt = FALSE` models the guards before #1806, which have no
`attempt` term. TLC violates `TerminalByCurrentClaim` with the trace from
the issue:

1. `w1` claims the row.
2. The stuck-running requeue frees the row.
3. `w1` claims the row again.
4. The stale cycle from the first claim closes the run.

`OwnerWritesByCurrentClaim` covers the suspension release too. Remove the
`attempt` term from that release alone, and the fixed config fails.

**The capability-miss release (issue #1917).** A cycle that started its
handler can release its claim through
`release_task_for_capability_miss_query` (phases `DuringHandler` and
`AfterHandler`). `CapMissGuard` selects the guard of that release:

- `WorkflowTaskClaim.cfg` uses `"epoch"`, the guard after #1917. It checks
  `attempt` too. The fixed config passes.
- `WorkflowTaskClaimCapMissPreFix.cfg` uses `"strikes"`, the guard before
  #1917. It checks `worker_id` and `crash_strikes` only. TLC violates
  `OwnerWritesByCurrentClaim`: after a stuck requeue and a re-claim by the
  same worker, the stale cycle re-pends the live claim.

This model found the #1917 gap before any test did.

**The unstarted release (issue #1813).** A draining worker gives back a
claim that never started, through `queue::release_unstarted_claim`.
`UnstartedRelease` models it. Its fence is `claim_held`, with no strikes
term. The release came after #1806, so its guard checks `attempt` in every
config, the pre-fix config included. It subtracts 1 from `attempt` and keeps `crash_strikes`. The trace
check of issue #2003 found this gap: the chaos trace of
`chaos_repro_1813_drain_releases_a_claim_that_never_started` matched no
action. Every config gives the same result with the new action.

#### `CodecRotation` — model (c)

The sweep reads a payload under the retired key and writes it under the
active key. Erasure writes a tombstone. `CLAUDE.md` names these two writers
as the only exceptions to the append-only `harvest_events` rule.

Invariants:

- `ErasureIsFinal`: an erased row stays erased.
- `CiphertextKeepsItsRow`: the sweep never writes one row's bytes into
  another row.

`Cas = FALSE` models a blind write. TLC violates `ErasureIsFinal`: the sweep
reads a row, erasure tombstones it, and the sweep writes ciphertext over the
tombstone. The compare-and-swap in `compare_and_swap_event` prevents this.

The model treats erasure as one step. The real erasure reads the row, then
writes it back without a lock. That is safe, because it rewrites only the
payload fields that the sweep also writes. The model does not check pass
completion or the unresolved count.

#### `OwnerLease` — model (e)

The prose spec is [ADR 0010](../adr/0010-fenced-owner-lease.md). Stage 1a
(issue #2251) implements the model in `decision_owner.rs`.
`decision_owner_sim_db_tests.rs` runs the simulator on that code. A renewal
needs a live lease, as in the code. The model has one
partition, one owner row and the history of one execution. Its actions:

| Action | Code |
|---|---|
| `Acquire` | The takeover: `SELECT ... FOR UPDATE`, then the write. Adds 1 to the epoch. Waits for every fence lock. Then loads the resident state. |
| `Release` | The graceful release. Ends the lease, keeps the holder and the epoch, and unloads the worker. |
| `Expire` | The lease time runs out: a pause or a clock jump. A lock does not stop it. |
| `Renew` | The renewal `UPDATE` of `lease_until`. Matches the holder and the epoch, and needs a live lease. A miss unloads the worker. |
| `Write`, `Ack` | The `harvest_signals` insert, then the HTTP 202. |
| `Receive` | The `memory-ack` defect only: an ack from owner memory. |
| `Begin` | A decision reads its resident state and pins the owner's epoch. |
| `Lock` | The commit reads the owner row `FOR KEY SHARE` and checks the fence. |
| `Commit` | The commit appends the event and consumes the signal. |

An unloaded worker keeps its decisions in flight, as a paused thread does.
A new tenure of that worker can begin a decision beside them. The model has
no event-id backstop, so the fence alone must hold.

Each decision records ghost fields: `pinned` (its epoch), `at` (the row
epoch at the commit) and `base` (the history length it saw). The owner row
has a ghost `tenure`, the number of acquisitions. The ghost `owners` maps
each epoch to the worker that took it. The invariants compare these
fields:

- `OneOwnerPerEpoch`: each event comes from the worker that took its
  epoch.
- `CommitAtPinnedEpoch`: a decision commits only at the epoch it pinned.
- `DecisionOnFreshHistory`: each decision saw every earlier event.
- `NoDuplicateEvent`: no two events consume one signal.
- `AckAfterDurableWrite`: a signal is acked only after its durable write.
- `NoAckedSignalLost`: an acked signal is pending, consumed, or in the
  memory of the worker that holds the row at its epoch. A stale owner can
  never commit from memory, so its memory does not count.
- `OwnerNeverFencesItself`: only a takeover fences a commit.
- `LiveRowHasAPinnedHolder`: the holder of a live row is pinned at the
  epoch of the row. Without it, a drain with no fault can stop with work
  left (issue #2294).

In the design, `AckAfterDurableWrite` restates the guard of `Ack`. It
matters only for the `memory-ack` mutant. `OneOwnerPerEpoch`,
`DecisionOnFreshHistory` and `NoAckedSignalLost` carry the weight.
`OwnerNeverFencesItself` is an availability rule: none of the three safety
properties of the issue catches `renew-bumps`.

`Defect = "none"` is the design. Each other value breaks one rule of the
ADR. Its config lists only the invariant that it must break:

| Config | Defect | Rule | Nemesis | Must violate |
|---|---|---|---|---|
| `OwnerLeaseNoFence.cfg` | `no-fence`: no check in the commit (golem#3766) | R1 | 1 | `OneOwnerPerEpoch` |
| `OwnerLeaseUnlockedFence.cfg` | `unlocked-fence`: the check takes no lock | R2 | 2 | `OneOwnerPerEpoch` |
| `OwnerLeaseHolderFence.cfg` | `holder-fence`: the check compares the holder | R3 | 2 | `DecisionOnFreshHistory` |
| `OwnerLeaseKeepCache.cfg` | `keep-cache`: an acquisition keeps old state | R4 | 2 | `DecisionOnFreshHistory` |
| `OwnerLeaseRenewBumps.cfg` | `renew-bumps`: a renewal adds 1 to the epoch | R5 | 3 | `OwnerNeverFencesItself` |
| `OwnerLeaseMemoryAck.cfg` | `memory-ack`: an ack from memory (restate#4566) | R6 | 4 | `NoAckedSignalLost` |
| `OwnerLeasePipeline.cfg` | `pipeline`: two decisions in one tenure | R7 | — | `DecisionOnFreshHistory` |
| `OwnerLeaseUnloadOnAnyFence.cfg` | `unload-on-any-fence`: a fence unloads a newer tenure | R8 | 2 | `LiveRowHasAPinnedHolder` |

`OwnerLeaseNoFence.cfg` gives this trace:

1. `w1` acquires epoch 1. A client writes `s1`.
2. `w1` begins a decision on `s1`.
3. `w1` releases the row. Its decision still runs.
4. `w2` acquires epoch 2.
5. `w1` commits. No fence stops it, so its event lands at epoch 2, which
   `w2` took.

`OwnerLease.cfg` checks the design with 3 workers, 2 signals and 4 epochs.
The workers are interchangeable, so the config sets
`SYMMETRY WorkerSymmetry`. TLC finds 4,980,851 distinct states and no
error, in about 2 minutes on 4 cores. Since issue #2251, a renewal needs a
live lease, and a release keeps the holder, as the code does. The two signals limit the history to
two events.

`OwnerLeaseReach.cfg` must violate `SomeNemesisUnseen`. The violation is
one behaviour of the design that reaches all four nemeses:

| Tag | Nemesis |
|---|---|
| `stale-commit` | 1. A stale owner reaches its commit after a takeover and is fenced. |
| `flap` | 2. A worker takes the lease back while its old decision is in flight. |
| `append-after-expiry` | 2. The lease ends during an append, and the append lands. |
| `renew-in-flight` | 3. A renewal lands while a commit holds the fence lock. |
| `ack-before-bump` | 4. The epoch bumps while an acked signal waits. |

`autumn_harvest::dst::owner` runs the same defects and nemeses from a seed.
See [`simulation.md`](simulation.md#the-owner-lease-simulation).

#### `AgentMesh` — model (f)

The prose spec is section 7 of `docs/design/DESIGN-2262.md`. Issue #2269 asks for this
model before the code. Agent A sends `PeerMessage` ops to agent B by
workflow id. Both agents are keyed entities. The send key is
`<from>:<seq>`. A durable sender sets `from` to
`<agent id>@<incarnation>`. An `agent_loop` sender sets it to its run id.
The seq is durable state of A. It moves only when the send commits with
the decision of A. The model has one sender, so the seq is the key.

A send takes one of two paths. On one shard, the decision of A inserts the
signal row of B in its own transaction. Across shards, the decision writes
an outbox row, and a relay delivers it at least once, in any order. One
deployment takes one path for sends by workflow id. The model lets each
send take either path, so it covers both deployments. Its actions:

| Action | Code |
|---|---|
| `Decide` | A picks its next send: seq + 1, a path, and `Mandate::outbound`. |
| `SendCommit` | The decision of A commits `signal_external_workflow_by_id_with_idempotency`. The inline path inserts the signal row of B. The outbox path writes an outbox row. |
| `Crash` | A loses its decision in flight. Its durable seq stays, so the next decision sends the same key. |
| `Relay` | `enforce_external_signals_outbox` delivers a row. A records `ExternalSignalDelivered`. |
| `RelayAgain` | The relay delivers a row but loses the outcome. The row stays, so the relay delivers it again. |
| `Begin` | The owner of B reads the head of its inbox. |
| `Commit` | The fenced commit of B consumes the op, applies it and updates the dedupe state of B. A fenced commit aborts and changes nothing. |
| `OwnerChange` | A new owner takes B, so the epoch moves. The new owner replays B. |
| `Checkpoint` | B continues as new. It carries its state and its waiting ops. |

B dedupes in two layers. Each layer is a constant, so a config can turn it
off:

| Constant | Values | Meaning |
|---|---|---|
| `EngineDedupe` | `"chain"`, `"run"`, `"off"` | The engine refuses a signal key that it took before. `"chain"` keeps the keys across continue-as-new, as the engine does. `"run"` keeps the keys of one run only. |
| `SeqDedupe` | `"applied_set"`, `"last_seq"`, `"off"` | The agent layer of B dedupes by seq. `"applied_set"` is the design: B skips a seq that it applied. `"last_seq"` was the first design: B skips each seq up to the highest seq that it applied. |
| `Accept` | `"meet"`, `"join"` | `"meet"` is `local.accept(inbound)`. `"join"` takes the union, which widens. |
| `MaxInFlight` | 1, 2 | The bound on sends of A that wait for their delivery. The design allows 2. |

**The applied set.** B keeps it in its state as `floor` and `above`. Each
seq up to `floor` is applied. `above` holds the applied seqs over `floor`.
A message is a duplicate when its seq is at most `floor`, or in `above`. To
apply seq s, B adds s to `above`. Then `floor` moves up while `floor + 1`
is in `above`, and those seqs leave `above`. When messages arrive in order,
`above` stays empty.

The fence is taken as given: a decision of B commits only at the epoch that
it read. `OwnerLease` proves that rule. The outbound mandate is the meet of
the mandate of A and the tools of the peer entry of B, as
`Mandate::outbound` computes it.

Invariants:

- `AtMostOnce`: B applies each seq at most once.
- `NoLostAck`: a message whose delivery A recorded is applied, or it still
  waits in the inbox of B.
- `MandateNarrows`: B runs each message within its local mandate, the
  inbound mandate and the mandate of A. A replay and a duplicate run under
  the same rule.
- `ConsumesTheHead`: the decision of the current owner holds the head of
  the inbox. Its commit therefore consumes the op that it read.
- `AppliedSetExact`: `floor` and `above` encode the applied seqs exactly.
  `above` holds no seq at `floor + 1` or lower, so `floor` is as high as it
  can be. A mutant that keeps the absorbed seqs in `above` fails it.

The pass configs check every invariant above. They also check the liveness
property `EventuallyApplied`: B applies each committed send. They use
`LiveSpec`, which adds weak fairness for the commit of A, the relay and B.
The nemeses have bounds, so each behaviour runs out of them.
`EventuallyApplied` with `AtMostOnce` is exactly-once delivery.

The table gives what each config changes from the design. A red config
lists only the invariant that it must break:

| Config | Change from the design | Result |
|---|---|---|
| `AgentMesh.cfg` | None: `EngineDedupe = "chain"`, `SeqDedupe = "applied_set"`, `MaxInFlight = 2`. | Pass. |
| `AgentMeshEngineOnly.cfg` | `SeqDedupe = "off"` | Pass. |
| `AgentMeshSeqOnly.cfg` | `EngineDedupe = "off"` | Pass. |
| `AgentMeshLastSeqSerial.cfg` | `EngineDedupe = "off"`, `SeqDedupe = "last_seq"`, `MaxInFlight = 1` | Pass. |
| `AgentMeshNoDedupe.cfg` | `EngineDedupe = "off"`, `SeqDedupe = "off"` | Violates `AtMostOnce`. |
| `AgentMeshRunScope.cfg` | `EngineDedupe = "run"`, `SeqDedupe = "off"` | Violates `AtMostOnce`. |
| `AgentMeshLastSeq.cfg` | `SeqDedupe = "last_seq"` | Violates `NoLostAck`. |
| `AgentMeshWiden.cfg` | `Accept = "join"` | Violates `MandateNarrows`. |
| `AgentMeshReach.cfg` | None. | Violates `SomeNemesisUnseen`. |

Each dedupe layer alone keeps delivery exactly once with two sends in
flight. In `AgentMesh.cfg` the engine refuses every duplicate, so the
applied set never skips a message. `AgentMeshSeqOnly.cfg` turns the engine
dedupe off. There the applied set skips each duplicate, in any order of
arrival. Turn both layers off, and the relay duplicate runs twice.
`AgentMeshRunScope.cfg` shows why the engine keeps the keys of the whole
chain:

1. A sends seq 1 through the outbox. The relay delivers it and loses the
   outcome.
2. B applies seq 1.
3. B continues as new. A dedupe of one run forgets key 1.
4. The relay delivers seq 1 again, and B applies it again.

**Why the design uses an applied set.** The first design kept only the
highest applied seq, `lastSeq`. The model found that this loses a message
when two sends are in flight. `AgentMeshLastSeq.cfg` gives this trace:

1. A sends seq 1 through the outbox.
2. A sends seq 2 on the inline path, so B gets it first.
3. The relay delivers seq 1, and A records the delivery.
4. B applies seq 2. `lastSeq` is now 2.
5. B skips seq 1, because `1 <= lastSeq`. `NoLostAck` fails.

Two outbox rows can lose a message in the same way. A sweep retries a row
on a later tick, after a newer row. Concurrent children (issue #2270) can
put two sends to one peer in one decision.

The applied set keeps seq 1. After step 4, `floor` is 0 and `above` is
{2}. Seq 1 is not a duplicate, so B applies it. `floor` moves to 2, and
`above` is empty again. `AgentMeshLastSeqSerial.cfg` shows that `last_seq`
is safe only with one send in flight.

The code must keep two rules that the model has no room to show:

- A seq counts the sends of one sender to one receiver. With one counter
  for all peers, each receiver sees gaps. Then `floor` stops, and `above`
  grows with each send.
- B keys the applied set by `from`. For a durable sender, `from` is
  `<agent id>@<incarnation>`. A new incarnation of the sender starts its
  seq at 1 again, so the key holds the incarnation.

`AgentMesh.cfg` checks 2 sends, 2 crashes of A, 3 epochs of B, one lost
relay outcome and one checkpoint. TLC finds 60,603 distinct states and no
error, in under 15 seconds on 4 cores.

`AgentMeshReach.cfg` must violate `SomeNemesisUnseen`. The violation is
one behaviour of the design that reaches every tag:

| Tag | Nemesis |
|---|---|
| `crash-before-commit` | A crashes with a send that it did not commit. |
| `crash-before-delivery` | A crashes with its send in the outbox. |
| `crash-after-delivery` | A crashes with its message delivered and waiting at B. |
| `relay-duplicate` | The relay delivers one row twice. |
| `owner-change-abort` | The fence aborts a decision of an old owner of B. |
| `replay-after-abort` | The new owner runs that message again. |
| `checkpoint-in-flight` | B continues as new with a message in flight. |
| `out-of-order-apply` | B applies a seq below one that it applied before. |

The model has these limits:

- It has one sender and one receiver, and no reply. The reply is a durable
  promise that A races against a deadline. The deadline ends only the wait
  of A.
- A send needs a running target. The model has no `target_unknown` and no
  `not_running` outcome.
- A crash of A drops only its decision in flight. A stale writer of A is
  the `OwnerLease` case.
- The mandate is a set of tools. Flow labels and budgets are not modelled.

#### `AgentWorkspace` — model (g)

`formal/tla/AgentWorkspace.tla` models the head of a workspace (issue
#2274). One op runs `Calls` tool calls in order. Each call can run as
several attempts. An attempt writes a snapshot, and a stale attempt of an
earlier call can still write. History records the result of one attempt of
each call. Retention keeps the head and deletes every other snapshot.

| Action | Code |
|---|---|
| `Write` | `WorkspaceStore::save` in the tool-call activity. |
| `Record` | The engine records the result. `adopt_workspace` moves the head. |
| `Retain` | The activity `agent_workspace_retain` at the end of the op. |

| Config | Switch | Result |
|---|---|---|
| `AgentWorkspace.cfg` | None: `AdoptOn = "record"`, `RetainWhen = "op_end"`. | Pass. |
| `AgentWorkspaceAdoptOnWrite.cfg` | `AdoptOn = "write"` | Violates `HeadIsRecorded`. |
| `AgentWorkspaceRetainAnyTime.cfg` | `RetainWhen = "any_time"` | Violates `HeadPresent`. |
| `AgentWorkspaceReach.cfg` | None. | Violates `SomeStaleWriteUnseen`. |

`HeadIsRecorded` says that the head is the snapshot of a recorded result.
`HeadPresent` says that the head is in the store, so the next call can load
it. A retention that runs while a call runs can delete a snapshot that
history then records. So retention runs only after the last call of an op.

The model has one op, and the store never loses a blob on its own.

#### `AgentWorkspaceRetention` — model (h)

`formal/tla/AgentWorkspaceRetention.tla` models retention across ops and
lives. Each op commits a snapshot, which becomes the head, resets the
workspace, or ends the life. The op end runs a retention activity with the
head as its input, or a reset activity. The workflow waits for the first
completed attempt. An older attempt can still run at any later time. A
stale attempt whose lease the engine reclaimed is such a late attempt.

| Action | Code |
|---|---|
| `Commit` | `WorkspaceStore::save` in the tool-call activity, then `adopt_workspace`. |
| `Reset` | `Control::ResetWorkspace`, which runs `agent_workspace_reset`. |
| `NewLife` | A delete, then a new run. `agent_workspace_floor` gives the floor. |
| `Complete` | The first completed attempt of an activity. |
| `Run` | A later attempt of `agent_workspace_retain` or `agent_workspace_reset`. |

| Config | Switch | Result |
|---|---|---|
| `AgentWorkspaceRetention.cfg` | None: `KeyBy = "generation"`, `FloorBy = "store"`, `MarkReset = TRUE`. | Pass. |
| `AgentWorkspaceRetentionContentKeys.cfg` | `KeyBy = "content"` | Violates `HeadPresent`. |
| `AgentWorkspaceRetentionClockFloor.cfg` | `FloorBy = "clock"` | Violates `HeadPresent`. |
| `AgentWorkspaceRetentionNoMarker.cfg` | `MarkReset = FALSE` | Violates `HeadPresent`. |
| `AgentWorkspaceRetentionReach.cfg` | None. | Violates `NoStaleRun`. |

With content keys, a stale retention keeps its old input head and deletes
every other key, the newer head too. A key that holds the generation of
its op fixes it within one life. Retention deletes only the keys of the
head generation or older, so a newer head survives, also when it has the
same content as an old one.

A new life starts with no state, so its generation needs a floor. A clock
can read low, and the new life then reuses old generations. A late attempt
of the old life deletes the new head. The design reads the floor from the
store: the highest key, or the marker that a reset leaves at its bound.
Without the marker, a reset that empties the store gives a floor of 0.

#### `OverrideCarry` — model (i)

`formal/tla/OverrideCarry.tla` models the carry of a DAG schedule override
(issues #2413, #2602). Each shard holds the override table of one DAG. An
operator sets or clears the override on the owner, and a routing change
moves the owner at any time. Each shard that holds a row but does not own
the DAG copies the row to the owner, then deletes it on the source. The
delete can fail, and the carry then starts again. A promotion of any shard
can happen at any time.

A version is the `updated_at` of a row. Each shard keeps a floor. A
replace raises it to the old row, and a clear raises it to a new version.
A copy skips a version at or below the floor of its destination. A carry
also moves the floor of its source to the owner.

| Action | Code |
|---|---|
| `Set`, `Clear` | `update_dag_override`, `clear_dag_override` on the owner. |
| `Flip` | A routing change. `FlipWhen` says when it can land. |
| `Begin` | `carry_dag_override` takes the registration lock of both shards. |
| `Copy` | The destination transaction of `carry_dag_override` commits. |
| `Abort` | The carry fails before its copy commits. |
| `Delete` | The source transaction of `carry_dag_override` commits. |
| `FailDelete` | A failed delete. The next tick runs the carry again. |
| `Promote` | `bump_generation` in the new region. |

| Config | Switch | Result |
|---|---|---|
| `OverrideCarry.cfg` | None: two shards, `FlipWhen = "any"`. | Pass. |
| `OverrideCarryThreeShards.cfg` | Three shards. | Pass. |
| `OverrideCarryFlipAfterFailedDelete.cfg` | `FlipWhen = "idle"`. | Pass. |
| `OverrideCarryFlipMidCarry.cfg` | A deeper bound. | Pass. |
| `OverrideCarrySkew.cfg` | `Fresh = FALSE` | Pass. |
| `OverrideCarrySkewLimit.cfg` | `Fresh = FALSE`. A known limit. | Violates `LastWriteWins`. |
| `OverrideCarryNoFloor.cfg` | `Floor = FALSE` | Violates `ClearIsKept`. |
| `OverrideCarryNoFloorCarry.cfg` | `CarryFloor = FALSE` | Violates `ClearIsKept`. |
| `OverrideCarryWallClock.cfg` | `Hlc = FALSE` | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryOwnerRowWins.cfg` | `Newest = FALSE` | Violates `LastWriteWins`. |
| `OverrideCarryUnserialized.cfg` | `Serial = FALSE` | Violates `NothingLostWithoutAClear`. |
| `OverrideCarryUnfenced.cfg` | `Fenced = FALSE` | Violates `NoWriteAfterPromotion`. |
| `OverrideCarryReach.cfg` | None. | Violates `NoClearOfACopiedRow`. |

Without a floor, a stale copy brings back a cleared version. A floor that
stays on its shard lets a route back serve a stale row. A write that does
not take a version above its shard can land below a floor. A copy that
always yields to the row of its destination lets a stale older row win.
Without the lock of both shards, two carries in opposite directions both
delete. With skewed clocks, a set within the skew of a clear on another
shard can lose. See [`DESIGN-2602.md`](../design/DESIGN-2602.md).

### Extend a model

1. Find the action that matches the code path. Change its guard or its
   effect. Keep one action for each atomic SQL statement or transaction.
2. Add the invariant that the change must keep. Name it in the config.
3. Run the runner. A new counter-example gets its own config and a
   `violation:` row.
4. Update the action table on this page.

### Add a model

1. Add `formal/tla/<Spec>.tla`. Put the issue number and the code paths in
   the header comment.
2. Add a passing config. Keep the run under one minute.
3. Add a counter-example config that turns the fix off. List only the
   invariant that it must break.
4. Add a reachability witness for the race that the fix closes.
5. Add a row for each config to `formal/tla/models.txt`.
6. Add a section to this page. The guard suite checks that it names the spec.

## Check engine traces against the models

Issue #2003 checks that the running engine follows `ActivityClaim` and
`WorkflowTaskClaim`. It uses trace validation: TLC checks that a recorded
history of a task row is a behavior of the spec.

```bash
cargo xtask check formal-traces formal/tla/trace/fixtures
```

`cargo xtask check formal-traces` needs Java 11 or later. It uses the
`tla2tools.jar` pin of `cargo xtask check formal-models`, and it reads
`TLA2TOOLS_JAR` in the same way. The fixtures take about 10 seconds.

### Record a trace

The chaos suite records when `HARVEST_TLA_TRACE_DIR` is set. Two triggers
copy each committed write to `harvest_task_queue`, and each activity or
terminal event in `harvest_events`, into the `harvest_tla_trace` table. A
trigger row rolls back with its transaction, so the log holds committed
steps only. The recorder is in `autumn-harvest/tests/integration/tla_trace.rs`.
Its unit tests run in every build, so each PR checks the exporter.

At the end of each case, the suite writes one NDJSON file for each task row:

- Line 1 is a header. It names the spec and the checks, for example
  `{"spec": "ActivityClaim", "checks": {"fixed": "accept"}}`.
- Line 2 is the row's insert, with op `init`.
- Each other line is one transaction. It holds the row's `state`,
  `worker_id`, `attempt`, `crash_strikes` and its count of terminal events.

A line has one of three ops:

| Op | Transaction |
|---|---|
| `write` | It changes a logged column or adds a terminal event. |
| `start` | It appends `ActivityStarted`. `by` names the event's worker. |
| `heartbeat` | It changes only `last_heartbeat_at` of an activity row. |

A heartbeat line in any state must match an accepted heartbeat. A beat on a
row that no claim holds therefore fails the check.

The exporter maps the engine onto the spec in three places:

- A parked workflow task is `RUNNING` with no `worker_id`. No claim holds
  it, so the trace logs it as `PENDING`.
- `ActivityCompleted` and `ActivityCompletedExternally` are always
  terminal. `ActivityFailed`, `ActivityTimedOut` and
  `ActivityFailedExternally` are terminal unless the same transaction
  requeues the row for a retry. A stale result with no requeue therefore
  counts as a second terminal event, and the check fails.
- Transactions are in the order of their last log id. Row locks serialize
  the writes of one task row, so this order is the commit order.

**The writer.** A test can open a connection with `tla_trace::actor_url`.
The URL sets `harvest.trace_actor` to `<task>/<worker>/<attempt>`, and the
trigger copies it. A line that names a claim of its own row must be a new
claim, or an owner action of the named claim. A system action, such as an
orphan reclaim, cannot explain it. A line with no name can be explained by
any action. Production code does not set the name.

The chaos suite names the writer of each decision cycle that it drives. Each
`chaos_drive_one_workflow_task` call gets its own connection, so the suite
passes an `actor_url` for the claim that it drives. A stale write by such a
cycle fails the check. The traces of a real `Worker` have no names, because
its pooled connections serve many claims.

### Check a trace

`xtask/src/dev/formal_traces.rs` writes a TLA+ module for each check. The module
holds the trace as the constant `Log`. TLC then checks `ActivityClaimTrace`
or `WorkflowTaskClaimTrace`, in `formal/tla/trace/`. Each extends its model
with a line cursor:

- `TraceInit` is the model's `Init`, and the `init` line must match it.
- `TraceNext` picks an action of the model that explains the next line.
  The post-state of that action must equal the line.
- TLC checks `LogNotConsumed`. A violation means that a behavior matches
  every line, so the trace is **accepted**. "No error" means that no
  behavior matches, so the trace is **rejected**. The runner then names the
  first line that no behavior matches.

A check names a guard setting:

| Guard | `ActivityClaim` | `WorkflowTaskClaim` |
|---|---|---|
| `fixed` | `Fenced = TRUE` | `ChecksAttempt = TRUE`, `CapMissGuard = "epoch"` |
| `pre-fix` | `Fenced = FALSE` | `ChecksAttempt = FALSE`, `CapMissGuard = "strikes"` |

The runner renames the workers `w1`, `w2` and so on. The models are
symmetric in their workers, so equal traces share one TLC run.

The runner fails in these cases:

- A result differs from its check.
- TLC fails for another reason.
- A trace is malformed, or a directory is empty.
- A directory has no trace for a spec.

### Red tests

A red trace holds an injected protocol violation. There are two kinds:

- A **fence** red trace is a stale owner write. Its header expects
  `"fixed": "reject@N"` and `"pre-fix": "accept"`. The second check proves
  that the fence causes the rejection, not a malformed trace.
- A **forged** red trace breaks the protocol under each guard, for example
  with a second terminal event. Its header expects `reject@N` from both.

`reject@N` names the file line that no behavior may match, usually the
injected line. A rejection at another line fails the check. A plain
`reject` accepts any line.

The sources are:

- `formal/tla/trace/fixtures/` holds at least one clean trace and one fence
  red trace for each spec. The `formal-models` job in `ci.yml` checks them
  on every PR that changes code.
- `chaos_tests::trace_red` runs the #1789 and #1806 races on Postgres. The
  engine fences the stale write. The test then injects the stale write with
  the pre-fix guard, on a connection that names the stale claim.
- `oracle_flags_a_duplicate_terminal_event` forges a second terminal event.
  Its trace expects `reject` from both guards.

### In CI

`chaos.yml` sets `HARVEST_TLA_TRACE_DIR`. After the chaos suite, it runs
`cargo xtask check formal-traces "$HARVEST_TLA_TRACE_DIR"`. The runner checks
every trace from the reproducers, the convergence sweep and the
infrastructure faults. TLC must reject each red trace.

The infrastructure faults need Docker. `chaos_tests::trace_activity` runs
activities on a real worker against any test database, so the
`ActivityClaim` check also has engine traces on a machine with no Docker.
It covers a claim, the start fence, heartbeats, a retry and an orphan
reclaim.

To check the chaos traces on your machine:

```bash
export HARVEST_TLA_TRACE_DIR=/tmp/harvest-traces
rm -rf "$HARVEST_TLA_TRACE_DIR"
cargo test -p autumn-harvest --features chaos --test integration chaos_tests:: \
  -- --test-threads=1
cargo xtask check formal-traces "$HARVEST_TLA_TRACE_DIR"
```

`formal_trace_coverage.rs` fails in these cases:

- A fixture is malformed.
- A spec has no clean fixture, or no red fixture that only the fence rejects.
- A trace spec does not extend its model.
- The trace runner pins its own TLC release.
- `ci.yml` or `chaos.yml` does not run the runner.
- The chaos suite does not install the recorder or export its traces.
- This page does not name a part of the trace check.

### Limits

- Only a test can name the writer. A real `Worker` does not name its
  claims, so any claim can explain its lines. A stale write of a real
  worker can therefore pass. The red tests and the driven cycles cover the
  fences of #1789 and #1806.
- The models have no workflow failure, no cancellation and no quarantine.
  A trace that reaches `FAILED` on a workflow row, or `CANCELLED`, is
  rejected. Extend the model before a chaos case reaches such a path.
- An unobserved action changes no logged column. `TraceNext` omits it. Such
  an action only drops a claim, so a match never needs it.
- A recording run leaves the triggers on its database. A shared
  `HARVEST_TEST_DATABASE_URL` keeps them, and `partition` then refuses to
  convert `harvest_events`. Drop the triggers before other tests use it, or
  use a new database.
- A trace starts at the row's insert. A row from before the recorder was
  installed fails the export.
- `ActivityClaim` does not model `crash_strikes`, so the activity check
  ignores that column. `WorkflowTaskClaim` matches it.
- The model of `CodecRotation` has no trace check.

## Run the Kani proofs

Install Kani once:

```bash
cargo install --locked kani-verifier --version 0.68.0
cargo kani setup
```

Run every proof:

```bash
cargo xtask check kani-proofs
```

The command runs
`cargo kani -p autumn-harvest --no-default-features --features chaos -Z stubbing`.
`chaos` turns on the chaos controller, which holds a proof. `-Z stubbing`
enables `#[kani::stub]`.

`cargo kani` exits 0 when it finds no proof, and when a `kani::cover!` is
unsatisfiable. The command fails in both cases. It also fails when Kani
verifies fewer proofs than the source holds.

To run one proof, call `cargo kani` with the same flags and
`--harness <name>`. The `kani` job in `.github/workflows/ci.yml` runs the
command. It skips draft PRs and docs-only changes, like `msrv`. The compile
takes most of the time. The proofs take about 1 minute together.

### The proofs

Each proof is a `#[kani::proof]` function in a `#[cfg(kani)] mod
kani_proofs` beside the code that it proves.

| Proof | File | Property |
|---|---|---|
| `uniform_inclusive_stays_in_range` | `policy.rs` | The jitter draw stays in `[lo, hi]`. |
| `full_jitter_is_at_most_base` | `policy.rs` | Full jitter is at most its base. |
| `equal_jitter_stays_in_upper_half` | `policy.rs` | Equal jitter stays in `[base/2, base]`. |
| `retry_delay_never_exceeds_max_interval` | `policy.rs` | `compute_retry_delay` does not panic. |
| `seeded_action_respects_caps_and_never_holds` | `chaos.rs` | A seeded plan picks only an allowed action, and never `Hold`, for every stream. |
| `execution_id_round_trips_every_encodable_shard` | `types.rs` | An id built for an encodable shard decodes to that shard, for every UUID. The other 14 bytes stay. |
| `supersede_count_sheds_exactly_down_to_the_cap` | `concurrency.rs` | The shed count is the least that meets the limit, up to `usize::MAX`. |
| `supersede_plan_sheds_only_candidates_and_meets_the_limit` | `concurrency.rs` | A latest-wins plan sheds at most the candidates and the scan limit. A full shed meets the limit. |
| `apply_action_stays_in_band_and_reports_the_move` | `slot_tuner.rs` | A tuner step lands in the band, even a degenerate band, and its decision names the real move. |
| `initial_target_lands_in_the_band` | `slot_tuner.rs` | The initial target is in the band, and a target in the band keeps its value. |
| `slot_occupancy_partitions_the_target` | `slot_tuner.rs` | `in_use + available == target` for every semaphore reading. |
| `counter_growth_is_one_byte_at_a_power_of_ten` | `entity.rs` | A counter grows by 1 byte exactly when `n + 1` gains a digit, else by 0. |

### Scope of each proof

A proof is only as wide as its inputs. These limits are deliberate:

- **The jitter proofs stub `mix64`.** The stub returns any `u64`. The bounds
  hold for every stub value, so they hold for the real mixer. The seed is
  any `u64` too, and `mix64` is a bijection, so the stub loses no case. With
  the real mixer, CBMC did not finish in 10 minutes.
- **The jitter proofs work in nanoseconds.** `full_jitter` and
  `equal_jitter` convert a `Duration` to nanoseconds and back. A proof
  through that round trip must relate a 128-bit multiply to a divide, and
  CBMC does not finish. The proofs cover `full_jitter_nanos`,
  `equal_jitter_nanos` and `uniform_inclusive`.
- **The Decorrelated jitter has no proof of its own.** It calls
  `uniform_inclusive`, which has one. Its own `lo` and `hi` arithmetic does
  not.
- **`retry_delay_never_exceeds_max_interval` excludes NaN.** CBMC reports
  every NaN result as an error, but the code handles NaN on purpose. The
  proof excludes a NaN coefficient and an initial interval of 0, because 0
  times an infinite power is NaN. The unit tests
  `compute_retry_delay_negative_nan` and
  `compute_retry_delay_zero_initial_with_overflowing_power` pin those cases.
- **`retry_delay_never_exceeds_max_interval` has bounded durations.** The
  initial interval and `max_interval` are at most `u32::MAX` milliseconds.
  The final `min` makes the bound true by construction. The proof shows
  that no float conversion panics on the way.
- **The chaos proof does not use the mixer.** CBMC did not prove the
  `splitmix64` bijection in 5 minutes. The chaos proof holds for every
  stream instead, so it holds for any mixer. Verus proves the bijection.
- **The supersede proof found an overflow.** `supersede_count` added 1 to
  any `usize`, and `supersede_plan` added two counts. `supersede_count`
  now subtracts `cap - 1` from the count, so it never adds 1.
  `supersede_plan` saturates its sum. The proof covers every `usize`.
- **The entity proof checks a table, not `ilog10`.** A harness that called
  `ilog10` and `pow` did not finish in 10 minutes. A table of the powers
  of 10 finishes in about 6 seconds.

Three kernels have no Kani proof. Issue #1819 names the first two:

- **`lifecycle::TRANSITIONS`.** The domain is 10 × 10 state pairs, so a
  unit test is exhaustive and runs in microseconds. `lifecycle.rs` holds
  `migration_states_are_one_way` and `completed_and_terminated_never_reopen`.
- **`failure_signature`.** Kani is slow on UTF-8 string code. A proof of
  `truncate_chars` on 3-byte inputs did not finish in 5 minutes. Unit tests,
  proptests and the `fuzz_failure_signature` target cover it.
- **`FailureTolerance::max_failures`.** It divides any `usize` by 100.
  CBMC did not finish that divide in 10 minutes. The fan-out unit tests
  cover it.

### Add a Kani proof

1. Pick a pure function with no I/O. Bound the input with `kani::assume`
   when the full input space is too large.
2. Add a `#[kani::proof]` function to the file's `kani_proofs` module. Add
   the module, under `#[cfg(kani)]`, if the file has none.
3. Add `#[kani::unwind(N)]` to a proof with a loop. Kani reports an
   unwinding failure when `N` is too small.
4. Add `kani::cover!` for an interesting case. A proof whose assumptions
   exclude every input passes for the wrong reason. The command fails on an
   unsatisfied cover.
5. Run the proof alone. Keep it under one minute. A slow proof usually
   relates a multiply to a divide, or builds a `Vec` or a `String`. Stub or
   split the function.
6. Add a row to the table above. The guard suite checks that this page
   names every proof.

## Run the Verus proofs

[Verus](https://github.com/verus-lang/verus) proves Rust code against a
specification with the Z3 SMT solver. A Kani proof checks a bounded
input space. A Verus proof covers every input by deduction. Verus fits the
integer kernels that make CBMC time out: a 64-bit multiply, a 128-bit
product, and a mixer that must be shown to be one to one.

```bash
cargo xtask check verus-proofs
```

The command downloads the pinned Verus release and checks its SHA-256. The pin
is a weekly release, because Verus deletes the assets of older rolling builds. To
use a local binary, set `VERUS=/path/to/verus`. The release names the Rust
toolchain that it links against, and the command installs that toolchain
with rustup. The proofs take about 2 seconds. The `verus` job in
`.github/workflows/ci.yml` runs the command. It skips draft PRs and
docs-only changes, like `msrv`.

The command fails when Verus reports an error, or when it verifies nothing.

### Mirrors

Verus cannot compile the engine crate, so each proof checks a copy of an
engine function in `formal/verus/src/`. A `// mirror: <path> <fn>` line
marks each copy. A mirror can copy a function of `autumn-harvest/src`,
`autumn-harvest-agent/src` or `autumn-harvest-plugin/src`. The guard suite
`autumn-harvest/tests/integration/verus_proofs_coverage.rs` removes
comments, whitespace and `proof { }` blocks from both bodies. The bodies
must then be equal. A `proof { }` block is ghost code. Verus erases it, so
it cannot change what the copy computes.

The guard suite fails in these cases:

- A mirrored body differs from its engine source.
- The mirrored functions differ from the engine functions whose `///` doc
  comment names a Verus file. A deleted `// mirror:` marker fails, and an
  editor of a proved function finds its proof in the doc comment.
- A Verus file uses `assume`, `admit`, `axiom`, `external_body` or another
  way to skip a step. The guard lexes the file and bans every name that
  holds `assume`, `admit`, `axiom`, `external` or `truncate`, so a comment,
  a space or a `use` alias cannot hide one. `macro_rules` is banned too,
  because a local macro can delete a proof before Verus sees it.
- A Verus proof file holds anything but `use` items and one `verus! { }`
  block. Verus checks only the code inside the block, so a mirror moved
  out of it would prove nothing.
- A Verus file uses an attribute other than `#[trigger]`. A
  `#[verifier::...]` attribute can trust a step, and a `#[cfg(...)]` can
  delete a proof, so a new attribute must be allowed in the guard on
  purpose.
- A Verus contract differs from its line in `formal/verus/contracts.txt`.
  The list holds the signature and the `requires`, `ensures` and
  `decreases` clauses of each function, and the whole of each `spec fn`.
  A weaker `ensures` clause still verifies, so this guard makes it a
  visible change to a second, generated file. A deleted lemma fails the
  same way. Do not edit the list by hand. Rewrite it with
  `UPDATE_VERUS_CONTRACTS=1 cargo test -p autumn-harvest --test integration verus_proofs_coverage::every_verus_contract_matches_the_stored_list`.
- `formal/verus/src/lib.rs` does not declare a module, so Verus never
  checks it.
- A Verus file sits below `formal/verus/src`, or a proof file loads
  another file with `mod <name>;` or `include!`. No guard would read that
  file.
- `ci.yml` does not run the command on every PR.
- This page does not name a Verus file or a mirrored function.

A Verus loop states its `invariant`, `invariant_except_break`, `ensures`
and `decreases` clauses in its header, before its body. The guard drops
each clause from its keyword to the `{` of the loop body, so a mirror can
hold a loop (issue #2330). Verus erases a clause, as it erases a
`proof { }` block. The guard drops a clause only inside a `loop` or
`while` header. A clause keyword anywhere else stays, so a Verus copy
cannot hide code behind it. An engine body that holds a clause keyword
fails the guard, so the drop changes only the Verus copy.

A change to a mirrored function therefore fails the no-DB suite until the
copy matches. Port the change, then run `cargo xtask check verus-proofs`.

### The proofs

| File | Mirrors | Property |
|---|---|---|
| `mixers.rs` | `mix64` in `policy.rs` and `dlq.rs`, `splitmix64` in `chaos.rs` | Each mixer is a bijection on `u64`. An explicit inverse undoes it. |
| `jitter.rs` | `uniform_inclusive`, `full_jitter_nanos`, `equal_jitter_nanos` in `policy.rs` | The jitter stays in its range for every input, with the real mixer. |
| `dlq_spread.rs` | `spread_offset_nanos` in `dlq.rs` | No step overflows. The jitter is exact and below one slot. Row `index` of `count` stays in its slot and in the window. |
| `owner_fence.rs` | `next_epoch` and `fence_passes` in `decision_owner.rs` | The next epoch is the epoch + 1 and never wraps. The fence passes a commit only at the epoch of the row, so never at an older one. Only an acquisition moves the epoch, so after a takeover the fence refuses every commit of the old tenure (issue #2251). A batch acquisition moves each locked row by one acquisition and leaves each other row (issue #2296). |
| `group_commit.rs` | `accepts`, `share` and `acknowledges` in `group_commit.rs` | A leader takes between 1 and `max_batch` entries, and below `max_batch` the `width` leaders cover the queue. A group commit batch starts with its leader, holds at most `max_batch` entries, keeps one tenure and holds one entry per execution. An ordinary failure of another stage cannot change one decision's result. A fence error or a failed `COMMIT` fails every decision, and no result goes out before the commit (issue #2256). |
| `agent_inbox.rs` | `admit` in `agent/inbox.rs` of the agent crate | A control op always applies. Work is never skipped, and runs only when the inbox is open. A tick is never held. Under any sequence of ops, the ops that ran and then the held ops are exactly the work ops received, in arrival order. So an acknowledged op is never lost, and none runs twice (issue #2263). |
| `agent_dedupe.rs` | `is_new` and `next_floor` in `agent/dedupe.rs` of the agent crate | The floor and the set above it hold exactly the applied numbers. An apply and its fold equal the plain set insert, in any order and with any repeats. So the receiver takes no new message for a duplicate. The floor never moves over a gap, and the bound on the set forgets only an applied number (issue #2269). |
| `agent_wake.rs` | `next_heartbeat` in `agent/wake.rs` of the agent crate | With no heartbeat, the function books nothing. A tick books the next tick strictly after the time that it ran, so a tick never runs in a loop. Any other op keeps the booked tick, so messages never starve the heartbeat (issue #2266). |
| `agent_siblings.rs` | `sibling_cap` in `bounds.rs` of the agent crate | No step overflows. Each share is the base share or one more. The shares of all siblings sum to the remaining tokens, so concurrent children never pass the cap of the parent (issue #2270). |
| `agent_fan_out.rs` | `can_join` and `fan_out_plan` in `fan_out_plan.rs` of the agent crate | No step overflows. A bound under 2 names no reason. With a bound of 2 or more, each delegation that can run leads a batch, joins one, or names why it ran alone. Each reason is true of the turn. A batch holds from 2 to `bound` calls that can join, and each member follows its lead. A batch stops short of `bound` only at the end of a run, so `BoundReached` follows a full batch. The proptests `the_plan_batches_as_before` and `a_round_plan_batches_as_before` check that the batches did not change (issue #2483). |
| `agent_reset.rs` | `reaches_percent`, `reset_due` and `full_round_resets` in `bounds.rs` of the agent crate | No step overflows. A zero cap never resets. A request over the cap reaches the threshold for every percent, so the check before a request finds a reset due while one is left. A request under the threshold fits the cap. A tool round that passes the cap ends the run before that check. No sequence of requests takes the reset count past `max_resets` (issue #2453). |
| `agent_episodes.rs` | `episode_evictions` and `consolidation_due` in `bounds.rs` of the agent crate | No step overflows. An append never passes `max_episodes`, and it evicts only stored episodes. A log in its bound loses at most one episode per append. No sequence of appends takes the log past the bound. A pass is due only at or over the trigger, a due pass stays due as appends grow, and a trigger of 0 never fires (issue #2454). |
| `agent_dispatch.rs` | `dispatch_passes` in `bounds.rs` of the agent crate | No step overflows. The bound of a firing is 1 to 256. A firing never starts more passes than the bound or the due agents. A firing with a due agent starts at least one pass. When the due agents fit the bound, none waits. With no new due agent, the queue drains within the firings that the bound gives (issue #2540). |
| `agent_overlay.rs` | `merged_bound` and `merged_read_only` in `agent/overlay.rs` of the agent crate | A spec of `apply` uses the two kernels. No result bound is above its ceiling. A result clears `read_only` only when the ceilings allow it. Its model is in the ceilings. The registry and the profile mandate hold an overlay model. Under `Ceilings::from_profile`, no bound rises, `read_only` stays set and the model does not change. A second apply changes no modelled field. `apply` refuses exactly when a rule fails. The model carries each field that an overlay cannot name unchanged, and leaves out the system prompt. The proptests in `overlay.rs` check both on the engine `apply` (issue #2370). `apply` refuses exactly when a merged guidance key names no registered tool. Guidance changes only the description of an app tool, so its name and effect stay, and a built-in keeps its text. That spec has no mirror. `tests/skills.rs` checks the engine (issue #2373). |
| `overlay_log.rs` | `next_overlay_version` and `decide_activate` in `overlay_log.rs` | A spec `Log` models the versions and the active pointer of one kind. A put returns a version above each stored version, and keeps each stored body. The pointer is unset or names a stored version. No sequence of calls rewrites a stored body. A refused activation changes nothing. Of two activations that read the same pointer, exactly one moves it. The shared overlay store suite checks the SQL of each store (issue #2371). Issue #2374 adds `logs_activation`, `rollback_target` and `decide_rollback`. A spec `Heads` models the pointer and its log of moves. An activation and a rollback keep the chain of moves. A rollback with no earlier activation changes nothing. A rollback restores the version before the current one, and a second rollback undoes it. Of two rollbacks that read the same move, only the first moves the pointer. A rollback swap over a stale read changes nothing, also when the pointer moved away and back. Issue #2394 adds `decide_auto_move`, the cap check that the stores run under the head lock. A stale pointer always conflicts. A spec `spec_auto_day` folds the kernel over the attempts of one UTC day, and the count of auto moves never passes the cap. `decide_guarded` checks the log head that the caller read. A guarded move after any later move never activates, also when the pointer came back. The gate of a refine run uses it. |
| `agent_propose.rs` | `next_step` in `refine/propose.rs` of the agent crate | A spec `run` drives the stage loop over any sequence of verdicts. A stage uses from 1 to `max` attempts and never ends on a retry. It falls back exactly when each of its `max` attempts is invalid. A stage that does not fall back ends at its first valid reply, so the proposer is never asked after a valid reply. A `Same` reply keeps the base, and a `Differs` reply proposes. The proptest `a_stage_is_bounded_and_stops_at_the_first_valid_reply` checks the engine loop (issue #2379). |
| `agent_refine.rs` | `identity_holds` in `refine/ledger.rs` of the agent crate | A spec `count` runs one refine plan over any sequence of stage outcomes. Both modes plan 3k stages for each source. Each plan meets the identity: 3k sessions, and one probe for each session that is not a skip. One probe fewer or one more breaks it. A run one session short breaks it. The run plays one plan for each source, so the proof covers each source count. The proptest `each_plan_meets_the_identity` checks the same counts on a model of the plan (issue #2380). |
| `agent_sampling.rs` | `draw_plan`, `wave_end`, `sample_step`, `cascade_step`, `answer_retry_cap`, `retry_after_reject`, `beam_draws` and `beam_slots` in `sampling/admit.rs` of the agent crate, and `next_policy_revision` in `agent_policy_log.rs` | An admitted strategy run has from 1 to `max_draws` draws. Its concurrency is from 1 to the cap of the policy, to `MAX_PARALLEL_CHILDREN` and to the draw count. Each wave holds from 1 to `concurrency` draws, and the waves cover each draw once, so no more than `concurrency` draws run at one time. A policy revision moves up by one and never wraps. The proptest `an_admission_never_passes_a_cap` checks the engine kernels (issue #2439). A sample wave holds from 1 to `concurrency` draws, never passes the planned draws, and never holds more draws than tokens are left. So each draw gets a share of at least one token, and the shares sum to the tokens left. With no token left and draws left, the step is `BudgetExceeded`. The proptest `a_sample_step_never_passes_a_bound` checks the engine kernel (issue #2443). An answer check never has more retries than the policy cap or `MAX_ANSWER_RETRIES`. In a spec model of the loop, a run with the cap `c` asks for exactly `c + 1` answers when each answer is rejected. The proptest `the_answers_of_a_run_never_pass_the_cap` links the model to the engine kernels (issue #2442). A beam run admits `width × expansion × max_rounds` draws, saturated at `u32::MAX`, and a product of 0 only when a factor is 0. A round keeps at most `width` less the complete beams. In a spec model, a run with at most `width` live beams per round never runs more steps than that product. Verus takes that bound on live beams as a precondition. The unit tests of `beam_rank` and `keep_beams` check that a round keeps no more, and the proptest `a_model_beam_never_runs_more_steps_than_its_draws` checks the engine kernels (issue #2446). A cascade step returns the first tier at or after `next` that the mandate allows, and skips each tier between them. With no token left and a tier left, the step is `BudgetExceeded`. In `spec_ran`, a spec model of the run, each tier runs at most once and in order, and each tier that runs is one that the mandate allows. `cascade_order`, the loop of the engine over `cascade_step`, asks exactly the tiers of `spec_ran`. The proptest `a_cascade_step_never_runs_a_forbidden_tier` checks the engine kernel (issue #2445). |
| `agent_mandate_draws.rs` | `meet_cap`, `draw_verdict` and `delegated_verdict` in `mesh.rs` of the agent crate | The meet of two draw caps is never above a cap of either side. It has no cap only when neither side has one, so a chain of meets only narrows. An admitted strategy run passes no cap, and its draws and the delegations left sum to the room of the tree. A refusal names the cap first, then the room. A `delegate` call with no cap gets one draw at most. An admitted call takes one unit for the delegation. It takes one more for each draw that counts against the fan-out. Its draws fit each cap above the caller. The proptests `delegate_never_widens_max_draws`, `admitted_draws_fit_the_cap_and_the_fan_out` and `a_delegated_strategy_never_passes_a_cap_above_it` check the engine (issues #2449 and #2610). |
| `agent_vote.rs` | `key_votes`, `pick_key`, `vote_winner`, `runner_up` and `vote_settled` in `sampling/vote.rs` of the agent crate | A vote is a draw id and an optional key index. The spec `votes_of` gives the votes of a key, and `first_of` its lowest draw id. `vote_winner` folds `pick_key` over the keys. The winner has the most votes, and the lowest first draw on a tie. With no vote, there is no winner. When `vote_settled` holds, the leader has more votes than the runner-up and the draws left together. Then each extension by at most the draws left keeps the winner, new keys included. With later draw ids, it also keeps the selected first draw. So an early stop gives the result of the full run, for each way the left draws vote. Two vote sequences with the same multiset give the same winner. With one vote per draw, a renamed and reordered sequence gives the renamed winner. So the key numbering of another cast order does not change the result. The proptests `an_early_stop_never_changes_the_winner` and `the_order_of_the_votes_does_not_change_the_tally` check the engine with key texts. |
| `agent_sweep.rs` | `sweep_plan` in `sampling/admit.rs` of the agent crate | A sweep refuses an item concurrency over the cap of its policy. The items and the draws of each item at the same time are each at least 1, and their product is never above the cap. So the open draws of a sweep never pass the policy cap. An ask of `n` items runs `n` items at the same time. The proptest `a_sweep_plan_never_passes_the_cap` checks the engine kernel (issue #2448). |
| `agent_select.rs` | `pick`, `best_draw` and `key_of_bits` in `score.rs` of the agent crate | The order key keeps the sign-magnitude order of the bits of an `f64`, so equal keys mean equal bits. A spec `chosen` folds `pick` over a sequence of entries, each a draw id and an order key or a reject. The winner is an entry, has the top key, and has the lowest draw id on a tie. The result is `None` exactly when each entry is a reject, so a reject never wins while a key exists. Two inputs with the same entries give the same result, so a permutation does not change it. IEEE 754 orders finite floats by those bits, except `-0.0`, which `FiniteF64` never holds. The proptest `the_order_key_matches_the_float_order` checks that step. `select_does_not_depend_on_the_order` checks the engine with floats (issue #2441). |
| `evidence_parts.rs` | `part_end` in `eval/evidence.rs` of the agent crate | A spec `tile` runs the split of an evidence file over any choice of cuts. A text that fits the cap is one part. There is no part of an empty text. Each part is not empty and holds at most the cap. Each part starts where the last one ended, and the last part ends at the end of the text. So the parts joined in order are the whole text, and the split cuts nothing. The proptest `the_parts_join_to_the_text_and_fit_the_cap` checks the engine split (issue #2377). |
| `agent_profile_read.rs` | `op_profile` in `agent/profile_read.rs` of the agent crate | With no active version, an op runs on the base profile with no version. With an active version that applies, it runs on the applied profile and records that version. With an active version that does not apply, it fails and names the version. So an active version never yields the base profile. The proptest `the_kernel_never_runs_bare_under_a_version` checks the engine kernel, and `tests/overlay_read_suite` checks the op read on SQLite and Postgres (issue #2372). |
| `agent_anonymise.rs` | `shared_digits` and `id_width` in `eval/anon.rs` of the agent crate | `shared_digits` returns the common prefix length of two keys. `id_width` returns a width from the floor that is above each shared count below the cap. No smaller width from the floor is above each count. Two keys that share fewer digits than the width have different prefixes of that width, so no anonymous id repeats. The proptests `distinct_keys_give_distinct_ids` and `the_input_order_changes_no_slot` check the ids (issues #2376 and #2377). |
| `refine_policy.rs` | `parts_allowed`, `decide_adoption` and `gate_timed_out` in `refine/policy.rs` of the agent crate | Each overlay part is one bit. An adoption needs a change, each changed part allowed, and room under the daily cap. The default policy allows no part, so it never adopts. An empty change is `Unchanged`. A larger allowed set keeps each adoption. A spec `spec_day` folds the kernel over the changes of one UTC day, and the count never passes the cap. The stores enforce the cap with `decide_auto_move`, which `overlay_log.rs` proves. A gate times out exactly when `now` reaches `requested + timeout`, and a timeout is final. The proptest `the_kernel_matches_the_oracle` checks the engine kernel, and the store suite checks the cap under the lock on each backend (issue #2394). |
| `refine_gate.rs` | `gate_step` in `refine/gate.rs` of the agent crate | The adoption gate of a refine run maps each event to its next step. Only an activation adopts. A refused or unreadable decision keeps the wait. A spec `spec_gate` folds the kernel over the events of one gate. A gate adopts only when an activation comes before every other end, so a gate with no activation never adopts. An end is final. A deadline ends the gate as `TimedOut`. The proptests `only_an_activation_adopts` and `an_end_is_final` check the engine kernel, and `tests/refine_gate.rs` checks the gate on SQLite. |
| `refine_lessons.rs` | `evictions` and `writes_lesson` in `refine/lessons.rs` of the agent crate | A write removes only stored entries, and the rest is at most the bound. A write in the bound removes nothing. A new lesson never evicts itself. A raised bound removes no more entries than a lower one. Only `adopted` and `auto_adopted` write a lesson, so a rejected, timed-out or superseded run writes none. The proptests and unit tests in `refine/lessons.rs` check the engine kernels, and `tests/refine_lessons.rs` checks the library on SQLite. |
| `scan_skip.rs` | `first_live` in `activity_scan_index.rs` | The query returns the first live entry at or after its start, or the end slot. It never skips a live entry. Each call keeps the skip links sound. A new list, an added entry and more dead entries keep them sound too. So a skip-list query finds the same entry as a plain walk of that list (issue #2330). The proptest `the_indexed_scan_matches_the_oracle` checks the role of each event. |
| `process_role.rs` | `role_default`, `switch_value`, `contradicts_role`, `resolve_switch`, `retention_placed` and `runs_retention` in `process_role.rs` of the plugin crate | A `web` role with no explicit value runs no worker and no scheduler. A `worker` or `combined` role runs both. No role keeps the old default, `true`, so a standalone runner keeps both. An explicit value always wins. The startup warning fires exactly when the resolved value differs from the default of a known role. With no role, nothing warns. `resolve_switch` joins the kernels with the right arguments. The unit tests in `process_role.rs` check the engine log lines (issue #2477). With `run_on = "scheduler"`, retention runs exactly where the resolved scheduler runs, so a `web` role with no explicit value runs none. Each other role, and no role, runs it. An explicit scheduler value moves retention with it. With `run_on = "all"`, enabled retention runs on each process. Retention that is off never runs. The DB test `retention_follows_the_scheduler_placement` checks the sweep on each placement (issue #2478). |
| `worker_queues.rs` | `queue_source` and `pin_misses` in `worker_queues.rs` of the plugin crate | An explicit queue list always wins. A role pin applies only with no explicit list, and only when it names a declared queue. Otherwise the worker polls each declared queue, as before. The warning for a pin fires only when the pin changes nothing, and never with no pin or with an explicit list. The proptests in `worker_queues.rs` check that a resolved list is a non-empty subset of the declared list, in code order (issue #2479). |
| `depth_cap.rs` | `add_bounded_depth` in `queue.rs` | Each call is one `step`, stays at or below the cap and never overflows. The sampler adds one bounded depth per shard. The fold over any number of shards equals the capped sum of the true counts. So the `harvest.queue.depth` gauge is exact below the cap and reads the cap at or above it (issue #2356). The DB test `bounded_depths_match_the_exact_count_up_to_the_cap` checks that the SQL returns `min(count, cap)` per shard. |

**The mixer inverse.** Each step of the `SplitMix64` finalizer has an
inverse. A right xor-shift by `s` bits is undone by shifts of `s` and
`2s` bits. Each odd multiplier has an inverse modulo 2^64. Bit-vector
lemmas prove the shifts. Integer lemmas prove the multiplies. The Kani
jitter proofs stub the mixer with any `u64`, and the bijection shows
that the stub loses no case.

**The agent kernels.** The durable agent of epic #2262 keeps its
decisions in pure kernels, so Verus proves them as it proves the engine
kernels. Each agent spec models the state that its kernel acts on. The
inbox is a status, a held queue and a run list. The applied set is a plain
set of numbers. The theorems hold for every sequence of ops. The
inbox model has no bound on the held queue. A full queue refuses an op and
names it in the agent state.

**The heartbeat saturation.** Verus has no spec of `i64::saturating_add`,
and the guard bans `assume_specification`. `next_heartbeat` therefore
saturates by hand. Its step is at least 1, so only the top bound applies,
and the result is the result of `saturating_add`. The unit test
`the_booking_saturates_and_the_step_is_at_least_one` pins the case.

**The jitter without a stub.** The Kani proofs of `uniform_inclusive`
stub the mixer. The Verus proofs call the verified copy of `mix64`
itself.

**The redrive spread overflow.** The Verus proof found a defect in
`dlq::redrive_spread_offset` (issue #1832). It computed the jitter as
`(slot * mix64(seed)) >> 64` in `u128`. For a window over about 2^34
seconds, the product passed `u128::MAX`. A debug build panicked. A
release build wrapped, and the row could leave its slot. The callers cap
the window at `MAX_REDRIVE_SPREAD_SECS`, so no production path reached
it, but the function is public. `spread_offset_nanos` now splits `slot`
at bit 64. The proof shows that the split gives the exact jitter, and
that no step overflows. The unit test
`spread_offset_does_not_overflow_for_a_long_window` pins the case.

### Add a Verus proof

1. Pick an integer kernel with no I/O. A Verus proof cannot cover a
   `Duration`, a `String` or a float. Extract an integer kernel first, as
   `spread_offset_nanos` does.
2. Copy the function into a file in `formal/verus/src/`. Put a
   `// mirror: <path> <fn>` line above it, and declare the module in
   `lib.rs`.
3. Write the property as an `ensures` clause. Put proof hints in
   `proof { }` blocks, or in separate lemmas.
4. Name the Verus file in a comment on the engine function.
5. Run `cargo xtask check verus-proofs`, then the guard suite.
6. Add a row to the table above.

## Not modelled yet

Issue #1819 lists more work than its acceptance criteria require:

- **Model (d), shard-generation fencing and rebalance.** It needs its own
  model of `replication::assert_fence` and the rebalance cutover.
- **Trace conformance of `CodecRotation`.** Issue #2003 checks chaos traces
  against `ActivityClaim` and `WorkflowTaskClaim` only.

Until then, a change to a modelled protocol must update its model in the
same PR.
