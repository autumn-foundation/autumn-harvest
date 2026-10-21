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
scripts/check-formal-models.sh
```

The script downloads `tla2tools.jar` 1.7.4 and checks its SHA-256. To use a
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

The `formal-models` job in `.github/workflows/ci.yml` runs the script. Like
`lint`, it skips draft PRs and docs-only changes.

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
| `OwnerLease` | Fenced owner lease of a decision partition (issue #2250) | design, six defects, reachability |

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

| Action | Planned code |
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
scripts/check-formal-traces.sh formal/tla/trace/fixtures
```

`scripts/check-formal-traces.sh` needs Java 11 or later and Python 3. It
pins the same `tla2tools.jar` as `scripts/check-formal-models.sh`, and it
reads `TLA2TOOLS_JAR` in the same way. The fixtures take about 10 seconds.

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

`scripts/formal_traces.py` writes a TLA+ module for each check. The module
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
`scripts/check-formal-traces.sh "$HARVEST_TLA_TRACE_DIR"`. The runner checks
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
scripts/check-formal-traces.sh "$HARVEST_TLA_TRACE_DIR"
```

`formal_trace_coverage.rs` fails in these cases:

- A fixture is malformed.
- A spec has no clean fixture, or no red fixture that only the fence rejects.
- A trace spec does not extend its model.
- The two runners pin different TLC releases.
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
scripts/check-kani-proofs.sh
```

The script runs
`cargo kani -p autumn-harvest --no-default-features --features chaos -Z stubbing`.
`chaos` turns on the chaos controller, which holds a proof. `-Z stubbing`
enables `#[kani::stub]`.

`cargo kani` exits 0 when it finds no proof, and when a `kani::cover!` is
unsatisfiable. The script fails in both cases. It also fails when Kani
verifies fewer proofs than the source holds.

To run one proof, call `cargo kani` with the same flags and
`--harness <name>`. The `kani` job in `.github/workflows/ci.yml` runs the
script. It skips draft PRs and docs-only changes, like `lint`. The compile
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
   exclude every input passes for the wrong reason. The script fails on an
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
scripts/check-verus-proofs.sh
```

The script downloads the pinned Verus release and checks its SHA-256. The pin
is a weekly release, because Verus deletes the assets of older rolling builds. To
use a local binary, set `VERUS=/path/to/verus`. The release names the Rust
toolchain that it links against, and the script installs that toolchain
with rustup. The proofs take about 2 seconds. The `verus` job in
`.github/workflows/ci.yml` runs the script. It skips draft PRs and
docs-only changes, like `lint`.

The script fails when Verus reports an error, or when it verifies nothing.

### Mirrors

Verus cannot compile the engine crate, so each proof checks a copy of an
engine function in `formal/verus/src/`. A `// mirror: <path> <fn>` line
marks each copy. The guard suite
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
- `ci.yml` does not run the script on every PR.
- This page does not name a Verus file or a mirrored function.

A change to a mirrored function therefore fails the no-DB suite until the
copy matches. Port the change, then run the script.

### The proofs

| File | Mirrors | Property |
|---|---|---|
| `mixers.rs` | `mix64` in `policy.rs` and `dlq.rs`, `splitmix64` in `chaos.rs` | Each mixer is a bijection on `u64`. An explicit inverse undoes it. |
| `jitter.rs` | `uniform_inclusive`, `full_jitter_nanos`, `equal_jitter_nanos` in `policy.rs` | The jitter stays in its range for every input, with the real mixer. |
| `dlq_spread.rs` | `spread_offset_nanos` in `dlq.rs` | No step overflows. The jitter is exact and below one slot. Row `index` of `count` stays in its slot and in the window. |
| `owner_fence.rs` | `next_epoch` and `fence_passes` in `decision_owner.rs` | The next epoch is the epoch + 1 and never wraps. The fence passes a commit only at the epoch of the row, so never at an older one. Only an acquisition moves the epoch, so after a takeover the fence refuses every commit of the old tenure (issue #2251). |

**The mixer inverse.** Each step of the `SplitMix64` finalizer has an
inverse. A right xor-shift by `s` bits is undone by shifts of `s` and
`2s` bits. Each odd multiplier has an inverse modulo 2^64. Bit-vector
lemmas prove the shifts. Integer lemmas prove the multiplies. The Kani
jitter proofs stub the mixer with any `u64`, and the bijection shows
that the stub loses no case.

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
5. Run the script, then the guard suite.
6. Add a row to the table above.

## Not modelled yet

Issue #1819 lists more work than its acceptance criteria require:

- **Model (d), shard-generation fencing and rebalance.** It needs its own
  model of `replication::assert_fence` and the rebalance cutover.
- **Trace conformance of `CodecRotation`.** Issue #2003 checks chaos traces
  against `ActivityClaim` and `WorkflowTaskClaim` only.

Until then, a change to a modelled protocol must update its model in the
same PR.
