# Formal methods: TLA+ models and Kani proofs

Issue #1819 adds two tools:

- **TLA+ and TLC** check the design of the core protocols. TLC explores every
  interleaving of a small, bounded model and checks safety invariants in each
  state.
- **Kani** proves bounds on pure Rust kernels. It checks every input of the
  real function, not a sample.

Neither tool replaces the Docker-backed integration tests. A model checks the
design. The integration tests check that the SQL implements it.
[`simulation.md`](simulation.md) covers a seeded harness that checks the
`ActivityClaim` invariants on an oracle. A differential test checks the
oracle against Postgres.
[`concurrency-model-checking.md`](concurrency-model-checking.md) covers loom
and Shuttle, which check in-process concurrency.

## Run the TLA+ models

The runner needs Java 11 or later.

```bash
scripts/check-formal-models.sh
```

The script downloads `tla2tools.jar` 1.7.4 and checks its SHA-256. To use a
local jar, set `TLA2TOOLS_JAR=/path/to/tla2tools.jar`. A full run takes about
40 seconds on 4 cores.

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
term. It subtracts 1 from `attempt` and keeps `crash_strikes`. The trace
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
steps only. The recorder is in
`autumn-harvest/tests/integration/chaos_tests/tla_trace.rs`.

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
| `heartbeat` | It changes only `last_heartbeat_at` of a running activity. |

The exporter maps the engine onto the spec in three places:

- A parked workflow task is `RUNNING` with no `worker_id`. No claim holds
  it, so the trace logs it as `PENDING`.
- `ActivityCompleted` and `ActivityCompletedExternally` are always
  terminal. `ActivityFailed`, `ActivityTimedOut` and
  `ActivityFailedExternally` are terminal only when the row becomes
  terminal in the same transaction. Otherwise a retry follows.
- Transactions are in the order of their last log id. Row locks serialize
  the writes of one task row, so this order is the commit order.

**The writer.** A test can open a connection with `tla_trace::actor_url`.
The URL sets `harvest.trace_actor` to `<task>/<worker>/<attempt>`, and the
trigger copies it. A line that names a claim of its own row must be a new
claim, or an owner action of the named claim. A system action, such as an
orphan reclaim, cannot explain it. A line with no name can be explained by
any action. Production code does not set the name.

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
  `"fixed": "reject"` and `"pre-fix": "accept"`. The second check proves
  that the fence causes the rejection, not a malformed trace.
- A **forged** red trace breaks the protocol under each guard, for example
  with a second terminal event. Its header expects `reject` from both.

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

- Only a test can name the writer. A real `Worker` does not
  name its claims, so its lines can be explained by any claim.
- An unobserved action changes no logged column. `TraceNext` omits it. Such
  an action only drops a claim, so a match never needs it.
- A recording run leaves the triggers on its database. A shared
  `HARVEST_TEST_DATABASE_URL` keeps them, and `partition` then refuses to
  convert `harvest_events`. Drop the triggers before other tests use it, or
  use a new database.
- A trace starts at the row's insert. A row from before the recorder was
  installed fails the export.
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
- **The `splitmix64` bijection is not proved.** CBMC did not finish in
  5 minutes. The chaos proof holds for every stream instead, so it holds for
  any mixer.

Two kernels that issue #1819 names have no Kani proof:

- **`lifecycle::TRANSITIONS`.** The domain is 10 × 10 state pairs, so a
  unit test is exhaustive and runs in microseconds. `lifecycle.rs` holds
  `migration_states_are_one_way` and `completed_and_terminated_never_reopen`.
- **`failure_signature`.** Kani is slow on UTF-8 string code. A proof of
  `truncate_chars` on 3-byte inputs did not finish in 5 minutes. Unit tests,
  proptests and the `fuzz_failure_signature` target cover it.

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

## Not modelled yet

Issue #1819 lists more work than its acceptance criteria require:

- **Model (d), shard-generation fencing and rebalance.** It needs its own
  model of `replication::assert_fence` and the rebalance cutover.
- **Trace conformance of `CodecRotation`.** Issue #2003 checks chaos traces
  against `ActivityClaim` and `WorkflowTaskClaim` only.

Until then, a change to a modelled protocol must update its model in the
same PR.
