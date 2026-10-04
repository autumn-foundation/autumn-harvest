# Formal methods: TLA+ models and Kani proofs

Issue #1819 adds two tools:

- **TLA+ and TLC** check the design of the core protocols. TLC explores every
  interleaving of a small, bounded model and checks safety invariants in each
  state.
- **Kani** proves bounds on pure Rust kernels. It checks every input of the
  real function, not a sample.

Neither tool replaces the Docker-backed integration tests. A model checks the
design. The integration tests check that the SQL implements it.
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
| `WorkflowTaskClaim` | Workflow-task terminal-write ownership (issues #1184, #1806) | fixed, pre-fix, reachability, capability-miss gap and fix |
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
pause and capability-miss guards in the code. Two facts make it equivalent
here. The pause releases run in the claim's own transaction. The
capability-miss release runs only on a worker without the handler, which
never starts the activity.

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

**Open gap: the capability-miss release.** A cycle that started its
handler can release its claim through
`release_task_for_capability_miss_query` (phases `DuringHandler` and
`AfterHandler`). That guard checks `worker_id` and `crash_strikes`, but
not `attempt`. `CapMissGuard` selects the guard:

- `WorkflowTaskClaimCapMissGap.cfg` uses the current guard. TLC violates
  `OwnerWritesByCurrentClaim`: after a stuck requeue and a re-claim by the
  same worker, the stale cycle re-pends the live claim. The terminal write
  stays safe, because its own guard checks `attempt`.
- `WorkflowTaskClaimCapMissFix.cfg` adds the `attempt` term. It passes.

The fixed config keeps this action off, so it checks the guards that #1806
fixed.

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
- **Trace conformance.** Check chaos and integration test traces against
  the models, so the models do not drift from the code.

Until then, a change to a modelled protocol must update its model in the
same PR.
