# Formal methods: TLA+ models and Kani proofs

Issue #1819 adds two tools:

- **TLA+ and TLC** check the design of the core protocols. TLC explores every
  interleaving of a small model and checks safety invariants in each state.
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
15 seconds.

To run one config by hand:

```bash
cd formal/tla
java -cp tla2tools.jar tlc2.TLC -config ActivityClaimPreFix.cfg ActivityClaim.tla
```

The `formal-models` job in `.github/workflows/ci.yml` runs the script on
every PR.

### The manifest

`formal/tla/models.txt` lists every config. Each row has three columns:

| Column | Meaning |
|---|---|
| spec | The module, `formal/tla/<spec>.tla`. |
| config | The TLC config in `formal/tla/`. |
| expect | `pass`, or `violation:<Invariant>`. |

A `pass` row must finish with no error. A `violation` row is a
counter-example. TLC must exit with code 12 and name that invariant. A row
that passes when it must fail is an error too, so a counter-example cannot
rot.

The guard suite `autumn-harvest/tests/integration/formal_models_coverage.rs`
fails when a config has no row, when a row names a missing invariant, or
when this page does not name a spec.

### The models

| Spec | Protocol | Configs |
|---|---|---|
| `ActivityClaim` | Activity claim epoch (issue #1789) | fixed, pre-fix, reachability |
| `WorkflowTaskClaim` | Workflow-task terminal-write ownership (issues #1184, #1806) | fixed, pre-fix |
| `CodecRotation` | Codec re-encryption against PII erasure (issues #948, #495) | CAS, blind write |

#### `ActivityClaim` — model (a)

The prose spec is design decision 9 in
[`architecture.md`](../architecture.md#key-design-decisions). The model has
one task row and these actions:

| Action | Code |
|---|---|
| `Claim` | `queue::claim_task`. Sets `RUNNING` and `worker_id`, adds 1 to `attempt`. |
| `Start` | The start fence, `lock_claim_for_update`. |
| `SelfRelease` | A pause release, capability-miss release or rate-limit deferral. Subtracts 1 from `attempt`. |
| `Heartbeat` | `record_heartbeat`. A lost lease cancels the activity. |
| `OrphanReclaim` | `requeue_orphan`. Keeps `attempt`. Has no guard on the worker, because a live worker can look dead. |
| `RetryRequeue` | `requeue_claimed_task_for_retry`. |
| `Finish` | The finalize paths. Append the terminal event and the row state in one step. |
| `TimeoutFail` | The timeout sweeper. Not an owner, so not fenced. |

`Fenced = TRUE` gives every owner write the `claim_held` predicate.
`Fenced = FALSE` gives it only `state = 'RUNNING'`, as before #1789.

Invariants:

- `AtMostOneTerminal`: at most one terminal event.
- `TerminalStateHasOneEvent`: a terminal row state has exactly one event.
- `TerminalByCurrentClaim`: an owner's terminal event takes effect only while
  its claim is current.
- `HeartbeatByCurrentClaim`: a heartbeat on a running row comes from the
  claim that the row holds.
- `ClaimIdsAreUnique`: no two live claims share `(worker_id, attempt)`.

The fixed config checks 3 workers and 5 claims (163,362 states). It passes.

`ActivityClaimPreFix.cfg` reproduces the #1789 bug. TLC prints this trace:

1. `w2` claims the row: attempt 1.
2. `w2` starts the activity and heartbeats.
3. The orphan reclaimer requeues the row. `w2` is still alive.
4. `w1` claims the row: attempt 2.
5. `w2` finishes late. Its write matches `state = 'RUNNING'`, so its result
   wins. `TerminalByCurrentClaim` fails: the event is by `(w2, 1)` but the
   row holds `(w1, 2)`.

Before #1789 the code also breaks `HeartbeatByCurrentClaim` and
`ClaimIdsAreUnique`. A stale rate-limit deferral could lower `attempt` under
a later claim, so a claim could reuse a live pair. The pre-fix config checks
`TerminalByCurrentClaim` first, so its trace is the shortest one above.

`ActivityClaimReach.cfg` is a reachability witness. It must violate
`NoStaleOwnerAfterFinish`, which says that no stale owner still runs after
a later claim finished the task. The violation proves that the fixed model
reaches the #1789 race. Without it, a model that never reaches the race
would pass for the wrong reason.

#### `WorkflowTaskClaim` — model (b)

A decision cycle persists the terminal state of its run only while
`claim_still_held_for_update` holds. The stuck-running requeue keeps
`crash_strikes` and `attempt`. The orphan reclaim adds a strike. The
suspension release resets the strikes to 0.

`ChecksAttempt = FALSE` models the guard before #1806, which has no
`attempt` term. TLC violates `TerminalByCurrentClaim` with the trace from
the issue: `w2` claims, the stuck requeue frees the row, `w2` claims again,
and the stale cycle from the first claim closes the run.

#### `CodecRotation` — model (c)

The sweep reads a payload under the retired key and writes it under the
active key. Erasure writes a tombstone. `CLAUDE.md` names these two writers
as the only exceptions to the append-only `harvest_events` rule.

Invariants:

- `ErasureIsFinal`: an erased row stays erased.
- `PlaintextPreserved`: re-encryption never changes the plaintext.

`Cas = FALSE` models a blind write. TLC violates `ErasureIsFinal`: the sweep
reads a row, erasure tombstones it, and the sweep writes ciphertext over the
tombstone. The compare-and-swap in `compare_and_swap_event` prevents this.

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
2. Add a passing config. Keep the bounds small enough for a run under one
   minute.
3. Add a counter-example config that turns the fix off, and a reachability
   witness when the passing config could pass for the wrong reason.
4. Add a row for each config to `formal/tla/models.txt`.
5. Add a section to this page. The guard suite checks that it names the spec.

## Run the Kani proofs

Install Kani once:

```bash
cargo install --locked kani-verifier --version 0.68.0
cargo kani setup
```

Run every proof:

```bash
cargo kani -p autumn-harvest --no-default-features --features chaos -Z stubbing
```

`chaos` turns on the chaos controller, which holds the seeded-plan proof.
`-Z stubbing` enables `#[kani::stub]`. Run one proof with
`--harness <name>`. The `kani` job in `.github/workflows/ci.yml` runs every
proof on every PR.

### The proofs

Each proof is a `#[kani::proof]` function in a `#[cfg(kani)] mod
kani_proofs` beside the code that it proves.

| Proof | File | Property |
|---|---|---|
| `uniform_inclusive_stays_in_range` | `policy.rs` | The jitter draw stays in `[lo, hi]`. |
| `full_jitter_is_at_most_base` | `policy.rs` | Full jitter is at most its base. |
| `equal_jitter_stays_in_upper_half` | `policy.rs` | Equal jitter stays in `[base/2, base]`. |
| `retry_delay_never_exceeds_max_interval` | `policy.rs` | `compute_retry_delay` never panics and never exceeds `max_interval`. |
| `migration_states_are_one_way` | `lifecycle.rs` | No transition leaves `MIGRATED` or enters `MIGRATING`. No self-transition. |
| `closed_runs_reopen_only_by_dlq_redrive` | `lifecycle.rs` | The only exit from a closed state to an open state is `FAILED` to `RUNNING`. |
| `seeded_action_respects_caps_and_never_holds` | `chaos.rs` | A seeded plan picks only an allowed action, and never `Hold`, for every stream. |

### Scope of each proof

A proof is only as wide as its inputs. These limits are deliberate:

- **The jitter proofs stub `mix64`.** The stub returns any `u64`. The bound
  must hold for every mixed value, so this over-approximates the mixer and
  the proof stays sound. With the real mixer, CBMC did not finish in
  10 minutes.
- **The jitter proofs work in nanoseconds.** `full_jitter` and
  `equal_jitter` convert a `Duration` to nanoseconds and back. A proof
  through that round trip must relate a 128-bit multiply to a divide, and
  CBMC does not finish. The proofs cover `full_jitter_nanos`,
  `equal_jitter_nanos` and `uniform_inclusive`, which hold all of the
  arithmetic.
- **`retry_delay_never_exceeds_max_interval` assumes a coefficient that is
  not NaN and an initial interval above 0.** CBMC reports every NaN result
  as an error, but the code handles NaN on purpose. The unit test
  `compute_retry_delay_negative_nan` pins that case.
- **`failure_signature` has no proof.** Kani is slow on UTF-8 string code.
  A proof of `truncate_chars` on 3-byte inputs did not finish cleanly in
  5 minutes. The proptest suite and the `fuzz_failure_signature` target
  cover this function instead.
- **The `splitmix64` bijection is not proved.** CBMC did not finish in
  5 minutes. The chaos proof holds for every stream instead, so it holds for
  any mixer.

### Add a Kani proof

1. Pick a pure function with no I/O and a small input space, or bound the
   input with `kani::assume`.
2. Add a `#[kani::proof]` function to the file's `kani_proofs` module. Add
   the module, under `#[cfg(kani)]`, if the file has none.
3. Add `#[kani::unwind(N)]` to a proof with a loop. Kani reports an
   unwinding failure when `N` is too small.
4. Add `kani::cover!` for an interesting case. A proof whose assumptions
   exclude every input passes for the wrong reason, and the cover check
   catches that.
5. Run the proof alone. Keep it under one minute. A slow proof usually
   relates a multiply to a divide. Stub or split the function.
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
