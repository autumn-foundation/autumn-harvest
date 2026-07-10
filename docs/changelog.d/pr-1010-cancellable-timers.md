## Phase 3.50 — Cancellable / renewable durable timers (issue #768)

Author-controlled durable timers that can be **cancelled** and **reset** (re-armed) without leaving orphaned firings — the missing complement to the fire-once `ctx.timer`/`ctx.sleep_until` primitives, for idle-session timeouts and sliding-window debounces. New `WorkflowContext::start_timer(id, secs) -> TimerHandle<'_>` arms a durable timer via a **non-suspending bookkeeping command** (returns immediately, does not park the coroutine); `TimerHandle` exposes `cancel()`, `reset(secs)`, and `await_fire() -> HarvestResult<TimerOutcome>` (`TimerOutcome::{Fired, Cancelled}`). `ctx.cancel_timer(id)` / `ctx.reset_timer(id, secs)` are the untyped equivalents. `TimerHandle`/`TimerOutcome` re-exported from `lib.rs` and the prelude.

**Design (Option C — bookkeeping arm, non-suspending):** the worker has no mixed-batch suspension (`handle_suspended_workflow` only recognizes single-shape batches), so a suspending arm could never be cancelled in-flight. Arm/cancel/reset are modelled as **bookkeeping commands** (`WorkflowCommand::ArmTimer`/`CancelTimer`, no `result_tx`), exactly like `CancelRaceLosers`/`RecordMarker`. The durable side runs in **two phases** inside the persist transaction at every persist site (sibling of `apply_race_loser_cancellations`): `worker::plan_timer_lifecycle` performs the `harvest_timers` row mutations (`ArmTimer` upserts the row and resolves a `TimerStarted` only when newly created — dedup, incl. within one batch, mirrors `persist_started_timer`; `CancelTimer` deletes the pending `fired = false` row via `queue::delete_pending_timer` and resolves `TimerCancelled`) and returns the resolved events **aligned to command indices** plus the minimum armed `fires_at`; `worker::build_suspension_events` then **interleaves** those `TimerStarted`/`TimerCancelled` events into the single suspension event batch at their `ArmTimer`/`CancelTimer` command-emission positions — exactly like `ChildWorkflowSpawnedDetached` — so `replay`'s strictly positional `match_timer_arm` sees the same order the live cycle emitted. (An earlier revision appended timer-lifecycle events at the *end* of the batch, which nd-blocked a `start_timer` + `side_effect`/`new_uuid`/`system_now` same-cycle run on first resume, since `drain_early_signals` does not skip `SideEffectRecorded`/`MarkerRecorded` and the cursor landed on them ahead of the positional timer-arm match.) A bookkeeping-only batch that armed a timer reschedules the workflow task to the armed `fires_at` (via `persist_bookkeeping_and_requeue_workflow`) so the armed timer actually wakes the workflow — unless a wake raced the park (`had_wake_requested`), in which case the task is woken now rather than deferred to the deadline (a wake landing while the row is still claimed — the sliding-window "reset on each event" path — is otherwise dropped up to a full timer duration). `await_fire` observes the outcome by re-arming idempotently (the durable row is deduped by `timer_id`, so no duplicate `TimerStarted`) and parking (`park_until_dropped`); the armed timer's fire is surfaced at the next wake and the next cycle resolves `Fired`/`Cancelled` by recorded-history order. On a **terminal cycle** an `ArmTimer` **skips only the `harvest_timers` row insert** (a sealing execution never fires the timer, so the row would only leak) but **still emits the `TimerStarted` event** at the `ArmTimer` command's position (via the pure `worker::terminal_arm_timer_events`), so a `start_timer`/`reset` that arms a timer and then completes in the **same** task replays deterministically — `start_timer` runs the positional `match_timer_arm`, which would diverge strict replay if the terminal history omitted its `TimerStarted` (Codex P1; see the post-review section). A trailing `handle.cancel()` cleanup is still honoured.

**One new `WorkflowEvent` variant** — `TimerCancelled { timer_id }`, appended at the END of the enum; adjacently-tagged serde is automatic, and pre-#768 histories (which never contain the variant) deserialize unchanged. **No migration** — the `harvest_timers` row deletion is the fire-suppression mechanism, and `delete_pending_timer` (filtering `fired = false`) already existed and is race-safe; a fire that already committed makes the delete a no-op, and recorded-history order (`TimerFired` vs `TimerCancelled`) decides the observed outcome (AC3).

**Replay (`replay.rs`):** new `match_timer_arm(id, dur)` (positional, consumes `TimerStarted` like a marker; `Diverged`/`NoMatch`/`Matched`), `match_timer_cancel(id)` (forward-scan claim of `TimerCancelled`), and `match_timer_or_cancel(id) -> TimerFireMatch { Fired, Cancelled, NoMatch }` (forward-scan for the first of `TimerFired`/`TimerCancelled`, deterministic recorded-order resolution). `TimerCancelled` is a command-bearing event that can interleave into unrelated scans, so a **transparent, non-consuming** skip-arm was added to every forward scan that could step over it — `scan_activity_terminal` (match_activity; `TimerStarted` is tolerated there too, for symmetry with a `reset`'s `[TimerCancelled, TimerStarted]` pair), `match_timer_strict`, `match_signal_or_timer`, `is_timer_started_next`, `match_signal`, `match_external_cancel`, `peek_u64_marker` (race/session markers), and `match_saga_marker` — leaving the event claimable by its own matcher (this deliberately differs from the stash-and-consume external-cancel pattern, since timer cancels are re-scanned from history rather than stashed; `match_child_workflow`'s existing catch-all `_ => scan_cursor += 1` and `drain_early_signals`'s `_ => break` already handle it correctly and were left unchanged). `TimerFireMatch` re-exported from `lib.rs`.

**Non-determinism classification:** new `NonDeterminismKind::TimerCancelMismatch` (`testing.rs`) — an unconsumed `TimerCancelled` (a removed `cancel_timer`/`handle.cancel`/`handle.reset` call) is classified precisely (mirrors the version/patch-marker special-case: any `actual` starting with `TimerCancelled`, plus an explicit `"timer-cancel"` message prefix). A renamed timer id surfaces as the ordinary `TimerMismatch`.

**Test harness (`WorkflowTestEnv`):** `ArmTimer` records `TimerStarted` + defers a `TimerFired` (idempotent re-arm no-ops); `CancelTimer` records `TimerCancelled` and retain-filters the deferred fire — so both the fire and cancel branches are driven deterministically with no real sleep. The terminal-command path (`record_terminal_pending_commands`) records the same for a fire-and-forget cancel on a completing cycle.

**No worker-suspension-shape change** (arm/cancel are bookkeeping added to every `extract_*`/predicate ignore-list; `await_fire` parks with an idempotent re-arm, routed through the existing `only_bookkeeping_commands` → `persist_bookkeeping_and_requeue_workflow` path). Example `autumn-harvest/examples/cancellable_timer_sla.rs` (a fulfillment-SLA timer that is renewed on each extension and cancelled on early ship, with embedded `WorkflowTestEnv` + `replay_check` tests). **Composition note:** composing an armed handle with a signal wait in one `select!`-style call (idle-timeout that resets on the next event *signal*) is a documented follow-up — use `ctx.receive_signal_timeout` (#476) for the two-branch signal-or-deadline shape today.

**Post-review hardening (Codex review, PR #1010) — one P1 + three P2s, all in the same reset-interleaving / terminal-cycle family:**

- **P1 (`worker.rs`, terminal persist path):** the initial FINDING-6 cut passed `skip_arm_inserts = true` to `plan_timer_lifecycle`, which dropped the `ArmTimer` **entirely** — no row insert *and* no `TimerStarted` event. A workflow that armed a timer and completed in the same task (`ctx.start_timer("x", 300); Ok(())`) therefore committed a terminal history with **no** `TimerStarted`, and strict replay / `WorkflowReplayer` diverged the moment the re-executed `start_timer` ran the positional `match_timer_arm` against the trailing `WorkflowCompleted`. Fixed by splitting the two effects: the terminal path now skips only the **row insert** while still emitting the **event**. The emission decision (with `StartTimer`-owned + in-batch, cancel-aware `active`-set dedup that mirrors the non-terminal DB path — including a same-cycle `reset`'s `[ArmTimer, CancelTimer, ArmTimer]` re-emitting at both arms) is the pure, unit-tested `worker::terminal_arm_timer_events`.
- **P2 (`replay.rs`, `match_timer_strict`):** the `ctx.timer` fire scan skipped an interleaved `TimerCancelled` but **stopped** on the reset's paired `TimerStarted` (a sibling-branch `reset()` records `[TimerCancelled(idle), TimerStarted(idle)]` between this timer's `TimerStarted` and its `TimerFired`), returning `NoMatch` instead of reaching the fire — strict replay failed and a worker could re-park an already-fired timer. `TimerStarted` added to the transparent (non-consuming, rewind) skip branch.
- **P2 (`replay.rs`, `match_signal`):** a `wait_for_signal` polled before a same-cycle `reset()` branch saw `[TimerCancelled, TimerStarted, SignalReceived]`; the scan skipped the cancel but **stopped** on the re-arm and reported a missing signal. `TimerStarted` added to the transparent branch.
- **P2 (`reset.rs`, `apply_event_to_pending`, the #148/#366 reset-from-history validator):** the pending-side-effect scan inserted on `TimerStarted` and closed only on `TimerFired`, so any reset point after `cancel_timer()`/`reset()` left the cancelled arming counted as pending and reset validation rejected/mis-planned the fork. A `TimerCancelled` now closes the pending arm, exactly like `TimerFired`.

**Symmetry audit (every forward-scan loop in `replay.rs` transparent to an interleaved `TimerCancelled` must also be transparent to the reset's paired `TimerStarted`):** `scan_activity_terminal`, `peek_u64_marker`, `match_external_cancel`, and the `ctx.race()` scan already handled **both** (no change). `match_timer_strict` and `match_signal` handled the cancel only → fixed (above). `match_saga_marker` handled the cancel only → `TimerStarted` added. `scan_local_activity_terminal` (the direct sibling of `scan_activity_terminal`) handled **neither** timer event and `break`d on both → **both** `TimerStarted` and `TimerCancelled` added, closing the same class of bug for a reset interleaved with a *retrying local activity*. `match_timer_or_cancel` (`_ => scan += 1`, skips all) and `match_child_workflow` (catch-all `_ => scan_cursor += 1`) are already fully transparent. `is_timer_started_next` is a peek that legitimately **stops** at `TimerStarted` (its purpose) and is deliberately left as-is.

**Post-review hardening round 2 (Codex 3× P2, PR #1010):** three genuine correctness findings in the same cancellable-timer family, all fixed TDD red→green.

- **P2 (`context.rs`, `await_timer_fire`) — same-cycle cancel-then-await re-armed a cancelled timer.** A `let h = ctx.start_timer(id, secs); h.cancel()?; h.await_fire().await?` (or a replay of the equivalent history) hit `await_fire`'s `NoMatch` arm — `match_timer_or_cancel` finds no *unconsumed* resolving event, because on replay `cancel()` already consumed the recorded `TimerCancelled` — and **unconditionally pushed another `ArmTimer`**, re-creating the durable row the cancel had torn down. The worker then persisted `TimerStarted, TimerCancelled, TimerStarted` and the "cancelled" timer parked and later fired (AC2/AC3 violation). Fixed with a per-context logical timer-lifecycle map (`cancellable_timer_state: HashMap<timer_id, Armed|Cancelled>`): `start_timer`/`reset_timer` set `Armed`, `cancel_timer` sets `Cancelled` (driven purely by the workflow's own calls, so identical live and on replay). `await_timer_fire` still consults **recorded history first** (`match_timer_or_cancel`) — preserving the deterministic fire-vs-cancel race resolution unchanged — and only on `NoMatch` falls back to the logical state, resolving `Cancelled` without re-arming. A `reset()` between the cancel and the await flips the state back to `Armed`, so it waits/fires normally. Regression tests: 2 `context.rs` unit tests (live same-cycle cancel-then-await resolves `Cancelled` with **no** re-arm command; cancel-then-reset-then-await parks/re-arms), and 2 `WorkflowReplayer` determinism fixtures in `tests/integration/replayer_tests.rs` (`cancel_then_await_same_cycle_replays_succeeded` — the direct replay gate, since without the fix the re-arm suspends mid-replay and the run never resolves; `cancel_then_reset_then_await_replays_fired`).
- **P2 (`testing.rs`, `WorkflowTestEnv`) — an armed timer fired during a mixed non-timer suspension.** The harness turned *every* `ArmTimer` into a deferred `TimerFired`, but production only reschedules the armed timer to fire on the bookkeeping-only `await_fire` path; when the batch also waits on an activity/signal the real worker records the arm and leaves the workflow parked on that other wait. A `start_timer()` before an activity/signal therefore produced impossible history such as `[TimerStarted, ActivityScheduled, TimerFired, ActivityCompleted]`, causing false non-determinism or test branches that cannot occur in production. Fixed by gating the deferred `TimerFired` on a new `is_competing_suspension` check: the harness auto-fires an armed timer only when the batch carries no genuine non-timer suspension (`ScheduleActivity`/`WaitForActivity`/`ScheduleExternalActivity`/`WaitForSignal`/`StartChildWorkflow`/`StartTimer`); otherwise it records only the `TimerStarted` and leaves the timer parked, matching the worker. The timer resolves later on a subsequent bookkeeping-only `await_fire` cycle. Regression test: `test_armed_timer_does_not_fire_during_a_mixed_activity_suspension` (asserts the arm records `TimerStarted`, never fires before the activity completes, and `replay_check` succeeds).
- **P2 (`worker.rs`, history hard-cap preflight) — timer-lifecycle events uncounted.** `ArmTimer`/`CancelTimer` append durable `TimerStarted`/`TimerCancelled` events, but `pre_suspension_event_count` counted only markers, side effects, detached spawns, and race-loser events. Near `event_hard_cap`, a reset batch (`CancelTimer` + `ArmTimer`) was counted as ~one pending event, passed the `>= cap` check, then appended two — reaching/exceeding the hard cap instead of being DLQed first. Fixed with the new pure `worker::timer_lifecycle_event_count` (folded into `pre_suspension_event_count`): +1 per `CancelTimer`, +1 per `ArmTimer`, **excluding** an `ArmTimer` whose id also has a `StartTimer` in the batch (`plan_timer_lifecycle` skips it and its event is already counted by the `extract_started_timer_for_suspension` branch). Counted conservatively (the preflight runs before the DB-dependent row-existence dedup, so an idempotent re-arm that appends nothing is still counted +1 — over-counting only ever DLQs one event early, never masks an overflow). Regression tests: 3 pure `worker.rs` unit tests (reset batch counts 2 via both `timer_lifecycle_event_count` and `pre_suspension_event_count`; `StartTimer`-owned arm excluded while a different-id cancel still counts; non-timer batches unaffected).

Tests, TDD red→green→refactor (all **executed** green, no DB required): event.rs round-trip + pre-#768 back-compat unit tests; 7 context.rs unit tests (arm emits non-suspending command, cancel/reset command emission, `await_fire` fired/cancelled/fire-wins-over-late-cancel replay, live re-arm-and-park, empty-id panic); replay.rs matcher unit tests (arm/cancel/or-cancel resolution, fire-vs-cancel ordering both directions, interleaved-cancel skip in the activity/signal scans, interleaved-arm skip in the activity scan); 2 testing.rs classification unit tests; 2 pure `worker.rs` interleaving unit tests (`build_suspension_events` places `TimerStarted` at the `ArmTimer` position — before a same-batch `SideEffectRecorded` — and records a reset's `[TimerCancelled, TimerStarted]` in emission order); `WorkflowReplayer` integration fixtures in `tests/integration/replayer_tests.rs` — including the falsifiable success-metric bar `reset_1000_times_replays_with_bounded_history` (1000 GENUINELY DISTINCT fixtures over a wide prime reset range; every third also interleaves a same-cycle `new_uuid()` side effect exercising the FINDING-1 ordering path, seeded byte-unique; plain fixtures pin 2 events/reset, interleaved pin 3 events/reset; all `ReplaySucceeded`, all ≤1 `TimerFired`), the FINDING-1 pair `timer_arm_then_side_effect_correct_order_replays_succeeded` / `..._wrong_order_diverges`, plus `removed_cancel_surfaces_as_timer_cancel_mismatch` and `renamed_timer_surfaces_as_timer_mismatch`; 3 `WorkflowTestEnv` integration tests in `tests/integration/workflow_test_env_tests.rs` (fires-when-not-cancelled, no-fire-when-cancelled, reset-then-fire, all with `replay_check`); and the example's 3 embedded tests. **Post-review (Codex P1 + 3 P2s):** 4 pure `worker.rs` unit tests for `terminal_arm_timer_events` (arm-without-await still emits `TimerStarted`; `StartTimer`-owned id emits nothing; a same-cycle `reset`'s `[Arm, Cancel, Arm]` re-emits at both arms; a repeat arm without an intervening cancel dedups) — each confirmed **red** against a buggy event-dropping body before the fix; 2 `WorkflowReplayer` regression tests in `tests/integration/replayer_tests.rs` (a fixed-worker terminal history *with* `TimerStarted` replays clean; a buggy history *missing* it diverges — proving the event is load-bearing); 2 `replay.rs` matcher tests (`match_timer` reaches its fire past an interleaved `[TimerCancelled, TimerStarted]` reset with both left claimable; `match_signal` matches past the same interleaved reset) — both confirmed red against the pre-fix scans; 1 `reset.rs` test (a `TimerCancelled` resolves the pending arm so the boundary after it validates, while the boundary before it still rejects) — confirmed red before the `apply_event_to_pending` fix. Full no-DB unit + `--features testing --test integration` suites pass; `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings` clean.

**Post-review hardening round 3 (Codex 2× P2, PR #1010):** two more genuine correctness findings in the same cancellable-timer state-machine family, both fixed TDD red→green (RED confirmed as `NonDeterminismDetected(TimerMismatch)` / a "suspended with no resolvable commands" harness stall before each fix), and the full Armed/Cancelled logical-state matrix is now fixture-covered.

- **P2 (`context.rs`, `start_timer`) — a duplicate ACTIVE `start_timer` diverged on replay.** `start_timer` is documented idempotent for an already-armed id, and the live worker (`plan_timer_lifecycle`'s row dedup) records no second `TimerStarted`. But `start_timer` unconditionally ran the positional `match_timer_arm` on every call, so a workflow that called `start_timer("idle", 300)` twice with no intervening cancel/reset (a real pattern — e.g. a top-of-loop "ensure the idle timer is armed" line) diverged on replay: the second call's match landed on the *next real event* the single recorded `TimerStarted` was followed by (an `ActivityScheduled`, a `TimerFired`, …) and reported a `TimerMismatch` for history this worker itself wrote. Fixed by making a duplicate arm of an already-`Armed` id (per the round-2 per-context `cancellable_timer_state` map) a true idempotent no-op: it returns a fresh handle **without** re-running `match_timer_arm` and **without** emitting a second `ArmTimer` — mirroring the live worker's row-dedup so live and replay agree. A prior `cancel_timer` clears the `Armed` state and `reset_timer` re-sets it, so a genuine re-arm after a cancel/reset still matches/records. New helper `WorkflowContext::timer_logically_armed`. Regression tests: 3 `context.rs` unit tests (live duplicate arm emits exactly one `ArmTimer`; replay of a one-`TimerStarted` history with two `start_timer` calls emits no command and does not diverge against the trailing `TimerFired`; a re-arm after `cancel` still records a full `[Arm, Cancel, Arm]`) and the `WorkflowReplayer` fixture `duplicate_active_start_timer_replays_succeeded` (a two-`start_timer` workflow replayed against the fixed-worker one-`TimerStarted` history → `ReplaySucceeded`; RED before the fix was `NonDeterminismDetected(TimerMismatch, event_index=2)`).
- **P2 (`testing.rs`, `WorkflowTestEnv`) — an already-armed timer never fired on a later bookkeeping-only `await_fire`.** The round-2 fix correctly stopped the harness auto-firing an armed timer during a *mixed* (activity/signal/child) suspension, but left a gap: when a LATER bookkeeping-only `await_fire` batch re-armed a timer whose `TimerStarted` was recorded in an EARLIER mixed batch (so this batch emits no new `TimerStarted`), the `already_armed` guard returned a no-op — `process_suspension` then found no resolvable command and the run stalled with "workflow suspended with no resolvable commands". Production reuses the existing durable row and reschedules it to fire. Fixed by splitting the guard: an `ArmTimer` whose fire is already deferred *this* batch stays a no-op, but an `ArmTimer` for a timer already active in *history* now fires (defers a `TimerFired`) when the batch is bookkeeping-only, and only stays parked when a competing suspension shares the batch (the round-2 rule, preserved). Regression tests: the `WorkflowTestEnv` fixtures `start_timer_then_activity_then_await_fire_fires` (asserts production-shaped ordering — `TimerStarted` before `ActivityCompleted`, `TimerFired` strictly after it, plus a passing `replay_check`; RED before the fix was the stall) and `start_timer_then_activity_then_cancel_then_await_cancelled` (matrix #6 regression guard: arm → activity → cancel → await resolves `Cancelled`, never fires).

The per-context Armed/Cancelled logical-state map is now the single source of truth governing all four timer verbs (first-arm-matches, duplicate-active-arm-is-noop, cancel→Cancelled, reset→re-Armed, await consults state), and the full state matrix is fixture-covered: (1) arm→await→Fired, (2) arm→cancel→await→Cancelled, (3) arm→cancel→reset→await→Fired, (4) arm→arm[dup active]→await→Fired with one `TimerStarted`, (5) arm→activity→await→Fired with valid ordering, (6) arm→activity→cancel→await→Cancelled, (7) the reset-1000× bounded-history fixture. Full local gate green: `cargo fmt --all -- --check`; `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings` (0 warnings); `--features testing --test integration` (783 passed); `--no-default-features --lib` (1351 passed, the pre-existing unrelated `wait_for_signal_returns_nondeterministic_on_diverged_history` sandbox hang skipped by name); `--features testing --example cancellable_timer_sla` (3 passed); `cargo check --workspace`. The `--features db --lib` suite is compile-checked (`--no-run`; no Docker/Postgres in the sandbox).

**Post-review hardening round 4 (Codex 2× P2, PR #1010):** two more genuine correctness findings in the worker persist path, both fixed TDD red→green. The change also carries a deliberate **semantic clarification**: a cancellable timer's deadline is now measured from when it is **awaited** (`await_fire`), not from `start_timer` — arming records only the `TimerStarted` event and inserts no `harvest_timers` row; the row (fire-eligibility) is created at `await_fire`. This aligns production with the already-shipped harness/replayer behaviour and the documented "an armed timer is observed only at `await_fire()`" contract, and suits every intended idle-timeout/debounce/lease pattern (which always await or reset the timer). Docs (`docs/getting-started/03-durable-timers.md`, `examples/cancellable_timer_sla.rs`, the `start_timer`/`await_fire` rustdoc) updated; the old "arming then parking on an activity/signal risks a spurious fire" footgun is gone (it can no longer happen).

- **P2 (`worker.rs`, `plan_timer_lifecycle`) — an armed-but-unawaited timer fired spuriously and broke a later `reset()`/replay.** A cancellable timer's `harvest_timers` row was inserted at ARM time on whatever suspension persisted the batch (activity/signal/child/bookkeeping/terminal). Because `ingest_due_timers_and_signals` runs on EVERY workflow claim (not just timer waits), an armed timer whose deadline passed while the workflow was parked on an ACTIVITY got a `TimerFired` appended on the activity's wake — a spurious fire that leaves an unconsumed `TimerFired` (→ non-determinism the next time `reset()`/`await_fire()` scans) and diverges replay. Fixed by inserting the row **only** on the `await_fire` path: `WorkflowCommand::ArmTimer` gained a `for_await: bool` field — `start_timer`/`reset` push a fresh arm (`for_await: false`) that records `TimerStarted` (for positional replay) but inserts no row, while `await_timer_fire` pushes a re-arm (`for_await: true`) that inserts the durable row (`fires_at = db_now + duration`), reschedules the parked task, and records no event (the arm's `TimerStarted` was already recorded). This makes row insertion a pure function of the command's role rather than of which persist path runs, so a fresh arm never leaks a row on ANY path (including the terminal seal — the previous `skip_arm_inserts` flag is subsumed and removed), and it structurally avoids a double `TimerStarted` in the arm-then-later-await case (the flag-only "insert at await" design would have double-emitted). `plan_timer_lifecycle`'s emission logic for fresh arms is the pure, unit-tested `arm_timer_events` (in-batch active-set + `StartTimer`-owned dedup; renamed from `terminal_arm_timer_events` and now used on all paths); the DB loop only inserts rows for `for_await: true` re-arms and deletes rows / emits `TimerCancelled` for cancels. The `WorkflowTestEnv` harness (`process_command`/terminal handler) was updated to the same model (a fresh arm records `TimerStarted` and never fires; a `for_await` re-arm fires only on a bookkeeping-only `await_fire` batch). Regression tests: `arm_timer_events` pure unit tests in `worker.rs` (fresh arm emits `TimerStarted` without signalling a row; a `for_await` re-arm emits no event; `StartTimer`-owned/in-batch dedup); the money-test `WorkflowTestEnv` fixture `start_timer_activity_reset_then_await_fires_without_spurious_fire` (arm → activity → reset → await: no `TimerFired` precedes `ActivityCompleted`, exactly one fire, `replay_check` `ReplaySucceeded`); and strengthened `context.rs` assertions that `start_timer`/`reset` arm `for_await: false` while `await_fire` re-arms `for_await: true`.
- **P2 (`worker.rs`, `persist_started_timer`) — a classic `ctx.timer` re-using a just-cancelled cancellable id could reschedule to a deleted row.** The classic `StartTimer` path computed its own `existing`/`is_new`/`fires_at` **before** the transaction (and before `plan_timer_lifecycle`). When a same-task `cancel_timer("x"); ctx.timer("x", n)` put a `CancelTimer("x")` and the classic `StartTimer("x")` in one batch, `plan_timer_lifecycle` deleted the pending row but `is_new` stayed `false`, so the classic path skipped its re-insert and then rescheduled the parked task to the row it had just deleted — the workflow could hang and replay diverge. Fixed defensively by moving the classic timer's `existing`/`is_new`/`fires_at`/`timer_event` resolution **into the transaction, after `plan_timer_lifecycle`**, so a same-batch same-id delete is reflected in `is_new` and the classic timer re-inserts a fresh row. (Under the FIX-A model this specific race is already unreachable — a fresh cancellable arm inserts no row for the cancel to delete — but the ordering fix keeps the classic path correct even when a live awaited row exists.) Regression tests: the `WorkflowTestEnv` fixture `cancel_then_classic_timer_same_id_arms_and_fires` (arm → cancel → classic `ctx.timer` same id → fires, `replay_check` `ReplaySucceeded`) and the `context.rs` unit test `cancel_then_classic_timer_same_id_emits_start_timer` (asserts the `[ArmTimer{for_await:false}, CancelTimer, StartTimer]` command sequence with no deferred non-determinism).

Whether FIX A subsumes FIX B: yes for the specific "fresh cancellable arm's row deleted out from under the classic timer" race (a fresh arm now inserts no row), but the FIX-B ordering change is applied regardless so the classic path stays correct if a live `await_fire`-inserted row ever coexists with a same-id cancel + `ctx.timer`. Full re-verified matrix (all via `WorkflowTestEnv` + `replay_check`): arm→await→Fired; arm→cancel→await→Cancelled; arm→cancel→reset→await→Fired; duplicate-active-arm→await→Fired (one `TimerStarted`); arm→activity→await→Fired (valid ordering); arm→activity→cancel→await→Cancelled; **arm→activity→reset→await (no spurious fire, FIX A)**; **cancel-then-`ctx.timer`-same-id (FIX B)**; reset-1000× bounded history. Example `cancellable_timer_sla` and its embedded tests unchanged and green (it arms and awaits in one cycle, so its SLA deadline coincides with `start_timer` time).

**Post-review hardening round 5 (Codex 2× P2, PR #1010):** two more genuine correctness findings in the cancellable-timer family, both fixed TDD red→green (RED confirmed by temporarily reverting each fix and observing the two RED-catcher tests fail).

- **P2 (`context.rs`, `start_timer`) — a duplicate ACTIVE `start_timer` with a DIFFERENT duration silently changed the live deadline.** Round-3 made a duplicate arm of an already-`Armed` id a history no-op (no second `TimerStarted`/reset event), but the returned `TimerHandle` still carried the NEW `duration_secs`, and `await_fire`'s `for_await:true` re-arm inserts the durable `harvest_timers` row (and advances the virtual clock) using the handle's duration. So `start_timer("idle", 300); start_timer("idle", 600); await_fire()` armed the row for 600s while history recorded only `TimerStarted(idle, 300)` — a live-vs-replay divergence, and later editing that duplicate duration was invisible to replay. Fixed by making the per-context logical timer state carry the recorded duration (`TimerLogicalState::Armed(u64)`, set when the ORIGINAL arm records its `TimerStarted`): a duplicate active `start_timer(id, new_dur)` now returns a handle preserving the RECORDED duration and ignores `new_dur`. A genuine duration change must go through `reset` (which records `TimerCancelled` + a fresh `TimerStarted`). `start_timer` rustdoc updated: a duplicate active `start_timer` is a duration-preserving idempotent no-op. Regression tests: `WorkflowTestEnv` fixture `test_duplicate_active_start_timer_preserves_recorded_duration` (arm 300, arm 600, await → the handle carries 300, exactly one `TimerStarted(idle, 300)` recorded, the virtual clock advances by 300 not 600, `replay_check` `ReplaySucceeded`; RED before the fix: handle carried 600) and a `WorkflowReplayer` replay-safety guard `duplicate_active_start_timer_diff_duration_preserves_recorded_duration`.
- **P2 (`replay.rs`, `match_timer_or_cancel`) — the `await_fire` timer-outcome scan crossed intervening command events (replay-soundness false-negative).** The fire/cancel-outcome forward scan used a `_ => scan += 1` catch-all that skipped EVERY unconsumed event until it reached the `TimerFired`/`TimerCancelled`, so a genuine command reorder slipped past strict replay: for history `[TimerStarted, ActivityScheduled, ActivityCompleted, TimerFired]`, code changed to await the timer BEFORE the activity claimed the trailing `TimerFired` across the unconsumed activity, then the activity still matched afterward — strict replay wrongly reported `ReplaySucceeded` despite a real command-order change (the inverse of the earlier round's false-positives). Fixed by making the crossable set EXPLICIT (mirroring `match_timer_strict`'s transparent set): the scan crosses only (a) already-`is_consumed` events — the legitimate `arm→activity→await_fire` flow, where `execute_activity` consumed the activity events in program order BEFORE `await_fire` ran — and (b) a bounded allowlist of genuinely transparent/interleaved events (`MarkerRecorded`, `SideEffectRecorded`, `ChildWorkflowSpawnedDetached`, the reset `TimerStarted`/`TimerCancelled` interleaving, stashed `SignalReceived` + external-signal/cancel triplets, update events). It STOPS (returns `NoMatch`, so `await_timer_fire`'s `check_strict_replay_no_match` surfaces the divergence) at any UNCONSUMED command-bearing event NOT on the allowlist (`ActivityScheduled`/`Completed`/`Failed`, attached `ChildWorkflow*`, `LocalActivity*`, a foreign `TimerFired`, ...). `match_timer_strict` (the classic-timer fire scan) was verified to already stop at unconsumed commands (final `break`), so no change needed there. Regression tests: the SOUNDNESS gate `await_timer_before_activity_detects_command_reorder` (reordered code → `NonDeterminismDetected`, NOT `ReplaySucceeded`; RED before the fix wrongly passed) and `activity_then_await_timer_replays_succeeded` (the legit arm→activity→await_fire flow against the SAME history still replays clean — the activity events cross via `is_consumed`), both `WorkflowReplayer` fixtures.

Full local gate: `cargo fmt --all -- --check` clean; `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings` (0 warnings); `--features testing --test integration` (789 passed, incl. the 4 new tests, with RED→GREEN confirmed for both RED-catchers). The full matrix from round 4 stays green; the legitimate `arm→activity→await_fire` flow is unchanged.

**Post-review hardening round 6 (Codex 2× P2, PR #1010):** two more genuine correctness findings in the same cancellable-timer state-machine family, both fixed TDD red→green. The change also **closes the class** — all four timer replay matchers now share one crossable-set discipline, and the full Armed→Fired logical-state lifecycle (not just the previously-covered Armed→Cancelled) is fixture-covered.

- **P2 (`replay.rs`, `match_timer_cancel`) — the cancel scan crossed intervening command events (replay-soundness false-negative; sibling of the round-5 `match_timer_or_cancel` fix).** The `TimerCancelled` forward scan used a `scan += 1` catch-all that skipped EVERY unconsumed event until it reached the cancel, so a genuine command reorder slipped past strict replay: for history `[TimerStarted, ActivityScheduled, ActivityCompleted, TimerCancelled, WorkflowCompleted]`, code changed to `cancel_timer` BEFORE the activity claimed the trailing `TimerCancelled` across the unconsumed `ActivityScheduled`, then the activity still matched afterward — strict replay wrongly reported `ReplaySucceeded`; and when no cancel was recorded, the scan crossed even terminal lifecycle to reach the end and let the caller enqueue a new `CancelTimer`. Fixed by making `match_timer_cancel` share `match_timer_or_cancel`'s **exact** crossable set: both now delegate to the new `HistoryMatcher::timer_scan_cross_or_stop` helper (extracted so the two scans are provably identical), which crosses only (a) already-`is_consumed` events and (b) a bounded allowlist of genuinely transparent/interleaved events (`MarkerRecorded`, `SideEffectRecorded`, `ChildWorkflowSpawnedDetached`, sibling-`reset` `TimerStarted`/`TimerCancelled` interleaving, stashed `SignalReceived` + external-signal/cancel triplets, update events), and STOPS at any unconsumed command-bearing event NOT on the allowlist (activities, attached child workflows, local activities, a foreign `TimerFired`, terminal lifecycle, ...). `cancel_timer`/`reset_timer` now surface the resulting `NoMatch` in strict replay via `check_strict_replay_no_match` (mirroring `await_timer_fire`), so the divergence is reported at the cancel rather than silently emitting a stray `CancelTimer`. Regression tests: the SOUNDNESS gate `cancel_timer_before_activity_detects_command_reorder` (reordered code → `NonDeterminismDetected`, NOT `ReplaySucceeded`) and `activity_then_cancel_timer_replays_succeeded` (the legit arm→activity→cancel flow against the SAME history still replays clean — the activity events cross via `is_consumed`), both `WorkflowReplayer` fixtures.
- **P2 (`context.rs`, `await_timer_fire`) — the `Armed` logical state was not cleared when a fire was consumed, so a loop reusing the timer id lost its fresh arm.** When `await_fire()` resolved `Fired`, the `Armed(dur)` entry set by the earlier `start_timer` was left in `cancellable_timer_state`. A sliding-window / idle-session LOOP that reuses the same id (`loop { start_timer(id, ..); await_fire → Fired; }`, possibly with a new duration each iteration) then treated the next `start_timer(id, ..)` as a duplicate **active** arm (round-3 idempotent no-op): it recorded no fresh `TimerStarted`, preserved the stale duration on the handle, and left the recorded `TimerStarted`s unconsumed → an early-completion divergence on replay. Fixed by clearing the logical state (new `WorkflowContext::clear_timer_logical_state`) when a fire is consumed, so a subsequent `start_timer(id, ..)` is a FRESH arm (records a new `TimerStarted`, uses the new duration). The `Cancelled` path is deliberately NOT cleared (a bare re-await must stay `Cancelled`; a genuine reuse goes through `start_timer`, which overwrites the entry to `Armed`). The clear runs in ALL builds (production replay + live), not just the test clock advance. Regression tests: the `context.rs` unit test `await_fire_clears_armed_state_so_reused_id_records_fresh_arm` (replay of a two-iteration, different-duration loop history — the re-armed handle must carry the fresh 600s, both `TimerStarted`/`TimerFired` pairs must be consumed, no divergence) and the `WorkflowReplayer` fixture `cancellable_timer_loop_reuse_replays_succeeded` (a 3-iteration fresh-duration-per-iteration loop → `ReplaySucceeded`).

**Timer-matcher lifecycle audit (closing the class):** the full Armed/Cancelled state machine and all four timer replay matchers were audited for the scan-past-a-command class of bug. `start_timer` (fresh → `Armed` + positional `TimerStarted`; already-`Armed` → duration-preserving no-op), `reset` (`Cancelled`+`TimerStarted` → `Armed`), `cancel` (→ `Cancelled` + `TimerCancelled`), and `await_fire` (→ `Fired`: clear state; → `Cancelled`: leave `Cancelled`) all transition correctly for both live and replay, and after a fire/cancel is consumed a subsequent `start_timer` records a fresh `TimerStarted` (balanced arm/fire bookkeeping). Matcher discipline: `match_timer_arm` is positional (no scan — sound by construction); `match_timer_strict` (classic `ctx.timer`) is positional at the arm and uses a settle-and-rewind scan (records interleaved commands as rewind points rather than claiming a fire across them — sound, a reorder is caught by its positional `TimerStarted` check); `match_timer_or_cancel` and `match_timer_cancel` now share the single `timer_scan_cross_or_stop` crossable-set discipline (this round). All four are now consistent — a timer outcome/cancel claim never crosses an unconsumed command-ordering point.

Full local gate: `cargo fmt --all -- --check` clean; `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings` (0 warnings); `--features testing --test integration` (792 passed, incl. the 3 new tests). The full round-5 matrix stays green; the legitimate `arm→activity→cancel` and `arm→activity→await_fire` flows are unchanged.

**Post-review hardening round 7 (Codex 2× P2, PR #1010):** two more genuine correctness findings in the cancellable-timer family, both fixed TDD red→green (RED confirmed by temporarily disabling each fix and observing the RED-catcher test fail), plus a completeness pass over every timer-event consumer.

- **P2 (`worker.rs`, `plan_timer_lifecycle`) — an `await_fire` raced by a same-task `cancel` rescheduled the parked task to a deleted row instead of waking now.** When `handle.await_fire()` polled and pushed its `for_await: true` `ArmTimer(X)` and a sibling branch then pushed `cancel_timer(X)`/`handle.cancel()` in the SAME workflow task, the DB loop inserted X's `harvest_timers` row, then deleted it and recorded `TimerCancelled`, but X's now-dead `fires_at` still counted toward `min_fires_at`. `persist_bookkeeping_and_requeue_workflow` then rescheduled the parked task to that deleted deadline (`reschedule_task`) instead of waking it, so the workflow only re-evaluated (and resolved `Cancelled` by consuming the recorded `TimerCancelled`) after the FULL timer duration. Fixed by excluding any id cancelled in the same batch from `min_fires_at`: the row-insert/deadline logic was extracted into a pure, unit-testable `plan_timer_lifecycle_pure(commands) -> (events_by_index, armed_indices)` that drops a `for_await: true` arm from `armed_indices` when a `CancelTimer` for the same id appears anywhere in the batch (order-independent — a cancel before OR after the arm cancels it). With no contributing arm, `armed_fires_at` is `None`, so `persist_bookkeeping_and_requeue_workflow` takes its existing wake-now path (also honouring a captured `wake_requested`), the workflow re-runs immediately, and `await_fire` consumes the `TimerCancelled` at once. The DB loop now deletes for every `CancelTimer` and upserts only the contributing armed rows (disjoint id sets → order-independent). NOTE: this is the WORKER/join case (the re-arm was pushed before the sibling cancel in the same batch); the context-side same-cycle cancel (a subsequent `await_fire` consulting `Cancelled` logical state) was fixed in an earlier round. Regression tests (pure, in `worker.rs`, run under `--features db` since `pub mod worker` is db-gated — no Postgres connection needed to run these pure-function tests): `plan_timer_lifecycle_pure_excludes_a_same_batch_cancelled_await_arm` (`[ArmTimer{X,for_await:true,300}, CancelTimer{X}]` → `armed_indices` empty + `TimerCancelled` emitted at the cancel index; reverse order equally cancelled; RED before the fix: `armed_indices == [0]`) and `plan_timer_lifecycle_pure_keeps_an_uncancelled_await_arm` (an uncancelled re-arm still contributes).
- **P2 (`timeline.rs`, `derive_timeline`) — cancelled/reset timers were left open forever, distorting the timeline rollup.** Since issue #768, a `cancel_timer()`/`reset()` records `TimerStarted … TimerCancelled` with NO `TimerFired`. `derive_timeline` (the per-execution timeline read model, issue #739) only closed an open timer step on `TimerFired`, so a cancelled/reset timer's step stayed `Pending` indefinitely — its `total_ms` measured to "now", inflating `total_wall_clock_ms`/`wait_ms` and the timer-wait bucket in the rollup. Fixed by adding a `TimerCancelled` arm that closes the OLDEST still-open step for that id as `StepOutcome::Cancelled` (mirroring `TimerFired`'s FIFO-by-id pairing, so a reused-id reset yields two correctly-paired steps: first `Cancelled`, second `Fired`). `StepOutcome::Cancelled` (previously reserved/unemitted) is now emitted by the core derivation; its doc and the module-doc step-reconstruction table were updated. Regression tests (pure, no-DB, in `timeline.rs`): `cancelled_timer_closes_as_cancelled_not_left_pending` (`TimerStarted … TimerCancelled`, no fire → the step closes `Cancelled` with `ended_at` set, `total_ms` bounded to the cancel ts, no open timer left; RED before the fix: `Pending`) and `reset_timer_first_cancelled_then_second_fired` (`TimerStarted, TimerCancelled, TimerStarted, TimerFired` → first step `Cancelled`, second `Fired`).

**Timer-event-consumer audit (closing the class):** every consumer of timer events outside the already-fixed `replay.rs` matchers and worker persist path was checked for `TimerCancelled`-handling completeness.

| Consumer | Pairs/counts/renders timer state? | Needs `TimerCancelled`? | Status |
|---|---|---|---|
| `replay.rs` matchers | yes | yes | already fixed (rounds 5/6) |
| `worker.rs` `plan_timer_lifecycle` | yes (deadline/rows) | yes | **FIX A this round** |
| `timeline.rs` `derive_timeline` | yes (pairs Started↔Fired) | yes | **FIX B this round** |
| `history_export.rs` | yes (redacts + renders) | already handles (both the payload-field filter and the human-label match) | complete (NA) |
| `reset.rs` `rebuild_pending` | yes (resolves pending arm) | already handles (`TimerFired \| TimerCancelled` resolve identically, with a test) | complete (NA) |
| `autumn-harvest-plugin/src/ui.rs` `human_label` | renders event label | cosmetic gap (fell to raw-type fallback) | **FIX C this round** (added "Timer cancelled") |
| `analyzer.rs` `SuspiciousTimerRule` | inspects individual `TimerStarted { duration_secs }` only | no (a 0-second arm is flagged regardless of later cancel) | NA |
| `context.rs` (author-side timer API) | yes | already handles (`is_timer_active`, logical state, strict-replay scans) | complete (NA) |
| `testing.rs` `WorkflowTestEnv` | yes (`is_timer_active` handles cancel) | `final_now()` virtual clock sums `TimerStarted { duration_secs }` incl. cancelled | NA (test-harness virtual clock, deliberate `Σ over all TimerStarted`; not a user-facing rollup) |
| `simulator.rs` | generates synthetic history | no (never emits cancels) | NA |
| `store.rs` / `erase.rs` / `executor.rs` | test fixtures / event-type name only | no | NA |

RED→GREEN evidence: the two timeline tests FAILED under `--no-default-features --lib` with FIX B absent (`Pending`/`Completed` != `Cancelled`), then GREEN once the arm was added; the worker `plan_timer_lifecycle_pure_excludes_…` test FAILED under `--features db --lib` with the cancelled-exclusion clause disabled (`armed_indices == [0]`), then GREEN once restored. No new `WorkflowEvent` variant, no migration, no replay-determinism change — a same-batch cancel now wakes immediately and a cancelled timer becomes a closed timeline step.

**Post-review hardening round 8 (Codex 2× P2, issue #768):** two genuine
duration/clock-accounting bugs, TDD red→green.

- **FIX A — `await_fire` used the awaiting handle's stale cached duration**
  (`context.rs` `await_timer_fire`). A `TimerHandle` caches `duration_secs` at
  creation; a reset through `ctx.reset_timer(id, secs)` (or another handle for the
  same id) updates the logical state map's `Armed(dur)` — which is what records the
  arm's `TimerStarted` — **without** touching the awaiting handle's cached field.
  `await_fire` then used the stale cached value to arm the live `harvest_timers`
  deadline and advance the test virtual clock, so `h = ctx.start_timer("idle",
  300); ctx.reset_timer("idle", 600); h.await_fire()` recorded `TimerStarted(600)`
  for replay but armed the deadline / advanced the clock for 300s. Fix:
  `await_timer_fire` now reads the CURRENT armed duration from the logical state
  map (`timer_logically_armed`) — the authoritative value that produced the
  recorded `TimerStarted` — for both the `for_await: true` `ArmTimer` command
  (live deadline) and `advance_timer_clock` (virtual clock); the handle's cached
  `duration_secs` is now only a belt-and-braces fallback for the (unreachable via
  `start_timer`) no-state-entry case. The state map's `Armed(dur)` is thus the
  single authoritative source for the live deadline / recorded `TimerStarted` /
  virtual clock everywhere post-arm; the handle's cached value is never used for a
  live/recorded deadline when a state entry exists.

- **FIX B — the WorkflowTestEnv virtual clock counted non-fired arms**
  (`testing.rs` `TestRunOutcome::final_now`/`elapsed`; the round-7 "NA" assessment
  above was WRONG). `final_now` summed every `TimerStarted { duration_secs }`, so a
  cancellable timer that was cancelled or repeatedly reset advanced the virtual
  clock for arms the workflow never observed firing, disagreeing with `ctx.now()`
  after an `await_fire()` (which advances only on a real fire). Fix: new pure
  `fired_timer_duration_secs(events)` sums only `TimerStarted`s paired with a
  matching `TimerFired` (per id, FIFO; a `TimerCancelled` discards the earliest
  pending arm uncounted). A cancelled timer now advances the clock by 0; a
  reset-then-fire advances only by the fired arm. The classic `ctx.timer` path
  always fires, so every one of its `TimerStarted` pairs with a `TimerFired` and
  the sum is unchanged (`test_billing_loop_dates_and_elapsed`/`test_365_days`
  green).

Audit (context.rs/worker.rs/testing.rs duration & clock reads):

| Site | Duration source | Correct? |
|---|---|---|
| `start_timer` / `reset_timer` fresh arm | call arg (the fresh arm's own duration) → `match_timer_arm` + `ArmTimer` cmd + `Armed(dur)` state | ✓ arg IS the fresh arm's duration |
| `start_timer` duplicate-active | `armed_duration` from state map | ✓ round-5 |
| `timer_logically_armed` | reads `Armed(dur)` from state map | ✓ authoritative source |
| `await_timer_fire` | **now** state map `Armed(dur)` (param = fallback) → deadline + clock | ✓ **FIX A** |
| `TimerHandle::{duration_secs,await_fire}` cached field | informational only; ignored by `await_timer_fire` when a state entry exists | ✓ |
| `timer()` classic / `receive_signal_timeout` / `race()` timer | call arg (single always-fresh, no cancellable state) | ✓ |
| `worker.rs` `arm_timer_events` (for_await:false) | `ArmTimer` cmd duration → `TimerStarted` | ✓ = fresh arm duration |
| `worker.rs` `plan_timer_lifecycle` (for_await:true) | `ArmTimer` cmd duration → row `fires_at` | ✓ cmd now carries `armed_duration` (FIX A) |
| `testing.rs` `final_now`/`elapsed` | **now** `fired_timer_duration_secs` (fired-only) | ✓ **FIX B** |

No new `WorkflowEvent` variant, no migration, no replay-determinism change.

## Post-review hardening round 9 (Codex 1× P2)

**FIX — reset-then-await in one task must arm the durable row in the transaction
that recorded the reset** (`worker.rs` `plan_timer_lifecycle_pure`). Round 8's
`cancelled_ids` set excluded an `await_fire` re-arm from `armed_indices`/`min_fires_at`
whenever a `CancelTimer(id)` appeared **anywhere** in the batch — an order-independent
rule. That over-suppressed the reset-then-await shape: a `reset_timer(id)` pushes
`CancelTimer(id) + ArmTimer(id, for_await: false)` and a following `await_fire()`
pushes `ArmTimer(id, for_await: true)`, so the batch is
`[CancelTimer(id), ArmTimer(id, false), ArmTimer(id, true)]`. Because a cancel was
present, the order-independent rule dropped the await's arm, so no durable
`harvest_timers` row was inserted and no `fires_at` was returned in the reset's own
transaction; `persist_bookkeeping_and_requeue_workflow` then woke/replayed the task
immediately and only armed the deadline on the *next* claim — under worker backlog
shifting the deadline later than the `await_fire()` call instead of anchoring it at
the reset.

The `harvest_timers` row state was already correct (the DB delete/insert loops
process commands in order: delete-all-then-insert-all leaves a reset-then-await id's
row present and an await-then-cancel id's row absent) — the bug was **only** in the
`armed_indices`/`min_fires_at` computation, which must match that end-of-batch row
state. The contribution is now **order-sensitive**: a `for_await: true` arm for id X
contributes iff X is *cancelled and not re-established* is false — i.e. the last
fresh-establish (`ArmTimer(X, for_await: false)` from `reset`/`start_timer`, or a
same-batch `StartTimer(X)`) vs `CancelTimer(X)` op for X is not a trailing cancel.
The `for_await: true` arm is only a firing request on an already-(being-)established
timer, so it does not itself count as an establish. This satisfies both shapes with
one rule:

- reset-then-await `[Cancel, Arm(false), Arm(true)]` → last establish/cancel op is
  the fresh arm → X live → the await arms the durable row + contributes its `fires_at`
  in this transaction (deadline starts when the reset was recorded). **The FIX.**
- await raced by a later sibling cancel `[Arm(true), Cancel]`, and
  cancel-then-await-with-no-reset `[Cancel, Arm(true)]` → last establish/cancel op is
  the cancel → X dead → the await is dropped and the parked task wakes immediately.
  **Round 7 behavior preserved** (both round-8 sub-assertions stay green).

Purely a change to the contribution/`armed_indices` computation in
`plan_timer_lifecycle_pure`; the event emission (`TimerStarted`/`TimerCancelled`),
dedup, and the in-order DB delete/insert are unchanged. New pure unit tests:
`plan_timer_lifecycle_pure_arms_a_reset_then_await_in_one_batch` (the FINDING,
RED before the fix) and `plan_timer_lifecycle_pure_resolves_two_ids_independently_by_order`
(per-id order-sensitivity: one reset-then-await arms, one await-then-cancel drops).
No new `WorkflowEvent` variant, no migration, no replay-determinism change.

## Post-review hardening round 10 (Codex 2× P2)

**Fix B (replay.rs) — the timer outcome/cancel scans now stop at an UNCONSUMED
SAME-id `TimerStarted`.** The round-6/7 shared helper `timer_scan_cross_or_stop`
(used by `match_timer_cancel` and `match_timer_or_cancel`) crossed *every*
`TimerStarted`/`TimerCancelled` transparently, regardless of id. That was too
loose: a same-id `TimerStarted` is the command-ordering **anchor** the cancel/fire
of that id must not cross. In strict replay of `start_timer("idle");
cancel_timer("idle")` with the two lines **reordered** to `cancel_timer("idle");
start_timer("idle")`, `match_timer_cancel("idle")` skipped over the unconsumed
same-id `TimerStarted`, consumed the later `TimerCancelled`, and then `start_timer`
consumed the start — so a real command-order change was silently accepted
(`ReplaySucceeded`). The helper is now **id-aware** (`timer_scan_cross_or_stop(scan,
timer_id)`): only a FOREIGN-id `TimerStarted`/`TimerCancelled` (an interleaved
sibling timer's lifecycle) stays crossable; an unconsumed SAME-id
`TimerStarted`/`TimerCancelled` falls through to the `_ => Stop` catch-all. The
caller claims its own-id fire/cancel *target* BEFORE this call, so a same-id one
reaching the helper is always a genuine unconsumed ordering point. In the normal
(non-reordered) flow the arm was already consumed by the preceding `start_timer`,
so the cancel scan skips it via `is_consumed` and never reaches the STOP — and the
reset loop `[TimerStarted(idle), TimerCancelled(idle), TimerStarted(idle),
TimerFired(idle)]` still replays cleanly (each scan claims its target directly
without crossing an unconsumed same-id start). Regression tests in
`tests/integration/replayer_tests.rs`:
`cancel_then_start_no_activity_detects_command_reorder` (RED before the fix →
`NonDeterminismDetected`) and `start_then_cancel_no_activity_replays_succeeded`
(the canonical arm→cancel order still `ReplaySucceeded`); the round-6/7 reorder
tests (`cancel_timer_before_activity_detects_command_reorder`,
`activity_then_cancel_timer_replays_succeeded`) and the full cancellable-timer
matrix stay green. No new `WorkflowEvent` variant, no migration.

**Fix A (worker.rs:6719 / reset boundary scanner) — DEFERRED pending maintainer
decision.** Codex's second P2: under the round-5 `for_await` model a lazy
cancellable arm (`start_timer`/`reset` → `ArmTimer { for_await: false }`) records a
plain `TimerStarted` **without** inserting a durable `harvest_timers` row (only
`await_fire`'s `for_await: true` arm inserts the row, and it emits no event), yet
`reset.rs::apply_event_to_pending` treats *every* open `TimerStarted` as an
unresolved pending side effect until a `TimerFired`/`TimerCancelled`. So a running
workflow that `start_timer("idle", …)`s then parks on an activity/signal — without
awaiting or cancelling — cannot be reset to a later clean boundary, even though
there is no durable timer row to carry over or remove. This is **SAFE** (it refuses
valid resets; it never produces a wrong fork), a genuine completeness gap.

The correct history-based fix is an additive `cancellable: bool` field on
`WorkflowEvent::TimerStarted` (`start_timer`/`reset` emit `true`; classic
`ctx.timer` emits the default `false`), with the reset scanner skipping
`pending`-insertion for an open `cancellable:true` arm. **Deferred** because the
field breaks ~109 exhaustive-binding sites (46 matchers in `replay.rs` alone that
bind `duration_secs` without `..`, plus `context.rs` 14, `timeline.rs` 10,
`worker.rs` 5, tests, …) — genuinely disproportionate churn for a Codex-confirmed
SAFE over-conservatism, and high-risk to iterate under this environment's severe
compile throttling. The history alone also cannot distinguish an *awaited*
(durable-row) cancellable timer from an *un-awaited* lazy arm (both are a bare
`cancellable:true` `TimerStarted` until fired/cancelled), so the field marks both
non-blocking — safe (the fork re-arms via `await_fire` replay and
`remove_pending_timers` deletes the source row), but a subtlety worth a maintainer
review. Left as a documented known limitation pending the decision between the
field and a doc-only note.

## Post-review hardening round 11 (Codex 1× P2)

`plan_timer_lifecycle` (worker.rs) now contributes a firing row for a
`for_await: true` (await) arm only when, at its own position, **no same-id
`CancelTimer` follows it in the batch** (forward, round 11) **and** the timer is
not sitting cancelled-without-reset before it (backward, round 9) — a
`for_await: false` (fresh start/reset) arm never contributes a firing row on its
own. This replaces round 10's end-of-batch-liveness rule
(`cancelled_and_not_reestablished`), which decided contribution from the id's
*final* liveness across the whole batch. That was wrong for an `await_fire()`
polled BEFORE a sibling `reset()` in the same workflow task: the batch is
`[ArmTimer(X, for_await:true, old), CancelTimer(X), ArmTimer(X, for_await:false,
new)]`, and because the fresh arm re-establishes X at end-of-batch, the round-10
rule KEPT the old await arm and armed a firing row off the **stale await
duration** — so the live run waited/fired on that row while a replay of the
recorded history (whose `TimerCancelled(X)` precedes the fresh
`TimerStarted(X, new)`) resolved the same `await_fire()` to `Cancelled`, a
live-vs-replay divergence. Under the round-11 rule the later same-id cancel
supersedes the await arm regardless of any later fresh arm, so `armed_indices` is
empty → the parked task wakes now → live and replay both resolve `Cancelled`. The
backward (round-9) half is retained so `[CancelTimer(X), ArmTimer(X, true)]`
(cancel-then-await with no reset) still wakes now while
`[CancelTimer(X), ArmTimer(X, false), ArmTimer(X, true)]` (reset-then-await) still
arms in the same transaction. Tests (TDD red→green): new pure planner test
`plan_timer_lifecycle_pure_excludes_an_await_arm_cancelled_by_a_later_reset`
(worker.rs — confirmed RED under the round-10 rule: `armed == [0]`; GREEN under
round 11: empty), the round-8/9/10 pure tests kept green, and a new context test
`await_fire_then_same_cycle_reset_emits_the_stale_arm_cancelling_batch` (context.rs)
proving the real `await_fire()`-then-`reset()` API sequence emits exactly that
`[Arm(true), Cancel, Arm(false)]` batch; the replay-resolves-`Cancelled` side is
already covered by `await_fire_returns_cancelled_when_history_has_timercancelled_before_fire`.

**Post-review hardening round 12 (Codex 1× P2):** a blocked cancellable-timer
scan — one that STOPPED (`TimerScanStep::Stop`) at an unconsumed command-bearing
event with the recorded `TimerCancelled`/`TimerFired` still ahead — is now treated
as a non-determinism divergence in NORMAL worker replay too, not only under strict
`WorkflowReplayer` mode. Previously the blocked case was surfaced solely through
`check_strict_replay_no_match`, a no-op when `strict_replay`/`canary_mode` are
false (i.e. the ordinary worker replay path). So a running execution whose history
was `TimerStarted(id), ActivityScheduled(...), …, TimerCancelled(id)` and whose new
code moved `cancel_timer(id)`/`reset_timer(id, …)` (or the `await_fire()`
resolution) BEFORE that activity would take the live-frontier append path in
production: the worker pushed a fresh `CancelTimer`/`ArmTimer` and extended history
AFTER a command it had not replayed — a silent corruption where issue #603 should
have nd-blocked. `WorkflowReplayer` (strict) caught it; the worker did not.

The matcher now distinguishes a *blocked* `NoMatch` (scan stopped at an unconsumed
command) from a *genuine live-frontier* `NoMatch` (scan ran off the end, cursor at
the frontier) via a new `HistoryMatcher::timer_scan_stopped_at_command()` flag,
reset on entry to `match_timer_cancel`/`match_timer_or_cancel` and set `true` on the
`Stop` break. `cancel_timer`/`reset_timer`/`await_timer_fire` read it after a
`NoMatch`: a blocked scan records a deferred nd error (→ #603 nd-block) and skips
the command append/re-arm+park entirely, while a genuine live-frontier `NoMatch`
still records a real new cancel/await as before. The strict `WorkflowReplayer`
immediate-`Err` path (rounds 6/10 reorder soundness gates) is unchanged — the
existing strict reorder tests stay green. No new `WorkflowEvent` variant, no
migration. Tests (TDD red→green): two new normal-replay context tests
`cancel_timer_blocked_by_unreplayed_command_records_deferred_nd_in_normal_replay`
and `await_timer_fire_blocked_by_unreplayed_command_records_deferred_nd_in_normal_replay`
(context.rs) — confirmed RED before the fix (the cancel test appended a stray
`CancelTimer` with no deferred nd; the await test parked/re-armed forever) and
GREEN after; the strict-mode reorder detection tests and the full timer matrix /
reset-loop tests stay green.

**Post-review hardening round 13 (regression fix, issue #768):** round 7 added
`WorkflowEvent::TimerStarted`/`TimerCancelled` to `match_signal`'s transparent
interleaved-command skip-arm so a signal scan could cross a same-cycle
`reset()`/`cancel_timer()`'s `[TimerCancelled, TimerStarted]` and still find a
later `SignalReceived` (`Matched`). But the arm skipped those events
**unconditionally**, so a `wait_for_signal` over a history carrying a STRAY,
unconsumed `TimerStarted` (or `TimerCancelled`) and NO matching signal ran the
scan off the end of history and returned `HistoryMatch::NoMatch` — which
`wait_for_signal` turns into a `WaitForSignal` command + `rx.await`, **parking a
genuinely-diverged workflow forever** instead of nd-blocking (#603). On base
(`origin/trunk-dev`) `TimerStarted` fell to `match_signal`'s divergence arm, so
`context::tests::wait_for_signal_returns_nondeterministic_on_diverged_history`
passed in 0.00s; this PR's round-7 change regressed it into an infinite hang.
Fixed by making `match_signal` diverge — not swallow to `NoMatch`/suspend — when
the scan reaches the end of history after crossing one or more UNCONSUMED
interleaved timer/detached-spawn commands (`first_interleaved_command.is_some()`).
An already-consumed reset's timers (claimed by a companion
`match_timer_cancel`/`match_timer_arm` earlier in the cycle) are skipped at the
top of the loop and never set `first_interleaved_command`, so a genuine
"signal has not arrived yet" suspend (its reset timers consumed) still returns
`NoMatch` and parks correctly; only a stray unconsumed timer where the workflow
expected a signal now diverges (→ `NonDeterministic`). The round-7 legit tests
(`matcher_signal_scan_skips_interleaved_reset`,
`matcher_signal_scan_skips_interleaved_timer_cancel`) — which cross UNCONSUMED
timers but find the signal later → `Matched` — are unaffected (they return
before end-of-scan). Tests: the hanging RED
`wait_for_signal_returns_nondeterministic_on_diverged_history` now passes; three
new pure-matcher guards in `replay.rs`
(`matcher_signal_scan_diverges_on_unconsumed_stray_timer` — stray unconsumed
`TimerStarted` + no signal → `Diverged`;
`matcher_signal_scan_sequential_reset_then_signal_matches` — sequential reset
consumes the timers, later signal → `Matched`;
`matcher_signal_scan_sequential_reset_no_signal_suspends` — sequential reset
consumes the timers, no signal → `NoMatch`/suspend, NOT a false `Diverged`).
Full `autumn-harvest` lib suite (1363 tests) completes without hang and with no
skipped tests; the full cancellable-timer matrix, reset-loop, and round-6/7/10/
11/12 reorder tests stay green.
