//! The single-writer worker pass.
//!
//! [`drain_ready`] is the analog of the Postgres poll/dispatch loop, reduced to
//! what a synchronous, embedded runtime needs: it claims every ready activity
//! task, runs its registered body, applies the retry policy, fires every due
//! timer, and appends the resulting terminal [`WorkflowEvent`]s to the canonical
//! history.
//!
//! # Crash model — activity execution is at-least-once (by design)
//!
//! A claim (`BEGIN IMMEDIATE` → `RUNNING`, committed) and the terminal finalize
//! (record-attempt + terminal-event + `DONE`/requeue, committed together in
//! [`finalize_activity_result`]) are each atomic, but the activity **body runs
//! between them, outside any transaction** — you cannot hold `SQLite`'s single
//! write lock across arbitrary user I/O. So a crash after the body runs but
//! before the finalize commits leaves the task `RUNNING`; on the next
//! [`SqliteRuntime::open`](crate::SqliteRuntime::open),
//! [`reclaim_orphaned_running`](crate::queue::reclaim_orphaned_running) flips it
//! back to `PENDING` and the **body re-runs** — hence *at-least-once*. Write
//! activity bodies to be idempotent. A crash *after* the finalize commits sees a
//! `DONE` task and never re-runs the body. The finalize is transactional so the
//! replayable log never records a half-completed activity.
//!
//! # Retry model
//!
//! A *retryable* activity failure is recorded in the `harvest_activity_attempts`
//! audit table and the task-queue row's `attempt` counter — it is **not** appended
//! to the replayable event log. Only the terminal outcome (`ActivityCompleted`, or
//! `ActivityFailed` after exhausting attempts) reaches `harvest_events`. This
//! matches the Postgres engine (`queue::requeue_for_retry` stores the attempt
//! error on the task row, never in `harvest_events`), keeping every persisted
//! history a clean, terminal-only, replay-correct log.

use std::collections::HashMap;

use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
use autumn_harvest::{ExecutionId, TimeoutType, TimerId, WorkflowCommand, WorkflowEvent};
use rusqlite::Connection;

use crate::error::SqliteResult;
use crate::queue::{self, ClaimedTask};
use crate::runtime::ActivitySpec;
use crate::store;

/// Run all currently-ready work for `exec_id` at logical time `now`: drain ready
/// activity tasks (running bodies + honouring retries) and fire due timers.
///
/// `failure_now` reads the crate clock seam at *finalize* time — the instant a
/// body actually returns — so a policy retry's backoff anchors to the failure
/// time, not the pre-body cycle-start `now` (Codex #1069 P2, `worker.rs:328`). A
/// body that consumes real time before failing would otherwise schedule its next
/// attempt `delay` after the *stale* cycle start, making the retry ready too early
/// (or immediately, if the body ran longer than the delay) — unlike the Postgres
/// path, which computes the delay AFTER handling the result. Read PER activity
/// (each body has its own runtime) inside the drain loop. In `_as_of` simulation
/// the seam returns the caller-fixed epoch, so `failure_now() == now` and behavior
/// is byte-identical to a wall-clock cycle with an instant body.
///
/// Returns `true` if any terminal event was appended (i.e. the workflow may have
/// made progress and should be re-run).
///
/// # Race settling — drain ONE branch at a time (issue #600; Codex #1069 P1)
///
/// `settle_after_first_terminal` gates a critical `ctx.race()` correctness
/// behavior. When a workflow is awaiting an activity `ctx.race()`, all N branches
/// are scheduled in one suspension batch, then this loop would ordinarily drain
/// EVERY ready branch to a terminal event before the workflow is re-polled — so by
/// the time the core's `settle_race` picks a winner and emits `CancelRaceLosers`,
/// every loser is already `DONE` (its body already ran, e.g. a second provider
/// charged) and there is nothing left to cancel. That silently violates the race
/// contract (only the winner runs; still-pending losers are cancelled) and
/// diverges from the Postgres backend, where each activity completion wakes the
/// workflow (one NOTIFY → one poll) so the race settles while the siblings are
/// still pending.
///
/// When `settle_after_first_terminal` is `true` (the caller detected an unsettled
/// activity race in flight — see [`history_has_unsettled_activity_race`] /
/// [`commands_open_activity_race`]), this pass stops as soon as ONE branch
/// produces a terminal event, so the outer `drive_one_cycle` loop re-polls the
/// workflow BEFORE the sibling branches are drained. The re-poll lets `settle_race`
/// pick the just-completed branch as the winner and emit `CancelRaceLosers`, which
/// cancels the still-`PENDING` losers (a synthetic `ActivityFailed` — body never
/// runs). Retry requeues (which append no terminal event) do NOT stop the pass, so
/// the loop keeps draining until one branch reaches a genuine terminal, matching
/// "first branch to finish wins". `false` is the default drain-ALL behavior, so
/// fan-out (wait-all, N schedules in one batch, resolves only when every branch
/// completes) and ordinary single/sequential activities are byte-for-byte
/// unchanged — they never set the flag.
pub fn drain_ready(
    conn: &mut Connection,
    exec_id: ExecutionId,
    now: i64,
    // `+ Send + Sync` keeps the enclosing async `drive_one_cycle` future `Send`
    // (clippy `future_not_send`); the drivers' closures capture only a `Send + Sync`
    // `NowFn` `Arc` (wall-clock) or a `Copy` `i64` (`_as_of`), so both satisfy it.
    failure_now: &(dyn Fn() -> i64 + Send + Sync),
    activities: &HashMap<String, ActivitySpec>,
    settle_after_first_terminal: bool,
) -> SqliteResult<bool> {
    let mut produced = false;

    // Drain activity tasks. A retry requeues at `run_at = now`, so it becomes
    // immediately ready again and this loop re-claims it — the whole retry
    // sequence converges in one drain pass under the polling model.
    //
    // EXCEPTION (`settle_after_first_terminal`, issue #600): under an unsettled
    // activity `ctx.race()`, break as soon as ONE branch reaches a terminal event
    // so the workflow re-settles and cancels the still-pending losers before they
    // are drained (see the fn doc). A retry (no terminal) never breaks.
    loop {
        // Claim the next ready task and FREEZE its resolved defaults ATOMICALLY —
        // both in ONE `BEGIN IMMEDIATE` transaction (issue #1068; Codex #1080 P2).
        // The old flow committed the `RUNNING` claim in one transaction and froze
        // the defaults in a SEPARATE one, leaving a committed `RUNNING`+unfrozen row
        // in the gap: a crash there was reclaimed `PENDING` (still unfrozen) by
        // `reclaim_orphaned_running`, and a re-registration under a CHANGED spec
        // before the next drive then froze the NEW defaults — silently altering an
        // "already-claimed" late-registered task's frozen contract. Committing the
        // claim and the freeze together makes that intermediate state unreachable:
        // either both commit (`RUNNING`+frozen) or neither does (the row stays
        // `PENDING`+unfrozen, and the freeze happens atomically with whatever spec
        // is registered at the ACTUAL first successful claim).
        //
        // The transaction is held only across the spec lookup + freeze (a hash
        // lookup + a pure resolution + one `UPDATE`) and is committed BEFORE the
        // activity body runs — never across the body — so the write lock is held
        // for microseconds and the body remains at-least-once (outside any tx).
        let (task, spec) = {
            let Some((tx, task)) = queue::claim_next_ready_task_tx(conn, exec_id, now)? else {
                break;
            };
            let mut task = task;
            // If the activity name has no registered body, ROLL BACK the uncommitted
            // claim (dropping the tx = rollback) so the row stays `PENDING` and is
            // re-claimable once the body is registered in the SAME runtime — no
            // orphaned `RUNNING` row, no DB reopen needed. The error still surfaces
            // loudly out of the drive loop so the caller is not spun (mirrors the
            // non-determinism gate).
            let Some(spec) = activities.get(&task.name) else {
                drop(tx);
                return Err(crate::error::SqliteError::unregistered(&task.name));
            };
            // FREEZE-OR-READ the four resolved defaults (issue #1068) IN THIS SAME
            // transaction, resolving from the now-present `spec` and persisting them
            // on a first (unfrozen) claim, then commit the claim+freeze together.
            // Extracted to [`freeze_defaults_at_claim`]; see its doc for the full
            // freeze/read contract.
            freeze_defaults_at_claim(&tx, &mut task, spec)?;
            tx.commit()?;
            (task, spec)
        };

        // Enforce the activity's TOTAL (cross-retry) schedule-to-close deadline
        // BEFORE running the body (issue #378, Codex #1069 P2 `runtime.rs:39`). A
        // task drained at/after its absolute deadline — the "idle past the deadline,
        // then finally drained" case, mirroring the Postgres timeout scanner
        // catching a `PENDING` row past deadline — must NOT run its body and then
        // record a (possibly successful) result past the declared total cap. Seal it
        // terminal `ActivityTimedOut { ScheduleToClose }` and move on. Cheap no-op
        // for the common case (`None`, or a deadline still in the future). The retry
        // path (below, in `finalize_within_tx`) catches the more common case: a body
        // that keeps failing/backing off past the deadline.
        //
        // Read a FRESH clock for THIS pre-run check (Codex #1069 P2, `worker.rs:194`)
        // — NOT the stale cycle-start `now`, which is captured ONCE at the top of the
        // pass. When several ready activities are drained in one pass, an EARLIER
        // body drained in this same pass may consume real wall-clock time before this
        // (later) task is claimed, so this task's ABSOLUTE deadline can elapse WHILE
        // it waited its turn. Comparing against the stale `now` would pass this check
        // and RUN the body anyway — the finalize-time recheck (below) only catches it
        // AFTER the side effect already happened, starting an attempt past the
        // declared wall-clock cap. Reading current time here seals an already
        // over-deadline queued task terminal via the SAME timeout path BEFORE its
        // body runs. In `_as_of` simulation `failure_now() == now`, so this is
        // byte-identical to the old behavior there.
        let claim_now = failure_now();
        if schedule_to_close_exceeded(claim_now, task.schedule_to_close_at) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::ScheduleToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        // Body runs OUTSIDE any transaction — at-least-once (see module docs).
        //
        // Contain a PANICKING body (issue #782 analog; Codex round 8). A body
        // that `panic!()`s (rather than returning `Err`) would otherwise unwind
        // past the finalize/requeue branch AFTER the row is already `RUNNING`, so
        // no failure/requeue ever commits and the run wedges permanently (the
        // claim query only re-selects `PENDING`, and a single-process design has
        // no live-peer poison-pill net). Catch the unwind and convert it to the
        // crate's NORMAL retryable-activity-failure path — treat it exactly like
        // the body returned `Err(msg)`: `finalize_activity_result` records the
        // attempt and follows the retry policy (requeue if attempts remain, else
        // terminal `ActivityFailed`). `AssertUnwindSafe` is sound: `spec.body`
        // and `task.input` are only read, and on a caught panic the closure's
        // result is discarded (no observable state was left half-mutated — the
        // body ran outside any transaction). Mirrors the Postgres worker's
        // handler-panic containment (`catch_unwind` → retryable typed failure).
        //
        // Measure the body's REAL wall-clock runtime (`Instant`, NOT the logical
        // `now` — start-to-close is a real budget for real execution) so a body
        // that overran its start-to-close budget records a timeout below.
        let started = std::time::Instant::now();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (spec.body)(task.input.clone())
        }))
        .unwrap_or_else(|payload| {
            // Encode the caught panic as the TYPED `harvest_activity_failure_v1`
            // envelope carrying `error_type = "HandlerPanic"` (issue #782; Codex
            // #1069 P2, `worker.rs:119`) — NOT a plain string. This is the one
            // activity-failure path that would otherwise skip the typed-envelope
            // treatment the ordinary `Err(String)` path already gets, because the
            // finalize path derives BOTH the retry classification
            // (`parse_typed_payload` → `RetryPolicy::is_non_retryable`) AND the
            // terminal `ActivityFailed` metadata (`parse_error_payload_full`) from
            // this string. Emitting the typed HandlerPanic envelope makes a retry
            // policy that marks `HandlerPanic` non-retryable actually match (the
            // panicking task fails terminally after ONE attempt instead of being
            // retried to `max_attempts`), and records a terminal
            // `ActivityFailed { error_type: "HandlerPanic", .. }` byte-equivalent
            // to the Postgres worker's caught-panic path
            // (`handler_panic_activity_envelope`). Retryable like the core — a
            // caught panic follows the activity's retry policy exactly as
            // `Err(String)` does. The carried message is the raw panic message (no
            // synthetic prefix), matching the core envelope.
            Err(handler_panic_activity_envelope(
                autumn_harvest::error::panic_message(payload),
            ))
        });
        let elapsed = started.elapsed();

        // Enforce the activity's start-to-close timeout (issue #1069 P2), when the
        // scheduling command carried one (`start_to_close_override`, persisted on the
        // task row). A synchronous body cannot be cancelled mid-flight in a
        // single-writer runtime, so this is a POST-EXECUTION outcome: if the body's
        // real runtime exceeded its budget, record a TERMINAL
        // `ActivityTimedOut { StartToClose }` — byte-equivalent to the durable event
        // the Postgres timeout scanner (`enforce_activity_timeout`) records — instead
        // of the body's actual result. Terminal, NOT retried (the Postgres scanner
        // appends `ActivityTimedOut` + `fail_task` without requeuing; the workflow
        // observes `HistoryMatch::TimedOut` and drives its own timeout branch).
        // Checked BEFORE the normal finalize so an over-budget body's outcome is a
        // timeout regardless of what it eventually returned. Replay-safe: the outcome
        // is recorded once, and replay never re-runs the body, so a body whose timing
        // varies across a crash-rerun (at-least-once) cannot diverge a committed
        // history.
        if start_to_close_exceeded(elapsed, task.start_to_close_ms) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::StartToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        // Honor the workflow's declared/per-call retry cap (issue #1069 P2). By this
        // point the task is ALWAYS frozen: the freeze-or-read branch above sets
        // `defaults_frozen = true` and `task.max_attempts = Some(..)` for a
        // previously-unfrozen row, and a schedule-time-frozen row already carries the
        // cap. So make the freeze invariant SELF-ENFORCING at the read site (issue
        // #1068 P3-1): for a frozen task NEVER fall back to `spec.max_attempts` —
        // re-consulting the mutable registry here would silently defeat the freeze if
        // a frozen row ever held NULL. The spec fallback is kept ONLY for the
        // unfrozen case, which is unreachable in practice (the branch above always
        // freezes first) but left as a defensive read. Clamp to `>= 1` to mirror
        // `ActivitySpec::new` (a body always runs once).
        debug_assert!(
            task.max_attempts.is_some(),
            "a frozen task must carry a resolved max_attempts (issue #1068 freeze invariant)",
        );
        let max_attempts = if task.defaults_frozen {
            task.max_attempts.unwrap_or(1).max(1)
        } else {
            task.max_attempts.unwrap_or(spec.max_attempts).max(1)
        };
        // Read the clock seam NOW — after the body has run and returned (Codex
        // #1069 P2, `worker.rs:328`). A policy retry's backoff is measured from
        // THIS instant, not the pre-body cycle `now`.
        let finalize_now = failure_now();

        // Recheck the TOTAL (cross-retry) schedule-to-close deadline at FINALIZE time,
        // for ANY body result — Ok OR Err (issue #378, FINDING A, Codex #1069 P2
        // `worker.rs:236`). This is the FIRST finalize decision, ahead of the
        // Ok-complete / Err-terminal-fail / Err-retry branching in
        // `finalize_activity_result`. The pre-body check above used the cycle-start
        // `now`; the absolute deadline may have ELAPSED WHILE the body ran — a body that
        // started just before its cap and returned after it, or whose deadline expired
        // mid-run. The core Postgres backend enforces `schedule_to_close` on a RUNNING
        // activity via its background timeout scanner (`find_timed_out_tasks` with
        // `TimeoutReason::ScheduleToClose` over RUNNING/PENDING) — timing out the row
        // REGARDLESS of the eventual Ok/Err (a late Ok/Err loses the race against a
        // finalize that finds the task no longer RUNNING). This scanner-less backend
        // enforces the cap entirely inline, so it must seal terminal
        // `ActivityTimedOut { ScheduleToClose }` once the total wall-clock cap has
        // elapsed at finalize time, for ANY result. Covering the Err case too closes two
        // holes the earlier `result.is_ok()`-only recheck left:
        //   - a NON-RETRYABLE / FINAL-ATTEMPT Err past the deadline was recorded as an
        //     ordinary `ActivityFailed` instead of a `ScheduleToClose` timeout; and
        //   - a ZERO-DELAY retry whose deadline elapsed mid-body was requeued and re-ran
        //     past the cap, because the `finalize_within_tx` retry check compares the next
        //     attempt's `run_at` (== the STALE cycle `now` for a zero-delay retry) — not
        //     the POST-body `finalize_now` — against the deadline.
        // Using the POST-body `finalize_now` (NOT the cycle-start `now`) is what catches
        // both. The retry-path check in `finalize_within_tx` remains for the DISTINCT case
        // where the deadline has NOT yet elapsed at finalize time but the next attempt's
        // positive-backoff `run_at` WOULD land at/after it. Terminal, NOT retried — the
        // workflow observes `HistoryMatch::TimedOut` and drives its own branch. The body
        // already ran (at-least-once, side effect done once); we only CHOOSE which
        // terminal event to record, exactly as the Postgres scanner does when it wins the
        // race against a completing/failing activity. Because this seals + `continue`s
        // before `finalize_activity_result`, a timed-out activity never ALSO records
        // `ActivityCompleted`/`ActivityFailed` (no double-record).
        if schedule_to_close_exceeded(finalize_now, task.schedule_to_close_at) {
            finalize_activity_timeout(conn, exec_id, &task, TimeoutType::ScheduleToClose)?;
            produced = true;
            if settle_after_first_terminal {
                break;
            }
            continue;
        }

        if finalize_activity_result(
            conn,
            exec_id,
            &task,
            max_attempts,
            now,
            finalize_now,
            result,
        )? {
            produced = true;
            // A terminal event was appended for this branch. Under an unsettled
            // race, stop so the workflow re-settles and cancels the losers before
            // they are drained (issue #600). A retryable failure returns `false`
            // (requeued, no terminal) and does NOT break — keep draining until one
            // branch genuinely finishes.
            if settle_after_first_terminal {
                break;
            }
        }
    }

    // Fire due timers (each append + mark is atomic — see `fire_timer`).
    for timer_id in queue::due_timers(conn, exec_id, now)? {
        fire_timer(conn, exec_id, &timer_id)?;
        produced = true;
    }

    Ok(produced)
}

/// FREEZE-OR-READ the four resolved activity defaults on a claimed task row (issue
/// #1068). The task row records whether its defaults are FROZEN (resolved once and
/// made immutable).
///
/// - `task.defaults_frozen == true`: the four scalars — `retry_policy`,
///   `max_attempts`, `start_to_close_ms`, and the absolute `schedule_to_close_at` —
///   were resolved at schedule time (or a prior claim) and are already loaded from
///   the row verbatim (a NULL column reads as `None`). This is a no-op: use them
///   AS-IS and do NOT re-consult the mutable registry for them. This is what makes
///   an in-flight activity's retry/deadline contract immutable — a crash/reopen or a
///   re-registration of the activity under a changed spec between attempts cannot
///   alter it (mirroring the Postgres engine, which freezes these onto the
///   task-queue row). Only the activity BODY is resolved from the registry (`spec`).
///
/// - `task.defaults_frozen == false`: the activity was scheduled before it was
///   registered (a late registration — the crate's supported flow: the task waits
///   `PENDING` until the body appears, then a re-drive picks it up), so the defaults
///   were never frozen. Resolve them now from the now-guaranteed-present `spec` (an
///   unregistered activity was released back to `PENDING` by the caller and never
///   reaches here), PERSIST them, and mark the row FROZEN — so any LATER claim
///   (crash/reopen, or a re-registration under a changed spec) reads the SAME values
///   rather than re-resolving against a mutated registry.
///
///   The resolution mirrors the schedule-time freeze in `apply_schedule_activity`
///   and the pre-#1068 claim-time fallback (Codex #1069 P2, `runtime.rs:331`): the
///   command override (persisted as `task.retry_policy` / `.start_to_close_ms` on the
///   TYPED path) wins; the RAW path (no override) falls back to the registered spec's
///   FULL `default_retry_policy` (backoff + `non_retryable_errors`) and
///   `default_start_to_close`. The absolute deadline is anchored to the task's
///   ORIGINAL schedule instant (`scheduled_at`, never mutated by a retry requeue),
///   NOT claim time (which would extend the wall-clock budget by however long the
///   task waited for registration) and NOT `run_at` (which retries bump forward). For
///   a normally-registered activity `scheduled_at` equals the schedule `now`, so the
///   resolved absolute deadline is byte-identical to the pre-#1068 value — no
///   behavior change for the common case.
///
/// Called by [`drain_ready`] **inside the same `BEGIN IMMEDIATE` transaction as the
/// `RUNNING` claim** (issue #1068; Codex #1080 P2): `conn` here is the still-open
/// claim transaction (a `&Transaction` deref-coerces to `&Connection`), so the freeze
/// `UPDATE` and the claim's `RUNNING` flip commit together. This closes the gap where
/// the claim committed `RUNNING` in one transaction and this freeze committed in a
/// SEPARATE one — a crash between them left a committed `RUNNING`+unfrozen row that a
/// reopen reclaimed `PENDING` (still unfrozen) and a re-registration could then freeze
/// against a CHANGED spec. Because the two now commit atomically, there is never a
/// committed `RUNNING`+unfrozen intermediate for a late-registered task: either both
/// commit (`RUNNING`+frozen) or neither does (the row stays `PENDING`+unfrozen, so the
/// freeze happens atomically with whatever spec is registered at the ACTUAL first
/// successful claim).
fn freeze_defaults_at_claim(
    conn: &Connection,
    task: &mut ClaimedTask,
    spec: &ActivitySpec,
) -> SqliteResult<()> {
    if task.defaults_frozen {
        return Ok(());
    }
    if task.retry_policy.is_none() {
        task.retry_policy.clone_from(&spec.default_retry_policy);
    }
    if task.start_to_close_ms.is_none() {
        task.start_to_close_ms = spec
            .start_to_close
            .map(crate::runtime::duration_to_millis_saturating);
    }
    task.schedule_to_close_at = spec.schedule_to_close.map(|d| {
        task.scheduled_at
            .saturating_add(crate::runtime::duration_to_millis_saturating(d))
    });
    // The frozen attempt cap: the command override's cap (persisted as
    // `task.max_attempts`) wins; otherwise the registered spec's derived cap.
    task.max_attempts = Some(task.max_attempts.unwrap_or(spec.max_attempts));

    let retry_json = task
        .retry_policy
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    queue::freeze_task_defaults(
        conn,
        &task.task_id,
        retry_json.as_deref(),
        task.max_attempts,
        task.start_to_close_ms,
        task.schedule_to_close_at,
    )?;
    task.defaults_frozen = true;
    Ok(())
}

/// True iff the recorded `history` contains an **unsettled** activity `ctx.race()`
/// (issue #600; Codex #1069 P1) — an "open" race marker (`race:{seq}`, recorded on
/// first dispatch to fix the branch count) with no matching "winner" marker
/// (`race_winner:{seq}`, recorded by `settle_race` once a winner is known).
///
/// The core lowers an activity/child race onto these two `MarkerRecorded` events
/// (an inline `format!("race:{seq}")` / `format!("race_winner:{seq}")` in
/// `autumn_harvest::context` — there is no exported constant, so the naming
/// convention is matched here, exactly as [`is_signal_timeout_deadline_timer`]
/// matches the `__signal_timeout:` timer-id convention). The timer+signal race
/// shape (`race_timer_signal_impl`) reuses the `wait_for_signal_timeout` machinery
/// and records NO `race:` marker, so it is correctly never detected here (it has no
/// multi-branch drain to gate). Used by `drive_one_cycle` to decide whether
/// [`drain_ready`] must settle after the first terminal on the CURRENT cycle: the
/// open marker is present from the first dispatch cycle onward (once committed),
/// covering backing-off race cycles whose re-dispatch commands carry only
/// `WaitForActivity` and no fresh open marker.
#[must_use]
pub fn history_has_unsettled_activity_race(history: &[WorkflowEvent]) -> bool {
    let mut open: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut settled: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for event in history {
        if let WorkflowEvent::MarkerRecorded { name, .. } = event {
            // Check the winner prefix first; `"race_winner:{seq}"` does not start
            // with `"race:"` (5th char `_` vs `:`), so the two are disjoint, but
            // ordering keeps the intent obvious.
            if let Some(seq) = name.strip_prefix("race_winner:") {
                settled.insert(seq);
            } else if let Some(seq) = name.strip_prefix("race:") {
                open.insert(seq);
            }
        }
    }
    open.iter().any(|seq| !settled.contains(seq))
}

/// True iff this decision cycle's suspension `commands` OPEN an activity `ctx.race()`
/// — a `RecordMarker` whose name starts with `race:` (issue #600; Codex #1069 P1).
///
/// The open marker is pushed as a bookkeeping command on the FIRST race-dispatch
/// cycle, at which point it is not yet in the loaded `history`; on later
/// still-pending cycles it is already in history (see
/// [`history_has_unsettled_activity_race`]). Checking both covers every cycle the
/// race is in flight. A `race_winner:` marker does not start with `race:`, so this
/// never matches the settle cycle's winner marker.
#[must_use]
pub fn commands_open_activity_race(commands: &[WorkflowCommand]) -> bool {
    commands.iter().any(|cmd| {
        matches!(cmd, WorkflowCommand::RecordMarker { name, .. } if name.starts_with("race:"))
    })
}

/// True iff an activity `ctx.race()` is in flight on this decision cycle (issue
/// #600; Codex #1069 P1) — either the loaded `history` still has an unsettled open
/// marker ([`history_has_unsettled_activity_race`]) or this cycle's `commands` open
/// one ([`commands_open_activity_race`], the first-dispatch case before the marker
/// is committed to history). The gate for [`drain_ready`]'s
/// `settle_after_first_terminal`.
#[must_use]
pub fn activity_race_in_flight(history: &[WorkflowEvent], commands: &[WorkflowCommand]) -> bool {
    history_has_unsettled_activity_race(history) || commands_open_activity_race(commands)
}

/// Encode a contained **activity** handler-panic message as the typed
/// `harvest_activity_failure_v1` envelope carrying the engine-reserved
/// [`ERROR_TYPE_HANDLER_PANIC`](autumn_harvest::failure) class (issue #782).
///
/// Mirrors the Postgres worker's private `handler_panic_activity_envelope`
/// verbatim, built from the same `pub` `autumn_harvest::failure` building blocks
/// ([`ActivityFailure::retryable`](autumn_harvest::failure::ActivityFailure::retryable)
/// and
/// [`IntoActivityErrorString::into_error_payload`](autumn_harvest::failure::IntoActivityErrorString)),
/// so a caught panic on this backend produces a byte-identical failure payload.
/// The failure is **retryable**: a caught panic follows the activity's retry
/// policy (honoring a `HandlerPanic`-non-retryable classification) exactly as an
/// ordinary `Err(String)` does.
fn handler_panic_activity_envelope(message: String) -> String {
    use autumn_harvest::failure::{ERROR_TYPE_HANDLER_PANIC, IntoActivityErrorString as _};
    autumn_harvest::failure::ActivityFailure::retryable(ERROR_TYPE_HANDLER_PANIC, message)
        .into_error_payload()
}

/// Encode a `PayloadTooLarge` activity-result-cap violation as the typed
/// `harvest_activity_failure_v1` envelope carrying `error_type = "PayloadTooLarge"`
/// and `non_retryable = true` (issue #252, Codex #1069 `worker.rs:340`).
///
/// This mirrors the Postgres worker's `handle_activity_result` normalization
/// VERBATIM: an oversized SUCCESSFUL result is refused, and instead of an
/// `ActivityCompleted` a NON-RETRYABLE `PayloadTooLarge` `ActivityFailed` is
/// recorded (so the workflow observes the same failure the Postgres worker would,
/// and no unbounded result row is ever stored). Routing it through the SAME
/// terminal-failure path as [`finalize_within_tx`]'s `Err` arm means the terminal
/// event's typed metadata (`error_type = "PayloadTooLarge"`, `non_retryable = true`,
/// the human `message`) is derived by the SAME `parse_error_payload_full` decode
/// the Postgres `finalize_activity_failure` uses — keeping the histories
/// byte-equivalent. Built from the same `pub` `autumn_harvest::failure` primitives
/// (`ActivityFailure::non_retryable(...).into_error_payload()`) the core uses, so
/// no core export is needed.
fn payload_too_large_activity_envelope(activity_name: &str, observed: u64, cap: u64) -> String {
    use autumn_harvest::failure::IntoActivityErrorString as _;
    autumn_harvest::failure::ActivityFailure::non_retryable(
        "PayloadTooLarge",
        format!(
            "activity '{activity_name}' result exceeds cap: {observed} bytes (cap {cap} bytes)"
        ),
    )
    .into_error_payload()
}

/// True iff `timer_id` is the deadline timer of a `wait_for_signal_timeout` /
/// `receive_signal_timeout` race for the signal named `signal_name`.
///
/// The core arms that deadline timer with the id `__signal_timeout:{seq}:{name}`
/// (an inline `format!` in `autumn_harvest::context`, issue #476 — there is no
/// exported constant, so the prefix convention is matched here). Only THAT timer
/// may be recorded ahead of the signal in [`ingest_awaited_signal`]; an unrelated
/// durable timer armed in the same batch must not (see the call site).
///
/// Parsed structurally — strip the `__signal_timeout:` prefix, then `split_once`
/// on the seq's colon so the remainder is the signal name — rather than a `LIKE`
/// pattern, so a signal name that itself contains `:` (or a `LIKE` metacharacter
/// like `%`/`_`) still matches exactly.
fn is_signal_timeout_deadline_timer(timer_id: &str, signal_name: &str) -> bool {
    timer_id
        .strip_prefix("__signal_timeout:")
        .and_then(|rest| rest.split_once(':'))
        .is_some_and(|(_seq, name)| name == signal_name)
}

/// Ingest one awaited signal chronologically against the execution's due timers
/// (issue #476), mirroring the Postgres `merge_wake_events`.
///
/// Consumes the oldest undelivered signal named `signal_name` (if any) and
/// appends its `SignalReceived` — but the **race deadline timer of THIS wait**
/// (`__signal_timeout:{seq}:{signal_name}`) is fired (`TimerFired`) **first** when
/// its `fire_at` is at or before the signal's `received_at`, so a signal delivered
/// *after* an expired `wait_for_signal_timeout` deadline can never retroactively
/// win the signal-or-deadline race. Two kinds of timer are deliberately NOT fired
/// ahead of the signal: (1) the race deadline timer whose deadline is *after* the
/// signal arrived (left for the ordinary [`drain_ready`] pass so an on-time signal
/// still wins even when the run is only driven past the deadline), and (2) any
/// **unrelated** durable timer (e.g. a `join!`ed `ctx.timer("t", 5)`) — firing it
/// before the signal would wedge replay (see the scoping note at the call site).
/// Runs entirely inside the caller's cycle transaction (`conn` is a `&tx`).
///
/// Returns `true` iff any event was appended (a signal was consumed and/or a due
/// deadline timer fired).
pub fn ingest_awaited_signal(
    conn: &Connection,
    exec_id: ExecutionId,
    signal_name: &str,
    now: i64,
) -> SqliteResult<bool> {
    // Peek (and eagerly deserialize) BEFORE mutating: a corrupt payload rolls the
    // whole cycle transaction back with the signal still undelivered (no timer
    // fired, nothing marked) rather than silently dropped.
    let Some(signal) = store::peek_pending_signal(conn, exec_id, signal_name)? else {
        return Ok(false);
    };
    // Fire the RACED signal-timeout deadline timer for THIS wait — but ONLY it —
    // ahead of the signal when its deadline expired at or before the signal
    // arrived (issue #476; Codex #1069 P2, runtime.rs:699). `due_timers_before_signal`
    // returns *every* due timer whose deadline is `<= received_at`, but only the
    // `__signal_timeout:{seq}:{signal_name}` deadline timer of this
    // `wait_for_signal_timeout` race may be recorded before the `SignalReceived`.
    // An UNRELATED durable timer armed in the same batch — e.g.
    // `join!(ctx.wait_for_signal("go"), ctx.timer("t", 5))` — must NOT fire first:
    // the core `match_signal` path does not skip an interleaved `TimerFired`, so a
    // `TimerFired(t)` recorded before `SignalReceived(go)` wedges replay (the run
    // stays parked waiting even though the signal row was delivered). Leave any
    // such unrelated due timer to the ordinary [`drain_ready`] pass, which fires
    // it *after* this signal at a history position the workflow's own timer-await
    // consumes.
    for timer_id in queue::due_timers_before_signal(conn, exec_id, now, signal.received_at)? {
        if is_signal_timeout_deadline_timer(&timer_id, signal_name) {
            fire_timer_within_tx(conn, exec_id, &timer_id)?;
        }
    }
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::SignalReceived {
            signal_name: signal_name.to_string(),
            payload: signal.payload,
        },
    )?;
    store::mark_signal_delivered(conn, signal.seq)?;
    Ok(true)
}

/// Finalize one activity attempt in a **single transaction** (AC7): the audit
/// record, the terminal event append (on success or retry-exhaustion), and the
/// task-row transition (`DONE` or requeue) commit together or not at all.
///
/// Returns `true` if a terminal event was appended (i.e. the run may progress).
/// A crash before this commits leaves the task `RUNNING` for orphan reclaim; a
/// crash after leaves a clean `DONE` row and a terminal-only history.
pub fn finalize_activity_result(
    conn: &mut Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    failure_now: i64,
    result: Result<serde_json::Value, String>,
) -> SqliteResult<bool> {
    let tx = conn.transaction()?;
    let produced = finalize_within_tx(&tx, exec_id, task, max_attempts, now, failure_now, result)?;
    tx.commit()?;
    Ok(produced)
}

/// The transactional body of [`finalize_activity_result`], factored out so the
/// atomic unit (record-attempt + terminal-event + task-transition) can be driven
/// directly inside a caller-owned transaction — the finalize atomicity tests
/// invoke this and then *drop the transaction without committing* to prove the
/// three writes roll back together.
pub fn finalize_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    max_attempts: u32,
    now: i64,
    failure_now: i64,
    result: Result<serde_json::Value, String>,
) -> SqliteResult<bool> {
    // Enforce the activity-result cap (issue #252, Codex #1069 `worker.rs:340`).
    // Mirror the Postgres worker's `handle_activity_result` normalization: an
    // oversized SUCCESSFUL result is NOT stored as `ActivityCompleted` (which would
    // bloat history and make the workflow observe a success other backends reject);
    // it is normalized here into a NON-RETRYABLE `PayloadTooLarge` `ActivityFailure`
    // envelope and routed through the terminal-failure path below (a non-retryable
    // failure never retries → terminal on the first attempt, exactly like the
    // Postgres path). Normalizing BEFORE `record_attempt` keeps the whole function
    // consistent — the audit row, the terminal decision, and the appended
    // `ActivityFailed { error_type: "PayloadTooLarge", non_retryable: true, .. }`
    // event all reflect the same outcome. The reused core context already enforces
    // the activity-INPUT cap at schedule time; the result cap is the one boundary
    // the executor leaves to the worker (`with_payload_caps(.., 0 /*result*/, ..)`),
    // so it must be enforced here. This backend has no per-activity result-cap
    // override (like the input cap, which uses the global), so the GLOBAL
    // `DEFAULT_MAX_ACTIVITY_RESULT_BYTES` (2 MiB) is the faithful cap — the same
    // constant the core resolves each activity's cap against. A zero cap = uncapped.
    let result = match result {
        Ok(output) if DEFAULT_MAX_ACTIVITY_RESULT_BYTES > 0 => {
            let observed = serde_json::to_string(&output).map_or(0, |s| s.len() as u64);
            if observed > DEFAULT_MAX_ACTIVITY_RESULT_BYTES {
                Err(payload_too_large_activity_envelope(
                    &task.name,
                    observed,
                    DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
                ))
            } else {
                Ok(output)
            }
        }
        other => other,
    };

    let attempt_num = task.attempt + 1;
    store::record_attempt(conn, exec_id, &task.name, attempt_num, &result)?;

    match result {
        Ok(output) => {
            store::append_event(
                conn,
                exec_id,
                &WorkflowEvent::ActivityCompleted {
                    activity_id: task.activity_id,
                    output,
                },
            )?;
            queue::finish_task(conn, &task.task_id)?;
            Ok(true)
        }
        Err(error) => {
            // Classify the failure exactly as the core worker does
            // (`failure::classify_activity_error`, issue #227): a typed
            // `ActivityFailure` payload carrying `non_retryable: true`, OR a
            // failure matching the resolved retry policy's `non_retryable_errors`
            // list (incl. legacy `Err(String)` values), must skip remaining
            // retries and fail terminally. Without this the SQLite worker would
            // retry a non-retryable error up to `max_attempts` — diverging from
            // Postgres semantics and potentially repeating side effects (issue
            // #1069 P2, Codex `runtime.rs:985`). `parse_typed_payload` returns
            // `Some` only for the typed envelope, so a plain `Err(String)` never
            // synthesizes an `"Error"` class the policy could accidentally match
            // (the back-compat guarantee from #227).
            let typed = autumn_harvest::failure::parse_typed_payload(&error);
            let payload_non_retryable = typed.as_ref().is_some_and(|f| f.non_retryable);
            let typed_error_type = typed.as_ref().map(|f| f.error_type.as_str());
            let policy_non_retryable = task
                .retry_policy
                .as_ref()
                .is_some_and(|p| p.is_non_retryable(typed_error_type, &error));
            let non_retryable = payload_non_retryable || policy_non_retryable;

            if attempt_num < max_attempts && !non_retryable {
                // Retryable: bump the attempt counter and requeue, honoring the
                // WHOLE persisted retry policy (`initial_interval`,
                // `backoff_coefficient`, `max_interval`) via the shared core helper
                // `policy::compute_retry_delay` — NOT an immediate requeue at `now`
                // (issue #1069 P2, Codex `runtime.rs:985`). `attempt_num` is 1-based
                // (1 after the first failure), matching `compute_retry_delay`'s
                // `attempt` (exp = attempt - 1), so the first retry waits
                // `initial_interval`. NOT recorded in the replayable event log (see
                // module docs).
                //
                // ANCHOR: the backoff is measured from `failure_now` — the instant
                // the body actually failed — NOT the pre-body cycle-start `now`
                // (Codex #1069 P2, `worker.rs:328`). A body that ran for real time
                // before failing schedules its next attempt `delay` after it failed,
                // matching Postgres (which computes the delay AFTER handling the
                // result); the old cycle-`now` anchor made a retry ready too early
                // (or immediately, if the body ran longer than the delay). A delayed
                // requeue sets `run_at` in the future, so `claim_next_ready_task_tx`
                // (`run_at <= now`) will NOT re-claim it until the driver advances
                // the clock past the deadline — the workflow blocks on the
                // backing-off activity (see `classify_block`) rather than
                // busy-retrying.
                //
                // A ZERO computed delay is an IMMEDIATE retry and MUST requeue at
                // the CYCLE-START `now`, so `claim_next_ready_task_tx(now)` re-claims it
                // in THIS same drain pass and the retry sequence converges in one
                // call. This covers both the raw/no-policy path (`retry_policy ==
                // None`) AND a zero-delay policy (e.g. `RetryPolicy::fixed(n, 0ms)`,
                // "retry n times with no backoff"). Anchoring an immediate retry to
                // `failure_now` instead (which is `>= now` under the wall clock)
                // would push `run_at` past `now`, so the loop would NOT re-claim it
                // this pass, `classify_block` would report `WaitingTimer`, and
                // `run_until_blocked` would return early — regressing the
                // converge-in-one-call contract. `failure_now` therefore anchors
                // ONLY a genuine, POSITIVE backoff delay (FIX A).
                let run_at = task.retry_policy.as_ref().map_or(now, |p| {
                    let delay = autumn_harvest::policy::compute_retry_delay(
                        p.initial_interval,
                        p.backoff_coefficient,
                        p.max_interval,
                        attempt_num,
                    );
                    let delay_ms = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
                    if delay_ms == 0 {
                        now
                    } else {
                        failure_now.saturating_add(delay_ms)
                    }
                });
                // Enforce the TOTAL (cross-retry) schedule-to-close deadline before
                // requeuing (issue #378, Codex #1069 P2 `runtime.rs:39`). This is the
                // finding's headline: an activity with a retry policy AND a declared
                // total deadline must NOT keep backing off/retrying past that deadline
                // and eventually complete. If the next attempt would land at/after the
                // deadline, seal it terminal `ActivityTimedOut { ScheduleToClose }`
                // instead of requeuing — byte-equivalent to the Postgres
                // `record_schedule_to_close_activity_timeout` path (which appends
                // `ActivityTimedOut { ScheduleToClose }` + `fail_task` instead of
                // `requeue_for_retry` once `now + retry_delay >= deadline`). The failed
                // attempt is already recorded above; the workflow observes
                // `HistoryMatch::TimedOut` and drives its own timeout branch.
                if schedule_to_close_exceeded(run_at, task.schedule_to_close_at) {
                    store::append_event(
                        conn,
                        exec_id,
                        &WorkflowEvent::ActivityTimedOut {
                            activity_id: task.activity_id,
                            timeout_type: TimeoutType::ScheduleToClose,
                        },
                    )?;
                    queue::finish_task(conn, &task.task_id)?;
                    return Ok(true);
                }
                // Requeue, choosing the ready-queue ordering by whether the retry
                // is IMMEDIATE or a real backoff (issue #1068, hardening item 3):
                //
                // - IMMEDIATE (`run_at <= now`, a zero-delay/no-policy retry): the
                //   task would be re-claimable in THIS same drain pass. Move it to
                //   the BACK of the ready queue (fresh highest `seq`) so
                //   `claim_next_ready_task_tx` (`ORDER BY seq`) YIELDS to every ready
                //   SIBLING before returning to the just-failed branch. Without
                //   this, a low-`seq` `ctx.race()` branch that fails-and-retries
                //   with zero delay re-selects itself every retry, monopolizes the
                //   drain, and can settle the race by exhausting/terminal-ing before
                //   a faster sibling ever runs — biasing the winner toward the
                //   lowest index and violating "first branch to finish wins". The
                //   inner loop stays bounded: each branch has finite `max_attempts`,
                //   so round-robin total iterations = sum of branches' attempts.
                //   Outside a race there are no siblings, so the bump is harmless
                //   (the task is re-claimed immediately anyway).
                // - BACKOFF (`run_at > now`, a positive delay): the task already
                //   yields naturally — its future `run_at` keeps it out of this
                //   pass's `run_at <= now` claim window — so keep its `seq`
                //   unchanged and leave the existing behavior byte-for-byte intact.
                if run_at <= now {
                    queue::requeue_task_to_back(conn, &task.task_id, attempt_num, run_at)?;
                } else {
                    queue::requeue_task(conn, &task.task_id, attempt_num, run_at)?;
                }
                Ok(false)
            } else {
                // Terminal (attempts exhausted OR non-retryable): the failure is
                // workflow-visible, so it goes into the event log. DECODE the
                // failure envelope (`parse_error_payload_full`, issue #1069 P2,
                // Codex `worker.rs:299`) so the terminal `ActivityFailed` carries
                // the SAME typed metadata the Postgres worker's
                // `finalize_activity_failure` records — `error_type` / `details` /
                // `non_retryable` plus the human `message` (never the raw
                // `harvest_activity_failure_v1` envelope) — keeping typed-failure
                // histories byte-equivalent across backends (AC5). A legacy
                // `Err(String)` decodes to `error_type = "Error"`,
                // `non_retryable = false`, `details = None`, `message = error`, so
                // its stored form is unchanged from before this fix.
                let failure = autumn_harvest::failure::parse_error_payload_full(&error);
                store::append_event(
                    conn,
                    exec_id,
                    &WorkflowEvent::ActivityFailed {
                        activity_id: task.activity_id,
                        error: failure.message,
                        attempt: attempt_num,
                        error_type: failure.error_type,
                        non_retryable: failure.non_retryable,
                        details: failure.details,
                    },
                )?;
                queue::finish_task(conn, &task.task_id)?;
                Ok(true)
            }
        }
    }
}

/// True iff a body's real wall-clock `elapsed` exceeded its start-to-close
/// `budget_ms` (issue #1069 P2).
///
/// `None` budget = no start-to-close timeout (unbounded — never exceeded, the
/// prior behavior for every activity without a declared/per-call timeout). A
/// non-positive budget is also treated as unbounded (defensive: the core never
/// emits a `<= 0` start-to-close, and a `0` budget would otherwise time out every
/// body). The comparison is strict (`>`), so a body that finishes exactly at its
/// budget completes normally rather than timing out.
#[must_use]
pub fn start_to_close_exceeded(elapsed: std::time::Duration, budget_ms: Option<i64>) -> bool {
    match budget_ms {
        Some(ms) if ms > 0 => elapsed.as_millis() > u128::try_from(ms).unwrap_or(u128::MAX),
        _ => false,
    }
}

/// True iff an absolute epoch-millisecond instant `at_ms` has reached or passed an
/// activity's ABSOLUTE total (cross-retry) `schedule_to_close` deadline (issue #378,
/// Codex #1069 P2 `runtime.rs:39`).
///
/// `None` deadline = no total cap (unbounded — never exceeded, the prior behavior
/// for every activity without a declared `schedule_to_close`). A non-positive
/// deadline is also treated as no cap (defensive: the core never emits a `<= 0`
/// schedule-to-close, and an absolute deadline is a real epoch-ms far above zero).
///
/// The comparison is `>=` (reached OR passed), mirroring the Postgres
/// `schedule_to_close_deadline_exceeded` contract (`now + retry_delay >= deadline`):
/// - the **pre-run** check in [`drain_ready`] passes the cycle `now` — a task drained
///   at/after its deadline is timed out before its body runs (the "idle past the
///   deadline, then drained" case; mirrors the Postgres scanner catching a `PENDING`
///   row past deadline);
/// - the **retry** check in [`finalize_within_tx`] passes the next attempt's `run_at`
///   — a retry whose next attempt (after back-off) would land at/after the deadline is
///   sealed terminal instead of requeued (the finding's headline: an activity that
///   would otherwise keep backing off/retrying past its declared total deadline).
#[must_use]
pub const fn schedule_to_close_exceeded(at_ms: i64, deadline_ms: Option<i64>) -> bool {
    match deadline_ms {
        Some(deadline) if deadline > 0 => at_ms >= deadline,
        _ => false,
    }
}

/// Finalize an activity that exceeded its start-to-close budget (issue #1069 P2)
/// in a **single transaction** (AC7): record the timed-out attempt in the audit
/// log, append the terminal `ActivityTimedOut` event, and mark the task `DONE`,
/// together or not at all.
///
/// **Terminal — no requeue.** This mirrors the Postgres `enforce_activity_timeout`,
/// which appends `ActivityTimedOut` and `fail_task`s without requeuing: a
/// start-to-close timeout is workflow-visible (`HistoryMatch::TimedOut`), not an
/// attempt-level retry. The workflow drives its own timeout branch.
pub fn finalize_activity_timeout(
    conn: &mut Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    timeout_type: TimeoutType,
) -> SqliteResult<()> {
    let tx = conn.transaction()?;
    finalize_timeout_within_tx(&tx, exec_id, task, timeout_type)?;
    tx.commit()?;
    Ok(())
}

/// The transactional body of [`finalize_activity_timeout`], factored out so the
/// atomic unit (audit-attempt + terminal `ActivityTimedOut` + `DONE`) can be
/// driven inside a caller-owned transaction and its roll-back-together atomicity
/// asserted directly.
pub fn finalize_timeout_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    task: &ClaimedTask,
    timeout_type: TimeoutType,
) -> SqliteResult<()> {
    let attempt_num = task.attempt + 1;
    // A timed-out attempt is an attempt outcome — record it in the per-attempt
    // audit log (mirroring `finalize_within_tx`, which always records the attempt).
    // The message names the specific timeout kind so a `ScheduleToClose` (total
    // cross-retry deadline, issue #378) attempt is not mislabelled as
    // `StartToClose` in the audit log.
    let timeout_reason = format!("activity exceeded its {timeout_type} timeout");
    store::record_attempt(conn, exec_id, &task.name, attempt_num, &Err(timeout_reason))?;
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::ActivityTimedOut {
            activity_id: task.activity_id,
            timeout_type,
        },
    )?;
    queue::finish_task(conn, &task.task_id)?;
    Ok(())
}

/// Fire one due timer atomically: append `TimerFired` and mark the timer row
/// `fired` in a single transaction, so a crash never records the fired event
/// without flipping the row — which would both re-fire the timer on restart and
/// leave a stray non-lifecycle `TimerFired` in history.
pub fn fire_timer(conn: &mut Connection, exec_id: ExecutionId, timer_id: &str) -> SqliteResult<()> {
    let tx = conn.transaction()?;
    fire_timer_within_tx(&tx, exec_id, timer_id)?;
    tx.commit()?;
    Ok(())
}

/// The transactional body of [`fire_timer`], factored out so the append-vs-flag
/// atomicity can be driven inside a caller-owned transaction (the timer-fire
/// atomicity test invokes this then *drops the transaction* to prove both writes
/// roll back together — a crash-between-append-and-flag double-fire is
/// impossible).
pub fn fire_timer_within_tx(
    conn: &Connection,
    exec_id: ExecutionId,
    timer_id: &str,
) -> SqliteResult<()> {
    store::append_event(
        conn,
        exec_id,
        &WorkflowEvent::TimerFired {
            timer_id: TimerId::new(timer_id.to_string()),
        },
    )?;
    queue::mark_timer_fired(conn, exec_id, timer_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use autumn_harvest::ActivityExecId;
    use autumn_harvest::policy::RetryPolicy;
    use rusqlite::Connection;

    use autumn_harvest::ExecutionId;

    use std::time::Duration;

    use super::{
        commands_open_activity_race, drain_ready, finalize_within_tx, fire_timer,
        fire_timer_within_tx, handler_panic_activity_envelope, history_has_unsettled_activity_race,
        ingest_awaited_signal, is_signal_timeout_deadline_timer,
        payload_too_large_activity_envelope, schedule_to_close_exceeded, start_to_close_exceeded,
    };
    use crate::queue::{self, ClaimedTask};
    use crate::runtime::ActivitySpec;
    use crate::{schema, store};

    /// A `PENDING`, immediately-claimable activity task enqueued at
    /// `scheduled_at == run_at == 0` — the raw seed for [`drain_ready`] tests (unlike
    /// [`seed_running_task`], which pre-flips the row to `RUNNING` and hand-builds a
    /// `ClaimedTask`). `drain_ready` claims it, resolves `schedule_to_close_at` from
    /// the registered spec, runs the body, and finalizes — so a finalize-time recheck
    /// exercises the real path end to end.
    fn seed_pending_activity(conn: &Connection, exec: ExecutionId, name: &str) {
        // Seed an UNFROZEN task (defaults_frozen = false): `drain_ready` resolves the
        // defaults (incl. `schedule_to_close_at`) from the registered spec at claim and
        // freezes them — exercising the claim-time freeze path end to end.
        queue::enqueue_activity(
            conn,
            exec,
            ActivityExecId::new(),
            name,
            &serde_json::json!({}),
            "default",
            0,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
    }

    /// An `ActivitySpec` with a declared total (cross-retry) `schedule_to_close` of
    /// `deadline_ms` and a body running `body`. Sets the `pub(crate)` field the
    /// info-based `register_activity` would otherwise populate (the hand-made
    /// [`ActivitySpec::new`] leaves it `None`).
    fn spec_with_schedule_to_close(
        deadline_ms: u64,
        body: impl Fn(serde_json::Value) -> Result<serde_json::Value, String> + Send + Sync + 'static,
    ) -> ActivitySpec {
        let mut spec = ActivitySpec::new(1, body);
        spec.schedule_to_close = Some(Duration::from_millis(deadline_ms));
        spec
    }

    /// A two-valued clock seam for the finalize-time-recheck drain tests: returns
    /// `before` on the FIRST read and `after` on every read thereafter. Now that the
    /// PRE-RUN `schedule_to_close` check also reads the clock (Codex #1069 P2,
    /// `worker.rs:194`), a single constant value past the deadline would seal the
    /// activity BEFORE its body runs — which is the NEW pre-run test's subject, not
    /// these. These backstop tests model "the deadline elapses WHILE the body runs":
    /// the first read (the pre-run check) must see `before` (< the deadline, so the
    /// body runs) and the second read (the post-body finalize recheck) must see
    /// `after` (>= the deadline, so the finalize recheck seals it). So the clock
    /// advances ACROSS the body, exactly as a real wall clock does.
    fn advancing_finalize_clock(before: i64, after: i64) -> impl Fn() -> i64 + Send + Sync {
        let reads = std::sync::atomic::AtomicUsize::new(0);
        move || {
            if reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                before
            } else {
                after
            }
        }
    }

    /// Requeue `run_at` (epoch-ms) of the single task after one retryable failure,
    /// finalized with a distinct cycle-start `now` and failure-time `failure_now`
    /// and the given retry `policy`. Asserts the failure was a (non-terminal) retry.
    fn requeued_run_at(policy: Option<RetryPolicy>, now: i64, failure_now: i64) -> i64 {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let mut task = seed_running_task(&conn, exec, "act");
        task.retry_policy = policy;
        let tx = conn.transaction().unwrap();
        let produced =
            finalize_within_tx(&tx, exec, &task, 3, now, failure_now, Err("boom".into())).unwrap();
        tx.commit().unwrap();
        assert!(!produced, "a retryable failure requeues (not terminal)");
        conn.query_row(
            "SELECT run_at FROM harvest_tasks WHERE task_id = ?1",
            [&task.task_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn open() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn
    }

    fn seed_running_task(conn: &Connection, exec: ExecutionId, name: &str) -> ClaimedTask {
        let act = ActivityExecId::new();
        queue::enqueue_activity(
            conn,
            exec,
            act,
            name,
            &serde_json::json!({}),
            "default",
            0,
            None,
            None,
            None,
            None,
            false,
        )
        .unwrap();
        conn.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();
        let task_id: String = conn
            .query_row("SELECT task_id FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        // A hand-built ClaimedTask that bypasses the claim SELECT (these tests drive
        // `finalize_within_tx`/`finalize_activity_result` directly). `defaults_frozen`
        // is irrelevant on this path — the freeze-or-read branch lives in `drain_ready`,
        // which these tests do not call — so it is left at the default `false`.
        ClaimedTask {
            task_id,
            activity_id: act,
            name: name.to_string(),
            input: serde_json::json!({}),
            attempt: 0,
            max_attempts: None,
            retry_policy: None,
            start_to_close_ms: None,
            scheduled_at: 0,
            schedule_to_close_at: None,
            defaults_frozen: false,
        }
    }

    // AC7: the terminal finalize is atomic — on rollback NEITHER the
    // ActivityCompleted event nor the DONE transition persists, leaving the task
    // RUNNING (re-runnable after orphan reclaim).
    #[test]
    fn finalize_rolls_back_terminal_event_and_task_transition_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(&tx, exec, &task, 1, 0, 0, Ok(serde_json::json!("done"))).unwrap();
            // Drop `tx` WITHOUT commit → rollback.
        }

        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "no ActivityCompleted must survive a rolled-back finalize"
        );
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("RUNNING"),
            "task must stay RUNNING after a rolled-back finalize"
        );
    }

    // AC7: on commit BOTH land together — a clean, terminal-only history and a
    // DONE task row.
    #[test]
    fn finalize_commits_terminal_event_and_done_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        {
            let tx = conn.transaction().unwrap();
            finalize_within_tx(&tx, exec, &task, 1, 0, 0, Ok(serde_json::json!("done"))).unwrap();
            tx.commit().unwrap();
        }

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        assert!(matches!(
            history[0],
            autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
        ));
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("DONE")
        );
    }

    // A retryable failure requeues (no terminal event) and records the attempt —
    // both inside the same finalize transaction.
    #[test]
    fn finalize_retryable_failure_requeues_without_terminal_event() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        let produced =
            super::finalize_activity_result(&mut conn, exec, &task, 3, 0, 0, Err("boom".into()))
                .unwrap();

        assert!(!produced, "a retry is not workflow-visible progress");
        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "a retryable failure must NOT reach the replayable log"
        );
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("PENDING"),
            "a retryable failure requeues the task"
        );
        assert_eq!(store::load_attempts(&conn, exec, "act").unwrap().len(), 1);
    }

    // A due timer fire is atomic on commit: the TimerFired event and the `fired`
    // flag land together.
    #[test]
    fn timer_fire_commits_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        queue::enqueue_timer(&conn, exec, "t1", 0).unwrap();

        fire_timer(&mut conn, exec, "t1").unwrap();

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1);
        assert!(matches!(
            history[0],
            autumn_harvest::WorkflowEvent::TimerFired { .. }
        ));
        assert!(
            !queue::has_unfired_timer(&conn, exec).unwrap(),
            "the fired timer must be marked"
        );
    }

    // Timer-fire atomicity (correction #1): a crash between the TimerFired append
    // and the `fired` flag update rolls BOTH back — the timer stays unfired (no
    // double-fire) and no stray non-lifecycle TimerFired event survives.
    #[test]
    fn timer_fire_rolls_back_event_and_flag_together() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        queue::enqueue_timer(&conn, exec, "t1", 0).unwrap();

        {
            let tx = conn.transaction().unwrap();
            fire_timer_within_tx(&tx, exec, "t1").unwrap();
            // Drop `tx` WITHOUT commit → rollback (simulated crash-between).
        }

        assert!(
            store::load_history(&conn, exec).unwrap().is_empty(),
            "no stray TimerFired event may survive a rolled-back fire"
        );
        assert!(
            queue::has_unfired_timer(&conn, exec).unwrap(),
            "the timer must remain unfired (no double-fire)"
        );
    }

    // FIX 2 (Codex #1069 P2, runtime.rs:715): the start-to-close budget predicate.
    // `None` (and a non-positive) budget is unbounded (never exceeded); a body is
    // over-budget only when it STRICTLY exceeds a positive budget, so a body that
    // finishes exactly at its budget completes normally.
    #[test]
    fn start_to_close_exceeded_truth_table() {
        // No budget → never exceeded (prior behavior for every un-timed activity).
        assert!(!start_to_close_exceeded(Duration::from_secs(10), None));
        // Under budget.
        assert!(!start_to_close_exceeded(Duration::from_millis(5), Some(10)));
        // Exactly at budget → NOT exceeded (strict `>`).
        assert!(!start_to_close_exceeded(
            Duration::from_millis(10),
            Some(10)
        ));
        // Over budget → exceeded.
        assert!(start_to_close_exceeded(Duration::from_millis(11), Some(10)));
        assert!(start_to_close_exceeded(Duration::from_secs(40), Some(1)));
        // A non-positive budget is treated as unbounded (defensive; core never
        // emits `<= 0`), so a `0`/negative budget never times a body out.
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(0)));
        assert!(!start_to_close_exceeded(Duration::from_secs(10), Some(-5)));
    }

    // The TOTAL (cross-retry) schedule-to-close deadline predicate (issue #378,
    // Codex #1069 P2 `runtime.rs:39`). `None`/non-positive = no total cap; the
    // comparison is `>=` (reached OR passed), mirroring the Postgres
    // `schedule_to_close_deadline_exceeded` contract used at BOTH the pre-run check
    // (cycle `now`) and the retry check (next attempt's `run_at`).
    #[test]
    fn schedule_to_close_exceeded_truth_table() {
        // No total deadline → never exceeded (prior behavior for every activity
        // without a declared schedule_to_close).
        assert!(!schedule_to_close_exceeded(1_000, None));
        // Before the deadline.
        assert!(!schedule_to_close_exceeded(999, Some(1_000)));
        // Exactly AT the deadline → exceeded (`>=`, matching Postgres
        // `now + retry_delay >= deadline`).
        assert!(schedule_to_close_exceeded(1_000, Some(1_000)));
        // Past the deadline (e.g. a retry whose next attempt lands 30s out).
        assert!(schedule_to_close_exceeded(31_000, Some(1_000)));
        // A non-positive deadline is treated as no cap (defensive; an absolute
        // epoch-ms deadline is always far above zero).
        assert!(!schedule_to_close_exceeded(1_000, Some(0)));
        assert!(!schedule_to_close_exceeded(1_000, Some(-5)));
    }

    // FIX 1 (Codex #1069 P2, runtime.rs:699): the deadline-timer-name predicate
    // matches ONLY a `__signal_timeout:{seq}:{name}` race timer for the exact
    // signal name — including a name that itself contains a colon — and never an
    // unrelated durable timer.
    #[test]
    fn signal_timeout_deadline_timer_predicate_matches_only_the_raced_timer() {
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:0:go",
            "go"
        ));
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:7:approval",
            "approval"
        ));
        // A signal name that itself contains ':' still matches (structural
        // `split_once`, not a fragile suffix/`LIKE` match).
        assert!(is_signal_timeout_deadline_timer(
            "__signal_timeout:3:ns:go",
            "ns:go"
        ));
        // Wrong signal name → no match (a deadline timer for a DIFFERENT wait must
        // not be reordered ahead of THIS signal).
        assert!(!is_signal_timeout_deadline_timer(
            "__signal_timeout:0:other",
            "go"
        ));
        // An UNRELATED durable timer (`join!(wait_for_signal("go"), timer("t", 5))`)
        // is never a race deadline timer.
        assert!(!is_signal_timeout_deadline_timer("t", "go"));
        assert!(!is_signal_timeout_deadline_timer("heartbeat", "go"));
        // A prefix look-alike that is not the real convention → no match.
        assert!(!is_signal_timeout_deadline_timer(
            "__signal_timeout_go",
            "go"
        ));
    }

    // FIX 1 (Codex #1069 P2, runtime.rs:699): `ingest_awaited_signal` reorders ONLY
    // the raced `__signal_timeout:{seq}:{name}` deadline timer ahead of the signal.
    // An UNRELATED due timer (e.g. a `join!`ed `ctx.timer("t", 5)`) is deliberately
    // LEFT UNFIRED here — firing a `TimerFired(t)` before `SignalReceived(go)` would
    // wedge the core `match_signal` replay (which does not skip an interleaved
    // `TimerFired`). The unrelated timer is instead left for the ordinary
    // `drain_ready` pass to fire AFTER the signal.
    //
    // Seed BOTH a raced deadline timer and an unrelated timer, both due at or
    // before the signal's arrival, then ingest the signal and assert only the
    // deadline timer fired ahead of it. Pre-fix (fire every due-before-signal
    // timer) this test is RED: it also records `TimerFired(t)` before the signal
    // and leaves `t` fired.
    #[test]
    fn ingest_reorders_only_the_raced_deadline_timer_ahead_of_the_signal() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();

        // Signal arrives at t=100.
        store::stage_signal(&conn, exec, "go", &serde_json::json!("v"), 100).unwrap();
        // The raced deadline timer of THIS wait, expired at t=60 (<= 100).
        queue::enqueue_timer(&conn, exec, "__signal_timeout:0:go", 60).unwrap();
        // An UNRELATED durable timer, also due at t=50 (<= 100) — must NOT fire
        // ahead of the signal.
        queue::enqueue_timer(&conn, exec, "t", 50).unwrap();

        // Ingest inside a transaction (the real caller passes a `&tx`); commit so
        // the assertions read the persisted state.
        let tx = conn.unchecked_transaction().unwrap();
        let produced = ingest_awaited_signal(&tx, exec, "go", 200).unwrap();
        tx.commit().unwrap();
        assert!(
            produced,
            "a signal was consumed (and/or the deadline timer fired)"
        );

        let history = store::load_history(&conn, exec).unwrap();
        let fired: Vec<&str> = history
            .iter()
            .filter_map(|e| match e {
                autumn_harvest::WorkflowEvent::TimerFired { timer_id } => Some(timer_id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            fired,
            vec!["__signal_timeout:0:go"],
            "ONLY the raced deadline timer may fire ahead of the signal; the \
             unrelated timer `t` must NOT (pre-fix this also fired `t`):\n{history:?}"
        );

        // The deadline TimerFired is recorded BEFORE the SignalReceived (the timeout
        // wins the race against a late signal); the signal is still recorded.
        let deadline_pos = history
            .iter()
            .position(|e| matches!(e, autumn_harvest::WorkflowEvent::TimerFired { .. }))
            .expect("the deadline timer fired");
        let signal_pos = history
            .iter()
            .position(|e| matches!(e, autumn_harvest::WorkflowEvent::SignalReceived { .. }))
            .expect("the signal was consumed");
        assert!(
            deadline_pos < signal_pos,
            "the raced deadline TimerFired must precede the SignalReceived:\n{history:?}"
        );

        // The unrelated timer `t` is still armed (unfired) — left for the ordinary
        // drain pass, which fires it at a position the workflow's own timer-await
        // consumes AFTER the signal.
        let t_fired: i64 = conn
            .query_row(
                "SELECT fired FROM harvest_timers WHERE exec_id = ?1 AND timer_id = 't'",
                rusqlite::params![exec.to_string()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            t_fired, 0,
            "the unrelated timer `t` must remain unfired after ingest"
        );
    }

    // FIX A (Codex #1069 P2, worker.rs:328): a POLICY retry's backoff anchors to the
    // FAILURE time (`failure_now`) — the instant the body actually returned — NOT the
    // pre-body cycle-start `now`. A body that consumed real time before failing must
    // schedule its next attempt `delay` after the FAILURE, matching Postgres (which
    // computes the delay AFTER handling the result), never after the stale cycle
    // start.
    //
    // RED pre-fix: `run_at = now + delay` used the cycle-start `now`, so the retry
    // became ready `delay` after cycle start regardless of how long the body ran.
    #[test]
    fn retry_backoff_anchors_to_failure_time_not_cycle_start() {
        // Cycle starts at 1s; the body runs and fails 45s later (failure_now = 46s).
        // fixed(60s) → the retry is armed 60s after the FAILURE (106s), not after the
        // cycle start (61s).
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(3, Duration::from_secs(60))),
            1_000,
            46_000,
        );
        assert_eq!(
            run_at,
            46_000 + 60_000,
            "backoff must anchor to failure_now (46s) + 60s, not cycle-start (1s) + 60s"
        );
        assert_ne!(
            run_at,
            1_000 + 60_000,
            "must NOT anchor to the cycle-start now"
        );

        // A body that ran LONGER than the delay (fixed 10s, but the body failed 45s
        // after cycle start) must NOT be immediately ready: run_at = failure_now + 10s
        // = 56s, strictly AFTER the failure instant (46s). Pre-fix run_at = 1s + 10s =
        // 11s, which is already <= failure_now (46s) — i.e. "ready" the moment the
        // body failed (Postgres never does this).
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(3, Duration::from_secs(10))),
            1_000,
            46_000,
        );
        assert_eq!(run_at, 46_000 + 10_000);
        assert!(
            run_at > 46_000,
            "a retry whose delay is shorter than the body runtime must still be in the \
             future relative to the failure instant, not immediately ready"
        );
    }

    // FIX A invariant preserved: the RAW/no-policy path still requeues at the
    // CYCLE-START `now` (delay 0, ignoring `failure_now`), so `claim_next_ready_task_tx`
    // re-claims it in the SAME drain pass and the retry sequence converges in one
    // cycle. Anchoring it to `failure_now` (>= now under the wall clock) would push
    // `run_at` past `now`, break the converge-in-one-pass contract, and make
    // `run_until_blocked` return `WaitingTimer` early.
    #[test]
    fn raw_path_retry_requeues_at_cycle_now_ignoring_failure_now() {
        // failure_now (99_999) is far ahead of the cycle-start now (1_000). The raw
        // path must IGNORE it and requeue at the cycle-start now.
        let run_at = requeued_run_at(None, 1_000, 99_999);
        assert_eq!(
            run_at, 1_000,
            "raw/no-policy path requeues at cycle-start now, never at failure_now"
        );
    }

    // FIX A refinement: a ZERO-delay POLICY (`fixed(n, 0ms)`, "retry n times with no
    // backoff") is an IMMEDIATE retry and must requeue at the CYCLE-START `now`, not
    // `failure_now`. Anchoring a 0-delay retry to `failure_now` (> now under the wall
    // clock) would defer the immediate retry past the drain pass and make
    // `run_until_blocked` return `WaitingTimer` instead of converging in one call —
    // the regression `declared_retry_policy_raises_attempts_over_registered_spec`
    // (a wall-clock, 0ms-policy convergence test) catches end-to-end.
    #[test]
    fn zero_delay_policy_retry_requeues_at_cycle_now_not_failure_now() {
        let run_at = requeued_run_at(
            Some(RetryPolicy::fixed(4, Duration::from_millis(0))),
            1_000,
            99_999,
        );
        assert_eq!(
            run_at, 1_000,
            "a zero-delay policy retry is immediate: requeue at cycle-start now so it \
             re-claims this drain pass, never at failure_now"
        );
    }

    // Codex #1069 P2 (`worker.rs:119`): a caught activity panic is encoded as the
    // TYPED `harvest_activity_failure_v1` envelope carrying `error_type =
    // "HandlerPanic"` — NOT a plain string. This is what makes the finalize path
    // (`parse_typed_payload` → retry classification, `parse_error_payload_full` →
    // terminal event metadata) treat a panic like the Postgres worker does: a retry
    // policy that marks `HandlerPanic` non-retryable actually matches, and the
    // terminal `ActivityFailed` records `error_type = "HandlerPanic"` byte-equivalent
    // to the core. Mirrors the core worker's
    // `handler_panic_activity_envelope_is_retryable_handler_panic`.
    #[test]
    fn handler_panic_activity_envelope_encodes_retryable_handler_panic() {
        let payload = handler_panic_activity_envelope("activity boom".to_string());
        // Not a plain string — the retry classifier must see a typed envelope so the
        // `error_type` is available to `RetryPolicy::is_non_retryable`.
        assert!(
            !payload.starts_with("activity boom"),
            "must be a versioned envelope, not the bare panic message"
        );
        let typed = autumn_harvest::failure::parse_typed_payload(&payload)
            .expect("a caught panic encodes a TYPED envelope, not a plain string");
        assert_eq!(
            typed.error_type,
            autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC,
        );
        assert!(
            !typed.non_retryable,
            "a caught panic is retryable (follows the activity's retry policy)"
        );
        // The terminal decode path records HandlerPanic metadata + the human message.
        let full = autumn_harvest::failure::parse_error_payload_full(&payload);
        assert_eq!(
            full.error_type,
            autumn_harvest::failure::ERROR_TYPE_HANDLER_PANIC,
        );
        assert_eq!(full.message, "activity boom", "raw panic message preserved");
        assert!(!full.non_retryable);
    }

    // Issue #252 (Codex #1069 `worker.rs:340`): the result-cap violation is encoded
    // as the TYPED `harvest_activity_failure_v1` envelope with `error_type =
    // "PayloadTooLarge"` and `non_retryable = true`, so the finalize path classifies
    // it terminal (never retried) and records byte-equivalent typed metadata to the
    // Postgres worker's normalization.
    #[test]
    fn payload_too_large_activity_envelope_encodes_non_retryable() {
        let payload = payload_too_large_activity_envelope("act", 4_000, 2_000);
        let typed = autumn_harvest::failure::parse_typed_payload(&payload)
            .expect("a cap violation encodes a TYPED envelope, not a plain string");
        assert_eq!(typed.error_type, "PayloadTooLarge");
        assert!(
            typed.non_retryable,
            "a result-cap violation is non-retryable (terminal on first attempt)"
        );
        let full = autumn_harvest::failure::parse_error_payload_full(&payload);
        assert_eq!(full.error_type, "PayloadTooLarge");
        assert!(full.non_retryable);
        assert!(
            full.message.contains("result exceeds cap"),
            "human message names the boundary: {}",
            full.message
        );
    }

    // Issue #252: an oversized SUCCESSFUL activity result is normalized into a
    // NON-RETRYABLE `PayloadTooLarge` terminal `ActivityFailed` — NOT stored as an
    // (unbounded) `ActivityCompleted`, and NOT retried despite `max_attempts = 3`.
    // Mirrors the Postgres worker's `handle_activity_result` normalization.
    // RED pre-fix: `finalize_within_tx` appended `ActivityCompleted` with the full
    // oversized payload.
    #[test]
    fn finalize_oversized_result_records_non_retryable_payload_too_large_terminal() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        // A JSON string whose SERIALIZED length exceeds the 2 MiB result cap.
        let cap = usize::try_from(autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES)
            .expect("cap fits usize");
        let oversized = serde_json::json!("x".repeat(cap + 1));

        // max_attempts = 3, yet the non-retryable normalization fails terminally
        // on the FIRST attempt (produced = true, a workflow-visible terminal event).
        let produced =
            super::finalize_activity_result(&mut conn, exec, &task, 3, 0, 0, Ok(oversized))
                .unwrap();
        assert!(
            produced,
            "an oversized result is a terminal event, not a retry"
        );

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        match &history[0] {
            autumn_harvest::WorkflowEvent::ActivityFailed {
                error_type,
                non_retryable,
                error,
                ..
            } => {
                assert_eq!(error_type, "PayloadTooLarge");
                assert!(*non_retryable, "must be non-retryable");
                assert!(
                    error.contains("result exceeds cap"),
                    "human message names the boundary: {error}"
                );
                assert!(
                    !error.contains("harvest_activity_failure_v1"),
                    "the raw wire envelope must not leak into the event message"
                );
            }
            other => panic!("expected a PayloadTooLarge ActivityFailed, got {other:?}"),
        }
        assert_eq!(
            queue::task_state(&conn, &task.task_id).unwrap().as_deref(),
            Some("DONE"),
            "the task is terminal (not requeued for retry)"
        );
    }

    // Issue #252: a result UNDER the cap still completes normally — the cap check
    // only diverts oversized successes.
    #[test]
    fn finalize_under_cap_result_completes_normally() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        let task = seed_running_task(&conn, exec, "act");

        let produced = super::finalize_activity_result(
            &mut conn,
            exec,
            &task,
            3,
            0,
            0,
            Ok(serde_json::json!({"ok": true})),
        )
        .unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1);
        assert!(
            matches!(
                history[0],
                autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
            ),
            "an under-cap result completes normally"
        );
    }

    // Codex #1069 P2 (`worker.rs:211`): a body that SUCCEEDS but whose FINALIZE clock
    // has reached the total schedule-to-close deadline is sealed terminal
    // `ActivityTimedOut { ScheduleToClose }` — NOT `ActivityCompleted`. The pre-body
    // check passed (cycle `now = 0 < deadline = 1000`, so the body ran) but the
    // post-body `failure_now` (1500) is at/past the absolute deadline. This is the
    // scanner-less backend's inline analog of the Postgres timeout scanner catching a
    // RUNNING activity past its `schedule_to_close` cap: the late completion loses the
    // race (`complete` finds the row no longer RUNNING and no-ops). The two-valued
    // clock seam is exactly what a real wall-clock cycle observes when the deadline
    // elapses WHILE the body runs.
    //
    // RED pre-fix: `drain_ready` had no finalize-time recheck, so the successful result
    // was finalized as `ActivityCompleted` past the declared total cap.
    #[test]
    fn drain_ready_seals_schedule_to_close_when_deadline_elapses_during_a_successful_body() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // scheduled_at == run_at == 0 → resolved absolute deadline = 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec_with_schedule_to_close(1_000, Ok));

        // Cycle-start now = 0 (< deadline 1000 → pre-body check passes, body runs);
        // post-body finalize clock = 1500 (>= deadline → finalize recheck seals the
        // timeout).
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(&mut conn, exec, 0, &failure_now, &activities, false).unwrap();
        assert!(produced, "a terminal timeout event was appended");

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a body finalized past its total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }}:\n{history:?}"
        );
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, autumn_harvest::WorkflowEvent::ActivityCompleted { .. })),
            "the late success must NOT record ActivityCompleted (RED pre-fix):\n{history:?}"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal (not requeued)"
        );
    }

    // Control: a body that SUCCEEDS whose finalize clock is still BEFORE the total
    // deadline completes normally. The finalize-time recheck only diverts a LATE
    // success; an on-time one keeps the common path byte-identical.
    #[test]
    fn drain_ready_completes_a_successful_body_finalized_before_the_deadline() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec_with_schedule_to_close(1_000, Ok));

        // finalize clock = 500 < deadline 1000 → normal completion.
        let failure_now = || 500_i64;
        let produced = drain_ready(&mut conn, exec, 0, &failure_now, &activities, false).unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert_eq!(history.len(), 1, "exactly one terminal event");
        assert!(
            matches!(
                history[0],
                autumn_harvest::WorkflowEvent::ActivityCompleted { .. }
            ),
            "an under-deadline success completes normally:\n{history:?}"
        );
    }

    // FINDING A (Codex #1069 P2, `worker.rs:236`): the finalize-time schedule-to-close
    // recheck must run for a FAILING body too, not only a successful one. A body that
    // starts before its total deadline but returns `Err` after the deadline has elapsed
    // (a body that ran real time and crossed the cap) would otherwise fall through to the
    // ordinary terminal `ActivityFailed` branch — misreporting a past-cap timeout as an
    // ordinary failure. The core Postgres backend times out ANY RUNNING activity past its
    // `schedule_to_close` via its scanner regardless of the eventual Ok/Err, so this
    // scanner-less backend must seal `ActivityTimedOut { ScheduleToClose }` once the total
    // wall-clock cap has elapsed at finalize time, for ANY result. `max_attempts = 1` so
    // the failing body would otherwise take the terminal `ActivityFailed` branch (not the
    // retry branch): pre-body now = 0 (< deadline 1000, body runs), post-body finalize_now
    // = 1500 (>= deadline → seal timeout).
    //
    // RED pre-fix: the finalize-time recheck was guarded `result.is_ok()`, so an Err body
    // past the deadline recorded a terminal `ActivityFailed` instead of `ActivityTimedOut`.
    #[test]
    fn drain_ready_seals_schedule_to_close_for_a_failing_body_finalized_past_the_deadline() {
        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // scheduled_at == run_at == 0 → resolved absolute deadline = 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        let mut activities = std::collections::HashMap::new();
        activities.insert(
            "act".to_string(),
            spec_with_schedule_to_close(1_000, |_| Err("boom".to_string())),
        );

        // Cycle-start now = 0 (< deadline 1000 → pre-body check passes, body runs);
        // post-body finalize clock = 1500 (>= deadline → generalized finalize recheck
        // seals the timeout even though the body returned Err).
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(&mut conn, exec, 0, &failure_now, &activities, false).unwrap();
        assert!(produced, "a terminal timeout event was appended");

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a FAILING body finalized past its total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }} (RED pre-fix: recorded ActivityFailed):\n{history:?}"
        );
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, autumn_harvest::WorkflowEvent::ActivityFailed { .. })),
            "the past-deadline failure must NOT record ActivityFailed (RED pre-fix):\n{history:?}"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal (not requeued)"
        );
    }

    // FINDING A (Codex #1069 P2, `worker.rs:236`): a ZERO-DELAY retry whose deadline
    // elapsed WHILE the body ran must seal `ActivityTimedOut { ScheduleToClose }`, NOT
    // requeue for another attempt past the cap. The pre-requeue check in
    // `finalize_within_tx` compares the next attempt's `run_at` (== the STALE cycle-start
    // `now` for a zero-delay retry) against the deadline — so with the deadline crossed
    // mid-body, `now < deadline` still held and the task was requeued and re-ran past the
    // cap. The generalized finalize-time recheck uses the POST-body finalize clock, so it
    // catches this: `finalize_now >= deadline` seals the timeout BEFORE the retry branch
    // is reached. `max_attempts = 2` and NO retry policy → the zero-delay (immediate)
    // requeue path (`run_at = now`).
    //
    // RED pre-fix: the finalize-time recheck was `result.is_ok()`-only, so the Err body
    // requeued at the stale cycle `now` (0 < 1000) and re-ran a second attempt past the cap.
    #[test]
    fn drain_ready_seals_schedule_to_close_for_a_zero_delay_retry_finalized_past_the_deadline() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // scheduled_at == run_at == 0 → resolved absolute deadline = 0 + 1000ms = 1000.
        seed_pending_activity(&conn, exec, "act");

        // max_attempts = 2, NO retry policy → the immediate/zero-delay requeue path
        // (`run_at = now`). The body counts its invocations so the RED "retried past the
        // cap" behavior is observable (2 calls) vs GREEN "sealed after one run" (1 call).
        let calls = Arc::new(AtomicUsize::new(0));
        let body_calls = Arc::clone(&calls);
        let spec = {
            let mut s = ActivitySpec::new(2, move |_| {
                body_calls.fetch_add(1, Ordering::SeqCst);
                Err("boom".to_string())
            });
            s.schedule_to_close = Some(Duration::from_secs(1));
            s
        };
        let mut activities = std::collections::HashMap::new();
        activities.insert("act".to_string(), spec);

        // Cycle-start now = 0 (< deadline 1000 → pre-body check passes AND the stale
        // zero-delay run_at = now = 0 would NOT be caught by the retry-path check);
        // post-body finalize clock = 1500 (>= deadline → the generalized finalize recheck
        // seals the timeout).
        let failure_now = advancing_finalize_clock(0, 1_500);
        let produced = drain_ready(&mut conn, exec, 0, &failure_now, &activities, false).unwrap();
        assert!(produced);

        let history = store::load_history(&conn, exec).unwrap();
        assert!(
            history.iter().any(|e| matches!(
                e,
                autumn_harvest::WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a zero-delay retry past the total deadline must seal ActivityTimedOut \
             {{ ScheduleToClose }} (RED pre-fix: requeued at the stale cycle now and \
             retried past the cap):\n{history:?}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the body must run ONCE then be sealed — NOT retried past the cap \
             (RED pre-fix ran it twice)"
        );
        let task_state: String = conn
            .query_row("SELECT state FROM harvest_tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            task_state, "DONE",
            "the timed-out task is terminal, not requeued PENDING"
        );
    }

    // Issue #600 / Codex #1069 P1: the race-in-flight detection helpers that gate
    // `drain_ready`'s settle-after-first-terminal behavior.
    fn marker(name: &str) -> autumn_harvest::WorkflowEvent {
        autumn_harvest::WorkflowEvent::MarkerRecorded {
            name: name.to_string(),
            details: serde_json::json!(2),
        }
    }

    #[test]
    fn history_unsettled_race_detects_open_marker_without_a_winner() {
        // An open race marker with no winner yet → unsettled.
        assert!(history_has_unsettled_activity_race(&[marker("race:0")]));
        // Open + matching winner (same seq) → settled.
        assert!(!history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:0"),
        ]));
        // A winner for a DIFFERENT seq does not settle the open one.
        assert!(history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:1"),
        ]));
        // Two races, one still open.
        assert!(history_has_unsettled_activity_race(&[
            marker("race:0"),
            marker("race_winner:0"),
            marker("race:1"),
        ]));
        // No race markers at all → not in flight.
        assert!(!history_has_unsettled_activity_race(&[
            marker("fan_out:3"),
            marker("side_effect:0"),
        ]));
        assert!(!history_has_unsettled_activity_race(&[]));
    }

    #[test]
    fn commands_open_race_matches_only_the_open_marker() {
        use autumn_harvest::WorkflowCommand;
        let open = WorkflowCommand::RecordMarker {
            name: "race:0".to_string(),
            details: serde_json::json!(2),
        };
        assert!(commands_open_activity_race(std::slice::from_ref(&open)));
        // The winner marker (`race_winner:`) must NOT be mistaken for opening a
        // race — it does not start with `race:`.
        let winner = WorkflowCommand::RecordMarker {
            name: "race_winner:0".to_string(),
            details: serde_json::json!(0),
        };
        assert!(!commands_open_activity_race(std::slice::from_ref(&winner)));
        // A fan-out marker is not a race.
        let fan_out = WorkflowCommand::RecordMarker {
            name: "fan_out:3".to_string(),
            details: serde_json::json!(3),
        };
        assert!(!commands_open_activity_race(std::slice::from_ref(&fan_out)));
        assert!(!commands_open_activity_race(&[]));
    }

    // ── FINDING 1 (Codex #1080 P2): claim + late-registration freeze are atomic ─────
    //
    // The `RUNNING` claim and the first-claim default-freeze commit in ONE
    // transaction, so a crash in the old claim→freeze gap can no longer leave a
    // committed `RUNNING`+unfrozen row — which a reopen would reclaim
    // `PENDING`-still-unfrozen, letting a re-registration under a CHANGED spec
    // re-freeze the NEW defaults and silently alter an already-claimed
    // late-registered task's frozen retry/deadline contract.

    /// `(state, defaults_frozen)` of the single task row for `exec` — the direct
    /// atomic-invariant probe.
    fn state_and_frozen(conn: &Connection, exec: ExecutionId) -> (String, i64) {
        conn.query_row(
            "SELECT state, defaults_frozen FROM harvest_tasks WHERE exec_id = ?1",
            [exec.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap()
    }

    fn seed_exec_with_pending_activity(conn: &Connection, exec: ExecutionId, name: &str) {
        store::insert_execution(
            conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // Enqueued UNFROZEN (defaults_frozen = 0) — the late-registration shape.
        seed_pending_activity(conn, exec, name);
    }

    /// A late-registered task's claim (`RUNNING` flip) and freeze commit ATOMICALLY:
    /// after the single commit the row is `RUNNING` **and** frozen — never
    /// `RUNNING`+unfrozen. Pre-fix the freeze ran in a SEPARATE transaction after the
    /// claim had already committed `RUNNING`, so a committed `RUNNING`+unfrozen state
    /// existed in the gap.
    #[test]
    fn late_registration_claim_and_freeze_are_atomic() {
        let mut conn = open();
        let exec = ExecutionId::new();
        seed_exec_with_pending_activity(&conn, exec, "act");
        let spec = ActivitySpec::new(3, |_| Ok(serde_json::json!(null)));

        // Atomic claim+freeze in ONE transaction, exactly as `drain_ready` does.
        let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
            .unwrap()
            .expect("a ready task");
        assert!(!task.defaults_frozen, "the seeded task is unfrozen");
        super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap();
        tx.commit().unwrap();

        // After the single commit: RUNNING AND frozen — the claim and the freeze
        // landed together. No committed RUNNING+unfrozen state ever existed.
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(state, "RUNNING", "the claim committed RUNNING");
        assert_eq!(frozen, 1, "...and the freeze committed atomically with it");
        assert!(
            !(state == "RUNNING" && frozen == 0),
            "invariant: no committed RUNNING+unfrozen row for a late-registered task",
        );
    }

    /// The falsifiable crash-in-gap immutability test: a crash AFTER the claim's
    /// `RUNNING` flip but BEFORE the freeze commits rolls the WHOLE claim back (both
    /// share one transaction), so the row returns to `PENDING`+unfrozen — never a
    /// committed `RUNNING`+unfrozen intermediate for a reopen+re-registration to
    /// re-freeze against a changed spec. Pre-fix `claim_next_ready_task` committed
    /// `RUNNING` immediately, so the SAME "crash before freeze" left a committed
    /// `RUNNING`+unfrozen row — the exact gap this closes.
    #[test]
    fn late_registration_frozen_defaults_survive_a_crash_in_the_claim_gap() {
        let mut conn = open();
        let exec = ExecutionId::new();
        seed_exec_with_pending_activity(&conn, exec, "act");

        // Model a crash in the claim→freeze gap: open the atomic claim (which flips
        // the row to RUNNING INSIDE the tx) and then CRASH before the freeze/commit —
        // i.e., DROP the transaction without committing. Because claim+freeze share
        // one tx, the drop rolls the RUNNING flip back too.
        {
            let (tx, task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
                .unwrap()
                .expect("a ready task");
            assert!(!task.defaults_frozen);
            // [crash — the freeze never runs and nothing commits]
            drop(tx);
        }

        // The uncommitted claim rolled back: PENDING (re-claimable) and still
        // unfrozen. Critically, no committed RUNNING+unfrozen row was ever left for a
        // reopen (`reclaim_orphaned_running`) + re-registration to re-freeze against a
        // hostile spec.
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            state, "PENDING",
            "the uncommitted claim rolled back — no orphaned RUNNING row",
        );
        assert_eq!(frozen, 0, "still unfrozen — the freeze never committed");
        assert!(
            !(state == "RUNNING" && frozen == 0),
            "a crash in the claim→freeze gap must never leave a committed RUNNING+unfrozen row",
        );

        // Recovery is clean: the row is re-claimable and freezes atomically against
        // whatever spec is registered at the ACTUAL first successful claim.
        let spec = ActivitySpec::new(2, |_| Ok(serde_json::json!(null)));
        let (tx, mut task) = queue::claim_next_ready_task_tx(&mut conn, exec, 0)
            .unwrap()
            .expect("re-claimable after the rolled-back gap");
        super::freeze_defaults_at_claim(&tx, &mut task, &spec).unwrap();
        tx.commit().unwrap();
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(
            (state.as_str(), frozen),
            ("RUNNING", 1),
            "the re-claim freezes atomically",
        );
        assert_eq!(
            task.max_attempts,
            Some(2),
            "frozen from the spec registered at the real first claim",
        );
    }

    /// `reclaim_orphaned_running` flips a stranded RUNNING row to PENDING but must NOT
    /// reset `defaults_frozen`: a frozen reclaimed task stays frozen, so its
    /// retry/deadline contract is not re-resolved against a mutated registry after a
    /// crash/reopen.
    #[test]
    fn reclaim_orphaned_running_preserves_defaults_frozen() {
        let conn = open();
        let exec = ExecutionId::new();
        store::insert_execution(
            &conn,
            exec,
            "wf",
            &exec.to_string(),
            &serde_json::json!(null),
        )
        .unwrap();
        // A FROZEN, stranded-RUNNING task.
        queue::enqueue_activity(
            &conn,
            exec,
            ActivityExecId::new(),
            "act",
            &serde_json::json!({}),
            "default",
            0,
            Some(3),
            None,
            None,
            None,
            true,
        )
        .unwrap();
        conn.execute("UPDATE harvest_tasks SET state = 'RUNNING'", [])
            .unwrap();

        let n = queue::reclaim_orphaned_running(&conn).unwrap();
        assert_eq!(n, 1, "the stranded RUNNING row is reclaimed");
        let (state, frozen) = state_and_frozen(&conn, exec);
        assert_eq!(state, "PENDING", "reclaim flips RUNNING → PENDING");
        assert_eq!(
            frozen, 1,
            "reclaim must NOT reset defaults_frozen (the frozen contract survives)",
        );
    }
}
