//! Replay-determinism proofs for `ctx.timer_business_days` (issue #806).
//!
//! These are the **money tests** for the issue's headline determinism AC:
//!
//! > on replay the recorded fire instant is used verbatim and the calendar is
//! > **not** re-read; an operator calendar edit can never change an
//! > already-armed timer's deadline.
//!
//! ## How each test fails on regression
//!
//! Every test replays a history whose recorded `TimerStarted { duration_secs }`
//! was produced by one calendar, against a replayer configured with a calendar
//! that **would compute a different answer** (or none at all). If the freeze
//! ever stopped being load-bearing — i.e. if the resolution closure re-ran on
//! replay — the recomputed duration would differ and
//! `HistoryMatcher::match_timer_strict`'s exact `!=` comparison surfaces it as
//! `ReplayStatus::NonDeterminismDetected { kind: TimerMismatch, .. }`.
//!
//! That makes the recorded timer duration itself the probe: no separate
//! branch-marker activity is needed, so these tests cannot pass vacuously the
//! way a "workflow completes either way" fixture would.
//!
//! Verified by mutation during development: forcing the closure to recompute on
//! replay turns tests 1–3 RED with exactly that error.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::calendar::BusinessCalendars;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::testing::{NonDeterminismKind, ReplayStatus, WorkflowReplayer};
use autumn_harvest::types::TimerId;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Fixture helpers
// ---------------------------------------------------------------------------

const FREEZE_NAME: &str = "__harvest_business_day:1:sla";

fn utc(s: &str) -> DateTime<Utc> {
    s.parse::<DateTime<Utc>>().expect("valid RFC 3339 instant")
}

const fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

/// The workflow under test: arm a single business-day timer, then complete.
///
/// Deliberately minimal — the *only* thing that varies across replays is the
/// resolved timer duration, so the recorded `TimerStarted` is the sole probe.
fn sla_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let deadline = ctx
            .timer_business_days("sla", 2, "biz")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!({ "escalated_at": deadline }))
    })
}

/// Build the `SideEffectRecorded` payload a live run would have frozen.
fn frozen_resolution(
    anchor: DateTime<Utc>,
    deadline: DateTime<Utc>,
    skipped: &[NaiveDate],
) -> Value {
    serde_json::json!({
        "v": 1,
        "outcome": "resolved",
        "anchor": anchor,
        "deadline": deadline,
        "calendar": "biz",
        "skipped": skipped,
    })
}

/// A complete, already-fired history for `sla_workflow`.
fn history(
    anchor: DateTime<Utc>,
    deadline: DateTime<Utc>,
    skipped: &[NaiveDate],
) -> Vec<WorkflowEvent> {
    let duration_secs = u64::try_from((deadline - anchor).num_seconds().max(0)).expect("in range");
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: anchor,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Custom,
            name: Some(FREEZE_NAME.to_string()),
            value: frozen_resolution(anchor, deadline, skipped),
        },
        WorkflowEvent::TimerStarted {
            timer_id: TimerId::new("sla"),
            duration_secs,
        },
        WorkflowEvent::TimerFired {
            timer_id: TimerId::new("sla"),
        },
    ]
}

/// The canonical fixture: Thu 2026-07-02T09:00Z + 2 business days = Mon 07-06.
fn canonical_history() -> Vec<WorkflowEvent> {
    history(
        utc("2026-07-02T09:00:00Z"),
        utc("2026-07-06T09:00:00Z"),
        &[day(2026, 7, 4), day(2026, 7, 5)],
    )
}

fn replayer(calendars: Option<BusinessCalendars>) -> WorkflowReplayer {
    let base = WorkflowReplayer::new().register_fn("sla_flow", sla_workflow as _);
    match calendars {
        Some(c) => base.with_state(c),
        None => base,
    }
}

// ---------------------------------------------------------------------------
// 1. No calendars registered at all
// ---------------------------------------------------------------------------

/// A replay/canary context legitimately holds **no** `BusinessCalendars` — the
/// snapshot is registered on the live worker's builder, and replay paths default
/// to empty shared state.
///
/// If the resolution closure re-ran on replay it would hit the prologue's
/// "no snapshot registered" error and the workflow would fail; because the value
/// is frozen, the closure is never invoked and the replay succeeds.
#[tokio::test]
async fn replay_business_day_history_with_no_calendars_registered_succeeds() {
    let report = replayer(None).replay_from_events(canonical_history()).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a frozen business-day history must replay with zero calendars registered:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// 2. The calendar was EDITED after the timer was armed
// ---------------------------------------------------------------------------

/// The headline AC: **an operator calendar edit can never change an
/// already-armed timer's deadline.**
///
/// The replay calendar marks the frozen deadline's own date (Mon 2026-07-06) as
/// a holiday, so a recomputation would resolve to Tue 07-07 — a different
/// `duration_secs` — and `match_timer_strict` would report `TimerMismatch`.
#[tokio::test]
async fn replay_business_day_history_with_mutated_calendar_keeps_frozen_deadline() {
    let mutated = BusinessCalendars::new().with_calendar(
        "biz",
        // Every date the original resolution walked over PLUS the deadline
        // itself: a recompute cannot possibly land on 2026-07-06.
        [day(2026, 7, 6), day(2026, 7, 7)],
    );
    let report = replayer(Some(mutated))
        .replay_from_events(canonical_history())
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "an operator calendar edit must NOT move an already-armed deadline:\n{report}"
    );
}

// ---------------------------------------------------------------------------
// 3. Determinism at scale — 1000 DISTINCT fixtures
// ---------------------------------------------------------------------------

/// The issue's success metric: replays to the **identical** fire instant in
/// **100%** of runs, including after the calendar is edited post-arming.
///
/// Each of the 1000 fixtures is genuinely distinct — the anchor's date,
/// time-of-day, and the recorded skip set all vary — and each is replayed under
/// a calendar that excludes the fixture's own deadline date, so a recompute is
/// guaranteed to produce a different answer. A single leak fails the run.
#[tokio::test]
async fn replay_business_day_1000_distinct_fixtures_all_succeed() {
    let base = utc("2026-01-05T00:00:00Z"); // a Monday
    let mut succeeded = 0_usize;

    for i in 0..1000_u32 {
        // Vary the anchor across ~200 calendar days and the full clock face, so
        // weekday, time-of-day, and the weekend-skip pattern all differ.
        let anchor = base
            + chrono::Duration::days(i64::from(i % 200))
            + chrono::Duration::minutes(i64::from(i % 1440));
        // Vary the recorded deadline between 1 and 6 calendar days out. The
        // fixture is self-consistent by construction: the recorded duration is
        // derived from (deadline - anchor), which is exactly what a live run
        // would have recorded for THAT frozen pair.
        let deadline = anchor + chrono::Duration::days(1 + i64::from(i % 6));
        // Vary the recorded skip set too, so no two fixtures share a payload
        // (the doc comment's "genuinely distinct" claim is load-bearing: an
        // all-identical corpus would make 1000 iterations no stronger than 1).
        let skipped: Vec<NaiveDate> = (0..(i % 3))
            .map(|k| (anchor + chrono::Duration::days(i64::from(k) + 1)).date_naive())
            .collect();
        let events = history(anchor, deadline, &skipped);

        // A calendar that excludes the frozen deadline's own date guarantees a
        // recompute would differ.
        let hostile = BusinessCalendars::new().with_calendar("biz", [deadline.date_naive()]);

        let report = replayer(Some(hostile)).replay_from_events(events).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "fixture {i} (anchor {anchor}, deadline {deadline}) leaked a recomputation:\n{report}"
        );
        succeeded += 1;
    }

    assert_eq!(
        succeeded, 1000,
        "100% of randomized fixtures must replay identically"
    );
}

// ---------------------------------------------------------------------------
// 4. The freeze event is load-bearing
// ---------------------------------------------------------------------------

/// Strip the `SideEffectRecorded` freeze and the replay must **diverge**.
///
/// Without this test the three above could all pass with a purely decorative
/// freeze (e.g. if the deadline were somehow recoverable from the timer alone).
#[tokio::test]
async fn replay_business_day_missing_freeze_event_diverges() {
    let mut events = canonical_history();
    events.retain(|e| !matches!(e, WorkflowEvent::SideEffectRecorded { .. }));

    let report = replayer(Some(BusinessCalendars::new().with_calendar("biz", [])))
        .replay_from_events(events)
        .await;
    // Assert the specific classification, not merely "not success" — a bare
    // `!ReplaySucceeded` would also be satisfied by an unrelated failure and
    // could not detect a regression that stopped reporting the missing freeze.
    match &report.status {
        ReplayStatus::NonDeterminismDetected {
            kind,
            expected,
            actual,
            ..
        } => {
            assert_eq!(
                kind,
                &NonDeterminismKind::SideEffectMismatch,
                "a missing freeze must classify as a side-effect mismatch:\n{report}"
            );
            assert!(
                expected.contains("SideEffectRecorded"),
                "the divergence must name the freeze it expected, got {expected:?}"
            );
            assert!(
                actual.contains("TimerStarted"),
                "the divergence must name what it found instead, got {actual:?}"
            );
        }
        other => panic!("expected non-determinism, got {other:?}:\n{report}"),
    }
}

/// A *drifted* freeze (recorded under a different name) is reported as a
/// side-effect mismatch, not silently recomputed.
#[tokio::test]
async fn replay_business_day_renamed_freeze_reports_side_effect_mismatch() {
    let mut events = canonical_history();
    for e in &mut events {
        if let WorkflowEvent::SideEffectRecorded { name, .. } = e {
            *name = Some("__harvest_business_day:1:renamed".to_string());
        }
    }

    let report = replayer(Some(BusinessCalendars::new().with_calendar("biz", [])))
        .replay_from_events(events)
        .await;
    match &report.status {
        ReplayStatus::NonDeterminismDetected { kind, .. } => {
            assert_eq!(
                kind,
                &NonDeterminismKind::SideEffectMismatch,
                "a renamed freeze must classify as a side-effect mismatch:\n{report}"
            );
        }
        other => panic!("expected non-determinism, got {other:?}:\n{report}"),
    }
}

// ---------------------------------------------------------------------------
// 5. A frozen REJECTION replays as the same rejection
// ---------------------------------------------------------------------------

/// A run that failed because its calendar could not cover the span must fail
/// **identically** on replay, even after the operator extends the calendar.
#[tokio::test]
async fn replay_business_day_frozen_rejection_survives_a_calendar_extension() {
    let anchor = utc("2026-07-02T09:00:00Z");
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: anchor,
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Custom,
            name: Some(FREEZE_NAME.to_string()),
            value: serde_json::json!({
                "v": 1,
                "outcome": "rejected",
                "anchor": anchor,
                "calendar": "biz",
                "rejection": { "reason": "coverage_exhausted", "coverage_end": "2026-07-03" },
            }),
        },
        WorkflowEvent::WorkflowFailed {
            error: "business calendar 'biz': resolution requires a date after 2026-07-03, \
                    the last date this calendar covers; extend the calendar's exclusions; \
                    this resolution is frozen in history, so extending the calendar requires \
                    a workflow reset"
                .to_string(),
            error_type: None,
            details: None,
            non_retryable: None,
        },
    ];

    // Replay under a calendar that covers well past the deadline — a recompute
    // would SUCCEED and the workflow would complete instead of failing.
    let generous = BusinessCalendars::new().with_calendar_covering("biz", [], day(2030, 12, 31));
    let report = replayer(Some(generous)).replay_from_events(events).await;
    // The SAME failure, not merely *a* failure: assert the recorded message is
    // reproduced verbatim, so a recompute that failed for some other reason
    // could not pass this test.
    match &report.status {
        ReplayStatus::WorkflowFailed { error, .. } => assert!(
            error.contains("resolution requires a date after 2026-07-03"),
            "the frozen coverage rejection must be reproduced verbatim, got: {error}"
        ),
        other => panic!(
            "a frozen rejection must replay as the same failure even once the \
             calendar is extended, got {other:?}:\n{report}"
        ),
    }
}
