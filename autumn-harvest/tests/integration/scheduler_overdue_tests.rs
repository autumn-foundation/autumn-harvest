#![cfg(feature = "db")]
//! Overdue-schedule detection tests — issue #696.
//!
//! Server-side detection that an *active* schedule is overdue to fire relative
//! to its own cadence (the scheduler loop stalled, `next_run_at` wedged in the
//! past, an HA claim that never released, or all scheduler replicas down).
//!
//! These tests drive the per-shard sampler function
//! (`scheduler::sample_overdue_schedules`) against a real Postgres container
//! and assert the `harvest.schedule.overdue` gauge is emitted correctly. AC3
//! (intentionally-not-firing states) and the at-capacity false-positive guard
//! are exercised in the DB path here; the pure predicate (`schedule_overdue`)
//! is unit-tested without a database in `scheduler.rs`.

use std::sync::Mutex;

use autumn_harvest::scheduler::{
    overdue_schedule_pass, overdue_schedule_samples, sample_overdue_schedules,
};
use autumn_harvest::schema::harvest_schedules;
use autumn_harvest::telemetry::{METRIC_SCHEDULE_OVERDUE, MetricsRecorder};
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── Recording metrics ───────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct RecordingMetrics {
    /// (kind, name, overdue) tuples in emission order.
    overdue: Mutex<Vec<(String, String, bool)>>,
}

impl RecordingMetrics {
    fn samples(&self) -> Vec<(String, String, bool)> {
        self.overdue.lock().unwrap().clone()
    }

    /// The recorded `overdue` flag for a `(kind, name)`, or `None` if not emitted.
    fn overdue_for(&self, kind: &str, name: &str) -> Option<bool> {
        self.overdue
            .lock()
            .unwrap()
            .iter()
            .find(|(k, n, _)| k == kind && n == name)
            .map(|(_, _, o)| *o)
    }
}

impl MetricsRecorder for RecordingMetrics {
    fn record_schedule_overdue(&self, kind: &str, name: &str, overdue: bool) {
        self.overdue
            .lock()
            .unwrap()
            .push((kind.to_owned(), name.to_owned(), overdue));
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────
//
// Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
// these against a shared local cluster (Docker-free); otherwise a fresh
// testcontainers Postgres 16 is started per test. On a shared DB each test
// `scrub()`s first for isolation.

async fn setup_db() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        // Assumed pre-migrated (see module doc); scrub per test isolates it.
        let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
        scrub(&mut conn).await;
        return (conn, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (conn, Some(container))
}

/// Clear schedules + executions so a shared migrated DB stays per-test isolated.
async fn scrub(conn: &mut AsyncPgConnection) {
    for stmt in [
        "DELETE FROM harvest_schedules",
        "DELETE FROM harvest_workflow_executions",
        "DELETE FROM harvest_start_throttle",
    ] {
        diesel::sql_query(stmt).execute(conn).await.expect(stmt);
    }
}

/// Insert a workflow schedule with an explicit `next_run_at` and AC3 flags.
#[allow(clippy::too_many_arguments)]
async fn insert_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    schedule_expr: &str,
    next_run_at: DateTime<Utc>,
    is_paused: bool,
    exhausted_at: Option<DateTime<Utc>>,
    max_active_runs: i32,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq(schedule_expr),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(max_active_runs),
            dsl::is_paused.eq(is_paused),
            dsl::next_run_at.eq(next_run_at),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
            dsl::exhausted_at.eq(exhausted_at),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

/// Insert one RUNNING execution row for `wf_name` (drives the at-capacity guard).
/// The `workflow_id` is unique per call so multiple RUNNING rows for the same
/// workflow name do not collide on the active-uniqueness index.
async fn insert_running_execution(conn: &mut AsyncPgConnection, wf_name: &str) {
    let sql = format!(
        "INSERT INTO harvest_workflow_executions \
         (id, workflow_name, workflow_id, run_id, shard_id, state, input, queue_name, started_at, created_at) \
         VALUES (gen_random_uuid(), '{wf_name}', '{wf_name}-' || gen_random_uuid()::text, gen_random_uuid(), 0, 'RUNNING', '{{}}'::jsonb, 'default', now(), now())"
    );
    conn.batch_execute(&sql).await.expect("insert execution");
}

/// Insert a workflow schedule with explicit bound/overlap fields (Codex P2
/// tests): `overlap_policy`, `catchup`, `max_runs`, `runs_started`, `end_at`.
#[allow(clippy::too_many_arguments)]
async fn insert_schedule_bounded(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    next_run_at: DateTime<Utc>,
    max_active_runs: i32,
    overlap_policy: &str,
    catchup: bool,
    max_runs: Option<i32>,
    runs_started: i32,
    end_at: Option<DateTime<Utc>>,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(catchup),
            dsl::max_active_runs.eq(max_active_runs),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(next_run_at),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq(overlap_policy),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
            dsl::exhausted_at.eq(Option::<DateTime<Utc>>::None), // NULL — tick hasn't stamped it
            dsl::max_runs.eq(max_runs),
            dsl::runs_started.eq(runs_started),
            dsl::end_at.eq(end_at),
        ))
        .execute(conn)
        .await
        .expect("insert bounded schedule");
    id
}

/// Insert a daily-cron workflow schedule attached to a calendar + skip policy
/// (Codex round 3 calendar-deferral test). `calendar_name = None` disables the
/// calendar (the control case). Uses a daily midnight cron so a slot on an
/// excluded (weekend) day is rebased to the next business day.
async fn insert_calendar_schedule(
    conn: &mut AsyncPgConnection,
    wf_name: &str,
    next_run_at: DateTime<Utc>,
    calendar_name: Option<&str>,
    skip_policy: &str,
) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("cron:0 0 * * *"), // daily at midnight (canonical prefix)
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(next_run_at),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq(skip_policy),
            dsl::calendar_name.eq(calendar_name),
        ))
        .execute(conn)
        .await
        .expect("insert calendar schedule");
    id
}

/// Insert one pending #607 start-throttle row for `wf_name` (drives the
/// throttle-aware at-capacity guard, matching the tick's basis).
async fn insert_pending_throttle(conn: &mut AsyncPgConnection, wf_name: &str) {
    let sql = format!(
        "INSERT INTO harvest_start_throttle \
         (id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name, input, start_options, deferred_at, shard_id, created_at) \
         VALUES (gen_random_uuid(), '{wf_name}', '', 'start-throttle:{wf_name}:', '{wf_name}-wid', 'default', '{{}}'::jsonb, '{{}}'::jsonb, now(), 0, now())"
    );
    conn.batch_execute(&sql).await.expect("insert throttle row");
}

// ── Unit markers ─────────────────────────────────────────────────────────────

#[test]
fn metric_constant_schedule_overdue_is_defined() {
    assert_eq!(METRIC_SCHEDULE_OVERDUE, "harvest.schedule.overdue");
}

// ── Integration tests ────────────────────────────────────────────────────────

/// AC6 / success metric: a schedule whose `next_run_at` is held in the past
/// (no scheduler progress) is reported overdue.
#[tokio::test]
async fn wedged_schedule_is_reported_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // interval:60 => grace = 60 + 0 + 1 = 61s. 200s past its slot => overdue.
    insert_schedule(
        &mut conn,
        "wedged_wf",
        "interval:60",
        now - chrono::Duration::seconds(200),
        false,
        None,
        10,
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "wedged_wf"),
        Some(true),
        "a wedged schedule must be reported overdue; samples: {:?}",
        metrics.samples()
    );
}

/// AC3: intentionally-not-firing states are never reported overdue, and the
/// gauge is still emitted (0) for them so it stays fresh.
#[tokio::test]
async fn healthy_paused_exhausted_are_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Healthy: just fired, well within grace.
    insert_schedule(
        &mut conn,
        "healthy_wf",
        "interval:60",
        now - chrono::Duration::seconds(10),
        false,
        None,
        10,
    )
    .await;
    // Paused but with a stale next_run_at (would be overdue if active).
    insert_schedule(
        &mut conn,
        "paused_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        true,
        None,
        10,
    )
    .await;
    // Exhausted (#478/#543): intentionally done, never overdue.
    insert_schedule(
        &mut conn,
        "exhausted_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        false,
        Some(now),
        10,
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(metrics.overdue_for("workflow", "healthy_wf"), Some(false));
    assert_eq!(metrics.overdue_for("workflow", "paused_wf"), Some(false));
    assert_eq!(metrics.overdue_for("workflow", "exhausted_wf"), Some(false));
}

/// §2 false-positive guard: a schedule the tick deliberately holds in the past
/// because it is at `max_active_runs` (a run is in flight) is NOT overdue.
#[tokio::test]
async fn at_capacity_schedule_is_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Stale next_run_at that WOULD be overdue if the schedule were free. Skip +
    // catchup is the tick's DEFERRING config (Codex P2-B), so at-capacity holds
    // next_run_at in the past and the predicate suppresses.
    insert_schedule_bounded(
        &mut conn,
        "busy_wf",
        now - chrono::Duration::seconds(5000),
        1,      // max_active_runs = 1
        "skip", // deferring policy
        true,   // catchup
        None,
        0,
        None,
    )
    .await;
    // One RUNNING execution => running (1) >= max_active_runs (1) => at capacity.
    insert_running_execution(&mut conn, "busy_wf").await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "busy_wf"),
        Some(false),
        "an at-capacity schedule holds next_run_at deliberately; not a wedge"
    );
}

/// The returning helper surfaces `overdue_by_secs` = `now - next_run_at`.
#[tokio::test]
async fn overdue_schedule_samples_reports_lag() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    insert_schedule(
        &mut conn,
        "lag_wf",
        "interval:60",
        now - chrono::Duration::seconds(300),
        false,
        None,
        10,
    )
    .await;

    let samples = overdue_schedule_samples(&mut conn, now)
        .await
        .expect("samples");
    let s = samples
        .iter()
        .find(|s| s.name == "lag_wf")
        .expect("lag_wf sample present");
    assert!(s.overdue, "lag_wf must be overdue");
    // now - next_run_at == 300 (allowing a small clock-read slack).
    let by = s.overdue_by_secs.expect("overdue_by_secs set");
    assert!(
        (299..=301).contains(&by),
        "overdue_by_secs should be ~300 (now - next_run_at), got {by}"
    );
}

/// AC6 "backfill" term: a HEALTHY schedule (fresh `next_run_at`) with an
/// in-progress backfill (#337 creates independent RUNNING executions carrying
/// the schedule's `workflow_name`, and never touches the live `next_run_at`)
/// reports `overdue: false`. Backfill runs can only inflate the running basis
/// (pushing toward suppression), never cause a false positive.
#[tokio::test]
async fn backfill_in_progress_does_not_flag_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Healthy: just fired, within grace. next_run_at is NOT touched by backfill.
    insert_schedule(
        &mut conn,
        "backfill_wf",
        "interval:60",
        now - chrono::Duration::seconds(10),
        false,
        None,
        10,
    )
    .await;
    // Three independent backfill-created executions RUNNING for the same name.
    for _ in 0..3 {
        insert_running_execution(&mut conn, "backfill_wf").await;
    }

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "backfill_wf"),
        Some(false),
        "a healthy schedule with an in-progress backfill must not be overdue"
    );
}

/// BLOCKER 1: the at-capacity basis includes the #607 pending-throttle count,
/// exactly as the scheduler tick does. A schedule with 0 RUNNING executions but
/// a pending throttled fire that pushes it to `max_active_runs` (the tick then
/// deliberately holds `next_run_at` in the past under Skip+catchup) must NOT be
/// reported overdue — otherwise a throttled schedule pages spuriously.
#[tokio::test]
async fn throttle_pending_at_capacity_is_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Stale next_run_at that WOULD be overdue if the schedule were free. Skip +
    // catchup so the tick defers (Codex P2-B) and at-capacity suppresses.
    insert_schedule_bounded(
        &mut conn,
        "throttled_wf",
        now - chrono::Duration::seconds(5000),
        1,      // max_active_runs = 1
        "skip", // deferring policy
        true,   // catchup
        None,
        0,
        None,
    )
    .await;
    // 0 RUNNING/PAUSED executions, but 1 pending throttle row => running basis
    // = 0 + 1 = 1 >= max_active_runs(1) => at capacity (matches the tick).
    insert_pending_throttle(&mut conn, "throttled_wf").await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "throttled_wf"),
        Some(false),
        "a throttle-deferred schedule at capacity must not be reported overdue"
    );
}

/// Codex P2-A: a schedule the tick WOULD exhaust (`runs_started >= max_runs`)
/// but whose tick died before stamping `exhausted_at` (so `exhausted_at IS
/// NULL`) must NOT be reported overdue — bounded-out is derived from the raw
/// budget fields, not just `exhausted_at`.
#[tokio::test]
async fn bounded_out_by_max_runs_with_null_exhausted_at_is_not_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    insert_schedule_bounded(
        &mut conn,
        "budget_wf",
        now - chrono::Duration::seconds(5000), // stale (would be overdue if active)
        10,
        "skip",
        false,
        Some(5), // max_runs
        5,       // runs_started == max_runs => budget spent, exhausted_at NULL
        None,
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "budget_wf"),
        Some(false),
        "budget-exhausted (runs_started >= max_runs) with NULL exhausted_at must not page"
    );
}

/// Codex P2-B: a NON-deferring overlap policy (`CancelOther`) at capacity with a
/// stale `next_run_at` is a GENUINE stall — the tick would ADVANCE `next_run_at`,
/// so a persistent past value means the scheduler is wedged. Must be reported
/// overdue (the blanket at-capacity suppression must NOT mask it).
#[tokio::test]
async fn non_deferring_policy_at_capacity_is_overdue() {
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // CancelOther cancels + proceeds (advances next_run_at); it never retains.
    insert_schedule_bounded(
        &mut conn,
        "cancel_wf",
        now - chrono::Duration::seconds(5000), // stale
        1,                                     // max_active_runs = 1
        "cancel_other",                        // NON-deferring policy
        true,                                  // catchup on, but cancel_other still advances
        None,
        0,
        None,
    )
    .await;
    // 1 RUNNING execution => at capacity, but the policy is non-deferring.
    insert_running_execution(&mut conn, "cancel_wf").await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "cancel_wf"),
        Some(true),
        "a non-deferring policy at capacity with a stale next_run_at is a genuine stall"
    );
}

/// Codex round 3: a calendar `run_next_business_day` schedule whose slot fell on
/// an excluded (weekend) day is deliberately deferred by the tick to the next
/// business day; while that adjusted fire is still in the FUTURE the tick keeps
/// `next_run_at` pinned to the (past) original slot. The sampler must NOT flag it
/// overdue — reusing the tick's own calendar rebasing. A control run WITHOUT the
/// calendar (same past slot) IS overdue, proving the calendar resolution is what
/// suppresses the false positive (not a blanket suppression).
#[tokio::test]
async fn calendar_deferred_weekend_schedule_is_not_overdue_via_sampler() {
    let (mut conn, _c) = setup_db().await;
    // Fixed weekend so the rebase is deterministic:
    //   2026-01-03 = Sat, 2026-01-04 = Sun, 2026-01-05 = Mon.
    // Slot on Sat midnight; run_next_business_day → Mon 2026-01-05 00:00 (future).
    let sat_slot: DateTime<Utc> = "2026-01-03T00:00:00Z".parse().expect("sat slot");
    let now: DateTime<Utc> = "2026-01-04T12:00:00Z".parse().expect("now (sun)");

    // Calendar-deferred: next fire is Monday (future) → not overdue.
    insert_calendar_schedule(
        &mut conn,
        "cal_deferred_wf",
        sat_slot,
        Some("weekends-off"),
        "run_next_business_day",
    )
    .await;
    // Control: identical past slot, NO calendar → genuinely overdue (~1.5d > grace).
    insert_calendar_schedule(&mut conn, "cal_control_wf", sat_slot, None, "skip").await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "cal_deferred_wf"),
        Some(false),
        "a calendar-deferred (future business-day) schedule must not be overdue; samples: {:?}",
        metrics.samples()
    );
    assert_eq!(
        metrics.overdue_for("workflow", "cal_control_wf"),
        Some(true),
        "the same past slot WITHOUT a calendar must still be overdue (control)"
    );
}

/// Codex round 3: detection is preserved for a calendar schedule genuinely wedged
/// PAST its calendar-adjusted fire. Here `now` is a Tuesday well after the rebased
/// Monday fire, so the effective fire is itself in the past by > grace → overdue.
#[tokio::test]
async fn calendar_schedule_wedged_past_adjusted_fire_is_overdue_via_sampler() {
    let (mut conn, _c) = setup_db().await;
    // Slot on Sat 2026-01-03; run_next_business_day → Mon 2026-01-05 00:00.
    let sat_slot: DateTime<Utc> = "2026-01-03T00:00:00Z".parse().expect("sat slot");
    // now = Wed 2026-01-07 12:00 → the Monday fire is ~2.5 days past (> ~1d grace).
    let now: DateTime<Utc> = "2026-01-07T12:00:00Z".parse().expect("now (wed)");

    insert_calendar_schedule(
        &mut conn,
        "cal_wedged_wf",
        sat_slot,
        Some("weekends-off"),
        "run_next_business_day",
    )
    .await;

    let metrics = RecordingMetrics::default();
    sample_overdue_schedules(&mut conn, now, &metrics)
        .await
        .expect("sampler pass");

    assert_eq!(
        metrics.overdue_for("workflow", "cal_wedged_wf"),
        Some(true),
        "a calendar schedule wedged past its adjusted business-day fire must still be overdue"
    );
}

/// Run ONE overdue sampling pass for a single shard connection and emit into
/// `metrics`, mirroring the worker sampler loop's per-pass reconciliation
/// (Codex round 5): run [`overdue_schedule_pass`], emit each verdict, then zero
/// any label from `previous_emitted` that disappeared from this COMPLETE pass via
/// [`autumn_harvest::worker::labels_to_clear`]. Returns `(current keys,
/// pass_complete)`. Single-shard, so a pass is complete iff the read succeeded.
async fn run_and_emit_overdue_pass(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    previous_emitted: &std::collections::HashSet<(String, String)>,
    metrics: &RecordingMetrics,
) -> (std::collections::HashSet<(String, String)>, bool) {
    let mut by_key: std::collections::BTreeMap<(String, String), bool> =
        std::collections::BTreeMap::new();
    let mut complete = true;
    match overdue_schedule_pass(conn, now).await {
        Ok(pass) => {
            for s in pass.samples {
                let e = by_key.entry((s.kind, s.name)).or_insert(false);
                *e = *e || s.overdue;
            }
        }
        Err(_) => complete = false,
    }
    let current: std::collections::HashSet<(String, String)> = by_key.keys().cloned().collect();
    for ((k, n), o) in by_key {
        metrics.record_schedule_overdue(&k, &n, o);
    }
    for (k, n) in autumn_harvest::worker::labels_to_clear(previous_emitted, &current, complete) {
        metrics.record_schedule_overdue(&k, &n, false);
    }
    (current, complete)
}

/// Codex round 5 F2: a schedule that was overdue and is then deleted has its
/// gauge series zeroed on the next COMPLETE pass, so `harvest.schedule.overdue`
/// cannot stick at `1` for a schedule that no longer exists. Drives the real
/// `overdue_schedule_pass` + `worker::labels_to_clear` reconciliation the sampler
/// loop performs, against a real DB, across a delete. (F1's eager-first-pass is a
/// structural loop change — sample-before-sleep — with no non-brittle unit seam;
/// "pass 1" here mirrors that first emission.)
#[tokio::test]
async fn deleted_overdue_schedule_is_zeroed_on_next_complete_pass() {
    use std::collections::HashSet;
    let (mut conn, _c) = setup_db().await;
    let now = Utc::now();
    // Wedged (interval:60, next_run_at 5000s ago) → overdue.
    insert_schedule(
        &mut conn,
        "disappear_wf",
        "interval:60",
        now - chrono::Duration::seconds(5000),
        false,
        None,
        10,
    )
    .await;

    // Pass 1 (the eager first pass): a single-shard COMPLETE pass emits true.
    let mut previous_emitted: HashSet<(String, String)> = HashSet::new();
    let m1 = RecordingMetrics::default();
    let (keys1, complete1) =
        run_and_emit_overdue_pass(&mut conn, now, &previous_emitted, &m1).await;
    assert!(complete1, "single-shard pass is complete");
    previous_emitted = keys1; // complete pass → track exactly the current keys
    assert_eq!(
        m1.overdue_for("workflow", "disappear_wf"),
        Some(true),
        "pass 1 must emit overdue=true for the wedged schedule"
    );

    // Delete the schedule; the next COMPLETE pass zeroes its disappeared label.
    diesel::delete(
        harvest_schedules::table.filter(harvest_schedules::workflow_name.eq("disappear_wf")),
    )
    .execute(&mut conn)
    .await
    .expect("delete schedule");
    let m2 = RecordingMetrics::default();
    let (_keys2, complete2) =
        run_and_emit_overdue_pass(&mut conn, now, &previous_emitted, &m2).await;
    assert!(complete2, "single-shard pass is complete");
    assert_eq!(
        m2.overdue_for("workflow", "disappear_wf"),
        Some(false),
        "the deleted schedule's gauge label must be zeroed on the next complete pass"
    );
}
