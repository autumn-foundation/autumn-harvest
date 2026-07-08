#![cfg(feature = "db")]
//! In-place schedule update tests — issue #771.
//!
//! Verifies `scheduler::update_workflow_schedule`: partial (only-provided-
//! fields-change) updates keyed by the schedule row id, `next_run_at`
//! recomputation on cadence change only, pause/bookkeeping preservation,
//! #478 exhaustion reconciliation in both directions, the #350 fire-claim
//! anti-race gate, and validate-before-commit atomicity.

use std::sync::Arc;

use autumn_harvest::models::HarvestSchedule;
use autumn_harvest::policy::{OverlapPolicy, Schedule, WorkflowSchedule};
use autumn_harvest::register_workflow_schedules;
use autumn_harvest::scheduler::{
    DagCatalog, ScheduleUpdateOutcome, WorkflowSchedulePatch, claim_and_fire_workflow_schedule,
    update_workflow_schedule,
};
use autumn_harvest::schema::{harvest_schedules, harvest_workflow_executions};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};
use autumn_harvest::types::ShardId;
use autumn_harvest::worker::HandlerRegistry;
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

// All migrations that touch harvest_schedules (same set as the bounded-runs suite).
const INIT_SQL: &str = concat!(
    include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000000_harvest_schedule_jitter/up.sql"),
    "\n",
    include_str!("../../migrations/20260517000001_harvest_schedule_overlap_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
    include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
    include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
    "\n",
    include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
    include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
);

async fn setup_db() -> (AsyncPgConnection, String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(INIT_SQL).await.expect("migration");
    (conn, url, container)
}

/// Register a workflow schedule and return its row.
async fn register_and_load(conn: &mut AsyncPgConnection, ws: &WorkflowSchedule) -> HarvestSchedule {
    register_workflow_schedules(conn, std::slice::from_ref(ws))
        .await
        .expect("register schedule");
    load_by_name(conn, &ws.workflow_name).await
}

async fn load_by_name(conn: &mut AsyncPgConnection, wf_name: &str) -> HarvestSchedule {
    harvest_schedules::table
        .filter(harvest_schedules::dsl::workflow_name.eq(wf_name))
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .expect("load schedule row")
}

async fn load_by_id(conn: &mut AsyncPgConnection, id: Uuid) -> HarvestSchedule {
    harvest_schedules::table
        .find(id)
        .select(HarvestSchedule::as_select())
        .first(conn)
        .await
        .expect("load schedule row by id")
}

fn expect_updated(outcome: ScheduleUpdateOutcome) -> HarvestSchedule {
    match outcome {
        ScheduleUpdateOutcome::Updated(row) => *row,
        other => panic!("expected Updated outcome, got {other:?}"),
    }
}

/// Patching only the input must leave the cadence, `next_run_at`, and queue
/// untouched while changing `workflow_input` (partial-merge semantics).
#[tokio::test]
async fn patch_input_only_changes_input_and_preserves_cadence() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_input_only_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    )
    .with_input(serde_json::json!({"env": "A"}));
    let before = register_and_load(&mut conn, &ws).await;
    assert!(before.next_run_at.is_some(), "seed must have next_run_at");

    let patch = WorkflowSchedulePatch {
        input: Some(serde_json::json!({"env": "B"})),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );

    assert_eq!(row.id, before.id, "schedule id must never change");
    assert_eq!(
        row.schedule_expr.as_deref(),
        Some("interval:3600"),
        "cadence must be unchanged"
    );
    assert_eq!(
        row.next_run_at, before.next_run_at,
        "non-cadence patch must preserve the pending next_run_at"
    );
    assert_eq!(row.queue_name.as_deref(), Some("default"));
    assert_eq!(row.workflow_input, Some(serde_json::json!({"env": "B"})));
}

/// Changing the schedule expression must recompute `next_run_at` anchored at
/// now — never retroactively firing slots that elapsed under the old spec.
#[tokio::test]
async fn cadence_change_recomputes_next_run_at_anchored_at_now() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new("upd_cadence_wf", Schedule::Cron("0 3 * * *".to_string()));
    let before = register_and_load(&mut conn, &ws).await;

    let patch_started = Utc::now();
    let patch = WorkflowSchedulePatch {
        schedule: Some(Schedule::Interval(std::time::Duration::from_secs(60))),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );

    assert_eq!(row.schedule_expr.as_deref(), Some("interval:60"));
    let next = row.next_run_at.expect("next_run_at must be repopulated");
    assert!(
        next > patch_started,
        "next_run_at must be in the future (anchored at now), got {next}"
    );
    assert!(
        next <= Utc::now() + chrono::Duration::seconds(120),
        "next_run_at must be ~now + 60s, got {next}"
    );
    assert_ne!(
        Some(next),
        before.next_run_at,
        "cadence change must not preserve the old next_run_at"
    );
}

/// A non-cadence policy change (overlap policy) must preserve `next_run_at`.
#[tokio::test]
async fn non_cadence_policy_change_preserves_next_run_at() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_policy_only_wf",
        Schedule::Interval(std::time::Duration::from_secs(1800)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    let patch = WorkflowSchedulePatch {
        overlap_policy: Some(OverlapPolicy::BufferOne),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );
    assert_eq!(row.overlap_policy, "buffer_one");
    assert_eq!(row.next_run_at, before.next_run_at);
}

/// A paused schedule stays paused across a patch, and scheduler bookkeeping
/// (`runs_started`, `last_run_at`, `consecutive_failure_count`, pause
/// metadata) is
/// preserved.
#[tokio::test]
async fn paused_schedule_stays_paused_and_bookkeeping_preserved() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_paused_wf",
        Schedule::Interval(std::time::Duration::from_secs(600)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    let paused_at = Utc::now() - chrono::Duration::minutes(10);
    let last_run = Utc::now() - chrono::Duration::minutes(30);
    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_at.eq(Some(paused_at)),
            dsl::paused_by.eq(Some("ops")),
            dsl::pause_reason.eq(Some("incident")),
            dsl::runs_started.eq(5),
            dsl::last_run_at.eq(Some(last_run)),
            dsl::consecutive_failure_count.eq(2),
        ))
        .execute(&mut conn)
        .await
        .expect("seed pause/bookkeeping");

    let patch = WorkflowSchedulePatch {
        input: Some(serde_json::json!({"edited": true})),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );

    assert!(row.is_paused, "patch must not resume a paused schedule");
    assert_eq!(row.paused_by.as_deref(), Some("ops"));
    assert_eq!(row.pause_reason.as_deref(), Some("incident"));
    assert_eq!(row.runs_started, 5, "runs_started must be preserved");
    assert_eq!(row.consecutive_failure_count, 2);
    assert_eq!(
        row.last_run_at.map(|t| t.timestamp()),
        Some(last_run.timestamp()),
        "last_run_at must be preserved"
    );
}

/// Raising `max_runs` on an exhausted schedule clears the exhaustion state and
/// repopulates `next_run_at` (issue #478 reconciliation, applied via PATCH).
#[tokio::test]
async fn raising_max_runs_clears_exhaustion_and_repopulates_next_run_at() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_unexhaust_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    )
    .with_max_runs(1);
    let before = register_and_load(&mut conn, &ws).await;

    // Simulate the scheduler having exhausted the budget.
    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::runs_started.eq(1),
            dsl::exhausted_at.eq(Some(Utc::now())),
            dsl::exhausted_reason.eq(Some("max_runs_exhausted")),
            dsl::next_run_at.eq(None::<DateTime<Utc>>),
        ))
        .execute(&mut conn)
        .await
        .expect("seed exhaustion");

    let patch = WorkflowSchedulePatch {
        max_runs: Some(Some(5)),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );

    assert!(row.exhausted_at.is_none(), "exhaustion must be cleared");
    assert!(row.exhausted_reason.is_none());
    assert!(
        row.next_run_at.is_some(),
        "next_run_at must be repopulated so the schedule fires again"
    );
    assert_eq!(row.max_runs, Some(5));
    assert_eq!(
        row.runs_started, 1,
        "runs_started is never reset by a patch"
    );
}

/// Lowering `max_runs` to (or below) `runs_started` exhausts the schedule
/// immediately rather than leaving it active-but-never-runnable.
#[tokio::test]
async fn lowering_max_runs_below_runs_started_exhausts_immediately() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_exhaust_now_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    );
    let before = register_and_load(&mut conn, &ws).await;
    diesel::update(harvest_schedules::table.find(before.id))
        .set(dsl::runs_started.eq(3))
        .execute(&mut conn)
        .await
        .expect("seed runs_started");

    let patch = WorkflowSchedulePatch {
        max_runs: Some(Some(2)),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );

    assert!(
        row.exhausted_at.is_some(),
        "schedule must exhaust immediately"
    );
    assert_eq!(row.exhausted_reason.as_deref(), Some("max_runs_exhausted"));
    assert!(
        row.next_run_at.is_none(),
        "exhausted schedule must not fire"
    );
}

/// A live (unexpired) fire claim blocks the update with `ClaimLive` and the
/// row is not modified (issue #350 anti-race, AC7).
#[tokio::test]
async fn live_fire_claim_blocks_update_with_claim_live_outcome() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_claim_live_wf",
        Schedule::Interval(std::time::Duration::from_secs(60)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::fire_claim_token.eq(Some(Uuid::new_v4())),
            dsl::fire_claimed_until.eq(Some(Utc::now() + chrono::Duration::seconds(25))),
        ))
        .execute(&mut conn)
        .await
        .expect("seed live claim");
    let snapshot = load_by_id(&mut conn, before.id).await;

    let patch = WorkflowSchedulePatch {
        input: Some(serde_json::json!({"blocked": true})),
        ..Default::default()
    };
    let outcome = update_workflow_schedule(&mut conn, before.id, &patch)
        .await
        .expect("update call must not error");
    assert!(
        matches!(outcome, ScheduleUpdateOutcome::ClaimLive),
        "a live fire claim must yield ClaimLive, got {outcome:?}"
    );

    let after = load_by_id(&mut conn, before.id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        serde_json::to_value(&snapshot).unwrap(),
        "a ClaimLive outcome must leave the row byte-for-byte unchanged"
    );
}

/// An expired fire claim (crashed replica) does not block the update.
#[tokio::test]
async fn expired_fire_claim_does_not_block_update() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_claim_expired_wf",
        Schedule::Interval(std::time::Duration::from_secs(60)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::fire_claim_token.eq(Some(Uuid::new_v4())),
            dsl::fire_claimed_until.eq(Some(Utc::now() - chrono::Duration::seconds(5))),
        ))
        .execute(&mut conn)
        .await
        .expect("seed expired claim");

    let patch = WorkflowSchedulePatch {
        input: Some(serde_json::json!({"proceeds": true})),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );
    assert_eq!(
        row.workflow_input,
        Some(serde_json::json!({"proceeds": true}))
    );
}

/// DAG schedule rows are owned by `PATCH /dags/{dag_name}` — the workflow
/// schedule updater must refuse them.
#[tokio::test]
async fn dag_schedule_row_returns_dag_schedule_outcome() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let id = Uuid::new_v4();
    diesel::insert_into(harvest_schedules::table)
        .values((
            dsl::id.eq(id),
            dsl::dag_name.eq("some_dag"),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(1),
            dsl::is_paused.eq(false),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(&mut conn)
        .await
        .expect("insert dag schedule row");

    let patch = WorkflowSchedulePatch {
        input: Some(serde_json::json!({"nope": true})),
        ..Default::default()
    };
    let outcome = update_workflow_schedule(&mut conn, id, &patch)
        .await
        .expect("update call must not error");
    assert!(
        matches!(outcome, ScheduleUpdateOutcome::DagSchedule),
        "a DAG schedule row must yield DagSchedule, got {outcome:?}"
    );
}

/// Unknown schedule id yields `NotFound`.
#[tokio::test]
async fn unknown_id_returns_not_found() {
    let (mut conn, _url, _c) = setup_db().await;
    let outcome =
        update_workflow_schedule(&mut conn, Uuid::new_v4(), &WorkflowSchedulePatch::default())
            .await
            .expect("update call must not error");
    assert!(
        matches!(outcome, ScheduleUpdateOutcome::NotFound),
        "unknown id must yield NotFound, got {outcome:?}"
    );
}

/// Validation runs on the merged spec before any write: an invalid schedule
/// leaves the row byte-for-byte unchanged.
#[tokio::test]
async fn invalid_merged_schedule_writes_nothing() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_validate_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    );
    let before = register_and_load(&mut conn, &ws).await;
    let snapshot = serde_json::to_value(&before).unwrap();

    // Zero interval is rejected by validate_schedule.
    let patch = WorkflowSchedulePatch {
        schedule: Some(Schedule::Interval(std::time::Duration::ZERO)),
        input: Some(serde_json::json!({"should": "not persist"})),
        ..Default::default()
    };
    let err = update_workflow_schedule(&mut conn, before.id, &patch)
        .await
        .expect_err("zero interval must be rejected");
    assert!(
        matches!(err, autumn_harvest::HarvestError::Config(_)),
        "validation failure must surface as Config, got {err:?}"
    );

    // Invalid cron expression is also rejected before any write.
    let patch = WorkflowSchedulePatch {
        schedule: Some(Schedule::Cron("not a cron".to_string())),
        ..Default::default()
    };
    update_workflow_schedule(&mut conn, before.id, &patch)
        .await
        .expect_err("invalid cron must be rejected");

    let after = load_by_id(&mut conn, before.id).await;
    assert_eq!(
        serde_json::to_value(&after).unwrap(),
        snapshot,
        "a rejected patch must write nothing"
    );
}

/// Tri-state nullable fields: explicit clear via `Some(None)`, absence leaves
/// the stored value unchanged.
#[tokio::test]
async fn tristate_clear_and_absence_semantics() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_tristate_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    );
    let before = register_and_load(&mut conn, &ws).await;
    // calendar_name carries a foreign key to harvest_calendars — create it.
    autumn_harvest::calendar::create_calendar(&mut conn, "us-holidays", None)
        .await
        .expect("create calendar");
    let end_at = Utc::now() + chrono::Duration::days(30);
    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::calendar_name.eq(Some("us-holidays")),
            dsl::end_at.eq(Some(end_at)),
            dsl::max_runs.eq(Some(9)),
        ))
        .execute(&mut conn)
        .await
        .expect("seed nullable fields");

    // Absent fields leave stored values unchanged.
    let row = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                input: Some(serde_json::json!({"touch": 1})),
                ..Default::default()
            },
        )
        .await
        .expect("update must succeed"),
    );
    assert_eq!(row.calendar_name.as_deref(), Some("us-holidays"));
    assert_eq!(row.end_at.map(|t| t.timestamp()), Some(end_at.timestamp()));
    assert_eq!(row.max_runs, Some(9));

    // Explicit Some(None) clears the stored values.
    let row = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                calendar: Some(None),
                end_at: Some(None),
                max_runs: Some(None),
                ..Default::default()
            },
        )
        .await
        .expect("update must succeed"),
    );
    assert!(row.calendar_name.is_none(), "calendar must be cleared");
    assert!(row.end_at.is_none(), "end_at must be cleared");
    assert!(row.max_runs.is_none(), "max_runs must be cleared");
}

/// Cadence-kind transition: cron → manual must NULL `next_run_at` (a manual
/// schedule has no automatic next slot).
#[tokio::test]
async fn cron_to_manual_nulls_next_run_at() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new("upd_cron_to_manual_wf", Schedule::Cron("0 3 * * *".into()));
    let before = register_and_load(&mut conn, &ws).await;
    assert!(before.next_run_at.is_some(), "cron seed must have a slot");

    let patch = WorkflowSchedulePatch {
        schedule: Some(Schedule::Manual),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );
    assert_eq!(row.schedule_expr.as_deref(), Some("manual"));
    assert!(
        row.next_run_at.is_none(),
        "manual schedules must have no automatic next slot"
    );
}

/// Cadence-kind transition: manual → cron must populate `next_run_at`.
#[tokio::test]
async fn manual_to_cron_populates_next_run_at() {
    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new("upd_manual_to_cron_wf", Schedule::Manual);
    let before = register_and_load(&mut conn, &ws).await;
    assert!(
        before.next_run_at.is_none(),
        "manual seed must have no slot"
    );

    let patch = WorkflowSchedulePatch {
        schedule: Some(Schedule::Cron("*/5 * * * *".into())),
        ..Default::default()
    };
    let row = expect_updated(
        update_workflow_schedule(&mut conn, before.id, &patch)
            .await
            .expect("update must succeed"),
    );
    assert_eq!(row.schedule_expr.as_deref(), Some("cron:*/5 * * * *"));
    let next = row.next_run_at.expect("next_run_at must be populated");
    assert!(next > Utc::now(), "next slot must be in the future");
}

/// An auto-paused schedule (issue #360): an unrelated patch preserves
/// `auto_paused_at`; disabling the limit (`Some(None)`) clears it.
#[tokio::test]
async fn autopaused_schedule_patch_preserves_then_clears_auto_paused_at() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_autopause_wf",
        Schedule::Interval(std::time::Duration::from_secs(600)),
    );
    let before = register_and_load(&mut conn, &ws).await;
    let auto_paused = Utc::now() - chrono::Duration::minutes(3);
    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::consecutive_failure_limit.eq(Some(3)),
            dsl::consecutive_failure_count.eq(3),
            dsl::auto_paused_at.eq(Some(auto_paused)),
        ))
        .execute(&mut conn)
        .await
        .expect("seed auto-pause state");

    // Unrelated patch: auto_paused_at (and the failure counter) preserved.
    let row = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                input: Some(serde_json::json!({"edited": true})),
                ..Default::default()
            },
        )
        .await
        .expect("update must succeed"),
    );
    assert_eq!(
        row.auto_paused_at.map(|t| t.timestamp()),
        Some(auto_paused.timestamp()),
        "an unrelated patch must not clear auto_paused_at"
    );
    assert_eq!(row.consecutive_failure_count, 3);
    assert_eq!(row.consecutive_failure_limit, Some(3));

    // Disabling the limit clears the auto-pause (the #360 clear rule).
    let row = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                consecutive_failure_limit: Some(None),
                ..Default::default()
            },
        )
        .await
        .expect("update must succeed"),
    );
    assert!(row.consecutive_failure_limit.is_none());
    assert!(
        row.auto_paused_at.is_none(),
        "disabling the failure limit must clear auto_paused_at"
    );
}

/// P1 regression (AC7 for non-cadence edits): the tick must fire from a fresh
/// post-claim read of the row, never from its pre-claim due-list snapshot.
///
/// An input/queue-only PATCH that commits between the tick's due-list SELECT
/// and its HA claim leaves `next_run_at` unchanged, so the claim still
/// succeeds — the fire must nevertheless dispatch the NEW input on the NEW
/// queue. Driven deterministically by handing
/// `claim_and_fire_workflow_schedule` a deliberately stale snapshot while the
/// DB row already carries the edited values.
#[tokio::test]
async fn tick_fires_fresh_row_not_pre_claim_snapshot() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_stale_snapshot_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    )
    .with_input(serde_json::json!({"env": "OLD"}));
    let before = register_and_load(&mut conn, &ws).await;

    // Arm the slot in the past so it is due.
    let slot = Utc::now() - chrono::Duration::seconds(60);
    diesel::update(harvest_schedules::table.find(before.id))
        .set(dsl::next_run_at.eq(Some(slot)))
        .execute(&mut conn)
        .await
        .expect("arm slot");

    // The tick's due-list SELECT happens "now": capture the stale snapshot.
    let stale_snapshot = load_by_id(&mut conn, before.id).await;
    assert_eq!(
        stale_snapshot.workflow_input,
        Some(serde_json::json!({"env": "OLD"}))
    );

    // A non-cadence PATCH commits between the due-list SELECT and the claim:
    // input + queue change, next_run_at untouched (still the armed slot).
    let patched = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                input: Some(serde_json::json!({"env": "NEW"})),
                queue_name: Some("edited-queue".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("concurrent edit must succeed"),
    );
    assert_eq!(
        patched.next_run_at.map(|t| t.timestamp_micros()),
        Some(slot.timestamp_micros()),
        "a non-cadence edit must leave the armed slot in place"
    );

    // Fire from the STALE snapshot — the claim's next_run_at guard passes.
    let registry = HandlerRegistry::new(vec![], vec![]);
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoOpMetrics);
    claim_and_fire_workflow_schedule(
        &mut conn,
        &stale_snapshot,
        Utc::now(),
        ShardId::new(0),
        &DagCatalog::default(),
        &registry,
        &metrics,
        &[],
    )
    .await
    .expect("claim-and-fire must succeed");

    // Exactly one execution (no double fire), started with the NEW input on
    // the NEW queue.
    let rows: Vec<(serde_json::Value, String)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::dsl::workflow_name.eq("upd_stale_snapshot_wf"))
        .select((
            harvest_workflow_executions::dsl::input,
            harvest_workflow_executions::dsl::queue_name,
        ))
        .load(&mut conn)
        .await
        .expect("load executions");
    assert_eq!(rows.len(), 1, "exactly one execution must be started");
    assert_eq!(
        rows[0].0,
        serde_json::json!({"env": "NEW"}),
        "the fire must use the post-edit input, not the pre-claim snapshot's"
    );
    assert_eq!(
        rows[0].1, "edited-queue",
        "the fire must use the post-edit queue, not the pre-claim snapshot's"
    );

    // The finalize advanced next_run_at and released the claim.
    let after = load_by_id(&mut conn, before.id).await;
    assert!(after.fire_claim_token.is_none(), "claim must be released");
    assert!(
        after.next_run_at.is_some_and(|t| t > slot),
        "next_run_at must advance past the fired slot"
    );
}

/// P2 regression: `update_workflow_schedule`'s initial load takes the row
/// lock (`FOR UPDATE`), so a PATCH blocks behind a concurrent transaction
/// holding the row (a tick's claim/advance/finalize cycle) and then applies
/// against the POST-commit row — never writing back a stale `next_run_at`
/// (which would re-arm an already-fired slot → double fire).
#[tokio::test]
async fn patch_blocks_on_row_lock_and_applies_against_post_commit_row() {
    use diesel_async::SimpleAsyncConnection as _;
    use harvest_schedules::dsl;

    let (mut conn, url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_row_lock_wf",
        Schedule::Interval(std::time::Duration::from_secs(3600)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    // conn2 simulates a mid-flight tick: hold the row lock in an explicit
    // transaction while advancing next_run_at (the pause_tests
    // hold_execution_row_lock precedent).
    let mut conn2 = AsyncPgConnection::establish(&url).await.expect("conn2");
    conn2.batch_execute("BEGIN").await.expect("begin");
    let advanced_slot = Utc::now() + chrono::Duration::seconds(999);
    diesel::update(harvest_schedules::table.find(before.id))
        .set(dsl::next_run_at.eq(Some(advanced_slot)))
        .execute(&mut conn2)
        .await
        .expect("advance under lock");

    // The PATCH must block on the FOR UPDATE row lock while conn2 holds it.
    let patch_url = url.clone();
    let id = before.id;
    let patch_task = tokio::spawn(async move {
        let mut c = AsyncPgConnection::establish(&patch_url)
            .await
            .expect("patch conn");
        update_workflow_schedule(
            &mut c,
            id,
            &WorkflowSchedulePatch {
                input: Some(serde_json::json!({"locked": true})),
                ..Default::default()
            },
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert!(
        !patch_task.is_finished(),
        "the PATCH must block on the row lock; without FOR UPDATE it would \
         read the pre-commit row and write back a stale next_run_at"
    );

    conn2.batch_execute("COMMIT").await.expect("commit");
    let row = expect_updated(
        patch_task
            .await
            .expect("join")
            .expect("patch must succeed after the lock is released"),
    );
    assert_eq!(
        row.next_run_at.map(|t| t.timestamp_micros()),
        Some(advanced_slot.timestamp_micros()),
        "the PATCH must merge against the post-commit row: the concurrently \
         advanced next_run_at must survive the edit"
    );
    assert_eq!(
        row.workflow_input,
        Some(serde_json::json!({"locked": true}))
    );
}

/// P2 regression: a PATCH that proceeds past an EXPIRED fire claim must fence
/// the stale token (NULL it) so a straggler fire's token-guarded finalize
/// matches zero rows and cannot clobber the edited row with old-spec values.
#[tokio::test]
async fn patch_on_expired_claim_fences_straggler_finalize() {
    use harvest_schedules::dsl;

    let (mut conn, _url, _c) = setup_db().await;
    let ws = WorkflowSchedule::new(
        "upd_fence_wf",
        Schedule::Interval(std::time::Duration::from_secs(60)),
    );
    let before = register_and_load(&mut conn, &ws).await;

    let stale_token = Uuid::new_v4();
    diesel::update(harvest_schedules::table.find(before.id))
        .set((
            dsl::fire_claim_token.eq(Some(stale_token)),
            dsl::fire_claimed_until.eq(Some(Utc::now() - chrono::Duration::seconds(5))),
        ))
        .execute(&mut conn)
        .await
        .expect("seed expired claim");

    let row = expect_updated(
        update_workflow_schedule(
            &mut conn,
            before.id,
            &WorkflowSchedulePatch {
                input: Some(serde_json::json!({"fenced": true})),
                ..Default::default()
            },
        )
        .await
        .expect("expired claim must not block the edit"),
    );
    assert!(
        row.fire_claim_token.is_none(),
        "the expired claim token must be fenced (nulled) by the edit"
    );
    assert!(row.fire_claimed_until.is_none());

    // A straggler fire's finalize UPDATE is guarded on its own token — it
    // must now match zero rows instead of clobbering the edited next_run_at.
    let old_spec_next = Utc::now() - chrono::Duration::seconds(30);
    let straggler_rows = diesel::update(
        harvest_schedules::table
            .find(before.id)
            .filter(dsl::fire_claim_token.eq(Some(stale_token))),
    )
    .set((
        dsl::next_run_at.eq(Some(old_spec_next)),
        dsl::fire_claim_token.eq(None::<Uuid>),
        dsl::fire_claimed_until.eq(None::<DateTime<Utc>>),
    ))
    .execute(&mut conn)
    .await
    .expect("straggler finalize");
    assert_eq!(
        straggler_rows, 0,
        "a straggler finalize using the fenced token must match zero rows"
    );
    let after = load_by_id(&mut conn, before.id).await;
    assert_ne!(
        after.next_run_at.map(|t| t.timestamp_micros()),
        Some(old_spec_next.timestamp_micros()),
        "the straggler must not have clobbered next_run_at"
    );
}
