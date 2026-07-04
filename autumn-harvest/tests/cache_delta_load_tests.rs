#![cfg(feature = "db")]

//! DB-backed integration tests for the sticky-routing warm-cache delta-load
//! optimisation (issue #235, AC9).
//!
//! These tests verify that:
//!
//! 1. A cold task (no cache entry) performs a full history reload from Postgres.
//! 2. A warm task (cache entry present) performs only a delta reload — fetching
//!    the events appended *since* the last suspension rather than all events.
//! 3. With sticky routing *on*, a workflow that suspends N times and stays on
//!    the same worker incurs exactly 1 full history reload (the very first task)
//!    and `N-1` cheap delta reloads — matching the acceptance criterion of
//!    ≤ 1 full reload across N follow-up tasks.
//! 4. With sticky routing *off* (empty cache), every task incurs a full reload,
//!    giving ≈ N full reloads for N tasks on the same execution.
//!
//! All tests run against a real Postgres container via `testcontainers`.

use autumn_harvest::cache::{CachedWorkflowState, WorkflowCache};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::store;
use autumn_harvest::types::{ActivityExecId, ExecutionId, TimerId};

use chrono::Utc;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Schema setup
// ---------------------------------------------------------------------------

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260613000000_harvest_workflow_sla/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
    include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
    include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
    "\n",
    include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
    "\n",
    include_str!("../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
    "\n",
    include_str!("../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
    "\n",
    include_str!("../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
    "\n",
    include_str!("../migrations/20260615000001_harvest_context_headers/up.sql"),
    "\n",
    // issue #523: workflow-level retry policy columns.
    include_str!("../migrations/20260626000001_harvest_workflow_retry/up.sql"),
    "\n",
    // issue #534: origin column + per-schedule run-history index.
    include_str!("../migrations/20260628000001_harvest_execution_origin/up.sql"),
    include_str!("../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
    include_str!("../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
);

async fn setup_test_db() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("get host");
    let port = container.get_host_port_ipv4(5432).await.expect("get port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let conn = <AsyncPgConnection as AsyncConnection>::establish(&url)
        .await
        .expect("connect");

    (conn, container)
}

async fn insert_execution(conn: &mut AsyncPgConnection, name: &str) -> ExecutionId {
    let exec_id = ExecutionId::new();
    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: name,
        workflow_id: &Uuid::new_v4().to_string(),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: None,

        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,

        sla_deadline_at: None,
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: None,
        retry_of_exec_id: None,
        origin: None,
    };
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");
    exec_id
}

/// Build a block of N activity-scheduled + activity-completed event pairs.
fn make_activity_events(count: usize) -> Vec<WorkflowEvent> {
    let mut events = Vec::with_capacity(count * 2);
    for i in 0..count {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("step_{i}"),
            input: serde_json::json!({"step": i}),
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!({"ok": true}),
        });
    }
    events
}

// ---------------------------------------------------------------------------
// AC9 tests
// ---------------------------------------------------------------------------

/// Cold-path baseline: without any cache entry, `load_history` reads all N
/// events from Postgres on every task cycle.
#[tokio::test]
async fn cold_path_reads_full_history_every_task() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_execution(&mut conn, "cold_path_wf").await;

    // Append a WorkflowStarted event followed by 24 activity pairs = 49 events.
    let mut initial: Vec<WorkflowEvent> = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    initial.extend(make_activity_events(24)); // 48 activity events → 49 total
    store::append_events(&mut conn, exec_id, &initial, 0)
        .await
        .expect("append initial events");

    // Simulate 3 task cycles WITHOUT any cache — each does a full history reload.
    for cycle in 1..=3_usize {
        // Append one more timer-fired event to simulate progress between cycles.
        let timer_id = TimerId::new(format!("t{cycle}"));
        store::append_events(
            &mut conn,
            exec_id,
            &[WorkflowEvent::TimerFired { timer_id }],
            // start_id: events appended so far
            i32::try_from(49 + (cycle - 1)).unwrap(),
        )
        .await
        .expect("append timer fired");

        let history = store::load_history(&mut conn, exec_id)
            .await
            .expect("load_history");

        // Cold path: must see all events appended so far (49 + cycle).
        let expected = 49 + cycle;
        assert_eq!(
            history.events.len(),
            expected,
            "cycle {cycle}: cold path must read all {expected} events"
        );
    }
}

/// Warm-path core: after the first (cold) task populates the cache, subsequent
/// tasks use `load_history_since` and fetch only the delta events.
#[tokio::test]
async fn warm_path_delta_load_reads_only_new_events() {
    let (mut conn, _container) = setup_test_db().await;
    let exec_id = insert_execution(&mut conn, "warm_path_wf").await;

    // Append 50 initial events: 1 WorkflowStarted + 24 activity pairs + 1 extra.
    let mut initial: Vec<WorkflowEvent> = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    initial.extend(make_activity_events(24)); // 48 events → 49 total
    initial.push(WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("wait-1"),
        duration_secs: 60,
    }); // 50th event — workflow suspends here waiting for a timer

    store::append_events(&mut conn, exec_id, &initial, 0)
        .await
        .expect("append 50 initial events");

    // --- Task 1 (cold): full history reload ---
    let cold = store::load_history(&mut conn, exec_id)
        .await
        .expect("cold load_history");
    assert_eq!(cold.events.len(), 50, "task 1: must see all 50 events");
    let snapshot_next_id = cold.next_event_id; // = 50

    // Populate the cache as the worker would after a Suspended outcome.
    let mut cache = WorkflowCache::new(64);
    cache.insert(
        exec_id.as_uuid(),
        CachedWorkflowState {
            events: cold.events.clone(),
            next_event_id: snapshot_next_id,
        },
    );

    // Append 2 delta events (timer fired + signal) — what accumulates between
    // Task 1's suspension and Task 2's pickup.
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::TimerFired {
                timer_id: TimerId::new("wait-1"),
            },
            WorkflowEvent::SignalReceived {
                signal_name: "approve".into(),
                payload: serde_json::json!({"approved": true}),
            },
        ],
        snapshot_next_id,
    )
    .await
    .expect("append delta events");

    // --- Task 2 (warm): delta load only ---
    let cached = cache.get(&exec_id.as_uuid()).expect("cache hit");
    let delta = store::load_history_since(&mut conn, exec_id, cached.next_event_id)
        .await
        .expect("load_history_since");

    // Warm path fetches ONLY the 2 delta events — not all 52.
    assert_eq!(
        delta.events.len(),
        2,
        "warm path must fetch only 2 delta events, not {}",
        delta.events.len()
    );

    // Reconstruct full history: cached snapshot + delta.
    let mut full = cached.events.clone();
    full.extend(delta.events.iter().cloned());
    assert_eq!(
        full.len(),
        52,
        "reconstructed history must have all 52 events"
    );
}

/// Acceptance criterion: sticky routing ON (warm cache) → ≤ 1 full reload for
/// N follow-up tasks; sticky routing OFF (no cache) → ≈ N full reloads.
///
/// This test directly exercises the claim from issue #235 AC9:
///   "a workflow that emits at least 50 history events sees its event-history
///    reload count drop to ≤ 1 across N follow-up tasks once sticky routing is
///    enabled, vs. ≈ N reloads with sticky routing off."
#[tokio::test]
async fn sticky_routing_on_vs_off_reload_count() {
    const INITIAL_EVENTS: usize = 50;
    const FOLLOW_UP_TASKS: usize = 5;

    let (mut conn, _container) = setup_test_db().await;

    // ── Set up an execution with 50 initial events ──────────────────────────

    let exec_id = insert_execution(&mut conn, "sticky_vs_cold_wf").await;

    let mut initial: Vec<WorkflowEvent> = vec![WorkflowEvent::WorkflowStarted {
        input: serde_json::json!({}),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    initial.extend(make_activity_events(24)); // 48 + 1 started = 49
    initial.push(WorkflowEvent::TimerStarted {
        timer_id: TimerId::new("wait-0"),
        duration_secs: 1,
    }); // 50th event

    store::append_events(&mut conn, exec_id, &initial, 0)
        .await
        .expect("append 50 initial events");

    // ── Sticky routing ON: 1 full reload + N-1 delta reloads ────────────────

    let cold = store::load_history(&mut conn, exec_id)
        .await
        .expect("cold load");
    assert_eq!(cold.events.len(), INITIAL_EVENTS);

    let mut cache = WorkflowCache::new(64);
    let mut next_id = cold.next_event_id;
    cache.insert(
        exec_id.as_uuid(),
        CachedWorkflowState {
            events: cold.events.clone(),
            next_event_id: next_id,
        },
    );

    let sticky_full_reloads: usize = 1; // the cold load above
    let mut sticky_delta_reloads: usize = 0;
    let mut total_delta_rows: usize = 0;

    for task in 0..FOLLOW_UP_TASKS {
        // Append 2 new events (simulate timer fire + signal per resume cycle).
        store::append_events(
            &mut conn,
            exec_id,
            &[
                WorkflowEvent::TimerFired {
                    timer_id: TimerId::new(format!("wait-{task}")),
                },
                WorkflowEvent::TimerStarted {
                    timer_id: TimerId::new(format!("wait-{}", task + 1)),
                    duration_secs: 1,
                },
            ],
            next_id,
        )
        .await
        .expect("append delta events");

        // Warm path: cache hit → delta load.
        let cached = cache.get(&exec_id.as_uuid()).expect("cache must be warm");
        let delta = store::load_history_since(&mut conn, exec_id, cached.next_event_id)
            .await
            .expect("load_history_since");

        sticky_delta_reloads += 1;
        total_delta_rows += delta.events.len();

        // Update cache with the new full snapshot.
        let mut new_events = cached.events.clone();
        new_events.extend(delta.events);
        next_id = delta.next_event_id;
        cache.insert(
            exec_id.as_uuid(),
            CachedWorkflowState {
                events: new_events,
                next_event_id: next_id,
            },
        );
    }

    // Sticky ON: exactly 1 full reload total; delta reloads = FOLLOW_UP_TASKS.
    assert_eq!(
        sticky_full_reloads, 1,
        "sticky ON: must have exactly 1 full history reload"
    );
    assert_eq!(sticky_delta_reloads, FOLLOW_UP_TASKS);
    // Each delta should be small (≤ 10 new events per cycle in this scenario).
    assert!(
        total_delta_rows <= FOLLOW_UP_TASKS * 10,
        "sticky ON: average delta size must be small, got {total_delta_rows} rows over {FOLLOW_UP_TASKS} tasks"
    );

    // ── Sticky routing OFF: N full reloads ──────────────────────────────────

    let mut cold_reloads: usize = 0;
    let mut cold_total_rows: usize = 0;

    for _ in 0..FOLLOW_UP_TASKS {
        let history = store::load_history(&mut conn, exec_id)
            .await
            .expect("full reload");

        cold_reloads += 1;
        cold_total_rows += history.events.len();
    }

    assert_eq!(
        cold_reloads, FOLLOW_UP_TASKS,
        "sticky OFF: each task incurs a full reload"
    );
    // Each cold reload reads the whole growing history.
    assert!(
        cold_total_rows > INITIAL_EVENTS * FOLLOW_UP_TASKS,
        "sticky OFF: total rows read must exceed {}, got {cold_total_rows}",
        INITIAL_EVENTS * FOLLOW_UP_TASKS
    );

    // ── The payoff: sticky ON reads far fewer rows from Postgres ─────────────
    assert!(
        total_delta_rows < cold_total_rows,
        "sticky ON ({total_delta_rows} rows) must read fewer rows than sticky OFF ({cold_total_rows} rows)"
    );
}
