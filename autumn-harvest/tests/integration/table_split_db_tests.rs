#![cfg(feature = "db")]
//! The split tables against a live Postgres (issue #2492).
//!
//! `harvest_workflow_executions`, `harvest_task_queue` and `harvest_schedules`
//! each have a core and an extension `table!` declaration. These tests write
//! every extension field through the flat models, and read it back. They also
//! cover the statement shapes that the bridge must keep working. These are a
//! batch insert, an insert with `ON CONFLICT`, a bridged update and a by-name
//! load.

use autumn_harvest::models::{
    HarvestSchedule, NewHarvestSchedule, NewTaskQueueItem, NewWorkflowExecution, TaskQueueItem,
    WorkflowExecution,
};
use autumn_harvest::schema::{
    harvest_schedules, harvest_task_queue, harvest_workflow_executions,
    harvest_workflow_executions_ext,
};
use autumn_harvest::table_split::SetExt;

use chrono::{Duration, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

async fn connect() -> (AsyncPgConnection, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
            .with_tag("16")
            .start()
            .await
            .expect("start the Postgres container");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    (conn, container)
}

/// An execution with every extension field set to a value that is not the
/// column default.
fn execution(id: Uuid, workflow_id: &str, parent: Uuid) -> NewWorkflowExecution<'_> {
    let now = Utc::now();
    NewWorkflowExecution {
        id,
        workflow_name: "split_flow",
        workflow_id,
        run_id: parent,
        shard_id: 0,
        input: json!({"n": 1}).into(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: Some(Duration::seconds(30)),
        deadline_at: Some(now + Duration::seconds(30)),
        chain_execution_timeout: Some(Duration::seconds(90)),
        chain_deadline_at: Some(now + Duration::seconds(90)),
        memo: Some(json!({"memo": true})),
        search_attrs: Some(json!({"k": "v"})),
        assigned_build_id: Some("build-7".to_string()),
        parent_close_policy: None,
        owner: Some("ops"),
        runbook_url: Some("https://runbook.example/split"),
        severity: Some("high"),
        context_headers: Some(json!({"x-tenant": "t1"})),
        sla: Some(Duration::seconds(60)),
        sla_deadline_at: Some(now + Duration::seconds(60)),
        schedule_id: None,
        scheduled_for: Some(now),
        workflow_attempt: 3,
        workflow_retry_policy: Some(json!({"max_attempts": 5})),
        retry_of_exec_id: None,
        origin: Some("manual_trigger"),
        completion_callbacks: Some(json!([{"url": "https://cb.example", "filter": null}])),
        continued_from_exec_id: Some(parent),
        first_exec_id: Some(parent),
        start_source: Some("api"),
        start_source_ref: Some("ref-1"),
        started_by: Some("ada"),
        quota_key: Some("quota-a"),
        tenant: Some("t1"),
    }
}

async fn load_execution(conn: &mut AsyncPgConnection, id: Uuid) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(id)
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load the execution")
}

#[tokio::test]
async fn an_execution_round_trips_every_extension_field() {
    let (mut conn, _container) = connect().await;
    let id = Uuid::new_v4();
    let parent = Uuid::new_v4();
    let workflow_id = format!("split-{id}");
    let row = execution(id, &workflow_id, parent);
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert through both declarations");

    let read = load_execution(&mut conn, id).await;
    assert_eq!(read.run_id, parent);
    assert_eq!(read.workflow_attempt, 3);
    assert_eq!(read.chain_execution_timeout, Some(Duration::seconds(90)));
    assert_eq!(read.memo, Some(json!({"memo": true})));
    assert_eq!(read.assigned_build_id.as_deref(), Some("build-7"));
    assert_eq!(
        read.runbook_url.as_deref(),
        Some("https://runbook.example/split")
    );
    assert_eq!(read.context_headers, Some(json!({"x-tenant": "t1"})));
    assert!(read.sla_deadline_at.is_some());
    assert!(read.scheduled_for.is_some());
    assert_eq!(read.workflow_retry_policy, Some(json!({"max_attempts": 5})));
    assert_eq!(read.origin.as_deref(), Some("manual_trigger"));
    assert!(read.completion_callbacks.is_some());
    assert_eq!(read.continued_from_exec_id, Some(parent));
    assert_eq!(read.first_exec_id, Some(parent));
    assert_eq!(read.start_source_ref.as_deref(), Some("ref-1"));
    assert_eq!(read.started_by.as_deref(), Some("ada"));
    assert_eq!(read.quota_key.as_deref(), Some("quota-a"));
    // Core fields read from the same row.
    assert_eq!(read.workflow_id, workflow_id);
    assert_eq!(read.tenant.as_deref(), Some("t1"));
    // Columns the insert leaves out read their defaults.
    assert!(!read.sla_breached);
    assert_eq!(read.nd_block_count, 0);
    assert_eq!(
        read.rebalance,
        autumn_harvest::models::RebalanceMarkers::default()
    );
}

#[tokio::test]
async fn an_execution_insert_keeps_its_on_conflict_clause() {
    let (mut conn, _container) = connect().await;
    let id = Uuid::new_v4();
    let workflow_id = format!("split-conflict-{id}");
    let row = execution(id, &workflow_id, Uuid::new_v4());
    for expected in [1, 0] {
        let inserted = diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .on_conflict(harvest_workflow_executions::id)
            .do_nothing()
            .execute(&mut conn)
            .await
            .expect("insert with ON CONFLICT");
        assert_eq!(inserted, expected);
    }
}

#[tokio::test]
async fn an_update_sets_core_and_extension_columns_together() {
    let (mut conn, _container) = connect().await;
    let id = Uuid::new_v4();
    let workflow_id = format!("split-update-{id}");
    let row = execution(id, &workflow_id, Uuid::new_v4());
    diesel::insert_into(harvest_workflow_executions::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert");

    let updated = diesel::update(
        harvest_workflow_executions::table
            .find(id)
            .filter(harvest_workflow_executions_ext::workflow_attempt.eq(3)),
    )
    .set((
        harvest_workflow_executions::owner.eq("platform"),
        SetExt((
            harvest_workflow_executions_ext::triage_note.eq("looked at it"),
            harvest_workflow_executions_ext::sla_breached.eq(true),
        )),
    ))
    .execute(&mut conn)
    .await
    .expect("bridged update");
    assert_eq!(updated, 1);

    let read = load_execution(&mut conn, id).await;
    assert_eq!(read.owner.as_deref(), Some("platform"));
    assert_eq!(read.triage_note.as_deref(), Some("looked at it"));
    assert!(read.sla_breached);
}

fn task(id: Uuid, queue: &str) -> NewTaskQueueItem<'_> {
    NewTaskQueueItem {
        id,
        queue_name: queue,
        task_type: "activity",
        workflow_exec_id: None,
        activity_name: Some("charge"),
        activity_id: Some(Uuid::new_v4()),
        input: json!({"amount": 5}).into(),
        priority: 2,
        max_attempts: 4,
        scheduled_at: Utc::now(),
        heartbeat_timeout: Some(Duration::seconds(5)),
        start_to_close: Some(Duration::seconds(20)),
        schedule_to_start: Some(Duration::seconds(10)),
        retry_policy: Some(json!({"backoff": 2})),
        heartbeat_details: None,
        sticky_worker_id: None,
        sticky_until: None,
        sticky_timeout: None,
        trace_context: Some(json!({"traceparent": "00-1"})),
        concurrency_key: None,
        concurrency_cap: Some(3),
        required_build_id: Some("build-9"),
        rate_limit_key: Some("rate-a"),
        schedule_to_close_at: None,
        required_capabilities: Some(json!(["gpu"])),
        context_headers: None,
        session_id: None,
        new_start: true,
        fairness_key: Some("fair-a"),
        decision_partition: Some(7),
    }
}

#[tokio::test]
async fn a_task_batch_round_trips_every_extension_field() {
    let (mut conn, _container) = connect().await;
    let queue = format!("split-queue-{}", Uuid::new_v4());
    let rows = [task(Uuid::new_v4(), &queue), task(Uuid::new_v4(), &queue)];
    let inserted = diesel::insert_into(harvest_task_queue::table)
        .values(&rows[..])
        .execute(&mut conn)
        .await
        .expect("batch insert through both declarations");
    assert_eq!(inserted, 2);

    let read: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::queue_name.eq(&queue))
        .select(TaskQueueItem::as_select())
        .load(&mut conn)
        .await
        .expect("load the batch");
    assert_eq!(read.len(), 2);
    for item in &read {
        assert_eq!(item.retry_policy, Some(json!({"backoff": 2})));
        assert_eq!(item.trace_context, Some(json!({"traceparent": "00-1"})));
        assert_eq!(item.heartbeat_timeout, Some(Duration::seconds(5)));
        assert_eq!(item.start_to_close, Some(Duration::seconds(20)));
        assert_eq!(item.schedule_to_start, Some(Duration::seconds(10)));
        assert_eq!(item.concurrency_cap, Some(3));
        assert_eq!(item.required_build_id.as_deref(), Some("build-9"));
        assert_eq!(item.rate_limit_key.as_deref(), Some("rate-a"));
        assert_eq!(item.required_capabilities, Some(json!(["gpu"])));
        assert!(item.new_start);
        assert_eq!(item.fairness_key.as_deref(), Some("fair-a"));
        // Defaults on columns the insert leaves out.
        assert!(!item.wake_requested);
        assert!(item.handler_started_at.is_none());
    }

    // A by-name load maps the extension fields through their SQL types.
    let by_name: TaskQueueItem =
        diesel::sql_query("SELECT * FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(read[0].id)
            .get_result(&mut conn)
            .await
            .expect("load by name");
    assert_eq!(by_name.retry_policy, read[0].retry_policy);
    assert_eq!(by_name.fairness_key, read[0].fairness_key);
    assert_eq!(by_name.heartbeat_timeout, read[0].heartbeat_timeout);
}

#[tokio::test]
async fn a_schedule_round_trips_its_extension_field() {
    let (mut conn, _container) = connect().await;
    let id = Uuid::new_v4();
    let name = format!("split-schedule-{id}");
    let row = NewHarvestSchedule {
        id,
        dag_name: None,
        schedule_expr: Some("0 * * * *"),
        timezone: "UTC",
        catchup: false,
        max_active_runs: 1,
        is_paused: false,
        workflow_name: Some(&name),
        workflow_input: Some(json!({"report": "daily"})),
        queue_name: Some("default"),
        jitter_secs: 0,
        overlap_policy: "skip",
        buffered_runs: json!([]),
        buffer_all_max: 1,
        calendar_name: None,
        skip_policy: "none",
    };
    diesel::insert_into(harvest_schedules::table)
        .values(&row)
        .execute(&mut conn)
        .await
        .expect("insert the schedule");

    let read: HarvestSchedule = harvest_schedules::table
        .find(id)
        .select(HarvestSchedule::as_select())
        .first(&mut conn)
        .await
        .expect("load the schedule");
    assert_eq!(read.workflow_input, Some(json!({"report": "daily"})));
    assert_eq!(read.workflow_name.as_deref(), Some(name.as_str()));
    assert!(read.last_run_at.is_none());
    assert!(read.retry_policy.is_none());
}
