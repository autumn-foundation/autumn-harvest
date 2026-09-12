//! Ledger perf pass: the Dead Letter Queue admin page's per-dead-letter
//! workflow-name and last-events lookups.
//!
//! # Workload
//!
//! `GET /ui/dead-letters` (Vantage's DLQ triage view) is the page an
//! operator reloads repeatedly during incident response. It lists up to
//! `MAX_PAGE_SIZE` (200) dead letters per page. For each row it also shows
//! the owning workflow's name and its last 10 history events leading up to
//! the failure.
//!
//! # Fixture
//!
//! Seeded, deterministic, one full page (200 dead letters): most point to a
//! distinct workflow execution each, a handful share one execution (two
//! task failures against the same run), and one has no execution at all
//! (`workflow_exec_id = NULL`, a queue-level dead letter). Execution event
//! counts are varied around the `LIMIT 10` boundary: 0, 3, exactly 10, and
//! 15, so the last-10-events lookup is exercised at and past its own limit.
//!
//! Run against a real Postgres via `HARVEST_TEST_DATABASE_URL` (this test
//! does not fall back to a testcontainer; Docker is not assumed available).

use std::collections::HashMap;
use std::sync::Arc;

use autumn_harvest::execution::StartWorkflowParams;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::models::NewHarvestEvent;
use autumn_harvest::scheduler::SchedulerMonitor;
use autumn_harvest::schema::harvest_events;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{RetentionConfig, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime};
use autumn_harvest_plugin::ui::harvest_ui_router;
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use std::fmt::Write as _;
use tower::ServiceExt;
use uuid::Uuid;

const TOTAL_DEAD_LETTERS: usize = 200;
/// Every 40th dead letter shares its execution with the one right after it
/// (two task failures against one run). Deliberately not a divisor of
/// `TOTAL_DEAD_LETTERS` step count -- just a fixed, small, realistic count
/// of "hot" executions, not most of the fixture.
const SHARED_EXEC_EVERY: usize = 40;

fn admin_url_or_skip() -> Option<String> {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return Some(url);
    }
    assert!(
        std::env::var("CI").is_err(),
        "dead-letter UI batch perf test needs HARVEST_TEST_DATABASE_URL under CI"
    );
    eprintln!("SKIP: HARVEST_TEST_DATABASE_URL not set and no Docker fallback here");
    None
}

async fn create_fresh_database(admin_url: &str) -> String {
    let mut admin_conn = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect admin database");
    let db_name = format!("harvest_dlq_ui_perf_{}", Uuid::new_v4().simple());
    diesel::sql_query(format!("CREATE DATABASE {db_name}"))
        .execute(&mut admin_conn)
        .await
        .expect("create fresh database");
    let (prefix, _old_db) = admin_url
        .rsplit_once('/')
        .expect("admin url has a path separator before the database name");
    let url = format!("{prefix}/{db_name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect fresh database");
    conn_batch_execute(&mut conn, &autumn_harvest::test_init_sql()).await;
    url
}

async fn conn_batch_execute(conn: &mut AsyncPgConnection, sql: &str) {
    use diesel_async::SimpleAsyncConnection;
    conn.batch_execute(sql).await.expect("apply schema");
}

fn build_test_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("build test pool")
}

fn echo_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "dlq_perf_workflow",
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
        }],
        vec![],
    ))
}

fn build_single_shard_ui_app(database_url: &str) -> axum::Router {
    let pool = build_test_pool(database_url);
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    api_state.install(HarvestApiRuntime::new(
        echo_registry(),
        Arc::new(HashMap::new()),
        Arc::new(Vec::new()),
        None,
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(RetentionConfig::default()),
        ShardRouter::single(),
    ));
    axum::Router::new()
        .nest("/ui", harvest_ui_router(api_state))
        .with_state(AppState::for_test().with_profile("test"))
}

async fn insert_workflow(database_url: &str, workflow_id: &str) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for workflow insert");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "dlq_perf_workflow",
            workflow_id,
            exec_id,
            input: json!({ "workflow_id": workflow_id }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: autumn_harvest::types::Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: None,
            origin: None,
            completion_callbacks: None,
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("start workflow");
    exec_id
}

/// Event `event_type` uses a `<` / `>` delimiter (`"Ev<N>"`) so an HTML
/// substring search for one execution's event can never match another
/// execution's, or a different event count of the SAME execution: `exec`
/// and `event_id` are both delimited on both sides (`N` / `Z`), so
/// `event_marker(3, 1)` (`"EvX3N1Z"`) is not a substring of
/// `event_marker(3, 15)` (`"EvX3N15Z"`) or of `event_marker(31, 1)`
/// (`"EvX31N1Z"`).
fn event_marker(exec: usize, event_id: i32) -> String {
    format!("EvX{exec}N{event_id}Z")
}

async fn append_events(database_url: &str, exec: usize, exec_id: ExecutionId, count: i32) {
    if count == 0 {
        return;
    }
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for event insert");
    for event_id in 1..=count {
        diesel::insert_into(harvest_events::table)
            .values(&NewHarvestEvent {
                workflow_exec_id: exec_id.as_uuid(),
                event_id,
                event_type: &event_marker(exec, event_id),
                event_data: json!({ "event": event_id }),
            })
            .execute(&mut conn)
            .await
            .expect("insert test event");
    }
}

async fn insert_dead_letter(database_url: &str, exec_id: Option<Uuid>, ordinal: usize) -> Uuid {
    let mut conn = AsyncPgConnection::establish(database_url)
        .await
        .expect("connect for dead-letter insert");
    autumn_harvest::dlq::dead_letter(
        &mut conn,
        &autumn_harvest::dlq::NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "activity".to_string(),
            workflow_exec_id: exec_id,
            activity_name: Some("charge_card".to_string()),
            input: json!({ "ordinal": ordinal }),
            error: format!("attempt {ordinal}: downstream timeout"),
            attempts: 1,
            owner: None,
            severity: None,
        },
    )
    .await
    .expect("dead-letter insert")
}

#[derive(QueryableByName)]
struct StatRow {
    #[diesel(sql_type = Text)]
    label: String,
    #[diesel(sql_type = BigInt)]
    calls: i64,
}

async fn pg_stat_statements_available(conn: &mut AsyncPgConnection) -> bool {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
    diesel::sql_query(
        "SELECT 1::bigint AS calls, ''::text AS label FROM pg_stat_statements WHERE FALSE",
    )
    .load::<StatRow>(conn)
    .await
    .is_ok()
}

async fn fetch_html(app: &axum::Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("GET request failed");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Deterministic per-index event count: exercises 0, a short tail, exactly
/// the `LIMIT 10` boundary, and past it.
const fn event_count_for(i: usize) -> i32 {
    match i % 4 {
        0 => 0,
        1 => 3,
        2 => 10,
        _ => 15,
    }
}

/// Seeds [`TOTAL_DEAD_LETTERS`] dead letters and returns the execution
/// ordinal `i` of one execution with 15 events (past the `LIMIT 10`
/// boundary), for the truncation check in the test below. That ordinal is
/// also the exec-id parameter [`event_marker`] takes, so the caller can
/// reconstruct that execution's own event markers without looking anything
/// up. Every other dead letter's expected workflow name is checked instead
/// through a plain page-wide count, since the fixture always names the
/// same one workflow type.
/// Returns `(boundary_exec, named_row_count)`: the ordinal from the doc
/// comment above, and the number of seeded rows whose `exec_uuid` ended up
/// `Some` (and so should render `dlq_perf_workflow` as their name) --
/// computed from the same branch the seeding loop itself takes, rather
/// than a separately hand-counted expectation that could drift from it.
async fn seed_fixture(database_url: &str) -> (usize, usize) {
    let mut last_exec: Option<Uuid> = None;
    let mut boundary_exec = None;
    let mut named_rows = 0usize;

    for i in 0..TOTAL_DEAD_LETTERS {
        let exec_uuid = if i % SHARED_EXEC_EVERY == 1 {
            // Share the previous row's execution instead of minting a new one.
            // `last_exec` is still `None` here for i == 1 (the row right
            // after the deliberately exec-less i == 0), so this branch can
            // itself produce a `None`.
            last_exec
        } else if i == 0 {
            // The very first row is the queue-level, execution-less case.
            None
        } else {
            let workflow_id = format!("dlq-perf-{i}");
            let exec_id = insert_workflow(database_url, &workflow_id).await;
            let count = event_count_for(i);
            append_events(database_url, i, exec_id, count).await;
            last_exec = Some(exec_id.as_uuid());
            if count == 15 && boundary_exec.is_none() {
                boundary_exec = Some(i);
            }
            Some(exec_id.as_uuid())
        };

        if exec_uuid.is_some() {
            named_rows += 1;
        }
        insert_dead_letter(database_url, exec_uuid, i).await;
    }

    (
        boundary_exec.expect("fixture always seeds at least one 15-event execution"),
        named_rows,
    )
}

async fn reset_pg_stat_statements(conn: &mut AsyncPgConnection) -> bool {
    let has_stats = pg_stat_statements_available(conn).await;
    if has_stats {
        let _ = diesel::sql_query(
            "SELECT pg_stat_statements_reset(0, \
                    (SELECT oid FROM pg_database WHERE datname = current_database()), 0)",
        )
        .execute(conn)
        .await;
    }
    has_stats
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_letter_ui_page_hydrates_details_correctly_and_batches_lookups() {
    let Some(admin_url) = admin_url_or_skip() else {
        return;
    };
    let database_url = create_fresh_database(&admin_url).await;
    let (boundary_exec, named_rows) = seed_fixture(&database_url).await;

    let app = build_single_shard_ui_app(&database_url);

    // -- profile: pg_stat_statements calls for the per-row lookups ---------
    let mut probe_conn = AsyncPgConnection::establish(&database_url)
        .await
        .expect("connect for stats probe");
    let has_stats = reset_pg_stat_statements(&mut probe_conn).await;

    let (status, html) = fetch_html(&app, "/ui/dead-letters?limit=200").await;
    assert_eq!(status, StatusCode::OK, "DLQ page should render: {html}");

    // -- correctness: every named dead letter's workflow name renders, and
    // the boundary row (15 events) shows only its last 10 (events 6-15), not
    // the 5 that fall outside the window. -----------------------------------
    assert!(
        html.matches("dlq_perf_workflow").count() >= named_rows,
        "expected every named dead letter's workflow_name to render at least once"
    );
    assert!(
        html.contains(&event_marker(boundary_exec, 15))
            && html.contains(&event_marker(boundary_exec, 6)),
        "boundary execution {boundary_exec} should show its last 10 events (6-15)"
    );
    assert!(
        !html.contains(&event_marker(boundary_exec, 5))
            && !html.contains(&event_marker(boundary_exec, 1)),
        "boundary execution {boundary_exec} should NOT show events past the LIMIT 10 window"
    );

    if has_stats {
        let rows: Vec<StatRow> = diesel::sql_query(
            "SELECT \
                CASE \
                    WHEN query ILIKE '%harvest_workflow_executions%' AND query ILIKE '%workflow_name%' \
                         AND query NOT ILIKE '%harvest_events%' THEN 'workflow_name_lookup' \
                    WHEN query ILIKE '%harvest_events%' AND query ILIKE '%LIMIT%' THEN 'events_lookup' \
                    WHEN query ILIKE '%harvest_dead_letters%' AND query ILIKE '%ORDER BY%' THEN 'dlq_list' \
                    ELSE 'other' \
                END AS label, \
                calls \
             FROM pg_stat_statements \
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND (query ILIKE '%harvest_workflow_executions%' \
                    OR query ILIKE '%harvest_events%' \
                    OR query ILIKE '%harvest_dead_letters%')",
        )
        .load(&mut probe_conn)
        .await
        .expect("query pg_stat_statements");

        let mut summary = String::new();
        for r in &rows {
            let _ = writeln!(summary, "{}: calls={}", r.label, r.calls);
        }
        eprintln!(
            "== pg_stat_statements after one /ui/dead-letters?limit=200 render ==\n{summary}"
        );

        // Regression guard: both lookups must stay batched at one call per
        // page, never one call per dead-letter row. See
        // docs/perf-artifacts/dead-letters-ui-detail-batch/ for the captured
        // before/after evidence (198 calls each, pre-fix, at this same
        // 200-row fixture size).
        let calls_for = |label: &str| -> i64 {
            rows.iter()
                .find(|r| r.label == label)
                .map_or(0, |r| r.calls)
        };
        assert_eq!(
            calls_for("workflow_name_lookup"),
            1,
            "workflow-name lookup must be one batched query per page, not one per row: {summary}"
        );
        assert_eq!(
            calls_for("events_lookup"),
            1,
            "last-events lookup must be one batched query per page, not one per row: {summary}"
        );
    } else {
        eprintln!("SKIP: pg_stat_statements not available on this server");
    }
}
