//! Integration tests for `GET /api/harvest/workflows/{id}/stream` — the
//! ephemeral progress SSE subscriber for `ctx.publish_progress` (issue #791).
//!
//! End-to-end proof (AC3/AC4): a real worker runs a workflow that publishes
//! ordered progress chunks then completes; a subscriber on the stream receives
//! every chunk in publish order with monotonically increasing `id:` seq, the
//! exact chunk JSON in each `event: progress` frame, and a terminal `event: end`
//! when the workflow reaches a terminal state. Also: an already-terminal
//! execution closes immediately with `event: end`, and an unknown execution id
//! returns 404.
//!
//! Dual-mode DB: prefers `HARVEST_TEST_DATABASE_URL` (creates a fresh uniquely
//! named database on that server, migrates it) so the suite is runnable without
//! Docker; falls back to testcontainers Postgres 16 (the mode CI runs).

#![allow(clippy::too_many_lines)]

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use futures::StreamExt as _;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// Keeps the backing database alive for the test's duration. In testcontainers
/// mode this owns the container; in local-Postgres mode it is a marker (the
/// created database is left in place — the sandbox is ephemeral).
#[allow(dead_code)]
enum DbGuard {
    // Boxed to keep the enum small (clippy::large_enum_variant): the container
    // variant is ~800 bytes while LocalPg carries no data.
    Container(Box<ContainerAsync<Postgres>>),
    LocalPg,
}

/// Replace the database name (final path segment) in a Postgres URL.
fn with_db(base: &str, db: &str) -> String {
    let (before, _after) = base.rsplit_once('/').expect("url has a database segment");
    format!("{before}/{db}")
}

/// Provision a migrated Postgres database. Prefers `HARVEST_TEST_DATABASE_URL`
/// (Docker-free); otherwise spins up a testcontainers Postgres 16 (CI mode).
async fn setup_database() -> (String, DbGuard) {
    if let Ok(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        let db_name = format!("harvest_progress_{}", uuid::Uuid::new_v4().simple());
        let mut admin = AsyncPgConnection::establish(&admin_url)
            .await
            .expect("connect admin");
        diesel::sql_query(format!("CREATE DATABASE \"{db_name}\""))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let url = with_db(&admin_url, &db_name);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect test db");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("run migrations");
        (url, DbGuard::LocalPg)
    } else {
        let container = Postgres::default()
            .with_init_sql(autumn_harvest::full_migrations_sql().as_bytes().to_vec())
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, DbGuard::Container(Box::new(container)))
    }
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

/// A workflow that publishes three ordered progress chunks then completes in a
/// single decision cycle (progress flushes on the terminal persist).
fn progress_workflow<'a>(
    ctx: &'a autumn_harvest::context::WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.publish_progress(json!({"step": 1, "msg": "starting"}))
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"step": 2, "msg": "working"}))
            .map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"step": 3, "msg": "done"}))
            .map_err(|e| e.to_string())?;
        Ok(input)
    })
}

/// A workflow that publishes one chunk, suspends on a durable 1-second timer
/// (ending decision cycle 1), then resumes (decision cycle 2) and publishes a
/// second chunk before completing. The timer's `TimerStarted`/`TimerFired`
/// events grow the loaded-history length (the `seq` epoch) between the two
/// cycles, so the second chunk's `seq` must land in a strictly higher epoch than
/// the first — the AC6 cross-cycle monotonicity crux.
fn progress_multicycle_workflow<'a>(
    ctx: &'a autumn_harvest::context::WorkflowContext,
    input: Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.publish_progress(json!({"cycle": 1, "msg": "before timer"}))
            .map_err(|e| e.to_string())?;
        // Suspend for a durable timer — this ends decision cycle 1 and appends
        // TimerStarted; on fire, TimerFired is ingested before cycle 2 resumes.
        ctx.timer("gate", 1).await.map_err(|e| e.to_string())?;
        ctx.publish_progress(json!({"cycle": 2, "msg": "after timer"}))
            .map_err(|e| e.to_string())?;
        Ok(input)
    })
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: None,
        sla: None,
        concurrency: None,
        debounce: None,
        batch: None,
        throttle: None,
        max_input_bytes: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
        retry_policy: None,
        owner: None,
        runbook_url: None,
        severity: None,
    }
}

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![
            wf_info("progress_wf", progress_workflow),
            wf_info("progress_multicycle_wf", progress_multicycle_workflow),
        ],
        vec![],
    ))
}

fn build_app(pool: &DbPool, url: &str) -> axum::Router {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    // Required so the stream handler can open a LISTEN connection for the shard.
    api_state.set_workflow_result_notification_database_url(url.to_string());
    // Shorten the terminal-close poll cadence so `event: end` follows the last
    // chunk quickly (bounded by min(keepalive, PROGRESS_STREAM_KEEPALIVE)).
    api_state.set_sse_keepalive_interval(Duration::from_millis(150));
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("progress-stream-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

fn start_params(exec_id: ExecutionId, workflow_id: &'static str) -> StartWorkflowParams<'static> {
    start_params_named(exec_id, "progress_wf", workflow_id)
}

fn start_params_named(
    exec_id: ExecutionId,
    workflow_name: &'static str,
    workflow_id: &'static str,
) -> StartWorkflowParams<'static> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: json!({"ok": true}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: autumn_harvest::WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        inherited_chain_deadline_at: None,
        concurrency_key: None,
        concurrency_limit: None,
        priority: Priority::default(),
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
    }
}

// ── SSE frame reader ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SseFrame {
    id: Option<String>,
    event: Option<String>,
    data: String,
}

fn parse_frame(raw: &str) -> Option<SseFrame> {
    let mut id = None;
    let mut event = None;
    let mut data_lines: Vec<String> = Vec::new();
    let mut has_field = false;
    for line in raw.split('\n') {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue; // blank line / keepalive comment
        }
        if let Some(v) = line.strip_prefix("id:") {
            id = Some(v.trim().to_string());
            has_field = true;
        } else if let Some(v) = line.strip_prefix("event:") {
            event = Some(v.trim().to_string());
            has_field = true;
        } else if let Some(v) = line.strip_prefix("data:") {
            data_lines.push(v.strip_prefix(' ').unwrap_or(v).to_string());
            has_field = true;
        }
    }
    if !has_field {
        return None;
    }
    Some(SseFrame {
        id,
        event,
        data: data_lines.join("\n"),
    })
}

/// Consume an SSE response body, collecting frames until a terminal (`end` /
/// `error`) frame arrives or `deadline` elapses. Returns the frames plus the
/// wall-clock elapsed at the first `progress` frame (time-to-first-chunk).
async fn read_sse(
    resp: axum::response::Response,
    deadline: Duration,
) -> (Vec<SseFrame>, Option<Duration>) {
    let mut stream = resp.into_body().into_data_stream();
    let mut buf = String::new();
    let mut frames: Vec<SseFrame> = Vec::new();
    let mut first_progress_at: Option<Duration> = None;
    let start = std::time::Instant::now();
    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            () = &mut sleep => break,
            chunk = stream.next() => match chunk {
                Some(Ok(bytes)) => {
                    buf.push_str(&String::from_utf8_lossy(&bytes));
                    while let Some(idx) = buf.find("\n\n") {
                        let raw: String = buf[..idx].to_string();
                        buf.drain(..idx + 2);
                        let Some(frame) = parse_frame(&raw) else { continue };
                        if frame.event.as_deref() == Some("progress")
                            && first_progress_at.is_none()
                        {
                            first_progress_at = Some(start.elapsed());
                        }
                        let terminal = matches!(frame.event.as_deref(), Some("end" | "error"));
                        frames.push(frame);
                        if terminal {
                            return (frames, first_progress_at);
                        }
                    }
                }
                Some(Err(_)) | None => break,
            }
        }
    }
    (frames, first_progress_at)
}

async fn sse_response(app: &axum::Router, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("stream request")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn progress_stream_delivers_ordered_chunks_and_ends_on_terminal() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, &url);
    let mut conn = pool.get().await.unwrap();

    // Start the workflow (task enqueued, RUNNING) BEFORE the worker runs.
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, "progress-ordered"), None)
        .await
        .unwrap();

    // Open the subscription first so LISTEN is established before any chunk is
    // published (progress has no backfill). `oneshot` resolves once the handler
    // has connected the listener and returned the streaming response.
    let resp = sse_response(&app, &format!("/workflows/{exec_id}/stream")).await;
    assert_eq!(resp.status(), StatusCode::OK, "stream must open with 200");

    // Now run the worker so the workflow publishes chunks and completes.
    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "progress-stream-worker".to_string();
    runtime_config.queues = vec!["default".to_string()];
    runtime_config.poll_interval = Duration::from_millis(20);
    runtime_config.shard_assignments = vec![ShardId::new(0)];
    let worker = Arc::new(Worker::new(runtime_config, test_registry()).unwrap());
    let worker_handle = {
        let worker = worker.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    let (frames, first_at) = read_sse(resp, Duration::from_secs(20)).await;

    worker.shutdown();
    let _ = worker_handle.await;

    let progress: Vec<&SseFrame> = frames
        .iter()
        .filter(|f| f.event.as_deref() == Some("progress"))
        .collect();
    assert_eq!(
        progress.len(),
        3,
        "expected exactly 3 progress frames, got frames: {frames:#?}"
    );

    // (a) Ordered, monotonically increasing `id:` seq.
    let seqs: Vec<u64> = progress
        .iter()
        .map(|f| {
            f.id.as_deref()
                .expect("progress frame carries an id (seq)")
                .parse::<u64>()
                .expect("seq is a u64")
        })
        .collect();
    assert!(
        seqs.windows(2).all(|w| w[1] > w[0]),
        "progress seqs must be strictly increasing, got {seqs:?}"
    );

    // (b) Each frame carries the exact chunk JSON, in publish order.
    let expected = [
        json!({"step": 1, "msg": "starting"}),
        json!({"step": 2, "msg": "working"}),
        json!({"step": 3, "msg": "done"}),
    ];
    for (frame, want) in progress.iter().zip(expected.iter()) {
        let got: Value = serde_json::from_str(&frame.data).expect("chunk data is JSON");
        assert_eq!(&got, want, "chunk JSON must match the published chunk");
    }

    // (c) Stream ends with a terminal `event: end` frame.
    assert_eq!(
        frames.last().and_then(|f| f.event.as_deref()),
        Some("end"),
        "stream must close with an event:end frame on terminal state"
    );

    // (d) Time-to-first-chunk is well under a second (smoke bound).
    let first_at = first_at.expect("a first progress frame arrived");
    assert!(
        first_at < Duration::from_secs(2),
        "time-to-first-chunk must be well under a second, was {first_at:?}"
    );
}

#[tokio::test]
async fn progress_stream_on_already_terminal_execution_closes_immediately() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, &url);
    let mut conn = pool.get().await.unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(&mut conn, start_params(exec_id, "progress-terminal"), None)
        .await
        .unwrap();
    // Seal it terminal directly (no worker) — no chunks will ever be published.
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("COMPLETED"),
            harvest_workflow_executions::completed_at.eq(chrono::Utc::now()),
        ))
        .execute(&mut conn)
        .await
        .unwrap();

    let resp = sse_response(&app, &format!("/workflows/{exec_id}/stream")).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let (frames, first_at) = read_sse(resp, Duration::from_secs(5)).await;
    assert!(
        first_at.is_none(),
        "an already-terminal execution must emit no progress frames"
    );
    assert_eq!(
        frames.iter().filter(|f| f.event.is_some()).count(),
        1,
        "exactly one (end) frame expected, got: {frames:#?}"
    );
    assert_eq!(
        frames.last().and_then(|f| f.event.as_deref()),
        Some("end"),
        "already-terminal execution closes immediately with event:end"
    );
}

/// AC6 (cross-cycle crux): a REAL workflow that publishes across TWO decision
/// cycles (separated by a durable timer) must emit progress `seq`s whose EPOCH
/// strictly grows across the cycle boundary — proving the epoch actually
/// advances with the loaded-history length, not just the within-cycle local
/// index. This exercises the real worker path (suspend → append timer events →
/// resume) end-to-end and reads the real `seq`s off the live SSE stream.
#[tokio::test]
async fn progress_stream_seq_strictly_increases_across_decision_cycles() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, &url);
    let mut conn = pool.get().await.unwrap();

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    start_or_load_workflow_execution(
        &mut conn,
        start_params_named(exec_id, "progress_multicycle_wf", "progress-multicycle"),
        None,
    )
    .await
    .unwrap();

    // Subscribe before the worker runs (progress has no backfill).
    let resp = sse_response(&app, &format!("/workflows/{exec_id}/stream")).await;
    assert_eq!(resp.status(), StatusCode::OK, "stream must open with 200");

    let mut runtime_config = WorkerRuntimeConfig::from(WorkerConfig::default());
    runtime_config.worker_id = "progress-multicycle-worker".to_string();
    runtime_config.queues = vec!["default".to_string()];
    runtime_config.poll_interval = Duration::from_millis(20);
    runtime_config.shard_assignments = vec![ShardId::new(0)];
    let worker = Arc::new(Worker::new(runtime_config, test_registry()).unwrap());
    let worker_handle = {
        let worker = worker.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            worker.run(&pool).await;
        })
    };

    // Deadline must exceed the 1-second durable timer.
    let (frames, _first_at) = read_sse(resp, Duration::from_secs(20)).await;

    worker.shutdown();
    let _ = worker_handle.await;

    let progress: Vec<&SseFrame> = frames
        .iter()
        .filter(|f| f.event.as_deref() == Some("progress"))
        .collect();
    assert_eq!(
        progress.len(),
        2,
        "expected exactly 2 progress frames (one per cycle), got frames: {frames:#?}"
    );

    let seqs: Vec<u64> = progress
        .iter()
        .map(|f| {
            f.id.as_deref()
                .expect("progress frame carries an id (seq)")
                .parse::<u64>()
                .expect("seq is a u64")
        })
        .collect();

    // Strictly increasing overall.
    assert!(
        seqs[1] > seqs[0],
        "cross-cycle progress seqs must strictly increase, got {seqs:?}"
    );

    // The CRUX: the epoch (high bits, seq >> PROGRESS_SEQ_LOCAL_BITS=24) must be
    // strictly higher for the second cycle — the loaded-history length grew when
    // the timer's TimerStarted/TimerFired events were appended between cycles.
    // A within-cycle local-index bump would leave the epoch unchanged; this
    // asserts the epoch itself advanced across the decision-cycle boundary.
    let epoch0 = seqs[0] >> 24;
    let epoch1 = seqs[1] >> 24;
    assert!(
        epoch1 > epoch0,
        "the second cycle's progress seq must land in a strictly higher epoch \
         (epoch grows with loaded-history length across cycles): epoch1={epoch1} \
         !> epoch0={epoch0} (seqs {seqs:?})"
    );

    // Stream closes on terminal state.
    assert_eq!(
        frames.last().and_then(|f| f.event.as_deref()),
        Some("end"),
        "stream must close with event:end on terminal state"
    );
}

#[tokio::test]
async fn progress_stream_unknown_execution_returns_404() {
    let (url, _guard) = setup_database().await;
    let pool = build_pool(&url);
    let app = build_app(&pool, &url);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let resp = sse_response(&app, &format!("/workflows/{unknown}/stream")).await;
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "an unknown execution id must return 404"
    );
}
