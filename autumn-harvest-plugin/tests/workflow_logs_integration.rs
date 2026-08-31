//! HTTP integration tests for `GET /api/harvest/workflows/{id}/logs` (issue #790).
//!
//! Tests run against a real Postgres instance (testcontainers, or an existing
//! database via `HARVEST_TEST_DATABASE_URL`) and exercise the wiring the core
//! DB tests in `autumn-harvest/tests/integration/workflow_logs_tests.rs` cannot:
//! routing, admin gating, param validation, response shape, and pagination.
//!
//! Scenarios covered:
//!   (a) The **success metric** — one call returns 100% of a run's lines, in
//!       emission order, with zero external log-aggregation correlation.
//!   (b) `?level=` filters; an unknown level is a 400, not a silent empty page.
//!   (c) Cursor pagination is exclusive and `next_cursor` terminates.
//!   (d) `?since=` bounds by `occurred_at`; a malformed value is a 400.
//!   (e) A run that never logged returns 200 with an empty list (not a 404) —
//!       the sink being disabled is not an error (AC6/AC7).
//!   (f) Unknown execution id → 404; malformed id → 400.
//!   (g) The cap marker surfaces `truncated: true` filter-independently (AC4).
//!   (h) The route is admin-gated (AC3).

#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::store;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{
    StartWorkflowParams, WorkflowIdReusePolicy, start_or_load_workflow_execution,
};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

/// Hold the container (when one was started) so it is not dropped mid-test.
struct TestDb {
    url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

/// Prefer an already-migrated database via `HARVEST_TEST_DATABASE_URL` (the
/// `retention_overrides_tests.rs` precedent, which lets the suite run where
/// Docker is unavailable); otherwise start a testcontainer.
async fn setup_database() -> TestDb {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return TestDb {
            url,
            _container: None,
        };
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    TestDb {
        url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        _container: Some(container),
    }
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn build_app(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("logs-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Build the same router WITHOUT `set_admin_auth_boundary(true)`, so the
/// per-route `require_admin` layer is LIVE.
///
/// `build_app` mirrors the sibling suites and sets the boundary, which means
/// "the embedder's own auth middleware already ran" and makes `require_admin`
/// a deliberate no-op — so it can never prove the gate. This builder is the
/// one that can.
fn build_app_without_admin_boundary(pool: &DbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("logs-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        ShardRouter::default(),
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    get_json_with_admin(app, uri, true).await
}

async fn get_json_with_admin(app: &HarvestApiApp, uri: &str, admin: bool) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if admin {
        builder = builder.header("x-harvest-admin", "true");
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .expect("GET request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Start a real execution row so the route's existence check passes.
///
/// The caller's `workflow_id` is suffixed with a fresh UUID so every call
/// yields a genuinely NEW execution. Without that, the default `AllowDuplicate`
/// reuse policy attaches to the prior run's row when the suite is re-run
/// against a persistent database (`HARVEST_TEST_DATABASE_URL`) — the execution
/// then already holds that test's log rows, so an assertion on how many rows a
/// write actually inserted reads 0 rather than the fresh count. CI's
/// per-test container hides this; a local re-run does not.
///
/// Returns the execution id the start RESOLVED to, not the minted one.
async fn seed_execution(conn: &mut AsyncPgConnection, workflow_id: &str) -> ExecutionId {
    let workflow_id = &format!("{workflow_id}-{}", uuid::Uuid::new_v4());
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let started = start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: "logged",
            workflow_id,
            exec_id,
            input: json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
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
        },
        None,
    )
    .await
    .expect("seed workflow");
    started.exec_id
}

fn line(seq: i64, level: &'static str, message: &str) -> store::WorkflowLogLine {
    store::WorkflowLogLine {
        seq,
        level,
        message: message.to_string(),
    }
}

/// Read `lines[].message` from a response body.
fn messages(body: &Value) -> Vec<String> {
    body["lines"]
        .as_array()
        .expect("lines must be an array")
        .iter()
        .map(|l| l["message"].as_str().expect("message").to_string())
        .collect()
}

/// The headline success metric (AC3 + the issue's success metric): an operator
/// reads **100%** of a run's author-emitted lines from ONE call, in emission
/// order, with zero external log-aggregation correlation.
#[tokio::test]
async fn single_call_returns_every_line_in_emission_order() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-all").await;
    let lines: Vec<_> = (0..25)
        .map(|i| line(i, "info", &format!("step {i}")))
        .collect();
    store::append_workflow_logs(&mut conn, exec_id, &lines, 1_000)
        .await
        .expect("append");

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["execution_id"], exec_id.to_string());
    assert_eq!(body["total_lines"], 25);
    assert_eq!(body["truncated"], false);
    assert_eq!(
        body["next_cursor"],
        Value::Null,
        "25 < the 200 default page"
    );

    // 100% of the lines, in emission order.
    let expected: Vec<String> = (0..25).map(|i| format!("step {i}")).collect();
    assert_eq!(messages(&body), expected);

    // Every element carries the documented shape.
    let first = &body["lines"][0];
    assert_eq!(first["seq"], 0);
    assert_eq!(first["level"], "info");
    assert!(first["occurred_at"].is_string());
    assert!(
        first.get("truncation_marker").is_none(),
        "an ordinary line must omit truncation_marker entirely"
    );
}

#[tokio::test]
async fn level_filter_narrows_and_an_unknown_level_is_a_400() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-levels").await;
    store::append_workflow_logs(
        &mut conn,
        exec_id,
        &[
            line(0, "info", "a"),
            line(1, "warn", "b"),
            line(2, "error", "c"),
            line(3, "error", "d"),
        ],
        1_000,
    )
    .await
    .expect("append");

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs?level=error")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body), vec!["c", "d"]);
    assert_eq!(
        body["total_lines"], 4,
        "total_lines is unaffected by the level filter"
    );

    // Repeatable AND comma-separated forms are both accepted.
    let (_, repeated) = get_json(
        &app,
        &format!("/workflows/{exec_id}/logs?level=warn&level=error"),
    )
    .await;
    assert_eq!(messages(&repeated), vec!["b", "c", "d"]);
    let (_, csv) = get_json(&app, &format!("/workflows/{exec_id}/logs?level=warn,error")).await;
    assert_eq!(messages(&csv), vec!["b", "c", "d"]);

    // A typo must be REJECTED, never silently answered with "no lines" — the
    // worst possible outcome for a triage endpoint.
    let (status, _) = get_json(&app, &format!("/workflows/{exec_id}/logs?level=warning")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cursor_pagination_is_exclusive_and_terminates() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-pages").await;
    let lines: Vec<_> = (0..10).map(|i| line(i, "info", &format!("m{i}"))).collect();
    store::append_workflow_logs(&mut conn, exec_id, &lines, 1_000)
        .await
        .expect("append");

    let (_, page1) = get_json(&app, &format!("/workflows/{exec_id}/logs?limit=4")).await;
    assert_eq!(messages(&page1), vec!["m0", "m1", "m2", "m3"]);
    let cursor = page1["next_cursor"].as_i64().expect("next_cursor");
    assert_eq!(cursor, 3);

    let (_, page2) = get_json(
        &app,
        &format!("/workflows/{exec_id}/logs?limit=4&cursor={cursor}"),
    )
    .await;
    assert_eq!(
        messages(&page2),
        vec!["m4", "m5", "m6", "m7"],
        "the cursor is EXCLUSIVE: page 2 starts strictly after page 1"
    );

    // Final short page terminates with a null cursor.
    let (_, page3) = get_json(&app, &format!("/workflows/{exec_id}/logs?limit=4&cursor=7")).await;
    assert_eq!(messages(&page3), vec!["m8", "m9"]);
    assert_eq!(page3["next_cursor"], Value::Null);

    // `after` is accepted as an alias for `cursor`.
    let (_, alias) = get_json(&app, &format!("/workflows/{exec_id}/logs?limit=4&after=3")).await;
    assert_eq!(messages(&alias), vec!["m4", "m5", "m6", "m7"]);

    // Malformed limit/cursor are 400s.
    for bad in ["?limit=0", "?limit=abc", "?cursor=-1", "?cursor=xyz"] {
        let (status, _) = get_json(&app, &format!("/workflows/{exec_id}/logs{bad}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad} must be rejected");
    }
}

#[tokio::test]
async fn since_bounds_by_occurred_at_and_a_bad_value_is_a_400() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-since").await;
    store::append_workflow_logs(&mut conn, exec_id, &[line(0, "info", "old")], 1_000)
        .await
        .expect("append old");
    // `occurred_at` defaults to NOW(); take the boundary from the stored row so
    // the assertion never depends on the test host's clock.
    let boundary = store::load_workflow_logs(
        &mut conn,
        exec_id,
        &store::WorkflowLogQuery {
            limit: 1,
            ..Default::default()
        },
    )
    .await
    .expect("load")[0]
        .occurred_at;
    store::append_workflow_logs(&mut conn, exec_id, &[line(1, "info", "new")], 1_000)
        .await
        .expect("append new");

    let since = boundary.to_rfc3339();
    let (status, body) = get_json(
        &app,
        &format!(
            "/workflows/{exec_id}/logs?since={}",
            urlencoding_min(&since)
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        messages(&body),
        vec!["new"],
        "since is an EXCLUSIVE lower bound on occurred_at"
    );

    let (status, _) = get_json(&app, &format!("/workflows/{exec_id}/logs?since=yesterday")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Minimal query-value escaping for the `+` in an RFC 3339 offset.
fn urlencoding_min(value: &str) -> String {
    value.replace('+', "%2B")
}

/// AC6/AC7: a run that logged nothing — including every run on a deployment
/// with the durable sink DISABLED — is a 200 with an empty list, never a 404
/// and never an error. Nothing is wrong with such a run.
#[tokio::test]
async fn a_run_that_never_logged_returns_an_empty_list_not_an_error() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-silent").await;
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["lines"].as_array().expect("lines").len(), 0);
    assert_eq!(body["total_lines"], 0);
    assert_eq!(body["truncated"], false);
    assert_eq!(body["next_cursor"], Value::Null);
}

#[tokio::test]
async fn unknown_and_malformed_ids_are_404_and_400() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);

    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _) = get_json(&app, &format!("/workflows/{unknown}/logs")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = get_json(&app, "/workflows/not-a-uuid/logs").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// AC4: the per-execution cap surfaces as a visible marker AND a
/// filter-independent `truncated` flag — never silent loss.
#[tokio::test]
async fn cap_marker_surfaces_truncated_independently_of_the_level_filter() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-capped").await;
    let lines: Vec<_> = (0..5).map(|i| line(i, "error", &format!("e{i}"))).collect();
    // Cap of 3 → lines 0,1,2 admitted, 3,4 dropped, one marker appended.
    store::append_workflow_logs(&mut conn, exec_id, &lines, 3)
        .await
        .expect("append");

    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], true);
    let msgs = messages(&body);
    assert_eq!(
        msgs.len(),
        4,
        "3 admitted lines plus exactly one truncation marker"
    );
    assert_eq!(&msgs[..3], &["e0", "e1", "e2"]);
    let marker = body["lines"].as_array().unwrap().last().unwrap();
    assert_eq!(
        marker["truncation_marker"], true,
        "the marker must be self-describing, not inferred from its text"
    );
    assert!(
        marker["message"].as_str().unwrap().contains("cap reached"),
        "the marker message must say what happened: {marker}"
    );

    // The flag is filter-independent: a filter that hides the marker row must
    // still report the truncation.
    let (_, filtered) = get_json(&app, &format!("/workflows/{exec_id}/logs?level=error")).await;
    assert_eq!(
        filtered["truncated"], true,
        "truncated must not depend on whether this page happens to include the marker"
    );
    assert!(
        filtered["lines"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l.get("truncation_marker").is_none()),
        "the marker is a warn row and must be hidden by ?level=error"
    );
}

/// AC3: admin/operator auth parity with the other per-execution diagnostic
/// reads.
///
/// `security.rs::eris_unauthenticated_workflow_logs_is_blocked` pins the 401 on
/// an app with no storage at all; this test adds what that one cannot — the
/// execution genuinely EXISTS and genuinely HAS log lines, so the 401 is proven
/// to be authorization rather than a 404 (or a missing route) in disguise.
#[tokio::test]
async fn route_is_admin_gated_even_for_an_existing_execution_with_lines() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-authz").await;
    store::append_workflow_logs(&mut conn, exec_id, &[line(0, "info", "secret")], 1_000)
        .await
        .expect("append");

    // Gate LIVE (no embedder auth boundary), no session → rejected.
    let gated = build_app_without_admin_boundary(&pool);
    let (status, body) = get_json(&gated, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        !body.to_string().contains("secret"),
        "a rejected read must not leak the line content: {body}"
    );

    // The SAME execution through an authorized app returns the lines — so the
    // 401 above was the gate, not a broken route or a missing execution.
    let authorized = build_app(&pool);
    let (status, body) = get_json(&authorized, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(messages(&body), vec!["secret"]);
}

/// The documented default per-execution line cap (`WorkflowLogPolicy::default`).
const SCALE_TOTAL_LINES: i64 = 1_000;

/// The issue's **success metric**, at the **documented scale**: an operator
/// reads a full 1,000-line run — the default `max_lines` cap — with a p95 read
/// latency well under 200 ms, and every page boundary behaves as documented.
///
/// The other tests use 25-line fixtures, which cannot observe:
///
/// * the **200-row default page** boundary (they never fill one page, so
///   `next_cursor` is always null and pagination is never actually exercised at
///   the documented default);
/// * the **`MAX_WORKFLOW_LOG_PAGE` = 1000 clamp** (an operator asking for more
///   than the ceiling must be clamped DOWN, not rejected and not honoured);
/// * the **latency target** at the volume the cap actually permits.
///
/// Latency is asserted as a *generous* ceiling, not a benchmark: CI runners are
/// noisy and a flaky perf assertion is worse than none. It is here to catch an
/// order-of-magnitude regression (an accidental full-table scan, an N+1, a
/// dropped index), which is the failure this bound can honestly detect.
#[tokio::test]
async fn a_full_thousand_line_run_reads_within_the_documented_bounds() {
    let db = setup_database().await;
    let pool = build_pool(&db.url);
    let app = build_app(&pool);
    let mut conn = AsyncPgConnection::establish(&db.url).await.unwrap();

    let exec_id = seed_execution(&mut conn, "logs-scale").await;

    // Fill the documented default cap exactly.
    let lines: Vec<_> = (0..SCALE_TOTAL_LINES)
        .map(|i| {
            let level = match i % 3 {
                0 => "info",
                1 => "warn",
                _ => "error",
            };
            line(i, level, &format!("line {i}"))
        })
        .collect();
    let written = store::append_workflow_logs(&mut conn, exec_id, &lines, 1_000)
        .await
        .expect("append 1000");
    assert_eq!(written, 1_000, "the whole cap must be admitted");

    // ── The 200-row default page boundary ───────────────────────────────────
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["lines"].as_array().expect("lines").len(),
        200,
        "the default page is 200 rows"
    );
    assert_eq!(
        body["total_lines"], SCALE_TOTAL_LINES,
        "total_lines reports the WHOLE run, not the current page"
    );
    assert_eq!(
        body["truncated"], false,
        "filling the cap exactly must NOT set truncated -- nothing was dropped"
    );
    let cursor = body["next_cursor"]
        .as_i64()
        .expect("a full page must hand back a cursor");
    assert_eq!(cursor, 199, "the cursor is the last row's seq");
    assert_eq!(messages(&body)[0], "line 0");

    // ── The MAX_WORKFLOW_LOG_PAGE = 1000 clamp ──────────────────────────────
    // Above the ceiling must clamp DOWN, not 400 and not honour 5000.
    let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs?limit=5000")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "an over-ceiling limit is clamped, not rejected"
    );
    assert_eq!(
        body["lines"].as_array().expect("lines").len(),
        1_000,
        "limit above MAX_WORKFLOW_LOG_PAGE clamps down to the 1000 ceiling"
    );
    assert_eq!(
        body["next_cursor"],
        Value::Null,
        "the clamped page covered every row, so there is no next page"
    );

    // ── The full read, in one call, in emission order ───────────────────────
    // Timed: 11 samples, assert the p95 (the 11th-highest-is-the-max here, so
    // take index 10 of the sorted list => the max, a strictly harder bar).
    let mut elapsed_ms: Vec<u128> = Vec::new();
    let mut last_body = Value::Null;
    for _ in 0..11 {
        let started = std::time::Instant::now();
        let (status, body) = get_json(&app, &format!("/workflows/{exec_id}/logs?limit=1000")).await;
        elapsed_ms.push(started.elapsed().as_millis());
        assert_eq!(status, StatusCode::OK);
        last_body = body;
    }
    elapsed_ms.sort_unstable();
    let p95 = elapsed_ms[10];

    // 100% of the run's lines, in emission order, from ONE call.
    let all = messages(&last_body);
    assert_eq!(all.len(), 1_000, "one call returns the whole run");
    assert_eq!(all[0], "line 0");
    assert_eq!(all[999], "line 999");
    assert!(
        all.windows(2).all(|w| {
            let a: i64 = w[0].trim_start_matches("line ").parse().unwrap();
            let b: i64 = w[1].trim_start_matches("line ").parse().unwrap();
            a < b
        }),
        "emission order must hold across the whole 1000-line read"
    );

    // Generous ceiling: the documented target is p95 < 200 ms. Assert 4x that so
    // a loaded CI runner does not flake, while an order-of-magnitude regression
    // (a lost index, a full scan) still fails loudly.
    assert!(
        p95 < 800,
        "reading 1000 lines must stay well inside the documented p95 < 200ms \
         target; got p95 = {p95}ms over samples {elapsed_ms:?} -- a value this \
         far out means an index or query-plan regression, not runner noise"
    );

    // ── Level filtering still works at scale, and paginates ─────────────────
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{exec_id}/logs?level=error&limit=1000"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let errors = messages(&body);
    assert_eq!(
        errors.len(),
        333,
        "every third line from index 2 is an error (2, 5, ... 998)"
    );
    assert_eq!(errors[0], "line 2");
}
