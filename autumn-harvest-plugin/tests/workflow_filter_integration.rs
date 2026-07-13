//! Integration tests for the new workflow-list filter knobs on
//! `GET /workflows` (issue #83). Seeds three workflows with distinct
//! `search_attrs` and asserts each filter combination returns exactly the
//! expected subset, including against a sharded deployment. Also verifies the
//! GIN index `idx_harvest_we_search` covers the search-attribute predicate via
//! `EXPLAIN`.

use std::collections::BTreeMap;

use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, Priority, ShardId};
use autumn_harvest::worker::DbPool;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::QueryableByName;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

/// When `HARVEST_TEST_PG_URL` is set (e.g. in CI without Docker Hub access),
/// we skip container startup and use an already-running Postgres instance.
/// Format: `postgres://user:pass@host:port/postgres` (admin database).
const LOCAL_PG_URL_ENV: &str = "HARVEST_TEST_PG_URL";

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

async fn setup_single_database() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(admin_url) = std::env::var(LOCAL_PG_URL_ENV) {
        // Use a fresh uniquely-named database on the already-running Postgres.
        let db_name = format!("harvest_test_{}", uuid::Uuid::new_v4().simple());
        let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
            .await
            .expect("failed to connect to local admin postgres");
        diesel::sql_query(format!("CREATE DATABASE {db_name}"))
            .execute(&mut admin_conn)
            .await
            .expect("failed to create test database");

        // Derive the database URL by replacing the database component.
        let database_url = {
            let base = admin_url.trim_end_matches('/');
            let without_db = base.rsplit_once('/').map_or(base, |(before, _)| before);
            format!("{without_db}/{db_name}")
        };

        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .expect("failed to connect to fresh test database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("failed to apply harvest migrations");

        return (database_url, None);
    }

    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get container port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, Some(container))
}

async fn setup_sharded_databases() -> ((String, String), Option<ContainerAsync<Postgres>>) {
    let (admin_url, container) = if let Ok(local_url) = std::env::var(LOCAL_PG_URL_ENV) {
        (local_url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container
            .get_host()
            .await
            .expect("failed to get container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to get container port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let shard0_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());
    let shard1_db = format!("harvest_shard_{}", uuid::Uuid::new_v4().simple());

    let mut admin_conn = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("failed to connect to admin database");
    diesel::sql_query(format!("CREATE DATABASE {shard0_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 0 database");
    diesel::sql_query(format!("CREATE DATABASE {shard1_db}"))
        .execute(&mut admin_conn)
        .await
        .expect("failed to create shard 1 database");

    // Derive the base URL (without database name) to construct shard URLs.
    let base_url = {
        let base = admin_url.trim_end_matches('/');
        let without_db = base.rsplit_once('/').map_or(base, |(before, _)| before);
        without_db.to_string()
    };
    let shard0_url = format!("{base_url}/{shard0_db}");
    let shard1_url = format!("{base_url}/{shard1_db}");

    for shard_url in [&shard0_url, &shard1_url] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(shard_url)
            .await
            .expect("failed to connect to shard database");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("failed to apply harvest migrations to shard database");
    }

    ((shard0_url, shard1_url), container)
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("failed to build test pool")
}

fn build_two_shard_pool(shard0_url: &str, shard1_url: &str) -> HarvestDbPool {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(shard0_url));
    pools.insert(ShardId::new(1), build_pool(shard1_url));
    HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0)))
}

async fn seed_workflow(
    database_url: &str,
    shard: ShardId,
    workflow_name: &str,
    workflow_id: &str,
    search_attrs: Option<Value>,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to seed workflow");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: json!({ "workflow_id": workflow_id }),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs,
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            trace_context: None,
            max_execution_timeout_ceiling: None,
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
        },
        None,
    )
    .await
    .expect("seed workflow should succeed");
    exec_id
}

async fn mark_state(database_url: &str, exec_id: ExecutionId, new_state: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to update state");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::state.eq(new_state.to_string()))
        .execute(&mut conn)
        .await
        .expect("state update should succeed");
}

async fn get_json(app: &HarvestApiApp, uri: impl Into<String>) -> (StatusCode, Value) {
    let uri = uri.into();
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .expect("GET request failed");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("failed to read response body");
    let json: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("response must be JSON")
    };
    (status, json)
}

fn workflow_ids(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("workflow list must be an array")
        .iter()
        .map(|row| {
            row["workflow_id"]
                .as_str()
                .expect("workflow_id must be a string")
                .to_string()
        })
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn workflow_list_filters_match_expected_subsets() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let onboarding_acme = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-onboarding-acme",
        Some(json!({ "tenant": "acme", "customer_id": "42" })),
    )
    .await;
    let onboarding_beta = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-onboarding-beta",
        Some(json!({ "tenant": "beta", "customer_id": "99" })),
    )
    .await;
    let billing_acme = seed_workflow(
        &database_url,
        ShardId::new(0),
        "billing",
        "wf-billing-acme",
        Some(json!({ "tenant": "acme", "customer_id": "42" })),
    )
    .await;

    // Mark one workflow as FAILED so we can exercise multi-state filters.
    mark_state(&database_url, billing_acme, "FAILED").await;

    // No filters -> all three rows visible (order is created_at desc).
    let (status, json) = get_json(&app, "/workflows").await;
    assert_eq!(status, StatusCode::OK);
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-billing-acme".to_string(),
            "wf-onboarding-acme".to_string(),
            "wf-onboarding-beta".to_string(),
        ]
    );

    // workflow_name only.
    let (_, json) = get_json(&app, "/workflows?workflow_name=onboarding").await;
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-onboarding-acme".to_string(),
            "wf-onboarding-beta".to_string(),
        ]
    );

    // search_attr (single key).
    let (_, json) = get_json(&app, "/workflows?search_attr=tenant:acme").await;
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-billing-acme".to_string(),
            "wf-onboarding-acme".to_string(),
        ]
    );

    // workflow_name + search_attr narrows to a single row.
    let (_, json) = get_json(
        &app,
        "/workflows?workflow_name=onboarding&search_attr=tenant:acme",
    )
    .await;
    assert_eq!(workflow_ids(&json), vec!["wf-onboarding-acme".to_string()]);

    // Multiple search_attr predicates AND together.
    let (_, json) = get_json(
        &app,
        "/workflows?search_attr=tenant:acme&search_attr=customer_id:42",
    )
    .await;
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-billing-acme".to_string(),
            "wf-onboarding-acme".to_string(),
        ]
    );

    // Repeating the same key with a different value narrows to zero rows.
    let (_, json) = get_json(
        &app,
        "/workflows?search_attr=tenant:acme&search_attr=tenant:beta",
    )
    .await;
    assert!(
        json.as_array().expect("array").is_empty(),
        "contradictory predicates should match nothing"
    );

    // state filter -- multiple values via comma-separated form.
    let (_, json) = get_json(&app, "/workflows?state=RUNNING,FAILED").await;
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-billing-acme".to_string(),
            "wf-onboarding-acme".to_string(),
            "wf-onboarding-beta".to_string(),
        ]
    );

    // state filter -- repeated key form is also accepted.
    let (_, json) = get_json(&app, "/workflows?state=FAILED&state=CANCELLED").await;
    assert_eq!(workflow_ids(&json), vec!["wf-billing-acme".to_string()]);

    // Combined: state + workflow_name + search_attr = 1 row.
    let (_, json) = get_json(
        &app,
        "/workflows?state=RUNNING&workflow_name=onboarding&search_attr=tenant:acme",
    )
    .await;
    assert_eq!(workflow_ids(&json), vec!["wf-onboarding-acme".to_string()]);

    // Reference the unused exec ids so dead-code lints stay quiet.
    let _ = (onboarding_acme, onboarding_beta);
}

#[tokio::test]
async fn workflow_list_invalid_filters_return_400() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let (status, body) = get_json(&app, "/workflows?state=NOT_A_STATE").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("unknown workflow state"),
        "expected helpful error, got {body}"
    );

    let (status, body) = get_json(&app, "/workflows?search_attr=tenant").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("invalid search_attr"),
        "expected helpful error, got {body}"
    );

    let (status, body) = get_json(&app, "/workflows?search_attr=:acme").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("missing a key"),
        "expected helpful error, got {body}"
    );

    let (status, _) = get_json(&app, "/workflows?limit=not-a-number").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn workflow_list_filters_apply_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let _ = seed_workflow(
        &shard0_url,
        ShardId::new(0),
        "onboarding",
        "wf-on-zero-acme",
        Some(json!({ "tenant": "acme" })),
    )
    .await;
    let _ = seed_workflow(
        &shard1_url,
        ShardId::new(1),
        "onboarding",
        "wf-on-one-acme",
        Some(json!({ "tenant": "acme" })),
    )
    .await;
    let _ = seed_workflow(
        &shard1_url,
        ShardId::new(1),
        "billing",
        "wf-on-one-beta",
        Some(json!({ "tenant": "beta" })),
    )
    .await;

    let (status, json) = get_json(
        &app,
        "/workflows?workflow_name=onboarding&search_attr=tenant:acme&limit=10",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let mut ids = workflow_ids(&json);
    ids.sort();
    assert_eq!(
        ids,
        vec!["wf-on-one-acme".to_string(), "wf-on-zero-acme".to_string()],
        "filter must apply to results from every shard"
    );

    // Shard merge respects the limit cap.
    let (_, json) = get_json(&app, "/workflows?search_attr=tenant:acme&limit=1").await;
    assert_eq!(
        json.as_array().map(Vec::len),
        Some(1),
        "limit must clamp the merged shard result set"
    );
}

#[derive(QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
    plan: String,
}

#[tokio::test]
async fn workflow_search_attr_predicate_uses_gin_index() {
    let (database_url, _container) = setup_single_database().await;

    let _onboarding = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-on-acme",
        Some(json!({ "tenant": "acme", "customer_id": "42" })),
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for EXPLAIN");
    // Force the planner to consider the GIN index even though the test table
    // is tiny; otherwise the seq scan will always win on cost and obscure the
    // index plan we want to assert on.
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .expect("disable seqscan should succeed");

    let plans: Vec<ExplainRow> = diesel::sql_query(
        "EXPLAIN SELECT id FROM harvest_workflow_executions \
         WHERE search_attrs @> '{\"tenant\":\"acme\"}'::jsonb",
    )
    .load(&mut conn)
    .await
    .expect("EXPLAIN should succeed");

    let plan_text = plans
        .into_iter()
        .map(|row| row.plan)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("idx_harvest_we_search"),
        "expected EXPLAIN plan to use idx_harvest_we_search, got:\n{plan_text}"
    );
}

#[tokio::test]
async fn workflow_list_filter_failure_cause() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let _nd_wf = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-failed-nd",
        Some(json!({
            "failure_cause": "non_determinism",
            "event_index": 3,
            "expected": "ActivityScheduled",
            "actual": "TimerStarted"
        })),
    )
    .await;

    let _other_wf = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-failed-other",
        Some(json!({ "tenant": "acme" })),
    )
    .await;

    // Filter by failure_cause=non_determinism.
    let (status, json) = get_json(&app, "/workflows?failure_cause=non_determinism").await;
    assert_eq!(status, StatusCode::OK);
    let ids = workflow_ids(&json);
    assert_eq!(ids, vec!["wf-failed-nd".to_string()]);
}

/// Issue #603: `?nd_blocked=true` returns only RUNNING executions currently
/// blocked on replay non-determinism. Terminal rows carrying a stale marker
/// (operator cancel/terminate escape hatches leave it in place) are excluded,
/// as are healthy RUNNING rows. The block columns surface in the row JSON.
#[tokio::test]
async fn workflow_list_filter_nd_blocked() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // A RUNNING execution blocked on divergence.
    let blocked = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-nd-blocked",
        None,
    )
    .await;
    // A healthy RUNNING execution.
    let _healthy = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-nd-healthy",
        None,
    )
    .await;
    // A CANCELLED execution with a stale marker (operator escape hatch does
    // not clear the marker; the state filter must hide it).
    let stale = seed_workflow(
        &database_url,
        ShardId::new(0),
        "onboarding",
        "wf-nd-stale",
        None,
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect to stamp nd-block columns");
    let now = chrono::Utc::now();
    diesel::update(harvest_workflow_executions::table.find(blocked.as_uuid()))
        .set((
            harvest_workflow_executions::nd_blocked_at.eq(Some(now)),
            harvest_workflow_executions::nd_block_reason.eq(Some(
                "non-deterministic replay: activity mismatch".to_string(),
            )),
            harvest_workflow_executions::nd_block_count.eq(2),
        ))
        .execute(&mut conn)
        .await
        .expect("stamp blocked row");
    diesel::update(harvest_workflow_executions::table.find(stale.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::nd_blocked_at.eq(Some(now)),
            harvest_workflow_executions::nd_block_reason
                .eq(Some("non-deterministic replay: timer mismatch".to_string())),
            harvest_workflow_executions::nd_block_count.eq(1),
        ))
        .execute(&mut conn)
        .await
        .expect("stamp stale row");

    let (status, json) = get_json(&app, "/workflows?nd_blocked=true").await;
    assert_eq!(status, StatusCode::OK);
    let ids = workflow_ids(&json);
    assert_eq!(
        ids,
        vec!["wf-nd-blocked".to_string()],
        "only the RUNNING blocked row matches; healthy and stale-terminal rows are excluded"
    );

    // The block columns surface on the row for operator triage (AC5).
    let row = &json.as_array().expect("array response")[0];
    assert!(row["nd_blocked_at"].is_string());
    assert_eq!(
        row["nd_block_reason"],
        "non-deterministic replay: activity mismatch"
    );
    assert_eq!(row["nd_block_count"], 2);

    // Unfiltered list still returns all three rows.
    let (status, json) = get_json(&app, "/workflows?limit=10").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().map(Vec::len), Some(3));
}

// ── Issue #498: cursor pagination + time-range filters ───────────────────────

/// Stamp `created_at` directly on an already-inserted execution so tests can
/// control insertion order without relying on `NOW()` timing.
async fn set_created_at(
    database_url: &str,
    exec_id: ExecutionId,
    ts: chrono::DateTime<chrono::Utc>,
) {
    use diesel::ExpressionMethods;
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect for set_created_at");
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set(harvest_workflow_executions::created_at.eq(ts))
        .execute(&mut conn)
        .await
        .expect("set_created_at should succeed");
}

/// Collect `workflow_id` strings from a paginated envelope
/// `{"workflows":[...], "next_cursor": ...}`.
fn page_ids(value: &Value) -> Vec<String> {
    value["workflows"]
        .as_array()
        .expect("paginated response must have a 'workflows' array")
        .iter()
        .map(|row| {
            row["workflow_id"]
                .as_str()
                .expect("workflow_id must be a string")
                .to_string()
        })
        .collect()
}

/// Extract the `next_cursor` from a paginated envelope (null → None).
fn next_cursor(value: &Value) -> Option<String> {
    value["next_cursor"].as_str().map(str::to_string)
}

#[tokio::test]
async fn workflow_list_started_after_before_filters_are_applied() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let old = seed_workflow(&database_url, ShardId::new(0), "billing", "wf-old", None).await;
    let new_wf = seed_workflow(&database_url, ShardId::new(0), "billing", "wf-new", None).await;

    // Back-date wf-old to 2025.
    let old_ts = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    set_created_at(&database_url, old, old_ts).await;

    // Also stamp started_at so the filter on started_at works.
    {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
            .await
            .unwrap();
        diesel::update(harvest_workflow_executions::table.find(old.as_uuid()))
            .set(harvest_workflow_executions::started_at.eq(old_ts))
            .execute(&mut conn)
            .await
            .unwrap();
    }

    // started_after=2026 should return only the recent workflow.
    let (status, json) = get_json(&app, "/workflows?started_after=2026-01-01T00:00:00Z").await;
    assert_eq!(status, StatusCode::OK);
    let ids = workflow_ids(&json);
    assert_eq!(ids, vec!["wf-new".to_string()]);

    // started_before=2026 should return only the old one.
    let (status, json) = get_json(&app, "/workflows?started_before=2026-01-01T00:00:00Z").await;
    assert_eq!(status, StatusCode::OK);
    let ids = workflow_ids(&json);
    assert_eq!(ids, vec!["wf-old".to_string()]);

    // Invalid RFC 3339 → 400.
    let (status, _) = get_json(&app, "/workflows?started_after=yesterday").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let _ = new_wf;
}

#[tokio::test]
async fn workflow_list_exec_id_prefix_filter_applied() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let exec_id = seed_workflow(
        &database_url,
        ShardId::new(0),
        "billing",
        "wf-prefix-test",
        None,
    )
    .await;

    let id_str = exec_id.to_string();
    let prefix = &id_str[..8];

    // Prefix matches → 1 result.
    let (status, json) = get_json(&app, &format!("/workflows?exec_id_prefix={prefix}")).await;
    assert_eq!(status, StatusCode::OK);
    let ids = workflow_ids(&json);
    assert_eq!(ids, vec!["wf-prefix-test".to_string()]);

    // Non-matching prefix → 0 results.
    let (status, json) = get_json(&app, "/workflows?exec_id_prefix=00000000").await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn workflow_list_legacy_call_returns_bare_array() {
    // Callers that pass only limit/state/search_attr must still receive a bare
    // JSON array — no envelope wrapping.
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let _ = seed_workflow(&database_url, ShardId::new(0), "billing", "wf-leg", None).await;

    // No pagination params → bare array.
    let (status, json) = get_json(&app, "/workflows?limit=10&state=RUNNING").await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.is_array(), "legacy call must return a bare JSON array");
}

#[tokio::test]
async fn workflow_list_pagination_returns_envelope() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // Seed 5 workflows with controlled created_at order.
    for i in 0..5u64 {
        let exec_id = seed_workflow(
            &database_url,
            ShardId::new(0),
            "billing",
            &format!("wf-p-{i:02}"),
            None,
        )
        .await;
        let ts =
            chrono::DateTime::from_timestamp(1_750_000_000 + i64::try_from(i).unwrap() * 10, 0)
                .unwrap();
        set_created_at(&database_url, exec_id, ts).await;
    }

    // Page 1 — page_size=2 desc.
    let (status, page1) = get_json(&app, "/workflows?page_size=2").await;
    assert_eq!(status, StatusCode::OK);
    let ids1 = page_ids(&page1);
    assert_eq!(ids1.len(), 2, "page 1 must have 2 rows");
    assert_eq!(ids1[0], "wf-p-04", "desc: newest row first");
    assert_eq!(ids1[1], "wf-p-03");

    let cursor1 = next_cursor(&page1).expect("page 1 must have next_cursor");

    // Page 2.
    let (status, page2) = get_json(&app, &format!("/workflows?page_size=2&cursor={cursor1}")).await;
    assert_eq!(status, StatusCode::OK);
    let ids2 = page_ids(&page2);
    assert_eq!(ids2[0], "wf-p-02");
    assert_eq!(ids2[1], "wf-p-01");

    let cursor2 = next_cursor(&page2).expect("page 2 must have next_cursor");

    // Page 3 — last page.
    let (status, page3) = get_json(&app, &format!("/workflows?page_size=2&cursor={cursor2}")).await;
    assert_eq!(status, StatusCode::OK);
    let ids3 = page_ids(&page3);
    assert_eq!(ids3, vec!["wf-p-00"]);
    assert!(
        next_cursor(&page3).is_none(),
        "last page must have null next_cursor"
    );

    // No duplicates or skips across all three pages.
    let mut all: Vec<_> = [ids1, ids2, ids3].concat();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 5, "all 5 rows must appear exactly once");
}

#[tokio::test]
async fn workflow_list_pagination_asc_order() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    for i in 0..4u64 {
        let exec_id = seed_workflow(
            &database_url,
            ShardId::new(0),
            "billing",
            &format!("wf-asc-{i:02}"),
            None,
        )
        .await;
        let ts =
            chrono::DateTime::from_timestamp(1_750_000_000 + i64::try_from(i).unwrap() * 10, 0)
                .unwrap();
        set_created_at(&database_url, exec_id, ts).await;
    }

    // Ascending: oldest first.
    let (status, page1) = get_json(&app, "/workflows?page_size=2&order=asc").await;
    assert_eq!(status, StatusCode::OK);
    let ids1 = page_ids(&page1);
    assert_eq!(ids1[0], "wf-asc-00", "asc: oldest first");
    assert_eq!(ids1[1], "wf-asc-01");

    let cursor1 = next_cursor(&page1).expect("page 1 must have next_cursor");

    let (status, page2) = get_json(
        &app,
        &format!("/workflows?page_size=2&order=asc&cursor={cursor1}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let ids2 = page_ids(&page2);
    assert_eq!(ids2[0], "wf-asc-02");
    assert_eq!(ids2[1], "wf-asc-03");
    assert!(
        next_cursor(&page2).is_none(),
        "last page next_cursor must be null"
    );
}

#[tokio::test]
async fn workflow_list_pagination_keyset_stability_under_concurrent_inserts() {
    // Fetch page 1, insert new rows with newer created_at, then fetch page 2
    // via cursor. The new rows must NOT appear on page 2 (they'd be on page 0
    // if re-fetched from scratch) and no rows must be duplicated or skipped.
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool.clone()));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // Seed 4 "old" workflows with ts < 2026.
    for i in 0..4u64 {
        let exec_id = seed_workflow(
            &database_url,
            ShardId::new(0),
            "billing",
            &format!("wf-stab-{i:02}"),
            None,
        )
        .await;
        let ts =
            chrono::DateTime::from_timestamp(1_700_000_000 + i64::try_from(i).unwrap() * 10, 0)
                .unwrap();
        set_created_at(&database_url, exec_id, ts).await;
    }

    // Page 1 (desc, page_size=2) → rows 3,2.
    let (status, page1) = get_json(&app, "/workflows?page_size=2").await;
    assert_eq!(status, StatusCode::OK);
    let ids1 = page_ids(&page1);
    assert_eq!(ids1.len(), 2);

    let cursor1 = next_cursor(&page1).expect("must have next_cursor");

    // Insert 2 "new" rows with future timestamps that would sort before the
    // cursor, i.e. they'd appear on page 1 if requested fresh.
    for i in 0..2u64 {
        let exec_id = seed_workflow(
            &database_url,
            ShardId::new(0),
            "billing",
            &format!("wf-new-{i}"),
            None,
        )
        .await;
        let ts =
            chrono::DateTime::from_timestamp(2_000_000_000 + i64::try_from(i).unwrap(), 0).unwrap();
        set_created_at(&database_url, exec_id, ts).await;
    }

    // Page 2 via cursor must return rows 1,0 — the new rows are after the
    // cursor boundary and must NOT appear here.
    let (status, page2) = get_json(&app, &format!("/workflows?page_size=2&cursor={cursor1}")).await;
    assert_eq!(status, StatusCode::OK);
    let ids2 = page_ids(&page2);
    assert_eq!(ids2.len(), 2);

    // No duplicates between page 1 and page 2.
    for id in &ids1 {
        assert!(!ids2.contains(id), "row {id} duplicated across pages");
    }
    // New rows must not appear on page 2.
    assert!(!ids2.contains(&"wf-new-0".to_string()));
    assert!(!ids2.contains(&"wf-new-1".to_string()));
}

#[tokio::test]
async fn workflow_list_pagination_sharded_global_order() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // Shard 0: 3 workflows, timestamps t+0, t+20, t+40.
    // Shard 1: 3 workflows, timestamps t+10, t+30, t+50.
    // Expected desc order: s1-2(t+50), s0-2(t+40), s1-1(t+30), s0-1(t+20), s1-0(t+10), s0-0(t+0).
    let base_ts: i64 = 1_750_000_000;
    for i in 0..3u64 {
        let exec_id = seed_workflow(
            &shard0_url,
            ShardId::new(0),
            "billing",
            &format!("wf-s0-{i}"),
            None,
        )
        .await;
        set_created_at(
            &shard0_url,
            exec_id,
            chrono::DateTime::from_timestamp(base_ts + i64::try_from(i).unwrap() * 20, 0).unwrap(),
        )
        .await;
    }
    for i in 0..3u64 {
        let exec_id = seed_workflow(
            &shard1_url,
            ShardId::new(1),
            "billing",
            &format!("wf-s1-{i}"),
            None,
        )
        .await;
        set_created_at(
            &shard1_url,
            exec_id,
            chrono::DateTime::from_timestamp(base_ts + 10 + i64::try_from(i).unwrap() * 20, 0)
                .unwrap(),
        )
        .await;
    }

    // Page through all 6 with page_size=2.
    let mut all_ids = Vec::new();
    let mut url = "/workflows?page_size=2".to_string();
    loop {
        let (status, page) = get_json(&app, &url).await;
        assert_eq!(status, StatusCode::OK);
        let ids = page_ids(&page);
        assert!(!ids.is_empty(), "page must be non-empty");
        all_ids.extend(ids);
        match next_cursor(&page) {
            Some(c) => url = format!("/workflows?page_size=2&cursor={c}"),
            None => break,
        }
    }

    // All 6 rows present, no duplicates.
    assert_eq!(all_ids.len(), 6, "all 6 rows must appear across pages");
    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 6, "no row must appear twice");

    // Global order is newest-first (desc).
    assert_eq!(all_ids[0], "wf-s1-2");
    assert_eq!(all_ids[1], "wf-s0-2");
    assert_eq!(all_ids[2], "wf-s1-1");
}

// ─── Issue #506: typed comparison & set predicates ──────────────────────────

/// Extract `workflow_id`s from either a bare array or a `{ workflows: [...] }`
/// pagination envelope (issue #498), so predicate tests can run with or without
/// pagination params.
fn workflow_ids_any(value: &Value) -> Vec<String> {
    let array = value
        .as_array()
        .or_else(|| value.get("workflows").and_then(Value::as_array))
        .expect("expected a workflow array or a paginated envelope");
    array
        .iter()
        .map(|row| {
            row["workflow_id"]
                .as_str()
                .expect("workflow_id must be a string")
                .to_string()
        })
        .collect()
}

/// Seed a `{ amount, phase, retry_count }` fixture covering numeric and string
/// attributes, then prove each predicate isolates exactly the right subset.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn workflow_search_attr_predicate_comparison_and_set() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // amount/phase/retry_count fixtures. `small-amount` deliberately probes the
    // string-vs-number coercion regression: amount=100 must sort ABOVE amount=20
    // numerically, not lexically ("100" < "20" as strings).
    let _big = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-big",
        Some(json!({ "amount": 15000, "phase": "blocked", "retry_count": 5 })),
    )
    .await;
    let _mid = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-mid",
        Some(json!({ "amount": 100, "phase": "awaiting_approval", "retry_count": 3 })),
    )
    .await;
    let _small = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-small",
        Some(json!({ "amount": 20, "phase": "running", "retry_count": 0 })),
    )
    .await;
    // A row whose amount is stored as a STRING — comparison ops must exclude it
    // (typed numeric comparison only matches number-typed stored values).
    let _stramount = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-str-amount",
        Some(json!({ "amount": "99999", "phase": "blocked", "retry_count": 9 })),
    )
    .await;

    // amount > 10000 → only the 15000 row (string "99999" excluded).
    let (status, body) = get_json(&app, "/workflows?search_attr_filter=amount:gt:10000").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workflow_ids_any(&body), vec!["wf-big".to_string()]);

    // Numeric regression: amount > 20 → {15000, 100} but NOT 20 and NOT the
    // string "99999". If this were lexical, "100" > "20" would be false.
    let (_, body) = get_json(&app, "/workflows?search_attr_filter=amount:gt:20").await;
    let mut ids = workflow_ids_any(&body);
    ids.sort();
    assert_eq!(ids, vec!["wf-big".to_string(), "wf-mid".to_string()]);

    // phase IN (blocked, awaiting_approval) → union (string set).
    let (_, body) = get_json(
        &app,
        "/workflows?search_attr_filter=phase:in:blocked,awaiting_approval",
    )
    .await;
    let mut ids = workflow_ids_any(&body);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-big".to_string(),
            "wf-mid".to_string(),
            "wf-str-amount".to_string(),
        ]
    );

    // retry_count >= 3 AND phase = blocked → intersection. Only wf-big
    // (retry 5, blocked) and wf-str-amount (retry 9, blocked) qualify.
    let (_, body) = get_json(
        &app,
        "/workflows?search_attr_filter=retry_count:gte:3&search_attr_filter=phase:eq:blocked",
    )
    .await;
    let mut ids = workflow_ids_any(&body);
    ids.sort();
    assert_eq!(ids, vec!["wf-big".to_string(), "wf-str-amount".to_string()]);

    // phase exists → every row has a phase key (all 4).
    let (_, body) = get_json(&app, "/workflows?search_attr_filter=phase:exists").await;
    assert_eq!(workflow_ids_any(&body).len(), 4);

    // amount < 100 → only the 20 row (numeric; string "99999" excluded).
    let (_, body) = get_json(&app, "/workflows?search_attr_filter=amount:lt:100").await;
    assert_eq!(workflow_ids_any(&body), vec!["wf-small".to_string()]);

    // phase != blocked → key present and not "blocked": wf-mid + wf-small.
    let (_, body) = get_json(&app, "/workflows?search_attr_filter=phase:ne:blocked").await;
    let mut ids = workflow_ids_any(&body);
    ids.sort();
    assert_eq!(ids, vec!["wf-mid".to_string(), "wf-small".to_string()]);
}

/// Malformed predicates return `400` naming the offending param; nested `.`-path
/// keys are rejected.
#[tokio::test]
async fn workflow_search_attr_predicate_invalid_returns_400() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // Unknown operator.
    let (status, body) = get_json(&app, "/workflows?search_attr_filter=amount:between:1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("between"),
        "error should name the bad op, got {body}"
    );

    // Non-numeric value for a comparison op.
    let (status, body) = get_json(&app, "/workflows?search_attr_filter=amount:gt:lots").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("numeric"),
        "error should explain numeric requirement, got {body}"
    );

    // Nested path key.
    let (status, body) = get_json(&app, "/workflows?search_attr_filter=a.b:eq:1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("nested") || body.to_string().contains("top-level"),
        "error should reject nested paths, got {body}"
    );

    // Missing value for eq.
    let (status, _) = get_json(&app, "/workflows?search_attr_filter=amount:eq").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A comparison predicate narrows on the `?` key-existence operator, which hits
/// the existing `idx_harvest_we_search` GIN index — proving the index path
/// rather than a full per-row scan (issue #506 AC).
#[tokio::test]
async fn workflow_search_attr_predicate_comparison_uses_gin_index() {
    let (database_url, _container) = setup_single_database().await;

    let _seed = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-explain",
        Some(json!({ "amount": 15000 })),
    )
    .await;

    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(&database_url)
        .await
        .expect("failed to connect for EXPLAIN");
    conn.batch_execute("SET enable_seqscan = off")
        .await
        .expect("disable seqscan should succeed");

    // Mirrors the SQL the Cmp predicate builds: existence narrows on GIN, then a
    // numeric recheck.
    let plans: Vec<ExplainRow> = diesel::sql_query(
        "EXPLAIN SELECT id FROM harvest_workflow_executions \
         WHERE search_attrs ? 'amount' \
         AND jsonb_typeof(search_attrs -> 'amount') = 'number' \
         AND (search_attrs ->> 'amount')::numeric > 10000",
    )
    .load(&mut conn)
    .await
    .expect("EXPLAIN should succeed");

    let plan_text = plans
        .into_iter()
        .map(|row| row.plan)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan_text.contains("idx_harvest_we_search"),
        "expected EXPLAIN plan to use idx_harvest_we_search, got:\n{plan_text}"
    );
}

/// Predicates push down per shard and the k-way merge composes with keyset
/// pagination — no row duplicated or skipped (issue #506 cross-shard AC).
#[tokio::test]
async fn workflow_search_attr_predicate_applies_across_shards_with_pagination() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(build_two_shard_pool(&shard0_url, &shard1_url));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    // Three matching rows spread across two shards, plus one non-matching row.
    for (url, shard, id, amount) in [
        (&shard0_url, ShardId::new(0), "wf-s0-a", 50000),
        (&shard0_url, ShardId::new(0), "wf-s0-b", 30000),
        (&shard1_url, ShardId::new(1), "wf-s1-a", 20000),
        (&shard1_url, ShardId::new(1), "wf-s1-low", 5),
    ] {
        let _ = seed_workflow(
            url,
            shard,
            "payment",
            id,
            Some(json!({ "amount": amount, "phase": "blocked" })),
        )
        .await;
    }

    // Page through amount > 10000 with page_size=2; expect exactly the 3 >10k
    // rows across pages, each once, never the amount=5 row.
    let mut all_ids: Vec<String> = Vec::new();
    let mut uri =
        "/workflows?search_attr_filter=amount:gt:10000&page_size=2&order=desc".to_string();
    loop {
        let (status, body) = get_json(&app, uri.clone()).await;
        assert_eq!(status, StatusCode::OK);
        all_ids.extend(workflow_ids_any(&body));
        match body.get("next_cursor").and_then(Value::as_str) {
            Some(cursor) => {
                uri = format!(
                    "/workflows?search_attr_filter=amount:gt:10000&page_size=2&order=desc&cursor={cursor}"
                );
            }
            None => break,
        }
    }

    let mut sorted = all_ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted,
        vec![
            "wf-s0-a".to_string(),
            "wf-s0-b".to_string(),
            "wf-s1-a".to_string()
        ],
        "predicate must select exactly the >10k rows across shards, no dup/skip"
    );
}

/// Backdate every event of an execution so it appears stalled (no event in the
/// last N minutes) to the `no_progress_minutes` scanner.
async fn backdate_events(database_url: &str, exec_id: ExecutionId, minutes: i64) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect to backdate events");
    diesel::sql_query(format!(
        "UPDATE harvest_events SET timestamp = NOW() - INTERVAL '{minutes} minutes' \
         WHERE workflow_exec_id = '{}'",
        exec_id.as_uuid()
    ))
    .execute(&mut conn)
    .await
    .expect("backdate update should succeed");
}

/// Regression for issue #506 review finding #1: `search_attr_filter` (and the
/// legacy `search_attr`) must narrow the stalled-workflow (`no_progress_minutes`)
/// path, not be silently dropped.
#[tokio::test]
async fn stalled_workflow_path_applies_search_attr_predicate() {
    let (database_url, _container) = setup_single_database().await;
    let pool = build_pool(&database_url);
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(HarvestDbPool::from(pool));
    let app = harvest_api_router(api_state).with_state(test_app_state_without_database());

    let blocked = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-stalled-blocked",
        Some(json!({ "phase": "blocked", "amount": 50000 })),
    )
    .await;
    let open = seed_workflow(
        &database_url,
        ShardId::new(0),
        "payment",
        "wf-stalled-open",
        Some(json!({ "phase": "open", "amount": 10 })),
    )
    .await;
    // Make both look stalled: no event progress for 10 minutes.
    backdate_events(&database_url, blocked, 10).await;
    backdate_events(&database_url, open, 10).await;

    // Typed predicate must narrow the stalled set to only the blocked row.
    let (status, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=5&search_attr_filter=phase:eq:blocked",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        workflow_ids(&body),
        vec!["wf-stalled-blocked".to_string()],
        "stalled path must honor search_attr_filter"
    );

    // Numeric comparison on the stalled path too.
    let (_, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=5&search_attr_filter=amount:gt:1000",
    )
    .await;
    assert_eq!(workflow_ids(&body), vec!["wf-stalled-blocked".to_string()]);

    // Legacy search_attr is also honored on the stalled path now.
    let (_, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=5&search_attr=phase:open",
    )
    .await;
    assert_eq!(workflow_ids(&body), vec!["wf-stalled-open".to_string()]);

    // Sanity: without a predicate, both stalled rows are returned.
    let (_, body) = get_json(&app, "/workflows?no_progress_minutes=5").await;
    let mut ids = workflow_ids(&body);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "wf-stalled-blocked".to_string(),
            "wf-stalled-open".to_string()
        ]
    );
}
