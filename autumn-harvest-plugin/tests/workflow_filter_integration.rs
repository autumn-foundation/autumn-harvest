//! Integration tests for the new workflow-list filter knobs on
//! `GET /workflows` (issue #83). Seeds three workflows with distinct
//! `search_attrs` and asserts each filter combination returns exactly the
//! expected subset, including against a sharded deployment. Also verifies the
//! GIN index `idx_harvest_we_search` covers the search-attribute predicate via
//! `EXPLAIN`.

use std::collections::BTreeMap;

use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::ShardedDbPool;
use autumn_harvest::types::{ExecutionId, ShardId};
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
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

const INIT_SQL: &str = concat!(
    include_str!("../../autumn-harvest/migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../../autumn-harvest/migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!(
        "../../autumn-harvest/migrations/20260518000000_harvest_workflow_execution_timeout/up.sql"
    ),
);

type HarvestApiApp = axum::Router;

fn test_app_state_without_database() -> AppState {
    AppState::for_test().with_profile("test")
}

async fn setup_single_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
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

    (database_url, container)
}

async fn setup_sharded_databases() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
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
    let admin_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
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

    let shard0_url = format!("postgres://postgres:postgres@{host}:{port}/{shard0_db}");
    let shard1_url = format!("postgres://postgres:postgres@{host}:{port}/{shard1_db}");

    for shard_url in [&shard0_url, &shard1_url] {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(shard_url)
            .await
            .expect("failed to connect to shard database");
        conn.batch_execute(INIT_SQL)
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
        },
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
