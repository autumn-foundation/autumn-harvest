//! Regression test for issue #1317 review. `signal-with-start` (and the
//! identical `update-with-start` scan) must place a genuinely fresh
//! post-reconciliation run on the business key's canonically-routed shard.
//! Never on whatever shard happens to hold a dead, terminal predecessor.
//!
//! `pool.iter_shards()`'s prescan can encounter a terminal
//! (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`) row for `(workflow_name,
//! workflow_id)` on a shard OTHER than the one
//! `ShardRouter::pick_for_new_workflow` would route a plain `start_workflow`
//! to. This happens whenever an execution's physical shard has drifted from
//! its hash-routed shard. The drift can come from a completed shard
//! rebalance (the migrated-seal-reconciliation feature, issue #1317), or
//! from a writable-subset change. `replace_execution` seals and inserts on
//! the SAME connection. So treating that stale row as the replace target
//! anchors the fresh run to the WRONG shard.
//!
//! A later plain `start_workflow` for the same key then routes to the
//! canonical shard and finds nothing occupying it. It inserts a SECOND live
//! run for the same business key -- the two-shard duplicate this test
//! proves cannot happen.
//!
//! Dual-mode like the sibling multi-shard suites: uses
//! `HARVEST_TEST_DATABASE_URL` when set, else boots two fresh testcontainers
//! Postgres-backed shard databases.

use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{StartWorkflowParams, WorkflowInfo, start_or_load_workflow_execution};
use autumn_harvest_plugin::HarvestDbPool;
use autumn_harvest_plugin::api::{
    HarvestApiRuntime, HarvestApiState, HarvestRetentionRuntime, harvest_api_router,
};
use autumn_web::AppState;
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use diesel::prelude::*;
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

type HarvestApiApp = axum::Router;

const SHARD_A: i32 = 0;
const SHARD_B: i32 = 1;

async fn setup_two_shard_databases() -> ((String, String), Option<ContainerAsync<Postgres>>) {
    let (admin_url, guard) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        (url, Some(container))
    };

    let names: Vec<String> = (0..2)
        .map(|_| format!("harvest_shard_{}", uuid::Uuid::new_v4().simple()))
        .collect();

    let mut admin = <AsyncPgConnection as AsyncConnection>::establish(&admin_url)
        .await
        .expect("admin connect");
    for db in &names {
        diesel::sql_query(format!("CREATE DATABASE {db}"))
            .execute(&mut admin)
            .await
            .expect("create db");
    }

    let base = admin_url.trim_end_matches('/');
    let (base, query) = match base.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (base, None),
    };
    let prefix = base.rsplit_once('/').map_or(base, |(prefix, _)| prefix);
    let shard_url = |dbname: &str| {
        query.map_or_else(
            || format!("{prefix}/{dbname}"),
            |q| format!("{prefix}/{dbname}?{q}"),
        )
    };
    let urls: Vec<String> = names.iter().map(|n| shard_url(n)).collect();

    for url in &urls {
        let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("shard connect");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate shard");
    }

    ((urls[0].clone(), urls[1].clone()), guard)
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("pool should build")
}

fn test_registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name: "onboarding",
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

/// Both shards readable and writable, so `pick_for_new_workflow` alone decides
/// which one is canonical for a given key -- no residency pin involved.
fn build_app(url_a: &str, url_b: &str) -> (HarvestApiApp, ShardRouter) {
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(SHARD_A), build_pool(url_a));
    pools.insert(ShardId::new(SHARD_B), build_pool(url_b));
    let storage = HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(SHARD_A)));

    let router = ShardRouter::new(
        vec![ShardId::new(SHARD_A), ShardId::new(SHARD_B)],
        vec![ShardId::new(SHARD_A), ShardId::new(SHARD_B)],
        ShardId::new(SHARD_A),
    );

    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(storage);
    api_state.install(HarvestApiRuntime::new(
        test_registry(),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("sws-rebalance-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router.clone(),
    ));
    (
        harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test")),
        router,
    )
}

async fn post_json(app: &HarvestApiApp, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .expect("POST request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("response is JSON")
    };
    (status, json)
}

/// Seed a COMPLETED row for `(workflow_name, workflow_id)` directly on
/// `shard`'s database. Stands in for a terminal predecessor left behind by a
/// completed shard rebalance (or a writable-subset drift). Skips replaying
/// the full migration/reconciliation state machine.
async fn seed_completed(database_url: &str, shard: ShardId, workflow_id: &str) -> ExecutionId {
    seed_with_state(database_url, shard, workflow_id, "COMPLETED").await
}

/// As [`seed_completed`], but for an arbitrary state -- e.g. `PAUSED`, to
/// stand in for a live-but-suspended run left off its hash-routed shard.
async fn seed_with_state(
    database_url: &str,
    shard: ShardId,
    workflow_id: &str,
    state: &str,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect to seed row");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "onboarding",
            workflow_id,
            exec_id,
            input: json!({}),
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
    .expect("seed row");

    let completed_at =
        matches!(state, "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT").then(chrono::Utc::now);
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .set((
            harvest_workflow_executions::state.eq(state),
            harvest_workflow_executions::completed_at.eq(completed_at),
        ))
        .execute(&mut conn)
        .await
        .expect("mark completed");
    exec_id
}

/// Count RUNNING rows for this business key on the given shard database.
async fn running_count(database_url: &str, workflow_id: &str) -> i64 {
    state_count(database_url, workflow_id, "RUNNING").await
}

/// Count rows in `state` for this business key on the given shard database.
async fn state_count(database_url: &str, workflow_id: &str, state: &str) -> i64 {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("connect to count");
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.eq(state))
        .count()
        .get_result(&mut conn)
        .await
        .expect("count")
}

/// A `workflow_id` whose `pick_for_new_workflow` canonical shard is `target`
/// -- brute forced, since the router's hash has no closed-form inverse.
fn workflow_id_routed_to(router: &ShardRouter, target: ShardId) -> String {
    (0..10_000)
        .map(|i| format!("routed-{i}"))
        .find(|id| router.pick_for_new_workflow("onboarding", id) == target)
        .expect("a matching workflow_id within 10,000 tries")
}

#[tokio::test]
async fn signal_with_start_finds_and_attaches_to_a_paused_run_off_its_routed_shard() {
    // Issue #1317 review. A live-but-PAUSED run found on a non-canonical
    // shard (a writable-subset artifact, independent of migration) must
    // never be treated as a dead row to skip past. `PAUSED` is the real
    // persisted name for a suspended run (`is_active_conflict_state`), not
    // `SUSPENDED`. Skipping it would abandon the paused row in place.
    // It would also create an unrelated fresh run on the canonical shard:
    // two rows for the same business key.
    //
    // A `PAUSED` prior under the default `AllowDuplicate` policy attaches
    // (the signal buffers for delivery on resume, like a direct
    // `send_signal` -- see `resolve_effective_signal_with_start_policy`,
    // issue #383). It does not seal-and-replace like a terminal prior does.
    let ((url_a, url_b), _guard) = setup_two_shard_databases().await;
    let (app, router) = build_app(&url_a, &url_b);

    let workflow_id = "paused-off-route-key";
    let canonical = router.pick_for_new_workflow("onboarding", workflow_id);
    let (canonical_url, other_url, other_shard) = if canonical == ShardId::new(SHARD_A) {
        (&url_a, &url_b, ShardId::new(SHARD_B))
    } else {
        (&url_b, &url_a, ShardId::new(SHARD_A))
    };

    let _paused = seed_with_state(other_url, other_shard, workflow_id, "PAUSED").await;

    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": workflow_id,
            "start_input": {},
            "signal_name": "wake",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["started_fresh"],
        json!(false),
        "the paused row must be found and attached to, not skipped past"
    );
    assert_eq!(body["signal_delivered"], json!(true));

    // The paused row is live: it must be found on ITS OWN shard, never
    // abandoned in favor of an unrelated fresh run elsewhere.
    assert_eq!(
        state_count(other_url, workflow_id, "PAUSED").await,
        1,
        "the paused row must still be the one and only row for this key"
    );
    assert_eq!(
        running_count(canonical_url, workflow_id).await,
        0,
        "no unrelated fresh run must appear on the canonical shard"
    );
}

#[tokio::test]
async fn signal_with_start_keeps_scanning_past_a_dead_canonical_row_for_a_live_run_elsewhere() {
    // Issue #1317 review. A dead, terminal row on the CANONICAL shard must
    // be kept only as a fallback, not treated as decisive the moment it is
    // seen. A live run for the same key can still exist on a later,
    // non-canonical shard (a writable-subset artifact). That is exactly the
    // reason this cross-shard scan exists at all. It must win.
    let ((url_a, url_b), _guard) = setup_two_shard_databases().await;
    let (app, router) = build_app(&url_a, &url_b);

    // Shards iterate in ascending id order. Pin the canonical shard to
    // SHARD_A (visited first) -- otherwise the dead row would never be
    // reached before the live one, regardless of this fix.
    let workflow_id = workflow_id_routed_to(&router, ShardId::new(SHARD_A));

    let _dead = seed_completed(&url_a, ShardId::new(SHARD_A), &workflow_id).await;
    let _live = seed_with_state(&url_b, ShardId::new(SHARD_B), &workflow_id, "RUNNING").await;

    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": workflow_id,
            "start_input": {},
            "signal_name": "wake",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(
        body["started_fresh"],
        json!(false),
        "the live run on shard B must be attached to, not shadowed by the \
         dead row the scan meets first on the canonical shard"
    );
    assert_eq!(
        running_count(&url_b, &workflow_id).await,
        1,
        "the original live run must be untouched"
    );
    assert_eq!(
        running_count(&url_a, &workflow_id).await,
        0,
        "the dead canonical row must not have spawned a second live run"
    );
}

#[tokio::test]
async fn signal_with_start_routes_a_fresh_run_past_a_stale_terminal_copy_to_the_canonical_shard() {
    let ((url_a, url_b), _guard) = setup_two_shard_databases().await;
    let (app, router) = build_app(&url_a, &url_b);

    let workflow_id = "cross-shard-key";
    let canonical = router.pick_for_new_workflow("onboarding", workflow_id);
    let (canonical_url, other_url) = if canonical == ShardId::new(SHARD_A) {
        (&url_a, &url_b)
    } else {
        (&url_b, &url_a)
    };
    let other_shard = if canonical == ShardId::new(SHARD_A) {
        ShardId::new(SHARD_B)
    } else {
        ShardId::new(SHARD_A)
    };

    // A dead, terminal predecessor sits on the NON-canonical shard, with
    // nothing at all on the canonical shard. That is the state left behind
    // once a migrated-and-reconciled seal is excluded from occupancy (issue
    // #1317) and its live copy has since itself completed.
    let _stale = seed_completed(other_url, other_shard, workflow_id).await;

    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/signal-with-start",
        json!({
            "workflow_id": workflow_id,
            "start_input": {},
            "signal_name": "wake",
            "signal_payload": {}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "body: {body}");
    assert_eq!(
        body["started_fresh"],
        json!(true),
        "no live prior anywhere, so this must be a fresh start"
    );

    // The fresh run must land on the CANONICAL shard -- the one a plain
    // `start_workflow` for this key would also use. Not on the shard that
    // merely happened to hold the dead predecessor.
    assert_eq!(
        running_count(canonical_url, workflow_id).await,
        1,
        "the fresh run must be created on the canonical shard"
    );
    assert_eq!(
        running_count(other_url, workflow_id).await,
        0,
        "the stale predecessor's shard must not gain a live run"
    );

    // End-to-end: a plain start for the same key must see the fresh run on
    // the canonical shard and refuse to create a second one. This proves the
    // two-shard duplicate this fix closes cannot happen.
    let (status, body) = post_json(
        &app,
        "/workflows/onboarding/start",
        json!({
            "workflow_id": workflow_id,
            "input": {},
            "reuse_policy": "reject_duplicate"
        }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "a second live run for the same key must be refused, not admitted \
         on the canonical shard: body {body}"
    );
    assert_eq!(
        running_count(canonical_url, workflow_id).await
            + running_count(other_url, workflow_id).await,
        1,
        "exactly one live run for this business key must exist, on either shard"
    );
}
