//! Integration tests for the `history_bloat_min_events` operator early-warning
//! discovery filter on `GET /workflows` (issue #704, AC1/AC2/AC7), plus a
//! regression proof (issue #704 / PR #1139 review, P1) that the pre-existing,
//! general-purpose `min_history_events` filter (issue #493) still composes
//! freely with `state=`/pagination — the exact composition an earlier
//! revision of this feature broke by reusing that query-parameter name for
//! the (functionally incompatible) dedicated discovery path below.
//!
//! Verifies, against a real Postgres-backed HTTP router, that
//! `history_bloat_min_events`:
//! - returns only live (non-terminal) executions whose current recorded
//!   history event count is `>= N`, sorted by history size descending (AC1);
//! - includes each row's current `history_event_count` so callers can
//!   rank/triage without a second call (AC2);
//! - rejects an invalid (non-numeric / negative) value with a `400` JSON
//!   error body, never a `500` or a silent empty match (AC7);
//! - rejects composition with `cursor`/`page_size`/`order` pagination with a `400`
//!   (the computed sort has no keyset cursor, so silently mis-ordering a
//!   paginated response would be worse than a clear rejection);
//!
//! and that `min_history_events` (issue #493, unchanged) is unaffected:
//! - composes with `state=` (including terminal states, which
//!   `history_bloat_min_events` always excludes);
//! - composes with `page_size`/`order` pagination (the paginated object
//!   response shape, not the `400` `history_bloat_min_events` produces for
//!   the same params).

use std::collections::BTreeMap;

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

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;

fn test_app_state() -> AppState {
    AppState::for_test().with_profile("test")
}

async fn setup_database() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (database_url, container)
}

/// Two genuinely separate Postgres databases (mirroring
/// `workflow_filter_integration.rs::setup_sharded_databases`), so the
/// cross-shard `history_bloat_min_events` fan-out (`build_history_bloat_fanout`) is
/// exercised end-to-end -- not just its pure in-memory merge/sort/truncate
/// unit tests.
async fn setup_sharded_databases() -> ((String, String), ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
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

    let base_url = format!("postgres://postgres:postgres@{host}:{port}");
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

fn build_app(database_url: &str) -> HarvestApiApp {
    let pool = build_pool(database_url);
    build_app_with_pool(HarvestDbPool::from(pool))
}

fn build_app_with_pool(pool: HarvestDbPool) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(pool);
    harvest_api_router(api_state).with_state(test_app_state())
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
        .expect("failed to read body");
    let json: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("response must be JSON")
    };
    (status, json)
}

/// Seed a RUNNING workflow execution (a single `WorkflowStarted` event) and
/// then pad its recorded history with `extra_events` additional raw rows so
/// its total `history_event_count` is `1 + extra_events`. The padding rows'
/// content is never deserialized/replayed by these HTTP-only tests -- only
/// counted -- so a minimal, structurally-arbitrary `MarkerRecorded` payload
/// is sufficient.
async fn seed_workflow_with_history_size(
    database_url: &str,
    shard: ShardId,
    workflow_id: &str,
    extra_events: i32,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: "history_bloat_filter_test",
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

    // `start_or_load_workflow_execution` appends exactly one `WorkflowStarted`
    // event at `event_id = 0`; pad with `extra_events` more, sequentially.
    for i in 0..extra_events {
        diesel::sql_query(
            "INSERT INTO harvest_events (workflow_exec_id, event_id, event_type, event_data, timestamp) \
             VALUES ($1, $2, 'MarkerRecorded', \
             '{\"type\":\"MarkerRecorded\",\"data\":{\"name\":\"padding\",\"details\":{}}}'::jsonb, NOW())",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .bind::<diesel::sql_types::Integer, _>(1 + i)
        .execute(&mut conn)
        .await
        .expect("insert padding event");
    }

    exec_id
}

/// Force an execution's persisted state directly (bypassing a real worker) --
/// used to prove a terminal execution is excluded from the discovery filter
/// (AC1: "live (non-terminal)") regardless of how large its history is.
async fn force_execution_state(database_url: &str, exec_id: ExecutionId, state: &str) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query("UPDATE harvest_workflow_executions SET state = $1 WHERE id = $2")
        .bind::<diesel::sql_types::Text, _>(state)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("force execution state");
}

fn workflow_ids_of(arr: &Value) -> Vec<String> {
    arr.as_array()
        .expect("response must be an array")
        .iter()
        .map(|r| r["workflow_id"].as_str().expect("workflow_id").to_string())
        .collect()
}

fn history_event_count_of(row: &Value) -> i64 {
    row["history_event_count"]
        .as_i64()
        .expect("history_event_count must be present and an integer (AC2)")
}

// ─── AC1 / AC2 ──────────────────────────────────────────────────────────────

/// AC1: only live (non-terminal) executions at or above the threshold are
/// returned, sorted by current history size descending. AC2: each row
/// carries its `history_event_count`.
#[tokio::test]
async fn history_bloat_min_events_returns_live_executions_sorted_by_size_descending() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // 1 (WorkflowStarted) + extra_events.
    let _small =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-small", 1).await; // total 2
    let _medium =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-medium", 4).await; // total 5
    let _large =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-large", 7).await; // total 8

    // A terminal execution whose history is larger than every live one above
    // must still be excluded -- AC1's "live (non-terminal)" restriction.
    let terminal =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-terminal-huge", 20)
            .await; // total 21
    force_execution_state(&database_url, terminal, "COMPLETED").await;

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=5").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"hb-medium".to_string()),
        "hb-medium (5 events) must be returned at the threshold; got {ids:?}"
    );
    assert!(
        ids.contains(&"hb-large".to_string()),
        "hb-large (8 events) must be returned above the threshold; got {ids:?}"
    );
    assert!(
        !ids.contains(&"hb-small".to_string()),
        "hb-small (2 events) must be excluded below the threshold; got {ids:?}"
    );
    assert!(
        !ids.contains(&"hb-terminal-huge".to_string()),
        "a terminal execution must never appear, regardless of history size; got {ids:?}"
    );

    // Sorted descending: hb-large (8) before hb-medium (5).
    let large_pos = ids.iter().position(|id| id == "hb-large").unwrap();
    let medium_pos = ids.iter().position(|id| id == "hb-medium").unwrap();
    assert!(
        large_pos < medium_pos,
        "results must be sorted by history size descending; got order {ids:?}"
    );

    // AC2: each returned row carries the exact current history_event_count.
    let rows = body.as_array().unwrap();
    let large_row = rows
        .iter()
        .find(|r| r["workflow_id"] == "hb-large")
        .expect("hb-large row");
    assert_eq!(history_event_count_of(large_row), 8);
    let medium_row = rows
        .iter()
        .find(|r| r["workflow_id"] == "hb-medium")
        .expect("hb-medium row");
    assert_eq!(history_event_count_of(medium_row), 5);
}

/// `history_bloat_min_events=0` is a well-defined boundary, not a "disabled"
/// sentinel: it accepts every live execution regardless of history size
/// (every recorded count is `>= 0`), and the value is `u64`-parseable so it
/// is never confused with AC7's rejection path.
#[tokio::test]
async fn history_bloat_min_events_zero_returns_every_live_execution() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Smallest possible live history: just the single `WorkflowStarted`
    // event (0 extra padding rows), total `history_event_count == 1`.
    let _tiny = seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-tiny", 0).await;

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=0").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"hb-tiny".to_string()),
        "history_bloat_min_events=0 must match even a minimal 1-event history; got {ids:?}"
    );

    let rows = body.as_array().unwrap();
    let tiny_row = rows
        .iter()
        .find(|r| r["workflow_id"] == "hb-tiny")
        .expect("hb-tiny row");
    assert_eq!(history_event_count_of(tiny_row), 1);
}

/// AC1's "live (non-terminal)" restriction applies regardless of state:
/// `history_bloat_min_events` excludes every terminal state unconditionally
/// -- unlike the general-purpose `min_history_events` filter (issue #493,
/// see the `min_history_events_composes_with_state_including_terminal` test
/// below), which has no such restriction and composes freely with an
/// explicit `state=` filter, including a terminal one.
#[tokio::test]
async fn history_bloat_min_events_excludes_every_terminal_state() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    for (label, state) in [
        ("hb-completed", "COMPLETED"),
        ("hb-failed", "FAILED"),
        ("hb-cancelled", "CANCELLED"),
        ("hb-timed-out", "TIMED_OUT"),
        ("hb-continued-as-new", "CONTINUED_AS_NEW"),
        ("hb-terminated", "TERMINATED"),
    ] {
        let exec_id =
            seed_workflow_with_history_size(&database_url, ShardId::new(0), label, 9).await;
        force_execution_state(&database_url, exec_id, state).await;
    }

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=1").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        ids.is_empty(),
        "every seeded execution is terminal; none should be returned, got {ids:?}"
    );
}

// ─── AC7 ────────────────────────────────────────────────────────────────────

/// AC7: a non-numeric `history_bloat_min_events` value returns `400` with a
/// JSON error body, never a `500` or a silent empty array.
#[tokio::test]
async fn history_bloat_min_events_non_numeric_value_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=not_a_number").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400; body={body}");
    assert!(
        body.is_object(),
        "a 400 must carry a JSON error object, not an array or empty body; got {body}"
    );
}

/// AC7: a negative `history_bloat_min_events` value returns `400`, not a
/// silently accepted (and meaningless, since counts can't be negative)
/// filter.
#[tokio::test]
async fn history_bloat_min_events_negative_value_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=-5").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400; body={body}");
    assert!(
        body.is_object(),
        "a 400 must carry a JSON error object; got {body}"
    );
}

// ─── AC1: pagination-combo rejection ───────────────────────────────────────

/// `history_bloat_min_events` sorts by a computed value with no keyset
/// cursor, so combining it with `cursor`/`page_size`/`order` pagination is
/// rejected with a clear `400` rather than silently mis-ordering (or
/// ignoring) the request.
#[tokio::test]
async fn history_bloat_min_events_combined_with_page_size_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=1&page_size=10").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400; body={body}");
    assert!(
        body.is_object(),
        "a 400 must carry a JSON error object; got {body}"
    );
}

#[tokio::test]
async fn history_bloat_min_events_combined_with_order_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=1&order=asc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400; body={body}");
}

// ─── AC1: cross-shard fan-out ───────────────────────────────────────────────

/// AC1 says "fanned out across shards, sorted by history size descending" --
/// exercised here against two *genuinely separate* Postgres databases (not
/// just `build_history_bloat_fanout`'s pure in-memory merge/sort/truncate
/// unit tests), proving the real HTTP path performs a global sort across
/// shards rather than concatenating each shard's already-sorted page (which
/// would silently misorder a large shard-1 row after a small shard-0 one).
#[tokio::test]
async fn history_bloat_min_events_merges_and_sorts_globally_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let app = build_app_with_pool(pool);

    // Shard 0: one above-threshold row (total 6), one below-threshold row.
    let _shard0_large =
        seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-shard0-large", 5).await; // total 6
    let _shard0_small =
        seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-shard0-small", 1).await; // total 2

    // Shard 1: one above-threshold row that is *larger* than shard 0's, so a
    // correct global sort must place it first -- proving the merge isn't
    // just "shard 0's page, then shard 1's page".
    let _shard1_larger = seed_workflow_with_history_size(
        &shard1_url,
        ShardId::new(1),
        "hb-shard1-larger",
        11, // total 12
    )
    .await;
    let _shard1_small =
        seed_workflow_with_history_size(&shard1_url, ShardId::new(1), "hb-shard1-small", 0).await; // total 1

    // A terminal row on shard 1, larger than everything else, must still be
    // excluded even though it is on the "other" shard from the live rows.
    let shard1_terminal = seed_workflow_with_history_size(
        &shard1_url,
        ShardId::new(1),
        "hb-shard1-terminal",
        30, // total 31
    )
    .await;
    force_execution_state(&shard1_url, shard1_terminal, "COMPLETED").await;

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=5").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"hb-shard0-large".to_string()),
        "shard 0's above-threshold row must be present; got {ids:?}"
    );
    assert!(
        ids.contains(&"hb-shard1-larger".to_string()),
        "shard 1's above-threshold row must be present; got {ids:?}"
    );
    assert!(
        !ids.contains(&"hb-shard0-small".to_string()),
        "shard 0's below-threshold row must be excluded; got {ids:?}"
    );
    assert!(
        !ids.contains(&"hb-shard1-small".to_string()),
        "shard 1's below-threshold row must be excluded; got {ids:?}"
    );
    assert!(
        !ids.contains(&"hb-shard1-terminal".to_string()),
        "a terminal row must never appear, regardless of shard or history size; got {ids:?}"
    );

    // Global descending sort: shard 1's larger row (12) must precede shard
    // 0's row (6) -- a per-shard-then-concatenate merge would get this
    // backwards (shard 0 first, since it's the default shard queried first).
    let shard1_pos = ids.iter().position(|id| id == "hb-shard1-larger").unwrap();
    let shard0_pos = ids.iter().position(|id| id == "hb-shard0-large").unwrap();
    assert!(
        shard1_pos < shard0_pos,
        "results must be sorted by history size descending across shards, \
         not concatenated per-shard; got order {ids:?}"
    );

    let rows = body.as_array().unwrap();
    let shard1_row = rows
        .iter()
        .find(|r| r["workflow_id"] == "hb-shard1-larger")
        .expect("hb-shard1-larger row");
    assert_eq!(history_event_count_of(shard1_row), 12);
    let shard0_row = rows
        .iter()
        .find(|r| r["workflow_id"] == "hb-shard0-large")
        .expect("hb-shard0-large row");
    assert_eq!(history_event_count_of(shard0_row), 6);
}

// ─── issue #493: `min_history_events` composition (PR #1139 review, P1) ────
//
// A prior revision of the `history_bloat_min_events` feature above (issue
// #704) reused the pre-existing `min_history_events` query-parameter name
// (issue #493) for its own, functionally incompatible dedicated-discovery
// trigger — silently breaking every caller who combined `min_history_events`
// with `state=` or pagination (a semantic merge conflict caught in PR #1139
// review, since git's line-level merge has no way to detect two independent
// features colliding on the same identifier). These tests pin the restored,
// composable behavior directly, so a future regression of this exact kind
// fails CI rather than shipping silently again.

/// `min_history_events` (issue #493) has no "live-only" restriction and
/// composes with an explicit `state=` filter -- including a *terminal*
/// state, which `history_bloat_min_events` above always excludes. Before the
/// P1 fix, `min_history_events` itself triggered the dedicated (terminal-
/// excluding) discovery path, so this exact query would have silently
/// returned an empty array instead of the completed row below.
#[tokio::test]
async fn min_history_events_composes_with_state_including_terminal() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Terminal, above the threshold: must be returned when explicitly
    // filtered to `state=COMPLETED`.
    let completed =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-completed-large", 9)
            .await; // total 10
    force_execution_state(&database_url, completed, "COMPLETED").await;

    // Live, but below the threshold: must be excluded by the count filter
    // regardless of state.
    let _running_small =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-running-small", 1)
            .await; // total 2

    // Live and above the threshold, but the wrong state: must be excluded by
    // the `state=` filter even though it satisfies `min_history_events`.
    let _running_large =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-running-large", 9)
            .await; // total 10

    let (status, body) = get_json(&app, "/workflows?state=COMPLETED&min_history_events=5").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    assert!(
        body.is_array(),
        "no pagination param was supplied, so this must stay the legacy \
         bare-array shape (never the {{workflows,...}} object); got {body}"
    );

    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"mhe-completed-large".to_string()),
        "a COMPLETED execution at/above the threshold must be returned when \
         explicitly requested via state=COMPLETED -- this is the exact \
         composition the P1 param-collision regression broke; got {ids:?}"
    );
    assert!(
        !ids.contains(&"mhe-running-small".to_string()),
        "a RUNNING (non-matching state) execution must be excluded by \
         state=COMPLETED regardless of history size; got {ids:?}"
    );
    assert!(
        !ids.contains(&"mhe-running-large".to_string()),
        "a RUNNING execution above the threshold must still be excluded by \
         state=COMPLETED; got {ids:?}"
    );
}

/// `min_history_events` composes with `page_size` pagination -- unlike
/// `history_bloat_min_events` above, which rejects that combination with a
/// `400` because its sort is by a computed, cursor-less value.
/// `min_history_events` is a plain filter with no such restriction, so
/// pagination proceeds normally and returns the paginated object shape.
#[tokio::test]
async fn min_history_events_composes_with_page_size_pagination() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let _large =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-paginated-large", 9)
            .await; // total 10

    let (status, body) = get_json(&app, "/workflows?min_history_events=5&page_size=10").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "min_history_events + page_size must NOT be rejected (unlike \
         history_bloat_min_events + pagination); body={body}"
    );
    assert!(
        body.is_object() && body.get("workflows").is_some(),
        "a pagination param was supplied, so the response must use the \
         paginated {{workflows, next_cursor, ...}} object shape; got {body}"
    );
    let ids = workflow_ids_of(&body["workflows"]);
    assert!(
        ids.contains(&"mhe-paginated-large".to_string()),
        "the above-threshold row must still be present under pagination; got {ids:?}"
    );
}

/// Same composition proof as above, for the `order` pagination param.
#[tokio::test]
async fn min_history_events_composes_with_order_pagination() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let _large =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-order-large", 9).await; // total 10

    let (status, body) = get_json(&app, "/workflows?min_history_events=5&order=asc").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "min_history_events + order must NOT be rejected; body={body}"
    );
    assert!(
        body.is_object() && body.get("workflows").is_some(),
        "an `order` param was supplied, so the response must use the \
         paginated object shape; got {body}"
    );
}
