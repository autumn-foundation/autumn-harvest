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
//!
//! Further PR #1139 review findings, each with a dedicated regression test at
//! the end of this file:
//! - finding #3: `failure_cause` (issue #506) must narrow the
//!   `history_bloat_min_events` results, not be silently ignored;
//! - finding #4: `no_progress_minutes` (issue #486, stalled-workflow
//!   discovery) and `history_bloat_min_events` are mutually-exclusive
//!   discovery paths and must be rejected with `400` when combined, rather
//!   than letting one silently win;
//! - finding B (second review round): `min_history_events` (issue #493) must
//!   still compose with `history_bloat_min_events` when both are supplied at
//!   once -- `AND`ing the two `>=` thresholds together (equivalent to the
//!   stricter/maximum of the two), not silently dropping one;
//! - finding C (second review round): the per-shard candidate query is
//!   bounded to `filters.limit`, ordered by history size descending, *before*
//!   any full execution row is loaded -- proven correct even when a single
//!   shard alone has more matching candidates than the global limit;
//! - finding b (third review round): `history_event_count` must stay ABSENT
//!   on the legacy `no_progress_minutes` (stalled-workflow) discovery path
//!   even when the composed `min_history_events` filter is ALSO satisfied --
//!   the field is exclusive to the dedicated `history_bloat_min_events` path.

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

/// Backdate every recorded event for an execution by `hours_ago` hours --
/// mirrors `stalled_workflow_tests.rs::seed_stalled_workflow`'s backdating
/// step, factored out here so it composes with `seed_workflow_with_history_size`
/// (which seeds a specific *event count*, not a specific *staleness*).  Used
/// to prove a row can satisfy BOTH `no_progress_minutes` (staleness) and
/// `min_history_events` (count) at once -- PR #1139 review, round 3, finding
/// b.
async fn backdate_all_events(database_url: &str, exec_id: ExecutionId, hours_ago: i64) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query(
        "UPDATE harvest_events SET timestamp = NOW() - ($1 * INTERVAL '1 hour') \
         WHERE workflow_exec_id = $2",
    )
    .bind::<diesel::sql_types::BigInt, _>(hours_ago)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("backdate events");
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

/// Force an execution's `search_attrs` column directly (bypassing a real
/// worker) -- mirrors `force_execution_state`'s pattern; used to seed the
/// `failure_cause` composition regression test (PR #1139 review, finding #3)
/// below.
async fn force_search_attrs(database_url: &str, exec_id: ExecutionId, search_attrs: Value) {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(database_url)
        .await
        .expect("failed to connect");
    diesel::sql_query("UPDATE harvest_workflow_executions SET search_attrs = $1 WHERE id = $2")
        .bind::<diesel::sql_types::Jsonb, _>(search_attrs)
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(&mut conn)
        .await
        .expect("force search_attrs");
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

// ─── PR #1139 review (finding #3): `failure_cause` composition ────────────

/// `failure_cause` (issue #506 discovery filter) must compose with
/// `history_bloat_min_events` and narrow its results -- before the fix,
/// `load_history_bloat_workflows` silently ignored `failure_cause` entirely,
/// so combining the two params returned every above-threshold row regardless
/// of its recorded failure cause instead of narrowing to the ones the caller
/// actually asked for.
#[tokio::test]
async fn history_bloat_min_events_composes_with_failure_cause() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let matching =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-cause-match", 9).await; // total 10
    force_search_attrs(
        &database_url,
        matching,
        json!({ "failure_cause": "non_determinism" }),
    )
    .await;

    let other_cause =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-cause-other", 9).await; // total 10
    force_search_attrs(
        &database_url,
        other_cause,
        json!({ "failure_cause": "poison_pill" }),
    )
    .await;

    // No `failure_cause` recorded at all -- also above threshold. Before the
    // fix this was returned unconditionally alongside `hb-cause-match`.
    let _no_cause =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-cause-none", 9).await; // total 10

    let (status, body) = get_json(
        &app,
        "/workflows?history_bloat_min_events=5&failure_cause=non_determinism",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert_eq!(
        ids,
        vec!["hb-cause-match".to_string()],
        "failure_cause must narrow the history-bloat discovery results to \
         only the matching cause, excluding both a differently-caused row \
         and an uncaused row that satisfy history_bloat_min_events alone; \
         got {ids:?}"
    );
}

// ─── PR #1139 review (finding B, second round): min_history_events compose ─

/// `min_history_events` (issue #493, the pre-existing general-purpose count
/// filter) must compose with `history_bloat_min_events` when the caller
/// supplies both at once, `AND`ing the two `>=` thresholds together -- before
/// the fix, `load_history_bloat_workflows` silently dropped
/// `min_history_events` entirely whenever `history_bloat_min_events` was also
/// present, so the *lower* of the two thresholds always won regardless of
/// which one the caller actually wanted enforced.
#[tokio::test]
async fn history_bloat_min_events_composes_with_min_history_events() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Below BOTH thresholds -- must never appear.
    let _too_small =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-mhe-too-small", 2)
            .await; // total 3

    // At/above the lower (`history_bloat_min_events=4`) but below the higher
    // (`min_history_events=6`) threshold -- before the fix this row was
    // wrongly returned, since only the lower threshold was ever enforced.
    let _between =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-mhe-between", 4).await; // total 5

    // At/above BOTH thresholds -- must always appear.
    let _above_both =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-mhe-above-both", 6)
            .await; // total 7

    let (status, body) = get_json(
        &app,
        "/workflows?history_bloat_min_events=4&min_history_events=6",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert_eq!(
        ids,
        vec!["hb-mhe-above-both".to_string()],
        "the stricter of the two composed count thresholds (min_history_events=6 \
         here, above history_bloat_min_events=4) must win -- a row satisfying \
         only the lower threshold must be excluded, not silently returned \
         because min_history_events was dropped; got {ids:?}"
    );

    // Reverse the roles -- `history_bloat_min_events` supplies the stricter
    // bound this time -- to prove composition is symmetric (an AND of both
    // predicates, not "whichever param happens to be checked first wins").
    let (status, body) = get_json(
        &app,
        "/workflows?history_bloat_min_events=6&min_history_events=4",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert_eq!(
        ids,
        vec!["hb-mhe-above-both".to_string()],
        "with the roles of the two params reversed, the same stricter bound \
         (6) must still apply; got {ids:?}"
    );
}

// ─── PR #1139 review (finding #4): mutually-exclusive discovery paths ─────

/// `no_progress_minutes` (stalled-workflow discovery, issue #486) and
/// `history_bloat_min_events` (this feature) are two separate,
/// mutually-exclusive discovery paths -- each sorts by a different computed
/// value the other loader neither computes nor honors. Combining them must
/// be rejected with a clear `400` rather than silently letting one win.
#[tokio::test]
async fn history_bloat_min_events_combined_with_no_progress_minutes_returns_400() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let (status, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=5&history_bloat_min_events=5",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "expected 400; body={body}");
    assert!(
        body.is_object(),
        "a 400 must carry a JSON error object; got {body}"
    );
}

// ─── PR #1139 review (round 3, finding b): no_progress_minutes must never ──
// leak `history_event_count`, even when the pre-existing, general-purpose
// `min_history_events` filter (issue #493) is ALSO supplied and satisfied.
//
// `history_event_count` is documented (`docs/api-contract.json`) and
// specified (`StalledWorkflowRow::history_event_count`'s doc comment) as
// populated ONLY on the dedicated `history_bloat_min_events` discovery path
// (`load_history_bloat_workflows`) -- never on the legacy `no_progress_minutes`
// path (`load_stalled_workflows`), regardless of which OTHER filters compose
// with it. An earlier revision ran an extra batch `COUNT(*)` query and
// populated the field whenever `min_history_events` was ALSO set on the
// `no_progress_minutes` path, silently changing the response shape for
// existing `?no_progress_minutes=N&min_history_events=M` callers.

/// A row matching BOTH `no_progress_minutes` (stalled) AND the composed
/// `min_history_events` count threshold must still omit `history_event_count`
/// entirely from its JSON -- not `null`, ABSENT (the field is
/// `#[serde(skip_serializing_if = "Option::is_none")]`).
#[tokio::test]
async fn no_progress_minutes_composed_with_min_history_events_never_leaks_history_event_count() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // 1 (WorkflowStarted) + 5 padding events = 6 total, satisfying
    // `min_history_events=4` below.
    let exec_id =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "no-progress-hb", 5).await;

    // Backdate every recorded event so the execution is ALSO "stalled" --
    // `no_progress_minutes` requires the MOST RECENT event to be older than
    // the threshold.
    backdate_all_events(&database_url, exec_id, 2).await;

    let (status, body) = get_json(
        &app,
        "/workflows?no_progress_minutes=60&min_history_events=4",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let rows = body.as_array().expect("response must be an array");
    let row = rows
        .iter()
        .find(|r| r["workflow_id"] == "no-progress-hb")
        .unwrap_or_else(|| panic!("expected the seeded row in the stalled results; body={body}"));
    assert!(
        row.get("history_event_count").is_none(),
        "`history_event_count` must be ABSENT (never populated, not even \
         `null`) on the `no_progress_minutes` (legacy stalled-workflow) \
         discovery path -- it is documented (docs/api-contract.json) as \
         exclusive to the dedicated `history_bloat_min_events` path; got \
         row={row}"
    );
}

// ─── PR #1139 review (finding C, second round): bounded per-shard load ────
//
// Before this fix, `load_history_bloat_workflows`'s per-shard candidate query
// had no `LIMIT` at all -- it loaded every matching live row (full
// `WorkflowExecution` structs, JSON payload columns included) and truncated
// only *after* the cross-shard merge. These tests prove the fix -- ordering
// by the current event count descending and applying `.limit(filters.limit)`
// per shard, before any full row is loaded -- still produces the exact same,
// correct top-K result: both within a single shard (proving the `ORDER BY`
// direction is right, not just "whatever LIMIT happens to return") and across
// shards where a single shard alone has *more* matching candidates than the
// global limit (proving the per-shard bound uses the full `filters.limit`,
// not some smaller divided value that would wrongly truncate a row the
// global top-K still needs).

/// A single shard with more above-threshold candidates than `limit`, seeded
/// in an order that does NOT match the count-descending result order -- so
/// only a genuinely-correct `ORDER BY (count) DESC LIMIT N` (not insertion
/// order, primary-key order, or the reverse direction) can produce the
/// expected top-2.
#[tokio::test]
async fn history_bloat_min_events_limit_orders_by_history_size_not_insertion_order() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Seeded smallest-first, then largest, then middle -- deliberately NOT in
    // count-descending order.
    let _smallest =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-limit-c", 5).await; // total 6
    let _largest =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-limit-a", 9).await; // total 10
    let _middle =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-limit-b", 7).await; // total 8

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=5&limit=2").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert_eq!(
        ids,
        vec!["hb-limit-a".to_string(), "hb-limit-b".to_string()],
        "the per-shard query must return the top-2 by current history size \
         descending (10, then 8), excluding the smallest (6) row entirely -- \
         a wrong ORDER BY direction, or one falling back to insertion/PK \
         order, would return a different pair or a different order; got {ids:?}"
    );
}

/// Two shards, each with *more* above-threshold candidates than the global
/// `limit`. The globally-second-largest row lives on the SAME shard as the
/// globally-largest row (shard 0's local rank-2), so it must survive shard
/// 0's own per-shard truncation to reach the final cross-shard merge -- proof
/// that the per-shard bound is the full `filters.limit`, not some smaller
/// value (e.g. `limit` divided across shards) that would wrongly drop it.
#[tokio::test]
async fn history_bloat_min_events_limit_survives_per_shard_truncation_across_shards() {
    let ((shard0_url, shard1_url), _container) = setup_sharded_databases().await;
    let pool = build_two_shard_pool(&shard0_url, &shard1_url);
    let app = build_app_with_pool(pool);

    // Shard 0: four candidates, counts [14, 20, 16, 18], seeded out of order.
    // Global top-3 needs BOTH x0-a (20, rank 1 globally) and x0-b (18, rank 3
    // globally / rank 2 *on this shard*) -- x0-c/x0-d must be excluded.
    let _x0_d = seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-x0-d", 13).await; // total 14
    let _x0_a = seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-x0-a", 19).await; // total 20
    let _x0_c = seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-x0-c", 15).await; // total 16
    let _x0_b = seed_workflow_with_history_size(&shard0_url, ShardId::new(0), "hb-x0-b", 17).await; // total 18

    // Shard 1: four candidates, counts [13, 19, 15, 17], seeded out of order.
    // Global top-3 needs ONLY x1-a (19, rank 2 globally) -- x1-b (17, rank 2
    // *on this shard*) is globally rank 4 and must be excluded by the final
    // cross-shard merge, even though it also survives its own shard's
    // per-shard truncation (limit=3 keeps 3 of shard 1's 4 rows locally).
    let _x1_d = seed_workflow_with_history_size(&shard1_url, ShardId::new(1), "hb-x1-d", 12).await; // total 13
    let _x1_a = seed_workflow_with_history_size(&shard1_url, ShardId::new(1), "hb-x1-a", 18).await; // total 19
    let _x1_c = seed_workflow_with_history_size(&shard1_url, ShardId::new(1), "hb-x1-c", 14).await; // total 15
    let _x1_b = seed_workflow_with_history_size(&shard1_url, ShardId::new(1), "hb-x1-b", 16).await; // total 17

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=5&limit=3").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");

    let ids = workflow_ids_of(&body);
    assert_eq!(
        ids,
        vec![
            "hb-x0-a".to_string(), // 20 -- global rank 1
            "hb-x1-a".to_string(), // 19 -- global rank 2
            "hb-x0-b".to_string(), // 18 -- global rank 3, shard 0's own local rank 2
        ],
        "the global top-3 must survive both the per-shard bound AND the final \
         cross-shard merge, in count-descending order; got {ids:?}"
    );
    for excluded in ["hb-x0-c", "hb-x0-d", "hb-x1-b", "hb-x1-c", "hb-x1-d"] {
        assert!(
            !ids.contains(&excluded.to_string()),
            "{excluded} must be excluded from the global top-3; got {ids:?}"
        );
    }
}

// ─── Bounded-EXISTS rewrite: exact-boundary / off-by-one equivalence ──────
//
// `apply_min_history_events_filter` (shared by both endpoints exercised
// above) replaced an unbounded `(SELECT COUNT(*) FROM harvest_events WHERE
// workflow_exec_id = id) >= min_events` with a bounded `EXISTS (... ORDER BY
// event_id OFFSET min_events - 1 LIMIT 1)`. The two are asserted
// boolean-equivalent for every `min_events` by construction (see that
// function's doc comment for the full derivation and buffer-count
// measurements), but the arithmetic that makes them equivalent -- `OFFSET
// min_events - 1`, not `min_events` or `min_events - 2` -- is exactly the
// kind of detail a careless rewrite gets wrong by one. The tests above cover
// generous margins (thresholds several events away from the seeded totals);
// these two pin the razor's edge directly: a workflow whose event count
// EXACTLY equals `min_events` must still be included (an off-by-one toward
// "exclusive" would wrongly drop it), and the identical workflow queried at
// `min_events + 1` must be excluded (an off-by-one toward "too permissive"
// would wrongly keep it) -- for both the smallest non-zero threshold (`1`,
// exercising `OFFSET 0`) and an arbitrary interior one (`7`, exercising
// `OFFSET 6`).

/// `min_history_events` (issue #493, the general-purpose `/workflows`
/// filter): exact-count inclusion and adjacent-count exclusion, at both the
/// `min_events=1` boundary (`OFFSET 0`) and an arbitrary interior boundary
/// (`min_events=7`, `OFFSET 6`).
#[tokio::test]
async fn min_history_events_exact_boundary_and_off_by_one_are_precise() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    // Total history_event_count == 1 (0 padding events) -- the smallest
    // possible non-zero history, exercising `OFFSET min_events - 1 == 0`.
    let _one =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-exact-one", 0).await;
    // Total history_event_count == 7 (6 padding events) -- an arbitrary
    // interior boundary, exercising `OFFSET min_events - 1 == 6`.
    let _seven =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "mhe-exact-seven", 6).await;

    // Exact boundary: `min_events == actual_count` must include both rows.
    let (status, body) = get_json(&app, "/workflows?min_history_events=1").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"mhe-exact-one".to_string()),
        "a 1-event history queried at min_history_events=1 (the exact \
         boundary, OFFSET 0) must be included; got {ids:?}"
    );

    let (status, body) = get_json(&app, "/workflows?min_history_events=7").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"mhe-exact-seven".to_string()),
        "a 7-event history queried at min_history_events=7 (the exact \
         boundary, OFFSET 6) must be included; got {ids:?}"
    );

    // Adjacent off-by-one: `min_events == actual_count + 1` must exclude
    // both rows -- the identical workflow, one threshold higher.
    let (status, body) = get_json(&app, "/workflows?min_history_events=2").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"mhe-exact-one".to_string()),
        "a 1-event history queried at min_history_events=2 (one past its \
         actual count) must be excluded; got {ids:?}"
    );

    let (status, body) = get_json(&app, "/workflows?min_history_events=8").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"mhe-exact-seven".to_string()),
        "a 7-event history queried at min_history_events=8 (one past its \
         actual count) must be excluded; got {ids:?}"
    );
}

/// Same exact-boundary / off-by-one proof as above, for
/// `history_bloat_min_events` (the dedicated discovery path) -- a second,
/// independent call site of the identical shared helper.
#[tokio::test]
async fn history_bloat_min_events_exact_boundary_and_off_by_one_are_precise() {
    let (database_url, _container) = setup_database().await;
    let app = build_app(&database_url);

    let _one =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-exact-one", 0).await;
    let _seven =
        seed_workflow_with_history_size(&database_url, ShardId::new(0), "hb-exact-seven", 6).await;

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=1").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"hb-exact-one".to_string()),
        "a 1-event history queried at history_bloat_min_events=1 (the exact \
         boundary, OFFSET 0) must be included; got {ids:?}"
    );

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=7").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        ids.contains(&"hb-exact-seven".to_string()),
        "a 7-event history queried at history_bloat_min_events=7 (the exact \
         boundary, OFFSET 6) must be included; got {ids:?}"
    );

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=2").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"hb-exact-one".to_string()),
        "a 1-event history queried at history_bloat_min_events=2 (one past \
         its actual count) must be excluded; got {ids:?}"
    );

    let (status, body) = get_json(&app, "/workflows?history_bloat_min_events=8").await;
    assert_eq!(status, StatusCode::OK, "expected 200; body={body}");
    let ids = workflow_ids_of(&body);
    assert!(
        !ids.contains(&"hb-exact-seven".to_string()),
        "a 7-event history queried at history_bloat_min_events=8 (one past \
         its actual count) must be excluded; got {ids:?}"
    );
}
