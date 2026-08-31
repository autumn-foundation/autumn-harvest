//! Ledger performance investigation: `GET /workflows/{id}/children?depth=N`.
//!
//! `load_workflow_children_tree_from_shards` (in
//! `autumn-harvest-plugin/src/api.rs`) walks the traversal frontier one
//! **parent at a time**, issuing one query per parent per shard per depth
//! level -- `O(nodes x shards)` round trips. Its own proven-fixed sibling,
//! `GET /workflows/{id}/tree` (issue #621), batches the *whole* frontier into
//! one `parent_id = ANY($1)` query per shard per depth level via
//! `store::load_workflow_children_batch` -- `O(depth x shards)` -- and says so
//! directly in a doc comment on that function:
//!
//! > "That turns a depth-`D` tree over `S` shards from `O(nodes x S)` round
//! > trips (one query per parent per shard -- what a naive recursion over
//! > `load_workflow_children` costs) into `O(D x S)`"
//!
//! `load_workflow_children_tree_from_shards` is exactly that naive
//! recursion: it calls the single-parent `store::load_workflow_children`
//! inside a `for parent in &frontier { for shard { .. } }` loop instead of a
//! batched `parent_id = ANY($1)` call. This is precisely the class of bug
//! Harvest's own performance-agent playbook calls out by name: "bookkeeping
//! queries that are individually trivial but collectively dominant -- these
//! won't show up in a buffers ranking, only in a `calls` ranking."
//!
//! This file is the harness + evidence generator for that investigation, and
//! (in its non-skipped test) a permanent regression guard for the fix's
//! correctness under real branching (multiple parents at one depth level,
//! spread across shards -- exactly the code path the batched query replaces).
//! It is not a duplicate of the 7 functional-correctness tests already
//! covering `GET /workflows/{id}/children` in `api_scheduler_integration.rs`
//! (filters, cursors, single-branch cross-shard recursion, depth caps, the
//! empty-vs-missing-parent distinction) -- those are untouched by this
//! change and still pass unmodified.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a Postgres **admin** URL (a
//! per-test database is created off it) to run without Docker; otherwise a
//! fresh testcontainers Postgres is booted for the non-skipped test (the
//! skipped-by-default evidence-capture test additionally needs
//! `pg_stat_statements` preloaded via `shared_preload_libraries`, which the
//! Docker fallback does not configure -- run it against a
//! `HARVEST_TEST_DATABASE_URL` target with
//! `shared_preload_libraries = 'pg_stat_statements'`). See
//! `docs/performance-workflow-children-traversal.md` for how the committed
//! evidence in `docs/perf-artifacts/workflow-children-traversal/` was
//! produced.

#![allow(clippy::too_many_lines, clippy::similar_names)]

use std::collections::BTreeMap;
use std::sync::Arc;

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::scheduler::{DagCatalog, SchedulerMonitor};
use autumn_harvest::schema::harvest_workflow_executions as wfx;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
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
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tower::ServiceExt;

// ── DB bootstrap (adapted from `lineage_tree_integration.rs`) ──────────────

fn init_sql() -> Vec<u8> {
    autumn_harvest::test_init_sql().as_bytes().to_vec()
}

type HarvestApiApp = axum::Router;
type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres container should start");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

async fn create_shard_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let url = replace_database(admin_url, name);
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh shard database");
    let bundle = String::from_utf8(init_sql()).expect("migration bundle is utf-8");
    conn.batch_execute(&bundle)
        .await
        .expect("apply migration bundle");
    url
}

fn replace_database(url: &str, name: &str) -> String {
    let (prefix, _) = url.rsplit_once('/').expect("url has a database segment");
    format!("{prefix}/{name}")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

fn router_for(shards: &[i32]) -> ShardRouter {
    let ids: Vec<ShardId> = shards.iter().map(|s| ShardId::new(*s)).collect();
    ShardRouter::new(ids.clone(), ids, ShardId::new(shards[0]))
}

fn build_app(pool: HarvestDbPool, router: ShardRouter) -> HarvestApiApp {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true);
    api_state.install_storage_pool(pool);
    api_state.install(HarvestApiRuntime::new(
        Arc::new(HandlerRegistry::new(vec![], vec![])),
        Arc::new(DagCatalog::default()),
        Arc::new(Vec::new()),
        Some("children-perf-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Two independent shard databases behind one router -- the shape that makes
/// the buggy loop's `for parent { for shard { .. } }` nesting actually cost
/// `shards` round trips per parent, not just 1.
async fn two_shard_app(admin_url: &str) -> (HarvestApiApp, [String; 2]) {
    let url0 = create_shard_db(admin_url, &unique("children_perf_s0")).await;
    let url1 = create_shard_db(admin_url, &unique("children_perf_s1")).await;
    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(ShardId::new(1), build_pool(&url1));
    let app = build_app(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );
    (app, [url0, url1])
}

async fn get_json(app: &HarvestApiApp, uri: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("x-harvest-admin", "true")
                .body(Body::empty())
                .unwrap(),
        )
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

// ── Fixture generation ──────────────────────────────────────────────────────

/// A generated node's identity, kept as owned data so the borrows inside
/// `NewWorkflowExecution<'_>` live only as long as one bulk-insert call.
struct Node {
    id: ExecutionId,
    shard: i32,
    parent: ExecutionId,
    workflow_id: String,
}

// `index % 2` is always 0 or 1, so this cast can neither truncate nor wrap.
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
const fn shard_for(index: usize) -> i32 {
    (index % 2) as i32
}

async fn insert_root(conn: &mut AsyncPgConnection, root: ExecutionId) {
    let row = NewWorkflowExecution {
        quota_key: None,
        continued_from_exec_id: None,
        first_exec_id: None,
        id: root.as_uuid(),
        workflow_name: "root_wf",
        workflow_id: "root",
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
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
        completion_callbacks: None,
        start_source: None,
        start_source_ref: None,
        started_by: None,
    };
    diesel::insert_into(wfx::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert root");
}

/// Bulk-insert `nodes` (all sharing `workflow_name`), chunked at 500 rows per
/// statement (`NewWorkflowExecution` has 34 columns; 500 x 34 = 17,000 bind
/// params, comfortably under Postgres's ~65,535 per-statement limit) and
/// grouped so each chunk lands on the connection for its own shard.
async fn bulk_insert(conns: &mut [AsyncPgConnection; 2], nodes: &[Node], workflow_name: &str) {
    for (shard, conn) in conns.iter_mut().enumerate() {
        // `shard` only ever ranges over `0..2` (the fixed two-shard array),
        // so this cast can neither truncate nor wrap.
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let shard_i32 = shard as i32;
        let shard_nodes: Vec<&Node> = nodes.iter().filter(|n| n.shard == shard_i32).collect();
        for chunk in shard_nodes.chunks(500) {
            let rows: Vec<NewWorkflowExecution> = chunk
                .iter()
                .map(|n| NewWorkflowExecution {
                    quota_key: None,
                    continued_from_exec_id: None,
                    first_exec_id: None,
                    id: n.id.as_uuid(),
                    workflow_name,
                    workflow_id: &n.workflow_id,
                    run_id: uuid::Uuid::new_v4(),
                    shard_id: n.shard,
                    input: json!({}),
                    parent_id: Some(n.parent.as_uuid()),
                    queue_name: "default",
                    execution_timeout: None,
                    deadline_at: None,
                    chain_execution_timeout: None,
                    chain_deadline_at: None,
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
                    completion_callbacks: None,
                    start_source: None,
                    start_source_ref: None,
                    started_by: None,
                })
                .collect();
            diesel::insert_into(wfx::table)
                .values(&rows)
                .execute(conn)
                .await
                .expect("bulk insert children chunk");
        }
    }
}

/// Mark every node whose fixture index is a multiple of 20 (within `ids`) as
/// FAILED, via one bulk `id = ANY($1)` UPDATE per shard rather than one
/// UPDATE per row -- deliberately not part of the code path under
/// investigation, so it must not itself distort the query-count evidence.
async fn mark_every_20th_failed(conns: &mut [AsyncPgConnection; 2], nodes: &[Node]) {
    let mut failed_ids: [Vec<uuid::Uuid>; 2] = [Vec::new(), Vec::new()];
    for (idx, node) in nodes.iter().enumerate() {
        if idx % 20 == 0 {
            #[allow(clippy::cast_sign_loss)]
            failed_ids[node.shard as usize].push(node.id.as_uuid());
        }
    }
    for shard in 0..2usize {
        if failed_ids[shard].is_empty() {
            continue;
        }
        diesel::update(wfx::table.filter(wfx::id.eq_any(&failed_ids[shard])))
            .set((
                wfx::state.eq("FAILED"),
                wfx::completed_at.eq(chrono::Utc::now()),
                wfx::error.eq(Some("synthetic failure for perf fixture".to_string())),
            ))
            .execute(&mut conns[shard])
            .await
            .expect("mark leaves failed");
    }
}

struct TreeShape {
    gen1: usize,
    gen2_per_gen1: usize,
    gen3_per_gen2: usize,
}

struct SeededTree {
    root: ExecutionId,
    gen1: Vec<Node>,
    gen2: Vec<Node>,
    gen3: Vec<Node>,
}

/// Seed a 3-generation tree rooted at a fresh execution, round-robining every
/// generation across both shards so a real fan-out (not a single-shard
/// happy path) is exercised at every depth level.
async fn seed_tree(conns: &mut [AsyncPgConnection; 2], shape: &TreeShape) -> SeededTree {
    let root = ExecutionId::new_for_shard(ShardId::new(0));
    insert_root(&mut conns[0], root).await;

    let mut gen1 = Vec::with_capacity(shape.gen1);
    for i in 0..shape.gen1 {
        let shard = shard_for(i);
        gen1.push(Node {
            id: ExecutionId::new_for_shard(ShardId::new(shard)),
            shard,
            parent: root,
            workflow_id: format!("gen1-{i}"),
        });
    }
    bulk_insert(conns, &gen1, "child_wf").await;

    let mut gen2 = Vec::with_capacity(shape.gen1 * shape.gen2_per_gen1);
    for (gi, g1) in gen1.iter().enumerate() {
        for j in 0..shape.gen2_per_gen1 {
            let idx = gi * shape.gen2_per_gen1 + j;
            let shard = shard_for(idx);
            gen2.push(Node {
                id: ExecutionId::new_for_shard(ShardId::new(shard)),
                shard,
                parent: g1.id,
                workflow_id: format!("gen2-{idx}"),
            });
        }
    }
    bulk_insert(conns, &gen2, "child_wf").await;

    let mut gen3 = Vec::with_capacity(gen2.len() * shape.gen3_per_gen2);
    for (gi, g2) in gen2.iter().enumerate() {
        for j in 0..shape.gen3_per_gen2 {
            let idx = gi * shape.gen3_per_gen2 + j;
            let shard = shard_for(idx);
            gen3.push(Node {
                id: ExecutionId::new_for_shard(ShardId::new(shard)),
                shard,
                parent: g2.id,
                workflow_id: format!("gen3-{idx}"),
            });
        }
    }
    bulk_insert(conns, &gen3, "child_wf").await;
    mark_every_20th_failed(conns, &gen3).await;

    SeededTree {
        root,
        gen1,
        gen2,
        gen3,
    }
}

// ── pg_stat_statements + EXPLAIN capture ────────────────────────────────────

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_hit: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    shared_blks_read: i64,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    total_buffers: i64,
}

#[derive(diesel::QueryableByName)]
struct ExplainRow {
    #[diesel(sql_type = diesel::sql_types::Text, column_name = "QUERY PLAN")]
    query_plan: String,
}

/// `pg_stat_statements` is a per-cluster (not per-database) shared catalog:
/// once the extension's view objects exist in *one* database reachable from
/// this connection, the rows it returns cover every database in the cluster,
/// filterable by `dbid`. So one connection (to shard 0) can snapshot both
/// shards' traversal-query cost in a single query.
async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

/// Scoped to the two shard databases' `dbid`s so a concurrent capture
/// against an unrelated database on the same cluster cannot pollute (or be
/// polluted by) this reset -- the argument-free cluster-wide reset form
/// would wipe a concurrent run's still-accumulating counters out from under
/// it.
async fn reset_stats_for_shards(conn: &mut AsyncPgConnection, shard_db_names: &[&str; 2]) {
    for name in shard_db_names {
        diesel::sql_query(format!(
            "SELECT pg_stat_statements_reset(0, \
                    (SELECT oid FROM pg_database WHERE datname = '{name}'), 0)"
        ))
        .execute(conn)
        .await
        .expect(
            "pg_stat_statements_reset(...) failed -- without a clean reset, a stale \
             entry from a prior capture could corrupt this snapshot's calls/buffers \
             ranking; the HARVEST_TEST_DATABASE_URL role must be able to reset \
             statistics (superuser, or explicitly granted EXECUTE on this function)",
        );
    }
}

/// Every statement whose text touches `harvest_workflow_executions` and
/// **filters** on `parent_id` (the `WHERE ... parent_id" = ` shape, not
/// merely a query that happens to *select* the `parent_id` column -- the
/// parent-existence lookup's `SELECT ... "parent_id", ... WHERE "id" = $1`
/// would otherwise false-positive-match a bare `%parent_id%` substring),
/// across both shard databases, ordered by total buffers touched.
async fn traversal_statements(
    conn: &mut AsyncPgConnection,
    shard_db_names: &[&str; 2],
) -> Vec<StatRow> {
    let dbid_list = shard_db_names
        .iter()
        .map(|name| format!("(SELECT oid FROM pg_database WHERE datname = '{name}')"))
        .collect::<Vec<_>>()
        .join(", ");
    diesel::sql_query(format!(
        "SELECT query, calls, shared_blks_hit, shared_blks_read, \
                (shared_blks_hit + shared_blks_read) AS total_buffers \
         FROM pg_stat_statements \
         WHERE query ILIKE '%harvest_workflow_executions%' \
           AND query ILIKE '%parent_id\" =%' \
           AND dbid IN ({dbid_list}) \
         ORDER BY total_buffers DESC LIMIT 10"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via \
         shared_preload_libraries for this capture to produce real evidence \
         rather than fail outright",
    )
}

async fn explain(conn: &mut AsyncPgConnection, sql: &str) -> String {
    diesel::sql_query("BEGIN")
        .execute(conn)
        .await
        .expect("begin");
    let loaded: Result<Vec<ExplainRow>, _> = diesel::sql_query(format!(
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF) {sql}"
    ))
    .load(conn)
    .await;
    diesel::sql_query("ROLLBACK")
        .execute(conn)
        .await
        .expect("rollback");
    loaded
        .unwrap_or_else(|e| panic!("EXPLAIN failed for {sql}: {e}"))
        .into_iter()
        .map(|r| r.query_plan)
        .collect::<Vec<_>>()
        .join("\n")
}

// ── Evidence capture (not a CI assertion) ───────────────────────────────────

#[tokio::test]
#[ignore = "evidence generator, not a CI assertion -- see \
            docs/performance-workflow-children-traversal.md"]
#[allow(clippy::too_many_lines)]
async fn zz_capture_children_traversal_perf_evidence() {
    let (admin, _guard) = setup_server().await;
    let (app, urls) = two_shard_app(&admin).await;
    let mut conns = [
        AsyncPgConnection::establish(&urls[0])
            .await
            .expect("shard 0 conn"),
        AsyncPgConnection::establish(&urls[1])
            .await
            .expect("shard 1 conn"),
    ];
    ensure_pg_stat_statements(&mut conns[0]).await;

    // ~3,900 descendants: 300 gen1 x (4 gen2 each) x (2 gen3 each), every
    // generation round-robined across both shards, every 20th leaf FAILED.
    let shape = TreeShape {
        gen1: 300,
        gen2_per_gen1: 4,
        gen3_per_gen2: 2,
    };
    let tree = seed_tree(&mut conns, &shape).await;
    eprintln!(
        "seeded: root + {} gen1 + {} gen2 + {} gen3 = {} total nodes",
        tree.gen1.len(),
        tree.gen2.len(),
        tree.gen3.len(),
        1 + tree.gen1.len() + tree.gen2.len() + tree.gen3.len(),
    );

    let out_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-plugin/ has a workspace-root parent")
        .join("docs")
        .join("perf-artifacts")
        .join("workflow-children-traversal");
    std::fs::create_dir_all(&out_dir).expect("create artifact output directory");

    let label = std::env::var("PERF_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    eprintln!("== capturing label={label} ==");

    let db_names = [
        urls[0].rsplit('/').next().expect("db name in url"),
        urls[1].rsplit('/').next().expect("db name in url"),
    ];

    let mut stats_conn = AsyncPgConnection::establish(&urls[0])
        .await
        .expect("stats connection");
    reset_stats_for_shards(&mut stats_conn, &db_names).await;

    // The one, real, public entry point -- the exact request an operator's
    // Vantage UI / CLI would make to inspect a run's fan-out. `limit=500`
    // (the endpoint's own MAX_WORKFLOW_CHILDREN_LIMIT ceiling) requests the
    // largest single page the API allows -- but the traversal that produces
    // it walks and queries every one of the ~3,900 descendants internally
    // *before* the final response is paginated down to `limit` rows (the
    // traversal_filters passed to the per-depth store calls carry
    // `limit: None` -- pagination is applied only to the flattened result,
    // never to the walk itself), so the query-count cost under investigation
    // is unaffected by what limit is requested here.
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{}/children?depth=2&limit=500", tree.root),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "traversal request must succeed");
    let returned = body["items"].as_array().expect("items array").len();
    assert_eq!(
        returned, 500,
        "the response page is capped at MAX_WORKFLOW_CHILDREN_LIMIT even though \
         the traversal beneath it touched every one of the ~3,900 descendants"
    );

    let stats_rows = traversal_statements(&mut stats_conn, &db_names).await;
    assert!(
        !stats_rows.is_empty(),
        "pg_stat_statements returned zero rows matching the children-traversal \
         shape after one real depth=2 request -- check pg_stat_statements.track \
         (must be 'all' or 'top', not 'none') and shared_preload_libraries",
    );
    let total_calls: i64 = stats_rows.iter().map(|r| r.calls).sum();
    let total_buffers: i64 = stats_rows.iter().map(|r| r.total_buffers).sum();

    let stats_text = stats_rows
        .iter()
        .map(|r| {
            format!(
                "calls={} shared_blks_hit={} shared_blks_read={} total_buffers={}\nquery={}\n",
                r.calls, r.shared_blks_hit, r.shared_blks_read, r.total_buffers, r.query,
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(
        out_dir.join(format!("{label}-pg_stat_statements.txt")),
        format!(
            "-- {label}: pg_stat_statements @ one real GET /workflows/{{id}}/children?depth=2 \
             request, 300 gen1 x 4 gen2 x 2 gen3, 2 shards --\n\
             TOTAL across matching statement shapes: calls={total_calls} buffers={total_buffers}\n\n\
             {stats_text}\n"
        ),
    )
    .expect("write pg_stat_statements artifact");
    eprintln!("total_calls={total_calls} total_buffers={total_buffers}");

    // A representative EXPLAIN for a single-parent lookup (the shape every
    // one of the pre-fix loop's per-parent queries takes) and for a
    // depth-2-sized batched lookup (the shape the fix's per-shard,
    // per-depth query takes at its most expensive level) -- both against a
    // real gen1/gen2 id set from this fixture, so the plan reflects the
    // actual index (`idx_harvest_we_parent_id`) and row cardinality.
    let one_parent = tree.gen1[0].id.as_uuid();
    let single_sql = format!(
        "SELECT id, workflow_name, state, started_at, completed_at, error, shard_id, \
                parent_close_policy \
         FROM harvest_workflow_executions WHERE parent_id = '{one_parent}' \
         ORDER BY started_at DESC, id DESC"
    );
    let single_plan = explain(&mut conns[0], &single_sql).await;
    std::fs::write(
        out_dir.join(format!("{label}-explain-single-parent.txt")),
        format!("-- {label}: single-parent lookup (one iteration of the pre-fix loop) --\n{single_plan}\n"),
    )
    .expect("write single-parent explain artifact");

    let gen2_ids_shard0 = tree
        .gen2
        .iter()
        .filter(|n| n.shard == 0)
        .map(|n| format!("'{}'", n.id.as_uuid()))
        .collect::<Vec<_>>()
        .join(",");
    let batched_sql = format!(
        "SELECT id, workflow_name, state, started_at, completed_at, error, shard_id, \
                parent_close_policy \
         FROM harvest_workflow_executions WHERE parent_id = ANY(ARRAY[{gen2_ids_shard0}]::uuid[]) \
         ORDER BY started_at DESC, id DESC"
    );
    let batched_plan = explain(&mut conns[0], &batched_sql).await;
    std::fs::write(
        out_dir.join(format!("{label}-explain-batched-frontier.txt")),
        format!(
            "-- {label}: batched depth-2-frontier lookup on shard 0 ({} parent ids, \
             the fix's per-shard/per-depth query at its most expensive level) --\n{batched_plan}\n",
            tree.gen2.iter().filter(|n| n.shard == 0).count(),
        ),
    )
    .expect("write batched-frontier explain artifact");

    std::fs::write(
        out_dir.join(format!("{label}-fixture-summary.txt")),
        format!(
            "label={label}\n\
             fixture: root + {} gen1 + {} gen2 + {} gen3 = {} total nodes, \
             round-robined across 2 shards, every 20th leaf FAILED\n\
             request: GET /workflows/{{root}}/children?depth=2\n\
             returned_rows={returned}\n\
             traversal_statement_shapes={}\n\
             total_calls={total_calls}\n\
             total_buffers={total_buffers}\n",
            tree.gen1.len(),
            tree.gen2.len(),
            tree.gen3.len(),
            1 + tree.gen1.len() + tree.gen2.len() + tree.gen3.len(),
            stats_rows.len(),
        ),
    )
    .expect("write fixture summary");

    eprintln!("evidence capture complete: label={label}");
}

// ── Regression: fix correctness under real multi-parent branching ─────────

/// Moderate-scale, permanent CI test: a 3-generation tree where **multiple
/// parents exist at every depth level, spread across both shards** -- the
/// exact shape that exercises the fix's batched-frontier query differently
/// from a single-branch chain (which a `parent_id = ANY($1)` call over a
/// one-element frontier can't meaningfully distinguish from the old
/// single-parent call). Asserts the full descendant set, per-depth stamps,
/// and the FAILED subset are exactly right by `workflow_id`, independent of
/// row order.
#[tokio::test]
async fn depth_traversal_with_branching_frontiers_returns_the_exact_descendant_set() {
    let (admin, _guard) = setup_server().await;
    let (app, urls) = two_shard_app(&admin).await;
    let mut conns = [
        AsyncPgConnection::establish(&urls[0])
            .await
            .expect("shard 0 conn"),
        AsyncPgConnection::establish(&urls[1])
            .await
            .expect("shard 1 conn"),
    ];

    let shape = TreeShape {
        gen1: 6,
        gen2_per_gen1: 3,
        gen3_per_gen2: 2,
    };
    let tree = seed_tree(&mut conns, &shape).await;
    assert_eq!(tree.gen1.len(), 6);
    assert_eq!(tree.gen2.len(), 18);
    assert_eq!(tree.gen3.len(), 36);

    // `limit=100` comfortably exceeds this fixture's 60 descendants so the
    // whole set fits on one page -- the default page size is 50, which would
    // otherwise silently truncate this exact-set comparison.
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{}/children?depth=2&limit=100", tree.root),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().expect("items array");
    assert_eq!(
        items.len(),
        6 + 18 + 36,
        "every non-root descendant, 3 generations"
    );

    // `WorkflowChildResponse` carries `exec_id` (not the business
    // `workflow_id` string) -- `exec_id` is just as good a unique identity
    // key for this comparison, since it's the primary key every node was
    // generated with.
    let mut by_depth: std::collections::HashMap<u64, Vec<String>> =
        std::collections::HashMap::new();
    let mut failed_exec_ids: Vec<String> = Vec::new();
    for item in items {
        let depth = item["depth"].as_u64().expect("depth field");
        let exec_id = item["exec_id"].as_str().map_or_else(
            || panic!("expected an exec_id field on each child row, got {item}"),
            ToOwned::to_owned,
        );
        by_depth.entry(depth).or_default().push(exec_id.clone());
        // `workflow_child_status_label` humanizes the raw DB state string
        // ("FAILED" -> "Failed") before it reaches the response JSON.
        if item["status"] == "Failed" {
            failed_exec_ids.push(exec_id);
        }
    }

    assert_eq!(by_depth.get(&0).map(Vec::len), Some(6), "depth 0 = gen1");
    assert_eq!(by_depth.get(&1).map(Vec::len), Some(18), "depth 1 = gen2");
    assert_eq!(by_depth.get(&2).map(Vec::len), Some(36), "depth 2 = gen3");

    let mut expected_gen1: Vec<String> = tree.gen1.iter().map(|n| n.id.to_string()).collect();
    let mut actual_gen1 = by_depth.remove(&0).unwrap();
    expected_gen1.sort();
    actual_gen1.sort();
    assert_eq!(
        actual_gen1, expected_gen1,
        "depth-0 set must match gen1 exactly"
    );

    let mut expected_gen2: Vec<String> = tree.gen2.iter().map(|n| n.id.to_string()).collect();
    let mut actual_gen2 = by_depth.remove(&1).unwrap();
    expected_gen2.sort();
    actual_gen2.sort();
    assert_eq!(
        actual_gen2, expected_gen2,
        "depth-1 set must match gen2 exactly"
    );

    let mut expected_gen3: Vec<String> = tree.gen3.iter().map(|n| n.id.to_string()).collect();
    let mut actual_gen3 = by_depth.remove(&2).unwrap();
    expected_gen3.sort();
    actual_gen3.sort();
    assert_eq!(
        actual_gen3, expected_gen3,
        "depth-2 set must match gen3 exactly"
    );

    let mut expected_failed: Vec<String> = tree
        .gen3
        .iter()
        .enumerate()
        .filter(|(idx, _)| idx % 20 == 0)
        .map(|(_, n)| n.id.to_string())
        .collect();
    failed_exec_ids.sort();
    expected_failed.sort();
    assert_eq!(
        failed_exec_ids, expected_failed,
        "every-20th-leaf FAILED marking must survive the traversal unchanged"
    );
}
