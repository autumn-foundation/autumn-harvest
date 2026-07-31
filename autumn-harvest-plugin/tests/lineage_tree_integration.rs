//! Integration tests for `GET /api/harvest/workflows/{id}/tree` (issue #621).
//!
//! The recursive, cross-shard lineage tree. The pure walk (bounds, cycle
//! safety, nesting, summarisation) is unit-tested in
//! `autumn-harvest-plugin/src/lineage.rs`; these tests verify the HTTP wiring
//! plus the thing no unit test can prove — that a child which landed on a
//! **different shard** than its parent still appears in the tree.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a Postgres **admin** URL (a
//! per-test database is created off it) to run without Docker; otherwise a
//! fresh testcontainers Postgres is booted with the full migration bundle.

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
use chrono::{Duration, Utc};
use diesel::prelude::*;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
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

/// Handle keeping a testcontainers Postgres alive for the test's duration
/// (`None` in the local-Postgres mode).
type DbGuard = Option<ContainerAsync<Postgres>>;

/// Admin URL of a Postgres that can create databases, plus a keep-alive guard.
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

/// Create a fresh, fully-migrated database and return its URL.
///
/// Each shard gets its own database so the cross-shard tests exercise genuinely
/// independent stores rather than one database pretending to be two.
async fn create_shard_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    // Ignore "already exists" — a re-run against a local server is fine.
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
        Some("lineage-test".to_string()),
        vec!["default".to_string()],
        SchedulerMonitor::offline(),
        HarvestRetentionRuntime::disabled(autumn_harvest::RetentionConfig::default()),
        router,
    ));
    harvest_api_router(api_state).with_state(AppState::for_test().with_profile("test"))
}

/// Single-shard app plus a connection to its database.
async fn single_shard_app(admin_url: &str) -> (HarvestApiApp, AsyncPgConnection) {
    let url = create_shard_db(admin_url, &unique("lineage_s0")).await;
    let app = build_app(HarvestDbPool::from(build_pool(&url)), router_for(&[0]));
    let conn = AsyncPgConnection::establish(&url).await.expect("connect");
    (app, conn)
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

struct Spec<'a> {
    shard: i32,
    workflow_name: &'a str,
    workflow_id: &'a str,
    state: &'a str,
    parent: Option<ExecutionId>,
    parent_close_policy: Option<&'a str>,
    offset_secs: i64,
}

impl<'a> Spec<'a> {
    const fn child(parent: ExecutionId, workflow_id: &'a str, offset: i64) -> Self {
        Self {
            shard: 0,
            workflow_name: "child_wf",
            workflow_id,
            state: "RUNNING",
            parent: Some(parent),
            parent_close_policy: None,
            offset_secs: offset,
        }
    }

    const fn root(workflow_id: &'a str) -> Self {
        Self {
            shard: 0,
            workflow_name: "root_wf",
            workflow_id,
            state: "RUNNING",
            parent: None,
            parent_close_policy: None,
            offset_secs: 0,
        }
    }

    const fn on_shard(mut self, shard: i32) -> Self {
        self.shard = shard;
        self
    }

    const fn in_state(mut self, state: &'a str) -> Self {
        self.state = state;
        self
    }

    const fn detached(mut self, policy: &'a str) -> Self {
        self.parent_close_policy = Some(policy);
        self
    }
}

async fn insert(conn: &mut AsyncPgConnection, spec: Spec<'_>) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(spec.shard));
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: spec.workflow_name,
        workflow_id: spec.workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: spec.shard,
        input: json!({}),
        parent_id: spec.parent.map(|p| p.as_uuid()),
        queue_name: "default",
        execution_timeout: None,
        deadline_at: None,
        chain_execution_timeout: None,
        chain_deadline_at: None,
        memo: None,
        search_attrs: None,
        assigned_build_id: None,
        parent_close_policy: spec.parent_close_policy.map(ToOwned::to_owned),
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
        .expect("insert execution");

    let started_at = Utc::now() + Duration::seconds(spec.offset_secs);
    let completed_at = if spec.state == "RUNNING" {
        None
    } else {
        Some(started_at + Duration::seconds(1))
    };
    diesel::update(wfx::table.find(exec_id.as_uuid()))
        .set((
            wfx::state.eq(spec.state),
            wfx::started_at.eq(started_at),
            wfx::completed_at.eq(completed_at),
        ))
        .execute(conn)
        .await
        .expect("set state");
    exec_id
}

fn children_of(node: &Value) -> &Vec<Value> {
    node["children"].as_array().expect("children array")
}

// ── 404 / 400 ────────────────────────────────────────────────────────────

#[tokio::test]
async fn unknown_root_returns_404() {
    let (admin, _guard) = setup_server().await;
    let (app, _conn) = single_shard_app(&admin).await;
    let unknown = ExecutionId::new_for_shard(ShardId::new(0));
    let (status, _) = get_json(&app, &format!("/workflows/{unknown}/tree")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "AC: 404 for an unknown root");
}

#[tokio::test]
async fn malformed_root_id_returns_400() {
    let (admin, _guard) = setup_server().await;
    let (app, _conn) = single_shard_app(&admin).await;
    let (status, _) = get_json(&app, "/workflows/not-a-uuid/tree").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn out_of_range_bounds_return_400_naming_the_offending_bound() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;
    let root = insert(&mut conn, Spec::root("r")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_depth=0")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("max_depth"),
        "the error must name the offending bound, got {body}"
    );

    let (status, _) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=999999")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&app, &format!("/workflows/{root}/tree?max_depth=abc")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-numeric bound");
}

// ── Shape ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_leaf_root_returns_an_empty_children_array_not_an_error() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;
    let root = insert(&mut conn, Spec::root("leaf")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK, "AC: a leaf is not an error");
    assert!(children_of(&body["root"]).is_empty());
    assert_eq!(body["node_count"], 1);
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn a_four_level_tree_is_returned_nested_in_a_single_call() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    let l1 = insert(&mut conn, Spec::child(root, "l1", 1)).await;
    let l2 = insert(&mut conn, Spec::child(l1, "l2", 2)).await;
    let l3 = insert(&mut conn, Spec::child(l2, "l3", 3).in_state("FAILED")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["root"]["execution_id"], root.to_string());
    assert_eq!(body["node_count"], 4);
    assert_eq!(body["max_depth_reached"], 3);

    let n1 = &children_of(&body["root"])[0];
    assert_eq!(n1["execution_id"], l1.to_string());
    assert_eq!(n1["depth"], 1);
    let n2 = &children_of(n1)[0];
    assert_eq!(n2["execution_id"], l2.to_string());
    let n3 = &children_of(n2)[0];
    assert_eq!(n3["execution_id"], l3.to_string());
    assert_eq!(n3["depth"], 3);

    // AC: every node carries the full identity + lifecycle projection.
    assert_eq!(n3["state"], "FAILED");
    assert_eq!(n3["workflow_name"], "child_wf");
    assert_eq!(n3["workflow_id"], "l3");
    assert_eq!(n3["parent_id"], l2.to_string());
    assert!(!n3["started_at"].is_null());
    assert!(
        !n3["closed_at"].is_null(),
        "a terminal node reports closed_at"
    );
    assert_eq!(
        n1["closed_at"],
        Value::Null,
        "a live node has null closed_at"
    );
}

#[tokio::test]
async fn a_mid_tree_root_returns_only_the_subtree_below_it() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("top")).await;
    let mid = insert(&mut conn, Spec::child(root, "mid", 1)).await;
    let leaf = insert(&mut conn, Spec::child(mid, "leaf", 2)).await;
    // A sibling branch that must NOT appear when rooting at `mid`.
    let other = insert(&mut conn, Spec::child(root, "other", 3)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{mid}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["root"]["execution_id"], mid.to_string());
    assert_eq!(
        body["root"]["depth"], 0,
        "the chosen root is always depth 0"
    );
    assert_eq!(
        body["root"]["parent_id"],
        root.to_string(),
        "a mid-tree root still reports its own parent"
    );
    assert_eq!(body["node_count"], 2, "only mid + leaf");
    let kids = children_of(&body["root"]);
    assert_eq!(kids.len(), 1);
    assert_eq!(kids[0]["execution_id"], leaf.to_string());
    assert!(
        !body.to_string().contains(&other.to_string()),
        "the sibling branch must not leak into a subtree view"
    );
}

#[tokio::test]
async fn a_detached_child_reports_its_parent_close_policy() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("p")).await;
    insert(&mut conn, Spec::child(root, "awaited", 1)).await;
    insert(
        &mut conn,
        Spec::child(root, "detached", 2).detached("terminate"),
    )
    .await;
    // `await_mode` is derived as "detached iff the stored policy parses", so
    // every accepted spelling must be exercised — an unparsed one would
    // silently report a detached child as awaited.
    insert(
        &mut conn,
        Spec::child(root, "detached-abandon", 3).detached("abandon"),
    )
    .await;
    insert(
        &mut conn,
        Spec::child(root, "detached-cancel", 4).detached("request_cancel"),
    )
    .await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    let kids = children_of(&body["root"]);
    let awaited = kids
        .iter()
        .find(|c| c["workflow_id"] == "awaited")
        .expect("awaited child");
    assert_eq!(awaited["await_mode"], "awaited");
    assert_eq!(awaited["parent_close_policy"], Value::Null);

    let detached = kids
        .iter()
        .find(|c| c["workflow_id"] == "detached")
        .expect("detached child");
    assert_eq!(detached["await_mode"], "detached");
    assert_eq!(detached["parent_close_policy"], "terminate");

    for (wf_id, policy) in [
        ("detached-abandon", "abandon"),
        ("detached-cancel", "request_cancel"),
    ] {
        let node = kids
            .iter()
            .find(|c| c["workflow_id"] == wf_id)
            .unwrap_or_else(|| panic!("{wf_id} child"));
        assert_eq!(node["await_mode"], "detached", "{wf_id}");
        assert_eq!(node["parent_close_policy"], policy, "{wf_id}");
    }
}

/// The root's OWN projection is easy to leave unasserted, because every other
/// test roots at a live workflow. Rooting at a *finished* saga is the common
/// triage case, so its terminal timestamp and identity must survive.
#[tokio::test]
async fn a_terminal_root_reports_its_own_closed_at_and_identity() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("done").in_state("COMPLETED")).await;
    insert(&mut conn, Spec::child(root, "kid", 1)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    let node = &body["root"];
    assert_eq!(node["execution_id"], root.to_string());
    assert_eq!(node["workflow_name"], "root_wf");
    assert_eq!(node["workflow_id"], "done");
    assert_eq!(node["state"], "COMPLETED");
    assert_eq!(node["depth"], 0);
    assert!(
        !node["closed_at"].is_null(),
        "a terminal root must report when it closed, got {node}"
    );
    assert!(!node["started_at"].is_null());
}

/// A budget that fits the whole tree exactly is a COMPLETE answer. This is the
/// guard against over-correcting the truncation fixes into false positives.
#[tokio::test]
async fn a_budget_that_fits_the_tree_exactly_is_not_reported_as_truncated() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("exact")).await;
    insert(&mut conn, Spec::child(root, "a", 1)).await;
    insert(&mut conn, Spec::child(root, "b", 2)).await;

    // root + 2 children == 3, and there are no grandchildren.
    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=3")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["node_count"], 3);
    assert_eq!(
        body["truncated"], false,
        "the whole tree fit; nothing was cut"
    );
    assert!(body["truncation_reason"].is_null());
    assert!(
        body["truncated_parent_ids"]
            .as_array()
            .is_none_or(Vec::is_empty)
    );
}

// ── Cross-shard (the headline AC) ────────────────────────────────────────

#[tokio::test]
async fn children_placed_on_a_different_shard_than_their_parent_appear_in_the_tree() {
    let (admin, _guard) = setup_server().await;
    let url0 = create_shard_db(&admin, &unique("lineage_x0")).await;
    let url1 = create_shard_db(&admin, &unique("lineage_x1")).await;

    let mut conn0 = AsyncPgConnection::establish(&url0).await.expect("shard 0");
    let mut conn1 = AsyncPgConnection::establish(&url1).await.expect("shard 1");

    // root -> (same-shard child, cross-shard child) -> cross-shard grandchild.
    let root = insert(&mut conn0, Spec::root("root")).await;
    let same = insert(&mut conn0, Spec::child(root, "same-shard", 1)).await;
    let cross = insert(&mut conn1, Spec::child(root, "cross-shard", 2).on_shard(1)).await;
    let grand = insert(
        &mut conn1,
        Spec::child(cross, "cross-grandchild", 3).on_shard(1),
    )
    .await;

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(ShardId::new(1), build_pool(&url1));
    let app = build_app(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "complete");
    assert_eq!(body["node_count"], 4, "root + 2 children + 1 grandchild");

    let rendered = body.to_string();
    assert!(rendered.contains(&same.to_string()), "same-shard child");
    assert!(
        rendered.contains(&cross.to_string()),
        "AC: a child on a DIFFERENT shard than its parent must appear"
    );
    assert!(
        rendered.contains(&grand.to_string()),
        "and the walk must continue through it"
    );

    // The cross-shard grandchild must be nested under the cross-shard child,
    // not flattened to the root.
    let cross_node = children_of(&body["root"])
        .iter()
        .find(|c| c["execution_id"] == cross.to_string())
        .expect("cross-shard child node")
        .clone();
    assert_eq!(cross_node["shard_id"], 1);
    assert_eq!(
        children_of(&cross_node)[0]["execution_id"],
        grand.to_string()
    );
}

#[tokio::test]
async fn an_unreachable_shard_degrades_to_partial_instead_of_failing_the_whole_call() {
    // Issue #756: a cross-shard read must not 500 during exactly the incident
    // it exists to triage.
    let (admin, _guard) = setup_server().await;
    let url0 = create_shard_db(&admin, &unique("lineage_p0")).await;
    let mut conn0 = AsyncPgConnection::establish(&url0).await.expect("shard 0");

    let root = insert(&mut conn0, Spec::root("root")).await;
    let child = insert(&mut conn0, Spec::child(root, "reachable", 1)).await;

    let mut pools = BTreeMap::new();
    pools.insert(ShardId::new(0), build_pool(&url0));
    pools.insert(
        ShardId::new(1),
        build_pool("postgres://postgres:postgres@127.0.0.1:1/nonexistent"),
    );
    let app = build_app(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK, "must not be a 500");
    assert_eq!(body["status"], "partial");
    let unavailable = body["unavailable_shards"]
        .as_array()
        .expect("unavailable_shards");
    assert_eq!(unavailable.len(), 1);
    assert_eq!(unavailable[0]["shard_id"], 1, "the down shard is named");
    // The reachable shard's rows still flow.
    assert!(body.to_string().contains(&child.to_string()));

    // Summary mode must carry the SAME verdict. A roll-up that reads
    // `complete` during an outage is worse than a partial tree, because the
    // counts look authoritative.
    let (status, summary) = get_json(&app, &format!("/workflows/{root}/tree?summary=true")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        summary["status"], "partial",
        "the summary must not claim a complete read"
    );
    let summary_unavailable = summary["unavailable_shards"]
        .as_array()
        .expect("summary names the down shard too");
    assert_eq!(summary_unavailable.len(), 1);
    assert_eq!(summary_unavailable[0]["shard_id"], 1);
}

// ── Bounds (AC: no silent cap) ───────────────────────────────────────────

#[tokio::test]
async fn max_depth_truncation_flags_the_reason_and_names_the_dropped_parent() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("deep")).await;
    let l1 = insert(&mut conn, Spec::child(root, "l1", 1)).await;
    let l2 = insert(&mut conn, Spec::child(l1, "l2", 2)).await;
    insert(&mut conn, Spec::child(l2, "l3", 3)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_depth=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], true);
    assert_eq!(body["truncation_reason"], "max_depth");
    assert_eq!(body["max_depth_reached"], 2);
    let dropped = body["truncated_parent_ids"]
        .as_array()
        .expect("truncated_parent_ids");
    assert_eq!(
        dropped,
        &vec![Value::String(l2.to_string())],
        "AC: the dropped subtree is named by its parent execution_id"
    );
    assert_eq!(
        body["limits"]["max_depth"], 2,
        "the applied bound is echoed"
    );
}

#[tokio::test]
async fn a_tree_that_ends_exactly_at_the_depth_cap_is_not_reported_truncated() {
    // No false-positive truncation: the probe must prove children exist.
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("exact")).await;
    insert(&mut conn, Spec::child(root, "l1", 1)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_depth=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], false);
    assert_eq!(body["truncation_reason"], Value::Null);
    assert!(
        body["truncated_parent_ids"]
            .as_array()
            .expect("array")
            .is_empty()
    );
}

#[tokio::test]
async fn max_nodes_truncation_flags_the_reason_and_names_the_dropped_parent() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("wide")).await;
    for i in 0..5 {
        insert(&mut conn, Spec::child(root, &format!("c{i}"), i)).await;
    }

    // root + 2 children.
    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=3")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], true);
    assert_eq!(body["truncation_reason"], "max_nodes");
    assert_eq!(body["node_count"], 3);
    assert_eq!(children_of(&body["root"]).len(), 2);
    let dropped = body["truncated_parent_ids"].as_array().expect("array");
    assert!(
        dropped.contains(&Value::String(root.to_string())),
        "the parent whose children were dropped is named"
    );
}

/// `max_nodes=1` spends the entire budget on the root, so the walk breaks
/// before querying a single shard. That must still be reported as a
/// truncation — a root with children is otherwise indistinguishable from a
/// leaf, which is exactly the silent cap AC4 forbids.
#[tokio::test]
async fn a_budget_that_only_fits_the_root_is_still_reported_as_truncated() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("budget-one")).await;
    insert(&mut conn, Spec::child(root, "only-child", 0)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["node_count"], 1);
    assert_eq!(
        body["truncated"], true,
        "the root provably has a child that did not fit the budget"
    );
    assert_eq!(body["truncation_reason"], "max_nodes");
    let dropped = body["truncated_parent_ids"].as_array().expect("array");
    assert!(
        dropped.contains(&Value::String(root.to_string())),
        "the root is named so the operator can re-root with a bigger budget"
    );
}

/// The mirror of the test above: when the root genuinely has no children, a
/// budget of one is a *complete* answer, not a truncated one.
#[tokio::test]
async fn a_childless_root_at_the_budget_floor_is_not_reported_as_truncated() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("budget-one-leaf")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["node_count"], 1);
    assert_eq!(body["truncated"], false, "a leaf is a complete answer");
    assert!(body["truncation_reason"].is_null());
}

/// A budget that is spent EXACTLY (no row is ever rejected for budget) still
/// stopped the walk for `max_nodes`, not `max_depth`. An alerting consumer
/// keys on the wire string, so raising the wrong bound must not be suggested.
#[tokio::test]
async fn an_exactly_spent_budget_reports_max_nodes_not_max_depth() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    // root -> a -> c : a chain, so each level admits exactly one node.
    let root = insert(&mut conn, Spec::root("chain-root")).await;
    let a = insert(&mut conn, Spec::child(root, "a", 0)).await;
    insert(&mut conn, Spec::child(a, "c", 1)).await;

    // root + a fits exactly; `c` is never fetched and nothing is rejected.
    let (status, body) = get_json(
        &app,
        &format!("/workflows/{root}/tree?max_nodes=2&max_depth=20"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["node_count"], 2);
    assert_eq!(body["truncated"], true);
    assert_eq!(
        body["truncation_reason"], "max_nodes",
        "the node budget is what stopped the walk; max_depth was nowhere near"
    );
}

/// An unrecognised `summary` value must be rejected, not silently treated as
/// `false` — a caller asking for a roll-up and receiving a full tree with a
/// 200 has no way to notice the typo.
#[tokio::test]
async fn an_unrecognised_summary_value_is_rejected() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;
    let root = insert(&mut conn, Spec::root("summary-typo")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?summary=yes")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body.to_string().contains("summary"),
        "the error names the offending parameter, got {body}"
    );

    // The accepted spellings still work, in both cases.
    for (value, expect_summary) in [
        ("true", true),
        ("TRUE", true),
        ("1", true),
        ("false", false),
    ] {
        let (status, body) =
            get_json(&app, &format!("/workflows/{root}/tree?summary={value}")).await;
        assert_eq!(status, StatusCode::OK, "summary={value}");
        assert_eq!(
            body.get("counts").is_some(),
            expect_summary,
            "summary={value} should{} produce a roll-up",
            if expect_summary { "" } else { " not" }
        );
    }
}

/// `status` must be evidence-based. A shard that was never queried cannot be
/// reported as inspected — otherwise a total outage reads `complete`.
#[tokio::test]
async fn a_shard_that_was_never_queried_is_not_reported_as_inspected() {
    let (admin, _guard) = setup_server().await;
    let shard0 = create_shard_db(&admin, &unique("lineage_s0")).await;
    let pool0 = build_pool(&shard0);

    // Shard 1 is known to the router but its pool points nowhere reachable.
    let dead = replace_database(&admin, "definitely_not_a_database");
    let pools = std::collections::BTreeMap::from([
        (ShardId::new(0), pool0.clone()),
        (ShardId::new(1), build_pool(&dead)),
    ]);
    let app = build_app(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );
    let mut conn = pool0.get().await.expect("shard 0 conn");

    let root = insert(&mut conn, Spec::root("never-queried")).await;
    insert(&mut conn, Spec::child(root, "kid", 0)).await;

    // Budget of one: the walk breaks before touching either shard.
    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(
        body["status"], "complete",
        "no shard was queried, so the read cannot claim to be complete"
    );
}

// ── Retention-summarised lineage (issue #752) ─────────────────────────────

/// Insert a `harvest_execution_summaries` row for a child that was demoted by
/// tiered summary retention and had its execution row collected.
async fn demote_child_to_summary(
    conn: &mut AsyncPgConnection,
    child: ExecutionId,
    parent: ExecutionId,
    state: &str,
) {
    diesel::sql_query(
        "INSERT INTO harvest_execution_summaries \
         (execution_id, workflow_name, workflow_id, state, started_at, completed_at, \
          shard_id, parent_id, summarized_at) \
         VALUES ($1, 'demoted_child', 'demoted-1', $2, NOW(), NOW(), 0, $3, NOW())",
    )
    .bind::<diesel::sql_types::Uuid, _>(child.as_uuid())
    .bind::<diesel::sql_types::Text, _>(state.to_string())
    .bind::<diesel::sql_types::Uuid, _>(parent.as_uuid())
    .execute(conn)
    .await
    .expect("insert summary row");
}

/// A terminal child demoted into a retention summary is invisible to a walk of
/// `harvest_workflow_executions`. The tree must not then claim completeness —
/// otherwise a FAILED child yields `truncated: false` and `counts.failed == 0`,
/// the exact silent omission the bounds machinery exists to prevent.
#[tokio::test]
async fn a_child_demoted_into_a_retention_summary_is_reported_not_silently_dropped() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    let live = insert(&mut conn, Spec::child(root, "live_child", 0)).await;

    // A FAILED child that retention already demoted: summary row only, no
    // execution row.
    let demoted = ExecutionId::new_for_shard(ShardId::new(0));
    demote_child_to_summary(&mut conn, demoted, root, "FAILED").await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);

    // The live child is still returned normally.
    let kids = body["root"]["children"].as_array().expect("children array");
    assert_eq!(kids.len(), 1, "only the live child can be nested: {body}");
    assert_eq!(kids[0]["execution_id"], live.to_string());

    // ...but the omission must be surfaced, naming the live parent to act from.
    let named = body["retained_summary_parent_ids"]
        .as_array()
        .expect("retained_summary_parent_ids must be present");
    assert!(
        named.iter().any(|v| v == &json!(root.to_string())),
        "the root has a retention-summarised child and must be named: {body}"
    );
}

/// `retained_summary_parent_ids` must only ever name nodes the caller can
/// actually find in the tree it was handed — the contract's literal wording.
///
/// The trap: the walk's `visited` set is the cycle/duplicate guard, and it
/// deliberately retains an id that was rejected for budget. Probing that
/// superset would name a node that `max_nodes` dropped, handing the operator
/// an id they cannot locate in the response.
#[tokio::test]
async fn a_budget_dropped_node_is_never_named_as_a_retention_summary_parent() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    // Two children, oldest first so admission order is deterministic.
    let kept = insert(&mut conn, Spec::child(root, "kept", 0)).await;
    let dropped = insert(&mut conn, Spec::child(root, "dropped", 1)).await;

    // The node that will NOT survive the budget is the one with a demoted
    // child, so a `visited`-sourced probe would name it.
    let demoted = ExecutionId::new_for_shard(ShardId::new(0));
    demote_child_to_summary(&mut conn, demoted, dropped, "FAILED").await;

    // Budget for root + exactly one child.
    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?max_nodes=2")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["truncated"], json!(true), "the budget was hit: {body}");

    let kids = body["root"]["children"].as_array().expect("children array");
    assert_eq!(kids.len(), 1, "only one child fits the budget: {body}");
    assert_eq!(kids[0]["execution_id"], kept.to_string());

    let named = body["retained_summary_parent_ids"]
        .as_array()
        .expect("retained_summary_parent_ids must be present");
    assert!(
        !named.iter().any(|v| v == &json!(dropped.to_string())),
        "the budget-dropped node is absent from the tree, so naming it would \
         hand the operator an id they cannot find: {body}"
    );
}

/// The signal must not fire on the default configuration — with summary
/// retention off there are no summary rows, so an ordinary tree stays clean.
#[tokio::test]
async fn a_tree_with_no_summarised_children_reports_none_omitted() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    insert(&mut conn, Spec::child(root, "kid", 0)).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["retained_summary_parent_ids"],
        json!([]),
        "no summaries exist, so nothing may be reported as omitted: {body}"
    );
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(body["status"], json!("complete"));
}

/// A summarised child hanging off a *non-root* node must also be caught — the
/// probe covers every admitted id, not just the unexpanded frontier.
#[tokio::test]
async fn a_summarised_child_of_an_interior_node_is_reported() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    let mid = insert(&mut conn, Spec::child(root, "mid", 0)).await;

    let demoted = ExecutionId::new_for_shard(ShardId::new(0));
    demote_child_to_summary(&mut conn, demoted, mid, "FAILED").await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(status, StatusCode::OK);
    let named = body["retained_summary_parent_ids"]
        .as_array()
        .expect("retained_summary_parent_ids must be present");
    assert!(
        named.iter().any(|v| v == &json!(mid.to_string())),
        "the interior node, not just the root, must be named: {body}"
    );
}

/// The roll-up carries the same signal, so a `--summary` triage pass cannot
/// read `failed: 0` as authoritative when lineage was omitted.
#[tokio::test]
async fn summary_mode_also_reports_retained_summary_parents() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("saga")).await;
    let demoted = ExecutionId::new_for_shard(ShardId::new(0));
    demote_child_to_summary(&mut conn, demoted, root, "FAILED").await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?summary=true")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["counts"]["failed"],
        json!(0),
        "the demoted child is not counted"
    );
    let named = body["retained_summary_parent_ids"]
        .as_array()
        .expect("summary shape must carry the signal too");
    assert!(
        named.iter().any(|v| v == &json!(root.to_string())),
        "a zero failed-count must not read as authoritative: {body}"
    );
}

// ── Root resolution vs. the default-shard fallback ────────────────────────

/// The root lookup must not silently fall back to the default shard.
///
/// `ShardedDbPool::pool_for` falls back to the default shard when the requested
/// shard has no entry, so routing the root off its encoded shard through the
/// lenient `pool_for_execution` would query the **wrong database**, find
/// nothing, and answer a confident `404` — for an execution that may well exist
/// on its own shard. That is the exact false-negative the sibling business-id
/// resolver (#805) fails closed against, and it would contradict this very
/// endpoint's own fan-out, which reports a router-known-but-poolless shard as
/// `unavailable` rather than pretending it is empty.
#[tokio::test]
async fn a_root_on_a_router_known_shard_with_no_pool_is_503_not_a_false_404() {
    let (admin, _guard) = setup_server().await;
    let shard0 = create_shard_db(&admin, &unique("lineage_s0")).await;

    // The router knows shards 0 and 1, but this process only has a pool for 0
    // — the documented mid-shard-add-rollout state (readable_shards widened
    // before the pool is configured).
    let pools = BTreeMap::from([(ShardId::new(0), build_pool(&shard0))]);
    let app = build_app(
        HarvestDbPool::sharded(ShardedDbPool::from_map(pools, ShardId::new(0))),
        router_for(&[0, 1]),
    );

    // An id whose embedded shard is 1 — the shard we cannot query.
    let root = ExecutionId::new_for_shard(ShardId::new(1));

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "the owning shard could not be queried, so existence is unknown — \
         answering 404 would be a false negative; body: {body}"
    );
    let message = body.to_string();
    assert!(
        message.contains('1'),
        "the response must name the shard that could not be queried: {message}"
    );
}

/// ...but the fix must not over-reach into a confusing `503` for an id no shard
/// in this deployment could ever host (an operator pasting a wrong UUID decodes
/// to an arbitrary shard number). Unknown-to-the-router stays a clean `404`.
#[tokio::test]
async fn a_root_on_a_shard_this_deployment_does_not_know_is_still_404() {
    let (admin, _guard) = setup_server().await;
    let (app, _conn) = single_shard_app(&admin).await;

    // Shard 7 is neither a configured pool nor known to the router.
    let root = ExecutionId::new_for_shard(ShardId::new(7));

    let (status, _body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "no shard in this deployment could host that id, so 404 is honest"
    );
}

// ── Cycle safety ─────────────────────────────────────────────────────────

#[tokio::test]
async fn a_parent_id_cycle_terminates_instead_of_recursing_forever() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    // A -> B, then rewrite A.parent_id = B to forge a cycle.
    let a = insert(&mut conn, Spec::root("a")).await;
    let b = insert(&mut conn, Spec::child(a, "b", 1)).await;
    diesel::update(wfx::table.find(a.as_uuid()))
        .set(wfx::parent_id.eq(Some(b.as_uuid())))
        .execute(&mut conn)
        .await
        .expect("forge cycle");

    let (status, body) = get_json(&app, &format!("/workflows/{a}/tree")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a malformed loop must not hang or 500"
    );
    assert_eq!(body["node_count"], 2, "AC: each execution_id appears once");
    assert_eq!(children_of(&body["root"]).len(), 1);
    assert!(
        children_of(&children_of(&body["root"])[0]).is_empty(),
        "the back-edge to the root is dropped"
    );
}

// ── Summary mode ─────────────────────────────────────────────────────────

#[tokio::test]
async fn summary_mode_returns_per_state_descendant_counts() {
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("agg")).await;
    let a = insert(&mut conn, Spec::child(root, "a", 1)).await;
    insert(&mut conn, Spec::child(root, "b", 2)).await;
    insert(&mut conn, Spec::child(root, "c", 3).in_state("FAILED")).await;
    insert(&mut conn, Spec::child(a, "d", 4).in_state("COMPLETED")).await;

    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree?summary=true")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_descendants"], 4);
    assert_eq!(body["counts"]["running"], 2);
    assert_eq!(body["counts"]["failed"], 1);
    assert_eq!(body["counts"]["completed"], 1);
    // AC: every known state key present even at zero.
    assert_eq!(body["counts"]["cancelled"], 0);
    assert_eq!(body["counts"]["timed_out"], 0);
    assert_eq!(body["counts"]["terminated"], 0);
    // The roll-up must NOT transfer the full tree.
    assert!(
        body["root"]["children"].is_null(),
        "summary mode must not embed the tree"
    );
    assert_eq!(body["root"]["execution_id"], root.to_string());
    assert_eq!(body["root"]["state"], "RUNNING");
}

// ── Success metric ───────────────────────────────────────────────────────

#[tokio::test]
async fn success_metric_depth_six_two_hundred_nodes_returns_in_one_call() {
    // Issue #621 success metric: a depth ≤ 6, ≤ 200-execution tree returns
    // complete in ONE request. (The p95 < 500 ms budget is what the batched
    // `parent_id = ANY($1)` frontier query buys; a per-parent walk would be
    // ~200 queries per shard here.)
    let (admin, _guard) = setup_server().await;
    let (app, mut conn) = single_shard_app(&admin).await;

    let root = insert(&mut conn, Spec::root("perf")).await;
    // 6 levels; ~200 nodes total.
    let mut frontier = vec![root];
    let mut total = 1usize;
    let mut offset = 0i64;
    for _ in 0..6 {
        let mut next = Vec::new();
        for parent in &frontier {
            for _ in 0..2 {
                if total >= 200 {
                    break;
                }
                offset += 1;
                let child = insert(
                    &mut conn,
                    Spec::child(*parent, &format!("n{offset}"), offset),
                )
                .await;
                next.push(child);
                total += 1;
            }
        }
        frontier = next;
        if total >= 200 {
            break;
        }
    }

    let started = std::time::Instant::now();
    let (status, body) = get_json(&app, &format!("/workflows/{root}/tree")).await;
    let elapsed = started.elapsed();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["truncated"], false,
        "must fit inside the default bounds"
    );
    assert_eq!(
        body["node_count"].as_u64().expect("node_count"),
        total as u64,
        "the COMPLETE tree is returned in one call"
    );
    // Generous vs. the 500 ms p95 target: this is a debug build on shared CI
    // hardware, so the assertion guards against an O(nodes × shards) regression
    // rather than pinning the production budget.
    assert!(
        elapsed.as_secs() < 5,
        "one call took {elapsed:?}; a per-parent walk regression is the usual cause"
    );
}
