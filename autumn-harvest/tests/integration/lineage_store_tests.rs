#![cfg(feature = "db")]
//! Core SQL coverage for the cross-shard lineage batch loaders (issue #621).
//!
//! The recursive lineage walk fans out level-by-level, so the per-level query
//! must accept the WHOLE frontier at once (`parent_id = ANY($1)`) rather than
//! one query per parent — otherwise a 200-node tree across 8 shards costs
//! ~1600 round trips and blows the p95 < 500 ms success metric.
//!
//! Two helpers are covered:
//!
//! - [`load_workflow_children_batch`] — the frontier fetch. Deterministic
//!   `started_at ASC, id ASC` ordering so a `max_nodes` truncation always drops
//!   the same rows, and a `limit` so a pathological fan-out cannot pull an
//!   unbounded result set into memory.
//! - [`load_parents_with_children`] — the bounded existence probe that lets the
//!   walk name *precisely* which leaf nodes have dropped subtrees, instead of
//!   conservatively naming every unexpanded leaf (issue #621: "no silent cap —
//!   the dropped subtrees are named by their parent `execution_id`").
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly; otherwise a fresh testcontainers Postgres is booted with
//! the full migration bundle.

use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::store::{AwaitMode, load_parents_with_children, load_workflow_children_batch};
use autumn_harvest::types::{ExecutionId, ShardId};
use chrono::{Duration, Utc};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn init_sql() -> Vec<u8> {
    autumn_harvest::full_migrations_sql().as_bytes().to_vec()
}

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(init_sql())
        .with_tag("16")
        .start()
        .await
        .expect("failed to start Postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (url, Some(container))
}

struct InsertSpec<'a> {
    workflow_name: &'a str,
    workflow_id: &'a str,
    state: &'a str,
    parent_id: Option<ExecutionId>,
    parent_close_policy: Option<&'a str>,
    started_at_offset_secs: i64,
}

async fn insert(conn: &mut AsyncPgConnection, spec: InsertSpec<'_>) -> ExecutionId {
    use autumn_harvest::schema::harvest_workflow_executions as t;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        continued_from_exec_id: None,
        first_exec_id: None,
        id: exec_id.as_uuid(),
        workflow_name: spec.workflow_name,
        workflow_id: spec.workflow_id,
        run_id: uuid::Uuid::new_v4(),
        shard_id: 0,
        input: json!({}),
        parent_id: spec.parent_id.map(|id| id.as_uuid()),
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
    diesel::insert_into(t::table)
        .values(&row)
        .execute(conn)
        .await
        .expect("insert execution");

    let started_at = Utc::now() + Duration::seconds(spec.started_at_offset_secs);
    let completed_at = if spec.state == "RUNNING" {
        None
    } else {
        Some(started_at + Duration::seconds(1))
    };
    diesel::update(t::table.find(exec_id.as_uuid()))
        .set((
            t::state.eq(spec.state),
            t::started_at.eq(started_at),
            t::completed_at.eq(completed_at),
        ))
        .execute(conn)
        .await
        .expect("set state");
    exec_id
}

const fn child_of(parent: ExecutionId, workflow_id: &str, offset: i64) -> InsertSpec<'_> {
    InsertSpec {
        workflow_name: "child_wf",
        workflow_id,
        state: "RUNNING",
        parent_id: Some(parent),
        parent_close_policy: None,
        started_at_offset_secs: offset,
    }
}

const fn root_spec(workflow_id: &str) -> InsertSpec<'_> {
    InsertSpec {
        workflow_name: "root_wf",
        workflow_id,
        state: "RUNNING",
        parent_id: None,
        parent_close_policy: None,
        started_at_offset_secs: 0,
    }
}

/// The frontier fetch must accept MANY parents in one query — that batching is
/// the whole reason the recursive walk is O(depth × shards) instead of
/// O(nodes × shards).
#[tokio::test]
async fn batch_loader_returns_children_of_every_frontier_parent_in_one_query() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let suffix = uuid::Uuid::new_v4();
    let a = insert(&mut conn, root_spec(&format!("a-{suffix}"))).await;
    let b = insert(&mut conn, root_spec(&format!("b-{suffix}"))).await;
    let a1 = insert(&mut conn, child_of(a, &format!("a1-{suffix}"), 1)).await;
    let b1 = insert(&mut conn, child_of(b, &format!("b1-{suffix}"), 2)).await;

    let rows = load_workflow_children_batch(&mut conn, &[a.as_uuid(), b.as_uuid()], 100)
        .await
        .expect("batch load");

    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.exec_id.as_uuid()).collect();
    assert!(ids.contains(&a1.as_uuid()), "a's child must be returned");
    assert!(ids.contains(&b1.as_uuid()), "b's child must be returned");
    assert_eq!(ids.len(), 2, "exactly the two children, no extras");

    // Each row must carry its OWN parent so the flat rows can be nested.
    let a1_row = rows
        .iter()
        .find(|r| r.exec_id == a1)
        .expect("a1 row present");
    assert_eq!(a1_row.parent_id, Some(a), "row must name its parent");
    // AC: each node carries workflow_id (the business key), not just the name.
    assert_eq!(a1_row.workflow_id, format!("a1-{suffix}"));
    assert_eq!(a1_row.workflow_name, "child_wf");
    assert_eq!(a1_row.state, "RUNNING");
    assert_eq!(a1_row.shard_id, 0);
}

/// An empty frontier must not hit the database at all (the walk calls this
/// once per shard per level and a level can legitimately be empty).
#[tokio::test]
async fn batch_loader_returns_empty_without_querying_for_empty_frontier() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let rows = load_workflow_children_batch(&mut conn, &[], 100)
        .await
        .expect("empty frontier");
    assert!(rows.is_empty());
}

/// Deterministic ordering is load-bearing: a `max_nodes` truncation must drop
/// the SAME rows on every call, so the tree an operator sees is stable across
/// retries.
#[tokio::test]
async fn batch_loader_orders_oldest_first_for_deterministic_truncation() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let suffix = uuid::Uuid::new_v4();
    let parent = insert(&mut conn, root_spec(&format!("p-{suffix}"))).await;
    // Insert newest-first so insertion order cannot be mistaken for sort order.
    let newest = insert(&mut conn, child_of(parent, &format!("c3-{suffix}"), 30)).await;
    let oldest = insert(&mut conn, child_of(parent, &format!("c1-{suffix}"), 10)).await;
    let middle = insert(&mut conn, child_of(parent, &format!("c2-{suffix}"), 20)).await;

    let rows = load_workflow_children_batch(&mut conn, &[parent.as_uuid()], 100)
        .await
        .expect("load");
    let ids: Vec<ExecutionId> = rows.iter().map(|r| r.exec_id).collect();
    assert_eq!(ids, vec![oldest, middle, newest], "oldest-first ordering");

    // And the limit must cut from the TAIL of that deterministic order.
    let limited = load_workflow_children_batch(&mut conn, &[parent.as_uuid()], 2)
        .await
        .expect("load limited");
    let limited_ids: Vec<ExecutionId> = limited.iter().map(|r| r.exec_id).collect();
    assert_eq!(limited_ids, vec![oldest, middle], "limit keeps the oldest");
}

/// The spawn relationship (AC: "suspended-parent vs detached child + its
/// `parent_close_policy` when detached") must be readable straight off the row.
#[tokio::test]
async fn batch_loader_reports_await_mode_and_parent_close_policy() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let suffix = uuid::Uuid::new_v4();
    let parent = insert(&mut conn, root_spec(&format!("p-{suffix}"))).await;
    let awaited = insert(&mut conn, child_of(parent, &format!("aw-{suffix}"), 1)).await;
    let detached = insert(
        &mut conn,
        InsertSpec {
            workflow_name: "child_wf",
            workflow_id: &format!("de-{suffix}"),
            state: "RUNNING",
            parent_id: Some(parent),
            parent_close_policy: Some("request_cancel"),
            started_at_offset_secs: 2,
        },
    )
    .await;

    let rows = load_workflow_children_batch(&mut conn, &[parent.as_uuid()], 100)
        .await
        .expect("load");

    let aw = rows.iter().find(|r| r.exec_id == awaited).expect("awaited");
    assert_eq!(aw.await_mode, AwaitMode::Awaited);
    assert!(aw.parent_close_policy.is_none());

    let de = rows
        .iter()
        .find(|r| r.exec_id == detached)
        .expect("detached");
    assert_eq!(de.await_mode, AwaitMode::Detached);
    assert!(
        de.parent_close_policy.is_some(),
        "detached child must surface its parent-close policy"
    );
}

/// The existence probe answers "which of these leaves actually have children?"
/// so a `max_depth`/`max_nodes` truncation names ONLY parents whose subtrees
/// were genuinely dropped — never a childless leaf (a false positive would send
/// an operator chasing a subtree that does not exist).
#[tokio::test]
async fn existence_probe_names_only_parents_that_actually_have_children() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let suffix = uuid::Uuid::new_v4();
    let has_kids = insert(&mut conn, root_spec(&format!("hk-{suffix}"))).await;
    let childless = insert(&mut conn, root_spec(&format!("cl-{suffix}"))).await;
    insert(&mut conn, child_of(has_kids, &format!("k-{suffix}"), 1)).await;

    let parents = load_parents_with_children(&mut conn, &[has_kids.as_uuid(), childless.as_uuid()])
        .await
        .expect("probe");

    assert!(
        parents.contains(&has_kids.as_uuid()),
        "a parent with children must be named"
    );
    assert!(
        !parents.contains(&childless.as_uuid()),
        "a childless leaf must NOT be named as a dropped subtree"
    );
}

/// The probe must be DISTINCT — a parent with 500 children yields one id, not
/// 500 — so naming dropped subtrees stays bounded by frontier size.
#[tokio::test]
async fn existence_probe_is_distinct_per_parent() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");

    let suffix = uuid::Uuid::new_v4();
    let parent = insert(&mut conn, root_spec(&format!("p-{suffix}"))).await;
    for i in 0..5 {
        insert(&mut conn, child_of(parent, &format!("c{i}-{suffix}"), i)).await;
    }

    let parents = load_parents_with_children(&mut conn, &[parent.as_uuid()])
        .await
        .expect("probe");
    assert_eq!(
        parents.iter().filter(|p| **p == parent.as_uuid()).count(),
        1,
        "one id per parent regardless of child count"
    );
}

#[tokio::test]
async fn existence_probe_returns_empty_for_empty_input() {
    let (url, _guard) = setup_db().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let parents = load_parents_with_children(&mut conn, &[])
        .await
        .expect("probe");
    assert!(parents.is_empty());
}
