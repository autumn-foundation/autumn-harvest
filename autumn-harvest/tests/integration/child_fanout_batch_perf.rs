#![cfg(feature = "db")]
//! Ledger performance fix: the local awaited-child fan-out insert/event/
//! enqueue N+1 (issue #1589).
//!
//! `persist_all_started_child_workflows`'s local-child loop
//! (`autumn-harvest/src/worker.rs`) used to call, per new local child: one
//! single-row `INSERT INTO harvest_workflow_executions`, one single-row
//! `INSERT INTO harvest_events` (the child's `WorkflowStarted`), and one
//! `queue::enqueue` (one single-row `INSERT INTO harvest_task_queue`). A
//! decision that fanned out to `n` awaited local children cost `3n` round
//! trips to persist.
//!
//! The fix splits local children by whether their own
//! `enforce_quota_admission` call would be a no-op (no declared policy, no
//! active cap, or no resolved key). A child in that shape is batched: one
//! multi-row `INSERT` per table for the whole group, via
//! [`autumn_harvest::store::append_new_execution_started_events_batch`] and
//! [`autumn_harvest::queue::enqueue_batch`] (already built for the sibling
//! `ScheduleActivity` fan-out, PR #1447). A child with an active cap on its
//! own declared policy keeps the original sequential insert-then-admit path
//! unchanged, so `enforce_quota_admission`'s graduated "admit first K,
//! reject the rest" property survives -- proven separately in
//! `quota_enforcement_tests.rs`.
//!
//! Evidence here is `pg_stat_statements` call counts, driven end-to-end
//! through a real `Worker` and a real workflow decision -- not a direct
//! call to an internal helper, since the row-building logic is inlined in
//! `persist_all_started_child_workflows` (matching every other per-child
//! insert call site in this file, none of which are factored out either).
//! This harness follows the same shape as `activity_enqueue_batch_perf.rs`:
//! a fresh, uniquely-named, fully-migrated database per measurement.
//! `pg_stat_statements` is reset immediately before the measured decision
//! and snapshotted immediately after it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::info::WorkflowInfo;
use autumn_harvest::types::{ExecutionId, Priority, ShardId, StartSource};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

use crate::integration_e2e::{build_runtime_worker, build_test_pool, spawn_test_worker};

// ── DB bootstrap (mirrors activity_enqueue_batch_perf.rs) ──────────────────

type DbGuard = Option<ContainerAsync<Postgres>>;

async fn setup_server() -> (String, DbGuard) {
    use testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

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

async fn create_fresh_db(admin_url: &str, name: &str) -> String {
    let mut admin = AsyncPgConnection::establish(admin_url)
        .await
        .expect("connect to admin database");
    let _ = diesel::sql_query(format!("CREATE DATABASE \"{name}\""))
        .execute(&mut admin)
        .await;

    let (prefix, _) = admin_url.rsplit_once('/').expect("url has a db segment");
    let url = format!("{prefix}/{name}");
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to fresh database");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("apply migration bundle");
    drop(conn);
    url
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

// ── pg_stat_statements capture (mirrors activity_enqueue_batch_perf.rs) ────

#[derive(diesel::QueryableByName, Debug)]
struct StatRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    query: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    calls: i64,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn ensure_pg_stat_statements(conn: &mut AsyncPgConnection) {
    let _ = diesel::sql_query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        .execute(conn)
        .await;
}

async fn reset_stats_for_db(conn: &mut AsyncPgConnection, db_name: &str) {
    diesel::sql_query(format!(
        "SELECT pg_stat_statements_reset(0, \
                (SELECT oid FROM pg_database WHERE datname = '{db_name}'), 0)"
    ))
    .execute(conn)
    .await
    .expect(
        "pg_stat_statements_reset(...) failed -- the HARVEST_TEST_DATABASE_URL role must be \
         able to reset statistics (superuser, or granted EXECUTE on this function)",
    );
}

async fn snapshot_statements(conn: &mut AsyncPgConnection, db_name: &str) -> Vec<StatRow> {
    diesel::sql_query(format!(
        "SELECT query, calls \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '{db_name}') \
           AND query NOT ILIKE '%pg_stat_statements%'"
    ))
    .load(conn)
    .await
    .expect(
        "pg_stat_statements query failed -- it must be preloaded via shared_preload_libraries \
         for this capture to produce real evidence rather than fail outright",
    )
}

/// Total `calls` across every statement whose normalized text is an
/// `INSERT INTO` (optionally quoted) the given table.
fn insert_calls_for_table(rows: &[StatRow], table: &str) -> i64 {
    let needle_a = format!("insert into {table}");
    let needle_b = format!("insert into \"{table}");
    rows.iter()
        .filter(|r| {
            let q = r.query.to_ascii_lowercase();
            q.contains(&needle_a) || q.contains(&needle_b)
        })
        .map(|r| r.calls)
        .sum()
}

// ── Worker/registry plumbing ────────────────────────────────────────────────

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    name: &str,
    id: &str,
    input: Value,
) -> ExecutionId {
    start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name: name,
            workflow_id: id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input,
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
            start_source: StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        None,
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

async fn get_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    use autumn_harvest::schema::harvest_workflow_executions;
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq(exec_id.as_uuid()))
        .select(harvest_workflow_executions::state)
        .first::<String>(conn)
        .await
        .expect("execution must exist")
}

async fn wait_for_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId, states: &[&str]) {
    for _ in 0..300 {
        let state = get_state(conn, exec_id).await;
        if states.contains(&state.as_str()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let state = get_state(conn, exec_id).await;
    panic!("execution {exec_id} never reached {states:?}; current state: {state}");
}

/// Poll until `exec_id`'s workflow task row is parked (claimed and released:
/// `RUNNING` with no `worker_id`) -- the state a decision cycle leaves
/// behind once it has persisted and suspended.
async fn wait_for_workflow_task_parked(conn: &mut AsyncPgConnection, exec_id: ExecutionId) {
    use autumn_harvest::schema::harvest_task_queue;

    for _ in 0..300 {
        if let Some((state, worker_id)) = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::task_type.eq("workflow"))
            .select((harvest_task_queue::state, harvest_task_queue::worker_id))
            .first::<(String, Option<String>)>(conn)
            .await
            .optional()
            .expect("workflow task query failed")
            && state == "RUNNING"
            && worker_id.is_none()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("workflow task for {exec_id} never parked");
}

fn wf_info(name: &'static str, handler: autumn_harvest::info::WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "test",
        handler,
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
    }
}

fn registry(infos: Vec<WorkflowInfo>) -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(infos, vec![]))
}

// ── Fixture workflows ───────────────────────────────────────────────────────

const FAN_OUT_N: usize = 12;

fn fan_out_parent<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_type = input["child_type"]
            .as_str()
            .expect("input.child_type must be present")
            .to_string();
        let children: Vec<(String, Value)> = (0..FAN_OUT_N)
            .map(|i| (child_type.clone(), json!({ "i": i })))
            .collect();
        let results = ctx
            .spawn_child_workflow_fan_out_raw(children)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

/// Never completes: the child's own first decision cycle suspends on a
/// signal that is never sent. This keeps every child parked in `RUNNING`
/// once spawned, so nothing beyond the parent's ONE fan-out decision
/// touches `harvest_events`/`harvest_workflow_executions`/
/// `harvest_task_queue` during the measurement window below -- a child
/// completing (or the parent reacting to that completion) would add its
/// own, unrelated `INSERT`s and mask the signal this test is isolating.
fn fan_out_child<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let _ = ctx.wait_for_signal("never_sent").await;
        Ok(Value::Null)
    })
}

/// End-to-end: a real workflow decision fanning out to
/// [`FAN_OUT_N`] awaited local children of a workflow type with NO declared
/// quota policy must persist that decision with O(1) `INSERT` calls per
/// table, not `O(FAN_OUT_N)`. This is the exact shape issue #1589 measured
/// (3 calls per child, unbatched) driven through the real
/// `persist_all_started_child_workflows` code path, not a direct call to an
/// internal helper.
#[tokio::test]
async fn awaited_local_child_fan_out_persists_with_one_insert_per_table() {
    let (admin, _guard) = setup_server().await;
    let db_name = unique("child_fanout_batch_perf");
    let url = create_fresh_db(&admin, &db_name).await;

    let parent_wf = "fan_out_batch_perf_parent";
    let child_wf = "fan_out_batch_perf_child";
    let reg = registry(vec![
        wf_info(parent_wf, fan_out_parent),
        wf_info(child_wf, fan_out_child),
    ]);

    let mut start_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("start connection");
    ensure_pg_stat_statements(&mut start_conn).await;
    let parent_id = start_workflow(
        &mut start_conn,
        parent_wf,
        &format!("parent-{}", uuid::Uuid::new_v4().simple()),
        json!({"child_type": child_wf}),
    )
    .await;

    let mut stats_conn = AsyncPgConnection::establish(&url)
        .await
        .expect("stats connection");
    reset_stats_for_db(&mut stats_conn, &db_name).await;

    let worker = build_runtime_worker("w-1589-fanout-batch-perf", 4, 1, Arc::clone(&reg));
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));

    // The parent's ONE decision cycle spawns all FAN_OUT_N children, then
    // parks waiting on them. Every child immediately suspends on a signal
    // that never arrives (see `fan_out_child`), so once the parent's task
    // is parked, nothing further will touch these three tables -- the
    // snapshot below captures exactly the one spawn decision, not any
    // later child-completion traffic.
    wait_for_workflow_task_parked(&mut start_conn, parent_id).await;
    assert_eq!(
        get_state(&mut start_conn, parent_id).await,
        "RUNNING",
        "the parent must be parked (RUNNING, awaiting its children), not terminal"
    );

    let rows = snapshot_statements(&mut stats_conn, &db_name).await;
    let workflow_execution_inserts = insert_calls_for_table(&rows, "harvest_workflow_executions");
    let event_inserts = insert_calls_for_table(&rows, "harvest_events");
    let task_queue_inserts = insert_calls_for_table(&rows, "harvest_task_queue");

    worker.shutdown();
    handle.await.expect("worker join");

    // The two tables issue #1589 targets are each covered by exactly ONE
    // multi-row `INSERT` for the whole FAN_OUT_N-wide group -- not one per
    // child, and not merely "fewer than N".
    assert_eq!(
        workflow_execution_inserts, 1,
        "harvest_workflow_executions INSERT calls must be exactly 1 for the whole \
         {FAN_OUT_N}-child batch, not one per child"
    );
    assert_eq!(
        task_queue_inserts, 1,
        "harvest_task_queue INSERT calls must be exactly 1 for the whole {FAN_OUT_N}-child \
         batch, not one per child"
    );
    // harvest_events carries both the batched insert this issue targets
    // (the children's own WorkflowStarted rows, 1 call) AND the PARENT's
    // own per-event append loop (the fan_out marker + one
    // ChildWorkflowStarted per child, `FAN_OUT_N + 1` separate calls) --
    // deliberately out of scope for issue #1589 (see its own text: scoped
    // to the "Insert rows and enqueue tasks for new children" section, not
    // the parent's own history append, which re-reads `MAX(event_id) FOR
    // UPDATE` per event to serialize against concurrent sibling
    // completions). The bound below is exact given that shape, proving the
    // children's own batch adds exactly one call on top of it.
    let fan_out_n_i64 = i64::try_from(FAN_OUT_N).expect("FAN_OUT_N fits in i64");
    assert_eq!(
        event_inserts,
        fan_out_n_i64 + 2,
        "harvest_events INSERT calls must be exactly the parent's own per-event appends \
         ({FAN_OUT_N} ChildWorkflowStarted + 1 marker) plus exactly ONE batched call for \
         all {FAN_OUT_N} children's own WorkflowStarted rows"
    );

    // Functional correctness alongside the call-count evidence: every
    // child actually exists, actually started, and is durably parked.
    let child_count: CountRow = diesel::sql_query(
        "SELECT COUNT(*)::BIGINT AS n FROM harvest_workflow_executions \
         WHERE workflow_name = $1 AND state = 'RUNNING'",
    )
    .bind::<diesel::sql_types::Text, _>(child_wf)
    .get_result(&mut start_conn)
    .await
    .expect("count running children");
    assert_eq!(
        child_count.n, fan_out_n_i64,
        "every one of the {FAN_OUT_N} batched children must exist and be RUNNING"
    );
}
