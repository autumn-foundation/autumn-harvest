//! A chain in flight keeps its chain deadline (issue #2406, AC1).
//!
//! An operator override of `chain_execution_timeout` applies to the next chain
//! that starts. A continue-as-new successor copies the stored
//! `chain_deadline_at` of its predecessor, for a same-type and a cross-type
//! successor alike. These tests store an override, run a worker through one
//! continue-as-new, and read the successor row.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::run_defaults::{
    RunDefaultChange, RunDefaultPatch, update_run_default_override,
};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::types::{ExecutionId, WorkflowIdReusePolicy};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{WorkflowContext, WorkflowInfo};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, load_history_from_url, setup_test_database_url_or_env,
    spawn_test_worker, wait_for_execution_state,
};

const HOUR: Duration = Duration::from_hours(1);

/// Give each test its own type names. The worker registry is global.
fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

/// Continue as the type that the input names, once.
fn continue_once<'a>(
    ctx: &'a WorkflowContext,
    input: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>> {
    Box::pin(async move {
        if input.get("continued").is_some() {
            return Ok(serde_json::json!({ "done": true }));
        }
        let next = serde_json::json!({ "continued": true });
        match input.get("next_type").and_then(serde_json::Value::as_str) {
            Some(target) => ctx.continue_as_new_as_type(target, next).await,
            None => ctx.continue_as_new(next).await,
        }
        .map_err(|e| e.to_string())?;
        unreachable!("continue_as_new does not resolve");
    })
}

fn wf(name: &'static str, handler: WorkflowHandlerFn) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "run_defaults_chain_tests",
        handler,
        execution_timeout: None,
        chain_execution_timeout: Some(HOUR),
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

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("connect")
}

/// Store an override of the chain cap of `name`.
async fn override_chain(conn: &mut AsyncPgConnection, name: &str, value: Duration) {
    let change = RunDefaultChange {
        workflow_name: name,
        patch: RunDefaultPatch {
            chain_execution_timeout: Some(Some(value)),
            ..RunDefaultPatch::default()
        },
        clear: false,
        actor: "ops",
        reason: Some("issue 2406"),
    };
    update_run_default_override(conn, &change)
        .await
        .expect("store override");
}

/// Start a root with the chain cap that the registry resolves for its type.
async fn start_root(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    name: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let workflow_id = format!("chain-{}", Uuid::new_v4().simple());
    let params = StartWorkflowParams {
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        chain_execution_timeout: registry
            .chain_execution_timeout(name)
            .and_then(|d| chrono::Duration::from_std(d).ok()),
        ..StartWorkflowParams::new(name, &workflow_id, ExecutionId::new(), input, "default")
    };
    start_or_load_workflow_execution(conn, params, None)
        .await
        .expect("start root")
        .exec_id
}

async fn load(conn: &mut AsyncPgConnection, id: ExecutionId) -> WorkflowExecution {
    harvest_workflow_executions::table
        .find(id.as_uuid())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .expect("load execution")
}

/// Load the stored overrides into the cache of `registry`, and check that it
/// holds `expected` for `name`.
///
/// A worker refreshes the cache in the background. A successor could seal
/// before that refresh lands. So the test loads the cache first. A successor
/// that read the override would then take it.
async fn load_cache(
    conn: &mut AsyncPgConnection,
    registry: &HandlerRegistry,
    name: &str,
    expected: Duration,
) {
    registry
        .run_defaults()
        .refresh(conn)
        .await
        .expect("load the overrides");
    assert_eq!(registry.chain_execution_timeout(name), Some(expected));
}

/// Run a worker until `root` continues, then return its successor.
async fn successor_of(url: &str, root: ExecutionId, registry: Arc<HandlerRegistry>) -> ExecutionId {
    let worker = build_runtime_worker("w-2406", 2, 1, registry);
    let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(url));
    let _sealed = wait_for_execution_state(url, root, "CONTINUED_AS_NEW").await;
    worker.shutdown();
    handle.await.expect("worker join");
    load_history_from_url(url, root)
        .await
        .events
        .iter()
        .find_map(|event| match event {
            autumn_harvest::event::WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id, ..
            } => Some(*new_exec_id),
            _ => None,
        })
        .expect("the root records its successor")
}

/// Assert that `successor` keeps the chain cap and deadline of `root`.
async fn assert_keeps_the_chain(
    conn: &mut AsyncPgConnection,
    root: ExecutionId,
    successor: ExecutionId,
) {
    let before = load(conn, root).await;
    let after = load(conn, successor).await;
    assert_eq!(
        before.chain_execution_timeout,
        chrono::Duration::from_std(HOUR).ok(),
        "the root took the code value"
    );
    assert_eq!(
        after.chain_execution_timeout,
        before.chain_execution_timeout
    );
    assert_eq!(
        after.chain_deadline_at, before.chain_deadline_at,
        "a chain in flight keeps its stored deadline"
    );
}

/// AC1: a same-type successor keeps the stored chain deadline. The override
/// that a later start takes does not reach it.
#[tokio::test]
async fn a_same_type_successor_keeps_the_stored_chain_deadline() {
    // The DB tests of the same table clear it on a shared database.
    let _shared = crate::run_defaults_db_tests::SHARED_DB.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let name = leaked("chain_same");
    let registry = Arc::new(HandlerRegistry::new(vec![wf(name, continue_once)], vec![]));

    let root = start_root(&mut conn, &registry, name, serde_json::json!({})).await;
    override_chain(&mut conn, name, 3 * HOUR).await;
    load_cache(&mut conn, &registry, name, 3 * HOUR).await;
    let successor = successor_of(&url, root, Arc::clone(&registry)).await;
    assert_keeps_the_chain(&mut conn, root, successor).await;

    // The next chain that starts takes the override.
    let next = start_root(&mut conn, &registry, name, serde_json::json!({})).await;
    assert_eq!(
        load(&mut conn, next).await.chain_execution_timeout,
        chrono::Duration::from_std(3 * HOUR).ok()
    );
}

/// AC1: a cross-type successor keeps the stored chain deadline, even when its
/// target type has an override.
#[tokio::test]
async fn a_cross_type_successor_keeps_the_stored_chain_deadline() {
    // The DB tests of the same table clear it on a shared database.
    let _shared = crate::run_defaults_db_tests::SHARED_DB.lock().await;
    let (url, _c) = setup_test_database_url_or_env().await;
    let mut conn = connect(&url).await;
    let from = leaked("chain_from");
    let to = leaked("chain_to");
    let registry = Arc::new(HandlerRegistry::new(
        vec![wf(from, continue_once), wf(to, continue_once)],
        vec![],
    ));

    let root = start_root(
        &mut conn,
        &registry,
        from,
        serde_json::json!({ "next_type": to }),
    )
    .await;
    override_chain(&mut conn, to, 3 * HOUR).await;
    load_cache(&mut conn, &registry, to, 3 * HOUR).await;
    let successor = successor_of(&url, root, registry).await;
    assert_eq!(load(&mut conn, successor).await.workflow_name, to);
    assert_keeps_the_chain(&mut conn, root, successor).await;
}
