#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Type-safe workflow client stubs and handles integration tests (issue #341).

use serde::{Deserialize, Serialize};
use std::time::Duration;

use autumn_harvest::error::HarvestError;
use autumn_harvest::worker::DbPool;
use autumn_harvest::{
    TypedSignalWithStartOptions, TypedStartOptions, WorkflowHandleClient, WorkflowResultState,
};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use autumn_harvest::prelude::*;

const INIT_SQL: &str = concat!(
    include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
    "\n",
    include_str!("../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
    "\n",
    include_str!("../migrations/20260424000000_harvest_sticky_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
    "\n",
    include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
    "\n",
    include_str!("../migrations/20260428000000_harvest_retention_scan_index/up.sql"),
    "\n",
    include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
    "\n",
    include_str!("../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
    "\n",
    include_str!("../migrations/20260430000001_harvest_external_tasks/up.sql"),
    "\n",
    include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
    "\n",
    include_str!("../migrations/20260501010000_harvest_batch_jobs/up.sql"),
    "\n",
    include_str!("../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
    "\n",
    include_str!("../migrations/20260503000000_harvest_workflow_reset/up.sql"),
    "\n",
    include_str!("../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
    "\n",
    include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
    "\n",
    include_str!("../migrations/20260506000000_harvest_audit_log/up.sql"),
    "\n",
    include_str!("../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
    "\n",
    include_str!("../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
    "\n",
    include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
    "\n",
    include_str!("../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260514020000_harvest_task_activity_id/up.sql"),
    "\n",
    include_str!("../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
    "\n",
    include_str!("../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
    "\n",
    include_str!("../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
    "\n",
    include_str!("../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
    "\n",
    include_str!("../migrations/20260522000001_harvest_rate_limiting/up.sql"),
    "\n",
    include_str!("../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
    "\n",
    include_str!("../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
    "\n",
    include_str!("../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
    "\n",
    include_str!("../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
    "\n",
    include_str!("../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
    "\n",
    include_str!("../migrations/20260603000000_harvest_completion_triggers/up.sql"),
    include_str!("../migrations/20260605000000_harvest_admission_gates/up.sql"),
    include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql")
);

async fn setup_database_url() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_init_sql(INIT_SQL.to_string().into_bytes())
        .start()
        .await
        .expect("failed to start Postgres container");

    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("container postgres port");
    let database_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    (database_url, container)
}

fn build_pool(database_url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
    deadpool::managed::Pool::builder(manager)
        .max_size(4)
        .build()
        .expect("test pool should build")
}

// ── Test Workflow Declaration ───────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct OnboardingInput {
    user_id: i64,
}

#[workflow]
async fn onboarding(ctx: &WorkflowContext, input: OnboardingInput) -> Result<String, String> {
    // Wait for the signal
    let val = ctx
        .wait_for_signal("subscription_active")
        .await
        .map_err(|e| e.to_string())?;
    let added: i64 = serde_json::from_value(val).map_err(|e| e.to_string())?;
    Ok(format!(
        "user {}: subscription active with value {}",
        input.user_id, added
    ))
}

#[query(workflow = "onboarding")]
fn current_progress(_ctx: &WorkflowContext) -> Result<i64, String> {
    Ok(100)
}

#[update(workflow = "onboarding")]
async fn check_progress(_ctx: &WorkflowContext, step: i64) -> Result<String, String> {
    Ok(format!("progress checked at {step}"))
}

#[allow(dead_code)]
#[signal(workflow = "onboarding")]
fn subscription_active(_ctx: &WorkflowContext, _val: i64) {}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_typed_workflow_client_stubs() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    let workflow_id = "onboarding-user-1";
    let input = OnboardingInput { user_id: 1337 };

    // 1. Start the workflow using the generated stub
    let handle = OnboardingStub::start(&mut conn, &client, workflow_id, input.clone())
        .await
        .unwrap();

    assert_eq!(handle.exec_id(), handle.inner().exec_id());

    // Verify snapshot shows Running state
    let snap = handle.result_snapshot().await.unwrap();
    assert_eq!(snap.state, WorkflowResultState::Running);
    assert!(snap.output.is_none());

    // 2. Query in process via generated typed method
    let query_res = OnboardingStub::query_current_progress(handle.inner())
        .await
        .unwrap();
    assert_eq!(query_res, 100);

    // 3. Send type-safe signal using generated stub method
    OnboardingStub::signal_subscription_active(&mut conn, handle.inner(), 42)
        .await
        .unwrap();

    // Since we don't have a worker executing, let's manually write the workflow completed event to database
    // to simulate the worker completing it, so we can await result().
    use autumn_harvest::WorkflowEvent;
    use autumn_harvest::schema::harvest_workflow_executions::dsl;

    let exec_id = handle.exec_id();
    let completed_output = serde_json::json!("user 1337: subscription active with value 42");

    conn.transaction::<(), HarvestError, _>(|conn| {
        let completed_output = completed_output.clone();
        Box::pin(async move {
            autumn_harvest::store::append_events(
                conn,
                exec_id,
                &[WorkflowEvent::WorkflowCompleted {
                    output: completed_output.clone(),
                }],
                1,
            )
            .await?;

            diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
                .set((dsl::state.eq("COMPLETED"), dsl::output.eq(completed_output)))
                .execute(conn)
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    // 4. Await result and check typed return value
    let result = handle.result().await.unwrap();
    assert_eq!(result, "user 1337: subscription active with value 42");

    // Check completed snapshot
    let final_snap = handle.result_snapshot().await.unwrap();
    assert_eq!(final_snap.state, WorkflowResultState::Completed);
    assert_eq!(
        final_snap.output.unwrap(),
        "user 1337: subscription active with value 42"
    );
}

#[tokio::test]
async fn test_typed_workflow_client_stubs_with_options() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    let workflow_id = "onboarding-user-2";
    let input = OnboardingInput { user_id: 1338 };
    let opts = TypedStartOptions {
        execution_timeout: Some(Duration::from_secs(3600)),
        ..Default::default()
    };

    let handle = OnboardingStub::start_with_options(&mut conn, &client, workflow_id, input, opts)
        .await
        .unwrap();

    assert_eq!(handle.exec_id(), handle.inner().exec_id());
}

#[tokio::test]
async fn test_typed_workflow_client_stubs_signal_with_start() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    let workflow_id = "onboarding-user-3";
    let input = OnboardingInput { user_id: 1339 };
    let opts = TypedSignalWithStartOptions::default();

    let handle = OnboardingStub::signal_with_start(
        &mut conn,
        &client,
        workflow_id,
        input,
        "subscription_active",
        42,
        opts,
    )
    .await
    .unwrap();

    assert_eq!(handle.exec_id(), handle.inner().exec_id());
}

#[tokio::test]
async fn test_typed_workflow_client_stubs_invalid_timeout() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    let workflow_id = "onboarding-user-4";
    let input = OnboardingInput { user_id: 1340 };
    // Pass overflowed execution_timeout
    let opts = TypedStartOptions {
        execution_timeout: Some(Duration::from_secs(u64::MAX)),
        ..Default::default()
    };

    let err = OnboardingStub::start_with_options(&mut conn, &client, workflow_id, input, opts)
        .await
        .unwrap_err();

    assert!(
        matches!(err, autumn_harvest::error::HarvestError::Config(_)),
        "expected HarvestError::Config due to duration overflow, got: {:?}",
        err
    );
}

#[tokio::test]
async fn test_typed_workflow_client_stubs_signal_with_start_payload_cap() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    let workflow_id = "onboarding-user-5";
    let input = OnboardingInput { user_id: 1341 };

    // Set max_signal_payload_bytes to 1 byte, and pass payload: 99999 (5 bytes)
    let opts = TypedSignalWithStartOptions {
        max_signal_payload_bytes: Some(1),
        ..Default::default()
    };

    let err = OnboardingStub::signal_with_start(
        &mut conn,
        &client,
        workflow_id,
        input,
        "subscription_active",
        99999,
        opts,
    )
    .await
    .unwrap_err();

    assert!(
        matches!(
            err,
            autumn_harvest::error::HarvestError::PayloadTooLarge { .. }
        ),
        "expected HarvestError::PayloadTooLarge due to signal payload size enforcement, got: {:?}",
        err
    );
}

// ── Dummy Workflow for Mismatch Testing ──────────────────────────────────────

#[workflow]
async fn dummy_other_workflow(_ctx: &WorkflowContext, input: String) -> Result<String, String> {
    Ok(input)
}

#[query(workflow = "dummy_other_workflow")]
fn dummy_query(_ctx: &WorkflowContext) -> Result<i64, String> {
    Ok(99)
}

#[update(workflow = "dummy_other_workflow")]
async fn dummy_update(_ctx: &WorkflowContext) -> Result<String, String> {
    Ok("ok".to_string())
}

#[allow(dead_code)]
#[signal(workflow = "dummy_other_workflow")]
fn dummy_signal(_ctx: &WorkflowContext) {}

#[tokio::test]
async fn test_typed_workflow_client_stubs_mismatch_validation() {
    let (db_url, _container) = setup_database_url().await;
    let pool = build_pool(&db_url);
    let mut conn = pool.get().await.unwrap();

    let client = WorkflowHandleClient::single(pool, db_url.clone());

    // 1. Start onboarding workflow
    let onboarding_handle = OnboardingStub::start(
        &mut conn,
        &client,
        "onboarding-user-mismatch",
        OnboardingInput { user_id: 9999 },
    )
    .await
    .unwrap();

    // 2. Query mismatch: call DummyOtherWorkflowStub::query_dummy_query with onboarding_handle
    let query_err = DummyOtherWorkflowStub::query_dummy_query(onboarding_handle.inner())
        .await
        .unwrap_err();

    assert!(
        matches!(query_err, autumn_harvest::error::HarvestError::Config(_)),
        "expected HarvestError::Config due to query workflow type mismatch, got: {:?}",
        query_err
    );
    assert!(
        query_err.to_string().contains("workflow type mismatch"),
        "expected mismatch error message, got: {}",
        query_err
    );

    // 3. Update mismatch: call DummyOtherWorkflowStub::update_dummy_update with onboarding_handle
    let update_err =
        DummyOtherWorkflowStub::update_dummy_update(&mut conn, onboarding_handle.inner())
            .await
            .unwrap_err();

    assert!(
        matches!(update_err, autumn_harvest::error::HarvestError::Config(_)),
        "expected HarvestError::Config due to update workflow type mismatch, got: {:?}",
        update_err
    );
    assert!(
        update_err.to_string().contains("workflow type mismatch"),
        "expected mismatch error message, got: {}",
        update_err
    );

    // 4. Signal mismatch: call DummyOtherWorkflowStub::signal_dummy_signal with onboarding_handle
    let signal_err =
        DummyOtherWorkflowStub::signal_dummy_signal(&mut conn, onboarding_handle.inner())
            .await
            .unwrap_err();

    assert!(
        matches!(signal_err, autumn_harvest::error::HarvestError::Config(_)),
        "expected HarvestError::Config due to signal workflow type mismatch, got: {:?}",
        signal_err
    );
    assert!(
        signal_err.to_string().contains("workflow type mismatch"),
        "expected mismatch error message, got: {}",
        signal_err
    );
}
