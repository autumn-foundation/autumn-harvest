//! Typed workflow failures — success-metric and handle-surface proofs (issue #767).
//!
//! Two layers:
//!
//! * **F1 (non-DB, headline success metric).** The parent's routing decision is
//!   keyed *purely* on the child's typed `error_type` (via
//!   [`HarvestError::workflow_error_type`]) with **zero** substring matching on
//!   the failure message. Two histories that differ only in the child's message
//!   text — but share the same `error_type` — must produce the *identical*
//!   routing decision, across ≥3 distinct error categories. This runs without
//!   Docker in the non-DB `--test integration` CI step.
//!
//! * **F2a (DB, handle surface / AC5).** A `FAILED` execution whose terminal
//!   `WorkflowFailed` event carries typed fields surfaces those fields to an
//!   embedder through [`WorkflowHandle::result_raw`],
//!   `TypedWorkflowHandle::result`/`result_snapshot`, and preserves the
//!   *human message* (never the wire envelope) in the `execution.error` column
//!   (AC4). Requires Postgres (testcontainers); Docker-gated CI step.

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure, decode_workflow_failure};
use autumn_harvest::types::ExecutionId;
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// F1 — headline success metric (no DB).
// ---------------------------------------------------------------------------

/// Reconstruct the `HarvestError` a parent observes when a child fails with a
/// typed `WorkflowFailure`, by replaying the parent's `spawn_child_workflow_raw`
/// against a hand-built history whose child terminal is a typed
/// `ChildWorkflowFailed`.
async fn observe_typed_child_failure(error_type: &str, message: &str) -> HarvestError {
    let child_id = ExecutionId::new();
    let decoded = decode_workflow_failure(
        &WorkflowFailure::new(error_type, message)
            .non_retryable()
            .into_workflow_error_payload(),
    );
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "risky_child".into(),
            input: Value::Null,
        },
        WorkflowEvent::child_workflow_failed_typed(child_id, &decoded),
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    ctx.spawn_child_workflow_raw("risky_child", Value::Null)
        .await
        .expect_err("a typed child failure must surface as an Err")
}

/// The parent's routing decision — keyed *only* on the typed error-type class.
/// The function never inspects the failure message: this is the "ZERO substring
/// matching" contract, enforced structurally by taking `&HarvestError` and
/// consulting only `workflow_error_type()`.
fn route_on_error_type(err: &HarvestError) -> &'static str {
    match err.workflow_error_type() {
        Some("ValidationRejected") => "reject_and_notify_customer",
        Some("BudgetExceeded") => "escalate_to_finance",
        Some("UpstreamUnavailable") => "reschedule_for_later",
        Some(_) => "generic_typed_handler",
        None => "untyped_fallback",
    }
}

#[tokio::test]
async fn routing_is_stable_across_reworded_messages_for_the_same_error_type() {
    // ≥3 distinct categories; each has two *reworded* messages that must route
    // identically because only the error_type is consulted.
    let categories: &[(&str, &str, [&str; 2])] = &[
        (
            "ValidationRejected",
            "reject_and_notify_customer",
            [
                "card declined by issuer",
                "issuer refused the charge (do_not_honor)",
            ],
        ),
        (
            "BudgetExceeded",
            "escalate_to_finance",
            [
                "monthly spend cap reached",
                "tenant is over its configured budget for July",
            ],
        ),
        (
            "UpstreamUnavailable",
            "reschedule_for_later",
            [
                "provider returned 503",
                "downstream gateway timed out after 30s",
            ],
        ),
    ];

    for (error_type, expected_branch, messages) in categories {
        let err_a = observe_typed_child_failure(error_type, messages[0]).await;
        let err_b = observe_typed_child_failure(error_type, messages[1]).await;

        // Both are typed WorkflowFailed carrying the same class.
        assert!(matches!(err_a, HarvestError::WorkflowFailed { .. }));
        assert!(matches!(err_b, HarvestError::WorkflowFailed { .. }));
        assert_eq!(err_a.workflow_error_type(), Some(*error_type));
        assert_eq!(err_b.workflow_error_type(), Some(*error_type));

        let branch_a = route_on_error_type(&err_a);
        let branch_b = route_on_error_type(&err_b);

        // The headline invariant: reworded messages → identical branch.
        assert_eq!(
            branch_a, branch_b,
            "error_type {error_type}: reworded messages must route identically"
        );
        assert_eq!(
            branch_a, *expected_branch,
            "error_type {error_type} must route to {expected_branch}"
        );
        // The human messages genuinely differ (so the invariant is meaningful).
        assert_ne!(messages[0], messages[1]);
    }
}

#[tokio::test]
async fn legacy_untyped_child_failure_routes_to_the_untyped_fallback() {
    // A pre-#767 (untyped) child failure has no error_type, so it deterministically
    // routes to the untyped fallback rather than masquerading as a typed class.
    let child_id = ExecutionId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: "risky_child".into(),
            input: Value::Null,
        },
        WorkflowEvent::child_workflow_failed(child_id, "ValidationRejected: card declined"),
    ];
    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let err = ctx
        .spawn_child_workflow_raw("risky_child", Value::Null)
        .await
        .expect_err("child failure must surface as an Err");

    assert_eq!(err.workflow_error_type(), None);
    assert_eq!(route_on_error_type(&err), "untyped_fallback");
}

// ---------------------------------------------------------------------------
// F2a — handle surface against real Postgres (AC5 / AC4).
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
mod db_handle_surface {
    use autumn_harvest::error::HarvestError;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::failure::{IntoWorkflowErrorString, WorkflowFailure};
    use autumn_harvest::models::NewWorkflowExecution;
    use autumn_harvest::schema::harvest_workflow_executions::dsl;
    use autumn_harvest::types::ExecutionId;
    use autumn_harvest::worker::DbPool;
    use autumn_harvest::{WorkflowHandleClient, WorkflowResultState};
    use diesel::prelude::*;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde_json::Value;
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

    const INIT_SQL: &str = concat!(
        include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
        "\n",
        include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
        "\n",
        include_str!("../../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
        "\n",
        include_str!("../../migrations/20260424000000_harvest_sticky_routing/up.sql"),
        "\n",
        include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
        "\n",
        include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
        "\n",
        include_str!("../../migrations/20260428000000_harvest_retention_scan_index/up.sql"),
        "\n",
        include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
        "\n",
        include_str!("../../migrations/20260430000000_harvest_workflow_schedules/up.sql"),
        "\n",
        include_str!("../../migrations/20260430000001_harvest_external_tasks/up.sql"),
        "\n",
        include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
        "\n",
        include_str!("../../migrations/20260501010000_harvest_batch_jobs/up.sql"),
        "\n",
        include_str!("../../migrations/20260501020000_harvest_batch_processed_ids/up.sql"),
        "\n",
        include_str!("../../migrations/20260503000000_harvest_workflow_reset/up.sql"),
        "\n",
        include_str!("../../migrations/20260504000000_harvest_workflow_parent_children/up.sql"),
        "\n",
        include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
        "\n",
        include_str!("../../migrations/20260506000000_harvest_audit_log/up.sql"),
        "\n",
        include_str!("../../migrations/20260508000000_harvest_external_task_updated_at/up.sql"),
        "\n",
        include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
        "\n",
        include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
        "\n",
        include_str!("../../migrations/20260513000000_harvest_schedule_pause_metadata/up.sql"),
        "\n",
        include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
        "\n",
        include_str!("../../migrations/20260518000000_harvest_signal_idempotency/up.sql"),
        "\n",
        include_str!("../../migrations/20260518000001_harvest_workflow_execution_timeout/up.sql"),
        "\n",
        include_str!("../../migrations/20260613000000_harvest_workflow_sla/up.sql"),
        "\n",
        include_str!("../../migrations/20260519000000_harvest_calendar_awareness/up.sql"),
        "\n",
        include_str!("../../migrations/20260522000000_harvest_schedule_decisions/up.sql"),
        "\n",
        include_str!("../../migrations/20260522000001_harvest_rate_limiting/up.sql"),
        "\n",
        include_str!("../../migrations/20260526000001_harvest_parent_close_policy/up.sql"),
        "\n",
        include_str!("../../migrations/20260530000000_harvest_schedule_ha_claim/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000000_harvest_schedule_auto_pause/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000001_harvest_poison_pill_strikes/up.sql"),
        "\n",
        include_str!("../../migrations/20260601000002_harvest_ownership_metadata/up.sql"),
        "\n",
        include_str!("../../migrations/20260603000000_harvest_completion_triggers/up.sql"),
        include_str!("../../migrations/20260708000001_harvest_completion_trigger_condition/up.sql"),
        include_str!("../../migrations/20260605000000_harvest_admission_gates/up.sql"),
        include_str!("../../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
        include_str!("../../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
        include_str!("../../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
        "\n",
        include_str!("../../migrations/20260607000002_harvest_workflow_pause/up.sql"),
        "\n",
        include_str!("../../migrations/20260609000001_harvest_workflow_current_details/up.sql"),
        "\n",
        include_str!("../../migrations/20260610000001_harvest_schedule_bounded_runs/up.sql"),
        "\n",
        include_str!("../../migrations/20260613000001_harvest_schedule_catchup_window/up.sql"),
        "\n",
        include_str!("../../migrations/20260616000001_harvest_workflow_schedule_id/up.sql"),
        "\n",
        include_str!("../../migrations/20260615000001_harvest_context_headers/up.sql"),
        "\n",
        // issue #523: workflow-level retry policy columns.
        include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
        "\n",
        // issue #534: origin column + per-schedule run-history index.
        include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
        include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
        include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
        include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
        "\n",
        include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
        include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    );

    async fn setup() -> (String, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_init_sql(INIT_SQL.to_string().into_bytes())
            .with_tag("16")
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

    async fn insert_running_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
        let exec_id = ExecutionId::new();
        diesel::insert_into(dsl::harvest_workflow_executions)
            .values(&NewWorkflowExecution {
                id: exec_id.as_uuid(),
                workflow_name: "typed_failing_wf",
                workflow_id: &format!("wf-typed-{}", Uuid::new_v4()),
                run_id: Uuid::new_v4(),
                shard_id: 0,
                input: serde_json::json!({}),
                memo: None,
                search_attrs: None,
                queue_name: "default",
                parent_id: None,
                parent_close_policy: None,
                assigned_build_id: None,
                execution_timeout: None,
                deadline_at: None,
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
            })
            .execute(conn)
            .await
            .expect("insert execution");
        exec_id
    }

    /// Simulate exactly what `worker::persist_workflow_failure` now does
    /// (issue #767): append a typed `WorkflowFailed` event, and stamp the
    /// **human message** (not the wire envelope) into the `error` column.
    async fn seal_typed_failure(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        cat: &str,
        msg: &str,
    ) {
        let payload = WorkflowFailure::new(cat, msg)
            .with_details(serde_json::json!({ "cat": cat }))
            .non_retryable()
            .into_workflow_error_payload();
        let decoded = autumn_harvest::failure::decode_workflow_failure(&payload);
        conn.transaction::<(), HarvestError, _>(|conn| {
            let decoded = decoded.clone();
            Box::pin(async move {
                autumn_harvest::store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::workflow_failed_typed(&decoded)],
                    1,
                )
                .await?;
                diesel::update(dsl::harvest_workflow_executions.find(exec_id.as_uuid()))
                    .set((dsl::state.eq("FAILED"), dsl::error.eq(&decoded.message)))
                    .execute(conn)
                    .await?;
                Ok(())
            })
        })
        .await
        .expect("seal typed failure");
    }

    #[tokio::test]
    async fn failed_handle_surfaces_typed_fields_across_categories() {
        let (db_url, _container) = setup().await;
        let pool = build_pool(&db_url);
        let mut conn = pool.get().await.unwrap();
        let client = WorkflowHandleClient::single(pool.clone(), db_url.clone());

        for (cat, msg) in [
            ("ValidationRejected", "card declined by issuer"),
            ("BudgetExceeded", "monthly spend cap reached"),
            ("UpstreamUnavailable", "provider returned 503"),
        ] {
            let exec_id = insert_running_execution(&mut conn).await;
            seal_typed_failure(&mut conn, exec_id, cat, msg).await;

            let handle = client.handle(exec_id);

            // result_raw → typed HarvestError::WorkflowFailed (D1/D2).
            let err = handle
                .result_raw()
                .await
                .expect_err("FAILED execution must return an Err");
            assert!(matches!(err, HarvestError::WorkflowFailed { .. }));
            assert_eq!(err.workflow_error_type(), Some(cat));
            assert!(err.is_workflow_non_retryable());
            assert_eq!(
                err.workflow_details(),
                Some(&serde_json::json!({ "cat": cat }))
            );

            // AC4: the execution.error column holds the human message, never the
            // wire envelope.
            let stored_error: Option<String> = dsl::harvest_workflow_executions
                .find(exec_id.as_uuid())
                .select(dsl::error)
                .first(&mut conn)
                .await
                .unwrap();
            assert_eq!(stored_error.as_deref(), Some(msg));
            assert!(
                !stored_error
                    .unwrap()
                    .contains("harvest_workflow_failure_v1")
            );

            // Typed snapshot (D3): the three typed fields are populated.
            let typed = autumn_harvest::TypedWorkflowHandle::<Value>::new(handle.clone());
            let snap = typed.result_snapshot().await.unwrap();
            assert_eq!(snap.state, WorkflowResultState::Failed);
            assert_eq!(snap.error_type.as_deref(), Some(cat));
            assert_eq!(snap.non_retryable, Some(true));
            assert_eq!(snap.error_details, Some(serde_json::json!({ "cat": cat })));
        }
    }
}
