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

    fn init_sql() -> Vec<u8> {
        autumn_harvest::full_migrations_sql().as_bytes().to_vec()
    }

    async fn setup() -> (String, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_init_sql(init_sql())
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
        insert_running_execution_retry_of(conn, None).await
    }

    /// Insert a RUNNING execution, optionally linked to a predecessor via the
    /// #523 workflow-level retry FK (`retry_of_exec_id`). A retry successor also
    /// carries `workflow_attempt = 2`.
    async fn insert_running_execution_retry_of(
        conn: &mut AsyncPgConnection,
        retry_of: Option<ExecutionId>,
    ) -> ExecutionId {
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
                workflow_attempt: if retry_of.is_some() { 2 } else { 1 },
                workflow_retry_policy: None,
                retry_of_exec_id: retry_of.map(|e| e.as_uuid()),
                origin: None,
                completion_callbacks: None,
                continued_from_exec_id: None,
                first_exec_id: None,
                start_source: None,
                start_source_ref: None,
                started_by: None,
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

            // Typed `result()` Err-arm also surfaces the typed class (D1).
            let typed_err = typed
                .result()
                .await
                .expect_err("FAILED execution must return an Err from result()");
            assert_eq!(typed_err.workflow_error_type(), Some(cat));
        }
    }

    /// FIX A (issue #767): `result_raw` follows the workflow-level retry chain
    /// (issue #523) via `load_effective_execution`, so for a retried workflow
    /// whose *final* attempt failed with a DIFFERENT typed class than the first,
    /// the caller must see the FINAL attempt's typed fields — enriched from the
    /// *effective* execution, not the original handle id.
    ///
    /// Also pins the consistency contract: `result_snapshot` does NOT follow the
    /// chain (it reports the original row), so its typed fields must stay keyed
    /// to the original execution it reports.
    #[tokio::test]
    async fn result_raw_enriches_from_the_effective_retry_execution() {
        let (db_url, _container) = setup().await;
        let pool = build_pool(&db_url);
        let mut conn = pool.get().await.unwrap();
        let client = WorkflowHandleClient::single(pool.clone(), db_url.clone());

        // The original attempt fails with one typed class...
        let original = insert_running_execution_retry_of(&mut conn, None).await;
        seal_typed_failure(
            &mut conn,
            original,
            "OriginalClass",
            "first attempt blew up",
        )
        .await;

        // ...and its #523 retry successor fails with a DIFFERENT typed class.
        let successor = insert_running_execution_retry_of(&mut conn, Some(original)).await;
        seal_typed_failure(
            &mut conn,
            successor,
            "FinalClass",
            "final attempt also failed",
        )
        .await;

        let handle = client.handle(original);

        // result_raw follows the retry chain and enriches from the EFFECTIVE
        // (final-attempt) execution — the FinalClass, never OriginalClass.
        let err = handle
            .result_raw()
            .await
            .expect_err("a fully-failed retry chain must return an Err");
        assert!(matches!(err, HarvestError::WorkflowFailed { .. }));
        assert_eq!(
            err.workflow_error_type(),
            Some("FinalClass"),
            "result_raw must enrich from the effective (final-attempt) execution, \
             not the original handle id"
        );
        assert_eq!(
            err.workflow_details(),
            Some(&serde_json::json!({ "cat": "FinalClass" }))
        );

        // result_snapshot does NOT follow the chain — it reports the original
        // row, so its typed fields must stay consistent with THAT execution.
        let typed = autumn_harvest::TypedWorkflowHandle::<Value>::new(handle.clone());
        let snap = typed.result_snapshot().await.unwrap();
        assert_eq!(snap.state, WorkflowResultState::Failed);
        assert_eq!(
            snap.error_type.as_deref(),
            Some("OriginalClass"),
            "result_snapshot reports the original row (no chain-follow), so its \
             typed fields must match the original execution"
        );
    }
}
