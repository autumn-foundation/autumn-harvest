//! Tests for `update_with_start` core API (issue #479).
//!
//! Covers the struct shape, outcome fields, and (with the `db` feature) the
//! full matrix of reuse-policy × prior-state combinations for the atomic
//! start-or-attach + update-admission primitive.

// ── Type-level tests (always run, DB feature required for execution types) ───

#[cfg(feature = "db")]
use autumn_harvest::execution::{UpdateWithStartOutcome, UpdateWithStartParams};
#[cfg(feature = "db")]
use autumn_harvest::types::{ExecutionId, UpdateId, WorkflowIdReusePolicy};

#[cfg(feature = "db")]
#[test]
fn update_with_start_params_is_cloneable_and_debug() {
    // Verifies the struct exists, all fields are accessible, and the expected
    // derives are present (Debug + Clone).
    let exec_id = ExecutionId::new();
    let update_id = UpdateId::new();
    let params = UpdateWithStartParams {
        workflow_name: "cart",
        workflow_id: "cart-123",
        exec_id,
        input: serde_json::json!({"tenant": "acme"}),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: Default::default(),
        update_id,
        update_name: "add_item".to_string(),
        update_args: serde_json::json!({"item_id": "sku-1", "qty": 2}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
    };

    let _cloned = params.clone();
    let debug_str = format!("{params:?}");
    assert!(
        debug_str.contains("cart"),
        "debug output must contain workflow_name"
    );
}

#[cfg(feature = "db")]
#[test]
fn update_with_start_outcome_is_cloneable_and_debug() {
    let exec_id = ExecutionId::new();
    let update_id = UpdateId::new();
    let outcome = UpdateWithStartOutcome {
        exec_id,
        workflow_name: "cart".to_string(),
        workflow_id: "cart-123".to_string(),
        state: "RUNNING".to_string(),
        started_fresh: true,
        update_id,
        update_admitted: true,
    };

    let cloned = outcome.clone();
    assert_eq!(cloned, outcome, "clone must equal original");
    let debug_str = format!("{outcome:?}");
    assert!(
        debug_str.contains("cart"),
        "debug output must contain workflow_name"
    );
}

#[cfg(feature = "db")]
#[test]
fn update_with_start_outcome_idempotency_key_roundtrip() {
    let exec_id = ExecutionId::new();
    let update_id = UpdateId::new();
    let params = UpdateWithStartParams {
        workflow_name: "cart",
        workflow_id: "cart-123",
        exec_id,
        input: serde_json::Value::Null,
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
        chain_execution_timeout: None,
        max_workflow_chain_timeout_ceiling: None,
        concurrency_key: None,
        concurrency_limit: None,
        concurrency_on_conflict: Default::default(),
        update_id,
        update_name: "add_item".to_string(),
        update_args: serde_json::Value::Null,
        idempotency_key: Some("stripe-evt-abc123".to_string()),
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: None,

        sla: None,
        workflow_retry_policy: None,
        max_workflow_attempts_ceiling: None,
        reject_fresh_if_debounced: false,
    };
    assert_eq!(
        params.idempotency_key.as_deref(),
        Some("stripe-evt-abc123"),
        "idempotency_key must round-trip through the struct"
    );
}

// ── TypedUpdateWithStartOptions lives in handle_typed (db feature required) ──

#[cfg(feature = "db")]
use autumn_harvest::TypedUpdateWithStartOptions;

#[cfg(feature = "db")]
#[test]
fn typed_update_with_start_options_default_is_safe() {
    let opts = TypedUpdateWithStartOptions::default();
    assert!(opts.exec_id.is_none(), "exec_id should be None by default");
    assert!(
        opts.idempotency_key.is_none(),
        "idempotency_key should be None by default"
    );
    assert!(
        opts.reuse_policy.is_none(),
        "reuse_policy should be None by default"
    );
}

// ── DB-gated integration tests ────────────────────────────────────────────────

#[cfg(feature = "db")]
mod db_tests {
    use autumn_harvest::execution::{
        StartWorkflowParams, UpdateWithStartParams, start_or_load_workflow_execution,
        update_with_start_workflow_execution,
    };
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
    use autumn_harvest::store;
    use autumn_harvest::types::{ExecutionId, Priority, UpdateId, WorkflowIdReusePolicy};
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    fn init_sql() -> Vec<u8> {
        autumn_harvest::full_migrations_sql().as_bytes().to_vec()
    }

    async fn setup_test_db() -> (
        diesel_async::AsyncPgConnection,
        testcontainers::ContainerAsync<Postgres>,
    ) {
        let container = Postgres::default()
            .with_init_sql(init_sql())
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let conn =
            <diesel_async::AsyncPgConnection as diesel_async::AsyncConnection>::establish(&url)
                .await
                .expect("connect");
        (conn, container)
    }

    fn make_params<'a>(
        workflow_name: &'a str,
        workflow_id: &'a str,
        exec_id: ExecutionId,
        update_id: UpdateId,
        reuse_policy: WorkflowIdReusePolicy,
    ) -> UpdateWithStartParams<'a> {
        UpdateWithStartParams {
            workflow_name,
            workflow_id,
            exec_id,
            input: serde_json::json!({"tenant": "acme"}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            // Chain-scoped lifetime cap (issue #617): SWS/UWS test fixture.
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
            update_id,
            update_name: "add_item".to_string(),
            update_args: serde_json::json!({"item_id": "sku-1", "qty": 2}),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,

            sla: None,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            reject_fresh_if_debounced: false,
        }
    }

    #[tokio::test]
    async fn should_start_fresh_and_admit_update_when_no_prior_execution() {
        let (mut conn, _container) = setup_test_db().await;
        let exec_id = ExecutionId::new();
        let update_id = UpdateId::new();

        let outcome = update_with_start_workflow_execution(
            &mut conn,
            make_params(
                "cart",
                "cart-no-prior",
                exec_id,
                update_id,
                WorkflowIdReusePolicy::AllowDuplicate,
            ),
        )
        .await
        .expect("should succeed with no prior execution");

        assert!(
            outcome.started_fresh,
            "must report started_fresh when no prior"
        );
        assert!(outcome.update_admitted, "must report update_admitted");
        assert_eq!(outcome.update_id, update_id, "update_id must match input");
        assert_eq!(outcome.workflow_name, "cart");

        // Verify UpdateAdmitted event exists in history.
        let history = store::load_history(&mut conn, outcome.exec_id)
            .await
            .expect("load history");
        let match_result = HistoryMatcher::new(history.events).match_update(update_id);
        // The update is admitted (UpdateAdmitted exists) but not yet completed.
        // match_update returns NoMatch when admitted but no completion event yet.
        assert!(
            matches!(match_result, HistoryMatch::NoMatch),
            "admitted-but-not-completed update should be NoMatch, got: {match_result:?}"
        );
    }

    #[tokio::test]
    async fn should_attach_update_to_running_execution_with_allow_duplicate() {
        let (mut conn, _container) = setup_test_db().await;

        // First start a workflow manually.
        let exec_id = ExecutionId::new();
        let first_params = StartWorkflowParams {
            workflow_name: "cart",
            workflow_id: "cart-running",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
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
        };
        start_or_load_workflow_execution(&mut conn, first_params, None)
            .await
            .expect("first start should succeed");

        // Now update_with_start should attach.
        let new_exec_id = ExecutionId::new();
        let update_id = UpdateId::new();
        let outcome = update_with_start_workflow_execution(
            &mut conn,
            make_params(
                "cart",
                "cart-running",
                new_exec_id,
                update_id,
                WorkflowIdReusePolicy::AllowDuplicate,
            ),
        )
        .await
        .expect("should attach to running execution");

        assert!(
            !outcome.started_fresh,
            "must not start fresh when prior is RUNNING"
        );
        assert!(
            outcome.update_admitted,
            "must admit update to running execution"
        );
        assert_eq!(outcome.exec_id, exec_id, "must reuse original execution ID");
    }

    #[tokio::test]
    async fn should_return_already_exists_under_reject_duplicate_with_running_prior() {
        let (mut conn, _container) = setup_test_db().await;

        // Start a workflow first.
        let exec_id = ExecutionId::new();
        let start_params = StartWorkflowParams {
            workflow_name: "cart",
            workflow_id: "cart-reject",
            exec_id,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: Default::default(),
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
        };
        start_or_load_workflow_execution(&mut conn, start_params, None)
            .await
            .expect("first start");

        // update_with_start with RejectDuplicate should fail.
        let update_id = UpdateId::new();
        let result = update_with_start_workflow_execution(
            &mut conn,
            make_params(
                "cart",
                "cart-reject",
                ExecutionId::new(),
                update_id,
                WorkflowIdReusePolicy::RejectDuplicate,
            ),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(autumn_harvest::error::HarvestError::AlreadyExists { .. })
            ),
            "RejectDuplicate + RUNNING prior must return AlreadyExists, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn should_deduplicate_via_idempotency_key() {
        let (mut conn, _container) = setup_test_db().await;
        let exec_id = ExecutionId::new();

        // Callers must derive update_id deterministically from the idempotency key
        // (e.g. via UUIDv5) so the dedupe lookup matches on retry.
        let idem_key = "idm-key-001";
        let ns = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let update_id = UpdateId::from_uuid(uuid::Uuid::new_v5(&ns, idem_key.as_bytes()));

        // Build params with idempotency key.
        let mut params = make_params(
            "cart",
            "cart-idem",
            exec_id,
            update_id,
            WorkflowIdReusePolicy::AllowDuplicate,
        );
        params.idempotency_key = Some(idem_key.to_string());

        let first = update_with_start_workflow_execution(&mut conn, params.clone())
            .await
            .expect("first call should succeed");
        assert!(first.started_fresh);
        assert!(first.update_admitted);

        // Second call with same idempotency key — exec_id differs (as on a real
        // HTTP retry) but update_id is the same deterministic value.
        let mut params2 = params.clone();
        params2.exec_id = ExecutionId::new(); // different exec_id, same update_id

        let second = update_with_start_workflow_execution(&mut conn, params2)
            .await
            .expect("second call should succeed (deduplicated)");

        // Must not start fresh again.
        assert!(
            !second.started_fresh,
            "idempotent retry must not start fresh"
        );
        assert!(
            !second.update_admitted,
            "idempotent retry must not re-admit update"
        );
    }
}
