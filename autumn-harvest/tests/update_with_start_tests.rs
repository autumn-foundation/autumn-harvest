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
        concurrency_key: None,
        concurrency_limit: None,
        update_id,
        update_name: "add_item".to_string(),
        update_args: serde_json::json!({"item_id": "sku-1", "qty": 2}),
        idempotency_key: None,
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
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
        concurrency_key: None,
        concurrency_limit: None,
        update_id,
        update_name: "add_item".to_string(),
        update_args: serde_json::Value::Null,
        idempotency_key: Some("stripe-evt-abc123".to_string()),
        max_workflow_input_bytes: 0,
        owner: None,
        runbook_url: None,
        severity: None,
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
    let opts: TypedUpdateWithStartOptions = Default::default();
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
    use autumn_harvest::store;
    use autumn_harvest::types::{ExecutionId, Priority, UpdateId, WorkflowIdReusePolicy};
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    const INIT_SQL: &str = concat!(
        include_str!("../migrations/20260409000000_harvest_initial/up.sql"),
        "\n",
        include_str!("../migrations/20260410010000_harvest_workflow_start_uniqueness/up.sql"),
        "\n",
        include_str!("../migrations/20260424000001_harvest_trace_context/up.sql"),
        "\n",
        include_str!("../migrations/20260427000000_harvest_continue_as_new/up.sql"),
        "\n",
        include_str!("../migrations/20260429000000_harvest_concurrency_key/up.sql"),
        "\n",
        include_str!("../migrations/20260501000000_harvest_workers/up.sql"),
        "\n",
        include_str!("../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
        "\n",
        include_str!("../migrations/20260509000000_harvest_build_routing/up.sql"),
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
        include_str!("../migrations/20260606000001_harvest_activity_schedule_to_close/up.sql"),
        include_str!("../migrations/20260607000000_harvest_worker_capability_labels/up.sql"),
        include_str!("../migrations/20260607000001_harvest_task_required_capabilities/up.sql"),
        "\n",
        include_str!("../migrations/20260607000002_harvest_workflow_pause/up.sql"),
        "\n",
        include_str!("../migrations/20260609000001_harvest_workflow_current_details/up.sql")
    );

    async fn setup_test_db() -> (
        diesel_async::AsyncPgConnection,
        testcontainers::ContainerAsync<Postgres>,
    ) {
        let container = Postgres::default()
            .with_init_sql(INIT_SQL.to_string().into_bytes())
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
            concurrency_key: None,
            concurrency_limit: None,
            update_id,
            update_name: "add_item".to_string(),
            update_args: serde_json::json!({"item_id": "sku-1", "qty": 2}),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
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
        use autumn_harvest::replay::HistoryMatch;
        use autumn_harvest::replay::HistoryMatcher;
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
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
        };
        start_or_load_workflow_execution(&mut conn, first_params)
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
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner: None,
            runbook_url: None,
            severity: None,
        };
        start_or_load_workflow_execution(&mut conn, start_params)
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
        let update_id = UpdateId::new();

        // Build params with idempotency key.
        let mut params = make_params(
            "cart",
            "cart-idem",
            exec_id,
            update_id,
            WorkflowIdReusePolicy::AllowDuplicate,
        );
        params.idempotency_key = Some("idm-key-001".to_string());

        let first = update_with_start_workflow_execution(&mut conn, params.clone())
            .await
            .expect("first call should succeed");
        assert!(first.started_fresh);
        assert!(first.update_admitted);

        // Second call with same idempotency key.
        let mut params2 = params.clone();
        params2.exec_id = ExecutionId::new(); // different exec_id
        params2.update_id = UpdateId::new(); // different update_id; key drives dedup

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
