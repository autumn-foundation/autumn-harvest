//! Tests for worker-session enqueue pinning and the hard-pin claim gate
//! (issue #606, TDD step 9).
//!
//! **Pure-logic tests (no DB required):** `EnqueueParams::with_session_id`
//! builder behavior.
//!
//! **DB integration tests (`db` feature, bottom of file):** the `session_id`
//! column round-trips through `enqueue`, and `claim_task`'s hard-pin gate --
//! unlike ordinary sticky routing, a session-tagged row is claimable *only*
//! by its pinned worker even after the (bookkeeping) sticky lease has
//! elapsed. Written RED-first; pass GREEN against the implementation in
//! `queue.rs`.
//!
//! Compile-checked only in this sandbox (no Docker/testcontainers available),
//! matching the #543/#544/#601 precedent documented in CLAUDE.md.

use autumn_harvest::queue::{EnqueueParams, TaskType};

// ---------------------------------------------------------------------------
// EnqueueParams::with_session_id (no DB required)
// ---------------------------------------------------------------------------

#[test]
fn enqueue_params_default_session_id_is_none() {
    let params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
    assert_eq!(params.session_id, None);
}

#[test]
fn with_session_id_sets_the_field() {
    let session_id = uuid::Uuid::new_v4();
    let params = EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null))
        .with_session_id(session_id);
    assert_eq!(params.session_id, Some(session_id));
}

#[test]
fn with_session_id_composes_with_with_sticky() {
    let session_id = uuid::Uuid::new_v4();
    let params = EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null))
        .with_session_id(session_id)
        .with_sticky("worker-42", std::time::Duration::from_secs(3600));
    assert_eq!(params.session_id, Some(session_id));
    assert_eq!(params.sticky_worker_id.as_deref(), Some("worker-42"));
}

// ---------------------------------------------------------------------------
// DB integration tests
// ---------------------------------------------------------------------------

#[cfg(feature = "db")]
mod db_tests {
    use autumn_harvest::models::NewWorkflowExecution;
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::harvest_workflow_executions;
    use autumn_harvest::types::ExecutionId;
    use diesel::sql_types::{Nullable, Text, Timestamptz, Uuid as SqlUuid};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use std::time::Duration;
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

    // Same base schema as sticky_routing_tests.rs, plus the worker-sessions
    // migration under test.
    const INIT_SQL: &str = concat!(
        include_str!("../../migrations/20260409000000_harvest_initial/up.sql"),
        "\n",
        include_str!("../../migrations/20260619000000_harvest_task_queue_created_at/up.sql"),
        "\n",
        include_str!("../../migrations/20260424000001_harvest_trace_context/up.sql"),
        "\n",
        include_str!("../../migrations/20260505000000_harvest_heartbeat_details/up.sql"),
        "\n",
        include_str!("../../migrations/20260427000000_harvest_continue_as_new/up.sql"),
        "\n",
        include_str!("../../migrations/20260429000000_harvest_concurrency_key/up.sql"),
        "\n",
        include_str!("../../migrations/20260501000000_harvest_workers/up.sql"),
        "\n",
        include_str!("../../migrations/20260508010000_harvest_workers_drain_deadline/up.sql"),
        "\n",
        include_str!("../../migrations/20260509000000_harvest_build_routing/up.sql"),
        "\n",
        include_str!("../../migrations/20260514020000_harvest_task_activity_id/up.sql"),
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
        include_str!("../../migrations/20260626000001_harvest_workflow_retry/up.sql"),
        "\n",
        include_str!("../../migrations/20260628000001_harvest_execution_origin/up.sql"),
        include_str!("../../migrations/20260703000000_harvest_task_queue_wake_requested/up.sql"),
        include_str!("../../migrations/20260704000001_harvest_build_policy_ramp/up.sql"),
        include_str!("../../migrations/20260704000000_harvest_workflow_nd_block/up.sql"),
        "\n",
        include_str!("../../migrations/20260705000000_harvest_completion_deliveries/up.sql"),
        "\n",
        // Worker sessions (issue #606) -- the migration under test.
        include_str!("../../migrations/20260706000000_harvest_worker_sessions/up.sql"),
    );

    async fn setup() -> (AsyncPgConnection, ContainerAsync<Postgres>) {
        let container = Postgres::default()
            .with_init_sql(INIT_SQL.to_string().into_bytes())
            .with_tag("16")
            .start()
            .await
            .expect("failed to start Postgres container");
        let host = container.get_host().await.expect("host");
        let port = container.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let conn = AsyncPgConnection::establish(&url).await.expect("connect");
        (conn, container)
    }

    async fn insert_execution(conn: &mut AsyncPgConnection) -> ExecutionId {
        let exec_id = ExecutionId::new();
        let row = NewWorkflowExecution {
            id: exec_id.as_uuid(),
            workflow_name: "session_test_wf",
            workflow_id: &Uuid::new_v4().to_string(),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: serde_json::json!({}),
            parent_id: None,
            queue_name: "default",
            execution_timeout: None,
            deadline_at: None,
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
        };
        diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .execute(conn)
            .await
            .expect("insert execution");
        exec_id
    }

    #[derive(diesel::QueryableByName, Debug)]
    struct SessionColumns {
        #[diesel(sql_type = Nullable<SqlUuid>)]
        session_id: Option<Uuid>,
        #[diesel(sql_type = Nullable<Text>)]
        sticky_worker_id: Option<String>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        sticky_until: Option<chrono::DateTime<chrono::Utc>>,
    }

    async fn read_session_columns(conn: &mut AsyncPgConnection, task_id: Uuid) -> SessionColumns {
        diesel::sql_query(
            "SELECT session_id, sticky_worker_id, sticky_until \
             FROM harvest_task_queue WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result(conn)
        .await
        .expect("read session columns")
    }

    /// Enqueueing with `with_session_id` + `with_sticky` writes both the
    /// `session_id` column and the ordinary sticky pin columns to the DB row.
    #[tokio::test]
    async fn enqueue_with_session_id_writes_column_to_db() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;
        let session_id = Uuid::new_v4();

        let mut params =
            EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        let params = params
            .with_session_id(session_id)
            .with_sticky("worker-a", Duration::from_secs(3600));

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
        let cols = read_session_columns(&mut conn, task_id).await;

        assert_eq!(cols.session_id, Some(session_id));
        assert_eq!(cols.sticky_worker_id.as_deref(), Some("worker-a"));
    }

    /// A non-session task's `session_id` column is NULL -- the default,
    /// zero-behavior-change case (AC2).
    #[tokio::test]
    async fn enqueue_without_session_id_leaves_column_null() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params =
            EnqueueParams::new("default", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");
        let cols = read_session_columns(&mut conn, task_id).await;

        assert_eq!(cols.session_id, None);
    }

    /// The hard-pin gate: the pinned worker can claim a session-tagged task.
    #[tokio::test]
    async fn session_worker_can_claim_its_pinned_task() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;
        let session_id = Uuid::new_v4();

        let mut params =
            EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("transcode_chunk".to_string());
        let params = params
            .with_session_id(session_id)
            .with_sticky("worker-host", Duration::from_secs(3600));

        let task_id = queue::enqueue(&mut conn, &params).await.expect("enqueue");

        let claimed = queue::claim_task(
            &mut conn,
            &["gpu-workers".to_string()],
            "worker-host",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");

        assert!(
            claimed.is_some(),
            "the session's host worker must be able to claim its pinned task"
        );
        assert_eq!(claimed.unwrap().id, task_id);
    }

    /// The hard-pin gate: a non-pinned worker cannot claim a session-tagged
    /// task, even though it is otherwise eligible (same queue, no build
    /// restriction).
    #[tokio::test]
    async fn non_session_worker_cannot_claim_session_task() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;
        let session_id = Uuid::new_v4();

        let mut params =
            EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("transcode_chunk".to_string());
        let params = params
            .with_session_id(session_id)
            .with_sticky("worker-host", Duration::from_secs(3600));

        queue::enqueue(&mut conn, &params).await.expect("enqueue");

        let claimed = queue::claim_task(
            &mut conn,
            &["gpu-workers".to_string()],
            "worker-imposter",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");

        assert!(
            claimed.is_none(),
            "a non-pinned worker must never claim a session-tagged task"
        );
    }

    /// THE headline hard-pin guarantee (distinguishing session pinning from
    /// ordinary sticky routing, issue #606): even after the sticky lease has
    /// elapsed, a session-tagged task still does NOT fail over to a
    /// different worker -- unlike plain sticky routing, where an expired
    /// lease makes the row claimable by anyone.
    #[tokio::test]
    async fn session_task_does_not_fail_over_after_sticky_lease_expires() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;
        let session_id = Uuid::new_v4();

        let mut params =
            EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("transcode_chunk".to_string());
        // A sticky_timeout in the past: the lease has already "expired" by
        // the time we try to claim.
        let params = params
            .with_session_id(session_id)
            .with_sticky("worker-host", Duration::from_millis(1));

        queue::enqueue(&mut conn, &params).await.expect("enqueue");
        tokio::time::sleep(Duration::from_millis(50)).await;

        // A plain sticky (non-session) task with the same expired lease WOULD
        // be claimable by any worker at this point -- but this task carries
        // session_id, so the hard-pin gate must still exclude the imposter.
        let claimed_by_imposter = queue::claim_task(
            &mut conn,
            &["gpu-workers".to_string()],
            "worker-imposter",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            claimed_by_imposter.is_none(),
            "a session task must never fail over to a different worker, even \
             after its sticky lease has elapsed"
        );

        // The genuine host can still claim it.
        let claimed_by_host = queue::claim_task(
            &mut conn,
            &["gpu-workers".to_string()],
            "worker-host",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            claimed_by_host.is_some(),
            "the session's host worker must still be able to claim its task \
             after the (bookkeeping-only) sticky lease elapses"
        );
    }

    /// Sanity: an ordinary (non-session) sticky task DOES fail over after its
    /// lease expires -- confirms the test's premise and that the new gate
    /// doesn't accidentally change existing sticky-routing behavior.
    #[tokio::test]
    async fn non_session_sticky_task_does_fail_over_after_lease_expires() {
        let (mut conn, _c) = setup().await;
        let exec_id = insert_execution(&mut conn).await;

        let mut params =
            EnqueueParams::new("gpu-workers", TaskType::Activity, serde_json::json!(null));
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.activity_name = Some("transcode_chunk".to_string());
        let params = params.with_sticky("worker-host", Duration::from_millis(1));

        queue::enqueue(&mut conn, &params).await.expect("enqueue");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let claimed = queue::claim_task(
            &mut conn,
            &["gpu-workers".to_string()],
            "worker-imposter",
            "",
            None,
            &[],
            &[],
        )
        .await
        .expect("claim_task");
        assert!(
            claimed.is_some(),
            "an ordinary sticky (non-session) task must fail over to any \
             worker once its lease expires"
        );
    }
}
