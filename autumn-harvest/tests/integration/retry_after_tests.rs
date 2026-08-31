//! Activity-driven next-retry delay — honor downstream `Retry-After` (issue #744).
//!
//! `ActivityFailure::with_retry_after(Duration)` lets an activity author hand the
//! engine a downstream-supplied delay hint (e.g. an HTTP `Retry-After` header) so
//! the NEXT retry attempt is scheduled at `now + hint` instead of the
//! `RetryPolicy`-computed backoff, for that one attempt only.
//!
//! `ActivityFailure`/`resolve_retry_after_hint` themselves are pure and already
//! covered by exhaustive unit tests in `src/failure.rs` and `src/policy.rs`
//! (including the 100-randomized-hints success-metric property test). This file
//! covers the two things those unit tests cannot:
//!
//! - **AC6 (replay determinism)** — a history produced by a retry_after-delayed
//!   retry contains nothing a replay could observe differently (`replay_tests`,
//!   requires only the `testing` feature; no DB).
//! - **AC2 / AC3 / AC4 / AC5 / the success metric** — proving the hint actually
//!   reaches the real `harvest_task_queue.scheduled_at` column end-to-end
//!   through a live worker (`db_tests`, requires the `db` feature + Postgres).
//! - **Local activities** (`#[activity(local = true)]`) — a Codex review finding
//!   on PR #1140 caught that `run_local_activity_inline`'s retry-sleep still read
//!   only `run.retry_policy.next_delay(attempt)`, ignoring `retry_after` and the
//!   ceiling entirely (local activities retry INLINE in the workflow task, never
//!   through `harvest_task_queue`, so this is a genuinely separate code path from
//!   the remote/task-queue retry loop `next_retry_delay` governs above). Covered
//!   by `local_retry_after_end_to_end_test` (`db_tests`) plus a dedicated pure
//!   unit-test suite on the extracted `worker::local_retry_delay` helper.
//!
//! RED PHASE (TDD): before `worker.rs`'s `next_retry_delay`/`handle_activity_result`
//! consult `ActivityFailure::retry_after` / `resolve_retry_after_hint`, every test
//! in `db_tests` below fails at RUNTIME (the observed `scheduled_at` reflects the
//! unmodified `RetryPolicy` delay, not the hint) — this file compiles against
//! today's code (no new public API is exercised here beyond the already-shipped
//! `ActivityFailure::with_retry_after` / `HandlerRegistry::with_retry_after_ceiling`
//! builder methods), so RED is a runtime assertion failure, not a compile error.

// ---------------------------------------------------------------------------
// AC6 — replay determinism: a retry_after-influenced history replays unchanged.
// ---------------------------------------------------------------------------
//
// `retry_after` never appears in `harvest_events`: an in-progress (non-terminal)
// retry attempt leaves NO event at all (only the task row's `scheduled_at`/`error`
// columns change), and the eventual terminal `ActivityFailed`/`ActivityCompleted`
// event carries no `retry_after` field. So a history that resulted from a
// retry_after-delayed retry is byte-identical to one that did not, and replays
// identically. This test locks that invariant in explicitly.
#[cfg(feature = "testing")]
mod replay_tests {
    use std::future::Future;
    use std::pin::Pin;

    use autumn_harvest::context::WorkflowContext;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
    use autumn_harvest::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;
    use serde_json::Value;

    fn failing_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            ctx.execute_activity_raw("call_api", Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
            unreachable!("the recorded history always ends in a terminal ActivityFailed")
        })
    }

    /// A history whose terminal `ActivityFailed.attempt == 2` is what a
    /// two-attempt run — one retry, delayed by a `retry_after` hint rather than
    /// the policy's own backoff — produces: the intermediate (non-terminal)
    /// attempt-1 failure leaves no event at all, only the transient task row's
    /// `scheduled_at`/`error` columns change, so no `retry_after`-specific
    /// event ever exists to replay differently.
    ///
    /// This snapshot is deliberately SIMPLIFIED relative to what a real worker
    /// would record: a genuine run also appends an `ActivityStarted` event on
    /// EVERY claim attempt (`worker::append_activity_started_if_pending`), so
    /// a real two-attempt history would carry two `ActivityStarted` events
    /// this snapshot omits entirely. That omission does not weaken the test —
    /// `HistoryMatcher::scan_activity_terminal`'s forward scan explicitly
    /// tolerates any number (including zero) of `ActivityStarted` events for a
    /// matching `activity_id` before it reaches the terminal event, precisely
    /// so a snapshot like this one — carrying only the events replay actually
    /// depends on — still exercises the real matcher path. Replay must
    /// succeed regardless of what wall-clock delay (or `ActivityStarted`
    /// count) produced this sequence.
    #[tokio::test]
    async fn retry_after_delayed_retry_history_replays_unchanged() {
        let exec_id = ExecutionId::new();
        let act_id = ActivityExecId::new();

        let events = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id: act_id,
                name: "call_api".into(),
                input: Value::Null,
                queue: "default".into(),
            },
            // The one and only recorded terminal event -- no trace of the
            // retry_after hint that governed the attempt-1 -> attempt-2 delay.
            WorkflowEvent::ActivityFailed {
                activity_id: act_id,
                error: "rate limited".into(),
                attempt: 2,
                error_type: "Http429".into(),
                non_retryable: false,
                details: None,
            },
        ];

        let snapshot = HistorySnapshot {
            workflow_name: "retry_after_wf".into(),
            execution_id: exec_id,
            events,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
        };

        let report = WorkflowReplayer::new()
            .register_fn("retry_after_wf", failing_workflow)
            .replay_from_snapshot(snapshot)
            .await;

        assert!(
            matches!(report.status, ReplayStatus::WorkflowFailed { .. }),
            "a retry_after-delayed-retry history must replay to the same \
             WorkflowFailed outcome a plain policy-driven retry would produce, \
             with no divergence: {report:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// AC2 / AC3 / AC4 / AC5 / success metric — end-to-end DB integration tests.
// ---------------------------------------------------------------------------
#[cfg(feature = "db")]
mod db_tests {
    use std::any::TypeId;
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
    use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
    use autumn_harvest::models::{NewWorkflowExecution, TaskQueueItem, WorkflowExecution};
    use autumn_harvest::queue::{self, EnqueueParams, TaskType};
    use autumn_harvest::schema::{harvest_task_queue, harvest_workflow_executions};
    use autumn_harvest::telemetry::TelemetryConfig;
    use autumn_harvest::types::ExecutionId;
    use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
    use autumn_harvest::{RetryPolicy, WorkflowContext, store};

    use chrono::Utc;
    use diesel::prelude::*;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use testcontainers::ContainerAsync;
    use testcontainers::ImageExt;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use uuid::Uuid;

    // -----------------------------------------------------------------------
    // DB setup — env-var-redirectable, otherwise testcontainers.
    // -----------------------------------------------------------------------

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

    fn build_pool(url: &str) -> DbPool {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        deadpool::managed::Pool::builder(manager)
            .max_size(8)
            .build()
            .expect("pool build failed")
    }

    async fn connect(url: &str) -> AsyncPgConnection {
        <AsyncPgConnection as AsyncConnection>::establish(url)
            .await
            .expect("failed to connect to Postgres")
    }

    // -----------------------------------------------------------------------
    // Handlers.
    // -----------------------------------------------------------------------

    type BoxFut<'a> =
        Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

    /// Workflow: schedule one activity named "echo" via the plain raw path (no
    /// per-call overrides) onto the workflow's own queue, then suspend on it.
    fn wf_one_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
        Box::pin(async move {
            let queue = ctx.queue_name().to_string();
            ctx.execute_activity_raw("echo", input, &queue)
                .await
                .map_err(|e| e.to_string())
        })
    }

    /// Always fails with a fast (2s) `retry_after` hint, while the registered
    /// `RetryPolicy` would compute a 999s delay -- proves the hint OVERRIDES the
    /// policy delay (AC2) without granting extra attempts (the policy's
    /// `max_attempts` cap is untouched).
    fn fail_retry_after_2s(
        _ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            Err(ActivityFailure::retryable("Http429", "rate limited")
                .with_retry_after(Duration::from_secs(2))
                .into_error_payload())
        })
    }

    /// Always fails with a `retry_after` hint (999s) far over a LOW configured
    /// ceiling -- proves the hint is CLAMPED to the ceiling, never rejected
    /// (AC3).
    fn fail_retry_after_over_ceiling(
        _ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            Err(ActivityFailure::retryable("Http429", "rate limited")
                .with_retry_after(Duration::from_secs(999))
                .into_error_payload())
        })
    }

    /// Always fails with `retry_after = Duration::ZERO` -- proves a zero hint
    /// falls through to the normal policy-computed delay rather than retrying
    /// immediately (AC3).
    fn fail_retry_after_zero(
        _ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            Err(ActivityFailure::retryable("Http429", "rate limited")
                .with_retry_after(Duration::ZERO)
                .into_error_payload())
        })
    }

    /// Fails NON-retryably with a (fast) `retry_after` hint present -- proves
    /// `non_retryable` wins: the task must go straight to terminal failure,
    /// never requeued, regardless of the hint (AC4).
    fn fail_non_retryable_with_retry_after(
        _ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            Err(ActivityFailure::non_retryable("Fatal", "boom")
                .with_retry_after(Duration::from_millis(1))
                .into_error_payload())
        })
    }

    /// Always fails with a LARGE (5s) `retry_after` hint while the registered
    /// `RetryPolicy` delay is small (50ms) -- used to prove the interaction
    /// with `schedule_to_close` (AC5): the hint alone crosses a tight
    /// `schedule_to_close` deadline that the policy's own small delay would
    /// not have crossed.
    fn fail_retry_after_5s(
        _ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            Err(ActivityFailure::retryable("Http429", "rate limited")
                .with_retry_after(Duration::from_secs(5))
                .into_error_payload())
        })
    }

    /// Workflow: schedule a LOCAL activity (retries run INLINE in the workflow
    /// task, never through `harvest_task_queue`) with an explicit retry policy
    /// passed at the call site.
    fn wf_one_local_activity(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
        Box::pin(async move {
            ctx.execute_local_activity_raw(
                "local_echo",
                input,
                Some(RetryPolicy::fixed(2, Duration::from_secs(999))),
                None,
            )
            .await
            .map_err(|e| e.to_string())
        })
    }

    /// Local activity: records `Instant::now()` on every attempt (via shared
    /// state, since `ActivityHandlerFn` is a plain fn pointer with no closure
    /// capture), fails attempt 1 with a fast (1.5s) `retry_after` hint against a
    /// vastly slower (999s) registered policy delay, then succeeds on attempt 2.
    /// If the hint is ignored (the bug this test exists to catch), the observed
    /// gap between the two timestamps would be ~999s, not ~1.5s.
    fn local_retry_after_2s(
        ctx: &autumn_harvest::ActivityContext,
        _input: serde_json::Value,
    ) -> BoxFut<'_> {
        Box::pin(async move {
            let stamps = Arc::clone(
                ctx.state::<Arc<std::sync::Mutex<Vec<std::time::Instant>>>>()
                    .expect("attempt-timestamp state should be registered"),
            );
            stamps
                .lock()
                .expect("timestamp lock poisoned")
                .push(std::time::Instant::now());
            if ctx.attempt() == 1 {
                return Err(ActivityFailure::retryable("Http429", "rate limited")
                    .with_retry_after(Duration::from_millis(1500))
                    .into_error_payload());
            }
            Ok(serde_json::json!("ok"))
        })
    }

    // -----------------------------------------------------------------------
    // Registry / worker construction.
    // -----------------------------------------------------------------------

    fn wf_info(
        name: &'static str,
        handler: autumn_harvest::info::WorkflowHandlerFn,
    ) -> WorkflowInfo {
        WorkflowInfo {
            quota: None,
            declared_activities: None,
            declared_children: None,
            mcp: false,
            name,
            module: "retry_after_tests",
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

    fn act_info(
        name: &'static str,
        handler: autumn_harvest::info::ActivityHandlerFn,
        retry: Option<RetryPolicy>,
        default_schedule_to_close: Option<Duration>,
    ) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "retry_after_tests",
            default_retry_policy: retry,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close,
            default_queue: Some("default"),
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: false,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler,
        }
    }

    /// Build a registry with the given activity registration and an optional
    /// `retry_after_ceiling` override (defaults to the builder-level constant
    /// when `None`).
    fn build_registry(
        workflows: Vec<WorkflowInfo>,
        activities: Vec<ActivityInfo>,
        retry_after_ceiling: Option<Duration>,
    ) -> Arc<HandlerRegistry> {
        build_registry_with_state(
            workflows,
            activities,
            retry_after_ceiling,
            autumn_harvest::context::empty_shared_state(),
        )
    }

    /// Same as [`build_registry`] but with caller-supplied typed shared state
    /// (used by the local-activity test to inject an `Arc<Mutex<Vec<Instant>>>`
    /// the handler records attempt timestamps into).
    fn build_registry_with_state(
        workflows: Vec<WorkflowInfo>,
        activities: Vec<ActivityInfo>,
        retry_after_ceiling: Option<Duration>,
        state: autumn_harvest::context::SharedState,
    ) -> Arc<HandlerRegistry> {
        let telemetry = Arc::new(TelemetryConfig::default());
        let mut registry =
            HandlerRegistry::with_state_and_telemetry(workflows, activities, state, telemetry);
        if let Some(ceiling) = retry_after_ceiling {
            registry = registry.with_retry_after_ceiling(ceiling);
        }
        Arc::new(registry)
    }

    /// `ActivityInfo` for a local (`is_local: true`) activity. Local activities
    /// have no `harvest_task_queue` row, so `default_queue` is irrelevant; the
    /// retry policy is supplied at the call site
    /// (`ctx.execute_local_activity_raw`), not read from this info, so it is
    /// left `None` here.
    fn local_act_info(
        name: &'static str,
        handler: autumn_harvest::info::ActivityHandlerFn,
    ) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "retry_after_tests",
            default_retry_policy: None,
            default_start_to_close: Some(Duration::from_secs(20)),
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_schedule_to_close: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            rate_limit_rps: None,
            rate_limit_burst: None,
            rate_limit_key: None,
            rate_limit_key_expr: None,
            circuit_breaker: None,
            is_local: true,
            max_input_bytes: None,
            max_result_bytes: None,
            requires: None,
            handler,
        }
    }

    fn build_worker(worker_id: &str, queue: &str, registry: Arc<HandlerRegistry>) -> Arc<Worker> {
        Arc::new(
            Worker::new(
                WorkerRuntimeConfig {
                    codec_rotation_batch_size: 0,
                    dr_fencing: false,
                    worker_id: worker_id.to_string(),
                    queues: vec![queue.to_string()],
                    notification_database_url: None,
                    max_concurrent_workflows: 1,
                    max_concurrent_activities: 1,
                    poll_interval: Duration::from_millis(25),
                    shutdown_timeout: Duration::from_secs(1),
                    cancellation_grace_period: Duration::from_secs(1),
                    sticky_timeout: Duration::from_secs(5),
                    max_local_activity_start_to_close: Duration::from_secs(60),
                    shard_assignments: vec![autumn_harvest::types::ShardId::new(0)],
                    worker_heartbeat_interval: Duration::from_secs(5),
                    build_id: String::new(),
                    deployment_name: None,
                    workflow_cache_size: 1000,
                    priority_aging_secs: None,
                    unknown_target_grace_window: Duration::from_secs(5),
                    poison_pill_threshold: 3,
                    capability_miss_max_redeliveries: 5,
                    workflow_task_timeout: Duration::from_secs(10),
                    workflow_panic_max_attempts: 3,
                    labels: HashMap::new(),
                    queue_weights: HashMap::new(),
                    max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                    max_workflow_history_events: None,
                    shard_notification_database_urls: Vec::new(),
                    sharded_pool: None,
                    slot_tuner: None,
                    max_concurrent_sessions: 0,
                },
                registry,
            )
            .expect("worker should build"),
        )
    }

    // -----------------------------------------------------------------------
    // Seeding + read helpers.
    // -----------------------------------------------------------------------

    async fn seed_workflow(
        conn: &mut AsyncPgConnection,
        workflow_name: &'static str,
        input: serde_json::Value,
        queue: &str,
    ) -> ExecutionId {
        let exec_id = ExecutionId::new_for_shard(autumn_harvest::types::ShardId::new(0));
        let row = NewWorkflowExecution {
            quota_key: None,
            id: exec_id.as_uuid(),
            workflow_name,
            workflow_id: &format!("wf-{}", exec_id.as_uuid()),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            input: input.clone(),
            parent_id: None,
            queue_name: queue,
            execution_timeout: None,
            deadline_at: None,
            chain_execution_timeout: None,
            chain_deadline_at: None,
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
            continued_from_exec_id: None,
            first_exec_id: None,
            start_source: None,
            start_source_ref: None,
            started_by: None,
        };
        diesel::insert_into(harvest_workflow_executions::table)
            .values(&row)
            .execute(conn)
            .await
            .expect("insert workflow execution");

        store::append_events(
            conn,
            exec_id,
            &[WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            0,
        )
        .await
        .expect("append WorkflowStarted");

        let mut params = EnqueueParams::new(queue, TaskType::Workflow, input);
        params.workflow_exec_id = Some(exec_id.as_uuid());
        params.scheduled_at = Utc::now() - chrono::Duration::seconds(5);
        queue::enqueue(conn, &params)
            .await
            .expect("enqueue workflow task");

        exec_id
    }

    async fn load_execution(url: &str, exec_id: ExecutionId) -> WorkflowExecution {
        let mut conn = connect(url).await;
        harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select(WorkflowExecution::as_select())
            .first(&mut conn)
            .await
            .expect("reload workflow execution")
    }

    async fn load_history(url: &str, exec_id: ExecutionId) -> Vec<WorkflowEvent> {
        let mut conn = connect(url).await;
        store::load_history(&mut conn, exec_id)
            .await
            .expect("load_history")
            .events
    }

    /// Load the single enqueued *activity* task row for `activity_name`.
    async fn load_activity_task(
        url: &str,
        exec_id: ExecutionId,
        activity_name: &str,
    ) -> TaskQueueItem {
        let mut conn = connect(url).await;
        let rows: Vec<TaskQueueItem> = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id.as_uuid())))
            .filter(harvest_task_queue::activity_name.eq(Some(activity_name.to_string())))
            .select(TaskQueueItem::as_select())
            .load(&mut conn)
            .await
            .expect("reload task rows");
        assert_eq!(
            rows.len(),
            1,
            "expected exactly one enqueued activity task for '{activity_name}'"
        );
        rows.into_iter().next().unwrap()
    }

    async fn wait_for_state(
        url: &str,
        exec_id: ExecutionId,
        want: &str,
        timeout: Duration,
    ) -> WorkflowExecution {
        tokio::time::timeout(timeout, async {
            loop {
                let e = load_execution(url, exec_id).await;
                if e.state == want {
                    break e;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("execution did not reach state {want} within {timeout:?}"))
    }

    /// Drive a worker until `exec_id` reaches `want`, then shut it down cleanly.
    async fn run_to_state(
        url: &str,
        pool: &DbPool,
        worker: Arc<Worker>,
        exec_id: ExecutionId,
        want: &str,
        timeout: Duration,
    ) -> WorkflowExecution {
        let runner = Arc::clone(&worker);
        let pool_for_run = pool.clone();
        let handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
        let execution = wait_for_state(url, exec_id, want, timeout).await;
        worker.shutdown();
        handle.await.expect("worker task joins cleanly");
        execution
    }

    // -------------------------------------------------------------------
    // AC2 -- retry_after overrides the policy's computed delay for that
    // attempt only; the policy's attempt-cap is unaffected.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_after_overrides_policy_delay_without_granting_extra_attempts() {
        let (url, _container) = setup_db().await;
        let queue = "q744-override";
        let mut conn = connect(&url).await;
        let exec_id =
            seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({}), queue).await;
        let t_before = Utc::now();

        // Policy delay would be 999s; max_attempts = 2, so exactly one retry
        // (attempt 1 -> attempt 2) happens before the terminal failure.
        let registry = build_registry(
            vec![wf_info("wf_one_activity", wf_one_activity)],
            vec![act_info(
                "echo",
                fail_retry_after_2s,
                Some(RetryPolicy::fixed(2, Duration::from_secs(999))),
                None,
            )],
            None,
        );
        let worker = build_worker("w744-override", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        let execution = run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(
            execution.state, "FAILED",
            "the policy's max_attempts=2 cap must still terminate the run \
             (retry_after must not grant unlimited retries)"
        );

        let task = load_activity_task(&url, exec_id, "echo").await;
        assert_eq!(
            task.attempt, 2,
            "exactly max_attempts (2) attempts must have been made -- \
             retry_after must not change the attempt-cap decision"
        );
        assert_eq!(task.state, "FAILED");

        // The ONE requeue (after attempt 1's failure) set scheduled_at to
        // roughly t_before + 2s (the retry_after hint), never anywhere near
        // t_before + 999s (the policy's own delay).
        let observed_delay = task.scheduled_at - t_before;
        assert!(
            observed_delay > chrono::Duration::milliseconds(500)
                && observed_delay < chrono::Duration::seconds(30),
            "expected the retry_after hint (~2s) to govern the retry delay, \
             not the policy's 999s backoff; observed delay = {observed_delay:?}"
        );

        let history = load_history(&url, exec_id).await;
        assert!(
            history.iter().any(|e| matches!(
                e,
                WorkflowEvent::ActivityFailed { error, .. } if error == "rate limited"
            )),
            "the terminal ActivityFailed must carry the human-readable message, \
             not the wire envelope; history: {history:?}"
        );
    }

    // -------------------------------------------------------------------
    // AC3 -- an over-ceiling retry_after hint is clamped, never rejected.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_after_over_ceiling_is_clamped_not_rejected() {
        let (url, _container) = setup_db().await;
        let queue = "q744-clamp";
        let mut conn = connect(&url).await;
        let exec_id =
            seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({}), queue).await;
        let t_before = Utc::now();

        // A 1s ceiling; the hint (999s) must be clamped down to ~1s.
        let registry = build_registry(
            vec![wf_info("wf_one_activity", wf_one_activity)],
            vec![act_info(
                "echo",
                fail_retry_after_over_ceiling,
                Some(RetryPolicy::fixed(2, Duration::from_secs(999))),
                None,
            )],
            Some(Duration::from_secs(1)),
        );
        let worker = build_worker("w744-clamp", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;

        let task = load_activity_task(&url, exec_id, "echo").await;
        let observed_delay = task.scheduled_at - t_before;
        assert!(
            observed_delay > chrono::Duration::milliseconds(200)
                && observed_delay < chrono::Duration::seconds(30),
            "a 999s hint against a 1s ceiling must clamp to ~1s, never the raw \
             999s hint and never rejected outright; observed delay = {observed_delay:?}"
        );
    }

    // -------------------------------------------------------------------
    // AC3 -- retry_after <= 0 falls through to the normal policy delay.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_retry_after_falls_through_to_policy_delay() {
        let (url, _container) = setup_db().await;
        let queue = "q744-zero-hint";
        let mut conn = connect(&url).await;
        let exec_id =
            seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({}), queue).await;
        let t_before = Utc::now();

        // Policy delay is 2s; retry_after is ZERO, so the policy's 2s delay
        // must be honored instead of an immediate/zero-delay retry.
        let registry = build_registry(
            vec![wf_info("wf_one_activity", wf_one_activity)],
            vec![act_info(
                "echo",
                fail_retry_after_zero,
                Some(RetryPolicy::fixed(2, Duration::from_secs(2))),
                None,
            )],
            None,
        );
        let worker = build_worker("w744-zero-hint", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;

        let task = load_activity_task(&url, exec_id, "echo").await;
        let observed_delay = task.scheduled_at - t_before;
        assert!(
            observed_delay > chrono::Duration::milliseconds(500)
                && observed_delay < chrono::Duration::seconds(30),
            "a zero retry_after hint must fall through to the policy's own 2s \
             delay, not retry immediately; observed delay = {observed_delay:?}"
        );
    }

    // -------------------------------------------------------------------
    // AC4 -- non_retryable wins over retry_after: immediate terminal failure.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_retryable_wins_over_retry_after() {
        let (url, _container) = setup_db().await;
        let queue = "q744-non-retryable";
        let mut conn = connect(&url).await;
        let exec_id =
            seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({}), queue).await;

        // Plenty of attempts remain (5) and the hint is fast (1ms) -- if
        // non_retryable were ignored we would see several fast retries.
        let registry = build_registry(
            vec![wf_info("wf_one_activity", wf_one_activity)],
            vec![act_info(
                "echo",
                fail_non_retryable_with_retry_after,
                Some(RetryPolicy::fixed(5, Duration::from_secs(1))),
                None,
            )],
            None,
        );
        let worker = build_worker("w744-non-retryable", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;

        let task = load_activity_task(&url, exec_id, "echo").await;
        assert_eq!(
            task.attempt, 1,
            "non_retryable must route straight to terminal failure on the \
             FIRST attempt -- no retry, regardless of the retry_after hint"
        );
        assert_eq!(task.state, "FAILED");

        let history = load_history(&url, exec_id).await;
        assert!(
            history.iter().any(|e| matches!(
                e,
                WorkflowEvent::ActivityFailed { non_retryable: true, error_type, .. }
                    if error_type == "Fatal"
            )),
            "history must record the non-retryable terminal failure; history: {history:?}"
        );
    }

    // -------------------------------------------------------------------
    // AC5 -- retry_after crossing schedule_to_close fires the existing
    // deadline-exceeded path instead of requeuing.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retry_after_crossing_schedule_to_close_deadline_fires_timeout_not_requeue() {
        let (url, _container) = setup_db().await;
        let queue = "q744-stc-interaction";
        let mut conn = connect(&url).await;
        let exec_id =
            seed_workflow(&mut conn, "wf_one_activity", serde_json::json!({}), queue).await;

        // schedule_to_close = 2s, set once at the FIRST schedule (~test
        // start) and never refreshed on retry. `fail_retry_after_5s` returns
        // retry_after = 5s on every attempt, including the very first one --
        // so the deadline check fires on attempt 1's failure, not after any
        // accumulated retries: `handle_activity_result` calls
        // `schedule_to_close_deadline_exceeded(task, delay)` right after
        // EVERY failed attempt (this file's own worker.rs read confirms
        // there is no "wait until near-exhaustion" special case), and
        // `now_at_attempt_1_failure + 5s` is *mathematically* guaranteed to
        // exceed `enqueue_time + 2s` by a full 3-second margin, since
        // `now_at_attempt_1_failure >= enqueue_time` always holds (time only
        // moves forward) -- no wall-clock race, however slow the runner.
        //
        // Without the retry_after fix, `next_retry_delay` would use the
        // 50ms/attempt policy delay instead (which never crosses 2s across
        // all 5 attempts), so the run would instead exhaust its 5 retries
        // and reach a plain retry-exhaustion `ActivityFailed` with NO
        // `ActivityTimedOut { ScheduleToClose }` in history at all -- that is
        // the actual RED/GREEN discriminator this test locks in, not a
        // timing margin.
        let registry = build_registry(
            vec![wf_info("wf_one_activity", wf_one_activity)],
            vec![act_info(
                "echo",
                fail_retry_after_5s,
                Some(RetryPolicy::fixed(5, Duration::from_millis(50))),
                Some(Duration::from_secs(2)),
            )],
            None,
        );
        let worker = build_worker("w744-stc-interaction", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "FAILED",
            Duration::from_secs(20),
        )
        .await;

        let history = load_history(&url, exec_id).await;
        assert!(
            history.iter().any(|e| matches!(
                e,
                WorkflowEvent::ActivityTimedOut {
                    timeout_type: autumn_harvest::TimeoutType::ScheduleToClose,
                    ..
                }
            )),
            "a retry_after hint that would cross schedule_to_close must fire \
             ActivityTimedOut {{ ScheduleToClose }}; history: {history:?}"
        );
        assert!(
            !history
                .iter()
                .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
            "no plain ActivityFailed should appear -- the deadline check must \
             short-circuit the requeue, not race it; history: {history:?}"
        );
    }

    // -------------------------------------------------------------------
    // Codex review (PR #1140) -- local activities (`local = true`) must
    // honor `retry_after` too. Local-activity retries run INLINE in the
    // workflow task via `run_local_activity_inline`, a code path entirely
    // separate from the remote/task-queue `next_retry_delay` the tests
    // above cover; before this fix it read only
    // `run.retry_policy.next_delay(attempt)`, silently ignoring both the
    // hint and the configured ceiling.
    // -------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_retry_after_end_to_end_test() {
        let (url, _container) = setup_db().await;
        let queue = "q744-local-retry-after";
        let mut conn = connect(&url).await;
        let exec_id = seed_workflow(
            &mut conn,
            "wf_one_local_activity",
            serde_json::json!({}),
            queue,
        )
        .await;

        let stamps: Arc<Mutex<Vec<std::time::Instant>>> = Arc::new(Mutex::new(Vec::new()));
        let mut state_map = HashMap::new();
        state_map.insert(
            TypeId::of::<Arc<Mutex<Vec<std::time::Instant>>>>(),
            Box::new(Arc::clone(&stamps)) as Box<dyn std::any::Any + Send + Sync>,
        );

        let registry = build_registry_with_state(
            vec![wf_info("wf_one_local_activity", wf_one_local_activity)],
            vec![local_act_info("local_echo", local_retry_after_2s)],
            None,
            Arc::new(state_map),
        );
        let worker = build_worker("w744-local-retry-after", queue, Arc::clone(&registry));
        let pool = build_pool(&url);

        let execution = run_to_state(
            &url,
            &pool,
            worker,
            exec_id,
            "COMPLETED",
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(execution.state, "COMPLETED");

        let recorded = stamps.lock().expect("timestamp lock poisoned").clone();
        assert_eq!(
            recorded.len(),
            2,
            "expected exactly two attempts (fail once, succeed on retry); recorded: {recorded:?}"
        );
        let observed_delay = recorded[1].duration_since(recorded[0]);
        assert!(
            observed_delay > Duration::from_millis(500) && observed_delay < Duration::from_secs(30),
            "expected the retry_after hint (~1.5s) to govern the local-activity \
             retry delay, not the registered policy's 999s backoff; observed \
             delay = {observed_delay:?}"
        );
    }
}
