//! LLM token and cost budgets per run and per tenant (issue #1997).
//!
//! A real worker runs a small agent-shaped loop against Postgres. Each loop
//! step is the `llm_step` activity. The step checks the budget, then records
//! the usage of one fake model call in the ledger. The loop stops when a step
//! is refused, and returns how many steps ran.
//!
//! - A run cap stops further steps of one run. Another run is not affected.
//! - A tenant cap stops steps across runs of one key. Another key is not
//!   affected.
//! - A cost cap works as a token cap does.
//! - Spend outside the tenant window does not count.
//! - The ledger row holds the usage in clear columns.
//! - With no LLM cap, no step is refused.
//! - A key that does not resolve fails open for the tenant caps only.
//!
//! Execution: set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run
//! against it directly. Otherwise a fresh testcontainers Postgres boots.

#![cfg(feature = "db")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::items_after_statements
)]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use autumn_harvest::execution::{StartWorkflowParams, start_or_load_workflow_execution};
use autumn_harvest::info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
use autumn_harvest::llm_budget::{LlmBudgetExceeded, LlmUsage};
use autumn_harvest::quota::QuotaPolicy;
use autumn_harvest::telemetry::MetricsRecorder;
use autumn_harvest::types::{
    ExecutionId, Priority, StartSource, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use autumn_harvest::worker::HandlerRegistry;
use autumn_harvest::{ActivityContext, RetryPolicy, WorkflowContext};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::integration_e2e::{
    build_runtime_worker, build_test_pool, setup_test_database_url_or_env, spawn_test_worker,
    wait_for_execution_state,
};

/// The fake model id that every step records.
const MODEL: &str = "test-model-1";

/// The step activity name.
const STEP: &str = "llm_budget_step";

/// Building a registry rewrites the process-global workflow metadata. Tests
/// that build one must not overlap.
static TEST_SERIAL: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// The `llm_step` calls that each run made, refused or not.
static STEP_CALLS: LazyLock<Mutex<HashMap<ExecutionId, u32>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn step_calls(exec_id: ExecutionId) -> u32 {
    STEP_CALLS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&exec_id)
        .copied()
        .unwrap_or(0)
}

type BoxFut<'a> = Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>>;

/// One LLM step: check the budget, then record one fake model call.
fn llm_step(ctx: &ActivityContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        *STEP_CALLS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(ctx.execution_id())
            .or_default() += 1;
        ctx.check_llm_budget().await?;
        let usage = LlmUsage::new(
            MODEL,
            input["input_tokens"].as_u64().unwrap_or(0),
            input["output_tokens"].as_u64().unwrap_or(0),
        )
        .with_cost_micros(input["cost_micros"].as_u64().unwrap_or(0))
        .with_latency(Duration::from_millis(42));
        let recorded = ctx
            .record_llm_usage(&usage)
            .await
            .map_err(|error| error.to_string())?;
        // A paid call that then fails, as a provider fault after the answer.
        if input["fail_first"].as_bool() == Some(true) && ctx.attempt() == 1 {
            return Err("transient fault after a paid call".to_owned());
        }
        Ok(json!({ "recorded": recorded }))
    })
}

/// Run up to `steps` LLM steps. Stop at the first refusal.
fn llm_loop(ctx: &WorkflowContext, input: Value) -> BoxFut<'_> {
    Box::pin(async move {
        let steps = input["steps"].as_u64().unwrap_or(0);
        let queue = ctx.queue_name().to_string();
        let mut ran = 0_u64;
        for _ in 0..steps {
            let outcome = ctx
                .execute_activity_raw_with_opts(
                    STEP,
                    input["step"].clone(),
                    &queue,
                    Some(RetryPolicy::fixed(3, Duration::from_millis(10))),
                    None,
                )
                .await;
            match outcome {
                Ok(_) => ran += 1,
                Err(error) => {
                    let refusal = LlmBudgetExceeded::from_error(&error)
                        .ok_or_else(|| format!("unexpected step error: {error}"))?;
                    return Ok(json!({ "ran": ran, "refused": refusal }));
                }
            }
        }
        // A source run for the reset test fails at the end, so reset admits it.
        if input["fail_end"].as_bool() == Some(true) {
            return Err(format!("failed after {ran} steps"));
        }
        Ok(json!({ "ran": ran, "refused": null }))
    })
}

fn leaked(prefix: &str) -> &'static str {
    Box::leak(format!("{prefix}_{}", Uuid::new_v4().simple()).into_boxed_str())
}

fn wf_info(
    name: &'static str,
    handler: WorkflowHandlerFn,
    quota: Option<QuotaPolicy>,
) -> WorkflowInfo {
    WorkflowInfo {
        quota,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "llm_budget_tests",
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

fn act_info(name: &'static str, handler: ActivityHandlerFn) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "llm_budget_tests",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
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

async fn connect(url: &str) -> AsyncPgConnection {
    AsyncPgConnection::establish(url).await.expect("connect")
}

fn params<'a>(
    workflow_name: &'a str,
    workflow_id: &'a str,
    exec_id: ExecutionId,
    input: Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: input.into(),
        parent_id: None,
        queue_name: "default",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        conflict_policy: WorkflowIdConflictPolicy::Unspecified,
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
    }
}

/// One test's database, workflow type and worker.
struct Harness {
    url: String,
    name: &'static str,
    /// A second workflow type with the same policy.
    other: &'static str,
    metrics: Arc<RejectedMetrics>,
    worker: Arc<autumn_harvest::worker::Worker>,
    handle: Option<tokio::task::JoinHandle<()>>,
    _container: Option<testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>>,
}

impl Harness {
    async fn new(prefix: &str, quota: Option<QuotaPolicy>) -> Self {
        let (url, container) = setup_test_database_url_or_env().await;
        let name = leaked(prefix);
        let other = leaked(&format!("{prefix}_other"));
        let metrics = Arc::new(RejectedMetrics::default());
        let telemetry = Arc::new(
            autumn_harvest::telemetry::TelemetryConfig::builder()
                .metrics(Arc::clone(&metrics) as Arc<dyn MetricsRecorder>)
                .build(),
        );
        let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
            vec![
                wf_info(name, llm_loop, quota),
                wf_info(other, llm_loop, quota),
            ],
            vec![act_info(STEP, llm_step)],
            autumn_harvest::context::empty_shared_state(),
            telemetry,
        ));
        let worker = build_runtime_worker(&format!("w-{name}"), 4, 4, registry);
        let handle = spawn_test_worker(Arc::clone(&worker), build_test_pool(&url));
        Self {
            url,
            name,
            other,
            metrics,
            worker,
            handle: Some(handle),
            _container: container,
        }
    }

    /// Start one run of `steps` steps, each with the given usage.
    async fn start(&self, tenant: Option<&str>, steps: u64, step: Value) -> ExecutionId {
        self.start_type(self.name, tenant, steps, step).await
    }

    /// [`Self::start`] for a chosen workflow type.
    async fn start_type(
        &self,
        workflow: &'static str,
        tenant: Option<&str>,
        steps: u64,
        step: Value,
    ) -> ExecutionId {
        let mut conn = connect(&self.url).await;
        let mut input = json!({ "steps": steps, "step": step });
        if let Some(tenant) = tenant {
            input["tenant"] = json!(tenant);
        }
        let workflow_id = format!("llm-{}", Uuid::new_v4().simple());
        start_or_load_workflow_execution(
            &mut conn,
            params(workflow, &workflow_id, ExecutionId::new(), input),
            None,
        )
        .await
        .expect("start run")
        .exec_id
    }

    /// Wait for the run to complete. Return its output.
    async fn output(&self, exec_id: ExecutionId) -> Value {
        wait_for_execution_state(&self.url, exec_id, "COMPLETED")
            .await
            .output
            .expect("a completed run has an output")
    }

    async fn run(&self, tenant: Option<&str>, steps: u64, step: Value) -> (ExecutionId, Value) {
        let exec_id = self.start(tenant, steps, step).await;
        (exec_id, self.output(exec_id).await)
    }

    async fn stop(mut self) {
        self.worker.shutdown();
        if let Some(handle) = self.handle.take() {
            handle.await.expect("worker join");
        }
    }
}

impl Drop for Harness {
    /// A failed test stops its worker too, so the worker does not run the
    /// steps of a later test.
    fn drop(&mut self) {
        self.worker.shutdown();
    }
}

/// Counts `harvest.quota.rejected` by `(workflow, resource)`.
#[derive(Default)]
struct RejectedMetrics {
    rejected: Mutex<Vec<(String, String)>>,
}

impl RejectedMetrics {
    fn rejected(&self) -> Vec<(String, String)> {
        self.rejected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl MetricsRecorder for RejectedMetrics {
    fn record_quota_rejected(&self, workflow: &str, resource: &str) {
        self.rejected
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((workflow.to_owned(), resource.to_owned()));
    }
}

/// 60 input and 40 output tokens, no cost.
fn hundred_tokens() -> Value {
    json!({ "input_tokens": 60, "output_tokens": 40, "cost_micros": 0 })
}

async fn ledger_rows(url: &str, exec_id: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    let row: Count =
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_llm_ledger WHERE execution_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
            .get_result(&mut conn)
            .await
            .expect("count ledger rows");
    row.n
}

fn refused(output: &Value) -> Option<&str> {
    output["refused"]["resource"].as_str()
}

#[tokio::test]
async fn a_run_budget_stops_further_llm_steps_once_exceeded() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(250);
    let harness = Harness::new("llm_run_cap", Some(policy)).await;

    // 100 tokens a step: 0, 100 and 200 pass. 300 reaches the cap of 250.
    let (first, output) = harness.run(Some("acme"), 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 3, "{output}");
    assert_eq!(refused(&output), Some("run_llm_tokens"));
    assert_eq!(output["refused"]["limit"], 250);
    assert_eq!(output["refused"]["current"], 300);
    assert_eq!(ledger_rows(&harness.url, first).await, 3);
    // The refusal is not retried: three paid steps and one refused call.
    assert_eq!(step_calls(first), 4);
    // The refusal counts on the quota metric, under its LLM resource.
    assert_eq!(
        harness.metrics.rejected(),
        vec![(harness.name.to_owned(), "run_llm_tokens".to_owned())]
    );

    // The cap is per run. A second run of the same tenant spends its own.
    let (second, output) = harness.run(Some("acme"), 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 3, "{output}");
    assert_eq!(refused(&output), Some("run_llm_tokens"));
    assert_eq!(ledger_rows(&harness.url, second).await, 3);
    harness.stop().await;
}

#[tokio::test]
async fn a_tenant_budget_stops_llm_steps_across_runs() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant").with_max_tenant_llm_tokens(250);
    let harness = Harness::new("llm_tenant_cap", Some(policy)).await;

    let (_, output) = harness.run(Some("acme"), 2, hundred_tokens()).await;
    assert_eq!(output["ran"], 2, "{output}");
    assert_eq!(output["refused"], Value::Null);

    // The tenant spent 200 of 250. One more step passes, then the cap holds.
    let (second, output) = harness.run(Some("acme"), 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 1, "{output}");
    assert_eq!(refused(&output), Some("tenant_llm_tokens"));
    assert_eq!(output["refused"]["current"], 300);
    assert_eq!(ledger_rows(&harness.url, second).await, 1);

    // A run of the same tenant now stops at its first step.
    let (third, output) = harness.run(Some("acme"), 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 0, "{output}");
    assert_eq!(refused(&output), Some("tenant_llm_tokens"));
    assert_eq!(step_calls(third), 1);

    // Another tenant has its own budget.
    let (_, output) = harness.run(Some("globex"), 2, hundred_tokens()).await;
    assert_eq!(output["ran"], 2, "{output}");

    // The tenant scope is the workflow type and the key, as for quota. The
    // same key under another type has its own budget.
    let other = harness
        .start_type(harness.other, Some("acme"), 2, hundred_tokens())
        .await;
    let output = harness.output(other).await;
    assert_eq!(output["ran"], 2, "{output}");
    harness.stop().await;
}

#[tokio::test]
async fn a_cost_budget_stops_llm_steps() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant")
        .with_max_run_llm_cost_micros(2_500)
        .with_max_tenant_llm_cost_micros(4_500);
    let harness = Harness::new("llm_cost_cap", Some(policy)).await;
    let step = json!({ "input_tokens": 1, "output_tokens": 1, "cost_micros": 1_000 });

    // The run cap: 0, 1,000 and 2,000 pass. 3,000 reaches 2,500.
    let (_, output) = harness.run(Some("acme"), 10, step.clone()).await;
    assert_eq!(output["ran"], 3, "{output}");
    assert_eq!(refused(&output), Some("run_llm_cost_micros"));

    // The tenant cap: 3,000 and 4,000 pass. 5,000 reaches 4,500.
    let (_, output) = harness.run(Some("acme"), 10, step).await;
    assert_eq!(output["ran"], 2, "{output}");
    assert_eq!(refused(&output), Some("tenant_llm_cost_micros"));
    assert_eq!(output["refused"]["current"], 5_000);
    harness.stop().await;
}

#[tokio::test]
async fn spend_outside_the_window_does_not_count() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant")
        .with_max_tenant_llm_tokens(250)
        .with_tenant_llm_window_secs(3_600);
    let harness = Harness::new("llm_window", Some(policy)).await;

    let (first, output) = harness.run(Some("acme"), 3, hundred_tokens()).await;
    assert_eq!(output["ran"], 3, "{output}");
    // Control: inside the window, the spend of 300 refuses the next step.
    let (_, output) = harness.run(Some("acme"), 1, hundred_tokens()).await;
    assert_eq!(output["ran"], 0, "{output}");
    assert_eq!(refused(&output), Some("tenant_llm_tokens"));

    // Move the spend two hours back, out of the one-hour window.
    let mut conn = connect(&harness.url).await;
    diesel::sql_query(
        "UPDATE harvest_llm_ledger SET recorded_at = recorded_at - INTERVAL '2 hours' \
         WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(first.as_uuid())
    .execute(&mut conn)
    .await
    .expect("back-date the ledger");

    let (_, output) = harness.run(Some("acme"), 2, hundred_tokens()).await;
    assert_eq!(output["ran"], 2, "{output}");
    assert_eq!(output["refused"], Value::Null);
    harness.stop().await;
}

#[tokio::test]
async fn the_ledger_row_holds_the_usage_in_clear_columns() {
    let _serial = TEST_SERIAL.lock().await;
    // A tenant cap makes the run use its key, so the row copies it.
    let policy = QuotaPolicy::new("tenant")
        .with_max_run_llm_tokens(1_000)
        .with_max_tenant_llm_tokens(1_000_000);
    let harness = Harness::new("llm_ledger_row", Some(policy)).await;
    let step = json!({ "input_tokens": 7, "output_tokens": 5, "cost_micros": 1_234 });
    let (exec_id, output) = harness.run(Some("acme"), 1, step).await;
    assert_eq!(output["ran"], 1, "{output}");

    #[derive(diesel::QueryableByName, Debug)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        quota_key: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Text)]
        activity_name: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
        #[diesel(sql_type = diesel::sql_types::Text)]
        model: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        input_tokens: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        output_tokens: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        cost_micros: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        latency_ms: i64,
    }
    let mut conn = connect(&harness.url).await;
    let row: Row = diesel::sql_query(
        "SELECT workflow_name, quota_key, activity_name, attempt, model, input_tokens, \
         output_tokens, cost_micros, latency_ms FROM harvest_llm_ledger WHERE execution_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("one ledger row");
    assert_eq!(row.workflow_name, harness.name);
    assert_eq!(row.quota_key.as_deref(), Some("acme"));
    assert_eq!(row.activity_name, STEP);
    assert_eq!(row.attempt, 1);
    assert_eq!(row.model, MODEL);
    assert_eq!((row.input_tokens, row.output_tokens), (7, 5));
    assert_eq!(row.cost_micros, 1_234);
    assert_eq!(row.latency_ms, 42);
    harness.stop().await;
}

#[tokio::test]
async fn a_retry_that_pays_again_records_again_and_counts() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(150);
    let harness = Harness::new("llm_retry", Some(policy)).await;
    let mut step = hundred_tokens();
    step["fail_first"] = json!(true);

    // Attempt 1 pays 100 and fails. Attempt 2 passes the check at 100 and
    // pays 100 more. The next step then reads 200 and is refused.
    let (exec_id, output) = harness.run(Some("acme"), 5, step).await;
    assert_eq!(output["ran"], 1, "{output}");
    assert_eq!(refused(&output), Some("run_llm_tokens"));
    assert_eq!(output["refused"]["current"], 200);

    #[derive(diesel::QueryableByName)]
    struct Attempt {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        attempt: i32,
    }
    let mut conn = connect(&harness.url).await;
    let attempts: Vec<Attempt> = diesel::sql_query(
        "SELECT attempt FROM harvest_llm_ledger WHERE execution_id = $1 ORDER BY id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load(&mut conn)
    .await
    .expect("ledger attempts");
    let attempts: Vec<i32> = attempts.into_iter().map(|row| row.attempt).collect();
    assert_eq!(attempts, vec![1, 2]);
    harness.stop().await;
}

#[tokio::test]
async fn a_run_only_budget_ignores_an_over_long_key() {
    let _serial = TEST_SERIAL.lock().await;
    // Only run caps: the tenant key is not used, so its length must not
    // refuse the start. The LLM caps do not change admission.
    let policy = QuotaPolicy::new("tenant").with_max_run_llm_tokens(250);
    let harness = Harness::new("llm_run_only_key", Some(policy)).await;
    let long_tenant = "t".repeat(300);
    let (_, output) = harness.run(Some(&long_tenant), 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 3, "{output}");
    assert_eq!(refused(&output), Some("run_llm_tokens"));
    harness.stop().await;
}

#[tokio::test]
async fn a_reset_fork_counts_toward_the_tenant_budget() {
    use autumn_harvest::reset::{
        ResetSignalReapplyPolicy, WorkflowResetRequest, reset_workflow_execution,
    };

    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant").with_max_tenant_llm_tokens(250);
    let harness = Harness::new("llm_reset_fork", Some(policy)).await;
    let mut conn = connect(&harness.url).await;
    let source = start_or_load_workflow_execution(
        &mut conn,
        params(
            harness.name,
            &format!("llm-{}", Uuid::new_v4().simple()),
            ExecutionId::new(),
            json!({ "tenant": "acme", "steps": 3, "step": hundred_tokens(), "fail_end": true }),
        ),
        None,
    )
    .await
    .expect("start the source")
    .exec_id;
    wait_for_execution_state(&harness.url, source, "FAILED").await;
    assert_eq!(ledger_rows(&harness.url, source).await, 3);

    // The fork replays the same input. Its tenant already spent 300 of 250,
    // so its first LLM step is refused.
    //
    // The reset runs as an API-only process would: with no workflow metadata
    // in the process. The fork must still keep the key of its source.
    let metadata = autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .write()
        .expect("metadata lock")
        .take();
    let fork = reset_workflow_execution(
        &mut conn,
        source,
        WorkflowResetRequest {
            reset_to_event_id: Some(0),
            reset_point: None,
            reason: "fork a budgeted run".into(),
            operator_id: "op".into(),
            signal_reapply: ResetSignalReapplyPolicy::default(),
            allow_terminal_source: true,
            refuse_erased_source: false,
        },
        None,
    )
    .await;
    *autumn_harvest::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .write()
        .expect("metadata lock") = metadata;
    let fork = fork.expect("reset").new_exec_id;
    let output = harness.output(fork).await;
    assert_eq!(output["ran"], 0, "{output}");
    assert_eq!(refused(&output), Some("tenant_llm_tokens"));
    harness.stop().await;
}

#[tokio::test]
async fn a_run_with_no_llm_cap_is_never_refused() {
    let _serial = TEST_SERIAL.lock().await;
    let harness = Harness::new("llm_no_cap", None).await;
    let (exec_id, output) = harness.run(Some("acme"), 5, hundred_tokens()).await;
    assert_eq!(output["ran"], 5, "{output}");
    // The usage is still recorded, with no key.
    assert_eq!(ledger_rows(&harness.url, exec_id).await, 5);
    harness.stop().await;
}

#[tokio::test]
async fn an_unresolvable_key_fails_open_for_the_tenant_caps_only() {
    let _serial = TEST_SERIAL.lock().await;
    let policy = QuotaPolicy::new("tenant")
        .with_max_tenant_llm_tokens(1)
        .with_max_run_llm_tokens(250);
    let harness = Harness::new("llm_no_key", Some(policy)).await;
    // No `tenant` field: the tenant cap of 1 does not apply, the run cap does.
    let (_, output) = harness.run(None, 10, hundred_tokens()).await;
    assert_eq!(output["ran"], 3, "{output}");
    assert_eq!(refused(&output), Some("run_llm_tokens"));
    harness.stop().await;
}
