#![cfg(feature = "db")]
//! The resident hit rate of the real agent loop (issue #2007).
//!
//! The test runs `autumn_harvest_agent::agent_loop` on one Postgres worker
//! with a scripted model. The model asks for two allowed tool calls, then a
//! gated call, then answers. The test asserts the resident outcome of each
//! decision, so it shows which agent shapes resume and which replay.
//!
//! Each model turn and each allowed tool call awaits one activity, so those
//! decisions resume. A gated call waits for its approval signal with a
//! deadline. That wait is a race, so the decision after it replays.
//!
//! `docs/rnd/typed-state-snapshots.md` records the rate. Print it with
//! `-- --nocapture`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::telemetry::{MetricsRecorder, NoOpPropagator, TelemetryConfig};
use autumn_harvest::types::ExecutionId;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    HarvestBuilder, StartWorkflowParams, WorkerConfig, start_or_load_workflow_execution,
};
use autumn_harvest_agent::approval::{Approval, approval_signal};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentTask, ChatRequest, ChatResponse, ContentPart,
    ErrorKind, FnTool, RunInfo, StopReason, TokenUsage, Tool, ToolCall, ToolDecision, ToolPolicy,
};
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};

use crate::integration_e2e::{
    build_test_pool, setup_test_database_url_or_env, wait_for_execution_state_with_timeout,
};

/// Each resident outcome of one worker, as `outcome/reason`, in order.
#[derive(Debug, Default)]
struct ResidentLog(Mutex<Vec<String>>);

impl ResidentLog {
    fn outcomes(&self) -> Vec<String> {
        self.0.lock().expect("log lock").clone()
    }
}

impl MetricsRecorder for ResidentLog {
    fn record_workflow_resident(
        &self,
        _workflow_name: &str,
        _queue: &str,
        outcome: &str,
        reason: &str,
    ) {
        self.0
            .lock()
            .expect("log lock")
            .push(format!("{outcome}/{reason}"));
    }
}

/// A model that answers from a fixed script.
#[derive(Debug)]
struct Script(Mutex<Vec<ChatResponse>>);

impl AgentModel for Script {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ChatResponse, AgentError>> + Send + 'a>> {
        let mut replies = self.0.lock().expect("script lock");
        let reply = if replies.is_empty() {
            Err(AgentError::new(ErrorKind::Provider, "script ran out"))
        } else {
            Ok(replies.remove(0))
        };
        Box::pin(async move { reply })
    }
}

/// A turn that asks for one tool call.
fn call(id: &str, name: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::ToolCall {
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: json!({}),
        }],
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(1, 0),
    }
}

/// A final answer.
fn answer(text: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(text.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(1, 0),
    }
}

/// Gates the `pay` tool and allows every other tool.
#[derive(Debug)]
struct GatePay;

impl ToolPolicy for GatePay {
    fn decide<'a>(
        &'a self,
        call: &'a ToolCall,
        _tool: Option<&'a dyn Tool>,
        _info: &'a RunInfo,
    ) -> futures::future::BoxFuture<'a, ToolDecision> {
        let decision = if call.name == "pay" {
            ToolDecision::RequireApproval {
                reason: "money moves".to_owned(),
            }
        } else {
            ToolDecision::Allow
        };
        Box::pin(async move { decision })
    }
}

fn echo_tool(name: &str) -> Arc<dyn Tool> {
    FnTool::new(
        name,
        "A test tool.",
        json!({"type": "object"}),
        |input: Value| async move { Ok(json!({ "echo": input })) },
    )
    .shared()
}

fn unique(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &suffix[..12])
}

fn build_worker(queue: &str, log: Arc<ResidentLog>) -> Arc<Worker> {
    let script = Script(Mutex::new(vec![
        call("c1", "lookup"),
        call("c2", "search"),
        call("c3", "pay"),
        answer("Done."),
    ]));
    let harness = AgentHarness::new(Arc::new(script))
        .tools([echo_tool("lookup"), echo_tool("search"), echo_tool("pay")])
        .policy(Arc::new(GatePay));
    let built = HarvestBuilder::new()
        .workflows(autumn_harvest_agent::workflow::workflows())
        .activities(autumn_harvest_agent::workflow::activities())
        .state(harness)
        .telemetry(TelemetryConfig {
            service_name: Arc::from("resident_hit_rate_db_tests"),
            propagator: Arc::new(NoOpPropagator),
            metrics: log as Arc<dyn MetricsRecorder>,
        })
        // The agent activities run on the `default` queue. CI runs this
        // suite serially on its own database, so no other worker polls it.
        .worker(WorkerConfig::default().with_queues([queue, "default"]))
        .build();
    let (registry, _dags, _schedules, worker_config) = built.into_worker_parts();
    let mut runtime: WorkerRuntimeConfig = worker_config.into();
    runtime.worker_id = unique("hit-rate-w");
    runtime.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker should build"))
}

fn spawn(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Start parameters for one run, as `sticky_default_tests` builds them.
fn start_workflow<'a>(
    workflow_name: &'a str,
    exec_id: ExecutionId,
    workflow_id: &'a str,
    queue_name: &'a str,
    input: serde_json::Value,
) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name,
        workflow_id,
        exec_id,
        input: input.into(),
        parent_id: None,
        queue_name,
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
        priority: autumn_harvest::types::Priority::default(),
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
        tenant: None,
    }
}

/// Waits until `log` holds `n` outcomes.
async fn wait_for_outcomes(log: &ResidentLog, n: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while log.outcomes().len() < n {
        assert!(
            Instant::now() < deadline,
            "only {:?} after 30 s",
            log.outcomes()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// AC (issue #2007): one measurement on an agent-loop example.
#[tokio::test]
async fn agent_loop_resumes_every_sequential_turn_and_replays_after_a_gate() {
    let (url, _container) = setup_test_database_url_or_env().await;
    let pool = build_test_pool(&url);
    let mut conn = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the test database");
    let queue = unique("hit-rate-q");
    let log = Arc::new(ResidentLog::default());
    let worker = build_worker(&queue, Arc::clone(&log));
    let handle = spawn(&worker, &pool);

    let exec_id = ExecutionId::new();
    let workflow_id = unique("hit-rate-run");
    let task = serde_json::to_value(AgentTask::new("Pay the invoice.")).expect("task encodes");
    start_or_load_workflow_execution(
        &mut conn,
        start_workflow("agent_loop", exec_id, &workflow_id, &queue, task),
        None,
    )
    .await
    .expect("start the agent loop");

    // Turns 1 and 2 call a tool each. Turn 3 asks for the gated call.
    wait_for_outcomes(&log, 6).await;
    let approval = serde_json::to_value(Approval::Approve).expect("approval encodes");
    autumn_harvest::signal::send_signal(&mut conn, exec_id, &approval_signal(2, 0, "c3"), approval)
        .await
        .expect("approve the gated call");
    wait_for_execution_state_with_timeout(&url, exec_id, "COMPLETED", Duration::from_secs(30))
        .await;

    let outcomes = log.outcomes();
    let hits = outcomes.iter().filter(|o| o.starts_with("hit/")).count();
    println!(
        "agent loop resident outcomes: {outcomes:?}; hit rate {hits}/{}",
        outcomes.len()
    );
    assert_eq!(
        outcomes,
        [
            "miss/cold",
            "hit/resumed",
            "hit/resumed",
            "hit/resumed",
            "hit/resumed",
            "hit/resumed",
            "miss/race",
            "miss/race_teardown",
            "miss/race_teardown",
        ],
        "each single-await turn resumes until the gate; after it, each replay re-issues the teardown"
    );

    worker.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
}
