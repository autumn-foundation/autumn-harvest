//! A live probe on a Postgres fork (issue #2378).
//!
//! Each test runs a source `agent_loop` run on a worker, then probes it with
//! `eval::live_probe`. The candidate calls a read-only, a write and an
//! external tool. Each tool counts its runs in `execute`.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database, and pass `--test-threads=1`. Without the variable,
//! each test starts a Postgres container. CI runs this target from a `linux`
//! row of `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::fork::{ForkEffects, fork_marker};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{ExecutionId, HarvestBuilder, ShardId, WorkerConfig, migrate};
use autumn_harvest_agent::eval::{PROBE_EFFECT_REFUSED, live_probe, probe_fork_id};
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::postgres::{self, PostgresAgents, RunState};
use autumn_harvest_agent::refine::{LiveEnd, LiveRequest, RefinePolicy, RefineSources as _};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, AgentReport, AgentTask, AllowAll,
    BoxFuture, Ceilings, ChatRequest, ChatResponse, ChatRole, ContentPart, Delivery, FnTool,
    Mandate, Peer, ProfileOverlay, Report, StopReason, TokenUsage, Tool, ToolEffect, ToolRules,
};
use common::tool_results;
use diesel::sql_types::Text;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// How long a test waits for a worker.
const LIMIT: Duration = Duration::from_secs(60);

/// The system prompt of the source run.
const SOURCE_SYSTEM: &str = "You answer directly.";

/// The system prompt of a candidate that calls each tool.
const CALL_TOOLS: &str = "Probe: call each tool.";

/// The system prompt of a candidate that delegates.
const DELEGATE: &str = "Probe: delegate.";

/// The task text of a delegated child.
const CHILD_TASK: &str = "child: write a note";

/// The system prompt of a candidate whose first turn waits for a release.
const HOLD: &str = "Probe: wait.";

/// A migrated database, and the container that serves it, if any.
async fn database() -> (String, Option<ContainerAsync<Postgres>>) {
    let (url, container) = if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        (url, None)
    } else {
        let container = Postgres::default()
            .with_tag("16")
            .start()
            .await
            .expect("postgres container should start");
        let host = container.get_host().await.expect("container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("container port");
        (
            format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            Some(container),
        )
    };
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    (url, container)
}

fn pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(16)
        .build()
        .expect("pool")
}

/// A name that no earlier test or run used, so a reused database works.
fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |at| at.as_nanos());
    format!("{prefix}-{nanos}")
}

/// A running worker. Dropping it stops the worker.
struct Running {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.worker.shutdown();
        self.handle.abort();
    }
}

/// Start a worker of the agent workflows on `queue` and `default`.
fn run_worker(url: &str, queue: &str, harness: AgentHarness) -> Running {
    let mut config = WorkerConfig::default()
        .with_queues([queue, "default"])
        .with_shard_assignments([ShardId::new(0)]);
    config.max_concurrent_workflows = 8;
    config.max_concurrent_activities = 8;
    let (registry, _dags, _schedules, config) = postgres::register(HarvestBuilder::new(), harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(25);
    let worker = Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"));
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(url);
        async move { worker.run(&pool).await }
    });
    Running { worker, handle }
}

/// A tool that counts the runs of `execute`.
fn counted(name: &str, effect: ToolEffect, runs: &Arc<AtomicUsize>) -> Arc<dyn Tool> {
    let runs = Arc::clone(runs);
    FnTool::new(
        name,
        "A counted tool.",
        json!({"type": "object"}),
        move |_| {
            runs.fetch_add(1, Ordering::SeqCst);
            async move { Ok(json!({"ran": true})) }
        },
    )
    .effect(effect)
    .shared()
}

/// A delivery that counts its reports.
#[derive(Debug, Default)]
struct CountingDelivery(AtomicUsize);

impl CountingDelivery {
    fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Delivery for CountingDelivery {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// A model that answers from the system prompt and the first user text.
///
/// - The source answers at once.
/// - [`CALL_TOOLS`] calls `read`, `write` and `external`, then answers.
/// - [`DELEGATE`] sends two delegations, then answers.
/// - A child calls `write`, then answers.
#[derive(Debug, Default)]
struct ProbeModel {
    requests: Mutex<Vec<ChatRequest>>,
    /// Lets a [`HOLD`] turn answer.
    released: Arc<std::sync::atomic::AtomicBool>,
}

impl ProbeModel {
    fn requests(&self) -> Vec<ChatRequest> {
        self.requests.lock().unwrap().clone()
    }
}

/// The first text of the first message of `role`.
fn first_text(request: &ChatRequest, role: ChatRole) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == role)
        .and_then(|message| match message.content.first() {
            Some(ContentPart::Text(text)) => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn first_user(request: &ChatRequest) -> String {
    first_text(request, ChatRole::User)
}

/// The tool results of a request, in order.
fn results(request: &ChatRequest) -> Vec<String> {
    request
        .messages
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .collect()
}

fn text(reply: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(reply.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(3, 1),
    }
}

fn tool_calls(list: &[(&str, &str, Value)]) -> ChatResponse {
    ChatResponse {
        content: list
            .iter()
            .map(|(id, name, arguments)| ContentPart::ToolCall {
                id: (*id).to_owned(),
                name: (*name).to_owned(),
                arguments: arguments.clone(),
            })
            .collect(),
        stop_reason: StopReason::ToolUse,
        usage: TokenUsage::new(3, 1),
    }
}

impl AgentModel for ProbeModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.requests.lock().unwrap().push(request.clone());
        let system = first_text(request, ChatRole::System);
        let answered = !results(request).is_empty();
        if system.contains(HOLD) && !answered {
            let released = Arc::clone(&self.released);
            return Box::pin(async move {
                while !released.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok(text("held done"))
            });
        }
        let reply = if first_user(request) == CHILD_TASK {
            if answered {
                text("child done")
            } else {
                tool_calls(&[("c1", "write", json!({}))])
            }
        } else if answered {
            text("probe done")
        } else if system.contains(CALL_TOOLS) {
            tool_calls(&[
                ("r1", "read", json!({})),
                ("w1", "write", json!({})),
                ("x1", "external", json!({})),
            ])
        } else if system.contains(DELEGATE) {
            tool_calls(&[
                (
                    "d1",
                    DELEGATE_TOOL,
                    json!({"agent": "helper", "task": CHILD_TASK}),
                ),
                (
                    "d2",
                    DELEGATE_TOOL,
                    json!({"agent": "helper", "task": CHILD_TASK}),
                ),
            ])
        } else {
            text("source done")
        };
        Box::pin(async move { Ok(reply) })
    }
}

/// The counters of the three tools.
#[derive(Default)]
struct Counters {
    read: Arc<AtomicUsize>,
    write: Arc<AtomicUsize>,
    external: Arc<AtomicUsize>,
}

impl Counters {
    fn get(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

/// One database, one worker, the tools, the model and the delivery.
struct Fixture {
    url: String,
    pg: PostgresAgents,
    harness: AgentHarness,
    counters: Counters,
    model: Arc<ProbeModel>,
    delivery: Arc<CountingDelivery>,
    _worker: Running,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Fixture {
    /// A fixture whose harness has `policy`.
    async fn new(policy: Arc<dyn autumn_harvest_agent::ToolPolicy>) -> Self {
        let (url, container) = database().await;
        let queue = unique("live-probe");
        let counters = Counters::default();
        let model = Arc::new(ProbeModel::default());
        let delivery = Arc::new(CountingDelivery::default());
        let harness = AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>)
            .tool(counted("read", ToolEffect::ReadOnly, &counters.read))
            .tool(counted("write", ToolEffect::Write, &counters.write))
            .tool(counted(
                "external",
                ToolEffect::External,
                &counters.external,
            ))
            .peer(Peer::new("helper", "Writes notes."))
            .delivery(Arc::clone(&delivery) as Arc<dyn Delivery>)
            .policy(policy);
        let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
        let worker = run_worker(&url, &queue, harness.clone());
        Self {
            url,
            pg,
            harness,
            counters,
            model,
            delivery,
            _worker: worker,
            _container: container,
        }
    }

    async fn conn(&self) -> AsyncPgConnection {
        AsyncPgConnection::establish(&self.url)
            .await
            .expect("connect")
    }

    /// Run `task` as the source, to its end.
    async fn source(&self, task: &AgentTask) -> ExecutionId {
        let exec = postgres::start(&self.pg, task).await.unwrap();
        let state = postgres::wait_run(&self.pg, exec, LIMIT).await.unwrap();
        assert!(
            matches!(state, RunState::Completed(_)),
            "the source should complete, got {state:?}"
        );
        exec
    }

    /// Probe `source` with `system` and `allowed`. Returns the probe run id
    /// and its report.
    async fn probe(
        &self,
        source: ExecutionId,
        system: &str,
        allowed: &[ToolEffect],
    ) -> (ExecutionId, AgentReport) {
        let overlay = ProfileOverlay {
            system: Some(system.to_owned()),
            ..ProfileOverlay::default()
        };
        let probe = live_probe(&self.pg, &self.harness, source, &overlay, allowed)
            .await
            .expect("the probe starts");
        assert_eq!(probe.forked_from_exec_id, source);
        assert_ne!(probe.exec_id, source);
        let state = postgres::wait_run(&self.pg, probe.exec_id, LIMIT)
            .await
            .unwrap();
        let RunState::Completed(output) = state else {
            panic!("the probe should complete, got {state:?}");
        };
        (probe.exec_id, serde_json::from_value(output).unwrap())
    }

    /// One digest of every `harvest_events` row of `exec`.
    async fn event_digest(&self, exec: ExecutionId) -> String {
        let rows = diesel::sql_query(
            "SELECT coalesce(md5(string_agg(id::text || ':' || event_id::text || ':' \
             || event_type || ':' || event_data::text || ':' || timestamp::text, '|' \
             ORDER BY event_id)), '') AS text \
             FROM harvest_events WHERE workflow_exec_id = $1::uuid",
        )
        .bind::<Text, _>(exec.to_string());
        let rows: Vec<TextRow> = diesel_async::RunQueryDsl::load(rows, &mut self.conn().await)
            .await
            .expect("hash the events");
        rows.into_iter()
            .next()
            .map(|row| row.text)
            .unwrap_or_default()
    }

    /// The number of cost ledger rows of `exec`.
    async fn ledger_rows(&self, exec: ExecutionId) -> usize {
        let rows = diesel::sql_query(
            "SELECT count(*)::text AS text FROM harvest_llm_ledger \
             WHERE workflow_exec_id = $1::uuid",
        )
        .bind::<Text, _>(exec.to_string());
        let rows: Vec<TextRow> = diesel_async::RunQueryDsl::load(rows, &mut self.conn().await)
            .await
            .expect("count the ledger rows");
        rows.into_iter()
            .next()
            .map_or(0, |row| row.text.parse().unwrap())
    }

    /// The decoded history of `exec`.
    async fn history(&self, exec: ExecutionId) -> Vec<WorkflowEvent> {
        autumn_harvest::store::load_history(&mut self.conn().await, exec)
            .await
            .expect("load the history")
            .events
    }
}

impl Fixture {
    /// The number of runs that hold the workflow id `id`.
    async fn runs_of(&self, id: &str) -> usize {
        let rows = diesel::sql_query(
            "SELECT count(*)::text AS text FROM harvest_workflow_executions \
             WHERE workflow_id = $1",
        )
        .bind::<Text, _>(id);
        let rows: Vec<TextRow> = diesel_async::RunQueryDsl::load(rows, &mut self.conn().await)
            .await
            .expect("count the runs");
        rows.into_iter()
            .next()
            .map_or(0, |row| row.text.parse().unwrap())
    }

    /// The number of forks of `source` with a derived workflow id.
    async fn derived_forks(&self, source: ExecutionId) -> usize {
        let rows = diesel::sql_query(
            "SELECT count(*)::text AS text FROM harvest_workflow_executions f \
             JOIN harvest_workflow_executions s ON s.id = $1::uuid \
             WHERE f.workflow_id LIKE s.workflow_id || '-fork-%'",
        )
        .bind::<Text, _>(source.to_string());
        let rows: Vec<TextRow> = diesel_async::RunQueryDsl::load(rows, &mut self.conn().await)
            .await
            .expect("count the forks");
        rows.into_iter()
            .next()
            .map_or(0, |row| row.text.parse().unwrap())
    }

    /// The number of [`HOLD`] runs that asked the model for a first turn.
    fn held_starts(&self) -> usize {
        self.model
            .requests()
            .iter()
            .filter(|request| {
                first_text(request, ChatRole::System).contains(HOLD) && results(request).is_empty()
            })
            .count()
    }

    /// Run one attempt of a refine live re-probe of `source` with `key`.
    fn reprobe(
        &self,
        source: ExecutionId,
        key: &str,
    ) -> tokio::task::JoinHandle<Result<LiveEnd, autumn_harvest_agent::refine::LiveProbeError>>
    {
        let sources = postgres::Sources::new(self.pg.clone());
        let harness = self.harness.clone();
        let key = key.to_owned();
        tokio::spawn(async move {
            let overlay = ProfileOverlay {
                system: Some(HOLD.to_owned()),
                ..ProfileOverlay::default()
            };
            let effects = BTreeSet::from([ToolEffect::ReadOnly]);
            let ceilings = Ceilings::from_profile(&AgentProfile::new("support"));
            sources
                .live_probe(LiveRequest {
                    harness: &harness,
                    source,
                    overlay: &overlay,
                    effects: &effects,
                    ceilings: &ceilings,
                    fork_key: Some(&key),
                })
                .await
        })
    }
}

/// Wait until `check` holds, up to [`LIMIT`].
async fn until(check: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + LIMIT;
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refine_probe_killed_during_the_wait_reuses_its_fork_on_retry() {
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    let source = fx.source(&source_task()).await;
    let key = unique("attempt-key");
    let fork_id = probe_fork_id(&key);

    // The first attempt forks, then the worker dies during the wait.
    let first = fx.reprobe(source, &key);
    until(|| fx.held_starts() == 1).await;
    assert_eq!(fx.runs_of(&fork_id).await, 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    // The retry has the same key. It finds the fork and waits for its end.
    let retry = fx.reprobe(source, &key);
    tokio::time::sleep(Duration::from_millis(200)).await;
    fx.model.released.store(true, Ordering::SeqCst);
    let end = retry.await.unwrap().expect("the retry reads the fork");

    assert!(end.completed, "{end:?}");
    assert_eq!(fx.runs_of(&fork_id).await, 1, "one fork holds the key");
    let resolved = postgres::run_state(&fx.pg, end.exec_id.parse().unwrap()).await;
    assert!(
        matches!(resolved, Ok(RunState::Completed(_))),
        "{resolved:?}"
    );
    assert_eq!(fx.derived_forks(source).await, 0, "no unkeyed fork");
    assert_eq!(fx.held_starts(), 1, "the model ran the probe once");
}

#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    text: String,
}

fn source_task() -> AgentTask {
    let mut task = AgentTask::new("Check the account.").system(SOURCE_SYSTEM);
    task.deliver = true;
    task
}

/// The probe refusals of `results`.
fn refusals(results: &[String]) -> Vec<Value> {
    results
        .iter()
        .filter_map(|result| serde_json::from_str::<Value>(result).ok())
        .filter(|value| value["error"] == PROBE_EFFECT_REFUSED)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_probe_runs_only_read_only_tools() {
    let fx = Fixture::new(Arc::new(
        ToolRules::new().effect(ToolEffect::External, autumn_harvest_agent::Rule::Allow),
    ))
    .await;
    let source = fx.source(&source_task()).await;
    let before = fx.event_digest(source).await;
    assert!(!before.is_empty(), "the digest covers the source rows");
    let source_ledger = fx.ledger_rows(source).await;
    assert!(source_ledger > 0, "the source has ledger rows");
    let delivered = fx.delivery.count();
    assert_eq!(delivered, 1, "the source delivers its report");

    // The real default of the operator setting, not a copy of it.
    let allowed: Vec<ToolEffect> = RefinePolicy::default().probe_effects.into_iter().collect();
    assert_eq!(allowed, [ToolEffect::ReadOnly]);
    let (probe, report) = fx.probe(source, CALL_TOOLS, &allowed).await;

    assert_eq!(Counters::get(&fx.counters.write), 0, "no write ran");
    assert_eq!(Counters::get(&fx.counters.external), 0, "no external ran");
    assert!(Counters::get(&fx.counters.read) > 0, "the read tool ran");
    let refused = refusals(&tool_results(&report));
    assert_eq!(refused.len(), 2, "two probe refusals: {refused:?}");
    let effects: BTreeSet<&str> = refused
        .iter()
        .filter_map(|value| value["effect"].as_str())
        .collect();
    assert_eq!(effects, BTreeSet::from(["external", "write"]));
    assert_eq!(report.text, "probe done");

    assert_eq!(fx.event_digest(source).await, before);
    // The fork names its source and marks itself as a probe.
    let history = fx.history(probe).await;
    assert_eq!(
        fork_marker(&history),
        Some((source, ForkEffects::LiveQuiet))
    );
    let Some(WorkflowEvent::WorkflowStarted { input, .. }) = history.first() else {
        panic!("the probe history starts with WorkflowStarted");
    };
    let task: AgentTask = serde_json::from_value(input.clone()).unwrap();
    let mark = task.probe.expect("the input marks the probe");
    assert_eq!(mark.effects(), BTreeSet::from([ToolEffect::ReadOnly]));
    assert_eq!(task.system.as_deref(), Some(CALL_TOOLS));
    // The ledger keys each row on its execution. The probe rows key on the
    // fork, and the source keeps its own count.
    assert!(fx.ledger_rows(probe).await > 0, "the probe has ledger rows");
    assert_eq!(
        fx.ledger_rows(source).await,
        source_ledger,
        "no probe row carries the source run id"
    );
    assert_eq!(fx.delivery.count(), delivered, "agent_deliver never ran");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_allow_all_policy_does_not_widen_the_probe() {
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    let source = fx.source(&source_task()).await;
    let delivered = fx.delivery.count();

    let (_probe, report) = fx.probe(source, CALL_TOOLS, &[ToolEffect::ReadOnly]).await;

    assert_eq!(Counters::get(&fx.counters.write), 0);
    assert_eq!(Counters::get(&fx.counters.external), 0);
    assert!(Counters::get(&fx.counters.read) > 0);
    assert_eq!(refusals(&tool_results(&report)).len(), 2);
    assert_eq!(fx.delivery.count(), delivered);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_that_allows_writes_still_refuses_external_tools() {
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    let source = fx.source(&source_task()).await;

    let (_probe, report) = fx
        .probe(
            source,
            CALL_TOOLS,
            &[ToolEffect::ReadOnly, ToolEffect::Write],
        )
        .await;

    assert!(Counters::get(&fx.counters.write) > 0, "the write tool ran");
    assert_eq!(Counters::get(&fx.counters.external), 0, "no external ran");
    let refused = refusals(&tool_results(&report));
    assert_eq!(refused.len(), 1, "one probe refusal: {refused:?}");
    assert_eq!(refused[0]["effect"], "external");
}

/// The tool results that each child request carries.
fn child_results(model: &ProbeModel) -> Vec<String> {
    model
        .requests()
        .iter()
        .filter(|request| first_user(request) == CHILD_TASK)
        .flat_map(results)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fan_out_source_probes_its_children_inline() {
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    let task = source_task()
        .mandate(Mandate::new("planner").peers(["helper"]))
        .parallel_children(2);
    let source = fx.source(&task).await;
    let source_ledger = fx.ledger_rows(source).await;

    let (probe, report) = fx.probe(source, DELEGATE, &[ToolEffect::ReadOnly]).await;

    assert_eq!(report.text, "probe done");
    assert_eq!(Counters::get(&fx.counters.write), 0, "no child wrote");
    let refused = refusals(&child_results(&fx.model));
    assert_eq!(refused.len(), 2, "each child got a refusal: {refused:?}");
    assert!(refused.iter().all(|value| value["effect"] == "write"));
    // A fan-out child is a separate run, which could notify on its end. So
    // the probe runs each child inline and starts no child run.
    let history = fx.history(probe).await;
    assert!(
        !history
            .iter()
            .any(|event| matches!(event, WorkflowEvent::ChildWorkflowStarted { .. })),
        "the probe started no child run"
    );
    let Some(WorkflowEvent::WorkflowStarted { input, .. }) = history.first() else {
        panic!("the probe history starts with WorkflowStarted");
    };
    let input: AgentTask = serde_json::from_value(input.clone()).unwrap();
    assert_eq!(input.parallel_children, 0);
    assert_eq!(fx.ledger_rows(source).await, source_ledger);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_inline_child_of_a_probe_refuses_a_write() {
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    let task = source_task().mandate(Mandate::new("planner").peers(["helper"]));
    let source = fx.source(&task).await;

    let (_probe, report) = fx.probe(source, DELEGATE, &[ToolEffect::ReadOnly]).await;

    assert_eq!(report.text, "probe done");
    assert_eq!(Counters::get(&fx.counters.write), 0, "no child wrote");
    let refused = refusals(&child_results(&fx.model));
    assert_eq!(refused.len(), 2, "{refused:?}");
    assert!(refused.iter().all(|value| value["effect"] == "write"));
}

/// Removes the completion trigger of a test, also on a panic.
struct TriggerRow {
    url: String,
    id: String,
}

impl Drop for TriggerRow {
    fn drop(&mut self) {
        let (url, id) = (self.url.clone(), self.id.clone());
        // A drop cannot await, so a scoped thread runs the delete.
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a runtime");
                runtime.block_on(async {
                    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
                    let delete = diesel::sql_query(
                        "DELETE FROM harvest_completion_triggers WHERE id = $1::uuid",
                    )
                    .bind::<Text, _>(id);
                    diesel_async::RunQueryDsl::execute(delete, &mut conn)
                        .await
                        .expect("delete the trigger");
                });
            });
        });
    }
}

/// The number of runs that a completion trigger started for `source`.
///
/// A trigger start records its source in `start_source_ref`. So another
/// run of the shared database cannot add to the count. A fork row names its
/// source there too, so the filter also reads `start_source`.
async fn trigger_starts(fx: &Fixture, source: ExecutionId) -> usize {
    let rows = diesel::sql_query(
        "SELECT count(*)::text AS text FROM harvest_workflow_executions \
         WHERE start_source = 'completion_trigger' AND start_source_ref = $1",
    )
    .bind::<Text, _>(source.to_string());
    let rows: Vec<TextRow> = diesel_async::RunQueryDsl::load(rows, &mut fx.conn().await)
        .await
        .expect("count the trigger starts");
    rows.into_iter()
        .next()
        .map_or(0, |row| row.text.parse().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_probe_fires_no_completion_trigger() {
    // A trigger start routes through the global router, as in a worker app.
    autumn_harvest::shard::install_global_router(autumn_harvest::shard::ShardRouter::single());
    let fx = Fixture::new(Arc::new(AllowAll)).await;
    // A registered target, so the terminal path can route its start. Its
    // run fails on the report input, and the test only counts the starts.
    let target = autumn_harvest_agent::heartbeat::HEARTBEAT_WORKFLOW_NAME.to_owned();
    let id = uuid_text();
    let insert = diesel::sql_query(
        "INSERT INTO harvest_completion_triggers \
            (id, source_workflow_name, terminal_states, target_workflow_name, input_mapping) \
         VALUES ($1::uuid, 'agent_loop', '[\"Completed\"]'::jsonb, $2, \
                 '{\"type\":\"Passthrough\"}'::jsonb)",
    )
    .bind::<Text, _>(id.clone())
    .bind::<Text, _>(target);
    diesel_async::RunQueryDsl::execute(insert, &mut fx.conn().await)
        .await
        .expect("insert the trigger");
    let _row = TriggerRow {
        url: fx.url.clone(),
        id,
    };

    let source = fx.source(&source_task()).await;
    assert_eq!(
        trigger_starts(&fx, source).await,
        1,
        "the source fires the trigger"
    );

    let (probe, _report) = fx.probe(source, CALL_TOOLS, &[ToolEffect::ReadOnly]).await;

    assert_eq!(trigger_starts(&fx, probe).await, 0, "the probe fires none");
    assert_eq!(trigger_starts(&fx, source).await, 1);
    let mut conn = fx.conn().await;
    for (exec, quiet) in [(source, false), (probe, true)] {
        let row = autumn_harvest::execution::load_execution(&mut conn, exec)
            .await
            .unwrap();
        assert_eq!(
            autumn_harvest::fork::skips_completion_effects(&mut conn, &row)
                .await
                .unwrap(),
            quiet,
            "{exec}"
        );
    }
}

/// A new UUID as text, from the clock and a counter.
fn uuid_text() -> String {
    ExecutionId::new().to_string()
}
