//! The `postgres` module on a real Postgres worker.
//!
//! Each test calls the module the way an app does, and a worker in the test
//! process runs the work:
//!
//! - An agent loop run waits for an approval, takes it, and completes.
//! - A durable agent runs one op for each message id, so a retry is free.
//! - A deleted agent has no live run and no state.
//! - `pause_mesh` pauses each running agent of one mesh, and no other.
//! - `LedgerSpend` sums the tokens of the runs of one mesh.
//! - A heartbeat tick runs and reports.
//! - A gated call of a durable agent sends one approval notice. `decide`
//!   takes its signal. A worker restart sends no second notice, and a failed
//!   notice keeps the call parked.
//! - One consolidation firing serves each agent of a scheduled kind, and
//!   skips an idle agent and an agent whose pass still runs.
//! - A two-shard target finds the run on the shard that the router picked.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database, and pass `--test-threads=1`. Each worker also polls
//! the shared `default` queue. Without the variable, each test starts a
//! Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use autumn_harvest::chrono::{DateTime, Utc};
use autumn_harvest::entity::ENTITY_STATE_QUERY;
use autumn_harvest::entity_state::ProjectedSnapshot;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{HarvestBuilder, ShardId, WorkerConfig, migrate};
use autumn_harvest_agent::consolidate::ConsolidationStop;
use autumn_harvest_agent::consolidate_all::{AgentDirectory, ConsolidateAll, DispatchReport};
use autumn_harvest_agent::delivery::{ApprovalNotice, Delivery, HEARTBEAT_OK, Report};
use autumn_harvest_agent::episode::{EpisodeSettings, EpisodeStore, InMemoryEpisodeStore};
use autumn_harvest_agent::heartbeat::{HeartbeatReport, HeartbeatTask};
use autumn_harvest_agent::mesh_control::MeshSpend;
use autumn_harvest_agent::postgres::{self, Directory, LedgerSpend, PostgresAgents, RunState};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentId, AgentMemory, AgentModel, AgentOp, AgentProfile, AgentReport,
    AgentState, AgentStatus, AgentTask, Approval, BoxFuture, ChatRequest, ChatResponse, ChatRole,
    ContentPart, InMemoryMemoryStore, Rule, ToolEffect, ToolRules,
};
use common::{Recorder, Reply, ScriptedModel, TryFnModel, answer, calls, recorded_tool};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// How long a test waits for a worker.
const LIMIT: Duration = Duration::from_secs(60);

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

/// Start a worker of the agent workflows on `queue`, with `harness`.
///
/// The worker claims work on `shards`. The agent activities name no queue,
/// so they go to `default`, and the worker polls that queue too.
fn run_worker(url: &str, queue: &str, harness: AgentHarness, shards: &[ShardId]) -> Running {
    run_worker_with(url, queue, harness, shards, |builder| builder)
}

/// [`run_worker`], with `tune` applied to the builder.
fn run_worker_with(
    url: &str,
    queue: &str,
    harness: AgentHarness,
    shards: &[ShardId],
    tune: impl FnOnce(HarvestBuilder) -> HarvestBuilder,
) -> Running {
    let mut config = WorkerConfig::default()
        .with_queues([queue, "default"])
        .with_shard_assignments(shards.iter().copied());
    config.max_concurrent_workflows = 8;
    config.max_concurrent_activities = 8;
    let (registry, _dags, _schedules, config) =
        tune(postgres::register(HarvestBuilder::new(), harness))
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

/// A model that answers each message with `done: <message>`.
fn echo_model(tokens: u32) -> Arc<dyn AgentModel> {
    TryFnModel::new(move |request: &ChatRequest| -> Reply {
        let last = request
            .messages
            .last()
            .map(|message| format!("{message:?}"))
            .unwrap_or_default();
        answer(&format!("done: {}", last.len()), tokens)
    })
}

/// Wait for the state of `id` to pass `until`. Fails the test after `LIMIT`.
async fn settled(
    pg: &PostgresAgents,
    id: &AgentId,
    until: impl Fn(&AgentState) -> bool,
) -> AgentState {
    postgres::wait_for(pg, id, LIMIT, until)
        .await
        .expect("read the state")
        .unwrap_or_else(|| panic!("the agent {id} did not settle"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_loop_run_waits_for_approval_and_completes() {
    let (url, _container) = database().await;
    let queue = unique("pg-loop");
    let writes = Arc::new(Recorder::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": "hi"}))], 3),
        answer("saved", 2),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);

    let exec = postgres::start(&pg, &AgentTask::new("Save a note."))
        .await
        .unwrap();
    let RunState::WaitingSignal(signal) = postgres::wait_run(&pg, exec, LIMIT).await.unwrap()
    else {
        panic!("the run should wait for an approval");
    };
    assert_eq!(
        autumn_harvest_agent::approval::approval_call_id(&signal),
        Some("w1")
    );
    assert_eq!(
        postgres::pending_approvals(&pg, exec).await.unwrap(),
        vec![signal.clone()]
    );
    assert_eq!(writes.runs().len(), 0, "no write before the approval");

    postgres::decide(&pg, exec, &signal, &Approval::Approve)
        .await
        .unwrap();
    let RunState::Completed(output) = postgres::wait_run(&pg, exec, LIMIT).await.unwrap() else {
        panic!("the run should complete");
    };
    let report: AgentReport = serde_json::from_value(output).unwrap();
    assert_eq!(report.text, "saved");
    assert_eq!(writes.runs(), vec![json!({"text": "hi"})]);
    assert_eq!(model.calls(), 2, "each model turn ran once");
    assert_eq!(
        postgres::pending_approvals(&pg, exec).await.unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_durable_agent_runs_one_op_for_each_message_id() {
    let (url, _container) = database().await;
    let queue = unique("pg-agent");
    let kind = unique("support");
    let harness = AgentHarness::new(echo_model(5)).agent(AgentProfile::new(&kind));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let id = AgentId::new(&kind, "acct-42").unwrap();

    assert_eq!(postgres::current_run(&pg, &id).await.unwrap(), None);
    assert!(postgres::state(&pg, &id).await.unwrap().is_none());

    let first = AgentOp::message_with_id("Where is my order?", "m-1");
    let sent = postgres::send(&pg, &id, &first, Some("m-1")).await.unwrap();
    assert!(sent.started_fresh, "the first send starts the agent");
    let again = postgres::send(&pg, &id, &first, Some("m-1")).await.unwrap();
    assert!(!again.started_fresh, "a retry finds the live run");

    let state = settled(&pg, &id, |state| state.segments >= 1).await;
    assert!(state.last_answer.as_deref().unwrap().starts_with("done: "));
    assert!(postgres::current_run(&pg, &id).await.unwrap().is_some());

    let second = AgentOp::message_with_id("And the invoice?", "m-2");
    postgres::send(&pg, &id, &second, Some("m-2"))
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.segments >= 2).await;
    assert_eq!(state.segments, 2, "the retry of m-1 ran no op");
    assert_eq!(state.tokens_used, 10);
}

/// The projection row of `id`, once its snapshot is an omitted marker.
async fn omitted_row(url: &str, id: &AgentId) -> autumn_harvest::entity_state::EntityStateRow {
    let deadline = std::time::Instant::now() + LIMIT;
    loop {
        let mut conn = AsyncPgConnection::establish(url).await.unwrap();
        let row = autumn_harvest::entity_state::read(
            &mut conn,
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
            "agent",
            id.as_str(),
        )
        .await
        .unwrap();
        if let Some(row) = row
            && matches!(row.snapshot, ProjectedSnapshot::Omitted)
        {
            return row;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the snapshot was never omitted"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_state_past_the_snapshot_cap_is_read_by_replay() {
    let (url, _container) = database().await;
    let queue = unique("pg-big");
    let kind = unique("verbose");
    let long = "x".repeat(6_000);
    let model = TryFnModel::new(move |_request: &ChatRequest| answer(&long, 1));
    let harness = AgentHarness::new(model).agent(AgentProfile::new(&kind));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    // The side-effect cap is the workflow input cap. A 6000-byte answer is
    // past it, so the snapshot holds an omitted marker, not the state.
    let _worker = run_worker_with(&url, &queue, harness, &[ShardId::new(0)], |builder| {
        builder.max_workflow_input_bytes(4_096)
    });
    let id = AgentId::new(&kind, "a").unwrap();

    postgres::send(
        &pg,
        &id,
        &AgentOp::message_with_id("talk", "m-1"),
        Some("m-1"),
    )
    .await
    .unwrap();
    let row = omitted_row(&url, &id).await;
    assert_eq!(row.snapshot.query_answer(ENTITY_STATE_QUERY), None);

    let state = postgres::state(&pg, &id)
        .await
        .unwrap()
        .expect("replay recovers the state");
    assert_eq!(state.segments, 1);
    assert_eq!(state.last_answer.as_deref().map(str::len), Some(6_000));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_agent_has_no_run_and_no_state() {
    let (url, _container) = database().await;
    let queue = unique("pg-delete");
    let kind = unique("scratch");
    let harness = AgentHarness::new(echo_model(1)).agent(AgentProfile::new(&kind));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let id = AgentId::new(&kind, "a").unwrap();

    postgres::send(
        &pg,
        &id,
        &AgentOp::message_with_id("hi", "m-1"),
        Some("m-1"),
    )
    .await
    .unwrap();
    settled(&pg, &id, |state| state.segments >= 1).await;
    postgres::delete(&pg, &id, Some("bye")).await.unwrap();

    let deadline = std::time::Instant::now() + LIMIT;
    while postgres::current_run(&pg, &id).await.unwrap().is_some() {
        assert!(std::time::Instant::now() < deadline, "the run never ended");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(postgres::state(&pg, &id).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_mesh_pauses_each_running_agent_of_the_mesh_and_no_other() {
    let (url, _container) = database().await;
    let queue = unique("pg-mesh");
    let mesh = unique("desk");
    let kind = unique("triage");
    let other_kind = unique("ops");
    let harness = AgentHarness::new(echo_model(4))
        .agent(AgentProfile::new(&kind).mesh(&mesh))
        .agent(AgentProfile::new(&other_kind).mesh(unique("other")));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let a = AgentId::new(&kind, "a").unwrap();
    let b = AgentId::new(&kind, "b").unwrap();
    let outside = AgentId::new(&other_kind, "c").unwrap();
    for id in [&a, &b, &outside] {
        postgres::send(&pg, id, &AgentOp::message_with_id("hi", "m-1"), Some("m-1"))
            .await
            .unwrap();
        settled(&pg, id, |state| state.segments >= 1).await;
    }

    let record = postgres::pause_mesh(&pg, &mesh, "incident 7")
        .await
        .unwrap();
    assert_eq!(record.mesh, mesh);
    assert_eq!(record.reason, "incident 7");
    assert_eq!(record.agents, vec![a.clone(), b.clone()]);
    for id in [&a, &b] {
        let state = settled(&pg, id, |state| {
            matches!(state.status, AgentStatus::Paused { .. })
        })
        .await;
        assert_eq!(
            state.status,
            AgentStatus::Paused {
                reason: Some("incident 7".into())
            }
        );
    }
    let outside_state = postgres::state(&pg, &outside).await.unwrap().unwrap();
    assert_eq!(outside_state.status, AgentStatus::Open);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ledger_spend_sums_the_tokens_of_one_mesh() {
    let (url, _container) = database().await;
    let queue = unique("pg-spend");
    let mesh = unique("billing");
    let kind = unique("biller");
    let other_kind = unique("noise");
    let harness = AgentHarness::new(echo_model(7))
        .agent(AgentProfile::new(&kind).mesh(&mesh))
        .agent(AgentProfile::new(&other_kind).mesh(unique("elsewhere")));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let since = Utc::now();
    let ids = [
        AgentId::new(&kind, "a").unwrap(),
        AgentId::new(&kind, "b").unwrap(),
        AgentId::new(&other_kind, "c").unwrap(),
    ];
    for id in &ids {
        postgres::send(
            &pg,
            id,
            &AgentOp::message_with_id("bill", "m-1"),
            Some("m-1"),
        )
        .await
        .unwrap();
        settled(&pg, id, |state| state.segments >= 1).await;
    }

    let spend = LedgerSpend::new(pg.clone());
    let total = spend.spent(&mesh, since).await.unwrap();
    assert_eq!(total.tokens, 14, "two runs of the mesh, 7 tokens each");
    assert_eq!(total.cost_usd_micros, 0, "the model sets no price");
    let later = spend.spent(&mesh, DateTime::<Utc>::MAX_UTC).await.unwrap();
    assert_eq!(later.tokens, 0, "no row is recorded after the bound");
    let none = spend.spent(&unique("empty"), since).await.unwrap();
    assert_eq!(none.tokens, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heartbeat_tick_runs_and_reports() {
    let (url, _container) = database().await;
    let queue = unique("pg-beat");
    let model = ScriptedModel::new(vec![answer(HEARTBEAT_OK, 1)]);
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(
        &url,
        &queue,
        AgentHarness::new(model.clone()),
        &[ShardId::new(0)],
    );

    let exec = postgres::start_heartbeat(&pg, &HeartbeatTask::new())
        .await
        .unwrap();
    let RunState::Completed(output) = postgres::wait_run(&pg, exec, LIMIT).await.unwrap() else {
        panic!("the tick should complete");
    };
    let report: HeartbeatReport = serde_json::from_value(output).unwrap();
    assert!(!report.skipped);
    assert!(!report.delivered, "a quiet tick delivers nothing");
    assert_eq!(model.calls(), 1);
}

/// The text of the user messages of `request`.
fn user_text(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::User)
        .flat_map(|message| message.content.iter())
        .filter_map(|part| match part {
            ContentPart::Text(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Whether `request` is the model call of a consolidation pass.
fn is_pass(request: &ChatRequest) -> bool {
    user_text(request).contains("<episode id=\"")
}

/// The op text whose pass waits at the gate.
const HELD: &str = "held-7f3a";

/// A model for consolidation passes. Each op answers with its user text, so
/// the episode holds it. A pass over the episode [`HELD`] waits until the
/// gate opens. The wait is async, so no worker thread blocks.
#[derive(Debug, Default)]
struct GateModel {
    closed: std::sync::atomic::AtomicBool,
    passes: std::sync::atomic::AtomicUsize,
}

impl GateModel {
    fn set_closed(&self, closed: bool) {
        self.closed
            .store(closed, std::sync::atomic::Ordering::SeqCst);
    }

    fn passes(&self) -> usize {
        self.passes.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl AgentModel for GateModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let text = user_text(request);
            if !is_pass(request) {
                return answer(&format!("re: {text}"), 2);
            }
            if text.contains(HELD) {
                while self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            self.passes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            answer(r#"{"facts":[]}"#, 2)
        })
    }
}

/// Run one dispatcher firing to its end.
async fn fire(pg: &PostgresAgents, task: &ConsolidateAll) -> DispatchReport {
    let exec = postgres::consolidate_all(pg, task).await.unwrap();
    match postgres::wait_run(pg, exec, LIMIT).await.unwrap() {
        RunState::Completed(output) => serde_json::from_value(output).unwrap(),
        other => panic!("the dispatcher did not complete: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_consolidation_firing_serves_each_agent_of_a_scheduled_kind() {
    let (url, _container) = database().await;
    let queue = unique("pg-consolidate");
    let kind = unique("desk");
    let model = Arc::new(GateModel::default());
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let episodes = Arc::new(InMemoryEpisodeStore::default());
    let directory = Directory::new(pg.clone());
    let harness = AgentHarness::new(model.clone())
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(episodes.clone())
        .directory(Arc::new(directory.clone()))
        .agent(
            AgentProfile::new(&kind)
                .memory(AgentMemory::Store)
                .episodes(EpisodeSettings::default()),
        );
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let (a, b) = (
        AgentId::new(&kind, "a").unwrap(),
        AgentId::new(&kind, "b").unwrap(),
    );
    for id in [&a, &b] {
        postgres::send(&pg, id, &AgentOp::message_with_id("hi", "m-1"), Some("m-1"))
            .await
            .unwrap();
        settled(&pg, id, |state| state.segments >= 1).await;
    }
    assert_eq!(
        directory.agents(&kind).await.unwrap(),
        [a.clone(), b.clone()]
    );
    assert_eq!(directory.last_pass(&a).await.unwrap(), None);

    // Both agents get a pass on one firing.
    let first = fire(&pg, &ConsolidateAll::default()).await;
    assert_eq!(first.started, 2, "{first:?}");
    for pass in &first.passes {
        assert_eq!(pass.stop, Some(ConsolidationStop::Completed), "{pass:?}");
    }
    assert_eq!(model.passes(), 2);
    let last = directory.last_pass(&a).await.unwrap().unwrap();
    assert!(!last.running, "{last:?}");

    // Both are idle now: no pass and no model call.
    let idle = fire(&pg, &ConsolidateAll::default()).await;
    assert_eq!((idle.idle, idle.started), (2, 0), "{idle:?}");
    assert_eq!(model.passes(), 2);

    // A pass of `a` still runs, so `a` gets no second pass.
    for (id, text) in [(&a, HELD), (&b, "again")] {
        postgres::send(&pg, id, &AgentOp::message_with_id(text, "m-2"), Some("m-2"))
            .await
            .unwrap();
        settled(&pg, id, |state| state.segments >= 2).await;
    }
    model.set_closed(true);
    let held = postgres::consolidate(&pg, &a).await.unwrap();
    let deadline = std::time::Instant::now() + LIMIT;
    while !directory
        .last_pass(&a)
        .await
        .unwrap()
        .is_some_and(|pass| pass.running)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "the pass never published"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let busy = fire(&pg, &ConsolidateAll::default()).await;
    model.set_closed(false);
    assert_eq!(busy.running, 1, "{busy:?}");
    assert_eq!(busy.passes.len(), 1);
    assert_eq!(busy.passes[0].agent, b);
    assert_eq!(busy.passes[0].stop, Some(ConsolidationStop::Completed));
    let RunState::Completed(_) = postgres::wait_run(&pg, held, LIMIT).await.unwrap() else {
        panic!("the held pass should complete");
    };
    let scope = autumn_harvest_agent::consolidate::episode_scope(&a);
    assert_eq!(episodes.load(&scope).await.unwrap().pending(10).len(), 0);
}

/// Put back a one-shard router when the test ends, so a later test in this
/// binary places its runs on shard 0.
struct OneShardRouter;

impl Drop for OneShardRouter {
    fn drop(&mut self) {
        let zero = ShardId::new(0);
        autumn_harvest::shard::install_global_router(ShardRouter::new(
            vec![zero],
            vec![zero],
            zero,
        ));
    }
}

/// A migrated database next to the one at `url`, on the same server.
async fn second_database(url: &str) -> String {
    let name = unique("harvest_agents_b").replace('-', "_");
    let mut admin = AsyncPgConnection::establish(url).await.expect("connect");
    diesel_async::SimpleAsyncConnection::batch_execute(
        &mut admin,
        &format!("CREATE DATABASE \"{name}\""),
    )
    .await
    .expect("create the second database");
    let (base, _old) = url.rsplit_once('/').expect("a database path");
    let other = format!("{base}/{name}");
    let mut conn = AsyncPgConnection::establish(&other).await.expect("connect");
    migrate::apply_to_connection(&mut conn, &migrate::embedded())
        .await
        .expect("migrate");
    other
}

/// The number of signals of `exec` in the database at `url`.
async fn signal_rows(url: &str, exec: autumn_harvest::ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let row: Count = diesel_async::RunQueryDsl::get_result(
        diesel::sql_query("SELECT count(*) AS n FROM harvest_signals WHERE workflow_exec_id = $1")
            .bind::<diesel::sql_types::Uuid, _>(exec.as_uuid()),
        &mut conn,
    )
    .await
    .expect("count signals");
    row.n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebalanced_agent_is_reached_on_its_new_shard_and_counted_once() {
    let (url, _container) = database().await;
    let _restore = OneShardRouter;
    let target_url = second_database(&url).await;
    let (source, target) = (ShardId::new(0), ShardId::new(1));
    let pools = ShardedDbPool::from_map(
        BTreeMap::from([(source, pool(&url)), (target, pool(&target_url))]),
        source,
    );
    // New runs land on the source. Both shards stay readable.
    let router = ShardRouter::new(vec![source, target], vec![source], source);
    autumn_harvest::shard::install_global_router(router.clone());
    let queue = unique("pg-move");
    let kind = unique("mover");
    let mesh = unique("moving");
    let pg = PostgresAgents::sharded(&pools, router).with_queue(&queue);
    let id = AgentId::new(&kind, "a").unwrap();
    let since = Utc::now();
    {
        let harness = AgentHarness::new(echo_model(9)).agent(AgentProfile::new(&kind).mesh(&mesh));
        let _worker = run_worker(&url, &queue, harness, &[source]);
        postgres::send(
            &pg,
            &id,
            &AgentOp::message_with_id("hi", "m-1"),
            Some("m-1"),
        )
        .await
        .unwrap();
        settled(&pg, &id, |state| state.segments >= 1).await;
        // The worker stops here, so the run is quiescent for the move.
    }
    let exec = postgres::current_run(&pg, &id).await.unwrap().unwrap();
    assert_eq!(exec.shard(), source);
    let outcome = autumn_harvest::shard_rebalance::migrate_execution(
        &pools,
        exec,
        source,
        target,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            outcome,
            autumn_harvest::shard_rebalance::MigrationOutcome::Migrated { .. }
        ),
        "{outcome:?}"
    );

    // The id is kept, and the live row is on the target now.
    assert_eq!(postgres::current_run(&pg, &id).await.unwrap(), Some(exec));
    assert_eq!(
        postgres::run_state(&pg, exec).await.unwrap(),
        RunState::Running
    );
    assert_eq!(
        postgres::pending_approvals(&pg, exec).await.unwrap(),
        Vec::<String>::new()
    );
    let state = postgres::state(&pg, &id)
        .await
        .unwrap()
        .expect("the moved state");
    assert_eq!(state.segments, 1);

    // A send after the move reaches the moved run, not the source seal.
    let source_signals = signal_rows(&url, exec).await;
    let target_signals = signal_rows(&target_url, exec).await;
    let sent = postgres::send(
        &pg,
        &id,
        &AgentOp::message_with_id("again", "m-2"),
        Some("m-2"),
    )
    .await
    .unwrap();
    assert!(!sent.started_fresh, "the send found the moved run");
    assert_eq!(sent.exec_id, exec);
    assert_eq!(signal_rows(&target_url, exec).await, target_signals + 1);
    assert_eq!(signal_rows(&url, exec).await, source_signals);

    // Both databases hold the ledger row of the turn. It counts once.
    let total = LedgerSpend::new(pg.clone())
        .spent(&mesh, since)
        .await
        .unwrap();
    assert_eq!(total.tokens, 9, "the moved run counts once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sharded_target_finds_the_run_on_the_picked_shard() {
    let (url, _container) = database().await;
    let _restore = OneShardRouter;
    let queue = unique("pg-shard");
    let kind = unique("sharded");
    let shards = [ShardId::new(0), ShardId::new(1)];
    let router = ShardRouter::new(shards.to_vec(), shards.to_vec(), shards[0]);
    autumn_harvest::shard::install_global_router(router.clone());
    let shared = pool(&url);
    let pools = ShardedDbPool::from_map(
        BTreeMap::from([(shards[0], shared.clone()), (shards[1], shared)]),
        shards[0],
    );
    let pg = PostgresAgents::sharded(&pools, router.clone()).with_queue(&queue);
    let harness = AgentHarness::new(echo_model(1)).agent(AgentProfile::new(&kind));
    let _worker = run_worker(&url, &queue, harness, &shards);

    // Find a name that the router places on shard 1, so the test does not
    // pass by landing on the default shard.
    let id = (0..64)
        .map(|n| AgentId::new(&kind, &format!("n{n}")).unwrap())
        .find(|id| router.pick_for_new_workflow("agent", id.as_str()) == shards[1])
        .expect("some name hashes to shard 1");
    postgres::send(
        &pg,
        &id,
        &AgentOp::message_with_id("hi", "m-1"),
        Some("m-1"),
    )
    .await
    .unwrap();
    let exec = postgres::current_run(&pg, &id).await.unwrap().unwrap();
    assert_eq!(exec.shard(), shards[1]);
    settled(&pg, &id, |state| state.segments >= 1).await;
}

/// A delivery that keeps each approval notice.
#[derive(Debug, Default)]
struct Notices(std::sync::Mutex<Vec<ApprovalNotice>>);

impl Delivery for Notices {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn notify_approval<'a>(
        &'a self,
        notice: &'a ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.0.lock().unwrap().push(notice.clone());
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gated_call_on_a_durable_agent_sends_one_notice_that_decide_accepts() {
    let (url, _container) = database().await;
    let queue = unique("pg-notice");
    let kind = unique("support");
    let writes = Arc::new(Recorder::default());
    let notices = Arc::new(Notices::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": "hi"}))], 3),
        answer("saved", 2),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .delivery(Arc::clone(&notices) as Arc<dyn Delivery>)
        .agent(AgentProfile::new(&kind));
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let id = AgentId::new(&kind, "acct-1").unwrap();

    let op = AgentOp::message_with_id("Save a note.", "m-1");
    postgres::send(&pg, &id, &op, Some("m-1")).await.unwrap();
    // The notice is the push: the app waits for it, not for a poll.
    let deadline = std::time::Instant::now() + LIMIT;
    let seen = loop {
        let seen = notices.0.lock().unwrap().clone();
        if !seen.is_empty() || std::time::Instant::now() > deadline {
            break seen;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(seen.len(), 1, "one notice for one parked call");
    let notice = &seen[0].call;
    let exec = postgres::current_run(&pg, &id).await.unwrap().unwrap();
    assert_eq!(notice.execution_id, exec);
    assert!(notice.signal.starts_with("tool_approval:"), "{notice:?}");
    assert_eq!(notice.agent_id.as_ref(), Some(&id));
    assert_eq!(notice.tool, "write");
    assert_eq!(notice.arguments, json!({"text": "hi"}));
    assert!(writes.runs().is_empty(), "the notice releases nothing");

    postgres::decide(&pg, notice.execution_id, &notice.signal, &Approval::Approve)
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.last_answer.is_some()).await;
    assert_eq!(state.last_answer.as_deref(), Some("saved"));
    assert_eq!(writes.runs(), vec![json!({"text": "hi"})]);
    assert_eq!(notices.0.lock().unwrap().len(), 1, "no second notice");
}

/// A delivery that approves each call from inside the notice, as a bot does.
/// The decision lands before the notice activity completes.
#[derive(Debug)]
struct AutoApprove {
    pg: PostgresAgents,
    seen: std::sync::Mutex<Vec<ApprovalNotice>>,
}

impl Delivery for AutoApprove {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn notify_approval<'a>(
        &'a self,
        notice: &'a ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        self.seen.lock().unwrap().push(notice.clone());
        Box::pin(async move {
            postgres::decide(
                &self.pg,
                notice.call.execution_id,
                &notice.call.signal,
                &Approval::Approve,
            )
            .await
            .map_err(|err| {
                AgentError::new(autumn_harvest_agent::ErrorKind::Transport, err.to_string())
            })
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_decision_sent_from_the_notice_releases_the_call_and_replays() {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

    let (url, _container) = database().await;
    let queue = unique("pg-auto");
    let kind = unique("support");
    let writes = Arc::new(Recorder::default());
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let auto = Arc::new(AutoApprove {
        pg: pg.clone(),
        seen: std::sync::Mutex::new(Vec::new()),
    });
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": "hi"}))], 3),
        answer("saved", 2),
    ]);
    let harness = AgentHarness::new(model.clone())
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .delivery(Arc::clone(&auto) as Arc<dyn Delivery>)
        .agent(AgentProfile::new(&kind));
    let _worker = run_worker(&url, &queue, harness, &[ShardId::new(0)]);
    let id = AgentId::new(&kind, "acct-2").unwrap();

    let op = AgentOp::message_with_id("Save a note.", "m-1");
    postgres::send(&pg, &id, &op, Some("m-1")).await.unwrap();
    let state = settled(&pg, &id, |state| state.last_answer.is_some()).await;
    assert_eq!(state.last_answer.as_deref(), Some("saved"));
    assert_eq!(writes.runs(), vec![json!({"text": "hi"})]);
    assert_eq!(auto.seen.lock().unwrap().len(), 1, "one notice");

    // The run replays with the decision that came before the wait opened.
    // Replay reads a run that ended, so delete the agent first.
    let exec = postgres::current_run(&pg, &id).await.unwrap().unwrap();
    postgres::delete(&pg, &id, None).await.unwrap();
    let ended = postgres::wait_run(&pg, exec, LIMIT).await.unwrap();
    assert!(!matches!(ended, RunState::Running), "{ended:?}");
    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let events = autumn_harvest::store::load_history(&mut conn, exec)
        .await
        .unwrap()
        .events;
    // The decision lands before the approval race opens. So the race
    // settles on its first poll: its winner marker follows its open marker,
    // and it arms no timer.
    let marker = |event: &WorkflowEvent| match event {
        WorkflowEvent::MarkerRecorded { name, .. } => Some(name.clone()),
        _ => None,
    };
    let decided = events
        .iter()
        .position(|event| {
            matches!(event, WorkflowEvent::SignalReceived { signal_name, .. }
                if signal_name.starts_with("tool_approval:"))
        })
        .expect("the decision is in history");
    let opened = (decided..events.len())
        .find(|&at| marker(&events[at]).is_some_and(|name| name.starts_with("race:")))
        .expect("the approval race opens after the decision");
    let seq = marker(&events[opened]).unwrap()["race:".len()..].to_owned();
    assert_eq!(
        marker(&events[opened + 1]),
        Some(format!("race_winner:{seq}")),
        "the race settles at once"
    );
    let replay = WorkflowReplayer::new()
        .register(vec![autumn_harvest_agent::agent_info()])
        .with_execution_id(exec)
        .with_workflow_id(id.as_str())
        .replay_from_events(events)
        .await;
    assert!(
        matches!(replay.status, ReplayStatus::ReplaySucceeded),
        "{replay}"
    );
}

/// A delivery whose every approval notice fails with an outage.
#[derive(Debug, Default)]
struct BrokenNotices(std::sync::Mutex<usize>);

impl Delivery for BrokenNotices {
    fn deliver<'a>(&'a self, _report: &'a Report) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async { Ok(()) })
    }

    fn notify_approval<'a>(
        &'a self,
        _notice: &'a ApprovalNotice,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        *self.0.lock().unwrap() += 1;
        let err = AgentError::new(autumn_harvest_agent::ErrorKind::Provider, "chat is down");
        Box::pin(async move { Err(err) })
    }
}

/// A harness whose `write` tool needs an approval, with `delivery`.
fn gated(
    model: Arc<ScriptedModel>,
    writes: &Arc<Recorder>,
    delivery: Arc<dyn Delivery>,
    kind: &str,
) -> AgentHarness {
    AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, writes))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .delivery(delivery)
        .agent(AgentProfile::new(kind))
}

/// Wait until `seen` reports at least one item. Fails the test after `LIMIT`.
async fn first_notice(seen: impl Fn() -> usize) {
    let deadline = std::time::Instant::now() + LIMIT;
    while seen() == 0 {
        assert!(std::time::Instant::now() < deadline, "no notice arrived");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_restart_during_the_wait_sends_no_second_notice() {
    let (url, _container) = database().await;
    let queue = unique("pg-restart");
    let kind = unique("support");
    let writes = Arc::new(Recorder::default());
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let id = AgentId::new(&kind, "acct-3").unwrap();
    let first = Arc::new(Notices::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": "hi"}))], 3),
        answer("saved", 2),
    ]);
    let worker = run_worker(
        &url,
        &queue,
        gated(model.clone(), &writes, first.clone(), &kind),
        &[ShardId::new(0)],
    );
    let op = AgentOp::message_with_id("Save a note.", "m-1");
    postgres::send(&pg, &id, &op, Some("m-1")).await.unwrap();
    first_notice(|| first.0.lock().unwrap().len()).await;
    let notice = first.0.lock().unwrap()[0].call.clone();
    drop(worker);

    // A new worker with a new delivery takes the run over.
    let second = Arc::new(Notices::default());
    let _worker = run_worker(
        &url,
        &queue,
        gated(model.clone(), &writes, second.clone(), &kind),
        &[ShardId::new(0)],
    );
    postgres::decide(&pg, notice.execution_id, &notice.signal, &Approval::Approve)
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.last_answer.is_some()).await;
    assert_eq!(state.last_answer.as_deref(), Some("saved"));
    assert_eq!(writes.runs(), vec![json!({"text": "hi"})]);
    assert!(second.0.lock().unwrap().is_empty(), "no second notice");
    assert_eq!(first.0.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_notice_keeps_the_call_parked_on_postgres() {
    let (url, _container) = database().await;
    let queue = unique("pg-broken");
    let kind = unique("support");
    let writes = Arc::new(Recorder::default());
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let id = AgentId::new(&kind, "acct-4").unwrap();
    let broken = Arc::new(BrokenNotices::default());
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": "hi"}))], 3),
        answer("saved", 2),
    ]);
    let _worker = run_worker(
        &url,
        &queue,
        gated(model, &writes, broken.clone(), &kind),
        &[ShardId::new(0)],
    );
    let op = AgentOp::message_with_id("Save a note.", "m-1");
    postgres::send(&pg, &id, &op, Some("m-1")).await.unwrap();
    first_notice(|| *broken.0.lock().unwrap()).await;

    // The op stays parked. The failed notice released nothing.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let state = postgres::state(&pg, &id).await.unwrap().unwrap();
    assert_eq!(state.last_answer, None, "the op still waits");
    assert_eq!(state.failures, 0, "a failed notice fails nothing");
    assert_eq!(
        writes.runs(),
        Vec::<serde_json::Value>::new(),
        "nothing ran"
    );

    // The run waits on the signal of the first call of the first tool step.
    let exec = postgres::current_run(&pg, &id).await.unwrap().unwrap();
    let signal = autumn_harvest_agent::approval::approval_signal(0, 0, "w1");
    postgres::decide(&pg, exec, &signal, &Approval::Approve)
        .await
        .unwrap();
    let state = settled(&pg, &id, |state| state.last_answer.is_some()).await;
    assert_eq!(state.last_answer.as_deref(), Some("saved"));
    assert_eq!(writes.runs(), vec![json!({"text": "hi"})]);
    assert_eq!(*broken.0.lock().unwrap(), 1, "a refusal is not retried");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_notice_over_the_worker_cap_is_skipped_and_the_run_fails_cleanly() {
    use autumn_harvest::testing::{ReplayStatus, WorkflowReplayer};

    let (url, _container) = database().await;
    let queue = unique("pg-cap");
    let writes = Arc::new(Recorder::default());
    let notices = Arc::new(Notices::default());
    // The task declares no cap, so the arguments are not cut. The worker
    // takes 6 000 bytes of activity input, so the notice does not fit. The
    // next model turn holds the same arguments, so it cannot fit either.
    let big = "x".repeat(8_000);
    let model = ScriptedModel::new(vec![
        calls(&[("w1", "write", json!({"text": big}))], 3),
        answer("not saved", 2),
    ]);
    let harness = AgentHarness::new(model)
        .tool(recorded_tool("write", ToolEffect::Write, &writes))
        .policy(Arc::new(
            ToolRules::new().effect(ToolEffect::Write, Rule::Ask),
        ))
        .delivery(Arc::clone(&notices) as Arc<dyn Delivery>);
    let pg = PostgresAgents::single(pool(&url)).with_queue(&queue);
    let _worker = run_worker_with(&url, &queue, harness, &[ShardId::new(0)], |builder| {
        builder.max_activity_input_bytes(6_000)
    });
    let task = AgentTask::new("Save a note.")
        .max_request_bytes(0)
        .approval_timeout(Duration::from_secs(1));
    let exec = postgres::start(&pg, &task).await.unwrap();

    // Nobody decides. The deadline denies the call. The next model turn is
    // over the cap, so the run fails with the cause. A notice whose skip
    // left no record would wedge the run on a replay divergence instead.
    let deadline = std::time::Instant::now() + LIMIT;
    let state = loop {
        let state = postgres::run_state(&pg, exec).await.unwrap();
        if matches!(state, RunState::Completed(_) | RunState::Failed(_)) {
            break state;
        }
        assert!(std::time::Instant::now() < deadline, "{state:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let RunState::Failed(error) = state else {
        panic!("the run fails on the next model turn: {state:?}");
    };
    assert!(error.contains("payload too large"), "{error}");
    assert!(
        notices.0.lock().unwrap().is_empty(),
        "no notice over the cap"
    );
    assert_eq!(writes.runs(), Vec::<serde_json::Value>::new());

    let mut conn = AsyncPgConnection::establish(&url).await.unwrap();
    let events = autumn_harvest::store::load_history(&mut conn, exec)
        .await
        .unwrap()
        .events;
    let skipped = events.iter().any(|event| {
        matches!(event, WorkflowEvent::SideEffectRecorded { name: Some(name), value, .. }
            if name == "agent.approval_notice.fits" && value == &json!(false))
    });
    assert!(skipped, "the skip is recorded");
    // A replay under the default, larger cap takes the recorded branch. It
    // then meets no notice, as the run did.
    let replay = WorkflowReplayer::new()
        .register(vec![autumn_harvest_agent::agent_loop_info()])
        .with_execution_id(exec)
        .replay_from_events(events)
        .await;
    assert!(
        !matches!(replay.status, ReplayStatus::NonDeterminismDetected { .. }),
        "{replay}"
    );
}
