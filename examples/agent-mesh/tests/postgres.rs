//! The agent loop and durable agents on Postgres workers (issues #2270,
//! #2268, #2269 and #2267).
//!
//! The SQLite runtime of the desk runs one activity at a time, on one shard.
//! These tests run the same workflows and activities on a Postgres worker:
//!
//! - Three children of one turn overlap on the wall clock. With one slot
//!   they run one after another, so the measure can tell the two apart.
//! - The live text of a model turn reaches a progress listener while the
//!   turn still runs. The durable record of the turn is stored once.
//! - A durable peer on another shard gets one message and answers once.
//! - An erased durable agent keeps no memory, stream chunk or transcript
//!   text in any table.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::notify::{ProgressWaitOutcome, WorkflowProgressListener};
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::shard::{ShardRouter, ShardedDbPool, install_global_router};
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, HarvestBuilder, ShardId, StartWorkflowParams, WorkerConfig, migrate,
};
use autumn_harvest_agent::mesh::DELEGATE_TOOL;
use autumn_harvest_agent::workflow::{self, WORKFLOW_NAME};
use autumn_harvest_agent::{
    AGENT_WORKFLOW_NAME, Agent, AgentError, AgentHarness, AgentId, AgentModel, AgentOp,
    AgentProfile, AgentReport, AgentStop, AgentTask, BoxFuture, ChatRequest, ChatResponse,
    ChatRole, ContentPart, LiveEvent, Mandate, Peer, StopReason, TokenUsage,
};
use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ── harness ──────────────────────────────────────────────────────────────────

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

/// A worker of the agent workflows on its own queue, with `harness` as
/// shared state.
///
/// The agent activities name no queue, so they go to `default`. The worker
/// also polls that queue. The tests run one at a time, so no other worker
/// takes their activities.
fn worker(queue: &str, harness: AgentHarness) -> Arc<Worker> {
    let mut config = WorkerConfig::default().with_queues([queue, "default"]);
    config.max_concurrent_workflows = 8;
    config.max_concurrent_activities = 8;
    let (registry, _dags, _schedules, config) = HarvestBuilder::new()
        .workflows(workflow::workflows())
        .activities(workflow::activities())
        .state(harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(25);
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"))
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

async fn start(url: &str, queue: &str, task: &AgentTask) -> ExecutionId {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let workflow_id = unique("mesh-pg");
    let params = StartWorkflowParams::new(
        WORKFLOW_NAME,
        &workflow_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        serde_json::to_value(task).expect("task"),
        queue,
    );
    autumn_harvest::execution::start_or_load_workflow_execution_with_codecs(
        &mut conn,
        params,
        None,
        &PayloadCodecs::default(),
    )
    .await
    .expect("start")
    .exec_id
}

#[derive(diesel::QueryableByName)]
struct RunRow {
    #[diesel(sql_type = Text)]
    state: String,
    #[diesel(sql_type = Nullable<Text>)]
    output: Option<String>,
}

/// Wait for the run to complete, up to `limit`. Returns its report.
async fn report(url: &str, exec: ExecutionId, limit: Duration) -> AgentReport {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let deadline = Instant::now() + limit;
    loop {
        let row: RunRow = diesel::sql_query(
            "SELECT state, output::text AS output FROM harvest_workflow_executions WHERE id = $1",
        )
        .bind::<SqlUuid, _>(exec.as_uuid())
        .get_result(&mut conn)
        .await
        .expect("load run");
        match row.state.as_str() {
            "COMPLETED" => {
                let output = row.output.expect("a completed run has an output");
                return serde_json::from_str(&output).expect("a report");
            }
            "FAILED" | "CANCELLED" | "TERMINATED" | "TIMED_OUT" => {
                panic!("the run ended {}: {:?}", row.state, row.output)
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "the run did not complete");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn count(url: &str, sql: &str, exec: ExecutionId) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let row: Count = diesel::sql_query(sql)
        .bind::<SqlUuid, _>(exec.as_uuid())
        .get_result(&mut conn)
        .await
        .expect("count");
    row.n
}

fn text(reply: &str) -> ChatResponse {
    ChatResponse {
        content: vec![ContentPart::Text(reply.to_owned())],
        stop_reason: StopReason::EndTurn,
        usage: TokenUsage::new(10, 2),
    }
}

/// The text of the first user message of a request.
///
/// The slice pattern reads the first part. A `first()` call would resolve
/// to the method of the `RunQueryDsl` import.
fn first_user(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .find(|message| message.role == ChatRole::User)
        .and_then(|message| match message.content.as_slice() {
            [ContentPart::Text(text), ..] => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn has_results(request: &ChatRequest) -> bool {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .any(|part| matches!(part, ContentPart::ToolResult { .. }))
}

// ── three children at the same time (issue #2270) ───────────────────────────

const ROOT_INPUT: &str = "Brief me on the three open questions.";

/// How long the model of each child takes. A long call keeps the measure
/// clear of the start skew of a loaded runner.
const CHILD_CALL: Duration = Duration::from_secs(2);

/// The root asks three researchers in one turn. Each child call sleeps,
/// and the model records when it started and ended on the wall clock.
#[derive(Debug, Default)]
struct SlowPeers {
    calls: Mutex<Vec<(Instant, Instant)>>,
}

impl AgentModel for SlowPeers {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let input = first_user(request);
            if input == ROOT_INPUT {
                if has_results(request) {
                    return Ok(text("The brief is ready."));
                }
                let content = ["export", "api-keys", "billing"]
                    .iter()
                    .enumerate()
                    .map(|(i, topic)| ContentPart::ToolCall {
                        id: format!("d{i}"),
                        name: DELEGATE_TOOL.to_owned(),
                        arguments: json!({"agent": "researcher", "task": format!("Find the notes on {topic}.")}),
                    })
                    .collect();
                return Ok(ChatResponse {
                    content,
                    stop_reason: StopReason::ToolUse,
                    usage: TokenUsage::new(10, 2),
                });
            }
            let started = Instant::now();
            tokio::time::sleep(CHILD_CALL).await;
            self.calls.lock().unwrap().push((started, Instant::now()));
            Ok(text(&format!("The notes say: {input}")))
        })
    }
}

/// Run the brief with `parallel` child slots. Returns the child call spans
/// in start order, and the wall time of the run.
async fn brief(url: &str, parallel: u32) -> (Vec<(Instant, Instant)>, Duration) {
    let model = Arc::new(SlowPeers::default());
    let harness =
        AgentHarness::new(model.clone()).peer(Peer::new("researcher", "Finds facts in the notes."));
    let queue = unique("mesh-fanout");
    let task = AgentTask::new(ROOT_INPUT)
        .mandate(Mandate::new("triage").peers(["researcher"]))
        .parallel_children(parallel);
    let exec = start(url, &queue, &task).await;
    let worker = worker(&queue, harness);
    let began = Instant::now();
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(url);
        async move { worker.run(&pool).await }
    });
    let report = report(url, exec, Duration::from_secs(60)).await;
    let wall = began.elapsed();
    worker.shutdown();
    handle.abort();
    assert_eq!(report.stop, AgentStop::Completed, "{report:?}");
    assert_eq!(report.text, "The brief is ready.");
    let mut calls = model.calls.lock().unwrap().clone();
    calls.sort_by_key(|(started, _)| *started);
    assert_eq!(calls.len(), 3, "one model call for each child");
    (calls, wall)
}

#[tokio::test(flavor = "multi_thread")]
async fn three_children_of_one_turn_overlap_on_the_wall_clock() {
    let (url, _container) = database().await;

    // Three slots: one instant lies inside all three calls. That is the
    // proof of the overlap. The bounds after it only rule out a serial run,
    // so they leave room for a slow runner.
    let (calls, wall) = brief(&url, 3).await;
    let last_start = calls.iter().map(|(started, _)| *started).max().unwrap();
    let first_end = calls.iter().map(|(_, ended)| *ended).min().unwrap();
    assert!(
        last_start < first_end,
        "the three child calls overlap: {calls:?}"
    );
    let span = calls.iter().map(|(_, ended)| *ended).max().unwrap() - calls[0].0;
    assert!(
        span < CHILD_CALL * 3,
        "three calls of {CHILD_CALL:?} took {span:?} together"
    );

    // One slot: each call ends before the next starts. The same measure
    // tells the two modes apart.
    let (serial, serial_wall) = brief(&url, 1).await;
    for pair in serial.windows(2) {
        assert!(
            pair[0].1 <= pair[1].0,
            "one slot runs one child at a time: {serial:?}"
        );
    }
    assert!(
        wall < serial_wall,
        "three slots took {wall:?}, one slot took {serial_wall:?}"
    );
}

// ── live text on Postgres (issue #2268) ─────────────────────────────────────

/// A text that only the live delta and the durable turn record hold.
const LIVE_TEXT: &str = "live-text-of-the-desk-turn";

static RELEASE: AtomicBool = AtomicBool::new(false);

/// Sends one delta, then waits until the test has read it.
#[derive(Debug)]
struct HeldStream;

impl HeldStream {
    fn answer() -> String {
        // Over the coalesce threshold, so the delta goes out at once.
        format!(
            "{LIVE_TEXT} {}",
            "x".repeat(autumn_harvest_agent::live::COALESCE_BYTES)
        )
    }
}

impl AgentModel for HeldStream {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move { Ok(text(&Self::answer())) })
    }

    fn chat_stream<'a>(
        &'a self,
        _request: &'a ChatRequest,
        on_delta: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let answer = Self::answer();
            on_delta(&answer);
            while !AtomicBool::load(&RELEASE, Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(text(&answer))
        })
    }
}

/// The next live event of the run, up to ten seconds. It must come as an
/// activity chunk, so it never takes a workflow `seq`.
async fn next_live(listener: &mut WorkflowProgressListener) -> LiveEvent {
    match listener
        .wait_for_progress_timeout(Duration::from_secs(10))
        .await
    {
        Ok(ProgressWaitOutcome::Chunk(payload)) => {
            assert!(
                payload.activity,
                "a live chunk is an activity chunk: {payload:?}"
            );
            serde_json::from_value(payload.chunk["data"].clone()).expect("a live event")
        }
        other => panic!("no live chunk arrived: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_text_arrives_while_the_turn_runs_and_the_turn_is_stored_once() {
    let (url, _container) = database().await;
    let queue = unique("mesh-live");
    let exec = start(&url, &queue, &AgentTask::new("Say hello.").stream()).await;
    let mut listener = WorkflowProgressListener::connect(&url, exec.as_uuid())
        .await
        .expect("listen");
    let worker = worker(&queue, AgentHarness::new(Arc::new(HeldStream)));
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(&url);
        async move { worker.run(&pool).await }
    });

    assert!(matches!(
        next_live(&mut listener).await,
        LiveEvent::Start { step: 0, .. }
    ));
    let LiveEvent::Delta { text, .. } = next_live(&mut listener).await else {
        panic!("a delta follows the start");
    };
    assert!(text.starts_with(LIVE_TEXT), "{text}");
    // The model still waits, so the turn has not completed.
    let completed = count(
        &url,
        "SELECT count(*) AS n FROM harvest_events \
          WHERE workflow_exec_id = $1 AND event_type = 'ActivityCompleted'",
        exec,
    )
    .await;
    assert_eq!(completed, 0, "the text came while the turn ran");

    RELEASE.store(true, Ordering::SeqCst);
    let report = report(&url, exec, Duration::from_secs(30)).await;
    worker.shutdown();
    handle.abort();
    assert_eq!(report.stop, AgentStop::Completed, "{report:?}");

    // The durable layer stores the whole text of the turn once.
    let stored = count(
        &url,
        &format!(
            "SELECT count(*) AS n FROM harvest_stream_chunks \
              WHERE workflow_exec_id = $1 AND chunk::text LIKE '%{LIVE_TEXT}%'"
        ),
        exec,
    )
    .await;
    assert_eq!(stored, 1, "one durable record of the turn");
}

// ── durable agents on Postgres: shared harness ─────────────────────────────

/// A worker of the agent workflows on `queue` that claims work on `shards`.
fn shard_worker(queue: &str, harness: AgentHarness, shards: &[ShardId]) -> Arc<Worker> {
    let mut config = WorkerConfig::default()
        .with_queues([queue, "default"])
        .with_shard_assignments(shards.iter().copied());
    config.max_concurrent_workflows = 8;
    config.max_concurrent_activities = 8;
    let (registry, _dags, _schedules, config) = HarvestBuilder::new()
        .workflows(workflow::workflows())
        .activities(workflow::activities())
        .state(harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(25);
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"))
}

/// Send `op` to the durable agent `id` with a signal-with-start, as an app
/// does on Postgres. A new run lands on `shard`.
async fn send_agent(
    url: &str,
    queue: &str,
    id: &AgentId,
    shard: ShardId,
    op: &AgentOp,
) -> ExecutionId {
    use autumn_harvest::WorkflowIdReusePolicy;
    use autumn_harvest::execution::{SignalWithStartParams, signal_with_start_workflow_execution};
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let outcome = signal_with_start_workflow_execution(
        &mut conn,
        SignalWithStartParams {
            workflow_name: AGENT_WORKFLOW_NAME,
            workflow_id: id.as_str(),
            exec_id: ExecutionId::new_for_shard(shard),
            input: json!({}),
            parent_id: None,
            queue_name: queue,
            execution_timeout: None,
            memo: None,
            search_attrs: None,
            reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            signal_name: autumn_harvest::entity::ENTITY_OP_SIGNAL,
            signal_payload: Agent::payload(op),
            idempotency_key: None,
            max_workflow_input_bytes: 0,
            max_signal_payload_bytes: 0,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            reject_fresh_if_debounced: false,
            workflow_retry_policy: None,
            max_workflow_attempts_ceiling: None,
            workflow_info: None,
            start_source_override: None,
            start_source_ref_override: None,
            tenant: None,
        },
    )
    .await
    .expect("signal-with-start");
    outcome.exec_id
}

/// Wait for the run `exec` to complete, up to `limit`.
async fn wait_completed(url: &str, exec: ExecutionId, limit: Duration) {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let deadline = Instant::now() + limit;
    loop {
        let row: RunRow = diesel::sql_query(
            "SELECT state, output::text AS output FROM harvest_workflow_executions WHERE id = $1",
        )
        .bind::<SqlUuid, _>(exec.as_uuid())
        .get_result(&mut conn)
        .await
        .expect("load run");
        if row.state == "COMPLETED" {
            return;
        }
        assert!(Instant::now() < deadline, "the run is still {}", row.state);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The latest entity snapshot of the agent `id`, as text.
async fn snapshot_text(url: &str, id: &AgentId) -> String {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        snapshot: String,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let rows: Vec<Row> = diesel::sql_query(
        "SELECT snapshot::text AS snapshot FROM harvest_entity_state \
          WHERE workflow_name = 'agent' AND workflow_id = $1 \
          ORDER BY event_row_id DESC LIMIT 1",
    )
    .bind::<Text, _>(id.as_str())
    .load(&mut conn)
    .await
    .expect("load snapshot");
    rows.into_iter()
        .next()
        .map(|row| row.snapshot)
        .unwrap_or_default()
}

/// Wait until the snapshot of `id` holds `text`, up to `limit`.
async fn wait_for_snapshot(url: &str, id: &AgentId, text: &str, limit: Duration) {
    let deadline = Instant::now() + limit;
    while !snapshot_text(url, id).await.contains(text) {
        assert!(
            Instant::now() < deadline,
            "the agent {id} never held {text:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── a durable peer send across shards (issue #2269) ────────────────────────

const ASK: &str = "Ask the researcher about the export limit.";
const FINDING: &str = "The export limit is 10000 rows.";
const FILED: &str = "Filed the finding.";

/// Triage asks the researcher once, then files. The researcher answers.
#[derive(Debug, Default)]
struct PeerScript {
    peer_calls: AtomicUsize,
}

impl AgentModel for PeerScript {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            if first_user(request).starts_with("[A message from the agent") {
                self.peer_calls.fetch_add(1, Ordering::SeqCst);
                return Ok(text(FINDING));
            }
            if has_results(request) {
                return Ok(text(FILED));
            }
            Ok(ChatResponse {
                content: vec![ContentPart::ToolCall {
                    id: "d1".to_owned(),
                    name: DELEGATE_TOOL.to_owned(),
                    arguments: json!({"agent": "researcher", "task": "Find the export limit."}),
                }],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(10, 2),
            })
        })
    }
}

/// Put back a one-shard router when the test ends, so a later test in this
/// binary places its runs on shard 0.
struct OneShardRouter;

impl Drop for OneShardRouter {
    fn drop(&mut self) {
        let zero = ShardId::new(0);
        install_global_router(ShardRouter::new(vec![zero], vec![zero], zero));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_send_across_shards_is_delivered_once() {
    let (url, _container) = database().await;
    let (zero, one) = (ShardId::new(0), ShardId::new(1));
    let router = ShardRouter::new(vec![zero, one], vec![zero, one], zero);
    install_global_router(router.clone());
    let _restore = OneShardRouter;
    // Both shards point at one database. The engine still takes the
    // cross-shard outbox path, because the execution ids name two shards.
    let shared = pool(&url);
    let _sharded = ShardedDbPool::from_map(
        [(zero, shared.clone()), (one, shared.clone())]
            .into_iter()
            .collect(),
        zero,
    );

    let tag = Uuid::new_v4().simple().to_string();
    let research = AgentId::new("research", &tag).expect("an agent id");
    let triage = AgentId::new("triage", &tag).expect("an agent id");
    let receiver_shard = router.pick_for_new_workflow(AGENT_WORKFLOW_NAME, research.as_str());
    let sender_shard = if receiver_shard == zero { one } else { zero };

    let model = Arc::new(PeerScript::default());
    let harness = AgentHarness::new(model.clone())
        .peer(Peer::agent("researcher", "Finds facts.", &research))
        .agent(AgentProfile::new("triage").mandate(Mandate::new("triage").peers(["researcher"])))
        .agent(AgentProfile::new("research").mandate(Mandate::new("researcher")));
    let queue = unique("mesh-shards");
    let worker = shard_worker(&queue, harness, &[zero, one]);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let shared = shared.clone();
        async move { worker.run(&shared).await }
    });

    // A send needs a running receiver, so the receiver starts first.
    let start = AgentOp::Control(autumn_harvest_agent::Control::Resume);
    let receiver = send_agent(&url, &queue, &research, receiver_shard, &start).await;
    wait_for_snapshot(&url, &research, "incarnation", Duration::from_secs(20)).await;
    let sender = send_agent(&url, &queue, &triage, sender_shard, &AgentOp::message(ASK)).await;
    assert_ne!(
        receiver.shard(),
        sender.shard(),
        "the two agents sit on two shards"
    );

    wait_for_snapshot(&url, &triage, FILED, Duration::from_secs(30)).await;
    worker.shutdown();
    handle.abort();

    // `RunQueryDsl` also has a `load`, so the atomic call names its type.
    let peer_calls = AtomicUsize::load(&model.peer_calls, Ordering::SeqCst);
    assert_eq!(peer_calls, 1, "the receiver ran the message once");
    assert!(
        snapshot_text(&url, &triage).await.contains(FINDING),
        "the reply reached the sender"
    );
    let keyed = count(
        &url,
        "SELECT count(*) AS n FROM harvest_signals \
          WHERE workflow_exec_id = $1 AND signal_name = 'harvest.entity.op' \
            AND idempotency_key IS NOT NULL",
        receiver,
    )
    .await;
    assert_eq!(keyed, 1, "one keyed peer message reached the receiver");
}

// ── erasure of a durable agent (issue #2267) ───────────────────────────────

const USER_MARKER: &str = "user-text-4b7e";
const MEMORY_MARKER: &str = "memory-note-5e1f";
const ANSWER_MARKER: &str = "answer-text-9c2d";

/// Writes one state memory note, then answers.
#[derive(Debug, Default)]
struct Rememberer;

impl AgentModel for Rememberer {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            if has_results(request) {
                return Ok(text(&format!("Noted: {ANSWER_MARKER}.")));
            }
            Ok(ChatResponse {
                content: vec![ContentPart::ToolCall {
                    id: "m1".to_owned(),
                    name: autumn_harvest_agent::memory::MEMORY_TOOL.to_owned(),
                    arguments: json!({"action": "add", "block": "memory", "text": MEMORY_MARKER}),
                }],
                stop_reason: StopReason::ToolUse,
                usage: TokenUsage::new(10, 2),
            })
        })
    }
}

/// How many rows of the run `exec` hold `marker`, over each table that can
/// hold agent text.
async fn rows_with(url: &str, exec: ExecutionId, marker: &str) -> i64 {
    let mut total = 0;
    for sql in [
        "SELECT count(*) AS n FROM harvest_events WHERE workflow_exec_id = $1 AND event_data::text LIKE '%{m}%'",
        "SELECT count(*) AS n FROM harvest_stream_chunks WHERE workflow_exec_id = $1 AND chunk::text LIKE '%{m}%'",
        "SELECT count(*) AS n FROM harvest_entity_state WHERE execution_id = $1 AND snapshot::text LIKE '%{m}%'",
        "SELECT count(*) AS n FROM harvest_signals WHERE workflow_exec_id = $1 AND payload::text LIKE '%{m}%'",
        "SELECT count(*) AS n FROM harvest_workflow_executions WHERE id = $1 \
           AND (input::text LIKE '%{m}%' OR coalesce(output::text, '') LIKE '%{m}%')",
    ] {
        total += count(url, &sql.replace("{m}", marker), exec).await;
    }
    total
}

#[tokio::test(flavor = "multi_thread")]
async fn an_erased_agent_keeps_no_memory_stream_chunk_or_transcript_text() {
    let (url, _container) = database().await;
    let tag = Uuid::new_v4().simple().to_string();
    let agent = AgentId::new("desk", &tag).expect("an agent id");
    let harness = AgentHarness::new(Arc::new(Rememberer)).agent(
        AgentProfile::new("desk")
            .memory(autumn_harvest_agent::AgentMemory::State)
            .stream(),
    );
    let queue = unique("mesh-erase");
    let worker = worker(&queue, harness);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(&url);
        async move { worker.run(&pool).await }
    });

    let op = AgentOp::message(format!("Please remember this: {USER_MARKER}"));
    let exec = send_agent(&url, &queue, &agent, ShardId::new(0), &op).await;
    wait_for_snapshot(&url, &agent, ANSWER_MARKER, Duration::from_secs(30)).await;
    // Delete the agent, so its run ends and can be erased.
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    autumn_harvest::signal::send_signal(
        &mut conn,
        exec,
        autumn_harvest::entity::ENTITY_OP_SIGNAL,
        Agent::delete_payload(),
    )
    .await
    .expect("delete");
    wait_completed(&url, exec, Duration::from_secs(20)).await;
    worker.shutdown();
    handle.abort();

    // Each text is there before the erasure, so the check below is not empty.
    for marker in [USER_MARKER, MEMORY_MARKER, ANSWER_MARKER] {
        assert!(
            rows_with(&url, exec, marker).await > 0,
            "{marker} was stored"
        );
    }
    let streamed = count(
        &url,
        &format!(
            "SELECT count(*) AS n FROM harvest_stream_chunks \
              WHERE workflow_exec_id = $1 AND chunk::text LIKE '%{ANSWER_MARKER}%'"
        ),
        exec,
    )
    .await;
    assert_eq!(streamed, 1, "the durable layer stored the turn");

    autumn_harvest::erase::erase_workflow_payloads(&mut conn, exec, "test")
        .await
        .expect("erase");
    for marker in [USER_MARKER, MEMORY_MARKER, ANSWER_MARKER] {
        assert_eq!(
            rows_with(&url, exec, marker).await,
            0,
            "{marker} remains after the erasure"
        );
    }
}
