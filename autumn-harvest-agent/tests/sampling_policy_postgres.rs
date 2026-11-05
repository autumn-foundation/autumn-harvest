//! The sampling policy on Postgres workers (issue #2439).
//!
//! - The store keeps one policy per kind and audits each change.
//! - With `max_concurrent_draws = 2`, a model that counts its open calls
//!   sees 2 and never 3. The worker runs 8 activities at once, so the bound
//!   comes from the policy, not from the worker.
//! - A set through one pool changes the next run of a worker on another
//!   pool, with no restart.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, HarvestBuilder, ShardId, StartWorkflowParams, WorkerConfig, migrate,
};
use autumn_harvest_agent::sampling::{
    DRAWS_WORKFLOW_NAME, DrawsInput, DrawsReport, PolicyAction, PostgresSamplingPolicyStore,
    SamplingPolicy, SamplingPolicyStore, Strategy,
};
use autumn_harvest_agent::workflow;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, BoxFuture, ChatRequest, ChatResponse,
    ContentPart, StopReason, TokenUsage,
};
use diesel::sql_types::{Nullable, Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
        .max_size(8)
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

fn best_of_n(max_draws: u32, max_concurrent: u32) -> SamplingPolicy {
    SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws,
        max_concurrent_draws: max_concurrent,
        ..SamplingPolicy::default()
    }
}

/// The agent activities name no queue, so they go to `default`. Each worker
/// polls it. So two worker tests at once would take each other's model
/// calls. Each worker test holds this lock.
static WORKERS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A worker of the agent workflows on its own queue, with 8 activity slots.
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

async fn start(url: &str, queue: &str, input: &DrawsInput) -> ExecutionId {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let workflow_id = unique("draws");
    let params = StartWorkflowParams::new(
        DRAWS_WORKFLOW_NAME,
        &workflow_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        serde_json::to_value(input).expect("input"),
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
    #[diesel(sql_type = Nullable<Text>)]
    error: Option<String>,
}

/// Wait for the run to end, up to one minute. Returns the report, or the
/// error of a failed run.
async fn outcome(url: &str, exec: ExecutionId) -> Result<DrawsReport, String> {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let row: RunRow = diesel::sql_query(
            "SELECT state, output::text AS output, error
             FROM harvest_workflow_executions WHERE id = $1",
        )
        .bind::<SqlUuid, _>(exec.as_uuid())
        .get_result(&mut conn)
        .await
        .expect("load run");
        match row.state.as_str() {
            "COMPLETED" => {
                let output = row.output.expect("a completed run has an output");
                return Ok(serde_json::from_str(&output).expect("a report"));
            }
            "FAILED" => return Err(row.error.or(row.output).unwrap_or_default()),
            "CANCELLED" | "TERMINATED" | "TIMED_OUT" => {
                panic!("the run ended {}: {:?}", row.state, row.output)
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "the run did not end");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A model that counts its open calls. It answers only requests whose
/// input holds `marker`, so a stray task from another suite never counts.
///
/// Each call waits until two calls are open, or until a timeout. A third
/// call would then show as a peak of 3. So the test does not depend on the
/// time that a worker takes to start the second draw.
#[derive(Debug)]
struct CountingModel {
    marker: String,
    open: AtomicUsize,
    peak: AtomicUsize,
    calls: AtomicUsize,
}

impl CountingModel {
    fn new(marker: &str) -> Arc<Self> {
        Arc::new(Self {
            marker: marker.to_owned(),
            open: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        })
    }
}

impl AgentModel for CountingModel {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        Box::pin(async move {
            let ours = request.messages.iter().any(|message| {
                message.content.iter().any(
                    |part| matches!(part, ContentPart::Text(text) if text.contains(&self.marker)),
                )
            });
            if ours {
                let now = self.open.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(now, Ordering::SeqCst);
                let deadline = Instant::now() + Duration::from_secs(10);
                while AtomicUsize::load(&self.open, Ordering::SeqCst) < 2
                    && Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                // Hold the call, so a third call would overlap it.
                tokio::time::sleep(Duration::from_millis(200)).await;
                self.open.fetch_sub(1, Ordering::SeqCst);
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(ChatResponse {
                content: vec![ContentPart::Text("an answer".to_owned())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(5, 1),
            })
        })
    }
}

/// Run `input` on a worker with `model` and a store on its own pool.
async fn run_on_worker(
    url: &str,
    model: &Arc<CountingModel>,
    input: &DrawsInput,
) -> Result<DrawsReport, String> {
    let store = Arc::new(PostgresSamplingPolicyStore::new(pool(url)));
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(&input.kind))
        .sampling_policies(store);
    let queue = unique("draws-q");
    let exec = start(url, &queue, input).await;
    let worker = worker(&queue, harness);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(url);
        async move { worker.run(&pool).await }
    });
    let result = outcome(url, exec).await;
    worker.shutdown();
    handle.abort();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_postgres_store_reads_sets_resets_and_audits() {
    let (url, _container) = database().await;
    let store = PostgresSamplingPolicyStore::new(pool(&url));
    let kind = unique("kind");

    let read = store.read(&kind).await.unwrap();
    assert_eq!(read.policy, SamplingPolicy::default());
    assert_eq!(read.revision, 0);

    let first = best_of_n(4, 2);
    let change = store.set(&kind, &first, "ops@example.com").await.unwrap();
    assert_eq!(
        (change.old_policy(), change.new_policy(), change.revision),
        (None, Some(first.clone()), 1)
    );
    let read = store.read(&kind).await.unwrap();
    assert_eq!((read.policy, read.revision), (first.clone(), 1));

    let refused = store.set(&kind, &best_of_n(0, 1), "ops@example.com").await;
    assert!(refused.is_err(), "a policy out of bounds is refused");
    assert!(
        store.set(&kind, &first, " ops").await.is_err(),
        "a padded actor is refused"
    );
    assert_eq!(
        store.read(&kind).await.unwrap().revision,
        1,
        "nothing changed"
    );

    let change = store.reset(&kind, "lead@example.com").await.unwrap();
    assert_eq!(
        (change.old_policy(), change.new, change.revision),
        (Some(first.clone()), None, 2)
    );
    let read = store.read(&kind).await.unwrap();
    assert_eq!((read.policy, read.revision), (SamplingPolicy::default(), 2));

    let audit = store.audit(&kind, 10).await.unwrap();
    let rows: Vec<_> = audit
        .iter()
        .map(|row| {
            (
                row.revision,
                row.action,
                row.actor.as_str(),
                row.old_policy(),
                row.new_policy(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            (
                2,
                PolicyAction::Reset,
                "lead@example.com",
                Some(first.clone()),
                None
            ),
            (1, PolicyAction::Set, "ops@example.com", None, Some(first)),
        ]
    );
    assert_eq!(
        store.audit(&kind, 1).await.unwrap().len(),
        1,
        "the limit holds"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sets_each_audit_the_body_that_they_replaced() {
    let (url, _container) = database().await;
    let store = Arc::new(PostgresSamplingPolicyStore::new(pool(&url)));
    let kind = unique("race");
    let mut tasks = Vec::new();
    for draws in 1..=8_u32 {
        let store = Arc::clone(&store);
        let kind = kind.clone();
        tasks.push(tokio::spawn(async move {
            store
                .set(&kind, &best_of_n(draws, 1), "ops@example.com")
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    // The audit is a chain: each row replaced the body of the row before it.
    let mut audit = store.audit(&kind, 100).await.unwrap();
    audit.reverse();
    assert_eq!(audit.len(), 8);
    assert_eq!(audit[0].old, None);
    for pair in audit.windows(2) {
        assert_eq!(pair[1].old, pair[0].new, "no lost update");
        assert!(pair[1].at >= pair[0].at, "the time never goes back");
        assert_eq!(pair[1].revision, pair[0].revision + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrent_draws_two_keeps_two_model_calls_open_at_most() {
    let _one_at_a_time = WORKERS.lock().await;
    let (url, _container) = database().await;
    let kind = unique("conc");
    let operator = PostgresSamplingPolicyStore::new(pool(&url));
    operator
        .set(&kind, &best_of_n(4, 2), "ops@example.com")
        .await
        .unwrap();
    let model = CountingModel::new(&kind);
    let input = DrawsInput::new(
        &kind,
        format!("Name a prime ({kind})."),
        Strategy::BestOfN,
        4,
    );
    let report = run_on_worker(&url, &model, &input)
        .await
        .expect("the run completes");
    assert_eq!(report.draws.len(), 4);
    for draw in &report.draws {
        assert_eq!(
            draw.stop,
            autumn_harvest_agent::AgentStop::Completed,
            "{draw:?}"
        );
    }
    assert_eq!(AtomicUsize::load(&model.calls, Ordering::SeqCst), 4);
    let peak = AtomicUsize::load(&model.peak, Ordering::SeqCst);
    assert_eq!(peak, 2, "two draws ran together, and never three");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_set_from_one_process_changes_the_next_run_of_a_running_worker() {
    let _one_at_a_time = WORKERS.lock().await;
    let (url, _container) = database().await;
    let kind = unique("live");
    let model = CountingModel::new(&kind);
    let input = DrawsInput::new(
        &kind,
        format!("Name a prime ({kind})."),
        Strategy::BestOfN,
        2,
    );

    // One worker keeps running across both runs.
    let store = Arc::new(PostgresSamplingPolicyStore::new(pool(&url)));
    let harness = AgentHarness::new(model.clone())
        .agent(AgentProfile::new(&input.kind))
        .sampling_policies(store);
    let queue = unique("live-q");
    let worker = worker(&queue, harness);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(&url);
        async move { worker.run(&pool).await }
    });

    let refused = outcome(&url, start(&url, &queue, &input).await).await;
    let error = refused.expect_err("the default policy refuses best_of_n");
    assert!(error.contains("strategy_not_allowed"), "{error}");
    assert_eq!(
        AtomicUsize::load(&model.calls, Ordering::SeqCst),
        0,
        "no model call"
    );

    // The operator process has its own pool.
    let operator = PostgresSamplingPolicyStore::new(pool(&url));
    operator
        .set(&kind, &best_of_n(2, 2), "ops@example.com")
        .await
        .unwrap();

    let report = outcome(&url, start(&url, &queue, &input).await)
        .await
        .expect("the next run reads the new policy");
    assert_eq!(report.draws.len(), 2);
    assert_eq!(report.revision, 1);
    worker.shutdown();
    handle.abort();
}
