//! The `agent_sample` workflow on a Postgres worker (issue #2443).
//!
//! Best-of-3 picks the answer that the scorer prefers. The usage of each
//! draw sums to the `harvest_llm_ledger` rows of the run tree.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database. Without it, each test starts a Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, HarvestBuilder, ShardId, StartWorkflowParams, WorkerConfig, migrate,
};
use autumn_harvest_agent::sampling::{
    PostgresSamplingPolicyStore, SAMPLE_WORKFLOW_NAME, SampleInput, SampleOutcome, SampleReport,
    SampleStrategy, SamplingPolicy, SamplingPolicyStore, ScorerRef, Strategy,
};
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentProfile, BoxFuture, ChatRequest, ChatResponse,
    ContentPart, DrawId, ExactMatch, StopReason, TokenUsage, workflow,
};
use diesel::sql_types::{BigInt, Nullable, Text, Uuid as SqlUuid};
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

/// A worker of the agent workflows on `queue` and on `default`, where the
/// agent activities go.
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

async fn start(url: &str, queue: &str, input: &SampleInput) -> ExecutionId {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let workflow_id = unique("sample");
    let params = StartWorkflowParams::new(
        SAMPLE_WORKFLOW_NAME,
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

/// Wait for the run to end, up to one minute.
async fn outcome(url: &str, exec: ExecutionId) -> SampleReport {
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
                return serde_json::from_str(&output).expect("a report");
            }
            "FAILED" | "CANCELLED" | "TERMINATED" | "TIMED_OUT" => {
                panic!(
                    "the run ended {}: {:?}",
                    row.state,
                    row.error.or(row.output)
                )
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "the run did not end");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(diesel::QueryableByName)]
struct LedgerRow {
    #[diesel(sql_type = BigInt)]
    calls: i64,
    #[diesel(sql_type = BigInt)]
    tokens: i64,
    #[diesel(sql_type = BigInt)]
    cost: i64,
}

/// The ledger total of the run `exec` and of its child runs.
async fn ledger_of_tree(url: &str, exec: ExecutionId) -> LedgerRow {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS calls,
                COALESCE(SUM(l.input_tokens + l.output_tokens), 0)::bigint AS tokens,
                COALESCE(SUM(l.cost_usd_micros), 0)::bigint AS cost
         FROM harvest_llm_ledger l
         WHERE l.workflow_exec_id = $1
            OR l.workflow_exec_id IN (
                SELECT id FROM harvest_workflow_executions WHERE parent_id = $1)",
    )
    .bind::<SqlUuid, _>(exec.as_uuid())
    .get_result(&mut conn)
    .await
    .expect("read the ledger")
}

/// A model that answers draw `n` with `"a"`, `"b"` or `"c"`. It prices
/// each token at 2 micro-dollars. It answers only requests that hold
/// `marker`, so a stray task from another suite never counts.
#[derive(Debug)]
struct Letters {
    marker: String,
}

impl AgentModel for Letters {
    fn chat<'a>(
        &'a self,
        request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        let ours = request.messages.iter().any(|message| {
            message
                .content
                .iter()
                .any(|part| matches!(part, ContentPart::Text(text) if text.contains(&self.marker)))
        });
        let text = match request.draw.map(DrawId::get) {
            Some(0) if ours => "a",
            Some(1) if ours => "b",
            _ => "c",
        };
        Box::pin(std::future::ready(Ok(ChatResponse {
            content: vec![ContentPart::Text(text.to_owned())],
            stop_reason: StopReason::EndTurn,
            usage: TokenUsage::new(6, 1),
        })))
    }

    fn model_id(&self) -> Option<&str> {
        Some("letters")
    }

    fn cost_usd_micros(&self, usage: &TokenUsage) -> Option<u64> {
        Some(u64::from(usage.total()) * 2)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn best_of_three_on_postgres_picks_b_and_sums_to_the_ledger() {
    let (url, _container) = database().await;
    let kind = unique("sample");
    let store = Arc::new(PostgresSamplingPolicyStore::new(pool(&url)));
    let policy = SamplingPolicy {
        strategies: BTreeSet::from([Strategy::BestOfN]),
        max_draws: 3,
        max_concurrent_draws: 3,
        ..SamplingPolicy::default()
    };
    store.set(&kind, &policy, "ops@example.com").await.unwrap();
    let harness = AgentHarness::new(Arc::new(Letters {
        marker: kind.clone(),
    }))
    .agent(AgentProfile::new(&kind))
    .scorer(Arc::new(ExactMatch::new("b")))
    .sampling_policies(store);
    let input = SampleInput::new(
        &kind,
        format!("Pick a letter ({kind})."),
        SampleStrategy::best_of_n(3),
        ScorerRef::deterministic(ExactMatch::ID).unwrap(),
    );
    let queue = unique("sample-q");
    let exec = start(&url, &queue, &input).await;
    let worker = worker(&queue, harness);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(&url);
        async move { worker.run(&pool).await }
    });
    let report = outcome(&url, exec).await;
    worker.shutdown();
    handle.abort();

    assert_eq!(report.selected.as_ref().map(|r| r.text.as_str()), Some("b"));
    assert_eq!(
        report.outcome,
        SampleOutcome::Selected {
            draw: DrawId::new(1)
        }
    );
    assert_eq!(report.draws.len(), 3, "3 verdicts");
    assert_eq!(report.concurrency, 3);

    let ledger = ledger_of_tree(&url, exec).await;
    assert_eq!(ledger.calls, 3, "one model call per draw");
    let tokens: u64 = report
        .draws
        .iter()
        .map(|draw| u64::from(draw.usage.total()))
        .sum();
    let cost: u64 = report.draws.iter().map(|draw| draw.cost_usd_micros).sum();
    assert_eq!(i64::try_from(tokens).unwrap(), ledger.tokens);
    assert_eq!(i64::try_from(cost).unwrap(), ledger.cost);
    assert_eq!(cost, 42, "3 draws of 7 tokens at 2 micro-dollars");
}
