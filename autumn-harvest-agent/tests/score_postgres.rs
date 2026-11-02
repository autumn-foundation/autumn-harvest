//! The `agent_score` activity on a Postgres worker (issue #2441).
//!
//! - The tokens of a judge scorer go to `harvest_llm_ledger` under the run
//!   that scores.
//! - A run budget of 0 refuses the judge call before it runs. The activity
//!   fails with `LlmBudgetExceeded`, and the judge model is never called.
//!
//! The tests use `HARVEST_TEST_DATABASE_URL` when it is set. Point it at a
//! dedicated database, and pass `--test-threads=1`. Each worker also polls
//! the shared `default` queue. Without the variable, each test starts a
//! Postgres container.
//! CI runs this target from a `linux` row of
//! `.github/ci/integration-suites.txt`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unnecessary_literal_bound
)]
// The `#[workflow]` macro reads its input through a binding.
#![allow(clippy::used_underscore_binding)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use autumn_harvest::llm_budget::LlmBudgetExceeded;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::prelude::*;
use autumn_harvest::worker::{DbPool, Worker, WorkerRuntimeConfig};
use autumn_harvest::{
    ExecutionId, HarvestBuilder, ShardId, StartWorkflowParams, WorkerConfig, migrate,
};
use autumn_harvest_agent::model::BoxFuture;
use autumn_harvest_agent::{
    AgentError, AgentHarness, AgentModel, AgentStop, ChatMessage, ChatRequest, ChatResponse,
    ChatRole, ContentPart, ScoreContext, ScoreInput, ScoreRequest, Scorer, StopReason, TokenUsage,
    Verdict, agent_score_info, workflow,
};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// The input of both test workflows.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Job {
    tenant: String,
    scorer: String,
}

/// Score once. Returns the verdict, or the error type of a failure.
async fn score_once(ctx: &WorkflowContext, job: Job) -> Value {
    let input = ScoreInput::new("Rate the answer.", "A fine answer.", AgentStop::Completed);
    let scored: HarvestResult<Verdict> = ctx
        .execute_activity(&agent_score_info(), ScoreRequest::new(job.scorer, input))
        .await;
    match scored {
        Ok(verdict) => json!({ "verdict": verdict }),
        Err(err) => json!({
            "error": err.activity_error_type(),
            "refusal": LlmBudgetExceeded::from_error(&err).map(|r| r.resource.as_str()),
        }),
    }
}

/// A workflow type with no LLM budget.
#[workflow]
async fn score_open(ctx: &WorkflowContext, job: Job) -> Result<Value, String> {
    Ok(score_once(ctx, job).await)
}

/// A workflow type whose run budget is 0 tokens.
#[workflow(quota(key = "input.tenant", max_run_llm_tokens = 0))]
async fn score_capped(ctx: &WorkflowContext, job: Job) -> Result<Value, String> {
    Ok(score_once(ctx, job).await)
}

/// A judge model that answers `0.6` and reports its usage.
#[derive(Debug, Default)]
struct JudgeModel {
    calls: AtomicUsize,
}

impl AgentModel for JudgeModel {
    fn chat<'a>(
        &'a self,
        _request: &'a ChatRequest,
    ) -> BoxFuture<'a, Result<ChatResponse, AgentError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ChatResponse {
                content: vec![ContentPart::Text("0.6".into())],
                stop_reason: StopReason::EndTurn,
                usage: TokenUsage::new(40, 2),
            })
        })
    }

    fn model_id(&self) -> Option<&str> {
        Some("judge-pg")
    }
}

/// A judge scorer: one judge call, then its reply as the score.
#[derive(Debug)]
struct Judge;

impl Scorer for Judge {
    fn id(&self) -> &str {
        "judge"
    }

    fn score<'a>(
        &'a self,
        input: &'a ScoreInput,
        cx: &'a ScoreContext<'_>,
    ) -> BoxFuture<'a, Result<Verdict, AgentError>> {
        Box::pin(async move {
            let request = ChatRequest {
                messages: vec![ChatMessage::text(ChatRole::User, input.answer.clone())],
                tools: Vec::new(),
                max_tokens: Some(8),
                temperature: None,
                draw: None,
            };
            let reply = cx.chat(None, &request).await?;
            let Some(ContentPart::Text(text)) = reply.content.first() else {
                return Ok(Verdict::reject("the judge gave no text"));
            };
            Verdict::score(text.parse().unwrap_or(f64::NAN))
        })
    }
}

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

/// A worker of the test workflows and the agent activities on `queue`.
///
/// The agent activities name no queue, so they go to `default`. The worker
/// also polls that queue.
fn worker(queue: &str, harness: AgentHarness) -> Arc<Worker> {
    let config = WorkerConfig::default().with_queues([queue, "default"]);
    let (registry, _dags, _schedules, config) = HarvestBuilder::new()
        .workflows(vec![score_open_info(), score_capped_info()])
        .activities(workflow::activities())
        .state(harness)
        .worker(config)
        .build()
        .into_worker_parts();
    let mut runtime = WorkerRuntimeConfig::from(config);
    runtime.poll_interval = Duration::from_millis(25);
    Arc::new(Worker::new(runtime, Arc::new(registry)).expect("worker"))
}

async fn start(url: &str, queue: &str, name: &str, job: &Job) -> ExecutionId {
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let workflow_id = unique("score-pg");
    let params = StartWorkflowParams::new(
        name,
        &workflow_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        serde_json::to_value(job).expect("job"),
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

/// The SQL reads of the tests. Diesel's `RunQueryDsl` stays in here, so
/// its `first` and `load` do not hide the methods of std types.
mod sql {
    use std::time::{Duration, Instant};

    use autumn_harvest::ExecutionId;
    use diesel::sql_types::{BigInt, Nullable, Text};
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde_json::Value;

    #[derive(diesel::QueryableByName)]
    struct RunRow {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = Nullable<Text>)]
        output: Option<String>,
    }

    /// Wait for the run to complete, up to `limit`. Returns its output.
    pub async fn output(url: &str, exec: ExecutionId, limit: Duration) -> Value {
        let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
        let deadline = Instant::now() + limit;
        loop {
            let row: RunRow = diesel::sql_query(
                "SELECT state, output::text AS output FROM harvest_workflow_executions \
                 WHERE id = $1::uuid",
            )
            .bind::<Text, _>(exec.to_string())
            .get_result(&mut conn)
            .await
            .expect("load run");
            match row.state.as_str() {
                "COMPLETED" => {
                    let output = row.output.expect("a completed run has an output");
                    return serde_json::from_str(&output).expect("an output");
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

    #[derive(Debug, PartialEq, Eq, diesel::QueryableByName)]
    pub struct LedgerRow {
        #[diesel(sql_type = Text)]
        pub activity_name: String,
        #[diesel(sql_type = Text)]
        pub model: String,
        #[diesel(sql_type = BigInt)]
        pub input_tokens: i64,
        #[diesel(sql_type = BigInt)]
        pub output_tokens: i64,
    }

    pub async fn ledger(url: &str, exec: ExecutionId) -> Vec<LedgerRow> {
        let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
        diesel::sql_query(
            "SELECT activity_name, model, input_tokens, output_tokens FROM harvest_llm_ledger \
             WHERE workflow_exec_id = $1::uuid ORDER BY event_id, call_index",
        )
        .bind::<Text, _>(exec.to_string())
        .load(&mut conn)
        .await
        .expect("ledger rows")
    }
}

/// Run one job of `name` on a fresh worker. Returns the run, its output
/// and the judge model.
async fn run(url: &str, name: &str) -> (ExecutionId, Value, Arc<JudgeModel>) {
    let model = Arc::new(JudgeModel::default());
    let harness =
        AgentHarness::new(Arc::clone(&model) as Arc<dyn AgentModel>).scorer(Arc::new(Judge));
    let queue = unique("score-q");
    let job = Job {
        tenant: unique("tenant"),
        scorer: "judge".into(),
    };
    let exec = start(url, &queue, name, &job).await;
    let worker = worker(&queue, harness);
    let handle = tokio::spawn({
        let worker = Arc::clone(&worker);
        let pool = pool(url);
        async move { worker.run(&pool).await }
    });
    let out = sql::output(url, exec, Duration::from_secs(60)).await;
    worker.shutdown();
    handle.abort();
    (exec, out, model)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn judge_tokens_land_in_the_ledger_of_the_run() {
    let (url, _container) = database().await;
    let (exec, out, model) = run(&url, "score_open").await;
    assert_eq!(out, json!({ "verdict": { "score": 0.6 } }));
    assert_eq!(model.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        sql::ledger(&url, exec).await,
        [sql::LedgerRow {
            activity_name: "agent_score".into(),
            model: "judge-pg".into(),
            input_tokens: 40,
            output_tokens: 2,
        }]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_run_budget_stops_the_judge_before_its_call() {
    let (url, _container) = database().await;
    let (exec, out, model) = run(&url, "score_capped").await;
    assert_eq!(
        out,
        json!({ "error": "LlmBudgetExceeded", "refusal": "run_llm_tokens" })
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 0, "no judge call ran");
    assert_eq!(sql::ledger(&url, exec).await.len(), 0);
}
