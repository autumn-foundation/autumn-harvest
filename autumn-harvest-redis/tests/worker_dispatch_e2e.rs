//! End-to-end tests: a real worker, a real Postgres, and Redis dispatch
//! (issue #1312).
//!
//! Every case runs an `autumn_harvest::worker::Worker` against a live
//! Postgres, with [`RedisDispatch`] installed as the process-global dispatch
//! channel. The channel carries references only. Each case asserts on both
//! sides. The workflow reaches its terminal state in Postgres. The Redis
//! stream, the pending entries list and the dedupe markers drain.
//!
//! ## Running them
//!
//! Set both variables to use an operator-supplied Postgres and Redis:
//!
//! ```sh
//! HARVEST_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/harvest_redis_e2e \
//! HARVEST_REDIS_TEST_URL=redis://127.0.0.1:6379 \
//!   cargo test -p autumn-harvest-redis --test worker_dispatch_e2e
//! ```
//!
//! Without them each case starts a `testcontainers` Postgres and a
//! `testcontainers` Redis, and skips when Docker is unavailable. Set
//! `HARVEST_TEST_REQUIRE_REDIS=1` to turn that skip into a failure, which is
//! what CI does: a suite that silently skips proves nothing.
//!
//! An operator-supplied database must carry the harvest schema, or the
//! fixture applies `autumn_harvest::test_init_sql()` to it once. A container
//! database gets the same SQL through `with_init_sql`.
//!
//! The cases share one process-global dispatch installation, so a static
//! mutex serializes them.
//!
//! ## What the counters prove
//!
//! Every case asserts that the run really travelled on the channel, through
//! the counting wrapper in [`CountingDispatch`]. Without that check a case
//! would pass on a worker that ignored the channel. An empty stream then
//! proves nothing.
//!
//! ## The two crash cases
//!
//! Both run a second copy of this test binary as a child process. Both kill
//! it between the by-id claim commit and the reference ack. Each child entry
//! point is `#[ignore]`d, so an ordinary test run never starts it.
//!
//! `crash_between_claim_commit_and_ack_neither_loses_nor_duplicates` installs
//! a channel that aborts the process inside `ack`. The kill lands on the
//! first claim, which is the workflow task.
//!
//! `crash_on_the_activity_claim_neither_loses_nor_duplicates` arms the chaos
//! point `DISPATCH_AFTER_CLAIM_BEFORE_ACK` on its second hit, which is the
//! activity task claim. The claim has committed, the row is `RUNNING`, and
//! the reference is still in the pending entries list.

use std::pin::Pin;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::dispatch::{
    DispatchHint, DispatchLease, DispatchMaintenance, DispatchSettings, TaskDispatch,
};
use autumn_harvest::error::HarvestResult;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{Priority, StartSource, StartWorkflowParams, WorkflowContext};
use autumn_harvest_redis::{RedisDispatch, RedisDispatchConfig};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::prelude::QueryableByName;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use redis::AsyncCommands;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Environment.
// ---------------------------------------------------------------------------

const DB_URL_VAR: &str = "HARVEST_TEST_DATABASE_URL";
const REDIS_URL_VAR: &str = "HARVEST_REDIS_TEST_URL";
const REQUIRE_VAR: &str = "HARVEST_TEST_REQUIRE_REDIS";
const PREFIX_VAR: &str = "HARVEST_E2E_KEY_PREFIX";
const EXEC_ID_VAR: &str = "HARVEST_E2E_EXEC_ID";
const RUN_ID_VAR: &str = "HARVEST_E2E_RUN_ID";

const CONSUMER_GROUP: &str = "harvest_workers";
const QUEUE: &str = "default";

/// The Postgres and Redis a case runs against, and the containers that serve
/// them.
///
/// The containers stop when this value drops, so a case must hold it for its
/// whole body.
struct Fixture {
    database_url: String,
    redis_url: String,
    _postgres: Option<ContainerAsync<Postgres>>,
    _redis: Option<ContainerAsync<Redis>>,
}

/// Whether a missing fixture must fail the run instead of skipping it.
fn fixture_is_required() -> bool {
    std::env::var(REQUIRE_VAR).is_ok_and(|value| value == "1")
}

/// Both fixtures, or `None` so the case skips.
///
/// An environment variable wins over a container, so a developer machine with
/// a local Postgres and Redis needs no Docker. Under `HARVEST_TEST_REQUIRE_REDIS=1`
/// a missing fixture panics: a suite that silently skips proves nothing.
async fn fixture() -> Option<Fixture> {
    match build_fixture().await {
        Ok(fixture) => Some(fixture),
        Err(reason) => {
            assert!(
                !fixture_is_required(),
                "{REQUIRE_VAR}=1 demands a live fixture, and none could be obtained: {reason}"
            );
            eprintln!("skipping: {reason}");
            None
        }
    }
}

async fn build_fixture() -> Result<Fixture, String> {
    let (redis_url, redis_container) = redis_fixture().await?;
    let (database_url, postgres_container) = postgres_fixture().await?;
    Ok(Fixture {
        database_url,
        redis_url,
        _postgres: postgres_container,
        _redis: redis_container,
    })
}

/// A Redis URL from the environment, or a `redis:5.0` container.
async fn redis_fixture() -> Result<(String, Option<ContainerAsync<Redis>>), String> {
    if let Ok(url) = std::env::var(REDIS_URL_VAR) {
        return Ok((url, None));
    }
    let container = Redis::default()
        .start()
        .await
        .map_err(|err| format!("no redis container: {err}"))?;
    let host = container
        .get_host()
        .await
        .map_err(|err| format!("no redis host: {err}"))?;
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .map_err(|err| format!("no redis port: {err}"))?;
    Ok((format!("redis://{host}:{port}"), Some(container)))
}

/// A Postgres URL from the environment, or a `postgres:16` container carrying
/// the harvest schema.
async fn postgres_fixture() -> Result<(String, Option<ContainerAsync<Postgres>>), String> {
    if let Ok(url) = std::env::var(DB_URL_VAR) {
        ensure_schema(&url).await?;
        return Ok((url, None));
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().into_bytes())
        .with_tag("16")
        .start()
        .await
        .map_err(|err| format!("no postgres container: {err}"))?;
    let host = container
        .get_host()
        .await
        .map_err(|err| format!("no postgres host: {err}"))?;
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .map_err(|err| format!("no postgres port: {err}"))?;
    Ok((
        format!("postgres://postgres:postgres@{host}:{port}/postgres"),
        Some(container),
    ))
}

/// Apply the harvest schema to an operator-supplied database once.
///
/// The probe runs on its own connection. A failed probe poisons the
/// connection, so the apply needs a fresh one.
async fn ensure_schema(url: &str) -> Result<(), String> {
    let mut conn = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .map_err(|err| format!("{DB_URL_VAR} is unreachable: {err}"))?;
    if conn
        .batch_execute("SELECT 1 FROM harvest_workflow_executions LIMIT 0")
        .await
        .is_ok()
    {
        return Ok(());
    }
    let mut fresh = <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .map_err(|err| format!("{DB_URL_VAR} is unreachable: {err}"))?;
    fresh
        .batch_execute(&autumn_harvest::test_init_sql())
        .await
        .map_err(|err| format!("the harvest schema could not be applied: {err}"))
}

/// Serializes the cases: the dispatch channel is process-global.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Database URL and run id for the activity handlers.
///
/// The handlers are plain function pointers with no shared state. They also
/// run in the child process of the crash case. The side-effect counter
/// therefore lives in Postgres, and these two statics carry the coordinates.
/// A process-global holds them rather than an environment variable, because
/// `set_var` is unsound while worker threads are running.
static HANDLER_DB_URL: std::sync::RwLock<String> = std::sync::RwLock::new(String::new());
static HANDLER_RUN_ID: std::sync::RwLock<String> = std::sync::RwLock::new(String::new());

fn set_handler_scope(db_url: &str, run_id: &str) {
    *HANDLER_DB_URL.write().expect("db url lock") = db_url.to_string();
    *HANDLER_RUN_ID.write().expect("run id lock") = run_id.to_string();
}

fn handler_db_url() -> String {
    HANDLER_DB_URL.read().expect("db url lock").clone()
}

fn handler_run_id() -> String {
    HANDLER_RUN_ID.read().expect("run id lock").clone()
}

/// Removes the process-global channel when a case ends.
struct DispatchGuard;

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        autumn_harvest::dispatch::uninstall();
    }
}

/// Read one counter.
///
/// A free function because `RunQueryDsl` is in scope here and its `load`
/// method shadows `AtomicUsize::load` on an `Arc`.
fn count(counter: &AtomicUsize) -> usize {
    counter.load(AtomicOrdering::Relaxed)
}

/// Counts what the engine did with the channel.
///
/// Without these counters a case would pass on a worker that ignored the
/// channel. The stream would then be empty for the wrong reason.
#[derive(Clone, Default)]
struct Counters {
    published: Arc<AtomicUsize>,
    delivered: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
}

impl Counters {
    fn assert_the_channel_carried_the_run(&self) {
        assert!(
            count(&self.published) > 0,
            "the engine must publish references through the channel"
        );
        assert!(
            count(&self.delivered) > 0,
            "the worker must read references from the channel"
        );
        assert!(
            count(&self.acked) > 0,
            "the worker must ack the references it claims"
        );
    }
}

/// A channel that counts what passes through it, over the real one.
#[derive(Debug)]
struct CountingDispatch {
    inner: RedisDispatch,
    published: Arc<AtomicUsize>,
    delivered: Arc<AtomicUsize>,
    acked: Arc<AtomicUsize>,
}

#[async_trait]
impl TaskDispatch for CountingDispatch {
    async fn publish(&self, hints: &[DispatchHint]) -> HarvestResult<()> {
        self.published
            .fetch_add(hints.len(), AtomicOrdering::Relaxed);
        self.inner.publish(hints).await
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> HarvestResult<Vec<DispatchLease>> {
        let leases = self.inner.next(queues, consumer, max, wait).await?;
        self.delivered
            .fetch_add(leases.len(), AtomicOrdering::Relaxed);
        Ok(leases)
    }

    async fn ack(&self, lease: &DispatchLease) -> HarvestResult<()> {
        self.acked.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.ack(lease).await
    }

    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()> {
        self.inner.release(lease, delay).await
    }

    async fn maintain(&self, queues: &[String]) -> HarvestResult<DispatchMaintenance> {
        self.inner.maintain(queues).await
    }
}

/// Install a counting `RedisDispatch` as the process-global channel.
///
/// Returns the bare channel as well, so a case can publish a reference by
/// hand without disturbing the counters.
async fn install_dispatch(
    redis_url: &str,
    prefix: &str,
    visibility_timeout: Duration,
) -> (RedisDispatch, Counters, DispatchGuard) {
    let dispatch = RedisDispatch::connect(
        redis_url,
        RedisDispatchConfig {
            key_prefix: prefix.to_string(),
            consumer_group: CONSUMER_GROUP.to_string(),
            visibility_timeout,
            dedupe_ttl: Duration::from_secs(600),
        },
    )
    .await
    .expect("redis dispatch should connect");
    let counters = Counters::default();
    autumn_harvest::dispatch::install(
        Arc::new(CountingDispatch {
            inner: dispatch.clone(),
            published: Arc::clone(&counters.published),
            delivered: Arc::clone(&counters.delivered),
            acked: Arc::clone(&counters.acked),
        }),
        DispatchSettings {
            poll_interval: Duration::from_millis(20),
            reconcile_interval: Duration::from_millis(500),
            reconcile_batch: 100,
            release_backoff_cap: Duration::from_secs(2),
        },
    );
    (dispatch, counters, DispatchGuard)
}

// ---------------------------------------------------------------------------
// Redis inspection.
// ---------------------------------------------------------------------------

/// Reads the dispatch key space directly, so a case can assert that the
/// channel drained.
struct RedisProbe {
    conn: redis::aio::ConnectionManager,
    prefix: String,
}

impl RedisProbe {
    async fn connect(url: &str, prefix: &str) -> Self {
        let client = redis::Client::open(url).expect("redis url should parse");
        let conn = redis::aio::ConnectionManager::new(client)
            .await
            .expect("redis should accept a connection");
        Self {
            conn,
            prefix: prefix.to_string(),
        }
    }

    fn stream_key(&self, queue: &str) -> String {
        format!("{}:dispatch:{queue}", self.prefix)
    }

    async fn stream_len(&self, queue: &str) -> i64 {
        let mut conn = self.conn.clone();
        conn.xlen(self.stream_key(queue)).await.unwrap_or(0)
    }

    async fn pending_count(&self, queue: &str) -> usize {
        let mut conn = self.conn.clone();
        let reply: redis::RedisResult<redis::streams::StreamPendingReply> =
            conn.xpending(self.stream_key(queue), CONSUMER_GROUP).await;
        match reply {
            Ok(redis::streams::StreamPendingReply::Data(data)) => data.count,
            _ => 0,
        }
    }

    async fn keys(&self, pattern: &str) -> Vec<String> {
        let mut conn = self.conn.clone();
        let mut cursor: u64 = 0;
        let mut found = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .expect("scan");
            found.extend(batch);
            cursor = next;
            if cursor == 0 {
                return found;
            }
        }
    }

    async fn marker_keys(&self) -> Vec<String> {
        self.keys(&format!("{}:dispatch:marker:*", self.prefix))
            .await
    }

    /// Delete every key under this fixture's prefix, and nothing else.
    async fn wipe_prefix(&self) {
        let keys = self.keys(&format!("{}:*", self.prefix)).await;
        if keys.is_empty() {
            return;
        }
        let mut conn = self.conn.clone();
        let _: i64 = conn.del(keys).await.expect("del");
    }

    /// Poll until the channel holds nothing for `queue`, or give up.
    ///
    /// A reference a crashed worker left in the pending entries list is
    /// recovered on the visibility timeout. That recovery can land after the
    /// run reaches its terminal state. It puts one entry back on the stream
    /// for the running worker to ack. The wait lets that happen, so the drain
    /// assertion measures convergence and not the instant the workflow
    /// finished. Call it while a worker still runs.
    async fn await_drained(&self, queue: &str, deadline: Duration) {
        let until = Instant::now() + deadline;
        loop {
            let drained = self.stream_len(queue).await == 0
                && self.pending_count(queue).await == 0
                && self.marker_keys().await.is_empty();
            if drained || Instant::now() >= until {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Assert that the channel holds nothing for `queue`.
    async fn assert_drained(&self, queue: &str) {
        assert_eq!(
            self.stream_len(queue).await,
            0,
            "the dispatch stream must be empty after the run"
        );
        assert_eq!(
            self.pending_count(queue).await,
            0,
            "the pending entries list must be empty after the run"
        );
        assert!(
            self.marker_keys().await.is_empty(),
            "every dedupe marker must be gone after the run"
        );
    }
}

// ---------------------------------------------------------------------------
// Postgres helpers.
// ---------------------------------------------------------------------------

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    value: i64,
}

#[derive(QueryableByName)]
struct TypedStateRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    value: String,
}

#[derive(QueryableByName)]
struct UuidRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    value: Uuid,
}

#[derive(QueryableByName)]
struct TimeRow {
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    value: DateTime<Utc>,
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("postgres should accept a connection")
}

fn build_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool should build")
}

/// Create the side-effect table these cases count rows in.
///
/// The table is not part of the engine schema. It records one row per
/// activity attempt, so a case can prove an activity body ran exactly once.
async fn ensure_side_effect_table(conn: &mut AsyncPgConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS harvest_redis_e2e_side_effects (
             id bigserial PRIMARY KEY,
             run_id text NOT NULL,
             noted_at timestamptz NOT NULL DEFAULT now()
         )",
    )
    .execute(conn)
    .await
    .expect("side effect table should be created");
}

async fn side_effect_count(conn: &mut AsyncPgConnection, run_id: &str) -> i64 {
    diesel::sql_query(
        "SELECT count(*)::bigint AS value FROM harvest_redis_e2e_side_effects WHERE run_id = $1",
    )
    .bind::<diesel::sql_types::Text, _>(run_id)
    .get_result::<CountRow>(conn)
    .await
    .expect("count")
    .value
}

async fn side_effect_times(conn: &mut AsyncPgConnection, run_id: &str) -> Vec<DateTime<Utc>> {
    diesel::sql_query(
        "SELECT noted_at AS value FROM harvest_redis_e2e_side_effects
         WHERE run_id = $1 ORDER BY id",
    )
    .bind::<diesel::sql_types::Text, _>(run_id)
    .load::<TimeRow>(conn)
    .await
    .expect("times")
    .into_iter()
    .map(|row| row.value)
    .collect()
}

async fn execution_state(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> String {
    diesel::sql_query("SELECT state AS value FROM harvest_workflow_executions WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .get_result::<TextRow>(conn)
        .await
        .expect("execution row")
        .value
}

async fn task_states(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Vec<String> {
    diesel::sql_query(
        "SELECT state AS value FROM harvest_task_queue WHERE workflow_exec_id = $1 ORDER BY id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load::<TextRow>(conn)
    .await
    .expect("task rows")
    .into_iter()
    .map(|row| row.value)
    .collect()
}

/// The state of every task row of `exec_id`, keyed by task type.
async fn task_states_by_type(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Vec<(String, String)> {
    diesel::sql_query(
        "SELECT task_type AS kind, state AS value FROM harvest_task_queue
         WHERE workflow_exec_id = $1 ORDER BY id",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .load::<TypedStateRow>(conn)
    .await
    .expect("task rows")
    .into_iter()
    .map(|row| (row.kind, row.value))
    .collect()
}

async fn any_task_id(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> Option<Uuid> {
    diesel::sql_query(
        "SELECT id AS value FROM harvest_task_queue WHERE workflow_exec_id = $1
         ORDER BY id LIMIT 1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<UuidRow>(conn)
    .await
    .ok()
    .map(|row| row.value)
}

/// Poll until `state` reaches one of `wanted`, or fail after `deadline`.
async fn await_execution_state(
    url: &str,
    exec_id: ExecutionId,
    wanted: &[&str],
    deadline: Duration,
) -> String {
    let mut conn = connect(url).await;
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < deadline {
        last = execution_state(&mut conn, exec_id).await;
        if wanted.contains(&last.as_str()) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("execution stayed in {last} after {deadline:?}, wanted one of {wanted:?}");
}

// ---------------------------------------------------------------------------
// Workflow start.
// ---------------------------------------------------------------------------

async fn start_workflow(
    conn: &mut AsyncPgConnection,
    workflow_name: &'static str,
    workflow_id: &str,
) -> ExecutionId {
    autumn_harvest::start_or_load_workflow_execution(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id,
            exec_id: ExecutionId::new_for_shard(ShardId::new(0)),
            input: serde_json::Value::Null,
            parent_id: None,
            queue_name: QUEUE,
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
        },
        None,
    )
    .await
    .expect("workflow start should succeed")
    .exec_id
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// A workflow with two activities in sequence.
fn wf_two_activities(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("e2e_note", input.clone(), QUEUE)
            .await
            .map_err(|err| err.to_string())?;
        ctx.execute_activity_raw("e2e_noop", input, QUEUE)
            .await
            .map_err(|err| err.to_string())
    })
}

/// A workflow that parks on a signal, then runs one activity.
fn wf_wait_for_signal(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.wait_for_signal("go")
            .await
            .map_err(|err| err.to_string())?;
        ctx.execute_activity_raw("e2e_note", input, QUEUE)
            .await
            .map_err(|err| err.to_string())
    })
}

/// A workflow whose single activity fails once and then succeeds.
fn wf_retrying(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("e2e_flaky", input, QUEUE)
            .await
            .map_err(|err| err.to_string())
    })
}

/// A workflow with exactly one side-effecting activity.
fn wf_single_note(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        ctx.execute_activity_raw("e2e_note", input, QUEUE)
            .await
            .map_err(|err| err.to_string())
    })
}

/// Records one row per attempt in the side-effect table.
fn act_note(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        note_side_effect().await;
        Ok(serde_json::json!({"noted": true}))
    })
}

/// Does nothing. Present so a workflow can have a second activity.
fn act_noop(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move { Ok(serde_json::json!({"ok": true})) })
}

/// Records the attempt, then fails on the first attempt only.
///
/// The row count is the attempt count, so the case can read the start time of
/// each attempt back out of Postgres.
fn act_flaky(_ctx: &autumn_harvest::ActivityContext, _input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let attempts = note_side_effect().await;
        if attempts <= 1 {
            return Err("transient failure".to_string());
        }
        Ok(serde_json::json!({"ok": true}))
    })
}

/// Insert one side-effect row and return the number of rows for this run.
///
/// The handler runs in the parent process and in the crash case's child
/// process, so the counter lives in Postgres rather than in process memory.
async fn note_side_effect() -> i64 {
    let url = handler_db_url();
    let run_id = handler_run_id();
    let mut conn = connect(&url).await;
    diesel::sql_query("INSERT INTO harvest_redis_e2e_side_effects (run_id) VALUES ($1)")
        .bind::<diesel::sql_types::Text, _>(&run_id)
        .execute(&mut conn)
        .await
        .expect("side effect insert");
    side_effect_count(&mut conn, &run_id).await
}

fn workflow_info(
    name: &'static str,
    handler: autumn_harvest::info::WorkflowHandlerFn,
) -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name,
        module: "worker_dispatch_e2e",
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

fn activity_info(
    name: &'static str,
    handler: autumn_harvest::info::ActivityHandlerFn,
    retry: Option<RetryPolicy>,
) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "worker_dispatch_e2e",
        default_retry_policy: retry,
        default_start_to_close: Some(Duration::from_secs(30)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some(QUEUE),
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

/// Every workflow and activity these cases use, in one registry.
fn registry() -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(Arc::new(NoOpMetrics) as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let retry = RetryPolicy {
        max_attempts: 3,
        initial_interval: Duration::from_secs(2),
        backoff_coefficient: 1.0,
        max_interval: Duration::from_secs(2),
        non_retryable_errors: Vec::new(),
        jitter: JitterPolicy::None,
    };
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![
            workflow_info("e2e_two_activities", wf_two_activities),
            workflow_info("e2e_wait_for_signal", wf_wait_for_signal),
            workflow_info("e2e_retrying", wf_retrying),
            workflow_info("e2e_single_note", wf_single_note),
        ],
        vec![
            activity_info("e2e_note", act_note, None),
            activity_info("e2e_noop", act_noop, None),
            activity_info("e2e_flaky", act_flaky, Some(retry)),
        ],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// Runtime config built from the builder default, so a field added to
/// `WorkerRuntimeConfig` does not break this file.
fn runtime_config(worker_id: &str, heartbeat: Duration) -> WorkerRuntimeConfig {
    let mut config: WorkerRuntimeConfig = WorkerConfig::default().with_queues([QUEUE]).into();
    config.worker_id = worker_id.to_string();
    config.poll_interval = Duration::from_millis(50);
    config.shutdown_timeout = Duration::from_secs(2);
    config.worker_heartbeat_interval = heartbeat;
    config.shard_assignments = vec![ShardId::new(0)];
    config.max_concurrent_workflows = 4;
    config.max_concurrent_activities = 4;
    config
}

/// A running worker plus the handle that stops it.
struct RunningWorker {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl RunningWorker {
    fn start(url: &str, worker_id: &str, heartbeat: Duration) -> Self {
        let worker = Arc::new(
            Worker::new(runtime_config(worker_id, heartbeat), registry())
                .expect("worker should build"),
        );
        let pool = build_pool(url);
        let handle = tokio::spawn({
            let worker = Arc::clone(&worker);
            async move { worker.run(&pool).await }
        });
        Self { worker, handle }
    }

    async fn stop(self) {
        self.worker.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

// ---------------------------------------------------------------------------
// Cases.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn workflow_completes_via_redis_dispatch() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(5)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let worker = RunningWorker::start(&db_url, &format!("w-{prefix}"), Duration::from_secs(1));
    let exec_id = start_workflow(&mut conn, "e2e_two_activities", &format!("wf-{prefix}")).await;

    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    assert_eq!(state, "COMPLETED");
    worker.stop().await;

    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        1,
        "the side-effecting activity must run exactly once"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn signal_wakes_a_parked_workflow_through_redis() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    set_handler_scope(&db_url, &prefix);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(5)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let worker = RunningWorker::start(&db_url, &format!("w-{prefix}"), Duration::from_secs(1));
    let exec_id = start_workflow(&mut conn, "e2e_wait_for_signal", &format!("wf-{prefix}")).await;

    // Let the run reach its first suspension before the signal arrives, so
    // the signal really has to wake a parked workflow task.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        execution_state(&mut conn, exec_id).await,
        "RUNNING",
        "the run must still be parked on the signal"
    );

    autumn_harvest::signal::send_signal(&mut conn, exec_id, "go", serde_json::json!({"n": 1}))
        .await
        .expect("signal should be accepted");

    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    assert_eq!(state, "COMPLETED");
    worker.stop().await;
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn activity_retry_delay_is_honoured() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(10)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let worker = RunningWorker::start(&db_url, &format!("w-{prefix}"), Duration::from_secs(1));
    let exec_id = start_workflow(&mut conn, "e2e_retrying", &format!("wf-{prefix}")).await;

    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(40)).await;
    assert_eq!(state, "COMPLETED");
    worker.stop().await;

    let times = side_effect_times(&mut conn, &run_id).await;
    assert_eq!(times.len(), 2, "the activity must run twice");
    let gap = times[1] - times[0];
    assert!(
        gap >= chrono::Duration::milliseconds(1800),
        "the second attempt must respect the two second retry delay, gap was {gap}"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_reference_for_a_completed_row_is_acked_without_rerun() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    let (dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(5)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let worker = RunningWorker::start(&db_url, &format!("w-{prefix}"), Duration::from_secs(1));
    let exec_id = start_workflow(&mut conn, "e2e_single_note", &format!("wf-{prefix}")).await;
    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    assert_eq!(state, "COMPLETED");
    assert_eq!(side_effect_count(&mut conn, &run_id).await, 1);

    // Publish a reference to a row that is already finished. The worker must
    // ack it and run nothing.
    let task_id = any_task_id(&mut conn, exec_id)
        .await
        .expect("a completed run must leave a task row to reference");
    dispatch
        .publish(&[DispatchHint {
            task_id,
            queue_name: QUEUE.to_string(),
            scheduled_at: Utc::now(),
            priority: 0,
            shard: None,
            kind: Some(autumn_harvest::dispatch::DispatchKind::Workflow),
        }])
        .await
        .expect("publish");

    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && !probe.marker_keys().await.is_empty() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    worker.stop().await;

    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        1,
        "a duplicate reference must not re-run the activity"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_between_claim_commit_and_ack_neither_loses_nor_duplicates() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    // The parent installs the channel too, so the workflow start publishes
    // the reference the child reads.
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(2)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let exec_id = start_workflow(&mut conn, "e2e_single_note", &format!("wf-{prefix}")).await;

    let mut child = spawn_child(
        "crash_child_worker",
        &db_url,
        &redis_url,
        &prefix,
        exec_id,
        &run_id,
    );
    let status = wait_for_child(&mut child, Duration::from_secs(25));

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        assert!(
            status.signal().is_some(),
            "the child must die by a signal, not exit normally: {status:?}"
        );
    }

    assert!(
        task_states(&mut conn, exec_id)
            .await
            .iter()
            .any(|state| state == "RUNNING"),
        "the claim committed, so a task row must be RUNNING"
    );
    assert_eq!(
        probe.pending_count(QUEUE).await,
        1,
        "the unacked reference must still be in the pending entries list"
    );

    // A second worker, in this process, must converge. The orphan reclaim
    // re-pends the row. The reconcile sweep republishes it, and the recovery
    // pass reclaims the stale reference.
    let worker = RunningWorker::start(&db_url, &format!("w2-{prefix}"), Duration::from_secs(1));
    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(30)).await;
    assert_eq!(state, "COMPLETED");
    // The stale reference the child left behind is recovered on the
    // visibility timeout, which can outlive the run. The worker stays up
    // until the channel is clean.
    probe.await_drained(QUEUE, Duration::from_secs(20)).await;
    worker.stop().await;

    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        1,
        "the crash must neither lose nor duplicate the activity"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_on_the_activity_claim_neither_loses_nor_duplicates() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    // The parent installs the channel too, so the workflow start publishes
    // the reference the child reads.
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(2)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let exec_id = start_workflow(&mut conn, "e2e_single_note", &format!("wf-{prefix}")).await;

    let mut child = spawn_child(
        "crash_child_worker_at_activity_claim",
        &db_url,
        &redis_url,
        &prefix,
        exec_id,
        &run_id,
    );
    let status = wait_for_child(&mut child, Duration::from_secs(25));

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        assert!(
            status.signal().is_some(),
            "the child must die by a signal, not exit normally: {status:?}"
        );
    }

    let rows = task_states_by_type(&mut conn, exec_id).await;
    assert!(
        rows.iter()
            .any(|(kind, state)| kind == "activity" && state == "RUNNING"),
        "the activity claim committed, so its row must be RUNNING: {rows:?}"
    );
    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        0,
        "the kill lands before the activity body runs"
    );
    assert_eq!(
        probe.pending_count(QUEUE).await,
        1,
        "the unacked activity reference must still be in the pending entries list"
    );

    // A second worker, in this process, must converge. The orphan reclaim
    // re-pends the row. The reconcile sweep republishes it, and the recovery
    // pass reclaims the stale reference.
    let worker = RunningWorker::start(&db_url, &format!("w2-{prefix}"), Duration::from_secs(1));
    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(60)).await;
    assert_eq!(state, "COMPLETED");
    // The stale reference the child left behind is recovered on the
    // visibility timeout, which can outlive the run. The worker stays up
    // until the channel is clean.
    probe.await_drained(QUEUE, Duration::from_secs(20)).await;
    worker.stop().await;

    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        1,
        "the crash must neither lose nor duplicate the activity"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_restart_converges_through_reconcile() {
    let Some(env) = fixture().await else {
        return;
    };
    let (db_url, redis_url) = (env.database_url.clone(), env.redis_url.clone());
    let _serial = SERIAL.lock().await;
    let prefix = format!("e2e_{}", Uuid::new_v4().simple());
    let run_id = prefix.clone();
    set_handler_scope(&db_url, &run_id);

    let mut conn = connect(&db_url).await;
    ensure_side_effect_table(&mut conn).await;
    let (_dispatch, counters, _guard) =
        install_dispatch(&redis_url, &prefix, Duration::from_secs(5)).await;
    let probe = RedisProbe::connect(&redis_url, &prefix).await;

    let worker = RunningWorker::start(&db_url, &format!("w-{prefix}"), Duration::from_secs(1));
    let exec_id = start_workflow(&mut conn, "e2e_two_activities", &format!("wf-{prefix}")).await;

    // Wait until the first activity has run, then lose every key. Only this
    // fixture's prefix is deleted.
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && side_effect_count(&mut conn, &run_id).await == 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    probe.wipe_prefix().await;

    let state =
        await_execution_state(&db_url, exec_id, &["COMPLETED"], Duration::from_secs(40)).await;
    assert_eq!(state, "COMPLETED");
    worker.stop().await;

    assert_eq!(
        side_effect_count(&mut conn, &run_id).await,
        1,
        "losing the channel must not re-run a finished activity"
    );
    counters.assert_the_channel_carried_the_run();
    probe.assert_drained(QUEUE).await;
}

/// Start a child copy of this test binary at one `#[ignore]`d entry point.
///
/// The child gets its fixture URLs through the environment, so it reaches the
/// same Postgres and the same Redis as the parent, container or not.
fn spawn_child(
    entry_point: &str,
    db_url: &str,
    redis_url: &str,
    prefix: &str,
    exec_id: ExecutionId,
    run_id: &str,
) -> std::process::Child {
    Command::new(std::env::current_exe().expect("test binary path"))
        .args(["--exact", entry_point, "--ignored", "--nocapture"])
        .env(DB_URL_VAR, db_url)
        .env(REDIS_URL_VAR, redis_url)
        .env(PREFIX_VAR, prefix)
        .env(EXEC_ID_VAR, exec_id.as_uuid().to_string())
        .env(RUN_ID_VAR, run_id)
        .spawn()
        .expect("the child worker should start")
}

/// Wait for the child, killing it if it outlives `deadline`.
fn wait_for_child(child: &mut std::process::Child, deadline: Duration) -> std::process::ExitStatus {
    let until = Instant::now() + deadline;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => return status,
            None if Instant::now() >= until => {
                let _ = child.kill();
                return child.wait().expect("wait after kill");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

// ---------------------------------------------------------------------------
// The child worker.
// ---------------------------------------------------------------------------

/// A channel that aborts the process inside `ack`.
///
/// The worker acks a reference immediately after the Postgres claim commits.
/// An abort here reproduces the crash window of the acceptance criterion. The
/// row is `RUNNING`, and the reference is still in the pending entries list.
#[derive(Debug)]
struct AbortBeforeAck {
    inner: RedisDispatch,
}

#[async_trait]
impl TaskDispatch for AbortBeforeAck {
    async fn publish(&self, hints: &[DispatchHint]) -> HarvestResult<()> {
        self.inner.publish(hints).await
    }

    async fn next(
        &self,
        queues: &[String],
        consumer: &str,
        max: usize,
        wait: Duration,
    ) -> HarvestResult<Vec<DispatchLease>> {
        self.inner.next(queues, consumer, max, wait).await
    }

    async fn ack(&self, _lease: &DispatchLease) -> HarvestResult<()> {
        eprintln!("child: aborting after the claim commit and before the ack");
        std::process::abort();
    }

    async fn release(&self, lease: &DispatchLease, delay: Duration) -> HarvestResult<()> {
        self.inner.release(lease, delay).await
    }

    async fn maintain(&self, queues: &[String]) -> HarvestResult<DispatchMaintenance> {
        self.inner.maintain(queues).await
    }
}

/// The child process entry point of the crash case.
///
/// Never returns normally: it aborts at the crash window, or the watchdog
/// stops it after the case's own deadline.
#[test]
#[ignore = "started as a child process by the crash case"]
fn crash_child_worker() {
    let db_url = std::env::var(DB_URL_VAR).expect("child needs the database url");
    let redis_url = std::env::var(REDIS_URL_VAR).expect("child needs the redis url");
    let prefix = std::env::var(PREFIX_VAR).expect("child needs the key prefix");
    let exec_id = std::env::var(EXEC_ID_VAR).expect("child needs the execution id");
    let run_id = std::env::var(RUN_ID_VAR).expect("child needs the run id");
    eprintln!("child: worker for execution {exec_id} under prefix {prefix}");
    set_handler_scope(&db_url, &run_id);

    // A panic anywhere in the worker becomes a process abort, so the parent
    // sees a signal rather than a quietly degraded worker.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("child: panic: {info}");
        std::process::abort();
    }));
    // The child must never outlive the parent's wait. Exiting normally here
    // fails the parent's signal assertion, which is the correct outcome: the
    // crash window was never reached.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(20));
        eprintln!("child: watchdog fired before the crash window");
        std::process::exit(9);
    });

    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    runtime.block_on(async move {
        let inner = RedisDispatch::connect(
            &redis_url,
            RedisDispatchConfig {
                key_prefix: prefix,
                consumer_group: CONSUMER_GROUP.to_string(),
                visibility_timeout: Duration::from_secs(2),
                dedupe_ttl: Duration::from_secs(600),
            },
        )
        .await
        .expect("child redis dispatch");
        autumn_harvest::dispatch::install(
            Arc::new(AbortBeforeAck { inner }),
            DispatchSettings {
                poll_interval: Duration::from_millis(20),
                reconcile_interval: Duration::from_millis(500),
                reconcile_batch: 100,
                release_backoff_cap: Duration::from_secs(2),
            },
        );

        let worker = Worker::new(
            runtime_config(&format!("child-{run_id}"), Duration::from_secs(1)),
            registry(),
        )
        .expect("child worker should build");
        let pool = build_pool(&db_url);
        worker.run(&pool).await;
    });
    eprintln!("child: worker returned without reaching the crash window");
    std::process::exit(10);
}

/// The child process entry point of the activity crash case.
///
/// The chaos point fires on its second hit. The first hit is the workflow
/// task claim, and the second is the activity task claim. The kill therefore
/// lands with the activity row `RUNNING` and its reference unacked.
#[test]
#[ignore = "started as a child process by the activity crash case"]
fn crash_child_worker_at_activity_claim() {
    let db_url = std::env::var(DB_URL_VAR).expect("child needs the database url");
    let redis_url = std::env::var(REDIS_URL_VAR).expect("child needs the redis url");
    let prefix = std::env::var(PREFIX_VAR).expect("child needs the key prefix");
    let exec_id = std::env::var(EXEC_ID_VAR).expect("child needs the execution id");
    let run_id = std::env::var(RUN_ID_VAR).expect("child needs the run id");
    eprintln!("child: chaos worker for execution {exec_id} under prefix {prefix}");
    set_handler_scope(&db_url, &run_id);

    // The child must never outlive the parent's wait. Exiting normally here
    // fails the parent's signal assertion, which is the correct outcome: the
    // crash window was never reached.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(20));
        eprintln!("child: watchdog fired before the crash window");
        std::process::exit(9);
    });

    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    runtime.block_on(async move {
        let _chaos =
            autumn_harvest::chaos::arm(autumn_harvest::chaos::ChaosPlan::scripted().kill_at_hit(
                autumn_harvest::chaos::points::DISPATCH_AFTER_CLAIM_BEFORE_ACK,
                2,
            ))
            .await;
        // `arm` installs a hook that reports a chaos kill and lets the task
        // unwind. The parent needs a dead process, so the hook is replaced
        // after arming, not before it.
        std::panic::set_hook(Box::new(|info| {
            eprintln!("child: panic: {info}");
            std::process::abort();
        }));

        let dispatch = RedisDispatch::connect(
            &redis_url,
            RedisDispatchConfig {
                key_prefix: prefix,
                consumer_group: CONSUMER_GROUP.to_string(),
                visibility_timeout: Duration::from_secs(2),
                dedupe_ttl: Duration::from_secs(600),
            },
        )
        .await
        .expect("child redis dispatch");
        autumn_harvest::dispatch::install(
            Arc::new(dispatch),
            DispatchSettings {
                poll_interval: Duration::from_millis(20),
                reconcile_interval: Duration::from_millis(500),
                reconcile_batch: 100,
                release_backoff_cap: Duration::from_secs(2),
            },
        );

        let worker = Worker::new(
            runtime_config(&format!("child-{run_id}"), Duration::from_secs(1)),
            registry(),
        )
        .expect("child worker should build");
        let pool = build_pool(&db_url);
        worker.run(&pool).await;
    });
    eprintln!("child: worker returned without reaching the crash window");
    std::process::exit(10);
}
