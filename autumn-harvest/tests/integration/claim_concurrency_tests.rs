#![cfg(feature = "db")]
//! Claim concurrency: one worker runs more than one claim at once.
//!
//! Assay #14 found one claim in flight per worker. The claim loop was busy
//! 94% of the time, so it capped throughput. These tests pin the fix and its
//! safety rules. See `DESIGN-claim-concurrency.md`.
//!
//! A test trigger makes each claim of one queue sleep, so claims of one
//! worker overlap only if the worker runs them at once. The trigger records
//! each claim interval in a probe table. Each test uses its own queue, probe
//! table and trigger, and drops them at the end.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise a testcontainers Postgres 16 starts.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::models::NewWorkflowExecution;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::schema::harvest_workflow_executions;
use autumn_harvest::telemetry::{DbOp, MetricsRecorder, NoOpMetrics, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};
use autumn_harvest::{RetryPolicy, WorkflowContext, store};

use chrono::Utc;
use diesel::sql_types::{BigInt, Text};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DB setup.
// ---------------------------------------------------------------------------

async fn setup_db() -> (String, Option<ContainerAsync<Postgres>>) {
    if let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") {
        return (url, None);
    }
    let container = Postgres::default()
        .with_init_sql(autumn_harvest::test_init_sql().as_bytes().to_vec())
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
        .max_size(16)
        .build()
        .expect("pool build failed")
}

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

async fn exec(conn: &mut AsyncPgConnection, sql: &str) {
    diesel::sql_query(sql)
        .execute(conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

// ---------------------------------------------------------------------------
// The slow-claim probe.
// ---------------------------------------------------------------------------

/// A trigger that makes each claim on one queue sleep, and logs its interval.
struct ClaimProbe {
    url: String,
    suffix: String,
}

impl ClaimProbe {
    /// Install the probe on `queue`. Each claim sleeps `sleep_ms`.
    async fn install(url: &str, queue: &str, sleep_ms: u32) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let mut conn = connect(url).await;
        exec(
            &mut conn,
            &format!(
                "CREATE TABLE claim_probe_{suffix} (
                     started timestamptz NOT NULL,
                     finished timestamptz NOT NULL)"
            ),
        )
        .await;
        exec(
            &mut conn,
            &format!(
                "CREATE FUNCTION claim_probe_fn_{suffix}() RETURNS trigger AS $$
                 DECLARE t0 timestamptz := clock_timestamp();
                 BEGIN
                     PERFORM pg_sleep({sleep_ms} / 1000.0);
                     INSERT INTO claim_probe_{suffix} VALUES (t0, clock_timestamp());
                     RETURN NEW;
                 END $$ LANGUAGE plpgsql"
            ),
        )
        .await;
        exec(
            &mut conn,
            &format!(
                "CREATE TRIGGER claim_probe_trg_{suffix}
                 BEFORE UPDATE ON harvest_task_queue
                 FOR EACH ROW
                 WHEN (OLD.state = 'PENDING' AND NEW.state = 'RUNNING'
                       AND NEW.queue_name = '{queue}')
                 EXECUTE FUNCTION claim_probe_fn_{suffix}()"
            ),
        )
        .await;
        Self {
            url: url.to_owned(),
            suffix,
        }
    }

    /// How many claims the probe saw.
    async fn claims(&self) -> i64 {
        self.scalar(&format!(
            "SELECT count(*) AS v FROM claim_probe_{}",
            self.suffix
        ))
        .await
    }

    /// The most claims that ran at one time.
    async fn max_overlap(&self) -> i64 {
        let s = &self.suffix;
        self.scalar(&format!(
            "SELECT coalesce(max(n), 0) AS v FROM (
                 SELECT count(*) AS n
                   FROM claim_probe_{s} p
                   JOIN claim_probe_{s} q
                     ON q.started < p.finished AND q.finished > p.started
                  GROUP BY p.started, p.finished) o"
        ))
        .await
    }

    async fn scalar(&self, sql: &str) -> i64 {
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = BigInt)]
            v: i64,
        }
        let mut conn = connect(&self.url).await;
        diesel::sql_query(sql)
            .get_result::<Row>(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .v
    }

    /// Drop the trigger, its function and the probe table.
    async fn remove(self) {
        let s = &self.suffix;
        let mut conn = connect(&self.url).await;
        exec(
            &mut conn,
            &format!("DROP TRIGGER claim_probe_trg_{s} ON harvest_task_queue"),
        )
        .await;
        exec(&mut conn, &format!("DROP FUNCTION claim_probe_fn_{s}()")).await;
        exec(&mut conn, &format!("DROP TABLE claim_probe_{s}")).await;
    }
}

// ---------------------------------------------------------------------------
// Handlers.
// ---------------------------------------------------------------------------

type BoxFut<'a> =
    Pin<Box<dyn std::future::Future<Output = Result<serde_json::Value, String>> + Send + 'a>>;

/// Activity runs, by execution id.
static ACTIVITY_RUNS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Workflow: run `cc_activity` when the input asks for it, else return.
fn cc_workflow(ctx: &WorkflowContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        if input.get("activity").and_then(serde_json::Value::as_bool) != Some(true) {
            return Ok(serde_json::json!("done"));
        }
        let queue = ctx.queue_name().to_string();
        let id = serde_json::json!(ctx.execution_id().as_uuid().to_string());
        ctx.execute_activity_raw("cc_activity", id, &queue)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Activity: count the run, then hold its permit for 100 ms.
fn cc_activity(_ctx: &autumn_harvest::ActivityContext, input: serde_json::Value) -> BoxFut<'_> {
    Box::pin(async move {
        let key = input.as_str().expect("activity input is an id").to_owned();
        *ACTIVITY_RUNS
            .lock()
            .expect("runs map")
            .entry(key)
            .or_insert(0) += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(input)
    })
}

fn workflow_info() -> WorkflowInfo {
    WorkflowInfo {
        quota: None,
        declared_activities: None,
        declared_children: None,
        mcp: false,
        name: "cc_workflow",
        module: "claim_concurrency_tests",
        handler: cc_workflow,
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

fn activity_info() -> ActivityInfo {
    ActivityInfo {
        name: "cc_activity",
        module: "claim_concurrency_tests",
        default_retry_policy: Some(RetryPolicy::fixed(1, Duration::from_millis(10))),
        default_start_to_close: Some(Duration::from_secs(60)),
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
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        requires: None,
        handler: cc_activity,
    }
}

fn build_registry(metrics: Arc<dyn MetricsRecorder>) -> Arc<HandlerRegistry> {
    let telemetry = Arc::new(TelemetryConfig::builder().metrics(metrics).build());
    Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![workflow_info()],
        vec![activity_info()],
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ))
}

/// The knobs a test varies.
struct Knobs {
    claims: usize,
    activities: usize,
    poll_interval: Duration,
}

fn build_worker(
    worker_id: &str,
    queue: &str,
    knobs: &Knobs,
    metrics: Arc<dyn MetricsRecorder>,
) -> Arc<Worker> {
    let mut config: WorkerRuntimeConfig = autumn_harvest::builder::WorkerConfig::default().into();
    config.worker_id = worker_id.to_owned();
    config.queues = vec![queue.to_owned()];
    config.max_concurrent_workflows = 16;
    config.max_concurrent_activities = knobs.activities;
    config.max_concurrent_claims = knobs.claims;
    config.poll_interval = knobs.poll_interval;
    config.shutdown_timeout = Duration::from_secs(5);
    config.shard_assignments = vec![ShardId::new(0)];
    Arc::new(Worker::new(config, build_registry(metrics)).expect("worker should build"))
}

// ---------------------------------------------------------------------------
// Seeding and read helpers.
// ---------------------------------------------------------------------------

async fn seed_workflow(
    conn: &mut AsyncPgConnection,
    queue: &str,
    input: serde_json::Value,
) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "cc_workflow",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: input.clone().into(),
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
        tenant: None,
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

async fn seed(url: &str, queue: &str, n: usize, with_activity: bool) -> Vec<ExecutionId> {
    let mut conn = connect(url).await;
    let input = serde_json::json!({ "activity": with_activity });
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        ids.push(seed_workflow(&mut conn, queue, input.clone()).await);
    }
    ids
}

/// One `count(*)` with one text bind.
async fn count(url: &str, sql: &str, bind: &str) -> i64 {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        v: i64,
    }
    let mut conn = connect(url).await;
    diesel::sql_query(sql)
        .bind::<Text, _>(bind)
        .get_result::<Row>(&mut conn)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .v
}

/// How many workflows on `queue` completed.
async fn completed(url: &str, queue: &str) -> i64 {
    count(
        url,
        "SELECT count(*) AS v FROM harvest_workflow_executions
          WHERE queue_name = $1 AND state = 'COMPLETED'",
        queue,
    )
    .await
}

/// Wait until `n` workflows on `queue` complete, or `limit` elapses.
async fn wait_completed(url: &str, queue: &str, n: i64, limit: Duration) -> bool {
    tokio::time::timeout(limit, async {
        while completed(url, queue).await < n {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

/// Run `worker` on `pool` in a task.
fn spawn_worker(worker: &Arc<Worker>, pool: &DbPool) -> tokio::task::JoinHandle<()> {
    let runner = Arc::clone(worker);
    let pool = pool.clone();
    tokio::spawn(async move { runner.run(&pool).await })
}

/// Counts the claim ops a worker records.
#[derive(Default)]
struct ClaimCounter(AtomicUsize);

impl MetricsRecorder for ClaimCounter {
    fn record_db_query_duration(&self, op: DbOp, _shard: u16, _seconds: f64) {
        if op == DbOp::Claim {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// AC1 and AC2: claims overlap, up to the cap.
// ---------------------------------------------------------------------------

/// Drain `n` no-activity workflows through a slow claim. Return the largest
/// claim overlap.
async fn drain_overlap(claims: usize, n: usize) -> i64 {
    let (url, _container) = setup_db().await;
    let queue = format!("cc-overlap-{claims}-{}", Uuid::new_v4());
    let probe = ClaimProbe::install(&url, &queue, 150).await;
    seed(&url, &queue, n, false).await;
    let pool = build_pool(&url);
    let knobs = Knobs {
        claims,
        activities: 4,
        poll_interval: Duration::from_millis(25),
    };
    let worker = build_worker(&queue, &queue, &knobs, Arc::new(NoOpMetrics));
    let handle = spawn_worker(&worker, &pool);

    let drained = wait_completed(&url, &queue, n as i64, Duration::from_secs(60)).await;
    worker.shutdown();
    handle.await.expect("worker joins");
    let overlap = probe.max_overlap().await;
    let seen = probe.claims().await;
    probe.remove().await;
    assert!(drained, "every workflow completes");
    assert!(
        seen >= n as i64,
        "the probe sees every claim: {seen} of {n}"
    );
    overlap
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_worker_overlaps_claims_up_to_its_cap() {
    let overlap = drain_overlap(4, 16).await;
    assert!(
        overlap >= 3,
        "a worker with 4 claim loops runs claims at once; largest overlap {overlap}"
    );
    assert!(
        overlap <= 4,
        "no more than 4 claims run at once; largest overlap {overlap}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_concurrent_claims_of_one_keeps_claims_serial() {
    let overlap = drain_overlap(1, 6).await;
    assert_eq!(overlap, 1, "one claim loop runs one claim at a time");
}

// ---------------------------------------------------------------------------
// AC3: safety.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_never_exceed_the_local_permits() {
    let (url, _container) = setup_db().await;
    let queue = format!("cc-permits-{}", Uuid::new_v4());
    let probe = ClaimProbe::install(&url, &queue, 50).await;
    let n = 8;
    seed(&url, &queue, n, true).await;
    let pool = build_pool(&url);
    let knobs = Knobs {
        claims: 4,
        activities: 1,
        poll_interval: Duration::from_millis(25),
    };
    let worker = build_worker(&queue, &queue, &knobs, Arc::new(NoOpMetrics));
    let handle = spawn_worker(&worker, &pool);

    // Sample the activity rows this worker holds until the backlog drains.
    let mut most = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while completed(&url, &queue).await < n as i64 && tokio::time::Instant::now() < deadline {
        let held = count(
            &url,
            "SELECT count(*) AS v FROM harvest_task_queue
              WHERE worker_id = $1 AND state = 'RUNNING' AND task_type = 'activity'",
            &queue,
        )
        .await;
        most = most.max(held);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    worker.shutdown();
    handle.await.expect("worker joins");
    let overlap = probe.max_overlap().await;
    probe.remove().await;

    assert_eq!(
        completed(&url, &queue).await,
        n as i64,
        "every workflow completes"
    );
    assert!(
        overlap >= 2,
        "claims overlap, so the gate is tested; largest overlap {overlap}"
    );
    assert_eq!(
        most, 1,
        "a worker with 1 activity permit holds at most 1 activity row"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_task_runs_once_under_concurrent_claims() {
    let (url, _container) = setup_db().await;
    let queue = format!("cc-once-{}", Uuid::new_v4());
    let n = 24;
    let ids = seed(&url, &queue, n, true).await;
    let pool = build_pool(&url);
    let knobs = Knobs {
        claims: 4,
        activities: 8,
        poll_interval: Duration::from_millis(25),
    };
    // Two workers, so claims race inside one worker and across workers.
    let a = build_worker(&format!("{queue}-a"), &queue, &knobs, Arc::new(NoOpMetrics));
    let b = build_worker(&format!("{queue}-b"), &queue, &knobs, Arc::new(NoOpMetrics));
    let ha = spawn_worker(&a, &pool);
    let hb = spawn_worker(&b, &pool);

    let drained = wait_completed(&url, &queue, n as i64, Duration::from_secs(60)).await;
    a.shutdown();
    b.shutdown();
    ha.await.expect("worker a joins");
    hb.await.expect("worker b joins");
    assert!(drained, "every workflow completes");

    let runs = ACTIVITY_RUNS.lock().expect("runs map").clone();
    for id in &ids {
        let key = id.as_uuid().to_string();
        assert_eq!(
            runs.get(&key),
            Some(&1),
            "activity of {key} runs exactly once"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_worker_claims_at_the_single_loop_rate() {
    let (url, _container) = setup_db().await;
    let queue = format!("cc-idle-{}", Uuid::new_v4());
    let pool = build_pool(&url);
    let poll = Duration::from_millis(50);
    let mut counts = Vec::new();
    for claims in [1, 4] {
        let counter = Arc::new(ClaimCounter::default());
        let knobs = Knobs {
            claims,
            activities: 4,
            poll_interval: poll,
        };
        let worker = build_worker(
            &format!("{queue}-{claims}"),
            &queue,
            &knobs,
            Arc::clone(&counter) as Arc<dyn MetricsRecorder>,
        );
        let handle = spawn_worker(&worker, &pool);
        tokio::time::sleep(Duration::from_secs(2)).await;
        worker.shutdown();
        handle.await.expect("worker joins");
        counts.push(AtomicUsize::load(&counter.0, Ordering::Relaxed));
    }
    let (one, four) = (counts[0], counts[1]);
    assert!(one > 0, "the idle worker polls");
    assert!(
        four * 2 <= one * 3,
        "extra claim loops add no idle polls: {one} claims with 1 loop, {four} with 4"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_task_stays_running_after_a_concurrent_worker_stops() {
    let (url, _container) = setup_db().await;
    let queue = format!("cc-stop-{}", Uuid::new_v4());
    let probe = ClaimProbe::install(&url, &queue, 200).await;
    seed(&url, &queue, 40, true).await;
    let pool = build_pool(&url);
    let knobs = Knobs {
        claims: 4,
        activities: 8,
        poll_interval: Duration::from_millis(25),
    };
    let worker = build_worker(&queue, &queue, &knobs, Arc::new(NoOpMetrics));
    let handle = spawn_worker(&worker, &pool);

    // Stop while claims are in flight.
    tokio::time::sleep(Duration::from_millis(900)).await;
    worker.shutdown();
    handle.await.expect("worker joins");
    let overlap = probe.max_overlap().await;
    probe.remove().await;

    assert!(
        overlap >= 2,
        "claims overlap before the stop; largest overlap {overlap}"
    );
    let running = count(
        &url,
        "SELECT count(*) AS v FROM harvest_task_queue
          WHERE worker_id = $1 AND state = 'RUNNING'",
        &queue,
    )
    .await;
    assert_eq!(
        running, 0,
        "the drain sees every claim, so no row stays RUNNING"
    );
}
