#![cfg(feature = "db")]
//! Engine connection timeouts against a real Postgres (issue #1788).
//!
//! * AC1: with every pool connection held, a claim and a heartbeat flush fail
//!   within the pool bound. Before the fix, a pool with no deadpool timeouts
//!   made both wait without limit. `worker::tests` covers that pool shape.
//! * AC2: `statement_timeout` cancels a slow statement in the persist path.
//! * AC3: an engine connection reports the configured session timeouts.
//!
//! Set `HARVEST_TEST_DATABASE_URL` to a migrated Postgres to run against it.
//! Otherwise the suite starts a testcontainers Postgres.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::pool::{
    DbRole, EngineDbTimeouts, PoolTimeouts, SessionTimeouts, acquire_bound, engine_pool,
};
use autumn_harvest::store;
use autumn_harvest::telemetry::{MetricsRecorder, TelemetryConfig};
use autumn_harvest::types::{ExecutionId, ShardId};
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker, WorkerRuntimeConfig};

use chrono::Utc;
use diesel::sql_types::Text;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

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

async fn connect(url: &str) -> AsyncPgConnection {
    <AsyncPgConnection as AsyncConnection>::establish(url)
        .await
        .expect("failed to connect to Postgres")
}

const fn timeouts(pool_ms: u64, session: SessionTimeouts) -> EngineDbTimeouts {
    let bound = Duration::from_millis(pool_ms);
    EngineDbTimeouts {
        pool: PoolTimeouts {
            wait: bound,
            create: Duration::from_secs(10),
            recycle: Duration::from_secs(10),
        },
        hot: session,
        scanner: session,
        maintenance: session,
    }
}

#[derive(diesel::QueryableByName)]
struct Setting {
    #[diesel(sql_type = Text)]
    value: String,
}

/// `current_setting(name)` returns the same text as `SHOW name`.
async fn show(conn: &mut AsyncPgConnection, name: &str) -> String {
    diesel::sql_query("SELECT current_setting($1) AS value")
        .bind::<Text, _>(name)
        .get_result::<Setting>(conn)
        .await
        .expect("read setting")
        .value
}

// ---------------------------------------------------------------------------
// AC3: SHOW returns the configured values.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_engine_connection_shows_the_configured_session_timeouts() {
    let (url, _container) = setup_db().await;
    let session = SessionTimeouts {
        statement: Duration::from_millis(1_500),
        lock: Duration::from_millis(2_250),
        idle_in_transaction: Duration::from_millis(61_500),
        transaction: Duration::ZERO,
    };
    for role in [DbRole::Hot, DbRole::Scanner, DbRole::Maintenance] {
        let pool = engine_pool(url.clone(), 1, role, &timeouts(5_000, session))
            .expect("engine pool builds");
        let mut conn = pool.get().await.expect("engine connection");
        assert_eq!(
            show(&mut conn, "statement_timeout").await,
            "1500ms",
            "{role:?}"
        );
        assert_eq!(show(&mut conn, "lock_timeout").await, "2250ms", "{role:?}");
        assert_eq!(
            show(&mut conn, "idle_in_transaction_session_timeout").await,
            "61500ms",
            "{role:?}"
        );
    }
}

/// `pg_settings.setting` reports a time setting in milliseconds.
async fn setting_ms(conn: &mut AsyncPgConnection, name: &str) -> String {
    diesel::sql_query("SELECT setting AS value FROM pg_settings WHERE name = $1")
        .bind::<Text, _>(name)
        .get_result::<Setting>(conn)
        .await
        .expect("read setting")
        .value
}

#[tokio::test]
async fn each_role_gets_its_own_default_session_timeouts() {
    let (url, _container) = setup_db().await;
    let defaults = EngineDbTimeouts::default();
    for role in [DbRole::Hot, DbRole::Scanner, DbRole::Maintenance] {
        let pool = engine_pool(url.clone(), 1, role, &defaults).expect("engine pool builds");
        let mut conn = pool.get().await.expect("engine connection");
        let want = defaults.session(role);
        assert!(
            !want.statement.is_zero(),
            "{role:?} needs a statement_timeout"
        );
        assert!(!want.lock.is_zero(), "{role:?} needs a lock_timeout");
        assert!(
            !want.idle_in_transaction.is_zero(),
            "{role:?} needs an idle limit"
        );
        assert_eq!(
            setting_ms(&mut conn, "statement_timeout").await,
            want.statement.as_millis().to_string(),
            "{role:?}"
        );
        assert_eq!(
            setting_ms(&mut conn, "lock_timeout").await,
            want.lock.as_millis().to_string(),
            "{role:?}"
        );
        assert_eq!(
            setting_ms(&mut conn, "idle_in_transaction_session_timeout").await,
            want.idle_in_transaction.as_millis().to_string(),
            "{role:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// AC2: statement_timeout cancels a slow persist.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn statement_timeout_cancels_a_slow_persist() {
    let (url, _container) = setup_db().await;
    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let suffix = exec_id.as_uuid().simple().to_string();
    let function = format!("harvest_test_1788_sleep_{suffix}");
    let trigger = format!("harvest_test_1788_sleep_{suffix}");

    let session = SessionTimeouts {
        statement: Duration::from_millis(500),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url.clone(), 1, DbRole::Hot, &timeouts(5_000, session))
        .expect("engine pool builds");
    let mut conn = pool.get().await.expect("engine connection");

    // A BEFORE INSERT trigger sleeps for this execution only. Nothing between
    // its creation and its drop can panic, so it cannot leak.
    let mut admin = connect(&url).await;
    diesel::sql_query(format!(
        "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN \
           IF NEW.workflow_exec_id = '{}'::uuid THEN PERFORM pg_sleep(5); END IF; \
           RETURN NEW; \
         END $$",
        exec_id.as_uuid()
    ))
    .execute(&mut admin)
    .await
    .expect("create sleep function");
    diesel::sql_query(format!(
        "CREATE TRIGGER {trigger} BEFORE INSERT ON harvest_events \
         FOR EACH ROW EXECUTE FUNCTION {function}()"
    ))
    .execute(&mut admin)
    .await
    .expect("create sleep trigger");

    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        store::append_events(
            &mut conn,
            exec_id,
            &[WorkflowEvent::WorkflowStarted {
                input: serde_json::json!({}),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            }],
            0,
        ),
    )
    .await;
    let elapsed = started.elapsed();

    diesel::sql_query(format!("DROP TRIGGER {trigger} ON harvest_events"))
        .execute(&mut admin)
        .await
        .expect("drop sleep trigger");
    diesel::sql_query(format!("DROP FUNCTION {function}()"))
        .execute(&mut admin)
        .await
        .expect("drop sleep function");

    let err = outcome
        .expect("the persist must return")
        .expect_err("statement_timeout must cancel the sleep");
    assert!(
        err.to_string().contains("statement timeout"),
        "expected a statement timeout, got: {err}"
    );
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
}

// ---------------------------------------------------------------------------
// AC1: a full pool fails a claim and a heartbeat flush within the bound.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct AcquireTimeouts(Mutex<Vec<String>>);

impl MetricsRecorder for AcquireTimeouts {
    fn record_db_pool_acquire_timeout(&self, site: &str) {
        self.0.lock().expect("lock").push(site.to_owned());
    }
}

fn build_worker(worker_id: &str, metrics: Arc<AcquireTimeouts>) -> Arc<Worker> {
    build_worker_with(worker_id, metrics, vec![], Duration::from_secs(5))
}

fn build_worker_with(
    worker_id: &str,
    metrics: Arc<AcquireTimeouts>,
    activities: Vec<autumn_harvest::info::ActivityInfo>,
    worker_heartbeat_interval: Duration,
) -> Arc<Worker> {
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![],
        activities,
        autumn_harvest::context::empty_shared_state(),
        telemetry,
    ));
    Arc::new(
        Worker::new(
            WorkerRuntimeConfig {
                codec_rotation_batch_size: 0,
                dr: autumn_harvest::replication::DrConfig::default(),
                worker_id: worker_id.to_string(),
                queues: vec![format!("q-{worker_id}")],
                notification_database_url: None,
                max_concurrent_workflows: 2,
                max_concurrent_activities: 2,
                poll_interval: Duration::from_millis(50),
                shutdown_timeout: Duration::from_secs(1),
                cancellation_grace_period: Duration::from_secs(1),
                sticky_timeout: Duration::from_secs(5),
                max_local_activity_start_to_close: Duration::from_secs(60),
                shard_assignments: vec![ShardId::new(0)],
                worker_heartbeat_interval,
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
                resident_workflows: true,
                priority_aging_secs: None,
                unknown_target_grace_window: Duration::from_secs(5),
                poison_pill_threshold: 3,
                capability_miss_max_redeliveries: 5,
                workflow_task_timeout: Duration::from_secs(10),
                workflow_panic_max_attempts: 3,
                labels: HashMap::new(),
                queue_weights: HashMap::new(),
                max_workflow_pause_duration: Duration::from_secs(24 * 3600),
                max_workflow_history_events: None,
                shard_notification_database_urls: Vec::new(),
                sharded_pool: None,
                slot_tuner: None,
                max_concurrent_sessions: 0,
            },
            registry,
        )
        .expect("worker should build"),
    )
}

/// Take every connection in `pool`. Other users of the pool time out and
/// release, so the loop gets each slot in turn.
async fn hold_every_connection(pool: &DbPool) -> Vec<autumn_harvest::pool::PooledConn> {
    let max = pool.status().max_size;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut held = Vec::with_capacity(max);
    while held.len() < max {
        assert!(Instant::now() < deadline, "could not take every pool slot");
        if let Ok(conn) = pool.get().await {
            held.push(conn);
        }
    }
    held
}

async fn worker_is_registered(url: &str, worker_id: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT count(*) AS n FROM harvest_workers WHERE worker_id = $1")
        .bind::<Text, _>(worker_id)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count workers")
        .n
        == 1
}

#[tokio::test]
async fn a_full_pool_fails_a_heartbeat_flush_within_the_bound() {
    let (url, _container) = setup_db().await;
    let pool = engine_pool(
        url,
        2,
        DbRole::Hot,
        &timeouts(300, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool builds");
    let _held = hold_every_connection(&pool).await;

    let bound = acquire_bound(&pool);
    assert_eq!(bound, Duration::from_millis(300));
    let started = Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        autumn_harvest::heartbeat::flush_heartbeat(
            &pool,
            &autumn_harvest::queue::TaskClaim::new(Uuid::new_v4(), "w-1", 1),
            serde_json::json!({"progress": 1}),
            bound,
        ),
    )
    .await
    .expect("a heartbeat flush must not hang on a full pool");
    let err = outcome.expect_err("a full pool cannot take a heartbeat");
    assert!(err.is_pool_acquire_timeout(), "{err}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_full_pool_fails_a_claim_within_the_bound() {
    let (url, _container) = setup_db().await;
    let pool = engine_pool(
        url.clone(),
        2,
        DbRole::Hot,
        &timeouts(300, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool builds");

    let worker_id = format!("pg-timeouts-{}", Uuid::new_v4());
    let metrics = Arc::new(AcquireTimeouts::default());
    let worker = build_worker(&worker_id, Arc::clone(&metrics));
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&pool_for_run).await });

    // Hold the pool only after the worker registers. An unregistered worker
    // does not claim.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !worker_is_registered(&url, &worker_id).await {
        assert!(Instant::now() < deadline, "worker never registered");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let held = hold_every_connection(&pool).await;
    // A timeout from the race to fill the pool does not count.
    metrics.0.lock().expect("lock").clear();
    let saturated_at = Instant::now();

    // A claim waits at most 300 ms for a slot, then the loop sleeps one poll
    // interval. Three seconds is a wide margin.
    let deadline = saturated_at + Duration::from_secs(3);
    loop {
        if metrics.0.lock().expect("lock").iter().any(|s| s == "claim") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no claim acquire timeout within 3 s of a full pool"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    drop(held);
    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(15), run_handle)
        .await
        .expect("worker stops")
        .expect("worker task joins");
}

/// A static rate-limit bucket whose startup registration times out is
/// registered later. Without it the activity can never run, because the
/// claim gate fails closed on a missing bucket.
#[tokio::test]
async fn a_rate_limit_bucket_registers_after_a_startup_timeout() {
    let (url, _container) = setup_db().await;
    let pool = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(200, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool builds");
    let key: &'static str = Box::leak(format!("rl-{}", Uuid::new_v4()).into_boxed_str());
    let activity = autumn_harvest::info::ActivityInfo {
        name: "rate_limited_work",
        module: "test",
        default_retry_policy: None,
        default_start_to_close: Some(Duration::from_secs(5)),
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: Some(5.0),
        rate_limit_burst: None,
        rate_limit_key: Some(key),
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };
    let worker_id = format!("pg-timeouts-{}", Uuid::new_v4());
    let worker = build_worker_with(
        &worker_id,
        Arc::new(AcquireTimeouts::default()),
        vec![activity],
        Duration::from_millis(200),
    );

    // Startup acquires wait 200 ms each, so two seconds covers them all.
    let held = hold_every_connection(&pool).await;
    let runner = Arc::clone(&worker);
    let pool_for_run = pool.clone();
    let run_handle = tokio::spawn(async move { runner.run(&pool_for_run).await });
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(
        !rate_limit_bucket_exists(&url, key).await,
        "the bucket must not register while the pool is full"
    );
    drop(held);

    let deadline = Instant::now() + Duration::from_secs(10);
    while !rate_limit_bucket_exists(&url, key).await {
        assert!(
            Instant::now() < deadline,
            "the bucket never registered after the pool freed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    worker.shutdown();
    tokio::time::timeout(Duration::from_secs(15), run_handle)
        .await
        .expect("worker stops")
        .expect("worker task joins");
}

async fn rate_limit_bucket_exists(url: &str, key: &str) -> bool {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = connect(url).await;
    diesel::sql_query("SELECT count(*) AS n FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(key)
        .get_result::<Count>(&mut conn)
        .await
        .expect("count buckets")
        .n
        == 1
}

// ---------------------------------------------------------------------------
// Review follow-ups: finalization retries, claim-fenced heartbeats.
// ---------------------------------------------------------------------------

/// A finalization write waits through several bounded attempts. A slot that
/// frees during them is taken, so an executed result is not dropped.
#[tokio::test]
async fn a_retrying_acquire_gets_a_slot_that_frees_later() {
    let (url, _container) = setup_db().await;
    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(200, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool builds");
    let held = hold_every_connection(&pool).await;
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        drop(held);
    });

    let conn = tokio::time::timeout(
        Duration::from_secs(10),
        autumn_harvest::pool::acquire_with_retries(&pool, 10),
    )
    .await
    .expect("the retries end")
    .expect("the freed slot is taken");
    drop(conn);
    releaser.await.expect("releaser joins");
}

#[derive(diesel::QueryableByName)]
struct Details {
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    heartbeat_details: serde_json::Value,
}

/// A heartbeat from an old claim must not touch the row after a new claim.
#[tokio::test]
async fn a_heartbeat_from_an_old_claim_is_rejected() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-hb-{}", Uuid::new_v4());
    let params = autumn_harvest::queue::EnqueueParams::new(
        &queue_name,
        autumn_harvest::queue::TaskType::Activity,
        serde_json::json!({}),
    );
    let task_id = autumn_harvest::queue::enqueue(&mut conn, &params)
        .await
        .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'new-worker', \
         attempt = 2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("model a newer claim");

    let stale = autumn_harvest::queue::record_heartbeat(
        &mut conn,
        &autumn_harvest::queue::TaskClaim::new(task_id, "old-worker", 1),
        serde_json::json!({"from": "old"}),
    )
    .await
    .expect("the write runs");
    assert_eq!(stale, autumn_harvest::queue::ClaimWrite::LeaseLost);

    let current = autumn_harvest::queue::record_heartbeat(
        &mut conn,
        &autumn_harvest::queue::TaskClaim::new(task_id, "new-worker", 2),
        serde_json::json!({"from": "new"}),
    )
    .await
    .expect("the write runs");
    assert_eq!(current, autumn_harvest::queue::ClaimWrite::Applied);

    let row = diesel::sql_query("SELECT heartbeat_details FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Details>(&mut conn)
        .await
        .expect("read details");
    assert_eq!(row.heartbeat_details, serde_json::json!({"from": "new"}));
}

/// The worker retries an activity result write after `lock_timeout`. This
/// checks what that retry relies on: the error is a session timeout, and the
/// same connection runs the write again once the lock frees.
#[tokio::test]
async fn a_lock_timeout_leaves_the_connection_ready_for_a_retry() {
    let (url, _container) = setup_db().await;
    let mut setup = connect(&url).await;
    let queue_name = format!("q-lock-{}", Uuid::new_v4());
    let task_id = autumn_harvest::queue::enqueue(
        &mut setup,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");

    // Another session holds the row lock for 600 ms.
    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(format!(
            "DO $$ BEGIN PERFORM 1 FROM harvest_task_queue WHERE id = '{task_id}' FOR UPDATE; \
             PERFORM pg_sleep(0.6); END $$"
        ))
        .execute(&mut conn)
        .await
        .expect("hold the row lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(150),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let mut conn = pool.get().await.expect("engine connection");
    let write = "UPDATE harvest_task_queue SET priority = priority WHERE id = $1";

    let first = diesel::sql_query(write)
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut *conn)
        .await
        .map_err(autumn_harvest::error::database_error)
        .expect_err("the held lock outlasts lock_timeout");
    assert!(autumn_harvest::pool::is_session_timeout(&first), "{first}");

    holder.await.expect("holder joins");
    diesel::sql_query(write)
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut *conn)
        .await
        .expect("the same connection runs the write again");
}

/// The claim release that runs after a pool acquire timeout works on an
/// activity row. It also refuses a newer claim of the same row.
#[tokio::test]
async fn a_stranded_activity_claim_is_released() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-release-{}", Uuid::new_v4());
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");
    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");

    // A stale claim (attempt 0) must not release the current one.
    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "w-1", 0, 0).await;
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");

    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "w-1", 0, 1).await;
    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// The claim release must ride out the same `lock_timeout` that stopped the
/// result write. Another session holds the row lock for 700 ms, and the pool
/// gives up on a lock after 150 ms.
#[tokio::test]
async fn a_claim_release_retries_after_a_lock_timeout() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-rl-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(format!(
            "DO $$ BEGIN PERFORM 1 FROM harvest_task_queue WHERE id = '{task_id}' FOR UPDATE; \
             PERFORM pg_sleep(0.7); END $$"
        ))
        .execute(&mut conn)
        .await
        .expect("hold the row lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(150),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "w-1", 0, 1).await;
    holder.await.expect("holder joins");

    assert_eq!(task_state(&mut conn, task_id).await, "PENDING");
}

/// A quarantine whose transaction a `lock_timeout` rolls back reports failure,
/// so the caller keeps the strike count.
#[tokio::test]
async fn a_rolled_back_quarantine_reports_failure() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-qr-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Workflow,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(format!(
            "DO $$ BEGIN PERFORM 1 FROM harvest_task_queue WHERE id = '{task_id}' FOR UPDATE; \
             PERFORM pg_sleep(1.5); END $$"
        ))
        .execute(&mut conn)
        .await
        .expect("hold the row lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(100),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let reached = autumn_harvest::worker::quarantine_workflow_task_timeout(
        &pool,
        task_id,
        None,
        "w-1",
        1,
        3,
        10,
        "wf",
        &queue_name,
        &autumn_harvest::telemetry::NoOpMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    holder.await.expect("holder joins");

    assert!(!reached, "a rolled-back quarantine must not report success");
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

/// A claim release that fails with a non-timeout error, such as a dropped
/// connection, also retries. A trigger fails the first update for this row
/// once. A sequence counts the tries, because it does not roll back.
#[tokio::test]
async fn a_claim_release_retries_after_a_non_timeout_error() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let queue_name = format!("q-ne-{suffix}");
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let seq = format!("harvest_test_1788_seq_{suffix}");
    let function = format!("harvest_test_1788_fail_once_{suffix}");
    let trigger = format!("harvest_test_1788_fail_once_{suffix}");
    for sql in [
        format!("CREATE SEQUENCE {seq}"),
        format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF NEW.id = '{task_id}'::uuid AND nextval('{seq}') = 1 THEN \
                 RAISE EXCEPTION 'transient failure for the test'; \
               END IF; \
               RETURN NEW; \
             END $$"
        ),
        format!(
            "CREATE TRIGGER {trigger} BEFORE UPDATE ON harvest_task_queue \
             FOR EACH ROW EXECUTE FUNCTION {function}()"
        ),
    ] {
        diesel::sql_query(sql)
            .execute(&mut conn)
            .await
            .expect("set up the trigger");
    }

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    autumn_harvest::worker::reset_timed_out_workflow_task(&pool, task_id, "w-1", 0, 1).await;
    let state = task_state(&mut conn, task_id).await;

    for sql in [
        format!("DROP TRIGGER {trigger} ON harvest_task_queue"),
        format!("DROP FUNCTION {function}()"),
        format!("DROP SEQUENCE {seq}"),
    ] {
        diesel::sql_query(sql)
            .execute(&mut conn)
            .await
            .expect("drop the trigger");
    }
    assert_eq!(state, "PENDING");
}

/// A timeout that ends the session is a transient error, and it needs a new
/// connection. PostgreSQL 16 has no `transaction_timeout`, so this test uses
/// `idle_in_transaction_session_timeout`, which ends the session the same way.
#[tokio::test]
async fn an_idle_transaction_timeout_is_a_lost_connection() {
    let (url, _container) = setup_db().await;
    let session = SessionTimeouts {
        idle_in_transaction: Duration::from_millis(100),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let mut conn = pool.get().await.expect("engine connection");

    let outcome = conn
        .transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            diesel::sql_query("SELECT 1")
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            tokio::time::sleep(Duration::from_millis(400)).await;
            diesel::sql_query("SELECT 1")
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            Ok(())
        })
        .await;
    let err = outcome.expect_err("the server ends the idle transaction");
    assert!(autumn_harvest::pool::is_connection_lost(&err), "{err:?}");
    assert!(autumn_harvest::pool::is_transient_db_error(&err), "{err:?}");
    assert!(!autumn_harvest::pool::is_session_timeout(&err), "{err:?}");
}

/// Insert a running workflow execution with a `WorkflowStarted` event.
/// The session row that a failed acquire on worker `w-1` tried to insert.
fn attempted_session_row(
    session_id: autumn_harvest::types::SessionId,
    exec_id: ExecutionId,
    queue_name: &str,
) -> autumn_harvest::worker::AttemptedSessionRow {
    autumn_harvest::worker::AttemptedSessionRow {
        session_id,
        exec_id,
        host_worker_id: "w-1".to_owned(),
        queue_name: queue_name.to_owned(),
    }
}

/// [`attempted_session_row`] for an execution that the test did not seed.
fn unseeded_session_row(
    session_id: autumn_harvest::types::SessionId,
) -> autumn_harvest::worker::AttemptedSessionRow {
    attempted_session_row(
        session_id,
        ExecutionId::new_for_shard(ShardId::new(0)),
        "q-none",
    )
}

async fn seed_execution(conn: &mut AsyncPgConnection, queue: &str) -> ExecutionId {
    use autumn_harvest::models::NewWorkflowExecution;
    use autumn_harvest::schema::harvest_workflow_executions;

    let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
    let row = NewWorkflowExecution {
        quota_key: None,
        id: exec_id.as_uuid(),
        workflow_name: "wf",
        workflow_id: &format!("wf-{}", exec_id.as_uuid()),
        run_id: Uuid::new_v4(),
        shard_id: 0,
        input: serde_json::json!({}).into(),
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
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
        0,
    )
    .await
    .expect("append WorkflowStarted");
    exec_id
}

/// A retry policy that does not parse fails the task after its handler ran.
/// That write must ride out a `lock_timeout`. Otherwise the claim is released,
/// and the handler runs again. Another session holds the execution row lock
/// for 700 ms, and the pool gives up on a lock after 150 ms.
#[tokio::test]
async fn an_invalid_retry_policy_failure_retries_after_a_lock_timeout() {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::{QueryDsl, SelectableHelper};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-ip-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         workflow_exec_id = $2, retry_policy = '{\"max_attempts\": \"many\"}'::jsonb \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");
    let task: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("load the task");

    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(format!(
            "DO $$ BEGIN PERFORM 1 FROM harvest_workflow_executions \
             WHERE id = '{}' FOR UPDATE; PERFORM pg_sleep(0.7); END $$",
            exec_id.as_uuid()
        ))
        .execute(&mut conn)
        .await
        .expect("hold the row lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(150),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let outcome = autumn_harvest::worker::retry_policy_or_fail_task(
        &pool,
        &task,
        "w-1",
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    holder.await.expect("holder joins");

    let err = outcome.expect_err("the policy does not parse");
    assert!(!autumn_harvest::pool::is_session_timeout(&err), "{err}");
    assert_eq!(task_state(&mut conn, task_id).await, "FAILED");
}

/// A write that loses its connection runs again on a new one. A trigger ends
/// its own backend on the first update of this task row. A sequence counts the
/// tries, because a sequence does not roll back.
#[tokio::test]
async fn an_invalid_retry_policy_failure_retries_after_a_lost_connection() {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use diesel::{QueryDsl, SelectableHelper};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let queue_name = format!("q-lc-{suffix}");
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         workflow_exec_id = $2, retry_policy = '{\"max_attempts\": \"many\"}'::jsonb \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");
    let task: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("load the task");

    let seq = format!("harvest_test_1788_lc_seq_{suffix}");
    let function = format!("harvest_test_1788_lc_{suffix}");
    let trigger = format!("harvest_test_1788_lc_{suffix}");
    for sql in [
        format!("CREATE SEQUENCE {seq}"),
        format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN \
               IF NEW.id = '{task_id}'::uuid AND nextval('{seq}') = 1 THEN \
                 PERFORM pg_terminate_backend(pg_backend_pid()); \
                 PERFORM pg_sleep(5); \
               END IF; \
               RETURN NEW; \
             END $$"
        ),
        format!(
            "CREATE TRIGGER {trigger} BEFORE UPDATE ON harvest_task_queue \
             FOR EACH ROW EXECUTE FUNCTION {function}()"
        ),
    ] {
        diesel::sql_query(sql)
            .execute(&mut conn)
            .await
            .expect("set up the trigger");
    }

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let outcome = autumn_harvest::worker::retry_policy_or_fail_task(
        &pool,
        &task,
        "w-1",
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    let state = task_state(&mut conn, task_id).await;

    for sql in [
        format!("DROP TRIGGER {trigger} ON harvest_task_queue"),
        format!("DROP FUNCTION {function}()"),
        format!("DROP SEQUENCE {seq}"),
    ] {
        diesel::sql_query(sql)
            .execute(&mut conn)
            .await
            .expect("drop the trigger");
    }
    let err = outcome.expect_err("the policy does not parse");
    assert!(!autumn_harvest::pool::is_connection_lost(&err), "{err:?}");
    assert_eq!(state, "FAILED");
}

/// A start that fails on a lost connection still refunds its rate-limit
/// token. The dead connection cannot run the refund, so the refund needs a new
/// connection.
#[tokio::test]
async fn a_start_error_on_a_lost_connection_still_refunds_the_token() {
    #[derive(diesel::QueryableByName)]
    struct Tokens {
        #[diesel(sql_type = diesel::sql_types::Double)]
        tokens: f64,
    }
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }

    let (url, _container) = setup_db().await;
    let mut admin = connect(&url).await;
    let key = format!("rl-lost-{}", Uuid::new_v4().simple());
    autumn_harvest::queue::ensure_rate_limit_bucket(&mut admin, &key, 0.0, 1.0)
        .await
        .expect("create the bucket");
    assert!(
        autumn_harvest::queue::try_consume_rate_limit_token(&mut admin, &key)
            .await
            .expect("debit the token"),
        "the bucket starts with one token"
    );

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let mut conn = pool.get().await.expect("engine connection");
    let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(&mut conn)
        .await
        .expect("read the backend pid")
        .pid;
    diesel::sql_query("SELECT pg_terminate_backend($1)")
        .bind::<diesel::sql_types::Integer, _>(pid)
        .execute(&mut admin)
        .await
        .expect("end the pooled session");
    tokio::time::sleep(Duration::from_millis(100)).await;

    autumn_harvest::worker::refund_after_start_error(
        &pool,
        Some(conn),
        Some(&key),
        &autumn_harvest::error::HarvestError::Database("connection closed".into()),
    )
    .await;

    let tokens = diesel::sql_query("SELECT tokens FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(&key)
        .get_result::<Tokens>(&mut admin)
        .await
        .expect("read the bucket")
        .tokens;
    diesel::sql_query("DELETE FROM harvest_rate_limit_buckets WHERE key = $1")
        .bind::<Text, _>(&key)
        .execute(&mut admin)
        .await
        .expect("drop the bucket");
    assert!((tokens - 1.0).abs() < 1e-9, "tokens = {tokens}");
}

/// A heartbeat that waits out a full pool keeps its own time. A retry that
/// stamps the write time instead would make a stalled handler look alive.
#[tokio::test]
async fn a_retried_heartbeat_keeps_its_receipt_time() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let queue_name = format!("q-hr-{suffix}");
    let worker_id = format!("w-hr-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(200, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_millis(200),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    let sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 1})));

    // Several flushes fail while the pool is full. Then the slot frees.
    tokio::time::sleep(Duration::from_secs(4)).await;
    drop(held);
    tokio::time::sleep(Duration::from_secs(3)).await;
    cancel.cancel();

    let beat = diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Beat>(&mut conn)
        .await
        .expect("read the heartbeat")
        .last_heartbeat_at
        .expect("the retry wrote the heartbeat");
    let lag = beat - sent_at;
    assert!(
        lag < chrono::Duration::milliseconds(1_500),
        "the heartbeat time moved to the retry: {lag}"
    );
}

/// A heartbeat keeps its send time when the runtime is busy. The handler
/// blocks this current-thread runtime right after the send, so the receiving
/// task runs late. A time taken there would make a stale heartbeat look new.
#[tokio::test]
async fn a_heartbeat_keeps_its_send_time_on_a_busy_runtime() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let queue_name = format!("q-hs-{suffix}");
    let worker_id = format!("w-hs-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_secs(5),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    let sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 1})));
    // Synchronous handler work holds the only runtime thread.
    std::thread::sleep(std::time::Duration::from_secs(2));
    tokio::time::sleep(Duration::from_secs(3)).await;
    cancel.cancel();

    let beat = diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Beat>(&mut conn)
        .await
        .expect("read the heartbeat")
        .last_heartbeat_at
        .expect("the flusher wrote the heartbeat");
    let lag = beat - sent_at;
    assert!(
        lag < chrono::Duration::milliseconds(1_000),
        "the heartbeat time moved to when the runtime got free: {lag}"
    );
}

/// A start whose connection dropped may still have committed. Only an
/// `ActivityStarted` from this worker after this claim's `started_at` proves
/// that. Both times come from the database clock.
#[tokio::test]
async fn a_lost_start_is_found_only_when_this_claim_wrote_it() {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use autumn_harvest::types::{ActivityExecId, WorkerId};
    use diesel::{QueryDsl, SelectableHelper};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-ls-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let activity_id = ActivityExecId::from_uuid(Uuid::new_v4());
    let started = |worker: &str| WorkflowEvent::ActivityStarted {
        activity_id,
        worker_id: WorkerId::new(worker),
    };
    store::append_events(
        &mut conn,
        exec_id,
        &[
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "act".to_owned(),
                input: serde_json::json!({}),
                queue: queue_name.clone(),
            },
            // An earlier attempt by the same worker.
            started("w-1"),
        ],
        1,
    )
    .await
    .expect("schedule the activity");

    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 2, \
         started_at = NOW(), workflow_exec_id = $2, activity_id = $3, activity_name = 'act' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");
    let load = async |conn: &mut AsyncPgConnection| -> TaskQueueItem {
        harvest_task_queue::table
            .find(task_id)
            .select(TaskQueueItem::as_select())
            .first(conn)
            .await
            .expect("load the task")
    };
    let task = load(&mut conn).await;
    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let committed = async |task: &TaskQueueItem| {
        autumn_harvest::worker::lost_start_committed(&pool, task, exec_id, "act", "w-1")
            .await
            .expect("reconcile reads")
    };

    assert!(
        !committed(&task).await,
        "an earlier attempt's start is not this claim's"
    );

    store::append_events(&mut conn, exec_id, &[started("w-2")], 3)
        .await
        .expect("another worker's start");
    assert!(
        !committed(&task).await,
        "another worker's start is not this claim's"
    );

    store::append_events(&mut conn, exec_id, &[started("w-1")], 4)
        .await
        .expect("this claim's start");
    assert!(committed(&task).await, "this claim's start committed");

    // A newer claim by the same worker owns that start, not this claim.
    diesel::sql_query("UPDATE harvest_task_queue SET attempt = 3 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("model a newer claim");
    assert!(
        !committed(&task).await,
        "a start under a newer claim is not this claim's"
    );
}

/// The reconcile read retries while the pool is busy. A single failed read
/// would release the claim, and the next claim would append a second start.
#[tokio::test]
async fn a_lost_start_check_waits_for_a_busy_pool() {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use autumn_harvest::types::{ActivityExecId, WorkerId};
    use diesel::{QueryDsl, SelectableHelper};

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-lw-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let activity_id = ActivityExecId::from_uuid(Uuid::new_v4());
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "act".to_owned(),
            input: serde_json::json!({}),
            queue: queue_name.clone(),
        }],
        1,
    )
    .await
    .expect("schedule the activity");
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         started_at = NOW(), workflow_exec_id = $2, activity_id = $3, activity_name = 'act' \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");
    store::append_events(
        &mut conn,
        exec_id,
        &[WorkflowEvent::ActivityStarted {
            activity_id,
            worker_id: WorkerId::new("w-1"),
        }],
        2,
    )
    .await
    .expect("this claim's start");
    let task: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(&mut conn)
        .await
        .expect("load the task");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(200, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(700)).await;
        drop(held);
    });
    let committed =
        autumn_harvest::worker::lost_start_committed(&pool, &task, exec_id, "act", "w-1")
            .await
            .expect("the check waits for the pool");
    release.await.expect("release joins");
    assert!(committed, "this claim's start committed");
}

/// A result write that runs after another worker reclaimed the task must not
/// reach the newer attempt. The retries of issue #1788 widen that window, so
/// each finalization checks the claim's `attempt` and `worker_id`.
#[tokio::test]
async fn a_stale_result_write_does_not_reach_a_newer_claim() {
    #[derive(diesel::QueryableByName)]
    struct Claim {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        worker_id: Option<String>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    for (case, result) in [
        ("completion", Ok(serde_json::json!({"done": true}))),
        ("retry", Err("transient failure".to_owned())),
        ("deadline", Err("transient failure".to_owned())),
    ] {
        let (exec_id, _activity_id, mut stale) = seed_claimed_activity(&mut conn, "q-sr").await;
        let task_id = stale.id;
        if case == "deadline" {
            // The 1 s retry delay crosses this deadline, so the write takes
            // the schedule-to-close timeout branch.
            let deadline = Utc::now() + chrono::Duration::milliseconds(500);
            stale.schedule_to_close_at = Some(deadline);
            diesel::sql_query(
                "UPDATE harvest_task_queue SET schedule_to_close_at = $2 WHERE id = $1",
            )
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .bind::<diesel::sql_types::Timestamptz, _>(deadline)
            .execute(&mut conn)
            .await
            .expect("set the deadline");
        }

        // Another worker reclaims the task.
        diesel::sql_query(
            "UPDATE harvest_task_queue SET worker_id = 'w-2', attempt = 2 WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("model the newer claim");

        let policy = autumn_harvest::policy::RetryPolicy::fixed(3, Duration::from_secs(1));
        let _ = autumn_harvest::worker::write_activity_result_for_task(
            &mut conn,
            &stale,
            "w-1",
            Some(&policy),
            result,
            &autumn_harvest::payload_codec::PayloadCodecs::default(),
        )
        .await;

        let claim =
            diesel::sql_query("SELECT state, worker_id FROM harvest_task_queue WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .get_result::<Claim>(&mut conn)
                .await
                .expect("read the claim");
        assert_eq!(
            (claim.state.as_str(), claim.worker_id.as_deref()),
            ("RUNNING", Some("w-2")),
            "{case}: the stale write changed the newer claim"
        );
        let history = store::load_history(&mut conn, exec_id)
            .await
            .expect("load history");
        assert!(
            !history
                .events
                .iter()
                .any(|event| matches!(event, WorkflowEvent::ActivityCompleted { .. })),
            "{case}: the stale write completed the activity"
        );
    }
}

/// Seed an activity that `w-1` scheduled, claimed (attempt 1) and started.
/// Returns the execution, the activity and the claimed task row.
async fn seed_claimed_activity(
    conn: &mut AsyncPgConnection,
    queue_prefix: &str,
) -> (
    ExecutionId,
    autumn_harvest::types::ActivityExecId,
    autumn_harvest::models::TaskQueueItem,
) {
    use autumn_harvest::models::TaskQueueItem;
    use autumn_harvest::schema::harvest_task_queue;
    use autumn_harvest::types::{ActivityExecId, WorkerId};
    use diesel::{QueryDsl, SelectableHelper};

    let queue_name = format!(
        "{queue_prefix}-{}",
        &Uuid::new_v4().simple().to_string()[..12]
    );
    let exec_id = seed_execution(conn, &queue_name).await;
    let activity_id = ActivityExecId::from_uuid(Uuid::new_v4());
    store::append_events(
        conn,
        exec_id,
        &[
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "act".to_owned(),
                input: serde_json::json!({}),
                queue: queue_name.clone(),
            },
            WorkflowEvent::ActivityStarted {
                activity_id,
                worker_id: WorkerId::new("w-1"),
            },
        ],
        1,
    )
    .await
    .expect("schedule and start the activity");
    let task_id = autumn_harvest::queue::enqueue(
        conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(conn, "w-1").await;
    register_live_worker(conn, "w-2").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         started_at = NOW(), workflow_exec_id = $2, activity_id = $3, \
         activity_name = 'act' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Uuid, _>(activity_id.as_uuid())
    .execute(conn)
    .await
    .expect("model the first claim");
    let task: TaskQueueItem = harvest_task_queue::table
        .find(task_id)
        .select(TaskQueueItem::as_select())
        .first(conn)
        .await
        .expect("load the first claim");
    (exec_id, activity_id, task)
}

/// A quarantine lookup that a session timeout cancels must not read as a
/// missing execution. That would dead-letter the task but leave the workflow
/// `RUNNING`, and report success. Another session locks the executions table
/// for 400 ms, and the pool gives up on a lock after 250 ms. The lookup fails,
/// but the quarantine transaction itself could still commit.
#[tokio::test]
async fn a_failed_quarantine_lookup_reports_failure() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-ql-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Workflow,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         workflow_exec_id = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(
            "DO $$ BEGIN LOCK TABLE harvest_workflow_executions IN ACCESS EXCLUSIVE MODE; \
             PERFORM pg_sleep(0.4); END $$",
        )
        .execute(&mut conn)
        .await
        .expect("hold the table lock");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(250),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let reached = autumn_harvest::worker::quarantine_workflow_task_timeout(
        &pool,
        task_id,
        Some(exec_id.as_uuid()),
        "w-1",
        1,
        3,
        10,
        "wf",
        &queue_name,
        &autumn_harvest::telemetry::NoOpMetrics,
        &autumn_harvest::payload_codec::PayloadCodecs::default(),
    )
    .await;
    holder.await.expect("holder joins");

    assert!(!reached, "a failed lookup must not report a quarantine");
    assert_eq!(task_state(&mut conn, task_id).await, "RUNNING");
}

/// A heartbeat that arrives while a flush is blocked keeps its arrival time.
/// The first flush waits 3 s for a held pool. The second heartbeat arrives
/// during that wait. The flusher reads it only after the wait, but its write
/// must carry the arrival time.
#[tokio::test]
async fn a_heartbeat_sent_during_a_blocked_flush_keeps_its_time() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let worker_id = format!("w-hb-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            format!("q-hb-{suffix}"),
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(3_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_secs(3),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    assert!(tx.send(serde_json::json!({"progress": 1})));
    // The first flush starts at about 1 s and waits until about 4 s.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 2})));
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    drop(held);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    cancel.cancel();

    let beat = diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Beat>(&mut conn)
        .await
        .expect("read the heartbeat")
        .last_heartbeat_at
        .expect("a flush wrote the heartbeat");
    let lag = beat - sent_at;
    assert!(
        lag < chrono::Duration::milliseconds(1_000),
        "the heartbeat time moved past its arrival: {lag}"
    );
}

/// A newer heartbeat that waits behind a blocked flush is written as soon as
/// that flush ends, not one interval later. Until then the row shows the old
/// send time, and a timeout scanner could reclaim a live activity.
#[tokio::test]
async fn a_newer_heartbeat_follows_a_blocked_flush_at_once() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let worker_id = format!("w-hb-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            format!("q-hb-{suffix}"),
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(10_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_secs(10),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    assert!(tx.send(serde_json::json!({"progress": 1})));
    // The first flush starts at about 1 s and blocks on the held pool.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let newer_sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 2})));
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    drop(held);
    let released = Instant::now();

    let deadline = released + Duration::from_secs(5);
    let written_after = loop {
        let beat =
            diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .get_result::<Beat>(&mut conn)
                .await
                .expect("read the heartbeat")
                .last_heartbeat_at;
        if beat.is_some_and(|at| at >= newer_sent_at) {
            break released.elapsed();
        }
        assert!(
            Instant::now() < deadline,
            "the newer heartbeat was never written"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    cancel.cancel();
    assert!(
        written_after < Duration::from_millis(600),
        "the newer heartbeat waited for another interval: {written_after:?}"
    );
}

/// A flush that waited on a full pool writes the newest heartbeat once it
/// gets a connection (issue #1788). A scanner that waits for the same slot
/// then never sees the old send time. Without this, the flush wrote the old
/// time and released the slot before it wrote the newer heartbeat.
#[tokio::test]
async fn a_scanner_behind_a_blocked_flush_sees_the_newer_heartbeat() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let worker_id = format!("w-hb-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            format!("q-hb-{suffix}"),
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1 \
         WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim");

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(10_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_secs(10),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    assert!(tx.send(serde_json::json!({"progress": 1})));
    // The first flush starts at about 1 s and blocks on the held pool.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let newer_sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 2})));
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The flush waits first, so it gets the slot. The scanner waits next, so
    // it gets the slot as soon as the flush releases it.
    drop(held);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut scanner = pool.get().await.expect("the scanner gets the slot");
    let beat = diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Beat>(&mut *scanner)
        .await
        .expect("read the heartbeat")
        .last_heartbeat_at
        .expect("the flush wrote a heartbeat");
    drop(scanner);
    cancel.cancel();
    assert!(
        beat >= newer_sent_at - chrono::Duration::milliseconds(50),
        "the scanner saw the old send time {beat}; the newer heartbeat was sent at {newer_sent_at}"
    );
}

/// A start that lost its connection on a one-slot pool must still find its
/// committed `ActivityStarted`. The dead connection holds the only slot. The
/// reconcile read needs that slot, so the dead connection must go back first.
#[tokio::test]
async fn a_lost_start_on_a_one_slot_pool_is_still_found() {
    #[derive(diesel::QueryableByName)]
    struct Pid {
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pid: i32,
    }

    let (url, _container) = setup_db().await;
    let mut admin = connect(&url).await;
    // `seed_claimed_activity` writes this claim's start after the claim, as
    // a start that committed before the connection dropped.
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut admin, "q-l1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET started_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task.id)
    .execute(&mut admin)
    .await
    .expect("move the claim time before the start");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task.id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut admin)
            .await
            .expect("reload the claim")
    };

    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(200, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let mut conn = pool.get().await.expect("the only connection");
    let pid = diesel::sql_query("SELECT pg_backend_pid() AS pid")
        .get_result::<Pid>(&mut conn)
        .await
        .expect("read the backend pid")
        .pid;
    diesel::sql_query("SELECT pg_terminate_backend($1)")
        .bind::<diesel::sql_types::Integer, _>(pid)
        .execute(&mut admin)
        .await
        .expect("end the pooled session");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let found =
        autumn_harvest::worker::append_start_for_test(&pool, conn, &task, exec_id, "act", "w-1")
            .await
            .expect("the reconcile reads the committed start");
    assert!(found, "this claim's committed start must be found");
}

/// A lost connection can leave the start transaction still committing
/// (issue #1788). The reconcile read must wait for it. A plain read sees no
/// start, the claim goes back, and the start then commits. The retry would
/// append a second `ActivityStarted` for a handler that never ran.
#[tokio::test]
async fn a_lost_start_read_waits_for_a_start_in_progress() {
    let (url, _container) = setup_db().await;
    let mut admin = connect(&url).await;
    // The seeded start is older than the claim, so it is not this claim's.
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut admin, "q-lw").await;

    // The start transaction of this claim: it holds its locks while it
    // commits.
    let mut starter = connect(&url).await;
    let start_task = task.clone();
    let start = tokio::spawn(async move {
        Box::pin(
            starter.transaction::<_, autumn_harvest::error::HarvestError, _>(async |conn| {
                autumn_harvest::worker::append_activity_started_for_test(
                    conn,
                    &start_task,
                    exec_id,
                    "act",
                    "w-1",
                    &autumn_harvest::payload_codec::PayloadCodecs::default(),
                )
                .await?;
                tokio::time::sleep(Duration::from_millis(600)).await;
                Ok(())
            }),
        )
        .await
        .expect("the start commits");
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let pool = engine_pool(
        url,
        2,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let found =
        autumn_harvest::worker::reconcile_lost_start_for_test(&pool, &task, exec_id, "act", "w-1")
            .await
            .expect("the reconcile reads");
    start.await.expect("the start joins");
    assert!(
        found,
        "the reconcile must wait for the start that is committing"
    );
}

/// A session-release write that a session timeout cancels must not fail the
/// workflow. The handler wrote nothing, so the claim goes back for a retry.
/// Another session locks `harvest_sessions` for 600 ms, and the pool gives up
/// on a lock after 150 ms.
#[tokio::test]
async fn a_session_release_timeout_does_not_fail_the_workflow() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut conn, "q-sx").await;
    diesel::sql_query("UPDATE harvest_task_queue SET input = to_jsonb($2::text) WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .bind::<Text, _>(Uuid::new_v4().to_string())
        .execute(&mut conn)
        .await
        .expect("make the input a session id");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task.id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("reload the claim")
    };

    let lock_url = url.clone();
    let holder = tokio::spawn(async move {
        let mut conn = connect(&lock_url).await;
        diesel::sql_query(
            "DO $$ BEGIN LOCK TABLE harvest_sessions IN ACCESS EXCLUSIVE MODE; \
             PERFORM pg_sleep(0.6); END $$",
        )
        .execute(&mut conn)
        .await
        .expect("hold the table lock");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let session = SessionTimeouts {
        lock: Duration::from_millis(150),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url, 1, DbRole::Hot, &timeouts(5_000, session)).expect("engine pool");
    let outcome =
        autumn_harvest::worker::handle_session_release_for_test(&pool, &task, "w-1", exec_id).await;
    holder.await.expect("holder joins");

    let err = outcome.expect_err("the release write timed out");
    assert!(autumn_harvest::pool::is_transient_db_error(&err), "{err}");
    assert_eq!(task_state(&mut conn, task.id).await, "RUNNING");
    let history = store::load_history(&mut conn, exec_id)
        .await
        .expect("load history");
    assert!(
        !history
            .events
            .iter()
            .any(|event| matches!(event, WorkflowEvent::WorkflowFailed { .. })),
        "a transient session write must not fail the workflow"
    );
}

/// A session acquire that fails on a transient error may still have
/// committed its row. The local slot must stay when this worker is the
/// recorded host. Otherwise the worker would run the session without
/// counting it. With no row, or another host, the slot goes back.
#[tokio::test]
async fn a_transient_session_acquire_keeps_the_slot_only_for_its_own_session() {
    use autumn_harvest::sessions::{
        new_session_slot_registry, record_session_acquired, session_slot_count,
        try_acquire_session_slot,
    };
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-ss-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let expires_at = Utc::now() + chrono::Duration::minutes(5);

    for (case, recorded_host, kept) in [
        ("hosted here", Some("w-1"), true),
        ("no row", None, false),
        ("hosted elsewhere", Some("w-2"), false),
    ] {
        let session_id = SessionId::new();
        if let Some(host) = recorded_host {
            record_session_acquired(
                &mut conn,
                session_id,
                exec_id,
                host,
                &queue_name,
                expires_at,
            )
            .await
            .expect("record the session");
        }
        let registry = new_session_slot_registry();
        assert!(try_acquire_session_slot(&registry, 4, session_id));

        autumn_harvest::worker::settle_session_slot_after_transient_error(
            &pool,
            &registry,
            &attempted_session_row(session_id, exec_id, &queue_name),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        assert_eq!(
            session_slot_count(&registry) == 1,
            kept,
            "{case}: wrong slot decision"
        );
    }
}

/// A heartbeat write that blocks for an interval and then hits `lock_timeout`
/// is followed on the same connection by a newer heartbeat (issue #1788).
/// The timeout keeps the session, so the connection still works. A scanner
/// that waits for the only slot then never reads the old stamp.
#[tokio::test]
async fn a_scanner_behind_a_timed_out_heartbeat_sees_the_newer_one() {
    #[derive(diesel::QueryableByName)]
    struct Beat {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_heartbeat_at: Option<chrono::DateTime<Utc>>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let suffix = Uuid::new_v4().simple().to_string()[..12].to_owned();
    let worker_id = format!("w-hb-{suffix}");
    register_live_worker(&mut conn, &worker_id).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            format!("q-hb-{suffix}"),
            autumn_harvest::queue::TaskType::Activity,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = $2, attempt = 1, \
         last_heartbeat_at = clock_timestamp() - INTERVAL '1 hour' WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<Text, _>(&worker_id)
    .execute(&mut conn)
    .await
    .expect("model the claim with an old heartbeat");

    let mut role_timeouts = SessionTimeouts::for_role(DbRole::Hot);
    role_timeouts.lock = Duration::from_millis(1_500);
    let pool = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(10_000, role_timeouts),
    )
    .expect("engine pool");

    // Lock the row, so the heartbeat write waits until `lock_timeout`.
    let mut locker = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut locker)
        .await
        .expect("begin");
    diesel::sql_query("SELECT id FROM harvest_task_queue WHERE id = $1 FOR UPDATE")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut locker)
        .await
        .expect("lock the row");

    let cancel = tokio_util::sync::CancellationToken::new();
    let tx = autumn_harvest::heartbeat::spawn_heartbeat_flusher_with(
        autumn_harvest::queue::TaskClaim::new(task_id, worker_id.clone(), 1),
        pool.clone(),
        cancel.clone(),
        autumn_harvest::heartbeat::HeartbeatFlushOptions {
            acquire_timeout: Duration::from_secs(10),
            metrics: Arc::new(autumn_harvest::telemetry::NoOpMetrics),
            shard: 0,
        },
    );
    assert!(tx.send(serde_json::json!({"progress": 1})));
    // The write starts at about 1 s and times out at about 2.5 s.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let newer_sent_at = Utc::now();
    assert!(tx.send(serde_json::json!({"progress": 2})));

    // The scanner waits for the only slot behind the flush.
    let scanner = {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut scanner = pool.get().await.expect("the scanner gets the slot");
            diesel::sql_query("SELECT last_heartbeat_at FROM harvest_task_queue WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(task_id)
                .get_result::<Beat>(&mut *scanner)
                .await
                .expect("read the heartbeat")
                .last_heartbeat_at
                .expect("a heartbeat exists")
        })
    };
    // Release the lock after the first write timed out.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    diesel::sql_query("COMMIT")
        .execute(&mut locker)
        .await
        .expect("release the lock");
    let beat = scanner.await.expect("the scanner joins");
    cancel.cancel();
    assert!(
        beat >= newer_sent_at - chrono::Duration::milliseconds(50),
        "the scanner saw the old stamp {beat}; the newer heartbeat was sent at {newer_sent_at}"
    );
}

/// An acquire that lost its connection can still run its insert on the
/// server (issue #1788). The re-check must wait for that insert. A plain
/// read sees no row, releases the slot, and the insert then commits a
/// session that this worker hosts but does not count.
#[tokio::test]
async fn a_session_recheck_waits_for_an_insert_in_progress() {
    use autumn_harvest::sessions::{
        new_session_slot_registry, session_slot_count, try_acquire_session_slot,
    };
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-ss-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let pool = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let session_id = SessionId::new();
    let registry = new_session_slot_registry();
    assert!(try_acquire_session_slot(&registry, 4, session_id));

    // The lost acquire: its insert runs, but has not committed yet.
    let mut original = connect(&url).await;
    diesel::sql_query("BEGIN")
        .execute(&mut original)
        .await
        .expect("begin");
    autumn_harvest::sessions::record_session_acquired(
        &mut original,
        session_id,
        exec_id,
        "w-1",
        &queue_name,
        Utc::now() + chrono::Duration::minutes(5),
    )
    .await
    .expect("insert the session in an open transaction");
    let commit = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        diesel::sql_query("COMMIT")
            .execute(&mut original)
            .await
            .expect("commit");
    });

    autumn_harvest::worker::settle_session_slot_after_transient_error(
        &pool,
        &registry,
        &autumn_harvest::worker::AttemptedSessionRow {
            session_id,
            exec_id,
            host_worker_id: "w-1".to_owned(),
            queue_name: queue_name.clone(),
        },
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    commit.await.expect("the commit joins");
    assert_eq!(
        session_slot_count(&registry),
        1,
        "the re-check released the slot of a session that this worker hosts"
    );
}

/// A kept slot is checked again once the database answers. Every first read
/// fails on a full pool, so the slot stays at first. After the pool frees, a
/// background read finds no session row and releases the slot. Otherwise the
/// slot would stay until the worker restarts.
#[tokio::test]
async fn a_kept_session_slot_is_released_once_the_row_proves_absent() {
    use autumn_harvest::sessions::{
        new_session_slot_registry, session_slot_count, try_acquire_session_slot,
    };
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let pool = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(100, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    let held = hold_every_connection(&pool).await;

    let session_id = SessionId::new();
    let registry = new_session_slot_registry();
    assert!(try_acquire_session_slot(&registry, 4, session_id));
    autumn_harvest::worker::settle_session_slot_after_transient_error(
        &pool,
        &registry,
        &unseeded_session_row(session_id),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    assert_eq!(
        session_slot_count(&registry),
        1,
        "with no answer, the slot stays"
    );

    drop(held);
    let deadline = Instant::now() + Duration::from_secs(5);
    while session_slot_count(&registry) == 1 {
        assert!(
            Instant::now() < deadline,
            "the slot of a session with no row must be released once the database answers"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// An acquire waits while a background re-check of its slot runs. Otherwise
/// the re-check can read no row just before the acquire inserts one. It then
/// releases the slot of a session that this worker hosts, and the worker can
/// exceed `max_concurrent_sessions`.
#[tokio::test]
async fn a_session_acquire_defers_while_its_slot_is_rechecked() {
    use autumn_harvest::sessions::{new_session_slot_registry, try_acquire_session_slot};
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut conn, "q-sr").await;
    let session_id = SessionId::new();
    diesel::sql_query("UPDATE harvest_task_queue SET input = to_jsonb($2::text) WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .bind::<Text, _>(session_id.to_string())
        .execute(&mut conn)
        .await
        .expect("make the input a session id");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task.id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("reload the claim")
    };

    // The re-check gets no answer, so it keeps running in the background.
    let starved = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(100, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("starved pool");
    let held = hold_every_connection(&starved).await;
    let registry = new_session_slot_registry();
    assert!(try_acquire_session_slot(&registry, 4, session_id));
    autumn_harvest::worker::settle_session_slot_after_transient_error(
        &starved,
        &registry,
        &unseeded_session_row(session_id),
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;

    let pool = engine_pool(
        url,
        2,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    autumn_harvest::worker::handle_session_acquire_for_test(
        &pool, &task, "w-1", exec_id, 4, &registry,
    )
    .await
    .expect("the acquire defers");

    assert_eq!(task_state(&mut conn, task.id).await, "PENDING");
    let rows: i64 = {
        use autumn_harvest::schema::harvest_sessions::dsl;
        use diesel::{ExpressionMethods, QueryDsl};
        dsl::harvest_sessions
            .filter(dsl::id.eq(session_id.as_uuid()))
            .count()
            .get_result(&mut conn)
            .await
            .expect("count session rows")
    };
    assert_eq!(rows, 0, "the acquire must not record the session yet");
    drop(held);
}

/// Worker shutdown also stops the first, awaited round of a slot re-check.
/// That round can take ten pool bounds, so a worker that waits for it would
/// outlive its drain deadline.
#[tokio::test]
async fn shutdown_stops_the_first_slot_recheck_round() {
    use autumn_harvest::sessions::{new_session_slot_registry, try_acquire_session_slot};
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let starved = engine_pool(
        url,
        1,
        DbRole::Hot,
        &timeouts(1_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("starved pool");
    let held = hold_every_connection(&starved).await;
    let registry = new_session_slot_registry();
    let session_id = SessionId::new();
    assert!(try_acquire_session_slot(&registry, 4, session_id));

    let shutdown = tokio_util::sync::CancellationToken::new();
    let canceller = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            shutdown.cancel();
        })
    };
    let clock = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(30),
        autumn_harvest::worker::settle_session_slot_after_transient_error(
            &starved,
            &registry,
            &unseeded_session_row(session_id),
            &shutdown,
        ),
    )
    .await
    .expect("the re-check ends");
    assert!(
        clock.elapsed() < Duration::from_secs(3),
        "shutdown must stop the first round, not wait out ten bounds: {:?}",
        clock.elapsed()
    );
    canceller.await.expect("canceller joins");
    drop(held);
}

/// Worker shutdown stops a slot re-check that gets no answer. The re-check
/// then releases its mark, so an acquire of the session no longer defers.
#[tokio::test]
async fn shutdown_stops_a_slot_recheck_that_gets_no_answer() {
    use autumn_harvest::sessions::{new_session_slot_registry, try_acquire_session_slot};
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut conn, "q-sd").await;
    let session_id = SessionId::new();
    diesel::sql_query("UPDATE harvest_task_queue SET input = to_jsonb($2::text) WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .bind::<Text, _>(session_id.to_string())
        .execute(&mut conn)
        .await
        .expect("make the input a session id");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task.id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("reload the claim")
    };

    let starved = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(100, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("starved pool");
    let held = hold_every_connection(&starved).await;
    let registry = new_session_slot_registry();
    assert!(try_acquire_session_slot(&registry, 4, session_id));
    let shutdown = tokio_util::sync::CancellationToken::new();
    autumn_harvest::worker::settle_session_slot_after_transient_error(
        &starved,
        &registry,
        &unseeded_session_row(session_id),
        &shutdown,
    )
    .await;

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pool = engine_pool(
        url,
        2,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    autumn_harvest::worker::handle_session_acquire_for_test(
        &pool, &task, "w-1", exec_id, 4, &registry,
    )
    .await
    .expect("the acquire runs");
    assert_eq!(
        task_state(&mut conn, task.id).await,
        "COMPLETED",
        "the acquire must not defer once shutdown stopped the re-check"
    );
    drop(held);
}

/// A drain inside the caller's transaction gives the session limits back.
/// Diesel runs a nested transaction as a savepoint. Releasing a savepoint
/// keeps a `SET LOCAL`, so the drain must restore the limits itself.
#[tokio::test]
async fn a_nested_drain_restores_the_session_limits() {
    #[derive(diesel::QueryableByName)]
    struct Limits {
        #[diesel(sql_type = Text)]
        statement_ms: String,
        #[diesel(sql_type = Text)]
        lock_ms: String,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let limits = Box::pin(
        conn.transaction::<Limits, autumn_harvest::error::HarvestError, _>(async |conn| {
            diesel::sql_query("SET LOCAL statement_timeout = '7s'")
                .execute(conn)
                .await?;
            diesel::sql_query("SET LOCAL lock_timeout = '3s'")
                .execute(conn)
                .await?;
            autumn_harvest::partition::drain_default(conn).await?;
            Ok(diesel::sql_query(
                "SELECT current_setting('statement_timeout') AS statement_ms, \
                 current_setting('lock_timeout') AS lock_ms",
            )
            .get_result::<Limits>(conn)
            .await?)
        }),
    )
    .await
    .expect("drain inside a transaction");
    assert_eq!(limits.statement_ms, "7s");
    assert_eq!(limits.lock_ms, "3s");
}

/// A drain step that fails returns its own error. Postgres aborts the
/// transaction on an error, so a restore of the limits would fail and hide
/// the cause. The rollback restores the limits instead.
#[tokio::test]
async fn a_failed_drain_step_keeps_its_own_error() {
    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let error = Box::pin(
        conn.transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            autumn_harvest::partition::exec_with_session_limits_off(conn, "SELECT 1 / 0").await
        }),
    )
    .await
    .expect_err("the statement fails");
    assert!(
        error.to_string().contains("division by zero"),
        "the drain must return the failing statement's error, got: {error}"
    );
}

/// A re-drive after the handler lookup clears the capability-miss evidence.
/// The lookup proved this worker capable. Stale evidence could end the
/// redelivery budget early.
#[tokio::test]
async fn a_post_lookup_redrive_clears_capability_misses() {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        capability_misses: i32,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-cm-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Workflow,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         workflow_exec_id = $2, capability_misses = 2, \
         capability_miss_workers = ARRAY['w-x', 'w-y'] WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the claim");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task_id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("load the claim")
    };

    autumn_harvest::worker::requeue_workflow_task_after_event_id_conflict(
        &mut conn,
        &task,
        "w-1",
        Duration::ZERO,
        exec_id,
    )
    .await
    .expect("re-drive");

    let row =
        diesel::sql_query("SELECT state, capability_misses FROM harvest_task_queue WHERE id = $1")
            .bind::<diesel::sql_types::Uuid, _>(task_id)
            .get_result::<Row>(&mut conn)
            .await
            .expect("read the row");
    assert_eq!(row.state, "PENDING");
    assert_eq!(row.capability_misses, 0);
}

/// A stale handler's re-drive leaves a peer's claim alone. The peer took the
/// row after this handler loaded it. A park without the claim fence would
/// clear the peer's ownership and let a third dispatch run.
#[tokio::test]
async fn a_stale_redrive_leaves_a_peer_claim_alone() {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
        worker_id: Option<String>,
    }

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let queue_name = format!("q-sp-{}", &Uuid::new_v4().simple().to_string()[..12]);
    let exec_id = seed_execution(&mut conn, &queue_name).await;
    let task_id = autumn_harvest::queue::enqueue(
        &mut conn,
        &autumn_harvest::queue::EnqueueParams::new(
            &queue_name,
            autumn_harvest::queue::TaskType::Workflow,
            serde_json::json!({}),
        ),
    )
    .await
    .expect("enqueue");
    register_live_worker(&mut conn, "w-1").await;
    register_live_worker(&mut conn, "w-2").await;
    diesel::sql_query(
        "UPDATE harvest_task_queue SET state = 'RUNNING', worker_id = 'w-1', attempt = 1, \
         workflow_exec_id = $2 WHERE id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(task_id)
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .execute(&mut conn)
    .await
    .expect("model the first claim");
    let stale = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task_id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("load the first claim")
    };
    diesel::sql_query("UPDATE harvest_task_queue SET worker_id = 'w-2', attempt = 2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .execute(&mut conn)
        .await
        .expect("model the peer's claim");

    autumn_harvest::worker::requeue_workflow_task_after_event_id_conflict(
        &mut conn,
        &stale,
        "w-1",
        Duration::ZERO,
        exec_id,
    )
    .await
    .expect("a lost claim is a no-op");

    let row = diesel::sql_query("SELECT state, worker_id FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<Row>(&mut conn)
        .await
        .expect("read the row");
    assert_eq!(row.state, "RUNNING");
    assert_eq!(row.worker_id.as_deref(), Some("w-2"));
}

/// An acquire also waits while the first, synchronous re-check runs. That
/// re-check can take ten pool bounds. An orphan reclaim can retry the task on
/// this worker in that time.
#[tokio::test]
async fn a_session_acquire_defers_during_the_first_recheck() {
    use autumn_harvest::sessions::{new_session_slot_registry, try_acquire_session_slot};
    use autumn_harvest::types::SessionId;

    let (url, _container) = setup_db().await;
    let mut conn = connect(&url).await;
    let (exec_id, _activity_id, task) = seed_claimed_activity(&mut conn, "q-sf").await;
    let session_id = SessionId::new();
    diesel::sql_query("UPDATE harvest_task_queue SET input = to_jsonb($2::text) WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task.id)
        .bind::<Text, _>(session_id.to_string())
        .execute(&mut conn)
        .await
        .expect("make the input a session id");
    let task = {
        use autumn_harvest::models::TaskQueueItem;
        use autumn_harvest::schema::harvest_task_queue;
        use diesel::{QueryDsl, SelectableHelper};
        harvest_task_queue::table
            .find(task.id)
            .select(TaskQueueItem::as_select())
            .first::<TaskQueueItem>(&mut conn)
            .await
            .expect("reload the claim")
    };

    // Every read of the first re-check waits out a full pool.
    let starved = engine_pool(
        url.clone(),
        1,
        DbRole::Hot,
        &timeouts(300, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("starved pool");
    let held = hold_every_connection(&starved).await;
    let registry = new_session_slot_registry();
    assert!(try_acquire_session_slot(&registry, 4, session_id));
    let recheck = {
        let starved = starved.clone();
        let registry = registry.clone();
        tokio::spawn(async move {
            autumn_harvest::worker::settle_session_slot_after_transient_error(
                &starved,
                &registry,
                &unseeded_session_row(session_id),
                &tokio_util::sync::CancellationToken::new(),
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !recheck.is_finished(),
        "the first re-check is still running"
    );

    let pool = engine_pool(
        url,
        2,
        DbRole::Hot,
        &timeouts(5_000, SessionTimeouts::for_role(DbRole::Hot)),
    )
    .expect("engine pool");
    autumn_harvest::worker::handle_session_acquire_for_test(
        &pool, &task, "w-1", exec_id, 4, &registry,
    )
    .await
    .expect("the acquire defers");

    assert_eq!(task_state(&mut conn, task.id).await, "PENDING");
    drop(held);
    recheck.await.expect("the re-check ends");
}

/// Mark `worker_id` as a live worker. A test row `RUNNING` under an unknown
/// worker is an orphan, and a reclaimer of a parallel suite would requeue it.
/// The row has no queues and a shard that does not exist, so no other code
/// gives it work.
async fn register_live_worker(conn: &mut AsyncPgConnection, worker_id: &str) {
    diesel::sql_query(
        "INSERT INTO harvest_workers \
         (worker_id, queues, shard_assignments, max_concurrency, in_flight_count, \
          status, last_heartbeat_at, started_at, host) \
         VALUES ($1, '[]'::jsonb, '[9999]'::jsonb, 1, 0, 'Active', \
                 NOW() + INTERVAL '1 hour', NOW(), 'pg-timeouts-test') \
         ON CONFLICT (worker_id) DO UPDATE SET last_heartbeat_at = EXCLUDED.last_heartbeat_at",
    )
    .bind::<Text, _>(worker_id)
    .execute(conn)
    .await
    .expect("register the test worker");
}

async fn task_state(conn: &mut AsyncPgConnection, task_id: Uuid) -> String {
    #[derive(diesel::QueryableByName)]
    struct State {
        #[diesel(sql_type = Text)]
        state: String,
    }
    diesel::sql_query("SELECT state FROM harvest_task_queue WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(task_id)
        .get_result::<State>(conn)
        .await
        .expect("read state")
        .state
}
