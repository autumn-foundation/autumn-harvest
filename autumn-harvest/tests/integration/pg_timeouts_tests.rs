#![cfg(feature = "db")]
//! Engine connection timeouts against a real Postgres (issue #1788).
//!
//! * AC1: with every pool connection held, a claim and a heartbeat flush fail
//!   within the pool bound. Before the fix both waited without limit.
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

fn timeouts(pool_ms: u64, session: SessionTimeouts) -> EngineDbTimeouts {
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

    // A BEFORE INSERT trigger sleeps for this execution only.
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

    let session = SessionTimeouts {
        statement: Duration::from_millis(500),
        ..SessionTimeouts::for_role(DbRole::Hot)
    };
    let pool = engine_pool(url.clone(), 1, DbRole::Hot, &timeouts(5_000, session))
        .expect("engine pool builds");
    let mut conn = pool.get().await.expect("engine connection");

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
    let telemetry = Arc::new(
        TelemetryConfig::builder()
            .metrics(metrics as Arc<dyn MetricsRecorder>)
            .build(),
    );
    let registry = Arc::new(HandlerRegistry::with_state_and_telemetry(
        vec![],
        vec![],
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
                worker_heartbeat_interval: Duration::from_secs(5),
                build_id: String::new(),
                deployment_name: None,
                workflow_cache_size: 100,
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
            Uuid::new_v4(),
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
