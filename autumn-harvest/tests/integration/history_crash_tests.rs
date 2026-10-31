#![cfg(feature = "db")]
//! Client guarantees under crashes, checked as histories (issue #1829).
//!
//! Each test drives concurrent clients against a real Postgres while it
//! injects two kinds of crash:
//!
//! - A client crash. A timeout drops the request future at a random moment,
//!   so the client never learns the outcome.
//! - A server-side crash of the session. A killer task calls
//!   `pg_terminate_backend` on the client connections at random moments.
//!
//! The test records each request as a Jepsen operation. A request with no
//! clear outcome is an `info` operation, which may or may not have taken
//! effect. After the crashes stop and every client closes its connection,
//! the test reads the final state. [`crate::history_checker`] then checks
//! that one real-time order of the history satisfies the guarantee.
//!
//! - Start idempotency (issue #808): one idempotency key creates at most one
//!   run, and every response names that run.
//! - Exactly-once schedule fires (issue #350): a schedule slot gets exactly
//!   one run through crashes and recovery.
//!
//! Set `HISTORY_SEED` to replay the random choices of a run. Each failure
//! prints its seed. The thread timing can still differ, so a replay is not
//! exact.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use autumn_harvest::execution::{
    IdempotentStartOutcome, start_or_load_workflow_execution_idempotent,
};
use autumn_harvest::prelude::*;
use autumn_harvest::types::{ExecutionId, ShardId, WorkflowIdReusePolicy};
use autumn_harvest::worker::{DbPool, HandlerRegistry};
use autumn_harvest::{DagCatalog, SchedulerMonitor, StartWorkflowParams, tick_once};
use diesel::sql_types::{Text, Uuid as SqlUuid};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use crate::history_checker::{
    ExactlyOnceFire, FireInput, FireOutput, Recorder, StartIdempotency, StartInput, StartOutput,
    assert_linearizable,
};

/// The schedule workload. It completes at once.
#[workflow]
#[allow(clippy::unused_async)] // the #[workflow] macro requires an `async fn` handler.
async fn history_noop(
    _ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = input;
    Ok(serde_json::json!("ok"))
}

/// A database that this test alone uses. A scheduler tick fires every due
/// schedule in its database, so a shared database is not safe. With
/// `HARVEST_TEST_DATABASE_URL` set, the test creates a throwaway database on
/// that server and drops it at the end. Otherwise it starts a Postgres 16
/// container.
async fn database() -> (
    String,
    Option<ContainerAsync<Postgres>>,
    Option<crate::throwaway_db::ThrowawayDb>,
) {
    use testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    if let Some(db) = crate::throwaway_db::ThrowawayDb::create("harvest_history").await {
        return (db.url(), None, Some(db));
    }
    let container = Postgres::default()
        .with_tag("16")
        .start()
        .await
        .expect("postgres start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    conn.batch_execute(&autumn_harvest::test_init_sql())
        .await
        .expect("migration");
    (url, Some(container), None)
}

/// The seed for this run: `HISTORY_SEED`, or a fresh random seed.
fn seed() -> u64 {
    std::env::var("HISTORY_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(rand::random)
}

/// `url` with an `application_name` that the killer task matches on.
fn tagged(url: &str, app: &str) -> String {
    crate::throwaway_db::with_application_name(url, app)
}

/// Terminate one busy backend of `app` at random moments until `stop` is
/// set. A pause before each strike is a random value in `pause_ms`. Only a
/// busy backend is a target, so each kill lands in the middle of a request.
/// Returns how many backends it terminated.
fn spawn_killer(
    url: String,
    app: &'static str,
    seed: u64,
    pause_ms: std::ops::Range<u64>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<i64> {
    tokio::spawn(async move {
        #[derive(diesel::QueryableByName)]
        struct Killed {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        let mut rng = StdRng::seed_from_u64(seed);
        let mut conn = AsyncPgConnection::establish(&url).await.expect("killer");
        let mut total = 0;
        while !AtomicBool::load(&stop, Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(rng.gen_range(pause_ms.clone()))).await;
            total += diesel::sql_query(
                // Count only a `true` result. A session that ends before
                // the call returns `false`, and no kill landed.
                "SELECT COUNT(*) FILTER (WHERE killed)::bigint AS n FROM ( \
                   SELECT pg_terminate_backend(pid) AS killed FROM ( \
                     SELECT pid FROM pg_stat_activity \
                     WHERE application_name = $1 AND state IS DISTINCT FROM 'idle' \
                     ORDER BY random() LIMIT 1) busy) strike",
            )
            .bind::<Text, _>(app)
            .get_result::<Killed>(&mut conn)
            .await
            .expect("terminate backends")
            .n;
        }
        total
    })
}

/// Wait until no backend of `app` is left. A request that a client dropped
/// can still run on the server, so a read must wait for it.
///
/// First the busy backends must finish. Then the test terminates the idle
/// ones, because a pooled session can still hold an unread statement in its
/// socket. A terminated backend never runs that statement.
async fn wait_for_quiescence(url: &str, app: &str) {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    let mut conn = AsyncPgConnection::establish(url).await.expect("connect");
    let busy = "SELECT COUNT(*)::bigint AS n FROM pg_stat_activity \
                WHERE application_name = $1 AND state IS DISTINCT FROM 'idle'";
    let close_idle = "SELECT COUNT(pg_terminate_backend(pid))::bigint AS n \
                      FROM pg_stat_activity WHERE application_name = $1";
    let left = "SELECT COUNT(*)::bigint AS n FROM pg_stat_activity \
                WHERE application_name = $1";
    for sql in [busy, close_idle, left] {
        for attempt in 0.. {
            let n = diesel::sql_query(sql)
                .bind::<Text, _>(app)
                .get_result::<Count>(&mut conn)
                .await
                .expect("query backends")
                .n;
            if sql == close_idle || n == 0 {
                break;
            }
            assert!(
                attempt < 500,
                "backends of {app} are still open after 10 seconds"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The ids of every run that `sql` selects for `key`.
async fn read_runs(conn: &mut AsyncPgConnection, sql: &str, key: &str) -> Vec<Uuid> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
    }
    let mut ids: Vec<Uuid> = diesel::sql_query(sql)
        .bind::<Text, _>(key)
        .load::<Row>(conn)
        .await
        .expect("read runs")
        .into_iter()
        .map(|r| r.id)
        .collect();
    ids.sort();
    ids
}

fn start_params<'a>(name: &'a str, wf_id: &'a str, exec: ExecutionId) -> StartWorkflowParams<'a> {
    StartWorkflowParams {
        workflow_name: name,
        workflow_id: wf_id,
        exec_id: exec,
        input: serde_json::json!(null).into(),
        parent_id: None,
        queue_name: "history",
        execution_timeout: None,
        memo: None,
        search_attrs: None,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
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
        start_source: autumn_harvest::StartSource::Api,
        start_source_ref: None,
        started_by: None,
        fairness_key: None,
        tenant: None,
    }
}

/// Counts of each outcome kind, for the anti-vacuity checks.
fn outcome_counts<I: Clone + std::fmt::Debug, O: Clone + std::fmt::Debug>(
    history: &[crate::history_checker::Operation<I, O>],
) -> (usize, usize) {
    use crate::history_checker::Outcome;
    let ok = history
        .iter()
        .filter(|op| matches!(op.outcome, Some(Outcome::Ok(_))))
        .count();
    let info = history
        .iter()
        .filter(|op| matches!(op.outcome, Some(Outcome::Info)))
        .count();
    (ok, info)
}

/// Concurrent idempotent starts under client and session crashes. Each
/// request uses a new workflow id, so only the idempotency key can dedup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// One linear scenario: crashing clients, a final read, then the check.
#[allow(clippy::too_many_lines)]
async fn start_idempotency_history_is_linearizable_under_crashes() {
    const KEYS: usize = 3;
    const CLIENTS: usize = 4;
    const REQUESTS: usize = 8;
    const APP: &str = "hist-start";
    // The fewest completed operations, final reads included, that must show a
    // dedup. Healthy runs complete far more and dedup 20 to 40 times.
    const MIN_OK_FOR_DEDUP: usize = 20;

    let seed = seed();
    let (url, _container, _db) = database().await;
    let run = Uuid::new_v4().simple().to_string();
    let history = Arc::new(Recorder::<StartInput, StartOutput>::new());
    let stop = Arc::new(AtomicBool::new(false));
    let killer_task = spawn_killer(url.clone(), APP, seed, 3..15, Arc::clone(&stop));

    let mut clients = Vec::new();
    for process in 0..KEYS * CLIENTS {
        let (url, run, history) = (tagged(&url, APP), run.clone(), Arc::clone(&history));
        let mut rng = StdRng::seed_from_u64(seed ^ (process as u64 + 1));
        clients.push(tokio::spawn(async move {
            let key = format!("key-{}", process % KEYS);
            let name = format!("hist_{run}_{}", process % KEYS);
            let mut conn = None;
            for request in 0..REQUESTS {
                for attempt in 0.. {
                    if conn.is_some() {
                        break;
                    }
                    assert!(attempt < 400, "client {process} cannot connect");
                    conn = AsyncPgConnection::establish(&url).await.ok();
                    if conn.is_none() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                let Some(c) = conn.as_mut() else { continue };
                let exec = ExecutionId::new_for_shard(ShardId::new(0));
                let wf_id = format!("{name}-{process}-{request}");
                let op = history.invoke(
                    process,
                    key.clone(),
                    StartInput::Start {
                        candidate: exec.as_uuid(),
                    },
                );
                let budget = Duration::from_millis(rng.gen_range(1..250));
                let out = tokio::time::timeout(
                    budget,
                    start_or_load_workflow_execution_idempotent(
                        c,
                        start_params(&name, &wf_id, exec),
                        &key,
                        86_400.0,
                        None,
                        None,
                    ),
                )
                .await;
                match out {
                    Ok(Ok(IdempotentStartOutcome::Started(s))) => {
                        history.ok(op, StartOutput::Started(s.exec_id.as_uuid()));
                    }
                    Ok(Ok(IdempotentStartOutcome::Deduplicated { exec_id, .. })) => {
                        history.ok(op, StartOutput::Deduplicated(exec_id.as_uuid()));
                    }
                    // A timeout or an error leaves the outcome unknown. The
                    // connection may be dead or mid-transaction, so drop it.
                    Ok(Err(_)) | Err(_) => {
                        history.info(op);
                        conn = None;
                    }
                }
            }
        }));
    }
    for client in clients {
        client.await.expect("client task");
    }
    stop.store(true, Ordering::SeqCst);
    let terminated = killer_task.await.expect("killer task");
    wait_for_quiescence(&url, APP).await;
    // Nothing a crashed request started can still run, so bound them.
    history.bound_open_infos();

    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    for k in 0..KEYS {
        let key = format!("key-{k}");
        let op = history.invoke(usize::MAX, key, StartInput::Read);
        let ids = read_runs(
            &mut conn,
            "SELECT id FROM harvest_workflow_executions WHERE workflow_name = $1",
            &format!("hist_{run}_{k}"),
        )
        .await;
        history.ok(op, StartOutput::Read(ids));
    }

    let history = history.snapshot();
    let (ok, info) = outcome_counts(&history);
    let context = format!("seed {seed}, {terminated} backends terminated, {ok} ok, {info} info");
    // Check the guarantee first, so a real violation reports as one.
    assert_linearizable(&StartIdempotency, &history, &context);
    assert!(
        info > 0,
        "no request crashed, so the run proves nothing; {context}"
    );
    assert!(
        terminated > 0,
        "no backend was terminated mid-request; {context}"
    );
    let dedups = history
        .iter()
        .filter(|op| {
            matches!(
                op.outcome,
                Some(crate::history_checker::Outcome::Ok(
                    StartOutput::Deduplicated(_)
                ))
            )
        })
        .count();
    // A run where the killer crashes almost every request may complete too few
    // of them to see a dedup. Such a run is weak, not wrong.
    assert!(
        dedups > 0 || ok < MIN_OK_FOR_DEDUP,
        "no request deduplicated, so the run proves little; {context}"
    );
    eprintln!("start idempotency history: {context}, {dedups} dedups");
}

/// Insert a schedule whose one slot is due now. The next slot is a minute
/// away, so each schedule fires exactly one slot during the test.
async fn insert_due_schedule(conn: &mut AsyncPgConnection, wf_name: &str) -> Uuid {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use diesel::ExpressionMethods;
    let id = Uuid::new_v4();
    diesel::insert_into(dsl::harvest_schedules)
        .values((
            dsl::id.eq(id),
            dsl::workflow_name.eq(wf_name),
            dsl::schedule_expr.eq("interval:60"),
            dsl::timezone.eq("UTC"),
            dsl::catchup.eq(false),
            dsl::max_active_runs.eq(10),
            dsl::is_paused.eq(false),
            dsl::next_run_at.eq(chrono::Utc::now() - chrono::Duration::seconds(5)),
            dsl::jitter_secs.eq(0_i64),
            dsl::overlap_policy.eq("skip"),
            dsl::buffered_runs.eq(serde_json::json!([])),
            dsl::buffer_all_max.eq(100),
            dsl::skip_policy.eq("skip"),
        ))
        .execute(conn)
        .await
        .expect("insert schedule");
    id
}

fn make_pool(url: &str) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(2)
        .build()
        .expect("pool")
}

/// Let every fire claim lapse, as if the 30-second claim TTL had passed.
async fn expire_fire_claims(conn: &mut AsyncPgConnection, schedules: &[Uuid]) {
    for id in schedules {
        diesel::sql_query(
            "UPDATE harvest_schedules SET fire_claimed_until = NOW() - INTERVAL '1 minute' \
             WHERE id = $1 AND fire_claim_token IS NOT NULL",
        )
        .bind::<SqlUuid, _>(*id)
        .execute(conn)
        .await
        .expect("expire claim");
    }
}

const SCHEDULE_RUNS_SQL: &str =
    "SELECT id FROM harvest_workflow_executions WHERE schedule_id::text = $1";

/// Scheduler replicas tick at once while they crash. Then the claims lapse,
/// one healthy tick recovers, and each slot must have exactly one run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
// One linear scenario: crash rounds, recovery, then the check. Splitting it
// would scatter the shared history across helpers.
#[allow(clippy::too_many_lines)]
async fn schedule_fire_history_is_exactly_once_under_crashes() {
    const SCHEDULES: usize = 8;
    const REPLICAS: usize = 6;
    const ROUNDS: usize = 4;
    const APP: &str = "hist-sched";

    let seed = seed();
    let (url, _container, _db) = database().await;
    let mut conn = AsyncPgConnection::establish(&url).await.expect("connect");
    let mut schedules = Vec::new();
    let run = Uuid::new_v4().simple().to_string();
    let registry = Arc::new(HandlerRegistry::new(vec![history_noop_info()], vec![]));
    let history = Arc::new(Recorder::<FireInput, FireOutput>::new());
    let mut rng = StdRng::seed_from_u64(seed);
    let mut terminated = 0;
    let mut fired_under_crashes = 0;

    for round in 0..ROUNDS {
        // Fresh due slots each round, so each round races its own fires.
        for i in 0..SCHEDULES {
            let name = format!("hist_sched_{run}_{round}_{i}");
            schedules.push(insert_due_schedule(&mut conn, &name).await);
        }
        let keys: Arc<Vec<String>> = Arc::new(schedules.iter().map(Uuid::to_string).collect());
        let stop = Arc::new(AtomicBool::new(false));
        // Ticks are short, so this killer strikes more often.
        let killer = spawn_killer(
            url.clone(),
            APP,
            seed ^ round as u64,
            10..40,
            Arc::clone(&stop),
        );
        let mut replicas = Vec::new();
        for replica in 0..REPLICAS {
            let pool = make_pool(&tagged(&url, APP));
            let (registry, history, keys) = (
                Arc::clone(&registry),
                Arc::clone(&history),
                Arc::clone(&keys),
            );
            // Replica 0 always crashes early. The others tick again and again
            // until the budget ends, so a tick after a kill can still fire.
            // The tick that the budget cuts off is a crash.
            let budget = if replica == 0 {
                rng.gen_range(1..20)
            } else {
                rng.gen_range(300..1500)
            };
            let deadline = tokio::time::Instant::now() + Duration::from_millis(budget);
            replicas.push(tokio::spawn(async move {
                loop {
                    let ops: Vec<_> = keys
                        .iter()
                        .map(|k| history.invoke(replica, k.clone(), FireInput::Fire))
                        .collect();
                    let tick = tick_once(
                        pool.clone(),
                        Arc::clone(&registry),
                        Arc::new(DagCatalog::default()),
                        Arc::new(vec![]),
                        SchedulerMonitor::offline(),
                    );
                    let ok = matches!(tokio::time::timeout_at(deadline, tick).await, Ok(Ok(())));
                    for op in ops {
                        if ok {
                            history.ok(op, FireOutput::Ticked);
                        } else {
                            history.info(op);
                        }
                    }
                    if !ok || tokio::time::Instant::now() >= deadline {
                        break;
                    }
                }
            }));
        }
        for replica in replicas {
            replica.await.expect("replica task");
        }
        stop.store(true, Ordering::SeqCst);
        terminated += killer.await.expect("killer task");
        wait_for_quiescence(&url, APP).await;
        // Nothing a crashed tick started can still run, so bound them.
        history.bound_open_infos();

        for (n, key) in keys.iter().enumerate() {
            let op = history.invoke(usize::MAX, key.clone(), FireInput::Read);
            let ids = read_runs(&mut conn, SCHEDULE_RUNS_SQL, key).await;
            let fresh = n >= keys.len() - SCHEDULES;
            fired_under_crashes += usize::from(fresh && !ids.is_empty());
            history.ok(op, FireOutput::Read(ids));
        }
        expire_fire_claims(&mut conn, &schedules).await;
    }

    // Recovery: no crashes, the claims have lapsed, one healthy tick.
    let keys: Vec<String> = schedules.iter().map(Uuid::to_string).collect();
    let ops: Vec<_> = keys
        .iter()
        .map(|k| history.invoke(usize::MAX, k.clone(), FireInput::Fire))
        .collect();
    tick_once(
        make_pool(&url),
        registry,
        Arc::new(DagCatalog::default()),
        Arc::new(vec![]),
        SchedulerMonitor::offline(),
    )
    .await
    .expect("recovery tick");
    for op in ops {
        history.ok(op, FireOutput::Ticked);
    }
    for key in &keys {
        let op = history.invoke(usize::MAX, key.clone(), FireInput::FinalRead);
        let ids = read_runs(&mut conn, SCHEDULE_RUNS_SQL, key).await;
        history.ok(op, FireOutput::Read(ids));
    }

    let history = history.snapshot();
    let (ok, info) = outcome_counts(&history);
    let context = format!(
        "seed {seed}, {terminated} backends terminated, {ok} ok, {info} info, \
         {fired_under_crashes} slots fired in their crash round"
    );
    assert_linearizable(&ExactlyOnceFire, &history, &context);
    assert!(
        fired_under_crashes > 0,
        "no slot fired during the crash rounds, so only recovery fired; {context}"
    );
    assert!(
        info > 0,
        "no tick crashed, so the run proves nothing; {context}"
    );
    assert!(
        terminated > 0,
        "no backend was terminated mid-tick; {context}"
    );
    eprintln!("schedule fire history: {context}");
}
