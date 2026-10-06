//! Process-, database- and network-level crash-recovery tests (issue #1801).
//!
//! The chaos harness KILL is a panic in one tokio task. These tests inject
//! faults below the engine instead. The faults are a killed backend, a
//! Postgres restart or pause, a slow or partitioned network, and a SIGKILL.
//!
//! Each test runs real workers against a Postgres container. All worker
//! traffic goes through a toxiproxy container, so a fault never changes the
//! worker URL. The test reads state through a second proxy that has no
//! toxics. Each test then checks the sweep oracle, [`assert_converged`]:
//! every workflow `COMPLETED`, no stranded task, and one terminal event per
//! execution.
//!
//! Each test also asserts proof that its fault landed. A fault that does not
//! land fails the test, so a healthy run cannot pass in its place.
//!
//! These tests always start their own containers. A restart or a pause cannot
//! target a shared `HARVEST_TEST_DATABASE_URL` database.

// `FaultDb` holds the `DB_BODY_SERIAL` guard for the whole test. Serial runs
// keep CPU contention low, because the 1 s lease TTL is sensitive to a
// starved runtime.
#![allow(clippy::significant_drop_tightening)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};
use diesel::OptionalExtension;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::{ContainerAsync, GenericImage};
use testcontainers_modules::postgres::Postgres;

use super::{
    CountRow, DB_BODY_SERIAL, assert_converged, base_params, chaos_noop_info, connect, event_count,
    exec_state,
};

/// The worker heartbeat interval. The stale threshold, the "lease TTL", is
/// twice this value: 1 s.
const HEARTBEAT: Duration = Duration::from_millis(500);

/// The worker settings that a test changes.
#[derive(Clone, Copy)]
struct Tuning {
    heartbeat: Duration,
    /// The local-activity cap. It raises the 10 s workflow-task budget to
    /// its own value. These tests run no local activity.
    local_activity_cap: Duration,
}

/// The default tuning. The 5 s cap keeps the workflow-task budget at 10 s.
/// The stuck-task backstop is then 4 × 10 s + 30 s = 70 s, inside
/// [`CONVERGE_DEADLINE`].
const FAST: Tuning = Tuning {
    heartbeat: HEARTBEAT,
    local_activity_cap: Duration::from_secs(5),
};

/// The tuning of the latency test.
///
/// A heartbeat tick must take less than one interval. At [`HEARTBEAT`], the
/// latency toxic breaks that rule, and bug #1879 then quarantines healthy
/// work. A decision cycle also takes more than 10 s at this latency. So the
/// test keeps the default 60 s cap, which gives a 60 s budget.
const SLOW_NETWORK: Tuning = Tuning {
    heartbeat: Duration::from_secs(2),
    local_activity_cap: Duration::from_secs(60),
};

/// A fault that must outlast the lease TTL lasts this long.
const PAST_LEASE_TTL: Duration = Duration::from_secs(4);

/// The upper bound on recovery after a fault is healed.
const CONVERGE_DEADLINE: Duration = Duration::from_secs(150);

/// The upper bound on a fault rendezvous, such as a blocked backend.
const RENDEZVOUS_DEADLINE: Duration = Duration::from_secs(60);

/// The child worker reads its database URL from this variable.
const CHILD_DB_URL_VAR: &str = "HARVEST_INFRA_CHILD_DB_URL";

/// The libtest name of the child worker entry point.
const CHILD_ENTRY: &str = "chaos_tests::infra_faults::sigkill_child_worker_entry";

/// The child worker uses this worker id.
const CHILD_WORKER_ID: &str = "infra-child-worker";

/// The partition test sets this flag to release the held first attempts.
static RELEASE_FIRST_ATTEMPTS: AtomicBool = AtomicBool::new(false);

/// The partition test sets this flag to release the held second attempts.
static RELEASE_SECOND_ATTEMPTS: AtomicBool = AtomicBool::new(false);

/// The number of first attempts that entered the hold.
static HELD_FIRST_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// The number of held first attempts that returned after the release.
static LATE_RETURNS: AtomicUsize = AtomicUsize::new(0);

// Fully qualified: the blanket `RunQueryDsl` puts a `load` method on every
// type, and that method wins method resolution.
fn released(flag: &AtomicBool) -> bool {
    AtomicBool::load(flag, Ordering::SeqCst)
}

fn count(counter: &AtomicUsize) -> usize {
    AtomicUsize::load(counter, Ordering::SeqCst)
}

// ── Workload ────────────────────────────────────────────────────────────────

/// Sleeps for `input.sleep_ms`. In the child worker process it sleeps for
/// ten minutes, so the parent can kill the child while the activity runs.
/// With `input.hold_attempts`, attempt 1 waits for [`RELEASE_FIRST_ATTEMPTS`],
/// and attempt 2 waits for [`RELEASE_SECOND_ATTEMPTS`].
///
/// A result write that fails after its repeats gives the claim back, so the
/// activity runs again. When the give-back also fails, only `start_to_close`
/// recovers the task. The retry policy then starts a new attempt (issue
/// #1870). The timeout is long, so a pause or a partition does not reach it.
#[activity(
    start_to_close = "30s",
    retry = autumn_harvest::policy::RetryPolicy::fixed(5, Duration::from_millis(200))
)]
async fn infra_step(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    if std::env::var_os(CHILD_DB_URL_VAR).is_some() {
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
    let attempt = ctx.info().attempt;
    if input["hold_attempts"].as_bool() == Some(true) && attempt <= 2 {
        let flag = if attempt == 1 {
            &RELEASE_FIRST_ATTEMPTS
        } else {
            &RELEASE_SECOND_ATTEMPTS
        };
        if attempt == 1 {
            HELD_FIRST_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        }
        // The bound stops a held attempt that a failed test never releases.
        let deadline = Instant::now() + CONVERGE_DEADLINE;
        while !released(flag) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if attempt == 1 && released(flag) {
            LATE_RETURNS.fetch_add(1, Ordering::SeqCst);
        }
    }
    let ms = input["sleep_ms"].as_u64().unwrap_or(0);
    tokio::time::sleep(Duration::from_millis(ms)).await;
    Ok(input)
}

/// Runs one [`infra_step`] activity, then completes.
#[workflow]
async fn infra_activity_wf(
    ctx: &WorkflowContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let out: serde_json::Value = ctx
        .execute_activity(&infra_step_info(), input)
        .await
        .map_err(|e| e.to_string())?;
    Ok(out)
}

fn registry() -> Arc<HandlerRegistry> {
    Arc::new(HandlerRegistry::new(
        vec![infra_activity_wf_info(), chaos_noop_info()],
        vec![infra_step_info()],
    ))
}

// ── Fixture ─────────────────────────────────────────────────────────────────

/// Postgres behind toxiproxy, on a private docker network.
///
/// Workers connect through the `worker` proxy. The test connects through the
/// `admin` proxy, which never gets a toxic.
struct FaultDb {
    pg: ContainerAsync<Postgres>,
    _toxiproxy: ContainerAsync<GenericImage>,
    api: String,
    worker_url: String,
    admin_url: String,
    // The last field drops last, so the lock outlives the containers.
    _body: tokio::sync::MutexGuard<'static, ()>,
}

impl FaultDb {
    async fn start() -> Self {
        use testcontainers::ImageExt;
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::AsyncRunner;

        let body = DB_BODY_SERIAL.lock().await;
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let network = format!("harvest-infra-{tag}");
        let pg_name = format!("harvest-infra-pg-{tag}");
        let pg = Postgres::default()
            .with_tag("16")
            .with_network(&network)
            .with_container_name(&pg_name)
            .start()
            .await
            .expect("postgres start");
        let toxiproxy = GenericImage::new("ghcr.io/shopify/toxiproxy", "2.9.0")
            .with_exposed_port(TOXIPROXY_API_PORT.tcp())
            .with_exposed_port(WORKER_PROXY_PORT.tcp())
            .with_exposed_port(ADMIN_PROXY_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Starting Toxiproxy HTTP server"))
            .with_network(&network)
            .start()
            .await
            .expect("toxiproxy start");

        let host = toxiproxy.get_host().await.expect("toxiproxy host");
        let port = |p: u16| {
            let toxiproxy = &toxiproxy;
            async move {
                toxiproxy
                    .get_host_port_ipv4(p.tcp())
                    .await
                    .expect("toxiproxy port")
            }
        };
        let api = format!("{host}:{}", port(TOXIPROXY_API_PORT).await);
        for (name, listen) in [("worker", WORKER_PROXY_PORT), ("admin", ADMIN_PROXY_PORT)] {
            let proxy = serde_json::json!({
                "name": name,
                "listen": format!("0.0.0.0:{listen}"),
                "upstream": format!("{pg_name}:5432"),
            });
            toxiproxy_request(&api, "POST", "/proxies", &proxy.to_string()).await;
        }
        let url = |p: u16| format!("postgresql://postgres:postgres@{host}:{p}/postgres");
        let worker_url = url(port(WORKER_PROXY_PORT).await);
        let admin_url = url(port(ADMIN_PROXY_PORT).await);

        connect(&admin_url)
            .await
            .batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migration");
        Self {
            pg,
            _toxiproxy: toxiproxy,
            api,
            worker_url,
            admin_url,
            _body: body,
        }
    }

    /// Kill Postgres with no grace period, then start it again.
    async fn crash_restart_postgres(&self) {
        self.pg
            .stop_with_timeout(Some(0))
            .await
            .expect("kill postgres");
        self.pg.start().await.expect("start postgres again");
        wait_for_db(&self.admin_url).await;
    }

    /// Make the server end a session that stays idle in a transaction for
    /// `secs` seconds.
    async fn limit_idle_in_transaction(&self, secs: u32) {
        // `ALTER SYSTEM` cannot run in a transaction block. A batch is one
        // transaction, so each statement has its own batch.
        let mut conn = connect(&self.admin_url).await;
        conn.batch_execute(&format!(
            "ALTER SYSTEM SET idle_in_transaction_session_timeout = '{secs}s'"
        ))
        .await
        .expect("set idle_in_transaction_session_timeout");
        conn.batch_execute("SELECT pg_reload_conf()")
            .await
            .expect("reload the server configuration");
    }

    async fn pause_postgres(&self) {
        self.pg.pause().await.expect("pause postgres");
    }

    async fn unpause_postgres(&self) {
        self.pg.unpause().await.expect("unpause postgres");
    }

    /// Add a toxic to the `worker` proxy.
    async fn add_toxic(&self, name: &str, kind: &str, stream: &str, attrs: serde_json::Value) {
        let toxic = serde_json::json!({
            "name": name,
            "type": kind,
            "stream": stream,
            "toxicity": 1.0,
            "attributes": attrs,
        });
        toxiproxy_request(
            &self.api,
            "POST",
            "/proxies/worker/toxics",
            &toxic.to_string(),
        )
        .await;
    }

    async fn remove_toxic(&self, name: &str) {
        let path = format!("/proxies/worker/toxics/{name}");
        toxiproxy_request(&self.api, "DELETE", &path, "").await;
    }
}

const TOXIPROXY_API_PORT: u16 = 8474;
const WORKER_PROXY_PORT: u16 = 8666;
const ADMIN_PROXY_PORT: u16 = 8667;

/// Send one request to the toxiproxy HTTP API and assert a 2xx status.
async fn toxiproxy_request(api: &str, method: &str, path: &str, body: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(api)
        .await
        .expect("connect to the toxiproxy api");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: toxiproxy\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write to the toxiproxy api");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read from the toxiproxy api");
    let status = response.split_whitespace().nth(1).unwrap_or_default();
    assert!(
        status.starts_with('2'),
        "toxiproxy {method} {path} failed: {response}"
    );
}

/// Wait until `url` accepts a connection and a query.
async fn wait_for_db(url: &str) {
    use diesel_async::AsyncConnection;

    let deadline = Instant::now() + RENDEZVOUS_DEADLINE;
    loop {
        if let Ok(mut conn) = AsyncPgConnection::establish(url).await
            && conn.batch_execute("SELECT 1").await.is_ok()
        {
            return;
        }
        assert!(Instant::now() < deadline, "postgres did not come back");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The write whose COMMIT the blocking trigger holds.
#[derive(Clone, Copy, Debug)]
enum CommitSite {
    /// An `ActivityCompleted` event append.
    Append,
    /// A `WorkflowCompleted` event append, the terminal write.
    Complete,
    /// The first task claim, a `PENDING` to `RUNNING` update. That is the
    /// claim of the first workflow task.
    Claim,
}

/// What happens to the held COMMIT.
#[derive(Clone, Copy, Debug)]
enum CommitFate {
    /// The backend dies before the commit. The transaction rolls back.
    RolledBack,
    /// The commit lands, but the worker never gets the acknowledgement.
    AckLost,
}

/// Holds the advisory lock that blocks the trigger, on its own connection.
struct CommitBlocker {
    conn: AsyncPgConnection,
}

impl CommitBlocker {
    /// Install a deferred trigger at `site` and take the lock that blocks it.
    async fn install(admin_url: &str, site: CommitSite) -> Self {
        let (event, table, when) = match site {
            CommitSite::Append => (
                "INSERT",
                "harvest_events",
                "NEW.event_type = 'ActivityCompleted'",
            ),
            CommitSite::Complete => (
                "INSERT",
                "harvest_events",
                "NEW.event_type = 'WorkflowCompleted'",
            ),
            CommitSite::Claim => (
                "UPDATE",
                "harvest_task_queue",
                "OLD.state = 'PENDING' AND NEW.state = 'RUNNING'",
            ),
        };
        // A deferred constraint trigger runs inside COMMIT. The shared lock
        // waits while this connection holds the exclusive lock.
        let mut conn = connect(admin_url).await;
        conn.batch_execute(&format!(
            "CREATE FUNCTION harvest_infra_block() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM pg_advisory_xact_lock_shared({BLOCK_LOCK_KEY}); RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER harvest_infra_block AFTER {event} ON {table} \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN ({when}) \
             EXECUTE FUNCTION harvest_infra_block(); \
             SELECT pg_advisory_lock({BLOCK_LOCK_KEY});"
        ))
        .await
        .expect("install the commit-blocking trigger");
        Self { conn }
    }

    /// Wait for a backend to block in the trigger, and return its pid.
    async fn wait_for_blocked_backend(&mut self) -> i32 {
        let deadline = Instant::now() + RENDEZVOUS_DEADLINE;
        loop {
            let waiting: Vec<PidRow> = diesel::sql_query(format!(
                "SELECT pid FROM pg_locks WHERE locktype = 'advisory' \
                 AND classid = 0 AND objid = {BLOCK_LOCK_KEY} AND objsubid = 1 AND NOT granted"
            ))
            .load(&mut self.conn)
            .await
            .expect("find the blocked backend");
            if let Some(row) = waiting.into_iter().next() {
                return row.pid;
            }
            assert!(
                Instant::now() < deadline,
                "no backend blocked in the commit trigger"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Terminate backend `pid`, and wait up to 10 s for it to exit.
    async fn terminate(&mut self, pid: i32) {
        let terminated: BoolRow = diesel::sql_query("SELECT pg_terminate_backend($1, 10000) AS ok")
            .bind::<diesel::sql_types::Integer, _>(pid)
            .get_result(&mut self.conn)
            .await
            .expect("terminate the blocked backend");
        assert!(terminated.ok, "backend {pid} did not exit");
    }

    /// Release the lock. A held COMMIT then continues.
    async fn release(&mut self) {
        self.conn
            .batch_execute(&format!("SELECT pg_advisory_unlock({BLOCK_LOCK_KEY})"))
            .await
            .expect("release the commit-blocking lock");
    }

    /// Wait until backend `pid` is idle, out of a transaction. Its held
    /// COMMIT has then landed.
    async fn wait_until_committed(&mut self, pid: i32) {
        #[derive(diesel::QueryableByName)]
        struct StateRow {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            state: Option<String>,
        }
        let deadline = Instant::now() + RENDEZVOUS_DEADLINE;
        loop {
            let row: Option<StateRow> =
                diesel::sql_query("SELECT state FROM pg_stat_activity WHERE pid = $1")
                    .bind::<diesel::sql_types::Integer, _>(pid)
                    .get_result(&mut self.conn)
                    .await
                    .optional()
                    .expect("read the backend state");
            let state = row.and_then(|r| r.state);
            if state.as_deref() == Some("idle") {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "backend {pid} did not commit; state {state:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// The advisory lock key of the commit-blocking trigger: the issue number.
const BLOCK_LOCK_KEY: i64 = 1801;

#[derive(diesel::QueryableByName)]
struct PidRow {
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pid: i32,
}

#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    ok: bool,
}

/// A worker that runs in this process until the value drops.
struct RunningWorker {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl RunningWorker {
    fn spawn(worker_id: &str, url: &str) -> Self {
        Self::spawn_tuned(worker_id, url, FAST)
    }

    fn spawn_tuned(worker_id: &str, url: &str, tuning: Tuning) -> Self {
        let worker = build_worker(worker_id, tuning);
        let pool = crate::integration_e2e::build_test_pool(url);
        let handle = crate::integration_e2e::spawn_test_worker(Arc::clone(&worker), pool);
        Self { worker, handle }
    }
}

impl Drop for RunningWorker {
    fn drop(&mut self) {
        self.worker.shutdown();
        self.handle.abort();
    }
}

/// A worker with a short heartbeat interval, so the lease TTL is short.
fn build_worker(worker_id: &str, tuning: Tuning) -> Arc<Worker> {
    let mut config =
        crate::integration_e2e::runtime_config(worker_id, 4, 4, Duration::from_secs(10));
    config.worker_heartbeat_interval = tuning.heartbeat;
    config.max_local_activity_start_to_close = tuning.local_activity_cap;
    config.poll_interval = Duration::from_millis(50);
    Arc::new(Worker::new(config, registry()).expect("worker builds"))
}

// ── Probes ──────────────────────────────────────────────────────────────────

/// Start `count` workflows of type `infra_activity_wf`.
async fn start_activity_workload(
    admin_url: &str,
    tag: &str,
    count: usize,
    sleep_ms: u64,
) -> Vec<ExecutionId> {
    let input = serde_json::json!({ "sleep_ms": sleep_ms });
    start_workload(admin_url, "infra_activity_wf", tag, count, &input).await
}

/// Start `count` single-cycle `chaos_noop` workflows.
async fn start_noop_workload(admin_url: &str, count: usize) -> Vec<ExecutionId> {
    start_workload(
        admin_url,
        "chaos_noop",
        "noop",
        count,
        &serde_json::Value::Null,
    )
    .await
}

/// Start `count` workflows of type `workflow_name` with `input`.
async fn start_workload(
    admin_url: &str,
    workflow_name: &'static str,
    tag: &str,
    count: usize,
    input: &serde_json::Value,
) -> Vec<ExecutionId> {
    let mut conn = connect(admin_url).await;
    let mut execs = Vec::with_capacity(count);
    for i in 0..count {
        let exec_id = ExecutionId::new_for_shard(ShardId::new(0));
        // `base_params` needs a `'static` workflow id. A leak is fine in a test.
        let wid: &'static str = Box::leak(format!("infra-{tag}-{i}").into_boxed_str());
        let params = base_params(workflow_name, wid, exec_id, input.clone());
        autumn_harvest::execution::start_or_load_workflow_execution(&mut conn, params, None)
            .await
            .expect("start workflow");
        execs.push(exec_id);
    }
    execs
}

/// Wait until every execution is terminal, then check the outcomes.
///
/// Every workflow must be `COMPLETED` and pass the sweep oracle. An activity
/// workflow must also record exactly one activity terminal event, an
/// `ActivityCompleted`.
async fn converge(admin_url: &str, execs: &[ExecutionId], activity_wf: bool, diag: &str) {
    // The admin proxy never gets a toxic, so one connection serves all polls.
    let deadline = Instant::now() + CONVERGE_DEADLINE;
    let mut conn = connect(admin_url).await;
    let states = loop {
        let mut states = Vec::with_capacity(execs.len());
        for exec_id in execs {
            states.push((*exec_id, exec_state(&mut conn, *exec_id).await));
        }
        if states
            .iter()
            .all(|(_, s)| autumn_harvest::erase::is_terminal_state(s))
        {
            break states;
        }
        if Instant::now() >= deadline {
            let history = history_dump(&mut conn, execs).await;
            panic!("{diag}: not terminal after {CONVERGE_DEADLINE:?}: {states:?}\n{history}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let mut completed = Vec::new();
    for (exec_id, state) in states {
        if state != "COMPLETED" {
            let history = history_dump(&mut conn, execs).await;
            panic!("{diag}: workflow {exec_id:?} ended {state}, not COMPLETED\n{history}");
        }
        completed.push(exec_id);
    }
    let history = history_dump(&mut conn, execs).await;
    assert_converged(admin_url, diag, &completed, &history).await;
    if activity_wf {
        for exec_id in &completed {
            assert!(
                one_activity_result(&mut conn, *exec_id).await,
                "{diag}: workflow {exec_id:?} must record exactly one activity result\n{history}"
            );
        }
    }
}

/// Return true when the history of `exec_id` has one activity terminal
/// event, and that event is an `ActivityCompleted`.
async fn one_activity_result(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> bool {
    diesel::sql_query(
        "SELECT COUNT(*) FILTER (WHERE event_type IN ('ActivityCompleted', 'ActivityFailed', \
                'ActivityTimedOut', 'ActivityCompletedExternally', 'ActivityFailedExternally')) = 1 \
            AND COUNT(*) FILTER (WHERE event_type = 'ActivityCompleted') = 1 AS ok \
         FROM harvest_events WHERE workflow_exec_id = $1",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<BoolRow>(conn)
    .await
    .expect("count activity terminal events")
    .ok
}

/// Return true when the activity of `exec_id` ran once. It has one activity
/// task at attempt 1 and one `ActivityStarted` event.
async fn one_activity_attempt(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> bool {
    diesel::sql_query(
        "SELECT (SELECT COUNT(*) = 1 AND MAX(attempt) = 1 FROM harvest_task_queue \
                 WHERE workflow_exec_id = $1 AND task_type = 'activity') \
            AND (SELECT COUNT(*) = 1 FROM harvest_events \
                 WHERE workflow_exec_id = $1 AND event_type = 'ActivityStarted') AS ok",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .get_result::<BoolRow>(conn)
    .await
    .expect("count activity attempts")
    .ok
}

/// Render the events and tasks of `execs` for a failure message.
async fn history_dump(conn: &mut AsyncPgConnection, execs: &[ExecutionId]) -> String {
    #[derive(diesel::QueryableByName)]
    struct Line {
        #[diesel(sql_type = diesel::sql_types::Text)]
        line: String,
    }
    let ids: Vec<uuid::Uuid> = execs.iter().map(ExecutionId::as_uuid).collect();
    let lines: Vec<Line> = diesel::sql_query(
        "SELECT format('event %s #%s %s %s', workflow_exec_id, event_id, event_type, \
                left(event_data::text, 300)) AS line \
         FROM harvest_events WHERE workflow_exec_id = ANY($1) \
         UNION ALL \
         SELECT format('task %s %s %s worker=%s attempt=%s strikes=%s error=%s', \
                workflow_exec_id, task_type, state, worker_id, attempt, crash_strikes, \
                left(error, 300)) \
         FROM harvest_task_queue WHERE workflow_exec_id = ANY($1)",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(ids)
    .load(conn)
    .await
    .unwrap_or_default();
    lines
        .into_iter()
        .map(|l| l.line)
        .collect::<Vec<_>>()
        .join("\n")
}

/// Wait until at least `min` activity tasks are `RUNNING`, and return their
/// executions. When `worker_id` is set, each task must belong to that worker.
async fn wait_for_running_activities(
    admin_url: &str,
    worker_id: Option<&str>,
    min: usize,
) -> Vec<ExecutionId> {
    let deadline = Instant::now() + RENDEZVOUS_DEADLINE;
    let mut conn = connect(admin_url).await;
    loop {
        let running = running_activity_execs(&mut conn, worker_id).await;
        if running.len() >= min {
            return running;
        }
        assert!(
            Instant::now() < deadline,
            "fewer than {min} activities started running (worker {worker_id:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The executions that have a `RUNNING` activity task. When `worker_id` is
/// set, the task must belong to that worker.
async fn running_activity_execs(
    conn: &mut AsyncPgConnection,
    worker_id: Option<&str>,
) -> Vec<ExecutionId> {
    #[derive(diesel::QueryableByName)]
    struct ExecRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        workflow_exec_id: uuid::Uuid,
    }
    let rows: Vec<ExecRow> = diesel::sql_query(
        "SELECT workflow_exec_id FROM harvest_task_queue \
         WHERE task_type = 'activity' AND state = 'RUNNING' \
           AND ($1::text IS NULL OR worker_id = $1)",
    )
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(worker_id)
    .load(conn)
    .await
    .expect("list running activities");
    rows.into_iter()
        .map(|r| ExecutionId::from_uuid(r.workflow_exec_id))
        .collect()
}

/// Wait until `counter` reaches `target`.
async fn wait_for_count(counter: &AtomicUsize, target: usize, what: &str) {
    let deadline = Instant::now() + RENDEZVOUS_DEADLINE;
    while count(counter) < target {
        assert!(
            Instant::now() < deadline,
            "only {} of {target} {what}",
            count(counter)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Count tasks of `execs` that the orphan reclaimer requeued at least once.
async fn reclaimed_tasks(admin_url: &str, execs: &[ExecutionId]) -> i64 {
    let ids: Vec<uuid::Uuid> = execs.iter().map(ExecutionId::as_uuid).collect();
    let mut conn = connect(admin_url).await;
    diesel::sql_query(
        "SELECT COUNT(*)::bigint AS n FROM harvest_task_queue \
         WHERE workflow_exec_id = ANY($1) AND crash_strikes > 0",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Uuid>, _>(ids)
    .get_result::<CountRow>(&mut conn)
    .await
    .expect("count reclaimed tasks")
    .n
}

// ── Scenario 1: pg_terminate_backend mid-commit ─────────────────────────────

/// Kill the worker's backend while a deferred trigger holds its COMMIT.
///
/// With [`CommitFate::RolledBack`], the backend dies before the commit. With
/// [`CommitFate::AckLost`], a downstream blackhole drops the replies first.
/// The commit then lands, and the backend dies before the worker reads the
/// reply. Either way, the worker sees a dropped connection.
async fn terminate_backend_in_commit(site: CommitSite, fate: CommitFate) {
    let db = FaultDb::start().await;
    let mut blocker = CommitBlocker::install(&db.admin_url, site).await;
    let (execs, activity_wf) = match site {
        CommitSite::Complete => (start_noop_workload(&db.admin_url, 1).await, false),
        CommitSite::Append | CommitSite::Claim => (
            start_activity_workload(&db.admin_url, "terminate", 1, 0).await,
            true,
        ),
    };
    let _worker = RunningWorker::spawn("infra-terminate", &db.worker_url);

    let pid = blocker.wait_for_blocked_backend().await;
    match fate {
        CommitFate::RolledBack => {
            // The backend exits before the unlock, so the COMMIT cannot land.
            blocker.terminate(pid).await;
            blocker.release().await;
        }
        CommitFate::AckLost => {
            let blackhole = serde_json::json!({ "timeout": 0 });
            db.add_toxic("ack_lost", "timeout", "downstream", blackhole)
                .await;
            blocker.release().await;
            blocker.wait_until_committed(pid).await;
            blocker.terminate(pid).await;
            // The removal closes the held worker connections.
            db.remove_toxic("ack_lost").await;
        }
    }

    let diag = format!("{site:?} {fate:?}");
    converge(&db.admin_url, &execs, activity_wf, &diag).await;

    // After a dropped connection, the worker writes the result again on a new
    // connection (#1871). The handler must not run a second time. Only the
    // rolled-back test needs the repeat. In the ack-lost test, the commit
    // landed, so the repeat must change nothing.
    if matches!(site, CommitSite::Append) {
        let mut conn = connect(&db.admin_url).await;
        for exec_id in &execs {
            if !one_activity_attempt(&mut conn, *exec_id).await {
                let history = history_dump(&mut conn, &execs).await;
                panic!("{diag}: workflow {exec_id:?} ran its activity more than once\n{history}");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_append() {
    terminate_backend_in_commit(CommitSite::Append, CommitFate::RolledBack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_complete() {
    terminate_backend_in_commit(CommitSite::Complete, CommitFate::RolledBack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_claim() {
    terminate_backend_in_commit(CommitSite::Claim, CommitFate::RolledBack).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_after_commit_ack_lost_append() {
    terminate_backend_in_commit(CommitSite::Append, CommitFate::AckLost).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_after_commit_ack_lost_complete() {
    terminate_backend_in_commit(CommitSite::Complete, CommitFate::AckLost).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_after_commit_ack_lost_claim() {
    terminate_backend_in_commit(CommitSite::Claim, CommitFate::AckLost).await;
}

// ── Scenario 2: Postgres restart and pause ──────────────────────────────────

/// Crash-restart Postgres while activities run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_crash_restart_mid_workload() {
    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "restart", 6, 1500).await;
    let _worker = RunningWorker::spawn("infra-restart", &db.worker_url);
    wait_for_running_activities(&db.admin_url, None, 1).await;

    db.crash_restart_postgres().await;

    // The crash can drop every write of an activity in flight. Then only
    // `start_to_close` recovers the task, and the retry policy runs it again.
    converge(&db.admin_url, &execs, true, "crash restart").await;
}

/// Pause Postgres for longer than the lease TTL while two workers hold
/// activities. On unpause every heartbeat is stale at once.
///
/// The test does not assert a reclaim. A reclaim races the first heartbeats
/// after the unpause, so its result is not deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_pause_longer_than_lease_ttl() {
    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "pause", 6, 1000).await;
    let _a = RunningWorker::spawn("infra-pause-a", &db.worker_url);
    let _b = RunningWorker::spawn("infra-pause-b", &db.worker_url);
    wait_for_running_activities(&db.admin_url, None, 1).await;

    db.pause_postgres().await;
    tokio::time::sleep(PAST_LEASE_TTL).await;
    db.unpause_postgres().await;

    converge(&db.admin_url, &execs, true, "pause").await;
}

// ── Scenario 3: toxiproxy latency and partition ─────────────────────────────

/// Add latency in both directions between the workers and Postgres.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toxiproxy_latency_between_worker_and_db() {
    let db = FaultDb::start().await;
    let latency = serde_json::json!({ "latency": 100, "jitter": 50 });
    db.add_toxic("lat_up", "latency", "upstream", latency.clone())
        .await;
    db.add_toxic("lat_down", "latency", "downstream", latency)
        .await;

    // Proof the toxic is live: one round trip takes at least the base delay.
    let started = Instant::now();
    let mut probe = connect(&db.worker_url).await;
    probe.batch_execute("SELECT 1").await.expect("probe");
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "the latency toxic is not active: {:?}",
        started.elapsed()
    );
    drop(probe);

    let execs = start_activity_workload(&db.admin_url, "latency", 4, 200).await;
    let _a = RunningWorker::spawn_tuned("infra-latency-a", &db.worker_url, SLOW_NETWORK);
    let _b = RunningWorker::spawn_tuned("infra-latency-b", &db.worker_url, SLOW_NETWORK);

    converge(&db.admin_url, &execs, true, "latency").await;
}

/// Blackhole worker A for longer than the lease TTL while it holds three
/// activities. Worker B, on a clean path, reclaims the tasks.
///
/// Attempt 1 on A and attempt 2 on B both wait. After the heal, the test
/// releases attempt 1, so A writes three stale results. No result exists yet,
/// so only the claim fence can reject them. Then the test releases attempt 2,
/// and B finishes the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toxiproxy_partition_longer_than_lease_ttl() {
    const HELD: usize = 3;

    RELEASE_FIRST_ATTEMPTS.store(false, Ordering::SeqCst);
    RELEASE_SECOND_ATTEMPTS.store(false, Ordering::SeqCst);
    HELD_FIRST_ATTEMPTS.store(0, Ordering::SeqCst);
    LATE_RETURNS.store(0, Ordering::SeqCst);
    let db = FaultDb::start().await;
    // Postgres keeps the open transaction of a partitioned worker. That
    // transaction keeps its row locks. Orphan reclaim skips a locked row
    // (issue #1876), but it cannot reclaim the row until the session ends.
    // This server timeout ends such a session within the test budget.
    db.limit_idle_in_transaction(5).await;
    let input = serde_json::json!({ "sleep_ms": 0, "hold_attempts": true });
    let execs = start_workload(
        &db.admin_url,
        "infra_activity_wf",
        "partition",
        HELD,
        &input,
    )
    .await;
    let a = RunningWorker::spawn("infra-partition-a", &db.worker_url);
    // A claimed task can still be on its way to the handler. Wait until every
    // handler on A is inside the hold, so each one returns after the heal.
    wait_for_count(
        &HELD_FIRST_ATTEMPTS,
        HELD,
        "first attempts held on worker A",
    )
    .await;

    let blackhole = serde_json::json!({ "timeout": 0 });
    db.add_toxic("bh_up", "timeout", "upstream", blackhole.clone())
        .await;
    db.add_toxic("bh_down", "timeout", "downstream", blackhole)
        .await;
    tokio::time::sleep(PAST_LEASE_TTL).await;

    // Worker B uses the admin proxy, which has no toxic. Wait until B holds
    // every task, so each claim of A is stale.
    let _b = RunningWorker::spawn("infra-partition-b", &db.admin_url);
    wait_for_running_activities(&db.admin_url, Some("infra-partition-b"), HELD).await;
    let reclaimed = reclaimed_tasks(&db.admin_url, &execs).await;
    assert_eq!(
        usize::try_from(reclaimed).ok(),
        Some(HELD),
        "worker B must reclaim every task of the partitioned worker"
    );

    // Remove the partition, then release the held attempts on A.
    db.remove_toxic("bh_up").await;
    db.remove_toxic("bh_down").await;
    RELEASE_FIRST_ATTEMPTS.store(true, Ordering::SeqCst);
    wait_for_count(
        &LATE_RETURNS,
        HELD,
        "held attempts on worker A that returned",
    )
    .await;
    // Give worker A time to make its stale result writes before it stops.
    tokio::time::sleep(PAST_LEASE_TTL).await;
    drop(a);

    // The fence must have rejected every stale write: no result exists yet.
    let mut conn = connect(&db.admin_url).await;
    for exec_id in &execs {
        assert_eq!(
            event_count(&mut conn, *exec_id, "ActivityCompleted").await,
            0,
            "the claim fence must reject the stale result of worker A for {exec_id:?}"
        );
    }

    RELEASE_SECOND_ATTEMPTS.store(true, Ordering::SeqCst);
    converge(&db.admin_url, &execs, true, "partition").await;
}

// ── Scenario 4: SIGKILL of a worker process ─────────────────────────────────

/// Kills the child worker on drop, so a failed test leaves no process.
struct ChildWorker(std::process::Child);

impl Drop for ChildWorker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// SIGKILL a worker that runs as a separate process while it holds an
/// activity. A worker in this process then reclaims and finishes the work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_child_worker_mid_activity() {
    use std::os::unix::process::ExitStatusExt;

    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "sigkill", 1, 0).await;
    let mut child = ChildWorker(
        std::process::Command::new(std::env::current_exe().expect("test binary path"))
            .args(["--exact", CHILD_ENTRY, "--ignored", "--nocapture"])
            .env(CHILD_DB_URL_VAR, &db.worker_url)
            .spawn()
            .expect("start the child worker"),
    );
    wait_for_running_activities(&db.admin_url, Some(CHILD_WORKER_ID), 1).await;

    child.0.kill().expect("SIGKILL the child worker");
    let status = child.0.wait().expect("reap the child worker");
    assert_eq!(
        status.signal(),
        Some(9),
        "the child must die by SIGKILL, not exit: {status:?}"
    );

    let _worker = RunningWorker::spawn("infra-sigkill-recover", &db.worker_url);
    converge(&db.admin_url, &execs, true, "sigkill").await;
    let reclaimed = reclaimed_tasks(&db.admin_url, &execs).await;
    assert!(reclaimed >= 1, "the killed worker's task must be reclaimed");
}

/// The child worker process. It runs only when the parent test sets
/// [`CHILD_DB_URL_VAR`]. Otherwise it returns at once.
#[test]
#[ignore = "started as a child process by sigkill_child_worker_mid_activity"]
fn sigkill_child_worker_entry() {
    let Ok(url) = std::env::var(CHILD_DB_URL_VAR) else {
        return;
    };
    // A panic becomes an abort, so the parent never sees a clean exit.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("child: panic: {info}");
        std::process::abort();
    }));
    // The child must not outlive a parent that failed to kill it.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(300));
        std::process::exit(3);
    });
    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    runtime.block_on(async move {
        let worker = build_worker(CHILD_WORKER_ID, FAST);
        let pool: DbPool = crate::integration_e2e::build_test_pool(&url);
        worker.run(&pool).await;
    });
}
