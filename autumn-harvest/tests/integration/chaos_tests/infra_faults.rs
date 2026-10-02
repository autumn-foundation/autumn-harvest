//! Process-, database- and network-level crash-recovery tests (issue #1801).
//!
//! The chaos harness KILL is a panic in one tokio task. These tests inject
//! faults below the engine instead: a killed backend, a Postgres restart or
//! pause, a slow or partitioned network, and a SIGKILL of a worker process.
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

use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::prelude::*;
use autumn_harvest::worker::{DbPool, HandlerRegistry, Worker};
use autumn_harvest::{ExecutionId, ShardId};
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::SimpleAsyncConnection;
use testcontainers::{ContainerAsync, GenericImage};
use testcontainers_modules::postgres::Postgres;

use super::{
    CountRow, DB_BODY_SERIAL, assert_converged, base_params, chaos_noop_info, connect, exec_state,
};

/// The worker heartbeat interval. The stale threshold, the "lease TTL", is
/// twice this value: 1 s.
const HEARTBEAT: Duration = Duration::from_millis(500);

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

// ── Workload ────────────────────────────────────────────────────────────────

/// Sleeps for `input.sleep_ms`. In the child worker process it sleeps for
/// ten minutes, so the parent can kill the child while the activity runs.
#[activity(start_to_close = "10s")]
async fn infra_step(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> Result<serde_json::Value, String> {
    if std::env::var_os(CHILD_DB_URL_VAR).is_some() {
        tokio::time::sleep(Duration::from_secs(600)).await;
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
    _body: tokio::sync::MutexGuard<'static, ()>,
    pg: ContainerAsync<Postgres>,
    _toxiproxy: ContainerAsync<GenericImage>,
    api: String,
    worker_url: String,
    admin_url: String,
}

impl FaultDb {
    async fn start() -> Self {
        todo!("GREEN: start Postgres and toxiproxy")
    }

    /// Kill Postgres with no grace period, then start it again.
    async fn crash_restart_postgres(&self) {
        todo!("GREEN: restart Postgres")
    }

    async fn pause_postgres(&self) {
        todo!("GREEN: pause Postgres")
    }

    async fn unpause_postgres(&self) {
        todo!("GREEN: unpause Postgres")
    }

    /// Add a toxic to the `worker` proxy.
    async fn add_toxic(&self, name: &str, kind: &str, stream: &str, attrs: serde_json::Value) {
        todo!("GREEN: add a toxic")
    }

    async fn remove_toxic(&self, name: &str) {
        todo!("GREEN: remove a toxic")
    }
}

/// The transaction that a commit-blocking trigger stops in.
#[derive(Clone, Copy, Debug)]
enum CommitSite {
    /// An `ActivityCompleted` event append.
    Append,
    /// A `WorkflowCompleted` event append, the terminal write.
    Complete,
    /// A task claim: a `PENDING` to `RUNNING` task-queue update.
    Claim,
}

/// Holds the advisory lock that blocks the trigger, on its own connection.
struct CommitBlocker {
    conn: AsyncPgConnection,
}

impl CommitBlocker {
    /// Install a deferred trigger at `site` and take the lock that blocks it.
    async fn install(admin_url: &str, site: CommitSite) -> Self {
        todo!("GREEN: install the trigger for {site:?}")
    }

    /// Wait for a backend to block in the trigger, terminate it, and release
    /// the lock. Return the terminated pid.
    async fn terminate_blocked_backend(self) -> i32 {
        todo!("GREEN: terminate the blocked backend")
    }
}

/// A worker that runs in this process until the value drops.
struct RunningWorker {
    worker: Arc<Worker>,
    handle: tokio::task::JoinHandle<()>,
}

impl RunningWorker {
    fn spawn(worker_id: &str, url: &str) -> Self {
        todo!("GREEN: spawn worker {worker_id} on {url}")
    }
}

// ── Probes ──────────────────────────────────────────────────────────────────

/// Start `count` workflows of type `infra_activity_wf`.
async fn start_activity_workload(
    admin_url: &str,
    tag: &str,
    count: usize,
    sleep_ms: u64,
) -> Vec<ExecutionId> {
    todo!("GREEN: start {count} workflows for {tag} on {admin_url}, {sleep_ms} ms")
}

/// Wait until every execution is `COMPLETED`, then run the sweep oracle and
/// the activity-result check.
async fn converge(admin_url: &str, execs: &[ExecutionId], activity_wf: bool, diag: &str) {
    todo!("GREEN: converge {execs:?} on {admin_url}, {activity_wf}, {diag}")
}

/// Wait until at least one activity task is `RUNNING`. When `worker_id` is
/// set, the task must belong to that worker.
async fn wait_for_running_activity(admin_url: &str, worker_id: Option<&str>) {
    todo!("GREEN: wait for a running activity on {admin_url}, {worker_id:?}")
}

/// Count tasks of `execs` that the orphan reclaimer requeued at least once.
async fn reclaimed_tasks(admin_url: &str, execs: &[ExecutionId]) -> i64 {
    todo!("GREEN: count reclaimed tasks of {execs:?} on {admin_url}")
}

// ── Scenario 1: pg_terminate_backend mid-commit ─────────────────────────────

/// Kill the worker's backend while its COMMIT waits in a deferred trigger.
/// The transaction rolls back, and the worker sees a dropped connection.
async fn terminate_backend_mid_commit(site: CommitSite) {
    let db = FaultDb::start().await;
    let blocker = CommitBlocker::install(&db.admin_url, site).await;
    let (execs, activity_wf) = match site {
        CommitSite::Complete => (start_noop_workload(&db.admin_url, 1).await, false),
        CommitSite::Append | CommitSite::Claim => (
            start_activity_workload(&db.admin_url, "terminate", 1, 0).await,
            true,
        ),
    };
    let _worker = RunningWorker::spawn("infra-terminate", &db.worker_url);

    let pid = blocker.terminate_blocked_backend().await;
    assert!(pid > 0, "{site:?}: no backend was terminated");

    converge(&db.admin_url, &execs, activity_wf, &format!("{site:?}")).await;
}

/// Start `count` single-cycle `chaos_noop` workflows.
async fn start_noop_workload(admin_url: &str, count: usize) -> Vec<ExecutionId> {
    todo!("GREEN: start {count} noop workflows on {admin_url}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_append() {
    terminate_backend_mid_commit(CommitSite::Append).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_complete() {
    terminate_backend_mid_commit(CommitSite::Complete).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminate_backend_mid_commit_claim() {
    terminate_backend_mid_commit(CommitSite::Claim).await;
}

// ── Scenario 2: Postgres restart and pause ──────────────────────────────────

/// Crash-restart Postgres while activities run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_crash_restart_mid_workload() {
    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "restart", 6, 1500).await;
    let _worker = RunningWorker::spawn("infra-restart", &db.worker_url);
    wait_for_running_activity(&db.admin_url, None).await;

    db.crash_restart_postgres().await;

    converge(&db.admin_url, &execs, true, "crash restart").await;
}

/// Pause Postgres for longer than the lease TTL while two workers hold
/// activities. On unpause every heartbeat is stale at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_pause_longer_than_lease_ttl() {
    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "pause", 6, 1000).await;
    let _a = RunningWorker::spawn("infra-pause-a", &db.worker_url);
    let _b = RunningWorker::spawn("infra-pause-b", &db.worker_url);
    wait_for_running_activity(&db.admin_url, None).await;

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
    let _a = RunningWorker::spawn("infra-latency-a", &db.worker_url);
    let _b = RunningWorker::spawn("infra-latency-b", &db.worker_url);

    converge(&db.admin_url, &execs, true, "latency").await;
}

/// Blackhole worker A for longer than the lease TTL while it holds
/// activities. Worker B, on a clean path, reclaims and finishes the work.
/// Then the partition heals, and the late writes of A must not duplicate a
/// result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toxiproxy_partition_longer_than_lease_ttl() {
    let db = FaultDb::start().await;
    let execs = start_activity_workload(&db.admin_url, "partition", 3, 3000).await;
    let a = RunningWorker::spawn("infra-partition-a", &db.worker_url);
    wait_for_running_activity(&db.admin_url, Some("infra-partition-a")).await;

    let blackhole = serde_json::json!({ "timeout": 0 });
    db.add_toxic("bh_up", "timeout", "upstream", blackhole.clone())
        .await;
    db.add_toxic("bh_down", "timeout", "downstream", blackhole)
        .await;
    tokio::time::sleep(PAST_LEASE_TTL).await;

    let _b = RunningWorker::spawn("infra-partition-b", &db.admin_url);
    converge(&db.admin_url, &execs, true, "partition, before heal").await;
    let reclaimed = reclaimed_tasks(&db.admin_url, &execs).await;
    assert!(
        reclaimed >= 1,
        "worker B must reclaim at least one task of the partitioned worker"
    );

    // Heal. Worker A sees its held connections drop and retries its writes.
    db.remove_toxic("bh_up").await;
    db.remove_toxic("bh_down").await;
    tokio::time::sleep(PAST_LEASE_TTL).await;
    drop(a);

    converge(&db.admin_url, &execs, true, "partition, after heal").await;
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
    wait_for_running_activity(&db.admin_url, Some(CHILD_WORKER_ID)).await;

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
/// [`CHILD_DB_URL_VAR`]; otherwise it returns at once.
#[test]
#[ignore = "started as a child process by sigkill_child_worker_mid_activity"]
fn sigkill_child_worker_entry() {
    todo!("GREEN: run the child worker")
}
