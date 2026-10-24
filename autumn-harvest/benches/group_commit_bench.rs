//! Decision persist throughput with and without group commit (issue #2256).
//!
//! The writers of one case share one decision partition and one owner
//! tenure. Each writer owns one execution and commits decisions in a loop.
//! The writer count is the queue depth. Each mode has `width` connections:
//!
//! - `direct`: each decision is its own transaction on a pool of `width`
//!   connections, as the persist runs today.
//! - `solo`: the lane with `max_batch = 1`. Each decision commits alone, in
//!   at most `width` transactions at once.
//! - `group`: the lane with the default `max_batch`. Waiting decisions
//!   share one transaction, with a savepoint each.
//!
//! Criterion reports the wall time per decision. At one writer, that is
//! the latency of one decision. At depth, its inverse is the throughput.
//! After each case the bench prints the p50 and p99 latency of one
//! decision, and the mean batch size.
//!
//! Two last passes cancel drift. At low load, one writer commits through
//! `direct`, `solo` and `group` in turn and prints the p50 of each mode. At
//! depth, bursts of `solo` and `group` alternate and print their throughput.
//!
//! Each decision models the worker persist with public engine calls, as
//! `decision_fence_bench` does. It runs the start fence, two appends, one
//! activity task enqueue and an execution update. The post-commit wake
//! sender is registered.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --features db --bench group_commit_bench
//!
//! # Against an existing server. The URL is an admin connection. A fresh
//! # database is made and dropped:
//! HARVEST_GROUP_BENCH_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench group_commit_bench
//! ```
//!
//! With no URL the bench starts a Docker Postgres. With no Docker it prints a
//! skip notice and exits 0. `HARVEST_GROUP_BENCH_WRITERS` sets the writer
//! counts as a comma list. The default is `1,4,16,64`.
//! `HARVEST_GROUP_BENCH_WIDTH` sets the width. The default is 4.
//! `HARVEST_GROUP_BENCH_SYNC=off` sets `synchronous_commit = off` on the
//! bench database. The default keeps the server setting.
//! `HARVEST_GROUP_BENCH_ORDER` sets the order of the modes as a comma list
//! of labels. A run in another order shows drift.

use std::sync::Arc;
use std::time::{Duration, Instant};

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnerHolder, OwnerLeaseConfig, OwnerTenure,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::group_commit::{self, GroupCommit, GroupCommitConfig};
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use criterion::{BenchmarkId, Criterion};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use futures::FutureExt;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::Mutex;

/// The shard of every execution.
const SHARD: ShardId = ShardId::new(0);

/// The queue of the enqueued activity tasks.
const QUEUE: &str = "group_bench";

/// The owner row of every writer.
const PARTITION: u16 = 9;

/// How one case commits its decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Direct,
    Solo,
    Group,
}

impl Mode {
    const ALL: [Self; 3] = [Self::Direct, Self::Solo, Self::Group];

    const fn label(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Solo => "solo",
            Self::Group => "group",
        }
    }
}

/// The latencies of one case, in decisions.
#[derive(Default)]
struct Samples {
    latencies: Vec<Duration>,
}

impl Samples {
    fn quantile(&mut self, q: f64) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }
        self.latencies.sort_unstable();
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "a sample count is far below 2^52"
        )]
        let at = ((self.latencies.len() - 1) as f64 * q).round() as usize;
        self.latencies[at]
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let Some(bench) = runtime.block_on(Bench::setup()) else {
        println!("SKIP: group_commit_bench needs Postgres.");
        println!("      Start Docker, or set HARVEST_GROUP_BENCH_URL to an admin URL.");
        return;
    };
    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(6))
        .configure_from_args();
    let width = width();
    for mode in mode_order() {
        let mut group = criterion.benchmark_group(format!("decision_commit/{}", mode.label()));
        for &writers in &writer_counts() {
            runtime.block_on(bench.reset());
            let lanes = Arc::new(GroupCommit::new(
                bench.pool.clone(),
                lane_config(mode, width),
            ));
            let samples = Arc::new(Mutex::new(Samples::default()));
            group.bench_with_input(
                BenchmarkId::new("writers", writers),
                &writers,
                |b, &writers| {
                    b.to_async(&runtime).iter_custom(|iters| {
                        let bench = &bench;
                        let lanes = Arc::clone(&lanes);
                        let samples = Arc::clone(&samples);
                        async move { bench.run(mode, &lanes, &samples, writers, iters).await }
                    });
                },
            );
            let (p50, p99) = runtime.block_on(async {
                let mut samples = samples.lock().await;
                (samples.quantile(0.5), samples.quantile(0.99))
            });
            let stats = lanes.stats();
            #[allow(
                clippy::cast_precision_loss,
                reason = "decision counts here are far below 2^52"
            )]
            let mean_batch = if stats.batches == 0 {
                1.0
            } else {
                stats.decisions as f64 / stats.batches as f64
            };
            println!(
                "group_commit_bench: mode={} writers={writers} width={width} \
                 p50_ms={:.3} p99_ms={:.3} mean_batch={mean_batch:.2} largest={}",
                mode.label(),
                p50.as_secs_f64() * 1e3,
                p99.as_secs_f64() * 1e3,
                stats.largest,
            );
        }
        group.finish();
    }
    runtime.block_on(bench.low_load_ab(width));
    runtime.block_on(bench.depth_ab(width));
    runtime.block_on(bench.teardown());
    criterion.final_summary();
}

/// The modes to run, in order, from `HARVEST_GROUP_BENCH_ORDER`.
fn mode_order() -> Vec<Mode> {
    std::env::var("HARVEST_GROUP_BENCH_ORDER")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|label| Mode::ALL.into_iter().find(|m| m.label() == label.trim()))
                .collect::<Vec<Mode>>()
        })
        .filter(|order| !order.is_empty())
        .unwrap_or_else(|| Mode::ALL.to_vec())
}

/// The lane config of `mode`. `direct` uses no lane.
const fn lane_config(mode: Mode, width: usize) -> GroupCommitConfig {
    let max_batch = match mode {
        Mode::Direct | Mode::Solo => 1,
        Mode::Group => group_commit::DEFAULT_MAX_BATCH,
    };
    GroupCommitConfig::new(width, max_batch).expect("a valid lane config")
}

/// Read a comma list of writer counts.
fn writer_counts() -> Vec<usize> {
    std::env::var("HARVEST_GROUP_BENCH_WRITERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|n| n.trim().parse().ok())
                .filter(|n| *n > 0)
                .collect::<Vec<usize>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| vec![1, 4, 16, 64])
}

/// The connections of each mode.
fn width() -> usize {
    std::env::var("HARVEST_GROUP_BENCH_WIDTH")
        .ok()
        .and_then(|raw| raw.trim().parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(group_commit::DEFAULT_WIDTH)
}

/// The bench database, its pool and its executions.
struct Bench {
    admin_url: String,
    database: String,
    pool: DbPool,
    tenure: OwnerTenure,
    executions: Vec<ExecutionId>,
    _sink: autumn_harvest::notify::NotifySink,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Bench {
    async fn setup() -> Option<Self> {
        let (admin_url, container) = admin_url().await?;
        let database = format!("harvest_group_bench_{}", std::process::id());
        let mut admin = AsyncPgConnection::establish(&admin_url).await.ok()?;
        for statement in [
            format!("DROP DATABASE IF EXISTS {database}"),
            format!("CREATE DATABASE {database}"),
        ] {
            admin
                .batch_execute(&statement)
                .await
                .expect("create the bench database");
        }
        if std::env::var("HARVEST_GROUP_BENCH_SYNC").as_deref() == Ok("off") {
            admin
                .batch_execute(&format!(
                    "ALTER DATABASE {database} SET synchronous_commit = off"
                ))
                .await
                .expect("set synchronous_commit");
        }
        let url = with_database(&admin_url, &database);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to the bench database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate the bench database");
        let tenure = acquire(&mut conn).await;
        let max_writers = writer_counts().into_iter().max().unwrap_or(1);
        let mut executions = Vec::with_capacity(max_writers);
        for n in 0..max_writers {
            executions.push(seed_execution(&mut conn, n).await);
        }
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
        let pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(width() + 2)
            .build()
            .expect("bench pool");
        let sink = autumn_harvest::notify::register_pool(&pool);
        Some(Self {
            admin_url,
            database,
            pool,
            tenure,
            executions,
            _sink: sink,
            _container: container,
        })
    }

    /// Run `iters` decisions over `writers` writers. Returns the wall time,
    /// scaled to `iters` decisions, so criterion reports the wall time of
    /// one decision.
    #[allow(
        clippy::cast_precision_loss,
        reason = "decision counts here are far below 2^52"
    )]
    async fn run(
        &self,
        mode: Mode,
        lanes: &Arc<GroupCommit<()>>,
        samples: &Arc<Mutex<Samples>>,
        writers: usize,
        iters: u64,
    ) -> Duration {
        let per_writer = iters.div_ceil(writers as u64).max(1);
        let slots = Arc::new(tokio::sync::Semaphore::new(width()));
        let started = Instant::now();
        let runs = self.executions[..writers].iter().map(|&exec_id| {
            let lanes = Arc::clone(lanes);
            let slots = Arc::clone(&slots);
            async move {
                let mut latencies = Vec::with_capacity(usize::try_from(per_writer).unwrap_or(0));
                for _ in 0..per_writer {
                    let began = Instant::now();
                    match mode {
                        Mode::Direct => {
                            let _slot = slots.acquire().await.expect("open semaphore");
                            let (pool, tenure) = (self.pool.clone(), self.tenure.clone());
                            tokio::spawn(direct(pool, tenure, exec_id))
                                .await
                                .expect("the direct task")
                        }
                        Mode::Solo | Mode::Group => {
                            lanes
                                .commit(self.tenure.clone(), exec_id, decision(exec_id))
                                .await
                        }
                    }
                    .expect("the decision commits");
                    latencies.push(began.elapsed());
                }
                latencies
            }
        });
        let latencies: Vec<Vec<Duration>> = futures::future::join_all(runs).await;
        let wall = started.elapsed();
        samples
            .lock()
            .await
            .latencies
            .extend(latencies.into_iter().flatten());
        let done = per_writer * writers as u64;
        wall.mul_f64(iters as f64 / done as f64)
    }

    /// One writer commits decisions in turn through `direct`, `solo` and
    /// `group`, so drift hits each mode alike. Prints the p50 and p99 of
    /// each mode. `HARVEST_GROUP_BENCH_AB` sets the decisions per mode. The
    /// default is 1000, and 0 skips the pass.
    async fn low_load_ab(&self, width: usize) {
        let rounds: usize = std::env::var("HARVEST_GROUP_BENCH_AB")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(1000);
        if rounds == 0 {
            return;
        }
        self.reset().await;
        let solo = GroupCommit::new(self.pool.clone(), lane_config(Mode::Solo, width));
        let group = GroupCommit::new(self.pool.clone(), lane_config(Mode::Group, width));
        let exec_id = self.executions[0];
        let mut samples: [Samples; 3] = Default::default();
        for _ in 0..rounds {
            for (at, mode) in Mode::ALL.into_iter().enumerate() {
                let began = Instant::now();
                match mode {
                    Mode::Direct => {
                        let (pool, tenure) = (self.pool.clone(), self.tenure.clone());
                        tokio::spawn(direct(pool, tenure, exec_id))
                            .await
                            .expect("the direct task")
                    }
                    Mode::Solo => {
                        solo.commit(self.tenure.clone(), exec_id, decision(exec_id))
                            .await
                    }
                    Mode::Group => {
                        group
                            .commit(self.tenure.clone(), exec_id, decision(exec_id))
                            .await
                    }
                }
                .expect("the decision commits");
                samples[at].latencies.push(began.elapsed());
            }
        }
        for (at, mode) in Mode::ALL.into_iter().enumerate() {
            let p50 = samples[at].quantile(0.5);
            let p99 = samples[at].quantile(0.99);
            println!(
                "group_commit_bench: ab mode={} decisions={rounds} p50_ms={:.3} p99_ms={:.3}",
                mode.label(),
                p50.as_secs_f64() * 1e3,
                p99.as_secs_f64() * 1e3,
            );
        }
    }

    /// At each writer count above `width`, bursts of `solo` and `group`
    /// alternate, so drift hits both alike. Each burst runs 20 decisions
    /// per writer. Prints the throughput of each burst pair.
    /// `HARVEST_GROUP_BENCH_DEPTH_AB` sets the pairs per writer count. The
    /// default is 10, and 0 skips the pass.
    #[allow(
        clippy::cast_precision_loss,
        reason = "decision counts here are far below 2^52"
    )]
    async fn depth_ab(&self, width: usize) {
        let pairs: usize = std::env::var("HARVEST_GROUP_BENCH_DEPTH_AB")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(10);
        for writers in writer_counts().into_iter().filter(|w| *w > width) {
            self.reset().await;
            let solo = Arc::new(GroupCommit::new(
                self.pool.clone(),
                lane_config(Mode::Solo, width),
            ));
            let group = Arc::new(GroupCommit::new(
                self.pool.clone(),
                lane_config(Mode::Group, width),
            ));
            let samples = Arc::new(Mutex::new(Samples::default()));
            let decisions = 20 * writers as u64;
            for pair in 0..pairs {
                // Alternate which mode runs first in a pair.
                let order = if pair % 2 == 0 {
                    [Mode::Solo, Mode::Group]
                } else {
                    [Mode::Group, Mode::Solo]
                };
                let mut per_second = [0.0; 2];
                for mode in order {
                    let lanes = if mode == Mode::Solo { &solo } else { &group };
                    // `run` returns the wall time of all `decisions`.
                    let wall = self.run(mode, lanes, &samples, writers, decisions).await;
                    let at = usize::from(mode == Mode::Group);
                    per_second[at] = decisions as f64 / wall.as_secs_f64();
                }
                println!(
                    "group_commit_bench: depth_ab writers={writers} pair={pair} \
                     solo_per_s={:.1} group_per_s={:.1} ratio={:.3}",
                    per_second[0],
                    per_second[1],
                    per_second[1] / per_second[0],
                );
            }
        }
    }

    /// Empty the event and task tables and refresh their statistics.
    async fn reset(&self) {
        let mut conn = self.pool.get().await.expect("a bench connection");
        for statement in [
            "TRUNCATE harvest_events, harvest_task_queue CASCADE",
            "VACUUM ANALYZE harvest_events, harvest_task_queue, harvest_workflow_executions",
        ] {
            conn.batch_execute(statement)
                .await
                .expect("reset the bench tables");
        }
    }

    async fn teardown(self) {
        drop(self.pool);
        if let Ok(mut admin) = AsyncPgConnection::establish(&self.admin_url).await {
            let _ = admin
                .batch_execute(&format!(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                    self.database
                ))
                .await;
        }
    }
}

/// One decision transaction with no lane, as the persist runs today.
///
/// The bench spawns it, as a lane spawns its batch. So both modes run their
/// queries on a worker thread of the runtime.
async fn direct(
    pool: DbPool,
    tenure: OwnerTenure,
    exec_id: ExecutionId,
) -> Result<(), HarvestError> {
    let mut conn = pool
        .get()
        .await
        .map_err(|e| HarvestError::Database(e.to_string()))?;
    decision_owner::scope_decision(async move {
        let _ = decision_owner::pin_decision(Some(tenure));
        conn.transaction::<_, HarvestError, _>(async |conn| {
            autumn_harvest::replication::assert_decision_fence(conn).await?;
            let id = decision_owner::conn_id(conn);
            decision_owner::fenced_scope(id, decision_body(conn, exec_id)).await
        })
        .await
    })
    .await
}

/// One decision as a lane stage.
fn decision(exec_id: ExecutionId) -> group_commit::Stage<()> {
    group_commit::stage(move |conn| decision_body(conn, exec_id).boxed())
}

/// The writes of one decision: two appends, one activity task and the
/// execution update. The wakes stage once, as in the worker persist.
async fn decision_body(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<(), HarvestError> {
    autumn_harvest::notify::coalesced(conn, async |conn| {
        let next = autumn_harvest::store::next_event_id_for(conn, exec_id).await?;
        let scheduled = WorkflowEvent::ActivityScheduled {
            activity_id: ActivityExecId::new(),
            name: "group_bench_step".to_string(),
            input: serde_json::Value::Null,
            queue: QUEUE.to_string(),
        };
        let boundary = WorkflowEvent::SignalReceived {
            signal_name: "group_bench".to_string(),
            payload: serde_json::Value::Null,
        };
        autumn_harvest::store::append_events(conn, exec_id, &[scheduled], next).await?;
        autumn_harvest::store::append_events(conn, exec_id, &[boundary], next + 1).await?;
        let params = EnqueueParams {
            workflow_exec_id: Some(exec_id.as_uuid()),
            activity_name: Some("group_bench_step".to_string()),
            ..EnqueueParams::new(QUEUE, TaskType::Activity, serde_json::Value::Null)
        };
        queue::enqueue(conn, &params).await?;
        diesel::sql_query(
            "UPDATE harvest_workflow_executions SET sticky_worker_id = 'group-bench' WHERE id = $1",
        )
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .map_err(autumn_harvest::error::database_error)?;
        Ok(())
    })
    .await
}

/// Acquire the owner row of the bench partition.
async fn acquire(conn: &mut AsyncPgConnection) -> OwnerTenure {
    let partition = DecisionPartition::new(PARTITION).expect("the bench partition is in range");
    let holder = OwnerHolder::for_worker("group-bench");
    match decision_owner::acquire(
        conn,
        SHARD,
        &holder,
        partition,
        &OwnerLeaseConfig::default(),
    )
    .await
    .expect("acquire the owner row")
    {
        Acquisition::Acquired(tenure) => tenure,
        other => panic!("the bench owner row is free: {other:?}"),
    }
}

/// Insert one running execution for writer `n`.
async fn seed_execution(conn: &mut AsyncPgConnection, n: usize) -> ExecutionId {
    let exec_id = ExecutionId::new_for_shard(SHARD);
    diesel::sql_query(
        "INSERT INTO harvest_workflow_executions \
             (id, workflow_name, workflow_id, run_id, shard_id, input, queue_name, state) \
         VALUES ($1, 'group_bench', $2, gen_random_uuid(), 0, '{}'::jsonb, $3, 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(format!("group-bench-{n}"))
    .bind::<diesel::sql_types::Text, _>(QUEUE)
    .execute(conn)
    .await
    .expect("seed an execution");
    exec_id
}

/// The admin URL from the environment, or a Docker Postgres.
async fn admin_url() -> Option<(String, Option<ContainerAsync<Postgres>>)> {
    if let Ok(url) = std::env::var("HARVEST_GROUP_BENCH_URL") {
        return Some((url, None));
    }
    let container = Postgres::default().with_tag("16").start().await.ok()?;
    let host = container.get_host().await.ok()?;
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    Some((url, Some(container)))
}

/// `url` with its database name replaced by `database`.
fn with_database(url: &str, database: &str) -> String {
    let (base, _) = url.rsplit_once('/').expect("a URL names its database");
    format!("{base}/{database}")
}
