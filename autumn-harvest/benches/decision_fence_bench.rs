//! Decision persist latency with zero, one and two fence rows (issue #2248).
//!
//! Owner mode adds a second locked row to each decision. The DR generation
//! row takes `FOR SHARE`. The owner row takes `FOR KEY SHARE`. This bench
//! times one decision transaction in four fence modes:
//!
//! - `none`: no DR pin and no owner tenure. No fence row.
//! - `generation`: a DR pin only. Each append takes the generation row.
//! - `owner`: an owner tenure only. The start fence takes the owner row.
//! - `generation_owner`: both. The start fence takes both rows.
//!
//! The stage 1b pass condition compares `generation_owner` with
//! `generation`. The delta must stay under 10%.
//!
//! Each decision is a model of the worker persist, made of public engine
//! calls: the decision fence, two appends, one activity task enqueue and an
//! execution update, all in one transaction. The post-commit wake sender is
//! registered, as in a worker. Each writer owns one execution. All writers
//! share the generation row and one owner row, so concurrent locks become
//! multixacts.
//!
//! # Running
//!
//! ```text
//! cargo bench -p autumn-harvest --features db --bench decision_fence_bench
//!
//! # Against an existing server. The URL is an admin connection. A fresh
//! # database is made and dropped:
//! HARVEST_FENCE_BENCH_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench decision_fence_bench
//! ```
//!
//! With no URL the bench starts a Docker Postgres. With no Docker it prints a
//! skip notice and exits 0. `HARVEST_FENCE_BENCH_WRITERS` sets the writer
//! counts as a comma list. The default is `1,8`.

use std::time::{Duration, Instant};

use autumn_harvest::decision_owner::{
    self, Acquisition, DecisionPartition, OwnerHolder, OwnerLeaseConfig, OwnerTenure,
};
use autumn_harvest::error::HarvestError;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::queue::{self, EnqueueParams, TaskType};
use autumn_harvest::replication::{self, FenceRegistry, ShardGeneration};
use autumn_harvest::types::{ActivityExecId, ExecutionId, ShardId};
use autumn_harvest::worker::DbPool;
use criterion::{BenchmarkId, Criterion};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::sync::Mutex;

/// The shard of every execution.
const SHARD: ShardId = ShardId::new(0);

/// The queue of the enqueued activity tasks.
const QUEUE: &str = "fence_bench";

/// The owner row that every decision fences.
const PARTITION: u16 = 7;

/// The fence rows that one decision takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fence {
    None,
    Generation,
    Owner,
    GenerationOwner,
}

impl Fence {
    const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Generation => "generation",
            Self::Owner => "owner",
            Self::GenerationOwner => "generation_owner",
        }
    }

    const fn pins_a_tenure(self) -> bool {
        matches!(self, Self::Owner | Self::GenerationOwner)
    }
}

/// One writer: its connection and its execution.
struct Writer {
    conn: AsyncPgConnection,
    exec_id: ExecutionId,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let Some(bench) = runtime.block_on(Bench::setup()) else {
        println!("SKIP: decision_fence_bench needs Postgres.");
        println!("      Start Docker, or set HARVEST_FENCE_BENCH_URL to an admin URL.");
        return;
    };
    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(6))
        .configure_from_args();
    let writer_counts = writer_counts();
    // The DR pin is process-wide and stays, so the modes with no pin run
    // first.
    for fence in [
        Fence::None,
        Fence::Owner,
        Fence::Generation,
        Fence::GenerationOwner,
    ] {
        if fence == Fence::Generation {
            FenceRegistry::register(SHARD, ShardGeneration::new(0))
                .expect("no other pin in this process");
        }
        let mut group = criterion.benchmark_group(format!("decision_persist/{}", fence.label()));
        for &writers in &writer_counts {
            group.bench_with_input(
                BenchmarkId::new("writers", writers),
                &writers,
                |b, &writers| {
                    b.to_async(&runtime).iter_custom(|iters| {
                        let bench = &bench;
                        async move { bench.run(fence, writers, iters).await }
                    });
                },
            );
        }
        group.finish();
    }
    FenceRegistry::clear();
    runtime.block_on(bench.teardown());
    criterion.final_summary();
}

/// Read a comma list of writer counts.
fn writer_counts() -> Vec<usize> {
    std::env::var("HARVEST_FENCE_BENCH_WRITERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|n| n.trim().parse().ok())
                .filter(|n| *n > 0)
                .collect::<Vec<usize>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| vec![1, 8])
}

/// The bench database, its writers and the owner tenure.
struct Bench {
    admin_url: String,
    database: String,
    writers: Vec<Mutex<Writer>>,
    tenure: OwnerTenure,
    _sink_pool: DbPool,
    _sink: autumn_harvest::notify::NotifySink,
    _container: Option<ContainerAsync<Postgres>>,
}

impl Bench {
    async fn setup() -> Option<Self> {
        let (admin_url, container) = admin_url().await?;
        let database = format!("harvest_fence_bench_{}", std::process::id());
        let mut admin = AsyncPgConnection::establish(&admin_url).await.ok()?;
        admin
            .batch_execute(&format!(
                "DROP DATABASE IF EXISTS {database}; CREATE DATABASE {database}"
            ))
            .await
            .expect("create the bench database");
        let url = with_database(&admin_url, &database);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to the bench database");
        conn.batch_execute(&autumn_harvest::test_init_sql())
            .await
            .expect("migrate the bench database");
        replication::ensure_generation_row(&mut conn, SHARD)
            .await
            .expect("provision the generation row");
        let tenure = acquire(&mut conn).await;

        let max_writers = writer_counts().into_iter().max().unwrap_or(1);
        let mut writers = Vec::with_capacity(max_writers);
        for n in 0..max_writers {
            let mut conn = AsyncPgConnection::establish(&url)
                .await
                .expect("connect a writer");
            let exec_id = seed_execution(&mut conn, n).await;
            writers.push(Mutex::new(Writer { conn, exec_id }));
        }
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&url);
        let sink_pool: DbPool = deadpool::managed::Pool::builder(manager)
            .max_size(2)
            .build()
            .expect("sink pool");
        let sink = autumn_harvest::notify::register_pool(&sink_pool);
        Some(Self {
            admin_url,
            database,
            writers,
            tenure,
            _sink_pool: sink_pool,
            _sink: sink,
            _container: container,
        })
    }

    /// Run `iters` decisions over `writers` concurrent writers. Returns the
    /// sum of the decision latencies, scaled to `iters` decisions, so
    /// criterion reports the mean latency of one decision.
    async fn run(&self, fence: Fence, writers: usize, iters: u64) -> Duration {
        let per_writer = iters.div_ceil(writers as u64).max(1);
        let tenure = fence.pins_a_tenure().then(|| self.tenure.clone());
        let runs = self.writers[..writers].iter().map(|writer| {
            let tenure = tenure.clone();
            async move {
                let mut writer = writer.lock().await;
                let mut total = Duration::ZERO;
                for _ in 0..per_writer {
                    let started = Instant::now();
                    decide(&mut writer, tenure.clone())
                        .await
                        .expect("the decision commits");
                    total += started.elapsed();
                }
                total
            }
        });
        let total: Duration = futures::future::join_all(runs).await.into_iter().sum();
        let done = per_writer * writers as u64;
        total.mul_f64(iters as f64 / done as f64)
    }

    async fn teardown(self) {
        drop(self.writers);
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

/// One decision transaction, shaped as the worker persist.
async fn decide(writer: &mut Writer, tenure: Option<OwnerTenure>) -> Result<(), HarvestError> {
    let exec_id = writer.exec_id;
    decision_owner::scope_decision(async {
        let _ = decision_owner::pin_decision(tenure);
        writer
            .conn
            .transaction::<_, HarvestError, _>(async |conn| {
                replication::assert_decision_fence(conn).await?;
                let id = decision_owner::conn_id(conn);
                decision_owner::fenced_scope(
                    id,
                    autumn_harvest::notify::coalesced(conn, async |conn| {
                        decision_body(conn, exec_id).await
                    }),
                )
                .await
            })
            .await
    })
    .await
}

/// The writes of one decision: two appends, one activity task and the
/// execution update.
async fn decision_body(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> Result<(), HarvestError> {
    let next = autumn_harvest::store::next_event_id_for(conn, exec_id).await?;
    let scheduled = WorkflowEvent::ActivityScheduled {
        activity_id: ActivityExecId::new(),
        name: "fence_bench_step".to_string(),
        input: serde_json::Value::Null,
        queue: QUEUE.to_string(),
    };
    autumn_harvest::store::append_events(conn, exec_id, &[scheduled], next).await?;
    let boundary = WorkflowEvent::SignalReceived {
        signal_name: "fence_bench".to_string(),
        payload: serde_json::Value::Null,
    };
    autumn_harvest::store::append_events(conn, exec_id, &[boundary], next + 1).await?;
    let params = EnqueueParams {
        workflow_exec_id: Some(exec_id.as_uuid()),
        activity_name: Some("fence_bench_step".to_string()),
        ..EnqueueParams::new(QUEUE, TaskType::Activity, serde_json::Value::Null)
    };
    queue::enqueue(conn, &params).await?;
    diesel::sql_query("UPDATE harvest_workflow_executions SET updated_at = NOW() WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
        .execute(conn)
        .await
        .map_err(autumn_harvest::error::database_error)?;
    Ok(())
}

/// Acquire the owner row that every decision fences.
async fn acquire(conn: &mut AsyncPgConnection) -> OwnerTenure {
    let partition = DecisionPartition::new(PARTITION).expect("a bench partition is in range");
    let holder = OwnerHolder::for_worker("fence-bench");
    // The commit fence checks the holder and the epoch, not the lease end.
    // So the lease can lapse during the run.
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
         VALUES ($1, 'fence_bench', $2, gen_random_uuid(), 0, '{}'::jsonb, $3, 'RUNNING')",
    )
    .bind::<diesel::sql_types::Uuid, _>(exec_id.as_uuid())
    .bind::<diesel::sql_types::Text, _>(format!("fence-bench-{n}"))
    .bind::<diesel::sql_types::Text, _>(QUEUE)
    .execute(conn)
    .await
    .expect("seed an execution");
    exec_id
}

/// The admin URL from the environment, or a Docker Postgres.
async fn admin_url() -> Option<(String, Option<ContainerAsync<Postgres>>)> {
    if let Ok(url) = std::env::var("HARVEST_FENCE_BENCH_URL") {
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
