//! Commit throughput with NOTIFY inside and after the write transaction
//! (issue #1796).
//!
//! A transaction that calls `pg_notify` takes a database-wide lock at commit.
//! This benchmark runs N concurrent writers in three modes and reports commits
//! per second and commit latency for each:
//!
//! - `none`: the write only. This is the upper bound.
//! - `in_transaction`: the write and a `pg_notify` in the same transaction.
//!   Harvest did this before issue #1796.
//! - `post_commit`: the write and `notify::notify_task_enqueued`, with the
//!   pool registered. The sender sends after commit.
//!
//! # Running
//!
//! ```text
//! ./benchmarks/notify-commit.sh
//!
//! # Or against an existing server (a scratch table is created and dropped):
//! HARVEST_NOTIFY_BENCH_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench notify_commit_bench
//! ```
//!
//! With no URL the benchmark starts a Docker Postgres. With no Docker it
//! prints a skip notice and exits 0.
//!
//! `HARVEST_NOTIFY_BENCH_SECS` sets the measured window of each scenario. The
//! default is 5 seconds. `HARVEST_NOTIFY_BENCH_WRITERS` sets the writer counts
//! as a comma list. The default is `1,4,16,32`.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "commit counts and ranks stay far below 2^52"
)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use autumn_harvest::worker::DbPool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use uuid::Uuid;

/// The queue the writers wake.
const QUEUE: &str = "notify_bench";

/// The scratch table the writers insert into.
const TABLE_SQL: &str = "DROP TABLE IF EXISTS harvest_notify_bench; \
    CREATE TABLE harvest_notify_bench (id BIGSERIAL PRIMARY KEY, body JSONB NOT NULL);";

/// How each writer sends its wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// No wake.
    None,
    /// `pg_notify` inside the write transaction.
    InTransaction,
    /// The post-commit sender.
    PostCommit,
}

impl Mode {
    const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InTransaction => "in_transaction",
            Self::PostCommit => "post_commit",
        }
    }
}

/// The result of one scenario.
struct Outcome {
    commits: usize,
    seconds: f64,
    latencies_ms: Vec<f64>,
    /// Notifications the sender lost.
    lost: u64,
}

impl Outcome {
    fn per_second(&self) -> f64 {
        self.commits as f64 / self.seconds
    }

    fn percentile(&self, p: f64) -> f64 {
        if self.latencies_ms.is_empty() {
            return 0.0;
        }
        let mut sorted = self.latencies_ms.clone();
        sorted.sort_by(f64::total_cmp);
        let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[rank.min(sorted.len() - 1)]
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(run());
}

/// Read a comma list of writer counts.
fn writer_counts() -> Vec<usize> {
    std::env::var("HARVEST_NOTIFY_BENCH_WRITERS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|n| n.trim().parse().ok())
                .filter(|n| *n > 0)
                .collect::<Vec<usize>>()
        })
        .filter(|counts| !counts.is_empty())
        .unwrap_or_else(|| vec![1, 4, 16, 32])
}

fn window() -> Duration {
    let secs = std::env::var("HARVEST_NOTIFY_BENCH_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(5);
    Duration::from_secs(secs.max(1))
}

/// Find a database: the URL from the environment, or a Docker Postgres.
async fn database() -> Option<(String, Option<ContainerAsync<Postgres>>)> {
    if let Ok(url) = std::env::var("HARVEST_NOTIFY_BENCH_URL") {
        return Some((url, None));
    }
    let container = Postgres::default().with_tag("16").start().await.ok()?;
    let host = container.get_host().await.ok()?;
    let port = container.get_host_port_ipv4(5432).await.ok()?;
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    Some((url, Some(container)))
}

fn build_pool(url: &str, size: usize) -> DbPool {
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
    deadpool::managed::Pool::builder(manager)
        .max_size(size)
        .build()
        .expect("pool")
}

async fn run() {
    let Some((url, _container)) = database().await else {
        println!(
            "SKIP: notify_commit_bench needs Postgres. Set HARVEST_NOTIFY_BENCH_URL or start Docker."
        );
        return;
    };
    let mut admin = AsyncPgConnection::establish(&url)
        .await
        .expect("connect to the benchmark database");
    admin
        .batch_execute(TABLE_SQL)
        .await
        .expect("create the scratch table");

    // A listener drains the channel, as a worker does.
    let mut listener = autumn_harvest::notify::QueueListener::connect(&url, &[QUEUE.to_string()])
        .await
        .expect("listener");
    let wakes = Arc::new(AtomicUsize::new(0));
    let drain = {
        let wakes = Arc::clone(&wakes);
        tokio::spawn(async move {
            while let Ok(autumn_harvest::notify::QueueWaitOutcome::Notification(_)) = listener
                .wait_for_notification_outcome(Duration::from_secs(3600))
                .await
            {
                wakes.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let window = window();
    let writer_counts = writer_counts();
    println!("# Commit throughput with NOTIFY (issue #1796)");
    println!();
    println!(
        "Window: {} s per scenario. Writers: {}.",
        window.as_secs(),
        writer_counts
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!();
    println!("| writers | mode | commits/s | p50 ms | p99 ms | wakes | lost |");
    println!("|---:|---|---:|---:|---:|---:|---:|");
    let modes = [Mode::None, Mode::InTransaction, Mode::PostCommit];
    for (round, writers) in writer_counts.into_iter().enumerate() {
        // Rotate the order, so no mode always runs last.
        for step in 0..modes.len() {
            let mode = modes[(round + step) % modes.len()];
            admin
                .batch_execute("TRUNCATE harvest_notify_bench")
                .await
                .expect("truncate the scratch table");
            let wakes_before = AtomicUsize::load(&wakes, Ordering::Relaxed);
            let outcome = scenario(&url, writers, mode, window).await;
            println!(
                "| {writers} | {} | {:.0} | {:.2} | {:.2} | {} | {} |",
                mode.label(),
                outcome.per_second(),
                outcome.percentile(50.0),
                outcome.percentile(99.0),
                AtomicUsize::load(&wakes, Ordering::Relaxed) - wakes_before,
                outcome.lost,
            );
        }
    }
    println!();
    println!(
        "A wake can stand for several commits, so `wakes` can be below the \
         commit count in `post_commit` mode."
    );

    drain.abort();
    admin
        .batch_execute("DROP TABLE IF EXISTS harvest_notify_bench")
        .await
        .expect("drop the scratch table");
}

/// Run `writers` concurrent writers in `mode` for `window`.
async fn scenario(url: &str, writers: usize, mode: Mode, window: Duration) -> Outcome {
    // One extra connection for the sender.
    let pool = build_pool(url, writers + 1);
    let sink = if mode == Mode::PostCommit {
        let sink = autumn_harvest::notify::register_pool(&pool);
        assert!(
            sink.wait_ready(Duration::from_secs(10)).await,
            "the notify sender must become ready"
        );
        Some(sink)
    } else {
        None
    };
    let start = Instant::now() + Duration::from_millis(200);
    let deadline = start + window;
    let pool = Arc::new(pool);
    let mut tasks = Vec::with_capacity(writers);
    for _ in 0..writers {
        let pool = Arc::clone(&pool);
        tasks.push(tokio::spawn(async move {
            let mut conn = pool.get().await.expect("writer connection");
            tokio::time::sleep_until(start.into()).await;
            let mut latencies = Vec::new();
            while Instant::now() < deadline {
                let began = Instant::now();
                write_once(&mut conn, mode).await;
                latencies.push(began.elapsed().as_secs_f64() * 1000.0);
            }
            latencies
        }));
    }
    let mut latencies_ms = Vec::new();
    for task in tasks {
        latencies_ms.extend(task.await.expect("writer task"));
    }
    // Let the sender drain before the pool drops, and let the last wakes
    // reach the listener.
    if let Some(sink) = &sink {
        sink.flush(Duration::from_secs(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    Outcome {
        commits: latencies_ms.len(),
        seconds: window.as_secs_f64(),
        latencies_ms,
        lost: sink
            .as_ref()
            .map_or(0, autumn_harvest::notify::NotifySink::send_failures),
    }
}

/// One write transaction in `mode`.
async fn write_once(conn: &mut AsyncPgConnection, mode: Mode) {
    let task_id = Uuid::new_v4();
    Box::pin(
        conn.transaction::<(), autumn_harvest::error::HarvestError, _>(async |conn| {
            diesel::sql_query("INSERT INTO harvest_notify_bench (body) VALUES ($1)")
                .bind::<diesel::sql_types::Jsonb, _>(serde_json::json!({"task": task_id}))
                .execute(conn)
                .await
                .map_err(autumn_harvest::error::database_error)?;
            match mode {
                Mode::None => {}
                Mode::InTransaction => {
                    let payload = serde_json::json!({"task_id": task_id}).to_string();
                    diesel::sql_query("SELECT pg_notify($1, $2)")
                        .bind::<diesel::sql_types::Text, _>(autumn_harvest::notify::queue_channel(
                            QUEUE,
                        ))
                        .bind::<diesel::sql_types::Text, _>(payload)
                        .execute(conn)
                        .await
                        .map_err(autumn_harvest::error::database_error)?;
                }
                Mode::PostCommit => {
                    autumn_harvest::notify::notify_task_enqueued(conn, QUEUE, task_id).await?;
                }
            }
            Ok(())
        }),
    )
    .await
    .expect("write transaction");
}
