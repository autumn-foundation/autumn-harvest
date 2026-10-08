//! Retention reclamation benchmark for the partitioned `harvest_events` layout
//! (issue #958).
//!
//! Produces the four numbers issue #958's Success Metric names, for **both**
//! layouts, from one seeded corpus:
//!
//! 1. Wall time of a retention pass reclaiming >= 50% of executions.
//! 2. Row-level `DELETE`s issued against `harvest_events` (the partitioned
//!    layout's headline claim is that this is zero).
//! 3. Dead-tuple ratio left behind, read before autovacuum catches up.
//! 4. Concurrent append and task-claim p99 during the pass, against a quiet
//!    baseline measured with the same load and the same window.
//!
//! # Running
//!
//! ```text
//! # Laptop/CI scale (20k executions x 10 events), throwaway Docker Postgres:
//! cargo bench -p autumn-harvest --features db --bench retention_reclaim_bench
//!
//! # The issue's headline scale (1M executions x 10 events = 10M rows).
//! # Needs a real server; the URL is used as an ADMIN connection and a fresh,
//! # uniquely-named database is created and migrated per arm, so a 10M-row
//! # corpus can never leak into a shared database.
//! HARVEST_BENCH_SCALE=full \
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo bench -p autumn-harvest --features db --bench retention_reclaim_bench
//! ```
//!
//! With neither available it prints a skip notice and exits 0, so `cargo bench`
//! on a machine without Docker is not a failure.
//!
//! # Why not criterion
//!
//! The measured operation is destructive and enormous: it collects half the
//! corpus and drops partitions. Criterion would run it thousands of times and
//! end up timing "a retention pass against an already-empty database". Each arm
//! is measured once, against a freshly seeded database, exactly as an operator
//! would experience it.
//!
//! # What it does not establish
//!
//! The numbers are single-sample, taken on whatever host runs them, with the
//! concurrent load being one connection rather than a fleet. They are evidence
//! about the *shape* of the two layouts' costs — zero row-deletes versus
//! millions, a bounded partition drop versus a cascade proportional to the
//! corpus — not a throughput SLO. The dead-tuple ratio in particular is read
//! deliberately before autovacuum runs: it measures the debt the DELETE path
//! creates, not a steady state.

#[path = "../tests/integration/retention_reclaim_support.rs"]
mod support;

#[cfg(feature = "db")]
mod runner {
    use std::time::Duration;

    use diesel_async::{AsyncConnection, AsyncPgConnection, SimpleAsyncConnection};

    use super::support::{Measurement, Scale, measure_pass, report, seed};

    /// How long the quiet-baseline and under-load windows each run.
    const LOAD_WINDOW: Duration = Duration::from_secs(5);

    pub async fn main() {
        let Some(admin_url) = std::env::var("HARVEST_TEST_DATABASE_URL").ok() else {
            run_with_container().await;
            return;
        };
        let scale = Scale::from_env();
        let mut arms = Vec::new();
        for partitioned in [false, true] {
            let db = fresh_database(&admin_url, partitioned).await;
            arms.push(run_arm(&db, partitioned, scale).await);
        }
        emit(scale, &arms);
    }

    /// Create, migrate and (optionally) partition a uniquely-named database.
    ///
    /// A fresh database per arm is not tidiness: the two arms measure
    /// `pg_stat` counters and dead-tuple ratios, which are per-database and
    /// cumulative, so sharing one would make the second arm read the first
    /// arm's debt.
    async fn fresh_database(admin_url: &str, partitioned: bool) -> String {
        let name = format!(
            "harvest_bench_{}_{}",
            if partitioned { "part" } else { "flat" },
            uuid::Uuid::new_v4().simple()
        );
        let mut admin = AsyncPgConnection::establish(admin_url)
            .await
            .expect("admin connection");
        admin
            .batch_execute(&format!("CREATE DATABASE {name}"))
            .await
            .expect("create bench database");
        let url = replace_database(admin_url, &name);
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("bench database connection");
        conn.batch_execute(autumn_harvest::full_migrations_sql())
            .await
            .expect("migrate");
        if partitioned {
            conn.batch_execute(&autumn_harvest::partition::enable_sql(
                &autumn_harvest::partition::EnableOptions::default(),
            ))
            .await
            .expect("enable partitioning");
        }
        url
    }

    /// Swap the database component of a libpq URL.
    fn replace_database(url: &str, name: &str) -> String {
        match url.rfind('/') {
            Some(i) => {
                let (head, tail) = url.split_at(i + 1);
                // Preserve any query string (sslmode=…, application_name=…).
                let query = tail.find('?').map_or("", |q| &tail[q..]);
                format!("{head}{name}{query}")
            }
            None => url.to_string(),
        }
    }

    async fn run_arm(url: &str, partitioned: bool, scale: Scale) -> Measurement {
        let mut conn = AsyncPgConnection::establish(url)
            .await
            .expect("seed connection");
        eprintln!(
            "seeding {} arm: {} executions x {} events…",
            if partitioned {
                "partitioned"
            } else {
                "unpartitioned"
            },
            scale.executions,
            scale.events_per_execution
        );
        seed(&mut conn, scale, partitioned).await;
        drop(conn);
        eprintln!("measuring…");
        measure_pass(url, partitioned, scale, LOAD_WINDOW).await
    }

    async fn run_with_container() {
        use testcontainers::ImageExt;
        use testcontainers_modules::postgres::Postgres;
        use testcontainers_modules::testcontainers::runners::AsyncRunner;

        let scale = Scale::from_env();
        let mut arms = Vec::new();
        for partitioned in [false, true] {
            let mut init = autumn_harvest::full_migrations_sql().to_string();
            if partitioned {
                init.push_str("\n\n");
                init.push_str(&autumn_harvest::partition::enable_sql(
                    &autumn_harvest::partition::EnableOptions::default(),
                ));
            }
            let Ok(container) = Postgres::default()
                .with_init_sql(init.into_bytes())
                .with_tag("16")
                .start()
                .await
            else {
                eprintln!(
                    "SKIP: no Docker and no HARVEST_TEST_DATABASE_URL — nothing to measure \
                     against. This is not a failure."
                );
                return;
            };
            let host = container.get_host().await.expect("host");
            let port = container.get_host_port_ipv4(5432).await.expect("port");
            let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
            arms.push(run_arm(&url, partitioned, scale).await);
        }
        emit(scale, &arms);
    }

    fn emit(scale: Scale, arms: &[Measurement]) {
        let table = report(scale, arms);
        println!("\n{table}");
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("docs/perf-artifacts/event-partition-retention");
        if std::fs::create_dir_all(&dir).is_ok() {
            let path = dir.join("measured.md");
            if std::fs::write(&path, &table).is_ok() {
                println!("wrote {}", path.display());
            }
        }
    }
}

#[cfg(feature = "db")]
fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(runner::main());
}

#[cfg(not(feature = "db"))]
fn main() {
    eprintln!("SKIP: retention_reclaim_bench requires the `db` feature.");
}
