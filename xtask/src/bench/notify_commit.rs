//! `cargo xtask bench notify-commit`: the port of `benchmarks/notify-commit.sh`.
//!
//! The commit throughput with NOTIFY inside and after the write transaction
//! (issue #1796).
//!
//! ```text
//! cargo xtask bench notify-commit
//! ```
//!
//! It starts one throwaway Postgres 16 and runs `notify_commit_bench` against
//! it. It writes the report to `benchmarks/results/notify-commit-<timestamp>.md`,
//! and it removes the container.
//!
//! Options, from the environment:
//!
//! - `HARVEST_NOTIFY_BENCH_SECS=<n>`: the measured window per scenario
//!   (default 5). The bench reads it.
//! - `HARVEST_NOTIFY_BENCH_WRITERS=<list>`: the writer counts (default
//!   1,4,16,32). The bench reads it.
//! - `HARVEST_NOTIFY_BENCH_PORT=<port>`: the host port of the container
//!   (default 55440).
//! - `HARVEST_BENCH_OUT=<path>`: write the report somewhere else.

use std::process::{Command, ExitCode, Stdio};

use smop::prelude::*;

use super::run::{probe, report_path, utc_stamp};
use crate::perf::support::{self, Ctx};

/// How many one-second readiness probes to try before the bench starts anyway.
const READY_ATTEMPTS: u32 = 60;

/// Removes the container on exit, as the `EXIT` trap did.
struct Container {
    name: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        let mut remove = Command::new("docker");
        remove.args(["rm", "-f", &self.name]).stderr(Stdio::null());
        support::cleanup(&mut remove);
    }
}

/// Returns the connection string of the container on `port`.
pub fn bench_url(port: &str) -> String {
    format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
}

/// Arguments of `cargo xtask bench notify-commit`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask bench notify-commit`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let ctx = Ctx::from_env();
    let benchmarks = ctx.path("benchmarks");
    let name = format!("harvest-notify-bench-{}", std::process::id());
    let port = support::var_or("HARVEST_NOTIFY_BENCH_PORT", "55440");

    if !probe("docker", &["version"]) {
        eprintln!("docker is required: see https://docs.docker.com/get-docker/");
        return Ok(ExitCode::FAILURE);
    }
    let container = Container { name };

    println!("==> starting Postgres 16 on port {port}");
    let mut start = Command::new("docker");
    start
        .args(["run", "-d", "--name", &container.name])
        .args(["-e", "POSTGRES_PASSWORD=postgres"])
        .arg("-p")
        .arg(format!("{port}:5432"))
        .arg("postgres:16");
    let status = support::status_to(&mut start, Stdio::null())?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }
    // The init server listens on a socket only. So a TCP check waits for the real one.
    for _ in 0..READY_ATTEMPTS {
        support::stop_on_interrupt()?;
        let ready = probe(
            "docker",
            &[
                "exec",
                &container.name,
                "pg_isready",
                "-h",
                "127.0.0.1",
                "-U",
                "postgres",
            ],
        );
        if ready {
            break;
        }
        time::sleep_secs(1);
    }

    let out = report_path(
        &benchmarks,
        support::non_empty_var("HARVEST_BENCH_OUT"),
        &format!("notify-commit-{}.md", utc_stamp()),
    );
    if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    println!("==> running notify_commit_bench");
    let mut bench = ctx.cargo([
        "bench",
        "-p",
        "autumn-harvest",
        "--features",
        "db",
        "--bench",
        "notify_commit_bench",
    ]);
    bench.env("HARVEST_NOTIFY_BENCH_URL", bench_url(&port));
    let status = support::tee_stdout(&mut bench, &out)?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }

    println!("==> report written to {}", out.display());
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bench_url_targets_the_published_port() {
        assert_eq!(
            bench_url("55440"),
            "postgres://postgres:postgres@127.0.0.1:55440/postgres"
        );
    }

    #[test]
    fn the_readiness_wait_is_bounded() {
        assert_eq!(READY_ATTEMPTS, 60);
    }
}
