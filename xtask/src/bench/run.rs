//! `cargo xtask bench run`: the port of `benchmarks/run.sh`.
//!
//! The one documented command for the end-to-end benchmark suite (issue #941).
//!
//! ```text
//! cargo xtask bench run
//! ```
//!
//! It brings up the four-shard Postgres topology in
//! `benchmarks/docker-compose.yml`. It runs every scenario at 1, 2 and 4
//! shards, and it writes the report to `benchmarks/results/<timestamp>.md`.
//! It then tears the topology down again.
//!
//! Options, from the environment:
//!
//! - `HARVEST_BENCH_KEEP=1`: leave the containers running afterwards.
//! - `HARVEST_BENCH_CHECK=1`: also compare this run against the published
//!   baselines at the documented tolerance, and print a verdict per number.
//!   The bench reads this variable itself.
//! - `HARVEST_BENCH_OUT=<path>`: write the report somewhere else.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

use smop::prelude::*;

use crate::perf::broken_session_scan::support::{self, Ctx};

/// The connection strings of the four shards in `docker-compose.yml`.
pub const SHARD_URLS: &str = "postgres://postgres:postgres@127.0.0.1:55432/postgres,\
postgres://postgres:postgres@127.0.0.1:55433/postgres,\
postgres://postgres:postgres@127.0.0.1:55434/postgres,\
postgres://postgres:postgres@127.0.0.1:55435/postgres";

/// Returns the report path: `HARVEST_BENCH_OUT`, or a UTC timestamp under `results/`.
pub fn report_path(benchmarks: &Path, out: Option<String>, file_name: &str) -> PathBuf {
    out.map_or_else(|| benchmarks.join("results").join(file_name), PathBuf::from)
}

/// Returns the `date -u +%Y%m%dT%H%M%SZ` stamp of now.
pub fn utc_stamp() -> String {
    time::format(&time::now(), "%Y%m%dT%H%M%SZ")
}

/// Runs `program` with its output discarded, and reports whether it succeeded.
///
/// This is the shell probe `if ! program args >/dev/null 2>&1`. A program
/// that is not on `PATH` fails the probe, as it did in the shell.
pub fn probe(program: &str, args: &[&str]) -> bool {
    sh::cmd(program).args(args).success().unwrap_or(false)
}

/// Tears the topology down on exit, unless `HARVEST_BENCH_KEEP=1`.
struct Topology {
    compose_file: PathBuf,
}

impl Drop for Topology {
    fn drop(&mut self) {
        if std::env::var("HARVEST_BENCH_KEEP").is_ok_and(|keep| keep == "1") {
            println!("==> leaving the benchmark topology running (HARVEST_BENCH_KEEP=1)");
            return;
        }
        println!("==> tearing down the benchmark topology");
        let mut down = Command::new("docker");
        down.arg("compose")
            .arg("-f")
            .arg(&self.compose_file)
            .args(["down", "-v"])
            .stderr(Stdio::null());
        let _ = support::status_to(&mut down, Stdio::null());
    }
}

/// Arguments of `cargo xtask bench run`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask bench run`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let ctx = Ctx::from_env();
    let benchmarks = ctx.path("benchmarks");
    let compose_file = benchmarks.join("docker-compose.yml");

    if !probe("docker", &["compose", "version"]) {
        eprintln!("docker compose is required: see https://docs.docker.com/compose/install/");
        return Ok(ExitCode::FAILURE);
    }
    let _topology = Topology {
        compose_file: compose_file.clone(),
    };

    println!("==> starting 4 Postgres shards");
    let mut up = Command::new("docker");
    up.arg("compose")
        .arg("-f")
        .arg(&compose_file)
        .args(["up", "-d", "--wait"]);
    let status = support::status(&mut up)?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }

    let out = report_path(
        &benchmarks,
        support::non_empty_var("HARVEST_BENCH_OUT"),
        &format!("{}.md", utc_stamp()),
    );
    if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    println!("==> running the suite (this takes roughly 20-40 minutes)");
    let mut bench = ctx.cargo([
        "bench",
        "-p",
        "autumn-harvest",
        "--features",
        "db,testing",
        "--bench",
        "e2e_bench",
    ]);
    bench.env("HARVEST_BENCH_SHARD_URLS", SHARD_URLS);
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
    fn shard_urls_name_the_four_compose_ports() {
        let urls: Vec<&str> = SHARD_URLS.split(',').collect();
        assert_eq!(urls.len(), 4);
        for (url, port) in urls.iter().zip(55432..) {
            assert_eq!(
                *url,
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres")
            );
        }
    }

    #[test]
    fn report_path_prefers_the_override() {
        let dir = Path::new("/repo/benchmarks");
        assert_eq!(
            report_path(dir, None, "x.md"),
            PathBuf::from("/repo/benchmarks/results/x.md")
        );
        assert_eq!(
            report_path(dir, Some("/tmp/out.md".into()), "x.md"),
            PathBuf::from("/tmp/out.md")
        );
    }

    #[test]
    fn probe_fails_for_a_missing_program() {
        assert!(probe("sh", &["-c", "exit 0"]));
        assert!(!probe("sh", &["-c", "exit 3"]));
        assert!(!probe("xtask-surely-missing-program", &["version"]));
    }

    #[test]
    fn utc_stamp_has_the_date_shape() {
        let stamp = utc_stamp();
        assert_eq!(stamp.len(), 16, "{stamp}");
        assert_eq!(stamp.as_bytes()[8], b'T');
        assert!(stamp.ends_with('Z'));
    }
}
