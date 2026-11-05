//! `cargo xtask dev run-mixed-version-smoke`: the port of
//! `scripts/run-mixed-version-smoke.sh`.
//!
//! Mixed-version smoke (issue #1828). It proves the rolling-deploy contract
//! in `docs/upgrading/README.md` for the current tree (N) and the previous
//! release (N-1).
//!
//! Steps:
//!
//! 1. Build `scripts/mixed-version-smoke` against both trees.
//! 2. Apply the migrations of N. Both versions then run on the schema of N.
//! 3. Roll forward: N-1 starts the runs, N finishes them.
//! 4. Roll back: N starts the runs, N-1 finishes them.
//! 5. Mixed fleet: N-1 and N poll the same queue at the same time.
//! 6. Steps 3 to 5 again, with a payload codec on both sides, as far as
//!    N-1 supports it.
//!
//! In steps 3 and 4 the first worker stops while each run waits on a timer.
//! The check then asserts which version ran each step.
//!
//! Needs a Postgres database that it can reset, and git tags. Run it from a
//! checkout with tags fetched:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/mvsmoke \
//!   cargo xtask dev run-mixed-version-smoke
//! ```
//!
//! Environment:
//!
//! - `DATABASE_URL`: required. The command drops and recreates its schema.
//! - `MV_SMOKE_PREVIOUS`: the previous release tag. The default is the
//!   highest `vX.Y.Z` tag of an earlier minor version.
//! - `MV_SMOKE_RUNS`: runs per scenario. Default: 6.
//! - `MV_SMOKE_TIMEOUT`: seconds for each wait. Default: 180.
//!
//! A failed step stops the smoke with the exit code of that step, as
//! `set -e` does. The cleanup then prints the worker logs and stops each
//! worker that still runs.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus, Stdio};
use std::time::Duration;

use regex::Regex;
use smop::prelude::*;

use crate::util::repo_root;

/// The scenarios, in the order that they run.
const SCENARIOS: [&str; 3] = ["roll-forward", "roll-back", "mixed"];

/// The line that a worker logs when it polls.
const READY_LINE: &str = "mixed-version-smoke worker ready";

/// How long a worker has to get ready, and to stop, in seconds.
const WORKER_WAIT_SECS: u32 = 60;

/// Arguments of `cargo xtask dev run-mixed-version-smoke`. It takes none.
#[derive(Debug, Args)]
pub struct Args {}

/// A step failed. The smoke exits with this code, as `set -e` does.
#[derive(Debug)]
struct StepFailed(u8);

impl fmt::Display for StepFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a step failed with exit code {}", self.0)
    }
}

impl std::error::Error for StepFailed {}

/// Returns the version of the first `version = "..."` line of a manifest,
/// as the `grep -m1 | sed` pipe of the old script did.
pub fn workspace_version(manifest: &str) -> Option<String> {
    let line = manifest
        .lines()
        .find(|line| line.starts_with("version = \""))?;
    let pattern = Regex::new(r#"version = "([^"]*)""#).ok()?;
    Some(pattern.replacen(line, 1, "$1").into_owned())
}

/// Returns the highest release tag of a minor version below `version`.
///
/// N-1 is the previous minor release, so a patch of the same minor does not
/// count. Pre-release tags do not count either.
pub fn previous_release<'a>(
    version: &str,
    tags: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    let current = Regex::new(r"^([0-9]+)\.([0-9]+)").ok()?;
    let caps = current.captures(version)?;
    let major: u64 = caps[1].parse().ok()?;
    let minor: u64 = caps[2].parse().ok()?;
    let release = Regex::new(r"^v([0-9]+)\.([0-9]+)\.([0-9]+)$").ok()?;
    tags.into_iter()
        .filter_map(|tag| {
            let caps = release.captures(tag)?;
            let parts: Vec<u64> = (1..=3)
                .map(|i| caps[i].parse().ok())
                .collect::<Option<_>>()?;
            let older = parts[0] < major || (parts[0] == major && parts[1] < minor);
            older.then(|| ((parts[0], parts[1], parts[2]), tag.to_owned()))
        })
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, tag)| tag)
}

/// The adapter feature for the API of the previous release. See
/// `scripts/mixed-version-smoke/src/compat.rs`.
pub fn adapter_features(previous: &str) -> &'static str {
    if previous.starts_with("v0.6.") {
        "v0_6"
    } else {
        ""
    }
}

/// The scenarios that run with a payload codec on both sides.
///
/// The previous release limits them. See "Known limits" in
/// `docs/upgrading/README.md`. 0.6 workers do not apply a payload codec.
/// They write plain payloads and cannot read an envelope that 0.7 writes. So
/// only a roll forward works.
pub fn codec_scenarios(previous: &str) -> &'static [&'static str] {
    if previous.starts_with("v0.6.") {
        &["roll-forward"]
    } else {
        &SCENARIOS
    }
}

/// Runs `cargo xtask dev run-mixed-version-smoke`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    let root = repo_root();
    std::env::set_current_dir(&root).context("cannot enter the repository root")?;
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => {
            eprintln!("DATABASE_URL: set DATABASE_URL to a database that this script may reset");
            return Ok(ExitCode::FAILURE);
        }
    };
    let runs = env_or("MV_SMOKE_RUNS", "6");
    let timeout = env_or("MV_SMOKE_TIMEOUT", "180");
    let Ok(run_count) = runs.parse::<u64>() else {
        eprintln!("MV_SMOKE_RUNS must be a whole number, not {runs:?}");
        return Ok(ExitCode::FAILURE);
    };
    let work = root.join("target/mixed-version-smoke");
    let mut smoke = Smoke {
        // The default target directory, so that the CI cache keeps the dependencies.
        target: root.join("target"),
        root,
        work,
        database_url,
        runs,
        double_runs: (2 * run_count).to_string(),
        timeout,
        codec: false,
        workers: Vec::new(),
    };
    let outcome = smoke.run();
    let passed = outcome.is_ok();
    smoke.cleanup(passed);
    match outcome {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(err) => {
            if let Some(StepFailed(code)) = err.downcast_ref::<StepFailed>() {
                return Ok(ExitCode::from(*code));
            }
            eprintln!("run-mixed-version-smoke: {err:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Reads a variable. An unset or empty value gives `default`.
fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// A worker that runs in the background.
struct Worker {
    label: String,
    child: Child,
}

/// The state of one smoke run.
struct Smoke {
    root: PathBuf,
    target: PathBuf,
    work: PathBuf,
    database_url: String,
    runs: String,
    double_runs: String,
    timeout: String,
    /// Set once the codec scenarios start, as `export MV_SMOKE_CODEC=1` did.
    codec: bool,
    /// The workers that run now.
    workers: Vec<Worker>,
}

impl Smoke {
    /// Returns a command with the environment that the old script exported.
    fn command(&self, program: impl AsRef<std::ffi::OsStr>) -> Command {
        let mut command = Command::new(program);
        command.env("CARGO_TARGET_DIR", &self.target);
        if self.codec {
            command.env("MV_SMOKE_CODEC", "1");
        }
        command
    }

    /// Returns a command for the smoke binary of version `label`.
    fn bin(&self, label: &str) -> Command {
        self.command(self.work.join("bin").join(label))
    }

    fn run(&mut self) -> Result<()> {
        remove_dir_all(&self.work.join("logs"))?;
        for dir in ["bin", "logs"] {
            let path = self.work.join(dir);
            std::fs::create_dir_all(&path)
                .with_context(|| format!("cannot create {}", path.display()))?;
        }

        let manifest = std::fs::read_to_string("Cargo.toml").context("cannot read Cargo.toml")?;
        // The old `grep -m1` pipe failed the script when no line matched.
        let Some(version) = workspace_version(&manifest) else {
            eprintln!("Cargo.toml has no `version = \"...\"` line");
            return Err(StepFailed(1).into());
        };
        let previous = match std::env::var("MV_SMOKE_PREVIOUS") {
            Ok(tag) if !tag.is_empty() => tag,
            _ => {
                // A failed `git tag` lists no tag. The check below reports it.
                let (_, tags) = capture(self.command("git").args(["tag", "-l", "v[0-9]*"]))?;
                previous_release(&version, tags.lines()).unwrap_or_default()
            }
        };
        if previous.is_empty() {
            eprintln!("no release tag below v{version}; fetch tags or set MV_SMOKE_PREVIOUS");
            return Err(StepFailed(1).into());
        }
        println!("mixed-version smoke: N = v{version} (this tree), N-1 = {previous}");

        let old_tree = self.check_out(&previous)?;
        let root = self.root.clone();
        self.build(&root, "", "current")?;
        self.build(&old_tree, adapter_features(&previous), "previous")?;

        self.migrate(&version)?;

        for name in SCENARIOS {
            self.scenario(name, name, &[])?;
        }

        // The same scenarios with a payload codec on both sides.
        let codec = codec_scenarios(&previous);
        self.codec = true;
        for name in SCENARIOS {
            if codec.contains(&name) {
                self.scenario(name, &format!("codec-{name}"), &["codec"])?;
            } else {
                println!("== codec-{name}: not supported with {previous} (see Known limits)");
            }
        }

        println!("mixed-version smoke passed: v{version} and {previous}");
        Ok(())
    }

    /// Checks out N-1, and returns its tree. Replaces a stale or partial
    /// checkout.
    fn check_out(&self, previous: &str) -> Result<PathBuf> {
        let old_tree = self.work.join(format!("tree-{previous}"));
        step(self.command("git").args(["worktree", "prune"]))?;
        let (_, head) = capture(
            self.command("git")
                .arg("-C")
                .arg(&old_tree)
                .args(["rev-parse", "HEAD"])
                .stderr(Stdio::null()),
        )
        .unwrap_or_else(|_| (ExitStatus::default(), String::new()));
        let (_, wanted) = capture(
            self.command("git")
                .arg("rev-parse")
                .arg(format!("{previous}^{{commit}}")),
        )?;
        if head.trim_end() != wanted.trim_end() {
            let removed = self
                .command("git")
                .args(["worktree", "remove", "--force"])
                .arg(&old_tree)
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success());
            if !removed {
                remove_dir_all(&old_tree)?;
            }
            step(self.command("git").args(["worktree", "prune"]))?;
            step(
                self.command("git")
                    .args(["worktree", "add", "--detach"])
                    .arg(&old_tree)
                    .arg(previous),
            )?;
        }
        Ok(old_tree)
    }

    /// Resets the schema, then applies the migrations of N.
    fn migrate(&self, version: &str) -> Result<()> {
        println!("resetting the database schema");
        step(
            self.command("psql")
                .arg(&self.database_url)
                .args(["-v", "ON_ERROR_STOP=1", "-q"])
                .args([
                    "-c",
                    "DROP SCHEMA public CASCADE",
                    "-c",
                    "CREATE SCHEMA public",
                ]),
        )?;

        println!("applying the migrations of v{version}");
        step(
            self.command("cargo")
                .args([
                    "run",
                    "-q",
                    "-p",
                    "autumn-harvest-cli",
                    "--",
                    "migrate",
                    "run",
                ])
                .args(["--database-url", &self.database_url])
                .args(["--include-dir", "autumn-harvest-plugin/migrations/harvest"]),
        )?;
        Ok(())
    }

    /// Builds the smoke crate against `tree` with `features`. Copies it to
    /// `bin/<label>`.
    fn build(&self, tree: &Path, features: &str, label: &str) -> Result<()> {
        let src = tree.join("target/mixed-version-smoke-src");
        remove_dir_all(&src)?;
        std::fs::create_dir_all(&src)
            .with_context(|| format!("cannot create {}", src.display()))?;
        step(
            self.command("cp")
                .arg("-R")
                .arg(self.root.join("scripts/mixed-version-smoke/."))
                .arg(format!("{}/", src.display())),
        )?;
        // Use the dependency versions that this tree shipped with.
        copy(&tree.join("Cargo.lock"), &src.join("Cargo.lock"))?;
        println!("building {label} from {}", tree.display());
        let mut build = self.command("cargo");
        build
            .args(["build", "--manifest-path"])
            .arg(src.join("Cargo.toml"));
        if !features.is_empty() {
            build.args(["--features", features]);
        }
        step(&mut build)?;
        copy(
            &self.target.join("debug/mixed-version-smoke"),
            &self.work.join("bin").join(label),
        )
    }

    /// Starts a worker of version `label` in the background. `prefix` names
    /// its log. Returns the index of the worker.
    fn start_worker(&mut self, label: &str, prefix: &str) -> Result<u32> {
        let log = self.work.join("logs").join(format!("{prefix}-{label}.log"));
        let out = File::create(&log).with_context(|| format!("cannot create {}", log.display()))?;
        let err = out.try_clone().context("cannot share the worker log")?;
        let child = spawn(
            self.bin(label)
                .arg("worker")
                .env("MV_SMOKE_LABEL", label)
                .stdin(Stdio::null())
                .stdout(out)
                .stderr(err),
        )?;
        let pid = child.id();
        self.workers.push(Worker {
            label: label.to_owned(),
            child,
        });
        for _ in 0..WORKER_WAIT_SECS {
            let text = std::fs::read(&log).unwrap_or_default();
            if String::from_utf8_lossy(&text).contains(READY_LINE) {
                return Ok(pid);
            }
            if self.exited(pid)? {
                eprintln!("the {label} worker exited before it was ready");
                return Err(StepFailed(1).into());
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        eprintln!("the {label} worker was not ready after 60s");
        Err(StepFailed(1).into())
    }

    /// Reports whether the worker with `pid` has exited.
    fn exited(&mut self, pid: u32) -> Result<bool> {
        let Some(worker) = self.workers.iter_mut().find(|w| w.child.id() == pid) else {
            return Ok(true);
        };
        Ok(worker
            .child
            .try_wait()
            .context("cannot poll a worker")?
            .is_some())
    }

    /// Stops the worker with `pid`. A graceful stop drains in-flight tasks.
    /// A worker that fails its stop fails the smoke.
    fn stop_worker(&mut self, pid: u32) -> Result<()> {
        let Some(index) = self.workers.iter().position(|w| w.child.id() == pid) else {
            return Ok(());
        };
        let mut worker = self.workers.remove(index);
        match stop(&mut worker)? {
            0 => Ok(()),
            code => Err(StepFailed(code).into()),
        }
    }

    /// Rolls from version `from` to version `to` while runs wait on their
    /// timers. `extra` goes to `check`.
    fn roll(&mut self, from: &str, to: &str, prefix: &str, extra: &[&str]) -> Result<()> {
        println!("== {prefix}: {from} starts the runs, {to} finishes them");
        let from_pid = self.start_worker(from, prefix)?;
        step(self.bin(from).args(["start", prefix, &self.runs]))?;
        step(
            self.bin("current")
                .args(["wait-first", prefix, &self.runs, &self.timeout]),
        )?;
        self.stop_worker(from_pid)?;
        let to_pid = self.start_worker(to, prefix)?;
        step(
            self.bin("current")
                .args(["wait-done", prefix, &self.runs, &self.timeout]),
        )?;
        step(
            self.bin("current")
                .args(["check", prefix, &self.runs, from, to])
                .args(extra),
        )?;
        self.stop_worker(to_pid)
    }

    /// Runs both versions on one queue. Each version starts half of the
    /// runs. `extra` goes to `check`.
    fn mixed(&mut self, prefix: &str, extra: &[&str]) -> Result<()> {
        println!("== {prefix}: both versions poll one queue");
        let old_pid = self.start_worker("previous", prefix)?;
        let new_pid = self.start_worker("current", prefix)?;
        step(
            self.bin("previous")
                .args(["start", &format!("{prefix}-previous"), &self.runs]),
        )?;
        step(
            self.bin("current")
                .args(["start", &format!("{prefix}-current"), &self.runs]),
        )?;
        step(
            self.bin("current")
                .args(["wait-done", prefix, &self.double_runs, &self.timeout]),
        )?;
        // The prefix matches the runs of both starters.
        step(
            self.bin("current")
                .args(["check", prefix, &self.double_runs, "*", "*", "both"])
                .args(extra),
        )?;
        self.stop_worker(old_pid)?;
        self.stop_worker(new_pid)
    }

    /// Runs scenario `name` with `prefix`. `extra` goes to `check`.
    fn scenario(&mut self, name: &str, prefix: &str, extra: &[&str]) -> Result<()> {
        match name {
            "roll-forward" => self.roll("previous", "current", prefix, extra),
            "roll-back" => self.roll("current", "previous", prefix, extra),
            "mixed" => self.mixed(prefix, extra),
            _ => Ok(()),
        }
    }

    /// Prints the worker logs after a failure, then stops each worker.
    fn cleanup(&mut self, passed: bool) {
        if !passed {
            let mut logs: Vec<PathBuf> = std::fs::read_dir(self.work.join("logs"))
                .map(|entries| {
                    entries
                        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                        .filter(|path| {
                            path.is_file() && path.extension().is_some_and(|ext| ext == "log")
                        })
                        .collect()
                })
                .unwrap_or_default();
            logs.sort();
            for log in logs {
                eprintln!("--- {} (last 60 lines)", log.display());
                if let Ok(text) = std::fs::read(&log) {
                    eprint!("{}", tail(&String::from_utf8_lossy(&text), 60));
                }
            }
        }
        for mut worker in std::mem::take(&mut self.workers) {
            let _ = stop(&mut worker);
        }
    }
}

/// Returns the last `count` lines of `text`, as `tail -n` prints them.
pub fn tail(text: &str, count: usize) -> String {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    lines[lines.len().saturating_sub(count)..].concat()
}

/// Stops `worker` and waits at most 60s, then kills it. Returns its exit
/// code.
///
/// SIGTERM, because the old script started its workers with SIGINT
/// ignored. A worker that has already exited counts as stopped, as a
/// failed `kill` did.
fn stop(worker: &mut Worker) -> Result<u8> {
    if worker
        .child
        .try_wait()
        .context("cannot poll a worker")?
        .is_some()
    {
        return Ok(0);
    }
    let pid = worker.child.id();
    let _ = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .stderr(Stdio::null())
        .status();
    for _ in 0..WORKER_WAIT_SECS {
        if worker
            .child
            .try_wait()
            .context("cannot poll a worker")?
            .is_some()
        {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    if worker
        .child
        .try_wait()
        .context("cannot poll a worker")?
        .is_none()
    {
        eprintln!("worker {pid} did not stop after 60s; killing it");
        let _ = worker.child.kill();
    }
    let status = worker
        .child
        .wait()
        .with_context(|| format!("cannot wait for the {} worker", worker.label))?;
    Ok(shell_code(status))
}

/// Removes a directory tree, as `rm -rf` does.
fn remove_dir_all(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            Err(err).with_context(|| format!("cannot remove {}", path.display()))
        }
        _ => Ok(()),
    }
}

/// Copies one file, as `cp` does.
fn copy(from: &Path, to: &Path) -> Result<()> {
    std::fs::copy(from, to)
        .map(|_| ())
        .with_context(|| format!("cannot copy {} to {}", from.display(), to.display()))
}

/// Runs `command`, and fails with its exit code when it fails.
fn step(command: &mut Command) -> Result<()> {
    let status = spawn(command)?.wait().context("cannot wait for a step")?;
    check(status)
}

/// Runs `command` and returns its status and its standard output.
fn capture(command: &mut Command) -> Result<(ExitStatus, String)> {
    command.stdout(Stdio::piped());
    let output = spawn(command)?
        .wait_with_output()
        .context("cannot wait for a step")?;
    Ok((
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
    ))
}

/// Starts `command`. A missing program fails with 127, as in a shell.
fn spawn(command: &mut Command) -> Result<Child> {
    command.spawn().map_err(|err| {
        let program = command.get_program().to_string_lossy().into_owned();
        if err.kind() == std::io::ErrorKind::NotFound {
            eprintln!("run-mixed-version-smoke: {program}: command not found");
            StepFailed(127).into()
        } else {
            eprintln!("run-mixed-version-smoke: {program}: {err}");
            StepFailed(126).into()
        }
    })
}

/// Maps a failed status to its exit code, as a shell reports it.
fn check(status: ExitStatus) -> Result<()> {
    match shell_code(status) {
        0 => Ok(()),
        code => Err(StepFailed(code).into()),
    }
}

/// Returns the exit code that a shell reports for `status`.
fn shell_code(status: ExitStatus) -> u8 {
    if let Some(code) = status.code() {
        return u8::try_from(code & 0xff).unwrap_or(1);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt as _;
        if let Some(signal) = status.signal() {
            return u8::try_from(128 + (signal & 0x7f)).unwrap_or(1);
        }
    }
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    const TAGS: [&str; 8] = [
        "v0.5.9",
        "v0.6.0",
        "v0.6.10",
        "v0.6.2",
        "v0.7.0-rc.1",
        "v0.7.0",
        "v0.7.1",
        "v1.0.0",
    ];

    #[test]
    fn the_previous_release_is_the_highest_earlier_minor() {
        assert_eq!(previous_release("0.7.3", TAGS).as_deref(), Some("v0.6.10"));
        assert_eq!(previous_release("0.8.0", TAGS).as_deref(), Some("v0.7.1"));
        assert_eq!(
            previous_release("1.2.0-dev", TAGS).as_deref(),
            Some("v1.0.0")
        );
        assert_eq!(previous_release("1.0.0", TAGS).as_deref(), Some("v0.7.1"));
    }

    #[test]
    fn no_earlier_minor_gives_none() {
        assert_eq!(previous_release("0.5.0", TAGS), None);
        assert_eq!(previous_release("0.9.0", ["v0.9.1", "v0.8.0-rc.1"]), None);
        assert_eq!(previous_release("not a version", TAGS), None);
    }

    #[test]
    fn the_version_comes_from_the_first_version_line() {
        let manifest = "[package]\nname = \"x\"\n\n[workspace.package]\nversion = \"0.9.0\" # pinned\nversion = \"1.0.0\"\n";
        assert_eq!(
            workspace_version(manifest).as_deref(),
            Some("0.9.0 # pinned")
        );
        assert_eq!(workspace_version("name = \"x\"\n"), None);
        let real = crate::util::read_repo_file("Cargo.toml").unwrap();
        let version = workspace_version(&real).unwrap();
        assert!(version.split('.').count() >= 3, "{version}");
    }

    #[test]
    fn the_previous_release_picks_features_and_codec_scenarios() {
        assert_eq!(adapter_features("v0.6.4"), "v0_6");
        assert_eq!(adapter_features("v0.7.0"), "");
        assert_eq!(codec_scenarios("v0.6.4"), ["roll-forward"]);
        assert_eq!(codec_scenarios("v0.7.0"), SCENARIOS);
    }

    #[test]
    fn tail_keeps_the_last_lines() {
        assert_eq!(tail("a\nb\nc\n", 2), "b\nc\n");
        assert_eq!(tail("a\nb", 5), "a\nb");
        assert_eq!(tail("", 3), "");
    }
}
