//! Helpers that the perf evidence generators and the bench drivers share.
//!
//! The perf commands reach it as `super::support`. The bench commands and
//! `dev fuzz-smoke` reach it as `crate::perf::support`.
//!
//! The shell scripts that these commands replace ran under
//! `set -euo pipefail`. Each helper here keeps one part of that contract. A
//! failed child stops the command with the exit code of that child. A
//! `cmd 2>&1 | tee log` pipeline fails with the status of `cmd`. An `EXIT`
//! trap still runs when the operator presses Ctrl-C.

use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus, Stdio};

use regex::Regex;
use smop::prelude::*;

/// The admin connection string that the capture tests read.
pub const DB_URL_VAR: &str = "HARVEST_TEST_DATABASE_URL";

/// The notice that each capture prints when `HARVEST_TEST_DATABASE_URL` is unset.
pub const UNSET_NOTICE: &str = "== HARVEST_TEST_DATABASE_URL is unset -- relying on the \
     testcontainer fallback (Docker must be reachable). Set the variable to an admin \
     connection string, e.g. postgres://postgres:postgres@localhost:5432/postgres, to \
     skip Docker entirely. ==";

/// Where a command runs, and which `cargo` it starts.
///
/// The commands take a `Ctx` so that the unit tests can point them at a
/// scratch repository and a stub `cargo`.
#[derive(Debug, Clone)]
pub struct Ctx {
    /// The repository root. Every child process starts there.
    pub root: PathBuf,
    /// The `cargo` program for workspace builds.
    pub cargo: OsString,
}

impl Ctx {
    /// Returns the context of a real run.
    ///
    /// The `cargo` program follows `smop::sh::cargo`. It is `$CARGO` when
    /// that variable is set, as it is under `cargo xtask`, and `cargo` otherwise.
    pub fn from_env() -> Self {
        Self {
            root: crate::util::repo_root(),
            cargo: std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()),
        }
    }

    /// Returns `path` joined onto the repository root.
    pub fn path(&self, path: impl AsRef<Path>) -> PathBuf {
        self.root.join(path)
    }

    /// Returns a `cargo` command that runs at the repository root.
    pub fn cargo<I, S>(&self, args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(&self.cargo);
        command.args(args).current_dir(&self.root);
        command
    }
}

/// Returns the value of an environment variable, or `None` when it is unset or empty.
///
/// This is the shell test `[ -n "${NAME:-}" ]`.
pub fn non_empty_var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Returns the value of an environment variable, or `default` when it is unset or empty.
///
/// This is the shell expansion `${NAME:-default}`.
pub fn var_or(name: &str, default: &str) -> String {
    non_empty_var(name).unwrap_or_else(|| default.to_string())
}

/// Prints the testcontainer notice when `HARVEST_TEST_DATABASE_URL` is unset or empty.
pub fn print_unset_notice() {
    if non_empty_var(DB_URL_VAR).is_none() {
        println!("{UNSET_NOTICE}");
    }
}

/// Returns the shell exit code of a finished child.
///
/// A child that a signal killed reports `128 + signal`, as bash does.
pub fn status_code(status: ExitStatus) -> u8 {
    if let Some(code) = status.code() {
        return u8::try_from(code & 0xff).unwrap_or(1);
    }
    signal_code(status)
}

#[cfg(unix)]
fn signal_code(status: ExitStatus) -> u8 {
    use std::os::unix::process::ExitStatusExt;
    status
        .signal()
        .and_then(|signal| u8::try_from(128 + signal).ok())
        .unwrap_or(1)
}

#[cfg(not(unix))]
const fn signal_code(_status: ExitStatus) -> u8 {
    1
}

/// Returns the process exit code that repeats the exit code of a child.
pub fn exit_code(status: ExitStatus) -> ExitCode {
    ExitCode::from(status_code(status))
}

/// Defers Ctrl-C, `SIGTERM` and `SIGHUP` while the parent waits for a child.
///
/// A shell runs its `EXIT` trap when the operator interrupts a child. Rust
/// runs no `Drop` when a signal kills the process. So each wait raises a
/// shield. The signal still reaches the child, because the terminal sends it
/// to the whole process group. The child exits, the wait returns, and the
/// cleanup guards of the caller run.
///
/// A child inherits ignored signals. So the shield goes up only after the
/// spawn, and it comes down before the next spawn.
pub struct Shield {
    _private: (),
}

#[cfg(unix)]
mod signals {
    use std::os::raw::c_int;
    use std::sync::Mutex;

    pub const DEFERRED: [c_int; 3] = [1, 2, 15];
    pub const SIG_IGN: usize = 1;

    /// The number of raised shields, and the dispositions that the first one saved.
    ///
    /// Only the first shield changes the dispositions, and only the last one
    /// restores them. So overlapping shields cannot leave a signal ignored.
    pub static STATE: Mutex<(usize, [usize; 3])> = Mutex::new((0, [0; 3]));

    unsafe extern "C" {
        /// The C library `signal` call. A handler is a pointer-sized value.
        pub fn signal(signum: c_int, handler: usize) -> usize;
    }
}

impl Shield {
    /// Ignores the deferred signals until the shield drops.
    #[must_use]
    #[cfg_attr(
        not(unix),
        allow(
            clippy::missing_const_for_fn,
            reason = "only the Unix body makes a call that is not const"
        )
    )]
    pub fn raise() -> Self {
        #[cfg(unix)]
        {
            let mut state = signals::STATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.0 == 0 {
                for (slot, signum) in state.1.iter_mut().zip(signals::DEFERRED) {
                    // SAFETY: `signal` with `SIG_IGN` installs no Rust code as
                    // a handler. It only changes the disposition of this process.
                    *slot = unsafe { signals::signal(signum, signals::SIG_IGN) };
                }
            }
            state.0 += 1;
        }
        Self { _private: () }
    }
}

impl Drop for Shield {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            let mut state = signals::STATE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0 = state.0.saturating_sub(1);
            if state.0 == 0 {
                for (previous, signum) in state.1.iter().zip(signals::DEFERRED) {
                    // SAFETY: the value came from `signal` for the same signal.
                    // So it is a disposition that this process had before.
                    unsafe {
                        signals::signal(signum, *previous);
                    }
                }
            }
        }
    }
}

/// Starts `command`, and names the command line in the error.
fn spawn(command: &mut Command) -> Result<Child> {
    command
        .spawn()
        .with_context(|| format!("Failed to execute command: {}", describe(command)))
}

/// Renders a command line for an error message.
pub fn describe(command: &Command) -> String {
    let mut line = command.get_program().to_string_lossy().into_owned();
    for arg in command.get_args() {
        line.push(' ');
        line.push_str(&arg.to_string_lossy());
    }
    line
}

/// Waits for `child` behind a [`Shield`].
fn wait(child: &mut Child) -> Result<ExitStatus> {
    let _shield = Shield::raise();
    child.wait().context("Failed to wait for a child process")
}

/// Runs `command` with inherited stdio and returns its status.
pub fn status(command: &mut Command) -> Result<ExitStatus> {
    let mut child = spawn(command)?;
    wait(&mut child)
}

/// Runs `command` with its stdout sent to `stdout`, and returns its status.
pub fn status_to(command: &mut Command, stdout: impl Into<Stdio>) -> Result<ExitStatus> {
    command.stdout(stdout);
    status(command)
}

/// Opens `path` for `>` (truncate) or `>>` (append).
pub fn open_out(path: &Path, append: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true);
    if append {
        options.append(true);
    } else {
        options.write(true).truncate(true);
    }
    options
        .open(path)
        .with_context(|| format!("Failed to open {}", path.display()))
}

/// Runs `command` with `input` on its stdin, as a shell heredoc does.
pub fn status_with_stdin(command: &mut Command, input: &str) -> Result<ExitStatus> {
    command.stdin(Stdio::piped());
    let mut child = spawn(command)?;
    if let Some(mut stdin) = child.stdin.take() {
        // A child that exits early closes the pipe. The shell ignores that
        // too, and the exit status of the child reports the failure.
        let _ = stdin.write_all(input.as_bytes());
    }
    wait(&mut child)
}

/// Runs `command` and returns its status and its stdout, as `$(command)` does.
///
/// The child inherits stderr. The trailing newlines of stdout are removed.
pub fn capture(command: &mut Command) -> Result<(ExitStatus, String)> {
    command.stdout(Stdio::piped());
    let mut child = spawn(command)?;
    let mut stdout = Vec::new();
    let read = {
        let _shield = Shield::raise();
        child
            .stdout
            .take()
            .map_or(Ok(0), |mut pipe| pipe.read_to_end(&mut stdout))
    };
    let status = wait(&mut child)?;
    read.context("Failed to read the output of a child process")?;
    Ok((status, trim_newlines(&String::from_utf8_lossy(&stdout))))
}

/// Runs `command` with stderr merged into stdout, and returns both, as `$(command 2>&1)` does.
pub fn capture_merged(command: &mut Command) -> Result<(ExitStatus, String)> {
    let mut out = Vec::new();
    let status = merged(command, &mut out)?;
    Ok((status, trim_newlines(&String::from_utf8_lossy(&out))))
}

/// Removes the trailing newlines, as command substitution does.
pub fn trim_newlines(text: &str) -> String {
    text.trim_end_matches('\n').to_string()
}

/// Runs `command` as `command 2>&1 | tee log`.
///
/// The merged stream goes to this process's stdout and to `log`. The status
/// is the status of `command`, as `pipefail` reports it.
pub fn tee_merged(command: &mut Command, log: &Path) -> Result<ExitStatus> {
    let mut file =
        File::create(log).with_context(|| format!("Failed to create {}", log.display()))?;
    let mut sink = Tee {
        first: io::stdout(),
        second: &mut file,
    };
    merged(command, &mut sink)
}

/// Runs `command` as `command | tee out`. Only stdout goes through the tee.
pub fn tee_stdout(command: &mut Command, out: &Path) -> Result<ExitStatus> {
    let mut file =
        File::create(out).with_context(|| format!("Failed to create {}", out.display()))?;
    let mut sink = Tee {
        first: io::stdout(),
        second: &mut file,
    };
    pipe_stdout(command, &mut sink)
}

/// Runs `command` and copies its stdout to `sink`. The child inherits stderr.
pub fn pipe_stdout(command: &mut Command, sink: &mut dyn Write) -> Result<ExitStatus> {
    command.stdout(Stdio::piped());
    let mut child = spawn(command)?;
    let copied = {
        let _shield = Shield::raise();
        child
            .stdout
            .take()
            .map_or(Ok(0), |mut pipe| io::copy(&mut pipe, sink))
    };
    let status = wait(&mut child)?;
    copied.context("Failed to copy the output of a child process")?;
    Ok(status)
}

/// Runs `command` with stdout and stderr on one pipe, and copies that pipe to `sink`.
fn merged(command: &mut Command, sink: &mut dyn Write) -> Result<ExitStatus> {
    let (mut reader, writer) = io::pipe().context("Failed to open a pipe")?;
    let writer_err = writer.try_clone().context("Failed to clone a pipe")?;
    command.stdout(writer).stderr(writer_err);
    let mut child = spawn(command)?;
    // The command still holds the write ends. Replace them, so the pipe
    // closes when the child exits and the copy below sees end of file.
    command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    let copied = {
        let _shield = Shield::raise();
        io::copy(&mut reader, sink)
    };
    // A failed copy stops early, before end of file. Close the read end
    // first, so the child gets a broken pipe and the wait returns.
    drop(reader);
    let status = wait(&mut child)?;
    copied.context("Failed to copy the output of a child process")?;
    Ok(status)
}

/// A writer that sends each write to two writers.
pub struct Tee<A: Write, B: Write> {
    /// The first writer, usually stdout.
    pub first: A,
    /// The second writer, usually a file.
    pub second: B,
}

impl<A: Write, B: Write> Write for Tee<A, B> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.second.write_all(buf)?;
        // `tee` keeps writing the file when stdout goes away.
        let _ = self.first.write_all(buf).and_then(|()| self.first.flush());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let _ = self.first.flush();
        self.second.flush()
    }
}

/// Counts the lines of `path` that match `pattern`, as `grep -c` does.
///
/// The patterns of the scripts are basic regular expressions without
/// special characters other than `^` and `.`. The `regex` crate reads both
/// the same way.
pub fn grep_count(path: &Path, pattern: &str) -> Result<usize> {
    let regex = Regex::new(pattern).with_context(|| format!("bad pattern {pattern:?}"))?;
    let bytes =
        std::fs::read(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let text = String::from_utf8_lossy(&bytes);
    Ok(text.lines().filter(|line| regex.is_match(line)).count())
}

/// Lists `dir` with `ls -la`, and returns the exit code of `ls`.
pub fn ls_la(dir: &Path) -> Result<ExitCode> {
    let status = sh::cmd("ls").arg("-la").arg(dir).status()?;
    Ok(exit_code(status))
}

/// The final report of a script that ends with `ls -la "$OUT_DIR"`.
pub fn report_artifacts(out_dir: &Path) -> Result<ExitCode> {
    println!("== done. Artifacts in {} ==", out_dir.display());
    ls_la(out_dir)
}

/// The kind of completion line that a capture test prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The test prints `equivalence confirmed:` after a before and after pair.
    Equivalence,
    /// The test prints a `capture complete` or `done` line.
    Completion,
}

/// One `#[ignore]`d evidence-capture test that runs once.
#[derive(Debug, Clone, Copy)]
pub struct Capture {
    /// The libtest filter of the capture test.
    pub filter: &'static str,
    /// The log that the run writes, a fixed path under `/tmp`.
    pub log: &'static str,
    /// The artifact directory, relative to the repository root.
    ///
    /// It must match the fixed path that the Rust test writes to. The test
    /// derives it from `CARGO_MANIFEST_DIR` at compile time, so it cannot
    /// be overridden from here. It is display-only, for the final `ls`.
    pub out_dir: &'static str,
    /// Whether the intro line says "before/after evidence".
    pub before_after: bool,
    /// The regular expression that a complete run prints at the start of a line.
    pub marker: &'static str,
    /// Which failure text a run without the marker prints.
    pub verdict: Verdict,
}

impl Capture {
    /// Returns the line that opens the capture.
    pub fn intro(&self) -> String {
        if self.before_after {
            format!("== capturing before/after evidence via {} ==", self.filter)
        } else {
            format!("== capturing via {} ==", self.filter)
        }
    }

    /// Returns the line that a run without the marker prints to stderr.
    pub fn fatal(&self) -> String {
        match self.verdict {
            Verdict::Equivalence => format!(
                "FATAL: the capture run did not report equivalence -- either it skipped (no \
                 database reachable) or the equivalence assertion inside the test itself \
                 failed. Check {}.",
                self.log
            ),
            Verdict::Completion => format!(
                "FATAL: the test run did not report capture completion -- either the capture \
                 test failed, or it SKIPped for lack of a reachable database. Check the log \
                 at {}.",
                self.log
            ),
        }
    }
}

/// The `cargo test` arguments that run one capture test of the integration suite.
pub fn capture_test_args(filter: &str) -> Vec<String> {
    [
        "test",
        "-p",
        "autumn-harvest",
        "--features",
        "db,testing",
        "--test",
        "integration",
        "--",
        "--ignored",
        "--nocapture",
        filter,
    ]
    .iter()
    .map(ToString::to_string)
    .collect()
}

/// Runs one capture test, checks its completion line, and lists the artifacts.
pub fn run_capture(ctx: &Ctx, capture: &Capture) -> Result<ExitCode> {
    print_unset_notice();
    println!("{}", capture.intro());
    let log = Path::new(capture.log);
    let status = tee_merged(&mut ctx.cargo(capture_test_args(capture.filter)), log)?;
    if !status.success() {
        return Ok(exit_code(status));
    }
    if grep_count(log, capture.marker)? == 0 {
        eprintln!("{}", capture.fatal());
        return Ok(ExitCode::FAILURE);
    }
    report_artifacts(&ctx.path(capture.out_dir))
}

/// The production file that a toggled capture reverts for its "before" half.
pub const QUEUE_RS: &str = "autumn-harvest/src/queue.rs";

/// One evidence-capture test that runs twice: on the tree as checked out, then
/// on `queue.rs` without the fix.
#[derive(Debug, Clone, Copy)]
pub struct Toggle {
    /// The libtest filter of the capture test.
    pub filter: &'static str,
    /// The artifact directory, relative to the repository root.
    ///
    /// It must match the fixed path that the Rust test writes to, as
    /// [`Capture::out_dir`] explains. It is display-only.
    pub out_dir: &'static str,
    /// A string that only the fixed `queue.rs` contains.
    pub fix_marker: &'static str,
    /// The log path of each half, without the label and the `.log` suffix.
    pub log_stem: &'static str,
}

impl Toggle {
    /// Returns the log path of the half with this label.
    pub fn log(&self, label: &str) -> String {
        format!("{}{label}.log", self.log_stem)
    }
}

/// How the "before" half changed `queue.rs`, and so how to undo it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreMode {
    /// Nothing to restore yet.
    None,
    /// The dev-loop half. `git stash pop --index` restores it.
    Stash,
    /// The clean-checkout half. `git checkout -- queue.rs` restores it.
    Checkout,
}

/// Restores `queue.rs` on every exit path, as the `EXIT` trap did.
///
/// The mode is set right before the matching mutation. So the guard knows
/// what to undo, whichever exit path runs.
pub struct Restore<'a> {
    ctx: &'a Ctx,
    /// What the guard undoes when it runs.
    pub mode: RestoreMode,
}

impl<'a> Restore<'a> {
    /// Returns a guard with nothing to restore yet.
    pub const fn new(ctx: &'a Ctx) -> Self {
        Self {
            ctx,
            mode: RestoreMode::None,
        }
    }

    /// Undoes the mutation, and returns the exit code of the git command.
    ///
    /// A failed git command leaves the mode set. The drop then tries once
    /// more, as the `EXIT` trap did after a failed explicit restore.
    pub fn restore(&mut self) -> Result<ExitCode> {
        let status = match self.mode {
            RestoreMode::Stash => {
                println!("== restoring {QUEUE_RS} (git stash pop --index) ==");
                // `--index` is required, not cosmetic. Without it,
                // `git stash pop` reapplies the stashed content as a plain
                // unstaged change. It does so whether the content was
                // staged, unstaged or mixed when `git stash push` saved it.
                // That would collapse the index state of the caller on every
                // dev-loop run.
                Some(
                    sh::cmd("git")
                        .args(["stash", "pop", "--index"])
                        .dir(&self.ctx.root)
                        .status()?,
                )
            }
            RestoreMode::Checkout => {
                println!("== restoring {QUEUE_RS} (git checkout) ==");
                Some(
                    sh::cmd("git")
                        .args(["checkout", "--", QUEUE_RS])
                        .dir(&self.ctx.root)
                        .status()?,
                )
            }
            RestoreMode::None => None,
        };
        match status {
            Some(status) if !status.success() => Ok(exit_code(status)),
            _ => {
                self.mode = RestoreMode::None;
                Ok(ExitCode::SUCCESS)
            }
        }
    }
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

/// Runs one half of a toggled capture, and checks that it reported its label.
pub fn toggle_half(ctx: &Ctx, toggle: &Toggle, label: &str) -> Result<ExitCode> {
    println!("== capturing '{label}' via {} ==", toggle.filter);
    let log = toggle.log(label);
    let status = tee_merged(
        &mut ctx.cargo(capture_test_args(toggle.filter)),
        Path::new(&log),
    )?;
    if !status.success() {
        return Ok(exit_code(status));
    }
    let pattern = format!("^== capture complete: label={label},");
    if grep_count(Path::new(&log), &pattern)? < 1 {
        eprintln!(
            "FATAL: the test run did not report 'label={label}' -- either \
             queue::claim_task_query() was not in the expected shape for this half of the \
             repro (see the before-half setup above/below), or the capture test itself \
             failed/skipped. Check the log at {log}."
        );
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

/// The outcome of the search for a revision of `queue.rs` without the fix.
#[derive(Debug, PartialEq, Eq)]
pub enum PreFix {
    /// The newest commit whose `queue.rs` lacks the fix marker.
    Found(String),
    /// Every commit that touches `queue.rs` already holds the marker.
    NotFound,
}

/// Walks `git log` for `queue.rs` to the newest commit without the fix marker.
///
/// A failed `git show` aborts with the error text. It must never read as
/// "marker not found". Otherwise a transient failure on the fix commit itself
/// would make that commit the pre-fix baseline. The capture would then
/// compare the fixed query against itself.
pub fn find_pre_fix_commit(
    ctx: &Ctx,
    fix_marker: &str,
) -> Result<std::result::Result<PreFix, String>> {
    // The shell read this list from a process substitution, which ignores
    // the exit status of `git log`. A failed walk finds no commit instead.
    let (_, log) = capture(
        Command::new("git")
            .args(["log", "--format=%H", "--", QUEUE_RS])
            .current_dir(&ctx.root),
    )?;
    for sha in log.lines() {
        let (status, content) = capture_merged(
            Command::new("git")
                .arg("show")
                .arg(format!("{sha}:{QUEUE_RS}"))
                .current_dir(&ctx.root),
        )?;
        if !status.success() {
            return Ok(Err(format!(
                "FATAL: 'git show {sha}:{QUEUE_RS}' failed -- cannot walk history to find a \
                 pre-fix revision:\n{content}"
            )));
        }
        // The shell used a herestring here, not `printf ... | grep -q`. On
        // content of this size, `grep -q` can exit before the producer
        // finishes. The producer then gets SIGPIPE, and `pipefail` turns a
        // match into a failure. A substring test has no second process, so
        // nothing can race.
        if !content.contains(fix_marker) {
            return Ok(Ok(PreFix::Found(sha.to_string())));
        }
    }
    Ok(Ok(PreFix::NotFound))
}

/// Runs a toggled capture: "after" on the tree as checked out, then "before".
pub fn run_toggle(ctx: &Ctx, toggle: &Toggle) -> Result<ExitCode> {
    print_unset_notice();
    let mut restore = Restore::new(ctx);

    println!("== half one: 'after' -- the tree as checked out ==");
    let code = toggle_half(ctx, toggle, "after")?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }

    println!("== half two: 'before' -- {QUEUE_RS} with the fix temporarily removed ==");
    let unchanged = sh::cmd("git")
        .args(["diff", "--quiet", "--", QUEUE_RS])
        .dir(&ctx.root)
        .status()?
        .success();
    if unchanged {
        println!(
            "== clean checkout: {QUEUE_RS} has no uncommitted diff -- looking up the last \
             commit predating the fix marker '{}' ==",
            toggle.fix_marker
        );
        let pre_fix = match find_pre_fix_commit(ctx, toggle.fix_marker)? {
            Ok(PreFix::Found(sha)) => sha,
            Ok(PreFix::NotFound) => {
                eprintln!(
                    "FATAL: every commit touching {QUEUE_RS} in this history already contains \
                     the fix marker '{}' -- could not locate a pre-fix revision to reproduce \
                     the baseline from.",
                    toggle.fix_marker
                );
                return Ok(ExitCode::FAILURE);
            }
            Err(fatal) => {
                eprintln!("{fatal}");
                return Ok(ExitCode::FAILURE);
            }
        };
        println!("== using {QUEUE_RS} from {pre_fix} (pre-fix) ==");
        // The mode is set BEFORE the mutation below. A failure in the
        // redirect itself then still leaves the guard knowing to check the
        // file out again. The redirect can create or truncate the file
        // before it fails part way through.
        restore.mode = RestoreMode::Checkout;
        // `git show` writes the raw bytes straight to the file. No string
        // round trip can alter them, trailing newlines included.
        let file = open_out(&ctx.path(QUEUE_RS), false)?;
        let shown = status_to(
            Command::new("git")
                .arg("show")
                .arg(format!("{pre_fix}:{QUEUE_RS}"))
                .current_dir(&ctx.root),
            file,
        )?;
        if !shown.success() {
            eprintln!(
                "FATAL: 'git show {pre_fix}:{QUEUE_RS}' failed on the materializing redirect, \
                 despite succeeding moments earlier during the search above -- transient \
                 error; restoring {QUEUE_RS} and aborting."
            );
            return Ok(ExitCode::FAILURE);
        }
    } else {
        println!("== dev-loop checkout: stashing the uncommitted {QUEUE_RS} diff ==");
        let stashed = sh::cmd("git")
            .args(["stash", "push", "--quiet", "--", QUEUE_RS])
            .dir(&ctx.root)
            .status()?;
        if !stashed.success() {
            return Ok(exit_code(stashed));
        }
        restore.mode = RestoreMode::Stash;
    }

    let code = toggle_half(ctx, toggle, "before")?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    let code = restore.restore()?;
    if code != ExitCode::SUCCESS {
        return Ok(code);
    }
    drop(restore);

    report_artifacts(&ctx.path(toggle.out_dir))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that refuses every write, as a log on a full disk does.
    struct FullDisk;

    impl Write for FullDisk {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("no space left on device"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A failed log write must not leave the child blocked on a full pipe.
    /// `tee` exits there, so the child gets a broken pipe and stops.
    #[cfg(unix)]
    #[test]
    fn a_failed_sink_does_not_hang_the_merged_pipe() {
        let (done, outcome) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = merged(&mut Command::new("yes"), &mut FullDisk);
            let _ = done.send(result.is_err());
        });
        let failed = outcome
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("merged must return after a failed write");
        assert!(failed, "a failed log write must fail the capture");
    }

    #[test]
    fn var_or_treats_an_empty_value_as_unset() {
        assert_eq!(
            var_or("XTASK_SUPPORT_TEST_SURELY_UNSET", "fallback"),
            "fallback"
        );
    }

    #[test]
    fn trim_newlines_strips_only_trailing_newlines() {
        assert_eq!(trim_newlines("a\nb\n\n"), "a\nb");
        assert_eq!(trim_newlines("\n a \n"), "\n a ");
        assert_eq!(trim_newlines(""), "");
    }

    #[test]
    fn capture_intro_and_fatal_text_match_the_scripts() {
        let capture = Capture {
            filter: "suite::zz_capture",
            log: "/tmp/x.log",
            out_dir: "docs/perf-artifacts/x",
            before_after: true,
            marker: "^equivalence confirmed:",
            verdict: Verdict::Equivalence,
        };
        assert_eq!(
            capture.intro(),
            "== capturing before/after evidence via suite::zz_capture =="
        );
        assert!(capture.fatal().ends_with("failed. Check /tmp/x.log."));
        let completion = Capture {
            before_after: false,
            verdict: Verdict::Completion,
            ..capture
        };
        assert_eq!(completion.intro(), "== capturing via suite::zz_capture ==");
        assert!(
            completion
                .fatal()
                .contains("SKIPped for lack of a reachable database. Check the log at /tmp/x.log.")
        );
    }

    #[test]
    fn unset_notice_reads_as_one_line() {
        assert!(!UNSET_NOTICE.contains('\n'));
        assert!(UNSET_NOTICE.contains("testcontainer fallback (Docker must be reachable)."));
        assert!(UNSET_NOTICE.contains("connection string, e.g. postgres://"));
    }

    #[test]
    fn grep_count_anchors_each_line() {
        let dir = fs::temp_dir().unwrap();
        let log = dir.path().join("log");
        fs::write_string(&log, "x equivalence confirmed:\nequivalence confirmed: 3\n").unwrap();
        assert_eq!(grep_count(&log, "^equivalence confirmed:").unwrap(), 1);
        assert_eq!(grep_count(&log, "^== done. Artifacts in ").unwrap(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn tee_merged_keeps_both_streams_and_the_exit_code() {
        let dir = fs::temp_dir().unwrap();
        let log = dir.path().join("log");
        let mut command = Command::new("sh");
        command.args(["-c", "echo out; echo err >&2; exit 3"]);
        let status = tee_merged(&mut command, &log).unwrap();
        assert_eq!(status_code(status), 3);
        let text = fs::read_string(&log).unwrap();
        assert!(text.contains("out\n") && text.contains("err\n"), "{text}");
    }

    #[cfg(unix)]
    #[test]
    fn status_code_maps_a_signal_to_128_plus_the_signal() {
        // `SIGKILL`, because a parallel test can hold a shield, and a child
        // inherits the signals that its parent ignores.
        let mut command = Command::new("sh");
        command.args(["-c", "kill -KILL $$"]);
        let status = status(&mut command).unwrap();
        assert_eq!(status_code(status), 137);
    }

    #[cfg(unix)]
    #[test]
    fn capture_strips_trailing_newlines_and_feeds_stdin() {
        let mut command = Command::new("printf");
        command.arg("a\\n\\n");
        let (status, out) = capture(&mut command).unwrap();
        assert!(status.success());
        assert_eq!(out, "a");
        let dir = fs::temp_dir().unwrap();
        let out = dir.path().join("out");
        let mut cat = Command::new("cat");
        cat.stdout(open_out(&out, false).unwrap());
        assert!(status_with_stdin(&mut cat, "fed\n").unwrap().success());
        assert_eq!(fs::read_string(&out).unwrap(), "fed\n");
    }

    /// Runs git in `dir` and asserts that it succeeds.
    #[cfg(unix)]
    fn git(dir: &Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// A scratch repository with one commit per `queue.rs` content, and a stub `cargo`.
    ///
    /// The stub reports the label that matches the `queue.rs` it sees, and it
    /// keeps a copy of that file. Its "before" half fails with 7 when the
    /// file `fail-before` exists.
    #[cfg(unix)]
    fn scratch_repo(commits: &[&str]) -> (fs::TempDir, Ctx, Toggle) {
        use std::os::unix::fs::PermissionsExt;
        let dir = fs::temp_dir().unwrap();
        let root = dir.path().join("repo");
        fs::create_dir_all(root.join("autumn-harvest/src")).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.name", "xtask"]);
        git(&root, &["config", "user.email", "xtask@example.invalid"]);
        git(&root, &["config", "commit.gpgsign", "false"]);
        for (index, content) in commits.iter().enumerate() {
            fs::write_string(root.join(QUEUE_RS), content).unwrap();
            git(&root, &["add", "-A"]);
            git(&root, &["commit", "-q", "-m", &format!("c{index}")]);
        }
        let stub = dir.path().join("cargo");
        fs::write_string(
            &stub,
            "#!/bin/sh\n\
             if grep -q MARKER autumn-harvest/src/queue.rs; then label=after; else label=before; fi\n\
             cp autumn-harvest/src/queue.rs \"../seen-$label\"\n\
             if [ $label = before ] && [ -e ../fail-before ]; then exit 7; fi\n\
             echo \"== capture complete: label=$label, ok\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let stem = format!("{}/log-", dir.path().display());
        let toggle = Toggle {
            filter: "suite::zz_capture",
            out_dir: "out",
            fix_marker: "MARKER",
            log_stem: Box::leak(stem.into_boxed_str()),
        };
        fs::create_dir_all(root.join("out")).unwrap();
        let ctx = Ctx {
            root,
            cargo: stub.into_os_string(),
        };
        (dir, ctx, toggle)
    }

    #[cfg(unix)]
    #[test]
    fn toggle_materializes_the_pre_fix_commit_and_restores_queue_rs() {
        let (dir, ctx, toggle) = scratch_repo(&["pre-fix query\n", "MARKER v1\n", "MARKER v2\n"]);
        let code = run_toggle(&ctx, &toggle).unwrap();
        assert_eq!(code, ExitCode::SUCCESS);
        let seen_before = fs::read_string(dir.path().join("seen-before")).unwrap();
        assert_eq!(seen_before, "pre-fix query\n");
        let seen_after = fs::read_string(dir.path().join("seen-after")).unwrap();
        assert_eq!(seen_after, "MARKER v2\n");
        assert_eq!(fs::read_string(ctx.path(QUEUE_RS)).unwrap(), "MARKER v2\n");
        assert_eq!(git(&ctx.root, &["status", "--porcelain"]), "");
        let log = fs::read_string(toggle.log("before")).unwrap();
        assert!(log.contains("label=before,"), "{log}");
    }

    #[cfg(unix)]
    #[test]
    fn toggle_stashes_an_uncommitted_fix_and_pops_it_with_the_index() {
        // The staged fix plus an unstaged edit. `git diff --quiet` sees the
        // unstaged edit, so the command takes the stash path.
        let (dir, ctx, toggle) = scratch_repo(&["pre-fix query\n"]);
        fs::write_string(ctx.path(QUEUE_RS), "MARKER staged\n").unwrap();
        git(&ctx.root, &["add", QUEUE_RS]);
        fs::write_string(ctx.path(QUEUE_RS), "MARKER staged\nunstaged\n").unwrap();
        let code = run_toggle(&ctx, &toggle).unwrap();
        assert_eq!(code, ExitCode::SUCCESS);
        let seen_before = fs::read_string(dir.path().join("seen-before")).unwrap();
        assert_eq!(seen_before, "pre-fix query\n");
        assert_eq!(
            fs::read_string(ctx.path(QUEUE_RS)).unwrap(),
            "MARKER staged\nunstaged\n"
        );
        assert_eq!(
            git(&ctx.root, &["status", "--porcelain"]),
            "MM autumn-harvest/src/queue.rs\n"
        );
        assert_eq!(
            git(&ctx.root, &["show", &format!(":{QUEUE_RS}")]),
            "MARKER staged\n"
        );
        assert_eq!(git(&ctx.root, &["stash", "list"]), "");
    }

    #[cfg(unix)]
    #[test]
    fn toggle_restores_a_fully_staged_fix_from_the_index() {
        // `git diff --quiet` compares the tree with the index only. So a
        // staged fix takes the history path, as it did in the shell.
        let (dir, ctx, toggle) = scratch_repo(&["pre-fix query\n"]);
        fs::write_string(ctx.path(QUEUE_RS), "MARKER staged\n").unwrap();
        git(&ctx.root, &["add", QUEUE_RS]);
        let code = run_toggle(&ctx, &toggle).unwrap();
        assert_eq!(code, ExitCode::SUCCESS);
        let seen_before = fs::read_string(dir.path().join("seen-before")).unwrap();
        assert_eq!(seen_before, "pre-fix query\n");
        assert_eq!(
            fs::read_string(ctx.path(QUEUE_RS)).unwrap(),
            "MARKER staged\n"
        );
        assert_eq!(
            git(&ctx.root, &["status", "--porcelain"]),
            "M  autumn-harvest/src/queue.rs\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn toggle_restores_queue_rs_when_the_before_half_fails() {
        let (dir, ctx, toggle) = scratch_repo(&["pre-fix query\n", "MARKER v1\n"]);
        fs::write_string(dir.path().join("fail-before"), "").unwrap();
        let code = run_toggle(&ctx, &toggle).unwrap();
        assert_eq!(code, ExitCode::from(7));
        assert_eq!(fs::read_string(ctx.path(QUEUE_RS)).unwrap(), "MARKER v1\n");
        assert_eq!(git(&ctx.root, &["status", "--porcelain"]), "");
    }

    #[cfg(unix)]
    #[test]
    fn toggle_fails_when_every_commit_has_the_fix() {
        let (_dir, ctx, toggle) = scratch_repo(&["MARKER v1\n", "MARKER v2\n"]);
        assert_eq!(
            find_pre_fix_commit(&ctx, toggle.fix_marker).unwrap(),
            Ok(PreFix::NotFound)
        );
        assert_eq!(run_toggle(&ctx, &toggle).unwrap(), ExitCode::FAILURE);
        assert_eq!(fs::read_string(ctx.path(QUEUE_RS)).unwrap(), "MARKER v2\n");
    }

    #[cfg(unix)]
    #[test]
    fn run_capture_fails_without_the_completion_marker() {
        let (dir, ctx, _toggle) = scratch_repo(&["MARKER v1\n"]);
        let log = format!("{}/capture.log", dir.path().display());
        let capture = Capture {
            filter: "suite::zz_capture",
            log: Box::leak(log.into_boxed_str()),
            out_dir: "out",
            before_after: false,
            marker: "^== capture complete: artifacts in ",
            verdict: Verdict::Completion,
        };
        assert_eq!(run_capture(&ctx, &capture).unwrap(), ExitCode::FAILURE);
        let passing = Capture {
            marker: "^== capture complete: label=after,",
            ..capture
        };
        assert_eq!(run_capture(&ctx, &passing).unwrap(), ExitCode::SUCCESS);
    }
}
