//! The Linux process executor: run one [`ExecPlan`] as a child process.
//!
//! The host `ProcessBackend` uses it, and so does the guest agent inside each
//! microVM. Inside a microVM it is a second layer. On the host it is the only
//! layer, and it shares the host kernel.
//!
//! What the child gets:
//!
//! - a new private work directory, removed after the run;
//! - no host environment, only `PATH`, `HOME`, `TMPDIR`, `LANG` and the plan
//!   variables;
//! - a new session, so the executor can kill the whole process group;
//! - every inherited descriptor above 2 marked close-on-exec;
//! - an empty signal mask;
//! - `PR_SET_NO_NEW_PRIVS`;
//! - rlimits for CPU, data memory, file size, open files and core dumps;
//! - an empty network namespace when the plan denies the network;
//! - a dedicated uid and gid when the executor runs as root.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::error::SandboxError;
use super::guest::PlanExecutor;
use super::outcome::SandboxOutcome;
use super::request::ExecPlan;
use super::runtime::BoxFuture;

/// The uid and gid of the code when the executor runs as root and the
/// options name none: `nobody`.
pub const NOBODY: (u32, u32) = (65_534, 65_534);

/// The time that the executor waits for the pipes to close after the main
/// process exits and the group is killed.
pub const PIPE_DRAIN_GRACE: Duration = Duration::from_secs(2);

/// How the executor runs plans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOptions {
    /// The directory that holds the work directory of each run.
    pub scratch_root: PathBuf,
    /// The uid and gid of the code. `None` keeps the uid of the executor, or
    /// uses [`NOBODY`] when the executor runs as root.
    pub run_as: Option<(u32, u32)>,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            scratch_root: std::env::temp_dir(),
            run_as: None,
        }
    }
}

/// A [`PlanExecutor`] that runs each plan with [`execute_plan`].
///
/// The guest agent uses it inside the microVM.
#[derive(Debug, Clone, Default)]
pub struct ProcessExecutor {
    options: ExecOptions,
}

impl ProcessExecutor {
    /// An executor with these options.
    #[must_use]
    pub const fn new(options: ExecOptions) -> Self {
        Self { options }
    }
}

impl PlanExecutor for ProcessExecutor {
    fn execute<'a>(
        &'a self,
        plan: &'a ExecPlan,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SandboxOutcome, SandboxError>> {
        Box::pin(execute_plan(plan, &self.options, cancel))
    }
}

/// Run `plan` to an outcome.
///
/// # Errors
///
/// Returns [`SandboxError::Denied`] when the plan fails its check or the
/// command cannot start, [`SandboxError::Unavailable`] when the executor
/// cannot isolate the network or set up the work directory, and
/// [`SandboxError::Cancelled`] when `cancel` fires. The executor kills the
/// process group and removes the work directory in each case.
pub async fn execute_plan(
    plan: &ExecPlan,
    options: &ExecOptions,
    cancel: &CancellationToken,
) -> Result<SandboxOutcome, SandboxError> {
    let _ = (plan, options, cancel);
    todo!("execute_plan")
}

/// Whether this host lets the executor create an empty network namespace.
///
/// The probe starts `/bin/sh -c true` with the same namespace step that a
/// run uses.
pub async fn network_isolation_available() -> bool {
    todo!("network_isolation_available")
}

/// Remove a work directory, and fix the modes that the code changed first.
///
/// # Errors
///
/// Returns the I/O error of the final removal.
pub fn remove_work_dir(dir: &Path) -> std::io::Result<()> {
    let _ = dir;
    todo!("remove_work_dir")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use super::*;
    use crate::sandbox::outcome::RunStatus;
    use crate::sandbox::policy::{NetworkPolicy, SandboxPolicy};
    use crate::sandbox::request::{SandboxFile, SandboxRequest};

    fn options() -> ExecOptions {
        ExecOptions::default()
    }

    fn plan(request: &SandboxRequest) -> ExecPlan {
        SandboxPolicy::for_development()
            .with_network(NetworkPolicy::Allow)
            .plan(request)
            .expect("valid request")
    }

    async fn run(request: SandboxRequest) -> SandboxOutcome {
        execute_plan(&plan(&request), &options(), &CancellationToken::new())
            .await
            .expect("the run must reach an outcome")
    }

    fn sh(code: &str) -> SandboxRequest {
        SandboxRequest::new("sh", code)
    }

    #[tokio::test]
    async fn exit_code_stdout_and_stderr_are_captured() {
        let out = run(sh("echo hello; echo oops >&2; exit 3")).await;
        assert_eq!(out.status, RunStatus::Exited { code: 3 });
        assert_eq!(out.stdout, "hello\n");
        assert_eq!(out.stderr, "oops\n");
        assert_eq!(out.stdout_bytes, 6);
        assert!(!out.stdout_truncated);
        assert!(!out.success());
    }

    #[tokio::test]
    async fn stdin_and_arguments_reach_the_code() {
        let out = run(sh("cat; echo \" $1 $2\"")
            .with_stdin("from stdin")
            .with_arg("a")
            .with_arg("b c"))
        .await;
        assert_eq!(out.stdout, "from stdin a b c\n");
    }

    #[tokio::test]
    async fn the_child_sees_no_host_environment() {
        let out = run(sh("env | cut -d= -f1 | sort").with_env("MY_VAR", "1")).await;
        let names: BTreeSet<&str> = out.stdout.lines().collect();
        let allowed: BTreeSet<&str> = [
            "HOME", "LANG", "MY_VAR", "OLDPWD", "PATH", "PWD", "SHLVL", "TMPDIR", "_",
        ]
        .into_iter()
        .collect();
        let extra: Vec<&&str> = names.difference(&allowed).collect();
        assert!(extra.is_empty(), "unexpected variables: {extra:?}");
        assert!(names.contains("MY_VAR"));
    }

    #[tokio::test]
    async fn files_are_written_and_an_executable_file_runs() {
        let mut request = sh("./bin/tool; cat data/in.txt");
        request.files.push(SandboxFile {
            path: "bin/tool".into(),
            text: Some("#!/bin/sh\necho tool ran\n".into()),
            executable: true,
            ..SandboxFile::default()
        });
        let out = run(request.with_file("data/in.txt", "data\n")).await;
        assert_eq!(out.stdout, "tool ran\ndata\n");
    }

    #[tokio::test]
    async fn the_work_directory_is_private_and_removed() {
        let out = run(sh("pwd; stat -c %a .")).await;
        let mut lines = out.stdout.lines();
        let dir = PathBuf::from(lines.next().unwrap());
        assert_eq!(lines.next(), Some("700"));
        assert!(!dir.exists(), "{} must be removed", dir.display());
    }

    #[tokio::test]
    async fn a_timeout_kills_the_run_and_is_an_outcome() {
        let started = Instant::now();
        let out = run(sh("echo before; sleep 30").with_timeout_ms(300)).await;
        assert_eq!(out.status, RunStatus::TimedOut);
        assert_eq!(out.stdout, "before\n");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn a_background_process_is_killed_when_the_main_process_exits() {
        let out = run(sh("sleep 300 & echo $!")).await;
        let pid: i32 = out.stdout.trim().parse().unwrap();
        assert!(process_is_gone(pid), "pid {pid} must not outlive the run");
    }

    #[tokio::test]
    async fn a_cancel_kills_the_run() {
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            trigger.cancel();
        });
        let started = Instant::now();
        let err = execute_plan(&plan(&sh("sleep 30")), &options(), &cancel)
            .await
            .expect_err("cancelled");
        assert_eq!(err, SandboxError::Cancelled);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn output_keeps_the_head_and_the_tail() {
        let mut request = sh("echo START; head -c 1000000 /dev/zero | tr '\\0' x; echo; echo END");
        request.limits.max_output_bytes = Some(100);
        let out = run(request).await;
        assert!(out.stdout_truncated);
        assert_eq!(out.stdout_bytes, 1_000_011);
        assert!(out.stdout.starts_with("START\n"), "{}", out.stdout);
        assert!(out.stdout.ends_with("END\n"), "{}", out.stdout);
    }

    #[tokio::test]
    async fn a_nul_byte_never_reaches_the_outcome() {
        let out = run(sh("printf 'a\\000b'; printf 'c\\000d' >&2")).await;
        assert_eq!(out.stdout, "a\u{fffd}b");
        assert_eq!(out.stderr, "c\u{fffd}d");
    }

    #[tokio::test]
    async fn requested_outputs_are_returned() {
        let out = run(sh("mkdir out; printf hi > out/a.txt").with_output("out/a.txt")).await;
        assert_eq!(out.outputs.len(), 1);
        assert_eq!(out.outputs[0].path, "out/a.txt");
        assert_eq!(out.outputs[0].content, b"hi");
        assert!(out.skipped_outputs.is_empty());
    }

    #[tokio::test]
    async fn a_missing_output_is_skipped() {
        let out = run(sh("true").with_output("nope.txt")).await;
        assert!(out.outputs.is_empty());
        assert_eq!(out.skipped_outputs[0].path, "nope.txt");
    }

    #[tokio::test]
    async fn a_symlinked_output_is_skipped() {
        let out = run(sh("ln -s /etc/hostname out.txt").with_output("out.txt")).await;
        assert!(out.outputs.is_empty(), "a symlink must not be followed");
        assert_eq!(out.skipped_outputs.len(), 1);
    }

    #[tokio::test]
    async fn a_symlinked_directory_in_an_output_path_is_skipped() {
        let out = run(sh("ln -s /etc d").with_output("d/hostname")).await;
        assert!(out.outputs.is_empty(), "a symlinked parent must not be followed");
    }

    #[tokio::test]
    async fn a_fifo_output_is_skipped_without_blocking() {
        let started = Instant::now();
        let out = run(sh("mkfifo pipe").with_output("pipe")).await;
        assert!(out.outputs.is_empty());
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn an_oversized_output_is_skipped() {
        let request = sh("head -c 300000 /dev/zero > big").with_output("big");
        let out = run(request).await;
        assert!(out.outputs.is_empty());
        assert!(out.skipped_outputs[0].reason.contains("bytes"));
    }

    #[tokio::test]
    async fn the_total_output_budget_is_enforced() {
        let request = sh("head -c 200000 /dev/zero > a; head -c 200000 /dev/zero > b; head -c 200000 /dev/zero > c")
            .with_output("a")
            .with_output("b")
            .with_output("c");
        let out = run(request).await;
        let total: usize = out.outputs.iter().map(|o| o.content.len()).sum();
        assert!(total <= 512 * 1024, "{total}");
        assert_eq!(out.outputs.len() + out.skipped_outputs.len(), 3);
        assert!(!out.skipped_outputs.is_empty());
    }

    #[tokio::test]
    async fn the_cpu_limit_ends_a_spinning_run() {
        let mut request = sh("while :; do :; done").with_timeout_ms(20_000);
        request.limits.cpu_seconds = Some(1);
        let out = run(request).await;
        assert!(
            matches!(out.status, RunStatus::Signaled { signal } if signal == libc::SIGXCPU || signal == libc::SIGKILL),
            "{:?}",
            out.status
        );
    }

    #[tokio::test]
    async fn the_file_size_limit_stops_a_large_write() {
        let mut policy = SandboxPolicy::for_development().with_network(NetworkPolicy::Allow);
        policy.default_limits.max_file_bytes = 100_000;
        let plan = policy
            .plan(&sh("head -c 1000000 /dev/zero > big; echo $?; wc -c < big"))
            .unwrap();
        let out = execute_plan(&plan, &options(), &CancellationToken::new())
            .await
            .unwrap();
        let mut lines = out.stdout.lines();
        assert_eq!(lines.next(), Some("153"), "128 + SIGXFSZ");
        assert_eq!(lines.next().map(str::trim), Some("100000"));
    }

    #[tokio::test]
    async fn the_open_file_limit_applies() {
        let out = run(sh("ulimit -n")).await;
        assert_eq!(out.stdout.trim(), "256");
    }

    #[tokio::test]
    async fn the_child_leads_its_own_session() {
        let out = run(sh("echo $$; cut -d' ' -f6 /proc/$$/stat")).await;
        let mut lines = out.stdout.lines();
        assert_eq!(lines.next(), lines.next(), "the shell must be its session leader");
    }

    #[tokio::test]
    async fn no_new_privileges_is_set() {
        let out = run(sh("grep NoNewPrivs /proc/self/status")).await;
        assert!(out.stdout.trim_end().ends_with('1'), "{}", out.stdout);
    }

    #[tokio::test]
    async fn an_inherited_descriptor_does_not_reach_the_child() {
        // Open a descriptor without close-on-exec, as a careless library
        // might, and move it to a high number.
        let path = std::ffi::CString::new("/dev/null").unwrap();
        // SAFETY: plain libc calls on a valid C string and our own fds.
        let leaked = unsafe {
            let fd = libc::open(path.as_ptr(), libc::O_RDONLY);
            assert!(fd >= 0);
            let high = libc::dup2(fd, 77);
            libc::close(fd);
            high
        };
        assert_eq!(leaked, 77);
        let out = run(sh("ls /proc/$$/fd")).await;
        // SAFETY: closes the descriptor that this test opened.
        unsafe { libc::close(leaked) };
        let fds: Vec<&str> = out.stdout.split_whitespace().collect();
        assert!(!fds.contains(&"77"), "fd 77 leaked: {fds:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_signal_mask_is_reset() {
        // Block SIGTERM on this thread. The current-thread runtime forks the
        // child from this thread, so the child would inherit the mask.
        // SAFETY: plain sigset manipulation on stack values.
        let old = unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            let mut old: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&raw mut set);
            libc::sigaddset(&raw mut set, libc::SIGTERM);
            libc::pthread_sigmask(libc::SIG_BLOCK, &raw const set, &raw mut old);
            old
        };
        let out = run(sh("grep SigBlk /proc/self/status")).await;
        // SAFETY: restores the mask that this test saved.
        unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, &raw const old, std::ptr::null_mut()) };
        assert!(out.stdout.contains("0000000000000000"), "{}", out.stdout);
    }

    #[tokio::test]
    async fn a_command_that_does_not_exist_is_denied() {
        let policy = SandboxPolicy::for_development()
            .with_network(NetworkPolicy::Allow)
            .with_runtime(
                "ghost",
                crate::sandbox::policy::RuntimeSpec::new(&["definitely-not-a-command-x9"], "a"),
            );
        let plan = policy.plan(&SandboxRequest::new("ghost", "")).unwrap();
        let err = execute_plan(&plan, &options(), &CancellationToken::new())
            .await
            .expect_err("no such command");
        assert!(matches!(err, SandboxError::Denied(ref m) if m.contains("could not start")), "{err:?}");
    }

    #[tokio::test]
    async fn a_plan_that_fails_its_check_is_denied_before_any_file_is_written() {
        let mut bad = plan(&sh("true"));
        bad.files[0].path = "../escape".into();
        let err = execute_plan(&bad, &options(), &CancellationToken::new())
            .await
            .expect_err("bad plan");
        assert!(matches!(err, SandboxError::Denied(_)));
    }

    #[tokio::test]
    async fn the_network_is_denied_or_the_run_fails_closed() {
        let denied = SandboxPolicy::for_development()
            .plan(&sh("cat /proc/net/dev"))
            .unwrap();
        let result = execute_plan(&denied, &options(), &CancellationToken::new()).await;
        if network_isolation_available().await {
            let out = result.expect("isolation is available");
            let interfaces: Vec<&str> = out
                .stdout
                .lines()
                .skip(2)
                .filter_map(|l| l.split(':').next())
                .map(str::trim)
                .collect();
            assert_eq!(interfaces, ["lo"], "only loopback in an empty namespace");
        } else {
            let err = result.expect_err("no isolation, so the run must fail closed");
            assert!(
                matches!(err, SandboxError::Unavailable(ref m) if m.contains("network namespace")),
                "{err:?}"
            );
        }
    }

    #[tokio::test]
    async fn root_runs_the_code_as_nobody() {
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipped: the uid switch needs root");
            return;
        }
        let out = run(sh("id -u; id -g; id -G")).await;
        let lines: Vec<&str> = out.stdout.lines().collect();
        assert_eq!(lines, ["65534", "65534", "65534"], "no supplementary groups");
    }

    #[tokio::test]
    async fn the_process_limit_stops_a_fork_bomb_under_a_dedicated_uid() {
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipped: the process limit needs a dedicated uid");
            return;
        }
        let mut policy = SandboxPolicy::for_development().with_network(NetworkPolicy::Allow);
        policy.default_limits.max_processes = 8;
        let plan = policy
            .plan(&sh("i=0; while [ $i -lt 50 ]; do sleep 5 & i=$((i+1)); done; echo done"))
            .unwrap();
        let options = ExecOptions {
            run_as: Some((54_321, 54_321)),
            ..ExecOptions::default()
        };
        let out = execute_plan(&plan, &options, &CancellationToken::new())
            .await
            .unwrap();
        assert!(!out.stderr.is_empty(), "the shell must report failed forks");
    }

    #[test]
    fn remove_work_dir_fixes_modes_that_the_code_changed() {
        let root = std::env::temp_dir().join(format!("harvest-rm-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("locked/inner")).unwrap();
        std::fs::write(root.join("locked/inner/f"), b"x").unwrap();
        // A mode of 0 blocks the removal for a non-root user.
        std::os::unix::fs::PermissionsExt::set_mode(
            &mut std::fs::metadata(root.join("locked")).unwrap().permissions(),
            0,
        );
        std::fs::set_permissions(
            root.join("locked"),
            std::os::unix::fs::PermissionsExt::from_mode(0),
        )
        .unwrap();
        remove_work_dir(&root).unwrap();
        assert!(!root.exists());
    }

    fn process_is_gone(pid: i32) -> bool {
        // A killed orphan can stay a zombie until init reaps it, which counts
        // as gone: it runs no code.
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => true,
            Ok(stat) => stat
                .rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z') || rest.starts_with('X')),
        }
    }
}
