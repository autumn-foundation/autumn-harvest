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

use std::io::{self, Read as _, Write as _};
use std::os::fd::{AsRawFd, FromRawFd as _, OwnedFd};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::sync::CancellationToken;

use super::capture::StreamCapture;
use super::error::SandboxError;
use super::guest::PlanExecutor;
use super::outcome::{MAX_SKIP_REASON_BYTES, OutputFile, RunStatus, SandboxOutcome, SkippedOutput};
use super::policy::{IsolationLevel, NetworkPolicy};
use super::request::ExecPlan;
use super::runtime::BoxFuture;

/// The uid and gid of the code when the executor runs as root and the
/// options name none: `nobody`.
pub const NOBODY: (u32, u32) = (65_534, 65_534);

/// The time that the executor waits for the pipes to close after the main
/// process exits and the group is killed.
pub const PIPE_DRAIN_GRACE: Duration = Duration::from_secs(2);

/// The errno that the child setup reports when it cannot create the empty
/// network namespace. No other setup step returns it, so the parent can name
/// the cause.
const NETNS_FAILED: i32 = libc::ENETDOWN;

/// The highest descriptor that the close-on-exec fallback marks.
const CLOEXEC_FALLBACK_MAX_FD: i32 = 4096;

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
/// - [`SandboxError::Denied`]: the plan fails its check, or the command
///   cannot start.
/// - [`SandboxError::Unavailable`]: the executor cannot isolate the network
///   or set up the work directory.
/// - [`SandboxError::Cancelled`]: `cancel` fires.
///
/// The executor kills the process group and removes the work directory in
/// each case.
pub async fn execute_plan(
    plan: &ExecPlan,
    options: &ExecOptions,
    cancel: &CancellationToken,
) -> Result<SandboxOutcome, SandboxError> {
    plan.validate()?;
    if cancel.is_cancelled() {
        return Err(SandboxError::Cancelled);
    }
    let run_as = effective_run_as(options);
    let work = WorkDir::create(&options.scratch_root, run_as)?;
    write_files(work.path(), plan, run_as)?;

    let setup = ChildSetup::new(plan, run_as);
    let mut command = tokio::process::Command::new(&plan.argv[0]);
    command
        .args(&plan.argv[1..])
        .env_clear()
        .env("PATH", &plan.path_env)
        .env("HOME", work.path())
        .env("TMPDIR", work.path())
        .env("PWD", work.path())
        .env("LANG", "C.UTF-8")
        .envs(&plan.env)
        .current_dir(work.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // SAFETY: `ChildSetup::apply` runs between fork and exec. It makes only
    // async-signal-safe system calls on values computed before the fork,
    // and it allocates nothing.
    unsafe {
        command.pre_exec(move || setup.apply());
    }
    let started = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|e| spawn_error(&plan.argv[0], &e))?;
    let group = child.id().and_then(|pid| i32::try_from(pid).ok());

    let stdin = plan.stdin.clone();
    if let Some(mut pipe) = child.stdin.take() {
        // A child that exits without reading stdin breaks the pipe, which
        // is not an error of the run.
        tokio::spawn(async move {
            let _ = pipe.write_all(&stdin).await;
        });
    }
    let cap = usize::try_from(plan.limits.max_output_bytes).unwrap_or(usize::MAX);
    let stdout = spawn_capture(child.stdout.take(), cap);
    let stderr = spawn_capture(child.stderr.take(), cap);

    let end = tokio::select! {
        status = child.wait() => End::Exited(status),
        () = tokio::time::sleep(plan.limits.wall_clock()) => End::TimedOut,
        () = cancel.cancelled() => End::Cancelled,
    };
    // Kill the group in every case: background processes must not outlive
    // the run, hold the pipes, or keep using the CPU.
    if let Some(group) = group {
        kill_group(group);
    }
    if !matches!(end, End::Exited(_)) {
        let _ = child.wait().await;
    }
    let duration = started.elapsed();
    let stdout = stdout.finish().await;
    let stderr = stderr.finish().await;

    let status = match end {
        End::Cancelled => return Err(SandboxError::Cancelled),
        End::TimedOut => RunStatus::TimedOut,
        End::Exited(Err(e)) => {
            return Err(SandboxError::Unavailable(format!(
                "could not wait for the child: {e}"
            )));
        }
        End::Exited(Ok(status)) => match (status.code(), status.signal()) {
            (Some(code), _) => RunStatus::Exited { code },
            (None, Some(signal)) => RunStatus::Signaled { signal },
            (None, None) => RunStatus::Signaled { signal: 0 },
        },
    };

    let (outputs, skipped_outputs) = {
        let dir = work.path().to_path_buf();
        let plan = plan.clone();
        tokio::task::spawn_blocking(move || collect_outputs(&dir, &plan))
            .await
            .map_err(|e| SandboxError::Unavailable(format!("output collection failed: {e}")))?
    };
    work.remove().await;

    Ok(SandboxOutcome {
        status,
        stdout: stdout.text,
        stderr: stderr.text,
        stdout_bytes: stdout.bytes,
        stderr_bytes: stderr.bytes,
        stdout_truncated: stdout.truncated,
        stderr_truncated: stderr.truncated,
        outputs,
        skipped_outputs,
        duration_ms: u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
        isolation: IsolationLevel::Process,
        image: None,
    })
}

/// How the main process ended.
enum End {
    Exited(io::Result<std::process::ExitStatus>),
    TimedOut,
    Cancelled,
}

/// The uid and gid of the code: the option, or `nobody` for a root executor.
fn effective_run_as(options: &ExecOptions) -> Option<(u32, u32)> {
    // SAFETY: geteuid has no preconditions.
    let root = unsafe { libc::geteuid() } == 0;
    options.run_as.or_else(|| root.then_some(NOBODY))
}

/// Map a spawn error to a sandbox error.
fn spawn_error(command: &str, err: &io::Error) -> SandboxError {
    match err.raw_os_error() {
        Some(NETNS_FAILED) => SandboxError::Unavailable(
            "the policy denies the network, and this host does not allow an empty network \
             namespace; the run fails closed"
                .into(),
        ),
        Some(libc::ENOENT | libc::EACCES | libc::ENOEXEC) => SandboxError::Denied(format!(
            "the runtime command {command:?} could not start: {err}"
        )),
        _ => SandboxError::Unavailable(format!("could not start {command:?}: {err}")),
    }
}

/// Send `SIGKILL` to each process of the group. A missing group is fine.
fn kill_group(group: i32) {
    // SAFETY: kill with a negative pid signals a process group. The group
    // id is the pid of the child, which leads a new session.
    unsafe {
        libc::kill(-group, libc::SIGKILL);
    }
}

/// The values that the child setup needs, computed before the fork.
#[derive(Debug, Clone, Copy)]
struct ChildSetup {
    deny_network: bool,
    root: bool,
    run_as: Option<(u32, u32)>,
    cpu_seconds: libc::rlim_t,
    data_bytes: libc::rlim_t,
    file_bytes: libc::rlim_t,
    open_files: libc::rlim_t,
    processes: libc::rlim_t,
}

impl ChildSetup {
    fn new(plan: &ExecPlan, run_as: Option<(u32, u32)>) -> Self {
        let limits = &plan.limits;
        Self {
            deny_network: plan.network == NetworkPolicy::Deny,
            // SAFETY: geteuid has no preconditions.
            root: unsafe { libc::geteuid() } == 0,
            run_as,
            cpu_seconds: limits.cpu_seconds,
            data_bytes: limits.memory_bytes,
            file_bytes: limits.max_file_bytes,
            open_files: libc::rlim_t::from(limits.max_open_files),
            processes: libc::rlim_t::from(limits.max_processes),
        }
    }

    /// Set up the child after the fork. Async-signal-safe: system calls only.
    fn apply(&self) -> io::Result<()> {
        // SAFETY: each call below is an async-signal-safe system call on
        // plain values. The child has one thread here.
        unsafe {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut empty: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&raw mut empty);
            if libc::pthread_sigmask(libc::SIG_SETMASK, &raw const empty, std::ptr::null_mut()) != 0
            {
                return Err(io::Error::last_os_error());
            }
            if self.deny_network {
                // Root can create a network namespace directly. Another user
                // needs a user namespace to hold the capability.
                let flags = if self.root {
                    libc::CLONE_NEWNET
                } else {
                    libc::CLONE_NEWUSER | libc::CLONE_NEWNET
                };
                if libc::unshare(flags) != 0 {
                    return Err(io::Error::from_raw_os_error(NETNS_FAILED));
                }
            }
            mark_descriptors_cloexec();
            set_limit(libc::RLIMIT_CORE, 0, 0)?;
            set_limit(
                libc::RLIMIT_CPU,
                self.cpu_seconds,
                self.cpu_seconds.saturating_add(1),
            )?;
            set_limit(libc::RLIMIT_DATA, self.data_bytes, self.data_bytes)?;
            set_limit(libc::RLIMIT_FSIZE, self.file_bytes, self.file_bytes)?;
            set_limit(libc::RLIMIT_NOFILE, self.open_files, self.open_files)?;
            if let Some((uid, gid)) = self.run_as {
                // A process count limit is a per-uid limit, so it is exact
                // only for a dedicated uid.
                set_limit(libc::RLIMIT_NPROC, self.processes, self.processes)?;
                if libc::setgroups(0, std::ptr::null()) != 0
                    || libc::setgid(gid) != 0
                    || libc::setuid(uid) != 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

/// The rlimit resource type: glibc and musl declare it apart.
#[cfg(target_env = "gnu")]
type Resource = libc::__rlimit_resource_t;
#[cfg(not(target_env = "gnu"))]
type Resource = libc::c_int;

/// Set one rlimit.
///
/// # Safety
///
/// Call it only between fork and exec, or in a single-threaded process.
unsafe fn set_limit(resource: Resource, soft: libc::rlim_t, hard: libc::rlim_t) -> io::Result<()> {
    let limit = libc::rlimit {
        rlim_cur: soft,
        rlim_max: hard,
    };
    // SAFETY: setrlimit reads a valid stack value.
    if unsafe { libc::setrlimit(resource, &raw const limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Mark each descriptor above 2 close-on-exec.
///
/// `close_range` with `CLOSE_RANGE_CLOEXEC` needs Linux 5.11. Older kernels
/// get a loop of `fcntl` calls.
///
/// # Safety
///
/// Call it only between fork and exec.
unsafe fn mark_descriptors_cloexec() {
    // SAFETY: close_range only changes descriptor flags.
    let done = unsafe {
        libc::syscall(
            libc::SYS_close_range,
            3u32,
            u32::MAX,
            libc::CLOSE_RANGE_CLOEXEC,
        )
    } == 0;
    if !done {
        for fd in 3..CLOEXEC_FALLBACK_MAX_FD {
            // SAFETY: fcntl on a closed descriptor fails with EBADF only.
            unsafe {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags >= 0 {
                    libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
                }
            }
        }
    }
}

/// A stream capture that a task fills and the executor reads.
struct CaptureTask {
    task: Option<tokio::task::JoinHandle<()>>,
    capture: Arc<Mutex<StreamCapture>>,
}

impl CaptureTask {
    /// Wait for the stream to end, at most [`PIPE_DRAIN_GRACE`]. A process
    /// that left the group can hold the pipe open, so the wait is bounded.
    async fn finish(mut self) -> super::capture::CapturedText {
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(PIPE_DRAIN_GRACE, task).await.is_err() {
                abort.abort();
            }
        }
        let capture = std::mem::replace(
            &mut *self.capture.lock().unwrap_or_else(PoisonError::into_inner),
            StreamCapture::new(0),
        );
        capture.finish()
    }
}

/// Read `pipe` to its end into a bounded capture.
fn spawn_capture<R: AsyncRead + Unpin + Send + 'static>(
    pipe: Option<R>,
    cap: usize,
) -> CaptureTask {
    let capture = Arc::new(Mutex::new(StreamCapture::new(cap)));
    let task = pipe.map(|mut pipe| {
        let capture = Arc::clone(&capture);
        tokio::spawn(async move {
            let mut chunk = [0u8; 8192];
            loop {
                match pipe.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => capture
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(&chunk[..n]),
                }
            }
        })
    });
    CaptureTask { task, capture }
}

/// A private work directory, removed when the run ends.
struct WorkDir {
    path: PathBuf,
    removed: bool,
}

impl WorkDir {
    fn create(root: &Path, run_as: Option<(u32, u32)>) -> Result<Self, SandboxError> {
        let path = root.join(format!("harvest-sbx-{}", uuid::Uuid::new_v4().simple()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .map_err(|e| {
                SandboxError::Unavailable(format!(
                    "could not create the work directory {}: {e}",
                    path.display()
                ))
            })?;
        let dir = Self {
            path,
            removed: false,
        };
        if let Some((uid, gid)) = run_as {
            std::os::unix::fs::chown(&dir.path, Some(uid), Some(gid)).map_err(|e| {
                SandboxError::Unavailable(format!("could not hand the work directory over: {e}"))
            })?;
        }
        Ok(dir)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    async fn remove(mut self) {
        self.removed = true;
        let path = self.path.clone();
        let removed = tokio::task::spawn_blocking(move || remove_work_dir(&path)).await;
        if !matches!(removed, Ok(Ok(()))) {
            tracing::warn!(
                path = %self.path.display(),
                "could not remove a sandbox work directory"
            );
        }
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        if !self.removed {
            let _ = remove_work_dir(&self.path);
        }
    }
}

/// Write the plan files into `dir`. The directory is new, and no code ran
/// in it yet, so no link can redirect a write.
fn write_files(
    dir: &Path,
    plan: &ExecPlan,
    run_as: Option<(u32, u32)>,
) -> Result<(), SandboxError> {
    let fail = |e: io::Error, what: &str| {
        SandboxError::Unavailable(format!("could not write the input file {what}: {e}"))
    };
    for file in &plan.files {
        let mut path = dir.to_path_buf();
        let mut components = file.path.split('/').peekable();
        while let Some(component) = components.next() {
            path.push(component);
            if components.peek().is_none() {
                break;
            }
            match std::fs::DirBuilder::new().mode(0o755).create(&path) {
                Ok(()) => {
                    if let Some((uid, gid)) = run_as {
                        std::os::unix::fs::chown(&path, Some(uid), Some(gid))
                            .map_err(|e| fail(e, &file.path))?;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(fail(e, &file.path)),
            }
        }
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(if file.executable { 0o755 } else { 0o644 })
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|e| fail(e, &file.path))?;
        out.write_all(&file.content)
            .map_err(|e| fail(e, &file.path))?;
        if let Some((uid, gid)) = run_as {
            std::os::unix::fs::fchown(&out, Some(uid), Some(gid))
                .map_err(|e| fail(e, &file.path))?;
        }
    }
    Ok(())
}

/// Read the requested outputs from `dir`, within the per-file and the total
/// budgets.
fn collect_outputs(dir: &Path, plan: &ExecPlan) -> (Vec<OutputFile>, Vec<SkippedOutput>) {
    let mut outputs = Vec::new();
    let mut skipped = Vec::new();
    let mut budget = plan.max_output_files_total_bytes;
    let skip = |skipped: &mut Vec<SkippedOutput>, path: &str, mut reason: String| {
        reason.truncate(MAX_SKIP_REASON_BYTES);
        skipped.push(SkippedOutput {
            path: path.to_owned(),
            reason,
        });
    };
    let root = match std::fs::File::open(dir) {
        Ok(root) => root,
        Err(e) => {
            for path in &plan.outputs {
                skip(
                    &mut skipped,
                    path,
                    format!("the work directory is gone: {e}"),
                );
            }
            return (outputs, skipped);
        }
    };
    for path in &plan.outputs {
        match read_output(&root, path, plan.max_output_file_bytes) {
            Ok(content) => {
                let size = u64::try_from(content.len()).unwrap_or(u64::MAX);
                if size > budget {
                    skip(
                        &mut skipped,
                        path,
                        format!(
                            "{size} bytes do not fit the {budget} bytes left of the output budget"
                        ),
                    );
                } else {
                    budget -= size;
                    outputs.push(OutputFile {
                        path: path.clone(),
                        content,
                    });
                }
            }
            Err(reason) => skip(&mut skipped, path, reason),
        }
    }
    (outputs, skipped)
}

/// Read one output file, beneath `root`, with no symbolic link anywhere on
/// the path.
fn read_output(root: &std::fs::File, path: &str, max: u64) -> Result<Vec<u8>, String> {
    let file = open_beneath(root, path).map_err(|e| match e.raw_os_error() {
        Some(libc::ENOENT) => "the file does not exist".to_owned(),
        Some(libc::ELOOP | libc::EXDEV) => {
            "the path holds a symbolic link or leaves the work directory".to_owned()
        }
        Some(libc::ENOTDIR) => "a parent of the path is not a directory".to_owned(),
        _ => format!("the file cannot be opened: {e}"),
    })?;
    let meta = file
        .metadata()
        .map_err(|e| format!("the file cannot be read: {e}"))?;
    if !meta.is_file() {
        return Err("the path is not a regular file".to_owned());
    }
    if meta.len() > max {
        return Err(format!(
            "the file holds {} bytes, over the limit of {max} bytes",
            meta.len()
        ));
    }
    let mut content = Vec::new();
    file.take(max.saturating_add(1))
        .read_to_end(&mut content)
        .map_err(|e| format!("the file cannot be read: {e}"))?;
    if u64::try_from(content.len()).unwrap_or(u64::MAX) > max {
        return Err(format!("the file grew over the limit of {max} bytes"));
    }
    Ok(content)
}

/// Open `path` beneath `root` for reading, with no symbolic link.
///
/// The open is non-blocking, so a FIFO cannot stall the executor. It uses
/// `openat2` with `RESOLVE_BENEATH` and `RESOLVE_NO_SYMLINKS` (Linux 5.6). An
/// older kernel gets an `openat` walk with `O_NOFOLLOW` on each component.
fn open_beneath(root: &std::fs::File, path: &str) -> io::Result<std::fs::File> {
    let flags = libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW;
    let c_path =
        std::ffi::CString::new(path).map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: open_how is a plain C struct, and zero is a valid value.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = u64::try_from(flags).unwrap_or(0);
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS;
    // SAFETY: openat2 reads a valid C string and a valid open_how of the
    // given size. A non-negative result is a new descriptor that we own.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            c_path.as_ptr(),
            &raw const how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if fd >= 0 {
        let fd = i32::try_from(fd).map_err(|_| io::Error::from_raw_os_error(libc::EBADF))?;
        // SAFETY: the descriptor is new and owned here.
        return Ok(std::fs::File::from(unsafe { OwnedFd::from_raw_fd(fd) }));
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() != Some(libc::ENOSYS) {
        return Err(err);
    }
    open_beneath_walk(root, path, flags)
}

/// The `openat2` fallback: open each component with `O_NOFOLLOW`.
fn open_beneath_walk(root: &std::fs::File, path: &str, flags: i32) -> io::Result<std::fs::File> {
    let mut dir: Option<OwnedFd> = None;
    let mut components = path.split('/').peekable();
    while let Some(component) = components.next() {
        let last = components.peek().is_none();
        let name = std::ffi::CString::new(component)
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        let at = dir
            .as_ref()
            .map_or_else(|| root.as_raw_fd(), AsRawFd::as_raw_fd);
        let component_flags = if last {
            flags
        } else {
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
        };
        // SAFETY: openat reads a valid C string. A non-negative result is a
        // new descriptor that we own.
        let fd = unsafe { libc::openat(at, name.as_ptr(), component_flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the descriptor is new and owned here.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        if last {
            return Ok(std::fs::File::from(owned));
        }
        dir = Some(owned);
    }
    Err(io::Error::from_raw_os_error(libc::EINVAL))
}

/// Whether this host lets the executor create an empty network namespace.
///
/// The probe starts `/bin/sh -c true` with the same namespace step that a
/// run uses.
pub async fn network_isolation_available() -> bool {
    // SAFETY: geteuid has no preconditions.
    let root = unsafe { libc::geteuid() } == 0;
    let mut command = tokio::process::Command::new("/bin/sh");
    command
        .args(["-c", "true"])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // SAFETY: the closure makes one async-signal-safe system call.
    unsafe {
        command.pre_exec(move || {
            let flags = if root {
                libc::CLONE_NEWNET
            } else {
                libc::CLONE_NEWUSER | libc::CLONE_NEWNET
            };
            if libc::unshare(flags) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
    match command.spawn() {
        Ok(mut child) => child.wait().await.is_ok_and(|s| s.success()),
        Err(_) => false,
    }
}

/// Remove a work directory, and fix the modes that the code changed first.
///
/// # Errors
///
/// Returns the I/O error of the final removal.
pub fn remove_work_dir(dir: &Path) -> io::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {}
    }
    // The code can remove the write or search bit of a directory, which
    // blocks the removal for a non-root executor. Restore the owner bits of
    // each real directory, without following a symbolic link, then retry.
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let _ =
            std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o700));
        if let Ok(entries) = std::fs::read_dir(&path) {
            stack.extend(entries.filter_map(Result::ok).map(|entry| entry.path()));
        }
    }
    std::fs::remove_dir_all(dir)
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
        assert_eq!(out.skipped_outputs, []);
    }

    #[tokio::test]
    async fn a_missing_output_is_skipped() {
        let out = run(sh("true").with_output("nope.txt")).await;
        assert_eq!(out.outputs, []);
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
        assert!(
            out.outputs.is_empty(),
            "a symlinked parent must not be followed"
        );
    }

    #[tokio::test]
    async fn a_fifo_output_is_skipped_without_blocking() {
        let started = Instant::now();
        let out = run(sh("mkfifo pipe").with_output("pipe")).await;
        assert_eq!(out.outputs, []);
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[tokio::test]
    async fn an_oversized_output_is_skipped() {
        let request = sh("head -c 300000 /dev/zero > big").with_output("big");
        let out = run(request).await;
        assert_eq!(out.outputs, []);
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
        assert_ne!(out.skipped_outputs, []);
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
        assert_eq!(
            lines.next(),
            lines.next(),
            "the shell must be its session leader"
        );
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
        assert!(
            matches!(err, SandboxError::Denied(ref m) if m.contains("could not start")),
            "{err:?}"
        );
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
        assert_eq!(
            lines,
            ["65534", "65534", "65534"],
            "no supplementary groups"
        );
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
            .plan(&sh(
                "i=0; while [ $i -lt 50 ]; do sleep 5 & i=$((i+1)); done; echo done",
            ))
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
        std::fs::set_permissions(
            root.join("locked"),
            std::os::unix::fs::PermissionsExt::from_mode(0o000),
        )
        .unwrap();
        remove_work_dir(&root).unwrap();
        assert!(!root.exists());
    }

    fn process_is_gone(pid: i32) -> bool {
        // A killed orphan can stay a zombie until init reaps it, which counts
        // as gone: it runs no code.
        std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('Z') || rest.starts_with('X'))
        })
    }
}
