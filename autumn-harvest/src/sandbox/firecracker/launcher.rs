//! Start and stop Firecracker microVMs.
//!
//! [`VmLauncher`] starts one microVM. [`LaunchedVm`] is a running microVM
//! that serves one run and then goes away. [`FirecrackerLauncher`] is the
//! real launcher: it runs Firecracker behind the jailer, or without a jail
//! for development.
//!
//! Teardown kills the process, kills the cgroup with `cgroup.kill`, and
//! removes the VM directory, in that order. The host never reads a pid from
//! the jail, because the jailed VMM can write there.
//!
//! Each launcher holds an owner lease: a locked file next to its VM
//! directories. [`FirecrackerLauncher::sweep_stale`] removes the VMs of each
//! owner whose lease is not locked, so a crashed worker leaves no VM behind
//! after the next start.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::api::ApiClient;
use super::config::{
    CONFIG_FILE, GuestImage, KERNEL_FILE, MAX_SOCKET_PATH_BYTES, MachineSpec, OWNER_TAG_HEX,
    ROOTFS_FILE, VM_ID_PREFIX, VM_ID_RANDOM_HEX, VSOCK_SOCKET, VmPaths, new_vm_id, validate_vm_id,
    vm_config,
};
use super::jailer::JailerConfig;
use super::posture::{PostureFinding, PosturePolicy};
use crate::sandbox::error::SandboxError;
use crate::sandbox::runtime::BoxFuture;

/// The time that teardown waits for the VMM process to exit.
pub const KILL_WAIT: Duration = Duration::from_secs(5);

/// The time that teardown waits for a killed cgroup to empty.
pub const CGROUP_EMPTY_WAIT: Duration = Duration::from_secs(2);

/// The bytes of VMM stderr that a launcher keeps for error reports.
pub const STDERR_TAIL_BYTES: usize = 2048;

/// A running microVM that serves one run.
pub trait LaunchedVm: Send {
    /// The VM id.
    fn id(&self) -> &str;

    /// The host path of the vsock socket.
    fn vsock_path(&self) -> &Path;

    /// Why the VMM stopped, when it has stopped.
    fn exit_status(&mut self) -> Option<String>;

    /// Kill the microVM and remove all that it left on the host.
    fn shutdown(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// Something that starts microVMs.
pub trait VmLauncher: Send + Sync + 'static {
    /// Start one microVM. The guest agent may still be booting when this
    /// returns.
    fn launch(&self) -> BoxFuture<'_, Result<Box<dyn LaunchedVm>, SandboxError>>;

    /// The name of the guest image, for outcomes.
    fn image_name(&self) -> Option<String> {
        None
    }
}

/// The two snapshot files of a golden microVM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFiles {
    /// The VM state file.
    pub state: PathBuf,
    /// The guest memory file.
    pub memory: PathBuf,
}

/// How a launcher boots each microVM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootMethod {
    /// Boot from `--config-file` with no API socket. This is the default.
    ConfigFile,
    /// Configure and start through the API socket.
    Api,
    /// Load a golden snapshot through the API socket, and resume it.
    Snapshot(SnapshotFiles),
}

/// Where a launcher runs Firecracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Confinement {
    /// Behind the jailer. Needs root. Use it in production.
    Jailer(JailerConfig),
    /// As a plain child process in `work_root`. For development only.
    Unjailed {
        /// The directory that holds one directory per VM.
        work_root: PathBuf,
    },
}

/// The configuration of a [`FirecrackerLauncher`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LauncherConfig {
    /// The Firecracker binary.
    pub firecracker_bin: PathBuf,
    /// The kernel and root file system.
    pub image: GuestImage,
    /// The size of each microVM.
    pub machine: MachineSpec,
    /// How to boot.
    pub boot: BootMethod,
    /// Where to run.
    pub confinement: Confinement,
    /// The vsock port of the guest agent.
    pub guest_port: u32,
    /// The time limit of the API socket and of each API call.
    pub api_timeout: Duration,
}

/// The real launcher.
#[derive(Debug)]
pub struct FirecrackerLauncher {
    config: LauncherConfig,
    exec_name: String,
    owner: String,
    lease: InstanceLease,
}

/// A locked file that marks a live owner of VM directories.
#[derive(Debug)]
pub struct InstanceLease {
    path: PathBuf,
    file: std::fs::File,
}

impl InstanceLease {
    /// Create and lock the lease of `owner` in `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when the file cannot be created or
    /// locked.
    pub fn acquire(dir: &Path, owner: &str) -> Result<Self, SandboxError> {
        use std::os::unix::fs::OpenOptionsExt as _;
        let path = dir.join(format!("{VM_ID_PREFIX}{owner}{LEASE_SUFFIX}"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|e| {
                SandboxError::Config(format!("cannot create the lease {}: {e}", path.display()))
            })?;
        if !try_lock(&file) {
            return Err(SandboxError::Config(format!(
                "the lease {} is held by another launcher",
                path.display()
            )));
        }
        Ok(Self { path, file })
    }

    /// The lease file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for InstanceLease {
    fn drop(&mut self) {
        // Remove the file while the lock is held. Closing the file then
        // releases the lock.
        let _ = std::fs::remove_file(&self.path);
        let _ = &self.file;
    }
}

/// The suffix of a lease file.
const LEASE_SUFFIX: &str = ".lease";

/// Take an exclusive `flock` on `file` without waiting.
fn try_lock(file: &std::fs::File) -> bool {
    use std::os::fd::AsRawFd as _;
    // SAFETY: flock on a descriptor that `file` owns.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
}

impl FirecrackerLauncher {
    /// A launcher for `config`, with a new owner tag and its lease.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when a path is missing, when a
    /// snapshot boot has no jailer, or when the lease cannot be taken.
    pub fn new(config: LauncherConfig) -> Result<Self, SandboxError> {
        let mut required = vec![
            ("the Firecracker binary", &config.firecracker_bin),
            ("the guest kernel", &config.image.kernel),
            ("the guest root file system", &config.image.rootfs),
        ];
        if let BootMethod::Snapshot(files) = &config.boot {
            required.push(("the snapshot state", &files.state));
            required.push(("the snapshot memory", &files.memory));
            // A restore needs the same paths as the snapshot. Only a jail gives
            // each microVM the same relative paths.
            if !matches!(config.confinement, Confinement::Jailer(_)) {
                return Err(SandboxError::Config(
                    "a snapshot boot needs the jailer, so each restore sees the same paths".into(),
                ));
            }
        }
        if let Confinement::Jailer(jailer) = &config.confinement {
            required.push(("the jailer binary", &jailer.jailer_bin));
        }
        for (what, path) in required {
            if !path.is_file() {
                return Err(SandboxError::Config(format!(
                    "{what} {} is not a file",
                    path.display()
                )));
            }
        }
        let exec_name = super::jailer::exec_name(&config.firecracker_bin)?;
        let owner = uuid::Uuid::new_v4().simple().to_string()[..OWNER_TAG_HEX].to_owned();
        let vm_root = vm_root_of(&config.confinement, &exec_name);
        check_socket_paths(&vm_root, &config.confinement)?;
        std::fs::create_dir_all(&vm_root).map_err(|e| {
            SandboxError::Config(format!("cannot create {}: {e}", vm_root.display()))
        })?;
        let lease = InstanceLease::acquire(&vm_root, &owner)?;
        Ok(Self {
            config,
            exec_name,
            owner,
            lease,
        })
    }

    /// The owner tag in each VM id of this launcher.
    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// The configuration.
    #[must_use]
    pub const fn config(&self) -> &LauncherConfig {
        &self.config
    }

    /// The lease file of this launcher.
    #[must_use]
    pub fn lease_path(&self) -> &Path {
        self.lease.path()
    }

    /// The directory that holds the VM directories and the leases.
    #[must_use]
    pub fn vm_root(&self) -> PathBuf {
        vm_root_of(&self.config.confinement, &self.exec_name)
    }

    /// Check the Firecracker version and the host posture under `/`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] for a version with a known escape,
    /// or for a critical finding under [`PosturePolicy::Enforce`].
    pub async fn verify_host(
        &self,
        policy: PosturePolicy,
    ) -> Result<Vec<PostureFinding>, SandboxError> {
        self.verify_host_at(Path::new("/"), policy).await
    }

    /// Check the Firecracker version and the host posture under `root`.
    ///
    /// # Errors
    ///
    /// The same as [`Self::verify_host`].
    pub async fn verify_host_at(
        &self,
        root: &Path,
        policy: PosturePolicy,
    ) -> Result<Vec<PostureFinding>, SandboxError> {
        super::posture::probe_version(&self.config.firecracker_bin).await?;
        let findings = super::posture::inspect_host(root);
        super::posture::enforce(&findings, policy)?;
        for finding in &findings {
            tracing::warn!(
                check = %finding.check,
                detail = %finding.detail,
                severity = ?finding.severity,
                "sandbox host posture finding"
            );
        }
        Ok(findings)
    }

    /// Remove the VMs of each owner whose lease is not locked. Returns the
    /// number of VM directories removed.
    pub fn sweep_stale(&self) -> usize {
        let root = self.vm_root();
        let Ok(entries) = std::fs::read_dir(&root) else {
            return 0;
        };
        let mut vm_dirs = Vec::new();
        let mut leases = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix(VM_ID_PREFIX) else {
                continue;
            };
            if let Some(owner) = rest.strip_suffix(LEASE_SUFFIX) {
                leases.push((owner.to_owned(), entry.path()));
            } else if let Some((owner, _)) = rest.split_once('-')
                && entry.file_type().is_ok_and(|t| t.is_dir())
            {
                vm_dirs.push((owner.to_owned(), name.clone(), entry.path()));
            }
        }
        // An owner is live when another process holds its lease lock.
        let mut live = std::collections::BTreeSet::new();
        live.insert(self.owner.clone());
        let mut dead_leases = Vec::new();
        for (owner, path) in leases {
            if owner == self.owner {
                continue;
            }
            match std::fs::OpenOptions::new().read(true).open(&path) {
                Ok(file) if try_lock(&file) => dead_leases.push((path, file)),
                Ok(_) => {
                    live.insert(owner);
                }
                Err(_) => {}
            }
        }
        let mut removed = 0;
        for (owner, vm_id, path) in vm_dirs {
            if live.contains(&owner) {
                continue;
            }
            if let Confinement::Jailer(jailer) = &self.config.confinement {
                kill_cgroup_now(&super::jailer::cgroup_dir(jailer, &vm_id));
            }
            if crate::sandbox::exec::remove_work_dir(&path).is_ok() {
                removed += 1;
            }
        }
        for (path, _locked) in dead_leases {
            let _ = std::fs::remove_file(path);
        }
        removed
    }

    /// Create the VM directory and the command for `id`.
    fn prepare(&self, id: &str) -> Result<Prepared, SandboxError> {
        match &self.config.confinement {
            Confinement::Jailer(jailer) => self.prepare_jailed(jailer, id),
            Confinement::Unjailed { work_root } => self.prepare_unjailed(work_root, id),
        }
    }

    /// Lay out the jail of `id` and build the jailer command.
    fn prepare_jailed(&self, jailer: &JailerConfig, id: &str) -> Result<Prepared, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        let config = &self.config;
        let jail = super::jailer::jail_dir(jailer, &self.exec_name, id);
        let root = jail.join("root");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)
            .map_err(|e| prepare_error(id, "the jail", &e))?;
        // A failed step removes the jail, so a failed launch leaves nothing.
        let fail = |what: &str, e: &std::io::Error| {
            let _ = crate::sandbox::exec::remove_work_dir(&jail);
            prepare_error(id, what, e)
        };
        link_or_copy(&config.image.kernel, &root.join(KERNEL_FILE))
            .map_err(|e| fail("the kernel", &e))?;
        link_or_copy(&config.image.rootfs, &root.join(ROOTFS_FILE))
            .map_err(|e| fail("the root file system", &e))?;
        let mut api_socket = None;
        let firecracker_args: Vec<String> = match &config.boot {
            BootMethod::ConfigFile => {
                let document = vm_config(
                    &config.image,
                    config.machine,
                    &VmPaths::jailed(),
                    config.guest_port,
                );
                write_config(&root.join(CONFIG_FILE), &document)
                    .map_err(|e| fail("the config", &e))?;
                vec![
                    "--config-file".into(),
                    CONFIG_FILE.into(),
                    "--no-api".into(),
                ]
            }
            BootMethod::Api => {
                api_socket = Some(root.join(super::api::JAILED_API_SOCKET));
                Vec::new()
            }
            BootMethod::Snapshot(files) => {
                link_or_copy(&files.state, &root.join(SNAPSHOT_STATE_FILE))
                    .map_err(|e| fail("the snapshot state", &e))?;
                link_or_copy(&files.memory, &root.join(SNAPSHOT_MEMORY_FILE))
                    .map_err(|e| fail("the snapshot memory", &e))?;
                api_socket = Some(root.join(super::api::JAILED_API_SOCKET));
                Vec::new()
            }
        };
        let mut command = tokio::process::Command::new(&jailer.jailer_bin);
        command
            .args(super::jailer::jailer_args(
                jailer,
                &config.firecracker_bin,
                id,
                config.machine,
                &firecracker_args,
            ))
            .current_dir(&jail);
        Ok(Prepared {
            vm_dir: jail,
            vsock: root.join(VSOCK_SOCKET),
            api_socket,
            cgroup: Some(super::jailer::cgroup_dir(jailer, id)),
            command,
        })
    }

    /// Create the directory of `id` and build the Firecracker command.
    fn prepare_unjailed(&self, work_root: &Path, id: &str) -> Result<Prepared, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        let config = &self.config;
        let dir = work_root.join(id);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .map_err(|e| prepare_error(id, "the VM directory", &e))?;
        let fail = |what: &str, e: &std::io::Error| {
            let _ = crate::sandbox::exec::remove_work_dir(&dir);
            prepare_error(id, what, e)
        };
        let mut args = vec!["--id".to_owned(), id.to_owned()];
        let mut api_socket = None;
        match &config.boot {
            BootMethod::ConfigFile => {
                let paths = self
                    .unjailed_paths(&dir)
                    .map_err(|e| fail("the paths", &e))?;
                let document = vm_config(&config.image, config.machine, &paths, config.guest_port);
                let path = dir.join(CONFIG_FILE);
                write_config(&path, &document).map_err(|e| fail("the config", &e))?;
                args.extend([
                    "--config-file".to_owned(),
                    path.display().to_string(),
                    "--no-api".to_owned(),
                ]);
            }
            BootMethod::Api | BootMethod::Snapshot(_) => {
                let socket = dir.join(UNJAILED_API_SOCKET);
                args.extend(["--api-sock".to_owned(), socket.display().to_string()]);
                api_socket = Some(socket);
            }
        }
        let mut command = tokio::process::Command::new(&config.firecracker_bin);
        command.args(args).current_dir(&dir);
        Ok(Prepared {
            vm_dir: dir.clone(),
            vsock: dir.join(VSOCK_SOCKET),
            api_socket,
            cgroup: None,
            command,
        })
    }

    /// The host paths of an unjailed VM, absolute.
    fn unjailed_paths(&self, dir: &Path) -> std::io::Result<VmPaths> {
        Ok(VmPaths {
            kernel: std::path::absolute(&self.config.image.kernel)?
                .display()
                .to_string(),
            rootfs: std::path::absolute(&self.config.image.rootfs)?
                .display()
                .to_string(),
            vsock: dir.join(VSOCK_SOCKET).display().to_string(),
        })
    }

    /// Configure and start, or restore, a VM through its API socket.
    async fn boot_through_api(
        &self,
        vm: &mut RunningVm,
        socket: &Path,
    ) -> Result<(), SandboxError> {
        let deadline = std::time::Instant::now() + self.config.api_timeout;
        loop {
            if let Some(status) = vm.exit_status() {
                return Err(SandboxError::Unavailable(format!(
                    "the VMM stopped before its API socket was ready: {status}"
                )));
            }
            if tokio::net::UnixStream::connect(socket).await.is_ok() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                return Err(SandboxError::Unavailable(format!(
                    "the API socket {} did not appear within {:?}",
                    socket.display(),
                    self.config.api_timeout
                )));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let client = ApiClient::new(socket).with_timeout(self.config.api_timeout);
        if let BootMethod::Snapshot(_) = &self.config.boot {
            return client
                .load_snapshot(SNAPSHOT_STATE_FILE, SNAPSHOT_MEMORY_FILE)
                .await;
        }
        let paths = match &self.config.confinement {
            Confinement::Jailer(_) => VmPaths::jailed(),
            Confinement::Unjailed { .. } => self
                .unjailed_paths(&vm.vm_dir)
                .map_err(|e| SandboxError::Unavailable(format!("bad VM paths: {e}")))?,
        };
        let document = vm_config(
            &self.config.image,
            self.config.machine,
            &paths,
            self.config.guest_port,
        );
        client.configure(&document).await?;
        client.start().await
    }
}

impl VmLauncher for FirecrackerLauncher {
    fn launch(&self) -> BoxFuture<'_, Result<Box<dyn LaunchedVm>, SandboxError>> {
        Box::pin(async move {
            let id = new_vm_id(&self.owner);
            validate_vm_id(&id)?;
            let prepared = self.prepare(&id)?;
            let mut vm = RunningVm::spawn(id, prepared)?;
            if let Some(socket) = vm.api_socket.clone()
                && let Err(e) = self.boot_through_api(&mut vm, &socket).await
            {
                vm.teardown().await;
                return Err(e);
            }
            Ok(Box::new(vm) as Box<dyn LaunchedVm>)
        })
    }

    fn image_name(&self) -> Option<String> {
        self.config.image.name.clone()
    }
}

/// The API socket of an unjailed VM, in its directory.
const UNJAILED_API_SOCKET: &str = "firecracker.socket";

/// The snapshot state file name in the chroot.
const SNAPSHOT_STATE_FILE: &str = "snapshot.state";

/// The snapshot memory file name in the chroot.
const SNAPSHOT_MEMORY_FILE: &str = "snapshot.mem";

/// Check that the longest socket path of a VM fits a Unix socket address.
///
/// The host connects to the vsock socket and to the API socket by their
/// full paths, so a long base directory breaks each connect.
fn check_socket_paths(vm_root: &Path, confinement: &Confinement) -> Result<(), SandboxError> {
    let id_len = VM_ID_PREFIX.len() + OWNER_TAG_HEX + 1 + VM_ID_RANDOM_HEX;
    let longest_suffix = match confinement {
        Confinement::Jailer(_) => format!("/root/{}", super::api::JAILED_API_SOCKET).len(),
        Confinement::Unjailed { .. } => format!("/{UNJAILED_API_SOCKET}").len(),
    }
    .max(format!("/root/{VSOCK_SOCKET}").len());
    let longest = vm_root.as_os_str().len() + 1 + id_len + longest_suffix;
    if longest > MAX_SOCKET_PATH_BYTES {
        return Err(SandboxError::Config(format!(
            "a VM socket path under {} takes {longest} bytes, over the {MAX_SOCKET_PATH_BYTES}-byte \
             limit of a Unix socket address; use a shorter chroot base or work root",
            vm_root.display()
        )));
    }
    Ok(())
}

/// The error of a failed launch step.
fn prepare_error(id: &str, what: &str, e: &std::io::Error) -> SandboxError {
    SandboxError::Unavailable(format!("cannot prepare {what} of VM {id}: {e}"))
}

/// The VM directory root of a confinement.
fn vm_root_of(confinement: &Confinement, exec_name: &str) -> PathBuf {
    match confinement {
        Confinement::Jailer(jailer) => jailer.chroot_base.join(exec_name),
        Confinement::Unjailed { work_root } => work_root.clone(),
    }
}

/// Hard-link `from` to `to`, or copy it across file systems.
fn link_or_copy(from: &Path, to: &Path) -> std::io::Result<()> {
    match std::fs::hard_link(from, to) {
        Ok(()) => Ok(()),
        Err(e) if matches!(e.raw_os_error(), Some(libc::EXDEV | libc::EPERM)) => {
            std::fs::copy(from, to).map(|_| ())
        }
        Err(e) => Err(e),
    }
}

/// Write a VM document as JSON.
fn write_config(path: &Path, document: &super::config::VmConfigFile) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(document).map_err(std::io::Error::other)?;
    std::fs::write(path, json)
}

/// The parts of a launch before the process starts.
struct Prepared {
    vm_dir: PathBuf,
    vsock: PathBuf,
    api_socket: Option<PathBuf>,
    cgroup: Option<PathBuf>,
    command: tokio::process::Command,
}

/// A running Firecracker process and what it owns on the host.
struct RunningVm {
    id: String,
    child: tokio::process::Child,
    vm_dir: PathBuf,
    vsock: PathBuf,
    api_socket: Option<PathBuf>,
    cgroup: Option<PathBuf>,
    stderr: Arc<Mutex<VecDeque<u8>>>,
    stderr_task: Option<tokio::task::JoinHandle<()>>,
    exited_at: Option<std::time::Instant>,
    down: bool,
}

impl RunningVm {
    fn spawn(id: String, mut prepared: Prepared) -> Result<Self, SandboxError> {
        prepared
            .command
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let mut child = match prepared.command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let _ = crate::sandbox::exec::remove_work_dir(&prepared.vm_dir);
                return Err(SandboxError::Unavailable(format!(
                    "cannot start the VMM for {id}: {e}"
                )));
            }
        };
        let stderr = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_TAIL_BYTES)));
        let stderr_task = child.stderr.take().map(|mut pipe| {
            let tail = Arc::clone(&stderr);
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt as _;
                let mut chunk = [0u8; 1024];
                while let Ok(n) = pipe.read(&mut chunk).await {
                    if n == 0 {
                        return;
                    }
                    let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
                    tail.extend(&chunk[..n]);
                    let excess = tail.len().saturating_sub(STDERR_TAIL_BYTES);
                    tail.drain(..excess);
                }
            })
        });
        Ok(Self {
            id,
            child,
            vm_dir: prepared.vm_dir,
            vsock: prepared.vsock,
            api_socket: prepared.api_socket,
            cgroup: prepared.cgroup,
            stderr,
            stderr_task,
            exited_at: None,
            down: false,
        })
    }

    fn stderr_tail(&self) -> String {
        let bytes: Vec<u8> = self
            .stderr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        String::from_utf8_lossy(&bytes).trim().to_owned()
    }

    async fn teardown(&mut self) {
        if self.down {
            return;
        }
        self.down = true;
        let _ = self.child.start_kill();
        let _ = tokio::time::timeout(KILL_WAIT, self.child.wait()).await;
        if let Some(cgroup) = &self.cgroup {
            kill_cgroup(cgroup).await;
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
        let dir = self.vm_dir.clone();
        let removed =
            tokio::task::spawn_blocking(move || crate::sandbox::exec::remove_work_dir(&dir)).await;
        if !matches!(removed, Ok(Ok(()))) {
            tracing::warn!(vm = %self.id, "could not remove a sandbox VM directory");
        }
    }
}

impl LaunchedVm for RunningVm {
    fn id(&self) -> &str {
        &self.id
    }

    fn vsock_path(&self) -> &Path {
        &self.vsock
    }

    fn exit_status(&mut self) -> Option<String> {
        let status = self.child.try_wait().ok().flatten()?;
        // Give the stderr reader a moment to take the last lines, so the
        // report holds the reason that the VMM printed.
        let first_seen = *self.exited_at.get_or_insert_with(std::time::Instant::now);
        let reader_done = self
            .stderr_task
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished);
        if !reader_done && first_seen.elapsed() < Duration::from_millis(200) {
            return None;
        }
        Some(format!(
            "firecracker exited with {status}: {}",
            self.stderr_tail()
        ))
    }

    fn shutdown(self: Box<Self>) -> BoxFuture<'static, ()> {
        let mut vm = self;
        Box::pin(async move { vm.teardown().await })
    }
}

impl Drop for RunningVm {
    fn drop(&mut self) {
        if self.down {
            return;
        }
        // The async teardown did not run. Kill now, and clean up what can
        // be cleaned without waiting. The next sweep removes the rest.
        let _ = self.child.start_kill();
        if let Some(cgroup) = &self.cgroup {
            let _ = std::fs::write(cgroup.join("cgroup.kill"), b"1");
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
        let _ = crate::sandbox::exec::remove_work_dir(&self.vm_dir);
    }
}

/// Kill every process in the cgroup at `dir`, wait for it to empty, and
/// remove it.
pub async fn kill_cgroup(dir: &Path) {
    if std::fs::write(dir.join("cgroup.kill"), b"1").is_err() {
        return;
    }
    let deadline = std::time::Instant::now() + CGROUP_EMPTY_WAIT;
    while !cgroup_is_empty(dir) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = std::fs::remove_dir(dir);
}

/// Kill the cgroup at `dir` and try to remove it, without waiting.
fn kill_cgroup_now(dir: &Path) {
    if std::fs::write(dir.join("cgroup.kill"), b"1").is_ok() {
        let deadline = std::time::Instant::now() + CGROUP_EMPTY_WAIT;
        while !cgroup_is_empty(dir) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = std::fs::remove_dir(dir);
    }
}

/// Whether `cgroup.events` reports no process left.
fn cgroup_is_empty(dir: &Path) -> bool {
    std::fs::read_to_string(dir.join("cgroup.events")).map_or(true, |events| {
        events.lines().any(|line| line == "populated 0")
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::time::Instant;

    use super::*;
    use crate::sandbox::firecracker::config::GUEST_AGENT_PORT;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "hl-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn image(dir: &Path) -> GuestImage {
        std::fs::write(dir.join("vmlinux"), b"kernel").unwrap();
        std::fs::write(dir.join("rootfs.ext4"), b"rootfs").unwrap();
        GuestImage::new(dir.join("vmlinux"), dir.join("rootfs.ext4")).with_name("test:1")
    }

    fn unjailed(dir: &Path, firecracker: PathBuf, boot: BootMethod) -> LauncherConfig {
        let work_root = dir.join("vms");
        std::fs::create_dir_all(&work_root).unwrap();
        LauncherConfig {
            firecracker_bin: firecracker,
            image: image(dir),
            machine: MachineSpec::default(),
            boot,
            confinement: Confinement::Unjailed { work_root },
            guest_port: GUEST_AGENT_PORT,
            api_timeout: Duration::from_secs(5),
        }
    }

    async fn wait_for(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "{} never appeared",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Let the writer finish the file.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn an_unjailed_config_file_launch_runs_with_no_host_env_and_cleans_up() {
        let dir = temp_dir();
        let fc = script(
            &dir,
            "firecracker",
            "printf '%s\\n' \"$@\" > args.txt; env > env.txt; exec sleep 60",
        );
        let launcher =
            FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let vm = launcher.launch().await.unwrap();
        let vm_dir = launcher.vm_root().join(vm.id());
        assert!(vm.id().starts_with(&format!("hsbx-{}-", launcher.owner())));
        assert_eq!(vm.vsock_path(), vm_dir.join("v.sock"));
        wait_for(&vm_dir.join("env.txt")).await;
        let args = std::fs::read_to_string(vm_dir.join("args.txt")).unwrap();
        let config_path = vm_dir.join("vm-config.json");
        assert_eq!(
            args.lines().collect::<Vec<_>>(),
            [
                "--id",
                vm.id(),
                "--config-file",
                config_path.to_str().unwrap(),
                "--no-api"
            ]
        );
        let env = std::fs::read_to_string(vm_dir.join("env.txt")).unwrap();
        assert!(!env.contains("CARGO"), "the host env leaked: {env}");
        assert!(!env.contains("HOME="), "the host env leaked: {env}");
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
        assert_eq!(
            config["boot-source"]["kernel_image_path"],
            dir.join("vmlinux").to_str().unwrap()
        );
        assert_eq!(
            config["vsock"]["uds_path"],
            vm_dir.join("v.sock").to_str().unwrap()
        );
        vm.shutdown().await;
        assert!(!vm_dir.exists(), "teardown must remove the VM directory");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn shutdown_kills_the_vmm_process() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo $$ > pid.txt; exec sleep 60");
        let launcher =
            FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let vm = launcher.launch().await.unwrap();
        let vm_dir = launcher.vm_root().join(vm.id());
        wait_for(&vm_dir.join("pid.txt")).await;
        let pid: i32 = std::fs::read_to_string(vm_dir.join("pid.txt"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        vm.shutdown().await;
        // SAFETY: signal 0 only checks for existence.
        let alive = unsafe { libc::kill(pid, 0) } == 0
            && std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .is_ok_and(|s| !s.contains(") Z"));
        assert!(!alive, "pid {pid} must be gone");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_vmm_that_exits_at_once_reports_its_status() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'bad kernel' >&2; exit 3");
        let launcher =
            FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let mut vm = launcher.launch().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = vm.exit_status() {
                break status;
            }
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert!(status.contains('3'), "{status}");
        assert!(status.contains("bad kernel"), "{status}");
        vm.shutdown().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn an_api_launch_that_finds_no_socket_fails_and_cleans_up() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exec sleep 60");
        let mut config = unjailed(&dir, fc, BootMethod::Api);
        config.api_timeout = Duration::from_millis(200);
        let launcher = FirecrackerLauncher::new(config).unwrap();
        let err = launcher.launch().await.err().expect("no api socket");
        assert!(matches!(err, SandboxError::Unavailable(_)));
        let left: Vec<_> = std::fs::read_dir(launcher.vm_root())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .collect();
        assert!(left.is_empty(), "a failed launch must leave nothing");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn an_api_launch_configures_and_starts_the_vm() {
        if std::process::Command::new("python3")
            .arg("-V")
            .output()
            .is_err()
        {
            eprintln!("skipped: the fake API server needs python3");
            return;
        }
        let dir = temp_dir();
        let server = dir.join("fake_api.py");
        std::fs::write(&server, FAKE_API_PY).unwrap();
        let fc = script(
            &dir,
            "firecracker",
            &format!("exec python3 {} \"$@\"", server.display()),
        );
        let launcher = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::Api)).unwrap();
        let vm = launcher.launch().await.unwrap();
        let log =
            std::fs::read_to_string(launcher.vm_root().join(vm.id()).join("requests.log")).unwrap();
        let lines: Vec<&str> = log.lines().collect();
        assert_eq!(
            lines,
            [
                "PUT /machine-config",
                "PUT /boot-source",
                "PUT /drives/rootfs",
                "PUT /vsock",
                "PUT /entropy",
                "PUT /actions",
            ]
        );
        vm.shutdown().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A fake Firecracker API: it logs each request line and answers 204.
    const FAKE_API_PY: &str = r#"
import os, socket, sys
args = sys.argv[1:]
sock_path = args[args.index("--api-sock") + 1]
log = open("requests.log", "a")
srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
srv.bind(sock_path)
srv.listen(8)
while True:
    conn, _ = srv.accept()
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
    if not data:
        conn.close()
        continue
    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        k, _, v = line.partition(b":")
        if k.strip().lower() == b"content-length":
            length = int(v.strip())
    while len(body) < length:
        body += conn.recv(4096)
    method, path = head.split(b"\r\n")[0].split(b" ")[:2]
    log.write(method.decode() + " " + path.decode() + "\n")
    log.flush()
    conn.sendall(b"HTTP/1.1 204 No Content\r\n\r\n")
    conn.close()
"#;

    #[test]
    fn the_sweep_removes_only_the_vms_of_dead_owners() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let live =
            FirecrackerLauncher::new(unjailed(&dir, fc.clone(), BootMethod::ConfigFile)).unwrap();
        let root = live.vm_root();
        let live_vm = root.join(format!("hsbx-{}-{}", live.owner(), "a".repeat(16)));
        std::fs::create_dir_all(&live_vm).unwrap();
        // A dead owner with an unlocked lease, and one with no lease at all.
        std::fs::write(root.join("hsbx-deadbeef.lease"), b"").unwrap();
        let dead_vm = root.join(format!("hsbx-deadbeef-{}", "b".repeat(16)));
        std::fs::create_dir_all(dead_vm.join("root")).unwrap();
        let orphan_vm = root.join(format!("hsbx-0badc0de-{}", "c".repeat(16)));
        std::fs::create_dir_all(&orphan_vm).unwrap();
        let unrelated = root.join("keep-me");
        std::fs::create_dir_all(&unrelated).unwrap();

        let sweeper = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        assert_eq!(sweeper.sweep_stale(), 2);
        assert!(live_vm.exists(), "a live owner keeps its VMs");
        assert!(live.lease_path().exists());
        assert!(!dead_vm.exists());
        assert!(!orphan_vm.exists());
        assert!(!root.join("hsbx-deadbeef.lease").exists());
        assert!(unrelated.exists());
        drop((live, sweeper));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_lease_is_exclusive_and_removed_on_drop() {
        let dir = temp_dir();
        let lease = InstanceLease::acquire(&dir, "0123abcd").unwrap();
        assert!(
            InstanceLease::acquire(&dir, "0123abcd").is_err(),
            "the lock is held"
        );
        let path = lease.path().to_path_buf();
        drop(lease);
        assert!(!path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_missing_image_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config.image.rootfs = dir.join("missing.ext4");
        assert!(matches!(
            FirecrackerLauncher::new(config),
            Err(SandboxError::Config(_))
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_base_dir_too_long_for_a_socket_path_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        let deep = dir.join("d".repeat(80));
        std::fs::create_dir_all(&deep).unwrap();
        config.confinement = Confinement::Unjailed { work_root: deep };
        let err = FirecrackerLauncher::new(config).expect_err("too long");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("Unix socket")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn verify_host_checks_the_version_and_the_posture() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'Firecracker v1.17.0'");
        let launcher =
            FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        // An empty root has no /dev/kvm and no cgroup v2: critical findings.
        let empty = dir.join("empty-root");
        std::fs::create_dir_all(&empty).unwrap();
        let err = launcher
            .verify_host_at(&empty, PosturePolicy::Enforce)
            .await
            .expect_err("critical findings");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("kvm")),
            "{err:?}"
        );
        let findings = launcher
            .verify_host_at(&empty, PosturePolicy::Report)
            .await
            .unwrap();
        assert_ne!(findings, []);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn verify_host_refuses_an_old_firecracker_even_when_reporting() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'Firecracker v1.14.0'");
        let launcher =
            FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let err = launcher
            .verify_host_at(&dir, PosturePolicy::Report)
            .await
            .expect_err("old version");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("CVE-")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn kill_cgroup_writes_the_kill_file() {
        let dir = temp_dir();
        let cgroup = dir.join("hsbx-x");
        std::fs::create_dir_all(&cgroup).unwrap();
        std::fs::write(cgroup.join("cgroup.kill"), b"").unwrap();
        std::fs::write(cgroup.join("cgroup.events"), b"populated 0\nfrozen 0\n").unwrap();
        kill_cgroup(&cgroup).await;
        assert_eq!(std::fs::read(cgroup.join("cgroup.kill")).unwrap(), b"1");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_jailed_launch_links_the_image_and_runs_the_jailer() {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } != 0 {
            eprintln!("skipped: a jailed launch needs root");
            return;
        }
        let dir = temp_dir();
        let jailer = script(
            &dir,
            "jailer",
            "printf '%s\\n' \"$@\" > jailer-args.txt; exec sleep 60",
        );
        let fc = script(&dir, "firecracker", "exit 0");
        let mut jail = JailerConfig::new(jailer, 12_345, 12_345);
        jail.chroot_base = dir.join("jail");
        jail.cgroup_root = dir.join("cgroup");
        let config = LauncherConfig {
            firecracker_bin: fc,
            image: image(&dir),
            machine: MachineSpec::default(),
            boot: BootMethod::ConfigFile,
            confinement: Confinement::Jailer(jail),
            guest_port: GUEST_AGENT_PORT,
            api_timeout: Duration::from_secs(5),
        };
        let launcher = FirecrackerLauncher::new(config).unwrap();
        let vm = launcher.launch().await.unwrap();
        let jail_dir = dir.join("jail/firecracker").join(vm.id());
        let root = jail_dir.join("root");
        assert_eq!(vm.vsock_path(), root.join("v.sock"));
        wait_for(&jail_dir.join("jailer-args.txt")).await;
        let args = std::fs::read_to_string(jail_dir.join("jailer-args.txt")).unwrap();
        assert!(args.lines().any(|l| l == "--cgroup-version"));
        assert!(
            args.ends_with("--\n--config-file\nvm-config.json\n--no-api\n"),
            "{args}"
        );
        let original = std::fs::metadata(dir.join("rootfs.ext4")).unwrap().ino();
        assert_eq!(
            std::fs::metadata(root.join("rootfs.ext4")).unwrap().ino(),
            original
        );
        assert!(root.join("vmlinux").exists());
        assert!(root.join("vm-config.json").exists());
        vm.shutdown().await;
        assert!(!jail_dir.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
