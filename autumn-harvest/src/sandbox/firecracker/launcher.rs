//! Start and stop Firecracker microVMs.
//!
//! [`VmLauncher`] starts one microVM. [`LaunchedVm`] is a running microVM
//! that serves one run and then goes away. [`FirecrackerLauncher`] is the
//! real launcher: it runs Firecracker behind the jailer, or without a jail
//! for development.
//!
//! Teardown kills the process, kills the cgroup with `cgroup.kill`, and
//! removes the VM directory, in that order. Without `cgroup.kill`, teardown
//! freezes the cgroup and kills each process in it. The host never reads a
//! pid from the jail, because the jailed VMM can write there.
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

    /// The vsock port of the guest agent, when the launcher sets it. The
    /// backend then uses it in place of its own setting.
    fn guest_port(&self) -> Option<u32> {
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
    /// The vsock port of the guest agent. It must not be `u32::MAX`, the
    /// wildcard port.
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
    /// Set after the first version check that passes.
    versions_checked: tokio::sync::OnceCell<()>,
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

/// The most vCPUs that Firecracker gives one microVM.
const MAX_VCPUS: u8 = 32;

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
    /// Returns [`SandboxError::Config`] in these cases:
    ///
    /// - the machine has no memory, or a vCPU count outside 1 to 32;
    /// - an API or snapshot boot has an API timeout of zero;
    /// - an extra boot argument would replace a launcher argument;
    /// - a path is missing;
    /// - a snapshot boot has no jailer;
    /// - the jail uid or gid is 0, or this process is not root with a jail;
    /// - the jail cannot read an image;
    /// - another user can change a binary, an image, the jail root or the
    ///   network namespace of a jailed launch;
    /// - the lease cannot be taken.
    pub fn new(config: LauncherConfig) -> Result<Self, SandboxError> {
        // SAFETY: geteuid has no preconditions.
        Self::new_with_euid(config, unsafe { libc::geteuid() })
    }

    /// [`Self::new`] for a process with the effective uid `euid`.
    fn new_with_euid(config: LauncherConfig, euid: u32) -> Result<Self, SandboxError> {
        // Firecracker refuses these at each launch. Refuse them once, here.
        let machine = config.machine;
        if !(1..=MAX_VCPUS).contains(&machine.vcpu_count) || machine.mem_size_mib == 0 {
            return Err(SandboxError::Config(format!(
                "a microVM needs 1 to {MAX_VCPUS} vCPUs and some memory, not {} vCPUs and {} MiB",
                machine.vcpu_count, machine.mem_size_mib
            )));
        }
        super::config::check_boot_args(&config.image)?;
        // The guest would listen on a port of its own choice.
        if config.guest_port == super::vsock::VMADDR_PORT_ANY {
            return Err(SandboxError::Config(
                "the guest port u32::MAX is VMADDR_PORT_ANY, which no host can connect to".into(),
            ));
        }
        if matches!(config.boot, BootMethod::Api | BootMethod::Snapshot(_))
            && config.api_timeout.is_zero()
        {
            return Err(SandboxError::Config(
                "an API or snapshot boot needs an API timeout above zero".into(),
            ));
        }
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
        for (what, path) in &required {
            if !path.is_file() {
                return Err(SandboxError::Config(format!(
                    "{what} {} is not a file",
                    path.display()
                )));
            }
        }
        if let Confinement::Jailer(jailer) = &config.confinement {
            if jailer.uid == 0 || jailer.gid == 0 {
                return Err(SandboxError::Config(
                    "the jailer must drop to a non-root uid and gid, or an escaped VMM keeps root"
                        .into(),
                ));
            }
            super::jailer::check_extra_cgroups(jailer)?;
            // The jailer makes the chroot and the cgroup, then drops to the
            // jail uid. Each step needs root, so another uid fails each launch.
            if euid != 0 {
                return Err(SandboxError::Config(format!(
                    "the jailer needs root, and this process runs as uid {euid}; run the worker \
                     as root or use Confinement::Unjailed"
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
        if let Confinement::Jailer(jailer) = &config.confinement {
            check_trusted_files(&config, jailer, &vm_root)?;
        }
        let lease = InstanceLease::acquire(&vm_root, &owner)?;
        Ok(Self {
            config,
            exec_name,
            owner,
            lease,
            versions_checked: tokio::sync::OnceCell::new(),
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
        self.check_versions().await?;
        let mut findings = super::posture::inspect_host(root);
        match &self.config.confinement {
            Confinement::Jailer(jailer) => {
                findings.extend(super::posture::inspect_jail_base(&jailer.chroot_base));
                if jailer.netns.is_none() {
                    findings.push(super::posture::PostureFinding {
                        check: "netns".into(),
                        detail: "the jailer keeps Firecracker in the worker network namespace, \
                                 so an escaped VMM has the host network; set JailerConfig::netns \
                                 to an empty namespace"
                            .into(),
                        severity: super::posture::Severity::Critical,
                    });
                }
            }
            Confinement::Unjailed { .. } => findings.push(super::posture::PostureFinding {
                check: "jailer".into(),
                detail: "Firecracker runs without the jailer: no chroot, no cgroup, no uid change"
                    .into(),
                severity: super::posture::Severity::Critical,
            }),
        }
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
        // A dropped microVM can leave its cgroup after its jail is gone.
        if let Confinement::Jailer(jailer) = &self.config.confinement {
            let parent = jailer.cgroup_root.join(&jailer.cgroup_parent);
            for entry in std::fs::read_dir(parent).into_iter().flatten().flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                let owner = name
                    .strip_prefix(VM_ID_PREFIX)
                    .and_then(|rest| rest.split_once('-'))
                    .map(|(owner, _)| owner);
                if owner.is_some_and(|owner| !live.contains(owner)) {
                    kill_cgroup_now(&entry.path());
                }
            }
        }
        for (path, _locked) in dead_leases {
            let _ = std::fs::remove_file(path);
        }
        removed
    }

    /// Check the Firecracker and jailer versions once. A failed check runs
    /// again on the next call.
    async fn check_versions(&self) -> Result<(), SandboxError> {
        self.versions_checked
            .get_or_try_init(|| async {
                use super::posture::{VmmBinary, probe_version};
                probe_version(&self.config.firecracker_bin, VmmBinary::Firecracker).await?;
                if let Confinement::Jailer(jailer) = &self.config.confinement {
                    probe_version(&jailer.jailer_bin, VmmBinary::Jailer).await?;
                }
                Ok::<(), SandboxError>(())
            })
            .await?;
        Ok(())
    }

    /// Create the VM directory and the command for `id`.
    async fn prepare(&self, id: &str, boot: &BootMethod) -> Result<Prepared, SandboxError> {
        match &self.config.confinement {
            Confinement::Jailer(jailer) => self.prepare_jailed(jailer, id, boot).await,
            Confinement::Unjailed { work_root } => self.prepare_unjailed(work_root, id, boot),
        }
    }

    /// Boot a golden microVM and save a full snapshot of it in `out_dir`.
    ///
    /// The golden microVM boots through the API with the image, machine and
    /// jailer of this launcher. The host waits for a pong from the guest
    /// agent, then pauses the microVM and takes the snapshot. No code runs
    /// in it. Give the files to [`BootMethod::Snapshot`] in a launcher with
    /// the same image, machine and jailer.
    ///
    /// The files get mode `0640` and the jailer group, so each jailed VMM
    /// can read them. They hold the guest memory, so the golden microVM must
    /// hold no secret.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] without the jailer, and
    /// [`SandboxError::Unavailable`] when a step fails. The golden microVM
    /// is torn down on each path.
    pub async fn create_golden_snapshot(
        &self,
        out_dir: &Path,
        boot_timeout: Duration,
    ) -> Result<SnapshotFiles, SandboxError> {
        let Confinement::Jailer(jailer) = &self.config.confinement else {
            return Err(SandboxError::Config(
                "a golden snapshot needs the jailer, so each restore sees the same paths".into(),
            ));
        };
        self.check_versions().await?;
        let id = new_vm_id(&self.owner);
        validate_vm_id(&id)?;
        let prepared = self.prepare_jailed(jailer, &id, &BootMethod::Api).await?;
        let mut vm = RunningVm::spawn(id, prepared)?;
        let result = self
            .snapshot_vm(&mut vm, out_dir, boot_timeout, jailer.gid)
            .await;
        vm.teardown().await;
        result
    }

    /// Boot `vm`, wait for its guest agent, pause it, snapshot it, and copy
    /// the files to `out_dir`.
    async fn snapshot_vm(
        &self,
        vm: &mut RunningVm,
        out_dir: &Path,
        boot_timeout: Duration,
        gid: u32,
    ) -> Result<SnapshotFiles, SandboxError> {
        let socket = vm.api_socket.clone().ok_or_else(|| {
            SandboxError::Unavailable("the golden microVM has no API socket".into())
        })?;
        self.boot_through_api(vm, &socket, &BootMethod::Api).await?;
        let deadline = std::time::Instant::now() + boot_timeout;
        let vsock = vm.vsock.clone();
        let mut exited = || vm.exit_status();
        let mut stream =
            super::vsock::connect_guest(&vsock, self.config.guest_port, deadline, &mut exited)
                .await?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        tokio::time::timeout(remaining, crate::sandbox::guest::ping(&mut stream))
            .await
            .map_err(|_| {
                SandboxError::Unavailable("the golden guest agent did not answer a ping".into())
            })??;
        drop(stream);
        let client = ApiClient::new(&socket).with_timeout(SNAPSHOT_TIMEOUT);
        client.pause().await?;
        client
            .create_snapshot(SNAPSHOT_STATE_FILE, SNAPSHOT_MEMORY_FILE)
            .await?;
        let root = vm.vm_dir.join("root");
        let files = SnapshotFiles {
            state: out_dir.join(SNAPSHOT_STATE_FILE),
            memory: out_dir.join(SNAPSHOT_MEMORY_FILE),
        };
        let copies = [
            (root.join(SNAPSHOT_STATE_FILE), files.state.clone()),
            (root.join(SNAPSHOT_MEMORY_FILE), files.memory.clone()),
        ];
        let out = out_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&out)?;
            for (from, to) in &copies {
                copy_out(from, to, gid)?;
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        .map_err(|e| SandboxError::Unavailable(format!("the snapshot copy failed: {e}")))?
        .map_err(|e| SandboxError::Unavailable(format!("cannot copy the snapshot: {e}")))?;
        Ok(files)
    }

    /// Lay out the jail of `id` and build the jailer command.
    async fn prepare_jailed(
        &self,
        jailer: &JailerConfig,
        id: &str,
        boot: &BootMethod,
    ) -> Result<Prepared, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        let config = &self.config;
        let jail = super::jailer::jail_dir(jailer, &self.exec_name, id);
        let root = jail.join("root");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)
            .map_err(|e| prepare_error(id, "the jail", &e))?;
        // A failed step or a dropped launch removes the jail, so it leaves
        // nothing.
        let guard = JailGuard::new(jail.clone());
        let fail = |what: &str, e: &std::io::Error| prepare_error(id, what, e);
        let mut links = vec![
            (
                config.image.kernel.clone(),
                root.join(KERNEL_FILE),
                "the kernel",
            ),
            (
                config.image.rootfs.clone(),
                root.join(ROOTFS_FILE),
                "the root file system",
            ),
        ];
        if let BootMethod::Snapshot(files) = boot {
            links.push((
                files.state.clone(),
                root.join(SNAPSHOT_STATE_FILE),
                "the snapshot state",
            ));
            links.push((
                files.memory.clone(),
                root.join(SNAPSHOT_MEMORY_FILE),
                "the snapshot memory",
            ));
        }
        // A copy across file systems can take seconds, so it runs off the
        // async workers. A dropped launch does not stop this task. So the
        // task removes the jail again when the guard is gone.
        let abandoned = guard.abandoned();
        let task_jail = jail.clone();
        let linked = tokio::task::spawn_blocking(move || {
            let linked = links
                .into_iter()
                .try_for_each(|(from, to, what)| link_or_copy(&from, &to).map_err(|e| (what, e)));
            if abandoned.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = crate::sandbox::exec::remove_work_dir(&task_jail);
            }
            linked
        })
        .await;
        match linked {
            Ok(Ok(())) => {}
            Ok(Err((what, e))) => return Err(fail(what, &e)),
            Err(e) => return Err(fail("the image links", &std::io::Error::other(e))),
        }
        let mut api_socket = None;
        let firecracker_args: Vec<String> = match boot {
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
            BootMethod::Api | BootMethod::Snapshot(_) => {
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
        guard.disarm();
        Ok(Prepared {
            vm_dir: jail,
            vsock: root.join(VSOCK_SOCKET),
            api_socket,
            cgroup: Some(super::jailer::cgroup_dir(jailer, id)),
            command,
        })
    }

    /// Create the directory of `id` and build the Firecracker command.
    fn prepare_unjailed(
        &self,
        work_root: &Path,
        id: &str,
        boot: &BootMethod,
    ) -> Result<Prepared, SandboxError> {
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
        match boot {
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
        boot: &BootMethod,
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
        if let BootMethod::Snapshot(_) = boot {
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
            self.check_versions().await?;
            let id = new_vm_id(&self.owner);
            validate_vm_id(&id)?;
            let prepared = self.prepare(&id, &self.config.boot).await?;
            let mut vm = RunningVm::spawn(id, prepared)?;
            if let Some(socket) = vm.api_socket.clone()
                && let Err(e) = self
                    .boot_through_api(&mut vm, &socket, &self.config.boot)
                    .await
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

    fn guest_port(&self) -> Option<u32> {
        Some(self.config.guest_port)
    }
}

/// The API socket of an unjailed VM, in its directory.
const UNJAILED_API_SOCKET: &str = "firecracker.socket";

/// The snapshot state file name in the chroot.
const SNAPSHOT_STATE_FILE: &str = "snapshot.state";

/// The snapshot memory file name in the chroot.
const SNAPSHOT_MEMORY_FILE: &str = "snapshot.mem";

/// The time limit of the pause and snapshot calls. A full snapshot writes
/// the whole guest memory.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Copy a file that a jailed VMM wrote to `to`, with mode `0640` and group
/// `gid`.
///
/// The VMM can write its jail, so the source must be a regular file and not
/// a symbolic link.
fn copy_out(from: &Path, to: &Path, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
    let mut source = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(from)?;
    if !source.metadata()?.is_file() {
        return Err(std::io::Error::other(format!(
            "{} is not a regular file",
            from.display()
        )));
    }
    let mut target = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o640)
        .custom_flags(libc::O_NOFOLLOW)
        .open(to)?;
    std::io::copy(&mut source, &mut target)?;
    std::os::unix::fs::fchown(&target, None, Some(gid))?;
    target.set_permissions(std::fs::Permissions::from_mode(0o640))?;
    target.sync_all()
}

/// Check that no other user can change a path that a jailed launch trusts.
///
/// The launcher runs the Firecracker and jailer binaries as root, before the
/// jailer drops to the jail uid. A user who can change one runs code as root.
///
/// The root jailer builds each jail under `vm_root`, and it joins the network
/// namespace. A user who can move either path can move a jail, or give an
/// escaped VMM a network.
///
/// The jail holds hard links to the kernel, the root file system and the
/// snapshot files. A file that the jail uid owns, or that its group or
/// others can write, lets one escaped VMM change the next microVM.
fn check_trusted_files(
    config: &LauncherConfig,
    jailer: &JailerConfig,
    vm_root: &Path,
) -> Result<(), SandboxError> {
    use std::os::unix::fs::MetadataExt as _;
    // SAFETY: geteuid has no preconditions.
    let euid = unsafe { libc::geteuid() };
    let inspect = |what: &Path| {
        std::fs::symlink_metadata(what)
            .map_err(|e| SandboxError::Config(format!("cannot inspect {}: {e}", what.display())))
    };
    let mut trusted = vec![
        config.firecracker_bin.as_path(),
        jailer.jailer_bin.as_path(),
        vm_root,
    ];
    trusted.extend(jailer.netns.as_deref());
    for path in trusted {
        check_trusted_ancestors(path, euid, inspect)?;
    }
    let jail_uid = jailer.uid;
    let mut shared = vec![&config.image.kernel, &config.image.rootfs];
    if let BootMethod::Snapshot(files) = &config.boot {
        shared.push(&files.state);
        shared.push(&files.memory);
    }
    for path in shared {
        check_trusted_ancestors(path, euid, inspect)?;
        let meta = inspect(path)?;
        if meta.uid() == jail_uid || meta.mode() & 0o022 != 0 {
            return Err(SandboxError::Config(format!(
                "{} must not belong to the jail uid {jail_uid}, and only its owner may write it",
                path.display()
            )));
        }
        // The VMM opens the file after the jailer drops to the jail uid and
        // gid, and the jail uid never owns it.
        let readable =
            meta.mode() & 0o004 != 0 || (meta.gid() == jailer.gid && meta.mode() & 0o040 != 0);
        if !readable {
            return Err(SandboxError::Config(format!(
                "the jail uid {jail_uid} and gid {} cannot read {}",
                jailer.gid,
                path.display()
            )));
        }
    }
    Ok(())
}

/// Check the path of a trusted file and each of its parent directories.
///
/// A user who can write a parent can replace the file between the check and
/// its use. So the path must hold no symbolic link, and each part must
/// belong to root or to this process. A directory that others can write
/// must be sticky, such as /tmp, so each entry stays with its owner.
fn check_trusted_ancestors(
    path: &Path,
    euid: u32,
    inspect: impl Fn(&Path) -> Result<std::fs::Metadata, SandboxError>,
) -> Result<(), SandboxError> {
    use std::os::unix::fs::MetadataExt as _;
    let absolute = std::path::absolute(path)
        .map_err(|e| SandboxError::Config(format!("bad path {}: {e}", path.display())))?;
    let canonical = std::fs::canonicalize(&absolute)
        .map_err(|e| SandboxError::Config(format!("cannot resolve {}: {e}", path.display())))?;
    if canonical != absolute {
        return Err(SandboxError::Config(format!(
            "{} must not pass through a symbolic link",
            path.display()
        )));
    }
    for part in canonical.ancestors() {
        let meta = inspect(part)?;
        let others_write = meta.mode() & 0o022 != 0;
        let sticky_dir = meta.is_dir() && meta.mode() & 0o1000 != 0;
        if (meta.uid() != 0 && meta.uid() != euid) || (others_write && !sticky_dir) {
            return Err(SandboxError::Config(format!(
                "{} must belong to root or to this process, and others must not write it",
                part.display()
            )));
        }
    }
    Ok(())
}

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
            copy_keeping_owner(from, to)
        }
        Err(e) => Err(e),
    }
}

/// Copy `from` to `to` with the owner, group and mode of `from`.
///
/// A hard link shares these with the source, and a plain copy takes the
/// owner of this process. A golden snapshot is mode `0640` for the jailer
/// group, so a plain copy would lock the jailed VMM out of it.
fn copy_keeping_owner(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let meta = std::fs::metadata(from)?;
    std::fs::copy(from, to)?;
    std::os::unix::fs::chown(to, Some(meta.uid()), Some(meta.gid()))?;
    // A change of owner clears the set-id bits, so set the mode again.
    std::fs::set_permissions(to, meta.permissions())
}

/// Write a VM document as JSON.
fn write_config(path: &Path, document: &super::config::VmConfigFile) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(document).map_err(std::io::Error::other)?;
    std::fs::write(path, json)
}

/// Removes a jail when a launch stops before the VMM owns it.
///
/// A dropped launch future drops the guard, and the guard removes the jail.
/// The guard also marks the jail as abandoned, so a task that still writes
/// into it removes it again when it ends.
struct JailGuard {
    jail: Option<PathBuf>,
    abandoned: Arc<std::sync::atomic::AtomicBool>,
}

impl JailGuard {
    fn new(jail: PathBuf) -> Self {
        Self {
            jail: Some(jail),
            abandoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The flag that the guard sets when it removes the jail.
    fn abandoned(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.abandoned)
    }

    /// Keep the jail. The caller now owns its removal.
    fn disarm(mut self) {
        self.jail = None;
    }
}

impl Drop for JailGuard {
    fn drop(&mut self) {
        if let Some(jail) = self.jail.take() {
            self.abandoned
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let _ = crate::sandbox::exec::remove_work_dir(&jail);
        }
    }
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

    /// Kill the VMM, then its cgroup, then remove its directory. Each step
    /// is safe to repeat. `down` is set only at the end, so a drop in the
    /// middle still runs the rest in [`Drop`].
    async fn teardown(&mut self) {
        if self.down {
            return;
        }
        let _ = self.child.start_kill();
        let _ = tokio::time::timeout(KILL_WAIT, self.child.wait()).await;
        if let Some(cgroup) = &self.cgroup {
            kill_cgroup(cgroup).await;
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
        let dir = self.vm_dir.clone();
        let removed = tokio::task::spawn_blocking(move || {
            match crate::sandbox::exec::remove_work_dir(&dir) {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                other => other,
            }
        })
        .await;
        if !matches!(removed, Ok(Ok(()))) {
            tracing::warn!(vm = %self.id, "could not remove a sandbox VM directory");
        }
        self.down = true;
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
            // The kill is not instant, so this removal can fail. The next
            // `sweep_stale` then removes the cgroup.
            crate::sandbox::cgroup::kill_and_remove(cgroup, Duration::ZERO);
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
        let _ = crate::sandbox::exec::remove_work_dir(&self.vm_dir);
    }
}

/// Kill every process in the cgroup at `dir`, wait for it to empty, and
/// remove it. See [`crate::sandbox::cgroup::kill`] for a kernel without
/// `cgroup.kill`.
pub async fn kill_cgroup(dir: &Path) {
    let dir = dir.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || {
        crate::sandbox::cgroup::kill_and_remove(&dir, CGROUP_EMPTY_WAIT);
    })
    .await;
}

/// Kill the cgroup at `dir`, wait for it to empty, and remove it.
fn kill_cgroup_now(dir: &Path) {
    crate::sandbox::cgroup::kill_and_remove(dir, CGROUP_EMPTY_WAIT);
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

    /// A fake binary. A fake `firecracker` or `jailer` answers `--version`
    /// like the real one, unless `body` prints its own version.
    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        let version = match name {
            "firecracker" if !body.contains(" v1.") => "Firecracker v1.17.0",
            "jailer" => "Jailer v1.17.0",
            _ => "",
        };
        let answer = if version.is_empty() {
            String::new()
        } else {
            format!("if [ \"$1\" = --version ]; then echo '{version}'; exit 0; fi\n")
        };
        crate::sandbox::firecracker::fake::write_executable(
            &path,
            &format!("#!/bin/sh\n{answer}{body}\n"),
        );
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
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        let launcher = new_launcher(config).unwrap();
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
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::Api)).unwrap();
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
        let live = new_launcher(unjailed(&dir, fc.clone(), BootMethod::ConfigFile)).unwrap();
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

        let sweeper = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        assert!(matches!(new_launcher(config), Err(SandboxError::Config(_))));
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
        let err = new_launcher(config).expect_err("too long");
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
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
    async fn verify_host_flags_a_jail_without_a_network_namespace() {
        let dir = temp_dir();
        let empty = dir.join("empty-root");
        std::fs::create_dir_all(&empty).unwrap();
        let netns_findings = |config: LauncherConfig| {
            let launcher = new_launcher(config).unwrap();
            let root = empty.clone();
            async move {
                launcher
                    .verify_host_at(&root, PosturePolicy::Report)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|f| f.check == "netns")
                    .collect::<Vec<_>>()
            }
        };
        // Without a namespace, an escaped VMM keeps the host network.
        let found = netns_findings(jailed(&dir)).await;
        assert!(
            matches!(found.as_slice(), [f] if f.severity == super::super::posture::Severity::Critical),
            "{found:?}"
        );
        let netns = dir.join("netns-vm");
        std::fs::write(&netns, b"").unwrap();
        let mut config = jailed(&dir);
        if let Confinement::Jailer(jail) = &mut config.confinement {
            jail.netns = Some(netns);
        }
        assert_eq!(netns_findings(config).await, []);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn verify_host_refuses_an_old_firecracker_even_when_reporting() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'Firecracker v1.14.0'");
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
    async fn a_cgroup_without_cgroup_kill_has_each_process_killed() {
        use std::os::unix::process::ExitStatusExt as _;
        let cgroup = temp_dir();
        // A directory makes the kill write fail, as on Linux before 5.14.
        std::fs::create_dir(cgroup.join("cgroup.kill")).unwrap();
        let mut child = std::process::Command::new("sleep")
            .arg("10")
            .spawn()
            .unwrap();
        std::fs::write(cgroup.join("cgroup.procs"), format!("{}\n", child.id())).unwrap();
        let waiter = std::thread::spawn(move || child.wait().unwrap());
        kill_cgroup(&cgroup).await;
        let status = waiter.join().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");
        std::fs::remove_dir_all(cgroup).unwrap();
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
    #[ignore = "needs root; the Sandbox (KVM) workflow runs it with sudo"]
    async fn root_jailed_launch_links_the_image_and_runs_the_jailer() {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: geteuid has no preconditions.
        assert_eq!(unsafe { libc::geteuid() }, 0, "run this test as root");
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
        let launcher = new_launcher(config).unwrap();
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

    #[tokio::test]
    async fn a_launch_refuses_a_firecracker_with_a_known_escape() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'Firecracker v1.14.3'");
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let err = launcher
            .launch()
            .await
            .map(|_| ())
            .expect_err("old version");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("CVE-2026-5747")),
            "{err:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_jailer_path_that_runs_firecracker_is_a_config_error() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let firecracker = config.firecracker_bin.clone();
        if let Confinement::Jailer(jail) = &mut config.confinement {
            jail.jailer_bin = firecracker;
        }
        let launcher = new_launcher(config).unwrap();
        let err = launcher
            .launch()
            .await
            .map(|_| ())
            .expect_err("the jailer path runs Firecracker");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("Jailer")),
            "{err:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A launcher for `config`, built as if this process were root. A
    /// jailed launcher needs root, and most tests run without it.
    fn new_launcher(config: LauncherConfig) -> Result<FirecrackerLauncher, SandboxError> {
        FirecrackerLauncher::new_with_euid(config, 0)
    }

    fn jailed(dir: &Path) -> LauncherConfig {
        let mut jail = JailerConfig::new(script(dir, "jailer", "exec sleep 60"), 12_345, 12_345);
        jail.chroot_base = dir.join("jail");
        jail.cgroup_root = dir.join("cgroup");
        LauncherConfig {
            firecracker_bin: script(dir, "firecracker", "exit 0"),
            image: image(dir),
            machine: MachineSpec::default(),
            boot: BootMethod::ConfigFile,
            confinement: Confinement::Jailer(jail),
            guest_port: GUEST_AGENT_PORT,
            api_timeout: Duration::from_secs(5),
        }
    }

    #[test]
    fn a_machine_outside_the_firecracker_range_is_a_config_error() {
        let dir = temp_dir();
        for machine in [
            MachineSpec {
                vcpu_count: 0,
                mem_size_mib: 512,
            },
            MachineSpec {
                vcpu_count: 33,
                mem_size_mib: 512,
            },
            MachineSpec {
                vcpu_count: 1,
                mem_size_mib: 0,
            },
        ] {
            let fc = script(&dir, "firecracker", "exit 0");
            let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
            config.machine = machine;
            let err = new_launcher(config).expect_err("bad machine");
            assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_root_jail_identity_is_a_config_error() {
        for (uid, gid) in [(0, 12_345), (12_345, 0)] {
            let dir = temp_dir();
            let mut config = jailed(&dir);
            if let Confinement::Jailer(jail) = &mut config.confinement {
                jail.uid = uid;
                jail.gid = gid;
            }
            let err = new_launcher(config).expect_err("root jail identity");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("root")),
                "{err:?}"
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn a_shared_image_in_a_directory_that_others_can_write_is_a_config_error() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let parent = config.image.rootfs.parent().unwrap().to_path_buf();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o777)).unwrap();
        let err = new_launcher(config.clone()).expect_err("writable parent");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("write")),
            "{err:?}"
        );
        // A sticky directory, such as /tmp, keeps each entry to its owner.
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o1777)).unwrap();
        new_launcher(config).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_shared_image_behind_a_symbolic_link_is_a_config_error() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let link = dir.join("rootfs-link.ext4");
        std::os::unix::fs::symlink(&config.image.rootfs, &link).unwrap();
        config.image.rootfs = link;
        let err = new_launcher(config).expect_err("symlink");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("symbolic link")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_launcher_binary_that_others_can_change_is_a_config_error() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let Confinement::Jailer(jail) = &config.confinement else {
            unreachable!("a jailed config");
        };
        let jailer_bin = jail.jailer_bin.clone();
        let link = dir.join("bin-link");
        for bin in [config.firecracker_bin.clone(), jailer_bin] {
            let name = bin.file_name().unwrap().to_str().unwrap().to_owned();
            let set = |config: &mut LauncherConfig, path: PathBuf| match &mut config.confinement {
                Confinement::Jailer(jail) if name == "jailer" => jail.jailer_bin = path,
                _ => config.firecracker_bin = path,
            };
            // The launcher runs each binary as root, before the jailer
            // drops to the jail uid.
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o757)).unwrap();
            let err = new_launcher(config.clone()).expect_err("writable binary");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains(&name)),
                "{name}: {err:?}"
            );
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            let _ = std::fs::remove_file(&link);
            std::os::unix::fs::symlink(&bin, &link).unwrap();
            let mut linked = config.clone();
            set(&mut linked, link.clone());
            let err = new_launcher(linked).expect_err("linked binary");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("symbolic link")),
                "{name}: {err:?}"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_launch_dropped_while_it_links_the_image_leaves_no_jail() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let vm_root = dir.join("jail/firecracker");
        let jails = || -> Vec<PathBuf> {
            std::fs::read_dir(&vm_root)
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir())
                .collect()
        };
        let mut launch = launcher.launch();
        // Poll once at a time. The link step is the first wait after the
        // jail exists, so the launch stops there.
        let deadline = Instant::now() + Duration::from_secs(5);
        while jails().is_empty() {
            assert!(Instant::now() < deadline, "the launch never made a jail");
            let polled = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(launch.as_mut().poll(cx).is_pending())
            })
            .await;
            assert!(polled, "the launch ended before it made a jail");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        drop(launch);
        // The link task can still run after the drop.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !jails().is_empty() {
            assert!(Instant::now() < deadline, "the jail stayed: {:?}", jails());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_jail_path_that_others_can_change_is_a_config_error() {
        let dir = temp_dir();
        let open = dir.join("open");
        std::fs::create_dir_all(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o777)).unwrap();
        let with = |change: &dyn Fn(&mut JailerConfig)| {
            let mut config = jailed(&dir);
            if let Confinement::Jailer(jail) = &mut config.confinement {
                change(jail);
            }
            new_launcher(config)
        };
        let refused = |result: Result<FirecrackerLauncher, SandboxError>, what: &str| {
            let err = result.expect_err(what);
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains(what)),
                "{what}: {err:?}"
            );
        };
        // The root jailer builds each jail under the chroot base.
        refused(with(&|jail| jail.chroot_base = open.join("jail")), "open");
        // An escaped VMM must stay in the empty network namespace.
        let netns = open.join("vm1");
        std::fs::write(&netns, b"").unwrap();
        std::fs::set_permissions(&netns, std::fs::Permissions::from_mode(0o444)).unwrap();
        refused(with(&|jail| jail.netns = Some(netns.clone())), "open");
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        with(&|jail| jail.netns = Some(netns.clone())).unwrap();
        let link = dir.join("netns-link");
        std::os::unix::fs::symlink(&netns, &link).unwrap();
        refused(
            with(&|jail| jail.netns = Some(link.clone())),
            "symbolic link",
        );
        refused(
            with(&|jail| jail.netns = Some(dir.join("missing"))),
            "missing",
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    #[ignore = "needs root; the Sandbox (KVM) workflow runs it with sudo"]
    fn root_a_copied_image_keeps_the_owner_group_and_mode() {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: geteuid has no preconditions.
        assert_eq!(unsafe { libc::geteuid() }, 0, "run this test as root");
        let dir = temp_dir();
        // A golden snapshot: mode 0640, and the jailer group may read it.
        let from = dir.join("snapshot.mem");
        std::fs::write(&from, b"memory").unwrap();
        std::os::unix::fs::chown(&from, Some(0), Some(12_345)).unwrap();
        std::fs::set_permissions(&from, std::fs::Permissions::from_mode(0o640)).unwrap();
        let to = dir.join("copy.mem");
        copy_keeping_owner(&from, &to).unwrap();
        let meta = std::fs::metadata(&to).unwrap();
        assert_eq!(
            (meta.uid(), meta.gid(), meta.mode() & 0o7777),
            (0, 12_345, 0o640),
            "a copy must give the jail the access that a hard link gives"
        );
        assert_eq!(std::fs::read(&to).unwrap(), b"memory");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_wildcard_vsock_port_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config.guest_port = u32::MAX;
        let err = new_launcher(config).expect_err("VMADDR_PORT_ANY");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("port")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_zero_api_timeout_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::Api);
        config.api_timeout = Duration::ZERO;
        let err = new_launcher(config.clone()).expect_err("zero API timeout");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("API timeout")),
            "{err:?}"
        );
        // A config-file boot never calls the API.
        config.boot = BootMethod::ConfigFile;
        new_launcher(config).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_extra_cgroup_that_replaces_a_launcher_limit_is_a_config_error() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        if let Confinement::Jailer(jailer) = &mut config.confinement {
            jailer.extra_cgroups.push(("pids.max".into(), "max".into()));
        }
        let err = new_launcher(config).expect_err("a replaced limit");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("pids.max")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_jailed_launcher_needs_root() {
        let dir = temp_dir();
        let err = FirecrackerLauncher::new_with_euid(jailed(&dir), 1000)
            .expect_err("the jailer needs root");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("root")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_shared_image_that_the_jail_cannot_read_is_a_config_error() {
        let dir = temp_dir();
        let config = jailed(&dir);
        std::fs::set_permissions(&config.image.rootfs, std::fs::Permissions::from_mode(0o600))
            .unwrap();
        let err = new_launcher(config.clone()).expect_err("unreadable image");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("read")),
            "{err:?}"
        );
        // SAFETY: geteuid has no preconditions.
        if unsafe { libc::geteuid() } == 0 {
            // The jail group may read a 0640 file of its group.
            std::os::unix::fs::chown(&config.image.rootfs, None, Some(12_345)).unwrap();
            std::fs::set_permissions(&config.image.rootfs, std::fs::Permissions::from_mode(0o640))
                .unwrap();
            new_launcher(config).unwrap();
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_shared_image_that_others_can_write_is_a_config_error() {
        let dir = temp_dir();
        let config = jailed(&dir);
        std::fs::set_permissions(&config.image.rootfs, std::fs::Permissions::from_mode(0o666))
            .unwrap();
        let err = new_launcher(config).expect_err("writable image");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("rootfs.ext4")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_sweep_kills_the_cgroups_of_dead_owners() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let Confinement::Jailer(jail) = &config.confinement else {
            unreachable!("a jailed config");
        };
        let parent = jail.cgroup_root.join(&jail.cgroup_parent);
        let dead = parent.join(format!("hsbx-deadbeef-{}", "d".repeat(16)));
        std::fs::create_dir_all(&dead).unwrap();
        std::fs::write(dead.join("cgroup.kill"), b"").unwrap();
        std::fs::write(dead.join("cgroup.events"), b"populated 0\n").unwrap();
        let launcher = new_launcher(config).unwrap();
        let mine = parent.join(format!("hsbx-{}-{}", launcher.owner(), "e".repeat(16)));
        std::fs::create_dir_all(&mine).unwrap();
        std::fs::write(mine.join("cgroup.kill"), b"").unwrap();
        launcher.sweep_stale();
        assert_eq!(std::fs::read(dead.join("cgroup.kill")).unwrap(), b"1");
        assert_eq!(
            std::fs::read(mine.join("cgroup.kill")).unwrap(),
            b"",
            "a live owner keeps its cgroups"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_golden_snapshot_needs_the_jailer() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::Api)).unwrap();
        let result = launcher
            .create_golden_snapshot(&dir.join("golden"), Duration::from_secs(5))
            .await;
        assert!(
            matches!(result, Err(SandboxError::Config(ref m)) if m.contains("jailer")),
            "{result:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    #[ignore = "needs root; the Sandbox (KVM) workflow runs it with sudo"]
    async fn root_golden_snapshot_boots_pings_pauses_and_copies_the_files_out() {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: geteuid has no preconditions.
        assert_eq!(unsafe { libc::geteuid() }, 0, "run this test as root");
        let dir = temp_dir();
        let server = dir.join("fake_api.py");
        std::fs::write(&server, FAKE_GOLDEN_API_PY).unwrap();
        let jailer = script(
            &dir,
            "jailer",
            &format!(
                "cd root && mkdir -p run && exec python3 {} --api-sock run/firecracker.socket",
                server.display()
            ),
        );
        let fc = script(&dir, "firecracker", "exit 0");
        let mut jail = JailerConfig::new(jailer, 12_345, 23_456);
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
        let launcher = new_launcher(config).unwrap();
        let out = dir.join("golden");
        let files = launcher
            .create_golden_snapshot(&out, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(files.state, out.join("snapshot.state"));
        assert_eq!(files.memory, out.join("snapshot.mem"));
        assert_eq!(std::fs::read(&files.state).unwrap(), b"state");
        assert_eq!(std::fs::read(&files.memory).unwrap(), b"memory");
        for file in [&files.state, &files.memory] {
            let meta = std::fs::metadata(file).unwrap();
            assert_eq!(meta.mode() & 0o777, 0o640, "{}", file.display());
            assert_eq!(meta.gid(), 23_456, "the jailed VMM reads it by group");
        }
        let jails = std::fs::read_dir(launcher.vm_root())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
            .count();
        assert_eq!(jails, 0, "the golden VM is torn down");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// A fake jailed Firecracker for a golden snapshot. It answers the API
    /// with 204, starts a fake guest agent on `PUT /actions`, refuses a
    /// snapshot of a running VM, and writes the snapshot files.
    const FAKE_GOLDEN_API_PY: &str = r#"
import json, os, socket, struct, sys, threading
args = sys.argv[1:]
sock_path = args[args.index("--api-sock") + 1]
state = {"paused": False}

def guest():
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind("v.sock")
    srv.listen(8)
    while True:
        conn, _ = srv.accept()
        line = b""
        while not line.endswith(b"\n"):
            chunk = conn.recv(1)
            if not chunk:
                break
            line += chunk
        if line != b"CONNECT 52\n":
            conn.close()
            continue
        conn.sendall(b"OK 1\n")
        head = conn.recv(4, socket.MSG_WAITALL)
        conn.recv(struct.unpack(">I", head)[0], socket.MSG_WAITALL)
        body = json.dumps({"version": 1, "message": {"type": "pong"}}).encode()
        conn.sendall(struct.pack(">I", len(body)) + body)
        conn.close()

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
    reply = b"HTTP/1.1 204 No Content\r\n\r\n"
    if path == b"/actions":
        threading.Thread(target=guest, daemon=True).start()
    elif path == b"/vm":
        state["paused"] = json.loads(body)["state"] == "Paused"
    elif path == b"/snapshot/create":
        if not state["paused"]:
            fault = b'{"fault_message":"the VM is not paused"}'
            reply = b"HTTP/1.1 400 Bad Request\r\nContent-Length: %d\r\n\r\n%s" % (len(fault), fault)
        else:
            request = json.loads(body)
            open(request["snapshot_path"], "w").write("state")
            open(request["mem_file_path"], "w").write("memory")
    conn.sendall(reply)
    conn.close()
"#;
}
