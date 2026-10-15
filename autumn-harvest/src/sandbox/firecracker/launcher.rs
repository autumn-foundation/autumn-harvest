//! Start and stop Firecracker microVMs.
//!
//! [`VmLauncher`] starts one microVM. [`LaunchedVm`] is a running microVM
//! that serves one run and then goes away. [`FirecrackerLauncher`] is the
//! real launcher: it runs Firecracker behind the jailer, or without a jail
//! for development.
//!
//! Teardown kills the process group, kills the cgroup with `cgroup.kill`, and
//! removes the VM directory, in that order. Without `cgroup.kill`, teardown
//! freezes the cgroup and kills each process in it. The host never reads a
//! pid from the jail, because the jailed VMM can write there.
//!
//! Each launcher holds an owner lease: a locked file next to its VM
//! directories. [`FirecrackerLauncher::sweep_stale`] removes the VMs of each
//! owner whose lease is not locked, so a crashed worker leaves no VM behind
//! after the next start.
//!
//! A jailed launcher pins its image at its start. It hard-links the kernel,
//! the root file system and the snapshot pair into a directory of its own,
//! and each launch links from there. So a new image that lands one file at
//! a time never gives a launch a mixed pair. It takes effect at the next
//! launcher start. The pin holds a shared lock on each image directory, and
//! a publish holds an exclusive one. So a pin never sees half a publish.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use super::api::ApiClient;
use super::config::{
    CONFIG_FILE, GUEST_MEMORY_RESERVE_MIB, GuestImage, KERNEL_FILE, MAX_SOCKET_PATH_BYTES,
    MachineSpec, OWNER_TAG_HEX, PINNED_IMAGE_SUFFIX, ROOTFS_FILE, VM_ID_PREFIX, VM_ID_RANDOM_HEX,
    VMM_COPIES_SUFFIX, VSOCK_SOCKET, VmPaths, new_vm_id, validate_vm_id, vm_config,
};
use super::jailer::JailerConfig;
use super::posture::{PostureFinding, PosturePolicy};
use crate::sandbox::error::SandboxError;
use crate::sandbox::runtime::BoxFuture;

/// The time that teardown waits for the VMM process to exit.
pub const KILL_WAIT: Duration = Duration::from_secs(5);

/// The time that teardown waits for a killed cgroup to empty.
pub const CGROUP_EMPTY_WAIT: Duration = Duration::from_secs(2);

/// The time that a launch can take to lay out its VM directory and start
/// the VMM. A launch hard-links each image into its jail and copies none,
/// so the image size does not count.
pub const LAUNCH_SETUP_ALLOWANCE: Duration = Duration::from_secs(10);

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

    /// The longest time that [`Self::launch`] can take. The backend adds it
    /// to its overhead. Zero by default.
    fn launch_budget(&self) -> Duration {
        Duration::ZERO
    }

    /// The most memory that one run can use, in bytes. `None` by default.
    fn memory_capacity(&self) -> Option<u64> {
        None
    }

    /// The bytes of the guest work directory. `None` by default.
    fn work_capacity(&self) -> Option<u64> {
        None
    }
}

/// The two snapshot files of a golden microVM.
///
/// A manifest next to the state file holds the boot arguments, the machine
/// and the image digests of the snapshot. Its name is the state file name and `.json`. A
/// launcher with other values refuses the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFiles {
    /// The VM state file.
    pub state: PathBuf,
    /// The guest memory file.
    pub memory: PathBuf,
}

/// What a golden snapshot keeps of its launcher. A restore cannot change
/// it: the guest agent keeps the port of its boot arguments.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SnapshotMeta {
    boot_args: String,
    vcpu_count: u8,
    mem_size_mib: u32,
    /// The SHA-256 digests of the image. The snapshot memory holds the
    /// booted kernel, and the guest reads the root file system.
    kernel_sha256: String,
    rootfs_sha256: String,
    /// The Firecracker version, the host kernel and the CPU model. A
    /// snapshot does not load on another one of them.
    firecracker: String,
    host_kernel: String,
    cpu: String,
}

impl SnapshotMeta {
    /// The values of `config`, with the image files `kernel` and `rootfs`.
    ///
    /// # Errors
    ///
    /// Returns the error of a file that cannot be read.
    fn of(
        config: &LauncherConfig,
        (kernel, rootfs): (&Path, &Path),
        firecracker: super::posture::Version,
    ) -> std::io::Result<Self> {
        let (major, minor, patch) = firecracker;
        Ok(Self {
            boot_args: super::config::boot_args(&config.image, config.guest_port),
            vcpu_count: config.machine.vcpu_count,
            mem_size_mib: config.machine.mem_size_mib,
            kernel_sha256: file_sha256(kernel)?,
            rootfs_sha256: file_sha256(rootfs)?,
            firecracker: format!("{major}.{minor}.{patch}"),
            host_kernel: host_kernel()?,
            cpu: cpu_model()?,
        })
    }

    /// The manifest file next to the snapshot state file `state`.
    fn path(state: &Path) -> PathBuf {
        let mut name = state.file_name().unwrap_or_default().to_os_string();
        name.push(".json");
        state.with_file_name(name)
    }

    /// Check that the snapshot of `files` keeps the values of `config` and
    /// this host, and return its manifest. The Firecracker version is
    /// checked apart, at the first version probe.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when the manifest is missing, is not
    /// valid, or holds other values. Each run would otherwise fail.
    fn check(config: &LauncherConfig, files: &SnapshotFiles) -> Result<Self, SandboxError> {
        let path = Self::path(&files.state);
        let found: Self = read_manifest(&path)
            .map_err(|e| e.to_string())
            .and_then(|bytes| serde_json::from_slice(&bytes).map_err(|e| e.to_string()))
            .map_err(|e| {
                SandboxError::Config(format!(
                    "cannot read the snapshot manifest {}: {e}; create the snapshot with \
                     create_golden_snapshot",
                    path.display()
                ))
            })?;
        let mut want = Self::of(
            config,
            (&config.image.kernel, &config.image.rootfs),
            (0, 0, 0),
        )
        .map_err(|e| {
            SandboxError::Config(format!("cannot read the guest image for its digest: {e}"))
        })?;
        want.firecracker.clone_from(&found.firecracker);
        if found != want {
            return Err(SandboxError::Config(format!(
                "the snapshot keeps {found:?}, but this launcher has {want:?}; a restored \
                 guest keeps the boot arguments, such as harvest.port, the machine and the \
                 image of its snapshot, and loads only on its host kernel and CPU model"
            )));
        }
        Ok(found)
    }

    /// Check that the snapshot was made with the Firecracker `version`.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] for another version. Each load would
    /// fail, and a retry cannot help.
    fn check_firecracker(&self, version: super::posture::Version) -> Result<(), SandboxError> {
        let (major, minor, patch) = version;
        let running = format!("{major}.{minor}.{patch}");
        if self.firecracker == running {
            return Ok(());
        }
        Err(SandboxError::Config(format!(
            "the snapshot was made with Firecracker {}, but this host runs Firecracker \
             {running}; create the snapshot again",
            self.firecracker
        )))
    }
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
    ///
    /// Teardown kills the process group of the VMM. A helper that leaves the
    /// group, for example with `setsid`, outlives it. Only the jailer cgroup
    /// holds every process.
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
    /// The time limit of the whole API boot: the wait for the socket and
    /// each call.
    pub api_timeout: Duration,
}

/// The real launcher.
#[derive(Debug)]
pub struct FirecrackerLauncher {
    config: LauncherConfig,
    exec_name: String,
    owner: String,
    /// The copies of the VMM binaries that the last version check passed.
    /// A binary replaced since then is checked again.
    versions_checked: tokio::sync::Mutex<Option<Arc<CheckedVmm>>>,
    /// The lease and the leftover cgroups, shared with each microVM.
    tenancy: Arc<Tenancy>,
}

/// What a launcher shares with each of its microVMs: its lease, and the
/// cgroups and VM directories that a teardown could not remove.
///
/// A process of the microVM can still be alive in such a cgroup.
/// [`FirecrackerLauncher::sweep_stale`] skips the cgroups of a live owner.
/// So the launcher kills and removes them again before each launch and in
/// each sweep.
///
/// A detached teardown can outlive the launcher. So the lease goes only
/// when the launcher and each of its microVMs are gone. The file stays
/// while a cgroup is left. A later sweep then sees a dead owner and removes
/// the cgroup.
#[derive(Debug)]
struct Tenancy {
    lease: InstanceLease,
    leftovers: Mutex<Vec<PathBuf>>,
    /// The VM, jail and copy directories that a removal could not remove.
    /// The sweep skips the directories of a live owner, so only the launcher
    /// tries them again.
    dirs: Mutex<Vec<PathBuf>>,
    /// The image that a jailed launcher pinned at its start.
    pinned: Option<PinnedImage>,
}

/// The image files of a launcher, hard-linked at its start into a directory
/// of its own.
#[derive(Debug)]
struct PinnedImage {
    dir: PathBuf,
    kernel: PathBuf,
    rootfs: PathBuf,
    snapshot: Option<SnapshotFiles>,
    /// The manifest of the snapshot, checked at the pin.
    meta: Option<SnapshotMeta>,
    /// Each pinned file, with its stamp from before the pin checks it.
    stamps: Vec<(PathBuf, FileStamp)>,
    /// The watch for a write to a pinned file.
    watch: WriteWatch,
}

/// What a pin records of an image file.
///
/// A hard link shares its inode, so a write in place reaches the pin. A
/// plain write changes the modification time. A write that keeps the size
/// and the time passes this check, as `cp -p` can, so [`WriteWatch`] also
/// watches each file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileStamp {
    dev: u64,
    ino: u64,
    size: u64,
    mtime: (i64, i64),
}

impl FileStamp {
    /// The stamp of the file at `path`.
    fn of(path: &Path) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt as _;
        let meta = std::fs::metadata(path)?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.size(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
        })
    }
}

/// An inotify watch for a write to the pinned image files.
///
/// The watch is on each inode, so it sees a write or a truncate through any
/// hard link. A link, an unlink and a read are not writes, so launches do not
/// trip it. A write through a shared memory map makes no event. It moves the
/// modification time that [`FileStamp`] checks only when it dirties a clean
/// page.
#[derive(Debug)]
struct WriteWatch {
    fd: std::os::fd::OwnedFd,
    /// The watch descriptor of each watched file, in order.
    watches: Vec<libc::c_int>,
    /// Set at the first event, and never cleared.
    written: Mutex<bool>,
}

impl WriteWatch {
    /// Watch each file of `paths` for a write.
    ///
    /// # Errors
    ///
    /// Returns the error of `inotify_init1` or `inotify_add_watch`, such as
    /// a full `fs.inotify` limit.
    fn new(paths: &[&Path]) -> std::io::Result<Self> {
        use std::os::fd::FromRawFd as _;
        // SAFETY: inotify_init1 reads no memory.
        let fd = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut watch = Self {
            // SAFETY: the descriptor is new, and nothing else owns it.
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            watches: Vec::with_capacity(paths.len()),
            written: Mutex::new(false),
        };
        for path in paths {
            let descriptor = watch.add(path)?;
            watch.watches.push(descriptor);
        }
        Ok(watch)
    }

    /// Watch `path` for a write, and return the watch descriptor.
    ///
    /// A hard link does not follow a symbolic link, so neither does the
    /// watch.
    fn add(&self, path: &Path) -> std::io::Result<libc::c_int> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::OsStrExt as _;
        let path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        // SAFETY: the path is a NUL-terminated string that outlives the call,
        // and the descriptor is open.
        let descriptor = unsafe {
            libc::inotify_add_watch(
                self.fd.as_raw_fd(),
                path.as_ptr(),
                libc::IN_MODIFY | libc::IN_DONT_FOLLOW,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(descriptor)
    }

    /// Whether each of `paths` is the file that the watch of the same index
    /// watches.
    ///
    /// One inotify instance gives one descriptor to each inode. A second
    /// watch on a watched inode returns the first descriptor, through any of
    /// its hard links.
    fn watches(&self, paths: &[&Path]) -> std::io::Result<bool> {
        if paths.len() != self.watches.len() {
            return Ok(false);
        }
        for (path, watched) in paths.iter().zip(&self.watches) {
            if self.add(path)? != *watched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether a watched file had a write since the watch started.
    ///
    /// Each event counts: a write, a lost watch or an overflow. So does a
    /// failed read, so the check fails closed.
    fn written(&self) -> bool {
        // The read takes the events from the queue. So the read and the flag
        // share one lock. Otherwise a second check could find the queue empty
        // before the first check sets the flag.
        let mut written = self.written.lock().unwrap_or_else(PoisonError::into_inner);
        *written = *written || self.take_event();
        *written
    }

    /// Whether a read takes at least one event from the queue. A failed read
    /// counts as one, so the check fails closed.
    fn take_event(&self) -> bool {
        use std::os::fd::AsRawFd as _;
        let mut events = [0u8; 4096];
        loop {
            // SAFETY: the buffer is valid for its length, and the descriptor
            // is open.
            let n = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    events.as_mut_ptr().cast(),
                    events.len(),
                )
            };
            if n > 0 {
                return true;
            }
            if n == 0 {
                return false;
            }
            match std::io::Error::last_os_error().kind() {
                std::io::ErrorKind::Interrupted => {}
                std::io::ErrorKind::WouldBlock => return false,
                _ => return true,
            }
        }
    }
}

/// Check that each file of `stamps` keeps its stamp.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] when a file changed or cannot be read.
/// A launch would boot bytes that the pin did not check, and a retry cannot
/// help until the worker restarts.
fn check_stamps(stamps: &[(PathBuf, FileStamp)]) -> Result<(), SandboxError> {
    for (path, stamp) in stamps {
        let found = FileStamp::of(path);
        if found.as_ref().is_ok_and(|found| found == stamp) {
            continue;
        }
        let found = found.map_or_else(|e| e.to_string(), |found| format!("{found:?}"));
        return Err(SandboxError::Config(format!(
            "the pinned image file {} changed after the pin: it had {stamp:?}, and it has \
             {found}; a hard link shares its inode, so a write in place reaches the pin; \
             publish an image by rename and restart the worker",
            path.display()
        )));
    }
    Ok(())
}

impl PinnedImage {
    /// Check that no pinned file changed since the pin.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when a file changed. A launch would
    /// boot bytes that the pin did not check, and a retry cannot help until
    /// the worker restarts.
    fn check(&self) -> Result<(), SandboxError> {
        check_stamps(&self.stamps)?;
        if self.watch.written() {
            return Err(SandboxError::Config(format!(
                "a pinned image file in {} had a write in place after the pin; a hard link \
                 shares its inode, so the write reaches the pin; publish an image by rename \
                 and restart the worker",
                self.dir.display()
            )));
        }
        Ok(())
    }

    /// Hard-link the image files of `config` into the directory of `owner`
    /// under `vm_root`. The construction checked that they are on the file
    /// system of the jail, so nothing is copied.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when a link fails. Nothing is left.
    fn pin(config: &LauncherConfig, vm_root: &Path, owner: &str) -> Result<Self, SandboxError> {
        Self::pin_with(config, vm_root, owner, || {})
    }

    /// [`Self::pin`], with `after_stamps` run once the stamps are taken, so
    /// a test can write in the window.
    fn pin_with(
        config: &LauncherConfig,
        vm_root: &Path,
        owner: &str,
        after_stamps: impl FnOnce(),
    ) -> Result<Self, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        let dir = vm_root.join(format!("{VM_ID_PREFIX}{owner}{PINNED_IMAGE_SUFFIX}"));
        let fail = |what: &Path, e: &std::io::Error| {
            SandboxError::Config(format!(
                "cannot pin {} in {}: {e}",
                what.display(),
                dir.display()
            ))
        };
        // A publish renames files under an exclusive lock of their directory.
        // The shared locks keep all links of the pin inside one publish.
        let mut files = vec![config.image.kernel.as_path(), config.image.rootfs.as_path()];
        if let BootMethod::Snapshot(snapshot) = &config.boot {
            files.extend([snapshot.state.as_path(), snapshot.memory.as_path()]);
        }
        let _locks = lock_dirs(&files, libc::LOCK_SH, IMAGE_LOCK_WAIT).map_err(|e| {
            SandboxError::Unavailable(format!(
                "cannot lock the image directories for the pin: {e}; a publish can hold them"
            ))
        })?;
        // The lock keeps out a rename, but not a write in place. The watch
        // comes first, so it sees each write after the stamps.
        let watch = WriteWatch::new(&files).map_err(|e| {
            SandboxError::Config(format!(
                "cannot watch the image files for writes: {e}; check the fs.inotify limits"
            ))
        })?;
        // The stamps come before the checks, so the pin refuses a write after
        // them.
        let stamps = files
            .iter()
            .map(|file| FileStamp::of(file).map_err(|e| fail(file, &e)))
            .collect::<Result<Vec<_>, _>>()?;
        after_stamps();
        // The manifest lands with the pair, so the lock covers it too.
        let meta = match &config.boot {
            BootMethod::Snapshot(snapshot) => Some(SnapshotMeta::check(config, snapshot)?),
            BootMethod::ConfigFile | BootMethod::Api => None,
        };
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .map_err(|e| fail(vm_root, &e))?;
        let link = |from: &Path, name: &str| {
            let to = dir.join(name);
            std::fs::hard_link(from, &to).map_err(|e| fail(from, &e))?;
            Ok::<_, SandboxError>(to)
        };
        let pinned = (|| {
            let snapshot = match &config.boot {
                BootMethod::Snapshot(files) => Some(SnapshotFiles {
                    state: link(&files.state, SNAPSHOT_STATE_FILE)?,
                    memory: link(&files.memory, SNAPSHOT_MEMORY_FILE)?,
                }),
                BootMethod::ConfigFile | BootMethod::Api => None,
            };
            let kernel = link(&config.image.kernel, KERNEL_FILE)?;
            let rootfs = link(&config.image.rootfs, ROOTFS_FILE)?;
            // The links come in the order of `files`.
            let links: Vec<PathBuf> = [&kernel, &rootfs]
                .into_iter()
                .chain(snapshot.iter().flat_map(|s| [&s.state, &s.memory]))
                .cloned()
                .collect();
            // Each link must be the file that the watch watches. A rename
            // outside the lock could swap a file after the watch.
            let linked: Vec<&Path> = links.iter().map(PathBuf::as_path).collect();
            if !watch.watches(&linked).map_err(|e| fail(&dir, &e))? {
                return Err(SandboxError::Config(format!(
                    "an image file was replaced during the pin of {}; publish an image under \
                     the lock of its directory",
                    dir.display()
                )));
            }
            let pinned = Self {
                dir: dir.clone(),
                kernel,
                rootfs,
                snapshot,
                meta,
                stamps: links.into_iter().zip(stamps).collect(),
                watch,
            };
            pinned.check()?;
            Ok(pinned)
        })();
        if pinned.is_err() {
            let _ = crate::sandbox::exec::remove_work_dir(&dir);
        }
        pinned
    }
}

impl Tenancy {
    const fn new(lease: InstanceLease, pinned: Option<PinnedImage>) -> Self {
        Self {
            lease,
            leftovers: Mutex::new(Vec::new()),
            dirs: Mutex::new(Vec::new()),
            pinned,
        }
    }

    /// Keep `cgroup` when it is still there.
    fn keep_if_left(&self, cgroup: &Path) {
        if crate::sandbox::cgroup::is_gone(cgroup) {
            return;
        }
        tracing::warn!(
            cgroup = %cgroup.display(),
            "a sandbox VM cgroup outlived its teardown; the launcher tries it again"
        );
        self.leftovers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(cgroup.to_path_buf());
    }

    /// Keep the directory `dir` of this launcher when it is still there.
    fn keep_dir_if_left(&self, dir: &Path) {
        if matches!(dir.try_exists(), Ok(false)) {
            return;
        }
        tracing::warn!(
            dir = %dir.display(),
            "could not remove a sandbox directory; the launcher tries it again"
        );
        self.dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(dir.to_path_buf());
    }

    /// Kill and remove each kept cgroup again, and remove each kept VM
    /// directory again. Keep each one that is still there.
    fn retry(&self) {
        self.dirs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|dir| {
                let _ = crate::sandbox::exec::remove_work_dir(dir);
                !matches!(dir.try_exists(), Ok(false))
            });
        self.leftovers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|cgroup| {
                crate::sandbox::cgroup::kill_and_remove(cgroup, Duration::ZERO);
                !crate::sandbox::cgroup::is_gone(cgroup)
            });
    }

    /// Whether no cgroup is kept.
    fn is_empty(&self) -> bool {
        self.leftovers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
    }
}

impl Drop for Tenancy {
    fn drop(&mut self) {
        // A sweep kills a cgroup only for an owner with a lease under its VM
        // root. So the lease file stays while a cgroup of this owner is left.
        self.retry();
        if !self.is_empty() {
            self.lease.keep_file();
        }
        // No launch can start now, and each microVM has its own links.
        if let Some(pinned) = &self.pinned {
            let _ = crate::sandbox::exec::remove_work_dir(&pinned.dir);
        }
    }
}

/// A locked file that marks a live owner of VM directories.
#[derive(Debug)]
pub struct InstanceLease {
    path: PathBuf,
    file: std::fs::File,
    /// Leave the file after the drop.
    keep_file: bool,
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
        Ok(Self {
            path,
            file,
            keep_file: false,
        })
    }

    /// The lease file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Leave the file after the drop. A later sweep then sees a dead owner
    /// and removes what the owner left.
    const fn keep_file(&mut self) {
        self.keep_file = true;
    }
}

impl Drop for InstanceLease {
    fn drop(&mut self) {
        // Remove the file while the lock is held. Closing the file then
        // releases the lock.
        if !self.keep_file {
            let _ = std::fs::remove_file(&self.path);
        }
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
    /// - an extra boot argument would replace a launcher argument, or the boot
    ///   line is longer than `config::MAX_BOOT_ARGS_BYTES`;
    /// - an extra cgroup key is not a plain controller file name of at most
    ///   `NAME_MAX` bytes;
    /// - a path is missing;
    /// - a snapshot boot has no jailer;
    /// - the jail uid or gid is 0, or this process is not root with a jail;
    /// - the file name of a jailed Firecracker binary is a name in the jail
    ///   root;
    /// - the jail cannot read an image;
    /// - `JailerConfig::cgroup_root` is not the cgroup v2 mount of the jailer;
    /// - another user can change a binary, an image, the jail root or the
    ///   network namespace of a jailed launch;
    /// - the lease cannot be taken.
    pub fn new(config: LauncherConfig) -> Result<Self, SandboxError> {
        let worker = Worker {
            // SAFETY: geteuid has no preconditions.
            euid: unsafe { libc::geteuid() },
            hard: super::jailer::HardCeilings::of_worker(),
        };
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
        Self::new_on_host(config, worker, &mounts, check_netns)
    }

    /// [`Self::new`] for `worker` on a host with the mount table `mounts`.
    /// `netns_check` checks the network namespace of a jailer.
    fn new_on_host(
        mut config: LauncherConfig,
        worker: Worker,
        mounts: &str,
        netns_check: fn(&Path) -> Result<(), SandboxError>,
    ) -> Result<Self, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        // Firecracker refuses these at each launch. Refuse them once, here.
        let machine = config.machine;
        // The guest kernel and agent keep some memory, so a run needs more.
        if !(1..=MAX_VCPUS).contains(&machine.vcpu_count)
            || machine.mem_size_mib <= GUEST_MEMORY_RESERVE_MIB
            || machine.mem_size_mib > super::config::MAX_GUEST_MEMORY_MIB
        {
            return Err(SandboxError::Config(format!(
                "a microVM needs 1 to {MAX_VCPUS} vCPUs and more than \
                 {GUEST_MEMORY_RESERVE_MIB} MiB up to {} MiB, not {} vCPUs and {} MiB",
                super::config::MAX_GUEST_MEMORY_MIB,
                machine.vcpu_count,
                machine.mem_size_mib
            )));
        }
        // A longer API timeout means no bound, and the launch budget must
        // still fit the engine clock.
        config.api_timeout = crate::sandbox::bounded(config.api_timeout);
        super::config::check_boot_args(&config.image, config.guest_port)?;
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
        check_launcher_paths(&config)?;
        if let Confinement::Jailer(jailer) = &config.confinement {
            check_jailed(jailer, machine, (worker, mounts), netns_check)?;
        }
        // Each version check copies the binaries. A missing binary is checked
        // there too.
        let mut binaries = vec![config.firecracker_bin.as_path()];
        if let Confinement::Jailer(jailer) = &config.confinement {
            binaries.push(&jailer.jailer_bin);
        }
        for path in binaries {
            if let Ok(meta) = std::fs::metadata(path) {
                check_vmm_size(path, meta.len())?;
            }
        }
        let exec_name = super::jailer::exec_name(&config.firecracker_bin)?;
        if matches!(config.confinement, Confinement::Jailer(_))
            && JAIL_ROOT_NAMES.contains(&exec_name.as_str())
        {
            return Err(SandboxError::Config(format!(
                "the Firecracker binary name {exec_name:?} is a name in the jail root, where \
                 the jailer copies the binary, so each launch would fail"
            )));
        }
        let owner = uuid::Uuid::new_v4().simple().to_string()[..OWNER_TAG_HEX].to_owned();
        let vm_root = vm_root_of(&config.confinement, &exec_name);
        check_socket_paths(&vm_root, &config.confinement)?;
        // A jailed launcher refuses a VM root that group or others can write.
        // The umask only takes bits away, so a new root has mode 0755 at most.
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o755)
            .create(&vm_root)
            .map_err(|e| {
                SandboxError::Config(format!("cannot create {}: {e}", vm_root.display()))
            })?;
        let jailed = matches!(config.confinement, Confinement::Jailer(_));
        check_mount_flags(&vm_root, mount_flags(&vm_root)?, jailed)?;
        if let Confinement::Jailer(jailer) = &config.confinement {
            check_trusted_files(&config, jailer, &vm_root)?;
        }
        let lease = InstanceLease::acquire(&vm_root, &owner)?;
        // The lease comes first, so a sweep sees a live owner of the pin.
        let pinned = match &config.confinement {
            Confinement::Jailer(_) => Some(PinnedImage::pin(&config, &vm_root, &owner)?),
            Confinement::Unjailed { .. } => None,
        };
        Ok(Self {
            config,
            exec_name,
            owner,
            versions_checked: tokio::sync::Mutex::new(None),
            tenancy: Arc::new(Tenancy::new(lease, pinned)),
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
        self.tenancy.lease.path()
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
        let cgroup_mount = match &self.config.confinement {
            Confinement::Jailer(jailer) => jailer.cgroup_root.clone(),
            Confinement::Unjailed { .. } => PathBuf::from(super::jailer::DEFAULT_CGROUP_ROOT),
        };
        let mut findings = super::posture::inspect_host(root, &cgroup_mount);
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
    ///
    /// Launchers with other VM roots can share the cgroup parent. So the
    /// sweep kills a cgroup only for a dead owner that this VM root shows: a
    /// dead lease or a VM directory here. A dead lease stays until the VMs
    /// and the cgroups of its owner are gone.
    ///
    /// The sweep also tries again each cgroup that outlived a teardown of
    /// this launcher.
    pub fn sweep_stale(&self) -> usize {
        self.tenancy.retry();
        let root = self.vm_root();
        let Ok(entries) = std::fs::read_dir(&root) else {
            return 0;
        };
        let mut vm_dirs = Vec::new();
        let mut leases = Vec::new();
        // A scan that fails can miss a VM directory, so each dead lease stays.
        let mut complete = true;
        for entry in entries {
            let Ok(entry) = entry else {
                complete = false;
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(rest) = name.strip_prefix(VM_ID_PREFIX) else {
                continue;
            };
            if let Some(owner) = rest.strip_suffix(LEASE_SUFFIX) {
                leases.push((owner.to_owned(), entry.path()));
            } else if let Some((owner, _)) = rest.split_once('-') {
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => {
                        vm_dirs.push((owner.to_owned(), name.clone(), entry.path()));
                    }
                    Ok(_) => {}
                    Err(_) => complete = false,
                }
            }
        }
        // An owner is live when another process holds its lease lock. An
        // owner whose lease cannot be opened also stays live, so a failed
        // check never kills the VMs of a running launcher.
        let mut live = std::collections::BTreeSet::new();
        live.insert(self.owner.clone());
        let mut dead_leases = Vec::new();
        for (owner, path) in leases {
            if owner == self.owner {
                continue;
            }
            match std::fs::OpenOptions::new().read(true).open(&path) {
                Ok(file) if try_lock(&file) => dead_leases.push((owner, path, file)),
                Ok(_) => {
                    live.insert(owner);
                }
                Err(e) => {
                    tracing::warn!(
                        lease = %path.display(),
                        error = %e,
                        "cannot check a sandbox lease"
                    );
                    live.insert(owner);
                }
            }
        }
        let mut dead: std::collections::BTreeSet<String> = dead_leases
            .iter()
            .map(|(owner, ..)| owner.clone())
            .collect();
        // The owners that still have a VM directory or a cgroup.
        let mut left = std::collections::BTreeSet::new();
        if !complete {
            left.extend(dead.iter().cloned());
        }
        let mut removed = 0;
        for (owner, vm_id, path) in vm_dirs {
            if live.contains(&owner) {
                continue;
            }
            if let Confinement::Jailer(jailer) = &self.config.confinement {
                let cgroup = super::jailer::cgroup_dir(jailer, &vm_id);
                kill_cgroup_now(&cgroup);
                if !crate::sandbox::cgroup::is_gone(&cgroup) {
                    left.insert(owner.clone());
                }
            }
            if crate::sandbox::exec::remove_work_dir(&path).is_ok() {
                removed += 1;
            } else {
                left.insert(owner.clone());
            }
            dead.insert(owner);
        }
        self.sweep_cgroups(&dead, &mut left);
        for (owner, path, _locked) in dead_leases {
            if !left.contains(&owner) {
                let _ = std::fs::remove_file(path);
            }
        }
        removed
    }

    /// Kill and remove each cgroup of a `dead` owner. A dropped microVM can
    /// leave its cgroup after its jail is gone. Add each owner with a cgroup
    /// that is still there to `left`.
    ///
    /// A scan that fails can miss a cgroup. Then each dead owner goes to
    /// `left`, so its lease stays for a later sweep. A parent that is surely
    /// absent holds no cgroup.
    fn sweep_cgroups(
        &self,
        dead: &std::collections::BTreeSet<String>,
        left: &mut std::collections::BTreeSet<String>,
    ) {
        let Confinement::Jailer(jailer) = &self.config.confinement else {
            return;
        };
        let parent = jailer.cgroup_root.join(&jailer.cgroup_parent);
        let entries = match std::fs::read_dir(&parent) {
            Ok(entries) => entries,
            Err(_) if crate::sandbox::cgroup::is_gone(&parent) => return,
            Err(_) => {
                left.extend(dead.iter().cloned());
                return;
            }
        };
        for entry in entries {
            let Ok(entry) = entry else {
                left.extend(dead.iter().cloned());
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some((owner, _)) = name
                .strip_prefix(VM_ID_PREFIX)
                .and_then(|rest| rest.split_once('-'))
            else {
                continue;
            };
            if dead.contains(owner) {
                kill_cgroup_now(&entry.path());
                if !crate::sandbox::cgroup::is_gone(&entry.path()) {
                    left.insert(owner.to_owned());
                }
            }
        }
    }

    /// Check the Firecracker and jailer versions, and copy the binaries that
    /// pass. A failed check runs again on the next call.
    ///
    /// A deploy can replace a binary while the worker runs. So a launch runs
    /// the copies, not the configured paths, and a check holds only for the
    /// files that it copied.
    async fn check_versions(&self) -> Result<Arc<CheckedVmm>, SandboxError> {
        use super::posture::{VmmBinary, probe_version};
        let mut sources = vec![("firecracker", self.config.firecracker_bin.clone())];
        if let Confinement::Jailer(jailer) = &self.config.confinement {
            sources.push(("jailer", jailer.jailer_bin.clone()));
        }
        let ids = sources
            .iter()
            .map(|(_, path)| file_id(path))
            .collect::<Result<Vec<_>, _>>()?;
        for ((_, path), id) in sources.iter().zip(&ids) {
            check_vmm_size(path, id.size)?;
        }
        let mut checked = self.versions_checked.lock().await;
        if let Some(vmm) = checked.as_ref()
            && vmm.sources == ids
        {
            return Ok(Arc::clone(vmm));
        }
        let dir = self
            .vm_root()
            .join(format!("{}{VMM_COPIES_SUFFIX}", new_vm_id(&self.owner)));
        // A dropped check removes the copies. A copy task that still runs
        // then removes them again when it ends.
        let guard = JailGuard::new(dir.clone(), Arc::clone(&self.tenancy));
        let abandoned = guard.abandoned();
        let task_dir = dir.clone();
        let task_ids = ids.clone();
        let task_tenancy = Arc::clone(&self.tenancy);
        let copies = tokio::task::spawn_blocking(move || {
            let copies = copy_binaries(&task_dir, &sources, &task_ids);
            if abandoned.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = crate::sandbox::exec::remove_work_dir(&task_dir);
                task_tenancy.keep_dir_if_left(&task_dir);
            }
            copies
        })
        .await
        .map_err(|e| SandboxError::Unavailable(format!("the binary copy failed: {e}")))??;
        guard.disarm();
        // The copies go with this value, so a failed check leaves nothing.
        let mut copies = copies.into_iter();
        let mut vmm = CheckedVmm {
            dir,
            firecracker: copies.next().unwrap_or_default(),
            jailer: copies.next(),
            version: (0, 0, 0),
            sources: ids,
            tenancy: Arc::clone(&self.tenancy),
        };
        vmm.version = probe_version(&vmm.firecracker, VmmBinary::Firecracker).await?;
        if let Some(jailer) = &vmm.jailer {
            probe_version(jailer, VmmBinary::Jailer).await?;
        }
        let meta = self.tenancy.pinned.as_ref().and_then(|p| p.meta.as_ref());
        if let Some(meta) = meta {
            meta.check_firecracker(vmm.version)?;
        }
        let vmm = Arc::new(vmm);
        *checked = Some(Arc::clone(&vmm));
        drop(checked);
        Ok(vmm)
    }

    /// Create the VM directory and the command for `id`. The command runs
    /// the binaries of `vmm`.
    async fn prepare(
        &self,
        id: &str,
        boot: &BootMethod,
        vmm: &Arc<CheckedVmm>,
    ) -> Result<Prepared, SandboxError> {
        match &self.config.confinement {
            Confinement::Jailer(jailer) => self.prepare_jailed(jailer, id, boot, vmm).await,
            Confinement::Unjailed { work_root } => self.prepare_unjailed(work_root, id, boot, vmm),
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
    /// Both files replace an old pair only after both copies are complete.
    /// A failed rename puts the old file back. A running launcher pinned its
    /// pair at its start, so the renames do not reach its launches. The
    /// renames hold an exclusive lock on `out_dir`, so a launcher that starts
    /// meanwhile waits for them. A host crash between the two renames can
    /// leave a mixed pair, so create the snapshot again after it.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] without the jailer, with an API
    /// timeout or a boot timeout of zero, or with a jailer `fsize` limit
    /// below the guest memory. A pinned image file that changed in place is
    /// a config error too. Returns
    /// [`SandboxError::Unavailable`] when a step fails. The golden microVM is
    /// torn down on each path.
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
        // The golden microVM boots through the API, whatever the boot method
        // of this launcher is.
        if self.config.api_timeout.is_zero() {
            return Err(SandboxError::Config(
                "a golden snapshot boots through the API, so it needs an API timeout above zero"
                    .into(),
            ));
        }
        // A zero boot timeout has expired before the first connect.
        if boot_timeout.is_zero() {
            return Err(SandboxError::Config(
                "a golden snapshot needs a boot timeout above zero".into(),
            ));
        }
        // The memory file holds the whole guest memory, and the `fsize`
        // rlimit of the VMM bounds it. A retry cannot fix a small limit.
        let memory = u64::from(self.config.machine.mem_size_mib) << 20;
        if let Some(limit) = jailer.fsize_limit
            && limit < memory
        {
            return Err(SandboxError::Config(format!(
                "a golden snapshot writes {} MiB of guest memory, above the jailer fsize limit \
                 of {limit} bytes",
                self.config.machine.mem_size_mib
            )));
        }
        let vmm = self.check_versions().await?;
        let id = new_vm_id(&self.owner);
        validate_vm_id(&id)?;
        let prepared = self
            .prepare_jailed(jailer, &id, &BootMethod::Api, &vmm)
            .await?;
        let mut vm = RunningVm::spawn(id, prepared, Arc::clone(&self.tenancy))?;
        let result = self
            .snapshot_vm(&mut vm, out_dir, (boot_timeout, jailer.gid), vmm.version)
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
        (boot_timeout, gid): (Duration, u32),
        firecracker: super::posture::Version,
    ) -> Result<SnapshotFiles, SandboxError> {
        let socket = vm.api_socket.clone().ok_or_else(|| {
            SandboxError::Unavailable("the golden microVM has no API socket".into())
        })?;
        self.boot_through_api(vm, &socket, &BootMethod::Api).await?;
        let deadline = crate::sandbox::deadline_after(boot_timeout);
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
        let mut copies = vec![
            (root.join(SNAPSHOT_STATE_FILE), files.state.clone()),
            (root.join(SNAPSHOT_MEMORY_FILE), files.memory.clone()),
        ];
        // The golden microVM booted the pinned image, so the digests come
        // from it.
        let (kernel, rootfs) = self.tenancy.pinned.as_ref().map_or_else(
            || {
                (
                    self.config.image.kernel.clone(),
                    self.config.image.rootfs.clone(),
                )
            },
            |pinned| (pinned.kernel.clone(), pinned.rootfs.clone()),
        );
        let config = self.config.clone();
        let meta_path = SnapshotMeta::path(&files.state);
        let out = out_dir.to_path_buf();
        let tenancy = Arc::clone(&self.tenancy);
        let copy_error =
            |e: std::io::Error| SandboxError::Unavailable(format!("cannot copy the snapshot: {e}"));
        tokio::task::spawn_blocking(move || {
            let meta =
                SnapshotMeta::of(&config, (&kernel, &rootfs), firecracker).map_err(copy_error)?;
            // The digests must come from the bytes that the golden microVM
            // booted. A changed file stays a config error.
            if let Some(pinned) = &tenancy.pinned {
                pinned.check()?;
            }
            let meta = serde_json::to_vec(&meta)
                .map_err(std::io::Error::other)
                .map_err(copy_error)?;
            std::fs::create_dir_all(&out).map_err(copy_error)?;
            // The manifest replaces the old one with the pair, as one set.
            let staged = staging_name(&meta_path);
            std::fs::write(&staged, &meta).map_err(copy_error)?;
            copies.push((staged.clone(), meta_path));
            let published = publish_snapshot(&copies, gid);
            let _ = std::fs::remove_file(&staged);
            published.map_err(copy_error)
        })
        .await
        .map_err(|e| SandboxError::Unavailable(format!("the snapshot copy failed: {e}")))??;
        Ok(files)
    }

    /// Lay out the jail of `id` and build the jailer command.
    async fn prepare_jailed(
        &self,
        jailer: &JailerConfig,
        id: &str,
        boot: &BootMethod,
        vmm: &Arc<CheckedVmm>,
    ) -> Result<Prepared, SandboxError> {
        use std::os::unix::fs::DirBuilderExt as _;
        let config = &self.config;
        // The image that this launcher pinned at its start.
        let pinned = self.tenancy.pinned.as_ref();
        if let Some(pinned) = pinned {
            pinned.check()?;
        }
        let jail = super::jailer::jail_dir(jailer, &self.exec_name, id);
        let root = jail.join("root");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&root)
            .map_err(|e| prepare_error(id, "the jail", &e))?;
        // A failed step or a dropped launch removes the jail, so it leaves
        // nothing.
        let guard = JailGuard::new(jail.clone(), Arc::clone(&self.tenancy));
        let fail = |what: &str, e: &std::io::Error| prepare_error(id, what, e);
        let kernel = pinned.map_or(&config.image.kernel, |pinned| &pinned.kernel);
        let rootfs = pinned.map_or(&config.image.rootfs, |pinned| &pinned.rootfs);
        let mut links = vec![
            (kernel.clone(), root.join(KERNEL_FILE), "the kernel"),
            (
                rootfs.clone(),
                root.join(ROOTFS_FILE),
                "the root file system",
            ),
        ];
        if let BootMethod::Snapshot(files) = boot {
            let files = pinned
                .and_then(|pinned| pinned.snapshot.as_ref())
                .unwrap_or(files);
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
        // The construction checked that each image is on the file system of
        // the jail, so a hard link works and nothing is copied. The links run
        // off the async workers. A dropped launch does not stop this task, so
        // the task removes the jail again when the guard is gone.
        let abandoned = guard.abandoned();
        let task_jail = jail.clone();
        let task_tenancy = Arc::clone(&self.tenancy);
        let linked = tokio::task::spawn_blocking(move || {
            let linked = links.into_iter().try_for_each(|(from, to, what)| {
                std::fs::hard_link(&from, &to).map_err(|e| (what, e))
            });
            if abandoned.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = crate::sandbox::exec::remove_work_dir(&task_jail);
                task_tenancy.keep_dir_if_left(&task_jail);
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
                config_file_args()
            }
            BootMethod::Api | BootMethod::Snapshot(_) => {
                api_socket = Some(root.join(super::api::JAILED_API_SOCKET));
                Vec::new()
            }
        };
        let mut command = vmm.jailer_command(jailer, id, config.machine, &firecracker_args)?;
        command.current_dir(&jail);
        guard.disarm();
        Ok(Prepared {
            vm_dir: jail,
            vsock: root.join(VSOCK_SOCKET),
            api_socket,
            cgroup: Some(super::jailer::cgroup_dir(jailer, id)),
            command,
            vmm: Some(Arc::clone(vmm)),
        })
    }

    /// Create the directory of `id` and build the Firecracker command.
    fn prepare_unjailed(
        &self,
        work_root: &Path,
        id: &str,
        boot: &BootMethod,
        vmm: &Arc<CheckedVmm>,
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
        let mut command = tokio::process::Command::new(&vmm.firecracker);
        command.args(args).current_dir(&dir);
        Ok(Prepared {
            vm_dir: dir.clone(),
            vsock: dir.join(VSOCK_SOCKET),
            api_socket,
            cgroup: None,
            command,
            vmm: Some(Arc::clone(vmm)),
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

    /// Configure and start, or restore, a VM through its API socket, within
    /// the API timeout in total.
    async fn boot_through_api(
        &self,
        vm: &mut RunningVm,
        socket: &Path,
        boot: &BootMethod,
    ) -> Result<(), SandboxError> {
        tokio::time::timeout(
            self.config.api_timeout,
            self.api_boot_steps(vm, socket, boot),
        )
        .await
        .map_err(|_| {
            SandboxError::Unavailable(format!(
                "the API boot did not finish within {:?}",
                self.config.api_timeout
            ))
        })?
    }

    /// The steps of [`Self::boot_through_api`].
    async fn api_boot_steps(
        &self,
        vm: &mut RunningVm,
        socket: &Path,
        boot: &BootMethod,
    ) -> Result<(), SandboxError> {
        let deadline = crate::sandbox::deadline_after(self.config.api_timeout);
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
            self.tenancy.retry();
            let vmm = self.check_versions().await?;
            let id = new_vm_id(&self.owner);
            validate_vm_id(&id)?;
            let prepared = self.prepare(&id, &self.config.boot, &vmm).await?;
            let mut vm = RunningVm::spawn(id, prepared, Arc::clone(&self.tenancy))?;
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

    /// The two version probes of the first launch, the API boot, and
    /// [`LAUNCH_SETUP_ALLOWANCE`].
    fn launch_budget(&self) -> Duration {
        let api = match self.config.boot {
            BootMethod::ConfigFile => Duration::ZERO,
            BootMethod::Api | BootMethod::Snapshot(_) => self.config.api_timeout,
        };
        super::posture::VERSION_PROBE_TIMEOUT
            .saturating_mul(2)
            .saturating_add(api)
            .saturating_add(LAUNCH_SETUP_ALLOWANCE)
    }

    /// The guest memory less the guest reserve.
    fn memory_capacity(&self) -> Option<u64> {
        Some(self.config.machine.run_memory_bytes())
    }

    /// The `/work` tmpfs that the guest mounts.
    fn work_capacity(&self) -> Option<u64> {
        Some(super::config::work_mib(&self.config.image).saturating_mul(1 << 20))
    }
}

/// The `statvfs` mount flags of the file system that holds `path`.
fn mount_flags(path: &Path) -> Result<libc::c_ulong, SandboxError> {
    use std::os::unix::ffi::OsStrExt as _;
    let fail = |e: &dyn std::fmt::Display| {
        SandboxError::Config(format!("cannot read the mount of {}: {e}", path.display()))
    };
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|e| fail(&e))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    // SAFETY: a NUL-terminated path and a buffer of the right size.
    if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(fail(&std::io::Error::last_os_error()));
    }
    // SAFETY: statvfs filled the buffer.
    Ok(unsafe { stat.assume_init() }.f_flag)
}

/// Check the mount flags `flags` of the VM root `root`.
///
/// The VMM copies run from the root, so `noexec` stops each launch. The
/// jailer makes `dev/kvm` in each jail below the root, and Firecracker cannot
/// open a device node on a `nodev` mount.
///
/// # Errors
///
/// [`SandboxError::Config`] for a `noexec` root, or a `nodev` jailed root.
fn check_mount_flags(root: &Path, flags: libc::c_ulong, jailed: bool) -> Result<(), SandboxError> {
    let problem = if flags & libc::ST_NOEXEC != 0 {
        "noexec, so the VMM copies there cannot run"
    } else if jailed && flags & libc::ST_NODEV != 0 {
        "nodev, so Firecracker cannot open /dev/kvm in a jail there"
    } else {
        return Ok(());
    };
    Err(SandboxError::Config(format!(
        "the VM root {} is on a mount with {problem}",
        root.display()
    )))
}

/// The API socket of an unjailed VM, in its directory.
const UNJAILED_API_SOCKET: &str = "firecracker.socket";

/// The names in a jail root before the jailer copies the Firecracker binary
/// there. The jailer makes `dev` and `run`, and the launcher the rest.
const JAIL_ROOT_NAMES: [&str; 8] = [
    "dev",
    "run",
    KERNEL_FILE,
    ROOTFS_FILE,
    CONFIG_FILE,
    super::config::VSOCK_SOCKET,
    SNAPSHOT_STATE_FILE,
    SNAPSHOT_MEMORY_FILE,
];

/// The snapshot state file name in the chroot.
const SNAPSHOT_STATE_FILE: &str = "snapshot.state";

/// The snapshot memory file name in the chroot.
const SNAPSHOT_MEMORY_FILE: &str = "snapshot.mem";

/// The time limit of the pause and snapshot calls. A full snapshot writes
/// the whole guest memory.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(120);

/// Copies of the VMM binaries that a version check passed.
///
/// Each launch runs these copies. A deploy can replace a configured binary
/// at any time, but it cannot change a copy. So a launch runs the bytes that
/// the check ran. The directory goes with the last user of the copies.
#[derive(Debug)]
struct CheckedVmm {
    /// The directory of the copies: `hsbx-<owner>-<random>-vmm` in the VM
    /// root. A sweep removes it as a VM directory of a dead owner.
    dir: PathBuf,
    /// The Firecracker copy. It keeps the file name, because the jailer
    /// names the chroot after it.
    firecracker: PathBuf,
    /// The jailer copy of a jailed launcher.
    jailer: Option<PathBuf>,
    /// The Firecracker version.
    version: super::posture::Version,
    /// The identity of each configured binary at its copy.
    sources: Vec<FileId>,
    /// The tenancy that tries the directory again when its removal fails.
    tenancy: Arc<Tenancy>,
}

impl CheckedVmm {
    /// The command that starts the jailer copy for `id`, with the Firecracker
    /// copy as its exec file.
    fn jailer_command(
        &self,
        jailer: &JailerConfig,
        id: &str,
        machine: MachineSpec,
        firecracker_args: &[String],
    ) -> Result<tokio::process::Command, SandboxError> {
        let bin = self
            .jailer
            .as_ref()
            .ok_or_else(|| SandboxError::Config("the version check copied no jailer".into()))?;
        let mut command = tokio::process::Command::new(bin);
        command.args(super::jailer::jailer_args(
            jailer,
            &self.firecracker,
            id,
            machine,
            firecracker_args,
        ));
        Ok(command)
    }
}

impl Drop for CheckedVmm {
    fn drop(&mut self) {
        let _ = crate::sandbox::exec::remove_work_dir(&self.dir);
        self.tenancy.keep_dir_if_left(&self.dir);
    }
}

/// What a launcher reads of the worker process, so a test can set it.
#[derive(Debug, Clone, Copy)]
struct Worker {
    /// The effective uid.
    euid: u32,
    /// The hard rlimits that a jailer of the worker cannot pass.
    hard: super::jailer::HardCeilings,
}

/// Check the jailer settings of a launcher with `machine`, for `worker` on a
/// host with the mount table `mounts`.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] for a setting that would fail each launch.
fn check_jailed(
    jailer: &JailerConfig,
    machine: MachineSpec,
    (worker, mounts): (Worker, &str),
    netns_check: fn(&Path) -> Result<(), SandboxError>,
) -> Result<(), SandboxError> {
    let euid = worker.euid;
    if jailer.uid == 0 || jailer.gid == 0 {
        return Err(SandboxError::Config(
            "the jailer must drop to a non-root uid and gid, or an escaped VMM keeps root".into(),
        ));
    }
    // The jailer would fail to drop to it at each launch.
    if jailer.uid == u32::MAX || jailer.gid == u32::MAX {
        return Err(SandboxError::Config(
            "the jail uid and gid cannot be u32::MAX, which is (uid_t)-1 and which \
                 setuid and setgid refuse"
                .into(),
        ));
    }
    // A non-root worker fails below with its own error. Its hard limits
    // would point the operator at the wrong remedy.
    let hard = if euid == 0 {
        worker.hard
    } else {
        super::jailer::HardCeilings::default()
    };
    super::jailer::check_limits(jailer, super::jailer::host_nr_open(), hard)?;
    super::jailer::check_cgroups(jailer, machine, mounts)?;
    // `RunningVm::spawn` clears the environment, so only the
    // arguments of the jailer count.
    super::jailer::check_jailer_exec(
        jailer,
        machine,
        &config_file_args(),
        crate::sandbox::exec::spawn_exec_capacity(),
    )?;
    if let Some(netns) = &jailer.netns {
        netns_check(netns)?;
    }
    // The jailer makes the chroot and the cgroup, then drops to the
    // jail uid. Each step needs root, so another uid fails each launch.
    if euid != 0 {
        return Err(SandboxError::Config(format!(
            "the jailer needs root, and this process runs as uid {euid}; run the worker \
                 as root or use Confinement::Unjailed"
        )));
    }
    Ok(())
}

/// The largest Firecracker or jailer binary that the launcher copies. A
/// release binary is a few MiB, and a debug build stays far below this.
pub const MAX_VMM_BINARY_BYTES: u64 = 256 << 20;

/// Refuse the VMM binary at `path` when its `size` is above
/// [`MAX_VMM_BINARY_BYTES`].
///
/// # Errors
///
/// Returns [`SandboxError::Config`]. The path names another file, and each
/// version check would copy all of it.
fn check_vmm_size(path: &Path, size: u64) -> Result<(), SandboxError> {
    if size > MAX_VMM_BINARY_BYTES {
        return Err(SandboxError::Config(format!(
            "{} is {size} bytes, above the {MAX_VMM_BINARY_BYTES} bytes of a VMM binary; the \
             path names another file, and each version check would copy all of it",
            path.display()
        )));
    }
    Ok(())
}

/// Copy each `(name, path)` of `sources` to `<dir>/<name>/<file name>`, with
/// mode `0555`. Each source must still be its entry of `ids`, before and
/// after its copy. Otherwise a deploy changed it during the check.
///
/// The jailer copies the Firecracker binary into the chroot with its mode,
/// and runs it as the jail uid. So each user may run a copy. The directory
/// has mode `0700`, so only the worker reaches the copies.
///
/// # Errors
///
/// Returns [`SandboxError::Unavailable`] when a copy fails or a source
/// changes. The next check tries again.
fn copy_binaries(
    dir: &Path,
    sources: &[(&str, PathBuf)],
    ids: &[FileId],
) -> Result<Vec<PathBuf>, SandboxError> {
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
    let fail = |what: &Path, e: &dyn std::fmt::Display| {
        SandboxError::Unavailable(format!("cannot copy {}: {e}", what.display()))
    };
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(dir)
        .map_err(|e| fail(dir, &e))?;
    let mut copies = Vec::with_capacity(sources.len());
    for ((name, from), id) in sources.iter().zip(ids) {
        let file_name = from
            .file_name()
            .ok_or_else(|| fail(from, &"the path has no file name"))?;
        let to = dir.join(name).join(file_name);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(dir.join(name))
            .map_err(|e| fail(from, &e))?;
        // A plain open of a FIFO waits for a writer. So the open does not
        // block, and a file that is not regular is refused.
        let mut source = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(from)
            .map_err(|e| fail(from, &e))?;
        if !source.metadata().is_ok_and(|meta| meta.is_file()) {
            return Err(fail(from, &"the path is not a regular file"));
        }
        let changed =
            |source: &std::fs::File| !source.metadata().is_ok_and(|meta| FileId::of(&meta) == *id);
        if changed(&source) {
            return Err(fail(from, &"a deploy changed the file during the check"));
        }
        let mut copy = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o555)
            .open(&to)
            .map_err(|e| fail(from, &e))?;
        // A file that grows during the copy stops one byte past the bound.
        let bytes = std::io::copy(
            &mut std::io::Read::take(&mut source, MAX_VMM_BINARY_BYTES + 1),
            &mut copy,
        )
        .map_err(|e| fail(from, &e))?;
        if bytes > MAX_VMM_BINARY_BYTES {
            return Err(fail(from, &"the file grew past the size of a VMM binary"));
        }
        // The umask can narrow the mode of the open, so it is set again.
        copy.set_permissions(std::fs::Permissions::from_mode(0o555))
            .map_err(|e| fail(from, &e))?;
        drop(copy);
        if changed(&source) {
            return Err(fail(from, &"a deploy changed the file during the check"));
        }
        copies.push(to);
    }
    Ok(copies)
}

/// The Firecracker arguments of a boot from [`CONFIG_FILE`]. No other boot
/// has longer ones.
fn config_file_args() -> Vec<String> {
    vec![
        "--config-file".into(),
        CONFIG_FILE.into(),
        "--no-api".into(),
    ]
}

/// The request type of `ioctl`: glibc and musl declare it apart.
#[cfg(target_env = "gnu")]
type Ioctl = libc::c_ulong;
/// The request type of `ioctl`: glibc and musl declare it apart.
#[cfg(not(target_env = "gnu"))]
type Ioctl = libc::c_int;

/// `NS_GET_NSTYPE`: `_IO(0xb7, 0x3)`. Only a namespace handle answers it.
const NS_GET_NSTYPE: Ioctl = 0xb703;

/// Check that `path` is a network namespace handle, such as a bind mount in
/// `/run/netns`. The jailer joins it with `setns`, so another file fails
/// each launch.
///
/// # Errors
///
/// Returns [`SandboxError::Config`] when `path` cannot be opened, or holds
/// another file or another namespace.
fn check_netns(path: &Path) -> Result<(), SandboxError> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let refuse =
        |why: &str| SandboxError::Config(format!("the network namespace {} {why}", path.display()));
    // A plain open of a FIFO waits for a writer.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .map_err(|e| refuse(&format!("cannot be opened: {e}")))?;
    // SAFETY: NS_GET_NSTYPE takes no argument, and the descriptor is open.
    let kind = unsafe { libc::ioctl(file.as_raw_fd(), NS_GET_NSTYPE) };
    if kind == libc::CLONE_NEWNET {
        Ok(())
    } else {
        Err(refuse("is not a network namespace handle"))
    }
}

/// What identifies the file at a path. A replaced or rewritten file has
/// another inode, size or change time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
    size: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl FileId {
    /// The identity in `meta`.
    fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt as _;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            size: meta.size(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
            ctime: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

/// The identity of the file at `path`.
fn file_id(path: &Path) -> Result<FileId, SandboxError> {
    let meta = std::fs::metadata(path)
        .map_err(|e| SandboxError::Config(format!("cannot inspect {}: {e}", path.display())))?;
    Ok(FileId::of(&meta))
}

/// The most bytes of a snapshot manifest.
const MAX_MANIFEST_BYTES: u64 = 64 * 1024;

/// Read the snapshot manifest at `path`. A symbolic link or a file that is
/// not regular is refused, and the read stops at [`MAX_MANIFEST_BYTES`].
/// The open does not block, so a FIFO cannot stall the launcher start.
fn read_manifest(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("it is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_MANIFEST_BYTES).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// The release of the host kernel, which restores a snapshot.
fn host_kernel() -> std::io::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/osrelease")?
        .trim()
        .to_owned())
}

/// The CPU model of this host: the model lines of its first CPU in
/// `/proc/cpuinfo`. A snapshot holds CPU state.
fn cpu_model() -> std::io::Result<String> {
    const KEYS: [&str; 9] = [
        "vendor_id",
        "cpu family",
        "model",
        "model name",
        "stepping",
        "CPU implementer",
        "CPU variant",
        "CPU part",
        "CPU revision",
    ];
    let info = std::fs::read_to_string("/proc/cpuinfo")?;
    let first = info.split("\n\n").next().unwrap_or_default();
    Ok(first
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| KEYS.contains(&key.trim()))
        .map(|(key, value)| format!("{}={}", key.trim(), value.trim()))
        .collect::<Vec<_>>()
        .join("; "))
}

/// The SHA-256 digest of the file at `path`, in hex.
fn file_sha256(path: &Path) -> std::io::Result<String> {
    use sha2::Digest as _;
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        let n = file.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        hasher.update(&chunk[..n]);
    }
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    Ok(hex)
}

/// The longest time that a pin or a snapshot publish waits for the lock of
/// an image directory.
const IMAGE_LOCK_WAIT: Duration = Duration::from_secs(30);

/// The time between two tries of a held lock.
const IMAGE_LOCK_POLL: Duration = Duration::from_millis(10);

/// Lock the directory of each file in `files` with `op`, a `flock`
/// operation. The locks go when the returned files close.
///
/// A pin takes shared locks. A publish takes an exclusive lock while it
/// renames files, as `publish-image.sh` does. So a pin never links the
/// files of a publish in progress.
///
/// # Errors
///
/// Returns `WouldBlock` when a lock stays held for `wait`, or the error of
/// an open.
fn lock_dirs(
    files: &[&Path],
    op: libc::c_int,
    wait: Duration,
) -> std::io::Result<Vec<std::fs::File>> {
    use std::os::fd::AsRawFd as _;
    let dirs: std::collections::BTreeSet<&Path> = files.iter().filter_map(|f| f.parent()).collect();
    let deadline = crate::sandbox::deadline_after(wait);
    let mut locks = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let file = std::fs::File::open(dir)?;
        loop {
            // SAFETY: flock reads no memory, and the descriptor is open.
            if unsafe { libc::flock(file.as_raw_fd(), op | libc::LOCK_NB) } == 0 {
                break;
            }
            let err = std::io::Error::last_os_error();
            let busy = matches!(
                err.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
            );
            if !busy || std::time::Instant::now() >= deadline {
                return Err(err);
            }
            std::thread::sleep(IMAGE_LOCK_POLL);
        }
        locks.push(file);
    }
    Ok(locks)
}

/// Copy each snapshot file to its target, and replace the old files only
/// when every copy is complete.
///
/// Each copy goes to a temporary name next to its target and is synced.
/// So a failed copy, such as on a full disk, keeps the old pair. A failed
/// rename puts back the old file that already moved, so it keeps the old
/// pair too.
fn publish_snapshot(copies: &[(PathBuf, PathBuf)], gid: u32) -> std::io::Result<()> {
    let mut staged = Vec::new();
    let mut publish = || {
        for (from, to) in copies {
            let temp = staging_name(to);
            staged.push(temp.clone());
            copy_out(from, &temp, gid)?;
        }
        let moves: Vec<_> = staged
            .iter()
            .cloned()
            .zip(copies.iter().map(|(_, to)| to.clone()))
            .collect();
        // A pin links under a shared lock of the directory. So it cannot link
        // one old file and one new file.
        let targets: Vec<&Path> = copies.iter().map(|(_, to)| to.as_path()).collect();
        let _lock = lock_dirs(&targets, libc::LOCK_EX, IMAGE_LOCK_WAIT)?;
        replace_all(&moves)?;
        // Sync the directory, so the new names survive a crash.
        if let Some(dir) = copies.first().and_then(|(_, to)| to.parent()) {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    };
    let result = publish();
    if result.is_err() {
        for temp in &staged {
            let _ = std::fs::remove_file(temp);
        }
    }
    result
}

/// Rename each staged file over its target.
///
/// Each old target first gets a hard link, so a failed rename can put back
/// the targets that already moved. The old set then stays whole. When a
/// target cannot be put back, each backup stays, and the error names them.
fn replace_all(moves: &[(PathBuf, PathBuf)]) -> std::io::Result<()> {
    replace_all_with(moves, |from, to| std::fs::rename(from, to))
}

/// [`replace_all`] with `rename` for each rename, so a test can fail one.
fn replace_all_with(
    moves: &[(PathBuf, PathBuf)],
    rename: impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut backups = Vec::new();
    // Set when the rollback fails. The backups then hold the old set.
    let mut keep_backups = false;
    let result = (|| {
        for (_, to) in moves {
            let backup = staging_name(to);
            match std::fs::hard_link(to, &backup) {
                Ok(()) => backups.push(Some(backup)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => backups.push(None),
                Err(e) => return Err(e),
            }
        }
        for (done, (temp, to)) in moves.iter().enumerate() {
            if let Err(e) = rename(temp, to) {
                let mut failed = Vec::new();
                for ((_, to), backup) in moves[..done].iter().zip(&backups) {
                    let restored = backup
                        .as_ref()
                        .map_or_else(|| std::fs::remove_file(to), |backup| rename(backup, to));
                    if let Err(restore) = restored {
                        failed.push(format!("{}: {restore}", to.display()));
                    }
                }
                if failed.is_empty() {
                    return Err(e);
                }
                keep_backups = true;
                let kept: Vec<String> = backups
                    .iter()
                    .flatten()
                    .filter(|backup| backup.exists())
                    .map(|backup| backup.display().to_string())
                    .collect();
                return Err(std::io::Error::other(format!(
                    "{e}; the old files could not be put back ({}); they stay at {}",
                    failed.join(", "),
                    kept.join(", ")
                )));
            }
        }
        Ok(())
    })();
    if !keep_backups {
        for backup in backups.into_iter().flatten() {
            let _ = std::fs::remove_file(backup);
        }
    }
    result
}

/// A new hidden name next to `to`.
fn staging_name(to: &Path) -> PathBuf {
    let name = to.file_name().unwrap_or_default().to_string_lossy();
    to.with_file_name(format!(".{name}.{}", uuid::Uuid::new_v4().simple()))
}

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
        .create_new(true)
        .mode(0o640)
        .custom_flags(libc::O_NOFOLLOW)
        .open(to)?;
    std::io::copy(&mut source, &mut target)?;
    std::os::unix::fs::fchown(&target, None, Some(gid))?;
    target.set_permissions(std::fs::Permissions::from_mode(0o640))?;
    target.sync_all()
}

/// Check the paths of `config`: each is absolute and valid UTF-8, and each
/// binary, image and snapshot is a file. A snapshot boot needs the jailer.
fn check_launcher_paths(config: &LauncherConfig) -> Result<(), SandboxError> {
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
    let mut paths = required.clone();
    match &config.confinement {
        Confinement::Unjailed { work_root } => paths.push(("the work root", work_root)),
        Confinement::Jailer(jailer) => {
            paths.push(("the jail base", &jailer.chroot_base));
            paths.push(("the cgroup root", &jailer.cgroup_root));
            if let Some(netns) = &jailer.netns {
                paths.push(("the network namespace", netns));
            }
        }
    }
    // Firecracker runs in the VM directory, so a relative path would
    // name another file there.
    if let Some((what, path)) = paths.iter().find(|(_, path)| !path.is_absolute()) {
        return Err(SandboxError::Config(format!(
            "{what} {} must be an absolute path",
            path.display()
        )));
    }
    // Firecracker and the jailer take paths as JSON strings and as
    // arguments. A lossy conversion would name another file.
    if let Some((what, path)) = paths.iter().find(|(_, path)| path.to_str().is_none()) {
        return Err(SandboxError::Config(format!(
            "{what} {} must be valid UTF-8",
            path.display()
        )));
    }
    for (what, path) in &required {
        if !path.is_file() {
            return Err(SandboxError::Config(format!(
                "{what} {} is not a file",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Check that no other user can change a path that a jailed launch trusts.
///
/// The launcher runs the Firecracker and jailer binaries as root, before the
/// jailer drops to the jail uid. A user who can change one runs code as root.
///
/// The root jailer builds each jail under `vm_root`, and it joins the network
/// namespace. A user who can move either path can move a jail, or give an
/// escaped VMM a network. A user who can add to `vm_root` can name a live VM
/// of another launcher, which the sweep then kills.
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
    // The sweep reads each entry of the VM root as a VM, and the cgroup of a
    // dead owner's VM dies. A sticky directory still takes new entries from
    // each user. So only the owner may write the VM root itself.
    if inspect(vm_root)?.mode() & 0o022 != 0 {
        return Err(SandboxError::Config(format!(
            "the VM root {} must not be writable by group or others, because the sweep \
             trusts its entries",
            vm_root.display()
        )));
    }
    let jail_uid = jailer.uid;
    let mut shared = vec![&config.image.kernel, &config.image.rootfs];
    if let BootMethod::Snapshot(files) = &config.boot {
        shared.push(&files.state);
        shared.push(&files.memory);
        // The manifest binds the pair to its image, so no other user may
        // change it. A missing one fails at the pin, with its own message.
        let manifest = SnapshotMeta::path(&files.state);
        if inspect(&manifest).is_ok() {
            check_trusted_ancestors(&manifest, euid, inspect)?;
        }
    }
    // A launch hard-links each image into its jail. A copy across file
    // systems would take time that no launch budget bounds.
    let jail_dev = inspect(vm_root)?.dev();
    for path in shared {
        check_trusted_ancestors(path, euid, inspect)?;
        let meta = inspect(path)?;
        if meta.dev() != jail_dev {
            return Err(SandboxError::Config(format!(
                "{} must be on the file system of the jail root {}, so a launch can hard-link it",
                path.display(),
                vm_root.display()
            )));
        }
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

/// Write a VM document as JSON.
///
/// A jailed Firecracker reads the file as the jail uid. The umask can narrow
/// the mode of the open, so the file gets mode 0644 again after the write.
fn write_config(path: &Path, document: &super::config::VmConfigFile) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
    let json = serde_json::to_vec_pretty(document).map_err(std::io::Error::other)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o644)
        .open(path)?;
    file.write_all(&json)?;
    file.set_permissions(std::fs::Permissions::from_mode(0o644))
}

/// Removes a jail when a launch stops before the VMM owns it.
///
/// A dropped launch future drops the guard, and the guard removes the jail.
/// The guard also marks the jail as abandoned, so a task that still writes
/// into it removes it again when it ends.
struct JailGuard {
    jail: Option<PathBuf>,
    abandoned: Arc<std::sync::atomic::AtomicBool>,
    /// The tenancy that tries the jail again when its removal fails.
    tenancy: Arc<Tenancy>,
}

impl JailGuard {
    fn new(jail: PathBuf, tenancy: Arc<Tenancy>) -> Self {
        Self {
            jail: Some(jail),
            abandoned: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            tenancy,
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
            self.tenancy.keep_dir_if_left(&jail);
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
    /// The checked binaries that `command` runs.
    vmm: Option<Arc<CheckedVmm>>,
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
    /// The lease of the launcher, held until the teardown is done.
    tenancy: Arc<Tenancy>,
    /// The process group of the VMM: its pid, as it leads a new group.
    group: Option<i32>,
    /// The checked binaries. The jailer copies Firecracker after its start,
    /// so they stay until the teardown.
    _vmm: Option<Arc<CheckedVmm>>,
}

impl RunningVm {
    fn spawn(
        id: String,
        mut prepared: Prepared,
        tenancy: Arc<Tenancy>,
    ) -> Result<Self, SandboxError> {
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
                tenancy.keep_dir_if_left(&prepared.vm_dir);
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
        let group = child.id().and_then(|pid| i32::try_from(pid).ok());
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
            tenancy,
            group,
            _vmm: prepared.vmm,
        })
    }

    /// Kill each process of the VMM group, such as a helper of a wrapper.
    /// The VMM is not reaped yet, so its pid still names this group.
    fn kill_group(&self) {
        if self.child.id().is_some()
            && let Some(group) = self.group
        {
            crate::sandbox::exec::kill_group(group);
        }
    }

    /// The kept tail of the VMM stderr, with each NUL as U+FFFD.
    fn stderr_tail(&self) -> String {
        let bytes: Vec<u8> = self
            .stderr
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        String::from_utf8_lossy(&bytes)
            .trim()
            .replace('\0', "\u{fffd}")
    }

    /// Kill the VMM, then its cgroup, then remove its directory. Each step
    /// is safe to repeat. `down` is set only at the end, so a drop in the
    /// middle still runs the rest in [`Drop`]. A cgroup that is still there
    /// goes to the launcher, which tries it again.
    async fn teardown(&mut self) {
        if self.down {
            return;
        }
        self.kill_group();
        let _ = self.child.start_kill();
        let _ = tokio::time::timeout(KILL_WAIT, self.child.wait()).await;
        if let Some(cgroup) = &self.cgroup {
            kill_cgroup(cgroup).await;
            self.tenancy.keep_if_left(cgroup);
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
            self.tenancy.keep_dir_if_left(&self.vm_dir);
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
        // The probe does not reap the VMM, so the teardown can still kill
        // its group by its pid.
        let status = match self.group {
            Some(pid) => exit_of(pid)?,
            None => self.child.try_wait().ok().flatten()?.to_string(),
        };
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
        // be cleaned without waiting.
        self.kill_group();
        let _ = self.child.start_kill();
        if let Some(cgroup) = &self.cgroup {
            // The kill is not instant, so this removal can fail. The
            // launcher then tries the cgroup again.
            crate::sandbox::cgroup::kill_and_remove(cgroup, Duration::ZERO);
            self.tenancy.keep_if_left(cgroup);
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
        if crate::sandbox::exec::remove_work_dir(&self.vm_dir).is_err() {
            self.tenancy.keep_dir_if_left(&self.vm_dir);
        }
    }
}

/// The exit of the child `pid`, or `None` while it runs. `WNOWAIT` leaves it
/// a zombie, so its pid and its group stay taken.
fn exit_of(pid: i32) -> Option<String> {
    let id = libc::id_t::try_from(pid).ok()?;
    // SAFETY: siginfo_t is plain data, and all zeros is a valid value.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: waitid writes only into `info`. WNOWAIT reaps nothing.
    let rc = unsafe {
        libc::waitid(
            libc::P_PID,
            id,
            &raw mut info,
            libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // A child that cannot be checked counts as gone, so the backend
        // tears it down.
        return (err.kind() != std::io::ErrorKind::Interrupted)
            .then(|| format!("an unknown status ({err})"));
    }
    // SAFETY: waitid filled `info`. With WNOHANG, si_pid stays 0 while the
    // child runs.
    let (found, code) = unsafe { (info.si_pid(), info.si_status()) };
    if found == 0 {
        return None;
    }
    Some(match info.si_code {
        libc::CLD_EXITED => format!("exit status: {code}"),
        libc::CLD_KILLED | libc::CLD_DUMPED => format!("signal: {code}"),
        _ => format!("status {code}"),
    })
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
        // The checked binaries belong to the launcher, not to the launch.
        let left: Vec<_> = std::fs::read_dir(launcher.vm_root())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .filter(|e| !e.file_name().to_string_lossy().ends_with(VMM_COPIES_SUFFIX))
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

    #[tokio::test]
    async fn the_api_boot_is_bounded_as_a_whole() {
        let dir = temp_dir();
        let server = dir.join("slow_api.py");
        let slow = FAKE_API_PY.replace(
            "    conn, _ = srv.accept()\n",
            "    conn, _ = srv.accept()\n    __import__('time').sleep(0.15)\n",
        );
        assert_ne!(slow, FAKE_API_PY);
        std::fs::write(&server, slow).unwrap();
        let fc = script(
            &dir,
            "firecracker",
            &format!("exec python3 {} \"$@\"", server.display()),
        );
        let mut config = unjailed(&dir, fc, BootMethod::Api);
        // Each call takes 150 ms, under the timeout. Six calls take more.
        config.api_timeout = Duration::from_millis(500);
        let launcher = new_launcher(config).unwrap();
        let err = launcher.launch().await.err().expect("a slow API boot");
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("API")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_launch_budget_covers_the_version_probes_and_the_api_boot() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::Api);
        config.api_timeout = Duration::from_secs(7);
        let launcher = new_launcher(config).unwrap();
        let need = super::super::posture::VERSION_PROBE_TIMEOUT * 2 + Duration::from_secs(7);
        assert!(
            launcher.launch_budget() > need,
            "{:?}",
            launcher.launch_budget()
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_snapshot_copy_keeps_the_old_pair() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir();
        let out = dir.join("golden");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("state"), b"old state").unwrap();
        std::fs::write(out.join("memory"), b"old memory").unwrap();
        std::fs::write(dir.join("new-state"), b"new state").unwrap();
        // SAFETY: getegid has no preconditions.
        let gid = unsafe { libc::getegid() };
        let copies = [
            (dir.join("new-state"), out.join("state")),
            (dir.join("missing-memory"), out.join("memory")),
        ];
        publish_snapshot(&copies, gid).expect_err("a missing memory file");
        assert_eq!(std::fs::read(out.join("state")).unwrap(), b"old state");
        assert_eq!(std::fs::read(out.join("memory")).unwrap(), b"old memory");
        assert_eq!(
            std::fs::read_dir(&out).unwrap().count(),
            2,
            "a temporary file is left"
        );
        std::fs::write(dir.join("new-memory"), b"new memory").unwrap();
        let copies = [
            (dir.join("new-state"), out.join("state")),
            (dir.join("new-memory"), out.join("memory")),
        ];
        publish_snapshot(&copies, gid).unwrap();
        assert_eq!(std::fs::read(out.join("state")).unwrap(), b"new state");
        assert_eq!(std::fs::read(out.join("memory")).unwrap(), b"new memory");
        let mode = std::fs::metadata(out.join("memory"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o640);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_second_rename_keeps_the_old_pair() {
        let dir = temp_dir();
        let out = dir.join("golden");
        std::fs::create_dir_all(out.join("memory")).unwrap();
        std::fs::write(out.join("state"), b"old state").unwrap();
        std::fs::write(dir.join("new-state"), b"new state").unwrap();
        std::fs::write(dir.join("new-memory"), b"new memory").unwrap();
        // SAFETY: getegid has no preconditions.
        let gid = unsafe { libc::getegid() };
        // The memory target is a directory, so its rename fails.
        let copies = [
            (dir.join("new-state"), out.join("state")),
            (dir.join("new-memory"), out.join("memory")),
        ];
        publish_snapshot(&copies, gid).expect_err("a directory in the way");
        assert_eq!(std::fs::read(out.join("state")).unwrap(), b"old state");
        assert!(out.join("memory").is_dir());
        assert_eq!(
            std::fs::read_dir(&out).unwrap().count(),
            2,
            "a temporary file is left"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_rename_that_fails_puts_back_the_targets_that_moved() {
        let dir = temp_dir();
        std::fs::write(dir.join("state"), b"old state").unwrap();
        std::fs::write(dir.join("memory"), b"old memory").unwrap();
        std::fs::write(dir.join("new-state"), b"new state").unwrap();
        std::fs::write(dir.join("new-file"), b"new file").unwrap();
        // The second staged file is missing, so its rename fails after the
        // first rename and the third target is new.
        let moves = [
            (dir.join("new-state"), dir.join("state")),
            (dir.join("new-file"), dir.join("file")),
            (dir.join("missing-memory"), dir.join("memory")),
        ];
        replace_all(&moves).expect_err("a missing staged file");
        assert_eq!(std::fs::read(dir.join("state")).unwrap(), b"old state");
        assert_eq!(std::fs::read(dir.join("memory")).unwrap(), b"old memory");
        assert!(!dir.join("file").exists(), "a new target is removed");
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        // The moved staged files are gone, and no backup is left.
        assert_eq!(names, ["memory", "state"]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_rollback_keeps_the_old_files() {
        let dir = temp_dir();
        std::fs::write(dir.join("state"), b"old state").unwrap();
        std::fs::write(dir.join("memory"), b"old memory").unwrap();
        std::fs::write(dir.join("new-state"), b"new state").unwrap();
        std::fs::write(dir.join("new-memory"), b"new memory").unwrap();
        let moves = [
            (dir.join("new-state"), dir.join("state")),
            (dir.join("new-memory"), dir.join("memory")),
        ];
        // The first rename works. The second rename and the rollback fail.
        let calls = std::cell::Cell::new(0);
        let err = replace_all_with(&moves, |from, to| {
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                std::fs::rename(from, to)
            } else {
                Err(std::io::Error::other("an injected failure"))
            }
        })
        .expect_err("the second rename fails");
        assert!(err.to_string().contains("put back"), "{err}");
        // The old state stays under its backup name, so it is not lost.
        let kept: Vec<Vec<u8>> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(".state.")
            })
            .map(|path| std::fs::read(path).unwrap())
            .collect();
        assert_eq!(kept, [b"old state".to_vec()]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_snapshot_manifest_that_others_can_change_is_a_config_error() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = temp_dir();
        let files = SnapshotFiles {
            state: dir.join("snap.state"),
            memory: dir.join("snap.mem"),
        };
        std::fs::write(&files.state, b"state").unwrap();
        std::fs::write(&files.memory, b"memory").unwrap();
        let mut config = jailed(&dir);
        config.boot = BootMethod::Snapshot(files.clone());
        write_snapshot_meta(&config, &files);
        let manifest = SnapshotMeta::path(&files.state);
        let refused = |what: &str| {
            let err = new_launcher(config.clone()).expect_err(what);
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("snap.state.json")),
                "{what}: {err:?}"
            );
        };
        // A forged manifest could pass a snapshot of another image.
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o666)).unwrap();
        refused("a manifest that others can write");
        std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o644)).unwrap();
        let real = dir.join("real.json");
        std::fs::rename(&manifest, &real).unwrap();
        std::os::unix::fs::symlink(&real, &manifest).unwrap();
        refused("a manifest behind a symbolic link");
        std::fs::remove_file(&manifest).unwrap();
        std::fs::rename(&real, &manifest).unwrap();
        drop(new_launcher(config.clone()).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_snapshot_manifest_that_is_not_a_file_is_a_config_error() {
        let dir = temp_dir();
        let files = SnapshotFiles {
            state: dir.join("snap.state"),
            memory: dir.join("snap.mem"),
        };
        std::fs::write(&files.state, b"state").unwrap();
        std::fs::write(&files.memory, b"memory").unwrap();
        let mut config = jailed(&dir);
        config.boot = BootMethod::Snapshot(files.clone());
        // A FIFO with no writer blocks a plain open for ever.
        let fifo = std::ffi::CString::new(
            SnapshotMeta::path(&files.state)
                .into_os_string()
                .into_encoded_bytes(),
        )
        .unwrap();
        // SAFETY: mkfifo reads a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o644) }, 0);
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || done.send(new_launcher(config).map(drop)));
        let result = result
            .recv_timeout(Duration::from_secs(5))
            .expect("the launcher start must not wait for a writer");
        assert!(
            matches!(result, Err(SandboxError::Config(ref m)) if m.contains("manifest")),
            "{result:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_replaced_vmm_binary_is_checked_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        assert_eq!(launcher.check_versions().await.unwrap().version, (1, 17, 0));
        // A deploy replaces Firecracker with a version that has a known escape.
        let next = dir.join("firecracker.next");
        crate::sandbox::firecracker::fake::write_executable(
            &next,
            "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'Firecracker v1.10.0'; exit 0; fi\n",
        );
        std::fs::rename(&next, &launcher.config().firecracker_bin).unwrap();
        let err = launcher
            .check_versions()
            .await
            .expect_err("the old check must not cover the new binary");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Replace the file at `path` with a script of `body`, as a deploy does.
    fn swap(path: &Path, body: &str) {
        let next = path.with_extension("next");
        crate::sandbox::firecracker::fake::write_executable(&next, body);
        std::fs::rename(&next, path).unwrap();
    }

    #[tokio::test]
    async fn a_launch_runs_the_firecracker_that_the_check_passed() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo checked > ran");
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let vmm = launcher.check_versions().await.unwrap();
        // A deploy swaps the binary after the check and before the spawn.
        swap(
            &launcher.config().firecracker_bin,
            "#!/bin/sh\necho swapped > ran\n",
        );
        let id = new_vm_id(launcher.owner());
        let mut prepared = launcher
            .prepare(&id, &BootMethod::ConfigFile, &vmm)
            .await
            .unwrap();
        assert!(prepared.command.status().await.unwrap().success());
        let ran = std::fs::read_to_string(prepared.vm_dir.join("ran")).unwrap();
        assert_eq!(ran.trim(), "checked");
        let root = launcher.vm_root();
        drop((prepared, vmm, launcher));
        // The copies go with their last user.
        let left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name.to_string_lossy().ends_with(VMM_COPIES_SUFFIX))
            .collect();
        assert!(left.is_empty(), "{left:?}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_jailer_command_over_the_exec_allowance_is_a_config_error() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let Confinement::Jailer(jail) = &mut config.confinement else {
            unreachable!()
        };
        // About 7 MB of arguments, over the 6 MiB that any stack limit gives.
        for i in 0..64 {
            jail.extra_cgroups
                .push((format!("io.x{i}"), "1".repeat(110_000)));
        }
        let err = new_launcher(config).expect_err("a jailer command over the allowance");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("exec allowance")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_vmm_binary_that_is_not_a_file_fails_the_check_at_once() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let launcher = new_launcher(unjailed(&dir, fc.clone(), BootMethod::ConfigFile)).unwrap();
        // The start refuses a FIFO, so a deploy swaps one in later.
        let fifo = dir.join("fifo");
        let path =
            std::ffi::CString::new(fifo.clone().into_os_string().into_encoded_bytes()).unwrap();
        // SAFETY: mkfifo reads a valid NUL-terminated path.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o755) }, 0);
        std::fs::rename(&fifo, &fc).unwrap();
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(launcher.check_versions()).map(drop);
            drop(launcher);
            done.send(result)
        });
        let result = result
            .recv_timeout(Duration::from_secs(5))
            .expect("the check must not wait for a writer");
        assert!(
            matches!(result, Err(SandboxError::Unavailable(ref m)) if m.contains("regular file")),
            "{result:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_jailed_launch_runs_the_binaries_that_the_check_passed() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let Confinement::Jailer(jail) = &config.confinement else {
            unreachable!()
        };
        let checked = (
            std::fs::read(&config.firecracker_bin).unwrap(),
            std::fs::read(&jail.jailer_bin).unwrap(),
        );
        let launcher = new_launcher(config.clone()).unwrap();
        let vmm = launcher.check_versions().await.unwrap();
        swap(&config.firecracker_bin, "#!/bin/sh\nexit 0\n");
        swap(&jail.jailer_bin, "#!/bin/sh\nexit 0\n");
        let id = new_vm_id(launcher.owner());
        let prepared = launcher
            .prepare(&id, &BootMethod::ConfigFile, &vmm)
            .await
            .unwrap();
        let command = prepared.command.as_std();
        assert_eq!(std::fs::read(command.get_program()).unwrap(), checked.1);
        let args: Vec<_> = command.get_args().collect();
        let at = args.iter().position(|a| *a == "--exec-file").unwrap();
        let exec_file = Path::new(args[at + 1]);
        assert_eq!(std::fs::read(exec_file).unwrap(), checked.0);
        // The jailer names the chroot after the file name.
        assert_eq!(exec_file.file_name(), config.firecracker_bin.file_name());
        // The jailer keeps the mode in the chroot, and the jail uid runs it.
        let mode = std::fs::metadata(exec_file).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o555, "{mode:o}");
        drop(prepared);
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_snapshot_of_another_firecracker_is_a_config_error() {
        let dir = temp_dir();
        let files = SnapshotFiles {
            state: dir.join("snap.state"),
            memory: dir.join("snap.mem"),
        };
        std::fs::write(&files.state, b"state").unwrap();
        std::fs::write(&files.memory, b"memory").unwrap();
        let mut config = jailed(&dir);
        config.boot = BootMethod::Snapshot(files.clone());
        // The fake Firecracker reports 1.17.0.
        let image = (config.image.kernel.as_path(), config.image.rootfs.as_path());
        write_meta(
            &files,
            &SnapshotMeta::of(&config, image, (1, 16, 0)).unwrap(),
        );
        let launcher = new_launcher(config.clone()).unwrap();
        let err = launcher
            .check_versions()
            .await
            .expect_err("another Firecracker");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("Firecracker")),
            "{err:?}"
        );
        drop(launcher);
        write_snapshot_meta(&config, &files);
        let launcher = new_launcher(config).unwrap();
        launcher.check_versions().await.unwrap();
        drop(launcher);
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
    fn the_sweep_keeps_the_vms_of_an_owner_whose_lease_it_cannot_open() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let sweeper = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let root = sweeper.vm_root();
        // A link to itself fails to open with ELOOP, even for root.
        let lease = root.join("hsbx-cafef00d.lease");
        std::os::unix::fs::symlink(&lease, &lease).unwrap();
        let vm = root.join(format!("hsbx-cafef00d-{}", "d".repeat(16)));
        std::fs::create_dir_all(&vm).unwrap();
        assert_eq!(sweeper.sweep_stale(), 0);
        assert!(vm.exists(), "an owner that cannot be checked stays live");
        drop(sweeper);
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
    fn a_boot_line_too_long_for_firecracker_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config
            .image
            .extra_boot_args
            .push(format!("harvest.pad={}", "a".repeat(3 * 1024)));
        let err = new_launcher(config).expect_err("too long");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("Firecracker")),
            "{err:?}"
        );
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
        std::fs::write(cgroup.join("cgroup.events"), b"populated 1\nfrozen 1\n").unwrap();
        let waiter = std::thread::spawn(move || child.wait().unwrap());
        kill_cgroup(&cgroup).await;
        let status = waiter.join().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL), "{status:?}");
        std::fs::remove_dir_all(cgroup).unwrap();
    }

    #[tokio::test]
    async fn a_cgroup_that_outlives_the_teardown_is_tried_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        // A cgroup that still reports a process cannot be removed.
        let name = format!("hsbx-{}-{}", launcher.owner(), "f".repeat(16));
        let cgroup = dir.join("cgroup").join(name);
        std::fs::create_dir_all(&cgroup).unwrap();
        std::fs::write(cgroup.join("cgroup.events"), b"populated 1\n").unwrap();
        let vm_dir = dir.join("vm");
        std::fs::create_dir_all(&vm_dir).unwrap();
        let mut command = tokio::process::Command::new("sleep");
        command.arg("30");
        let prepared = Prepared {
            vsock: vm_dir.join("v.sock"),
            vm_dir,
            api_socket: None,
            cgroup: Some(cgroup.clone()),
            command,
            vmm: None,
        };
        let mut vm =
            RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy)).unwrap();
        vm.teardown().await;
        drop(vm);
        // The sweep skips the cgroups of a live owner, but not this one.
        let kill = cgroup.join("cgroup.kill");
        std::fs::remove_file(&kill).unwrap();
        launcher.sweep_stale();
        assert!(kill.exists(), "the sweep did not kill the cgroup again");
        assert!(!launcher.tenancy.is_empty());
        // A cgroup that is gone leaves the list.
        std::fs::remove_dir_all(&cgroup).unwrap();
        launcher.sweep_stale();
        assert!(launcher.tenancy.is_empty());
        drop(launcher);
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
        // The jailer of a test uses the cgroup root of its config.
        let mounts = match &config.confinement {
            Confinement::Jailer(jail) => {
                // The root lists the controllers that a host has.
                let controllers = jail.cgroup_root.join("cgroup.controllers");
                if !controllers.exists() {
                    std::fs::create_dir_all(&jail.cgroup_root).unwrap();
                    std::fs::write(&controllers, "cpuset cpu io memory hugetlb pids\n").unwrap();
                }
                format!("cgroup2 {} cgroup2 rw 0 0\n", jail.cgroup_root.display())
            }
            Confinement::Unjailed { .. } => String::new(),
        };
        // A test cannot make a namespace handle without root.
        FirecrackerLauncher::new_on_host(config, as_root(), &mounts, |_| Ok(()))
    }

    /// A root worker with no hard rlimit ceiling, so a test does not depend
    /// on the limits of the test process.
    const fn as_root() -> Worker {
        Worker {
            euid: 0,
            hard: super::super::jailer::HardCeilings {
                nofile: None,
                fsize: None,
            },
        }
    }

    fn jailed(dir: &Path) -> LauncherConfig {
        let mut jail = JailerConfig::new(script(dir, "jailer", "exec sleep 60"), 12_345, 12_345);
        jail.chroot_base = dir.join("jail");
        jail.cgroup_root = dir.join("cgroup");
        // The root lists the controllers that a host has.
        std::fs::create_dir_all(&jail.cgroup_root).unwrap();
        std::fs::write(
            jail.cgroup_root.join("cgroup.controllers"),
            "cpuset cpu io memory hugetlb pids\n",
        )
        .unwrap();
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
    fn a_guest_work_size_below_the_policy_inputs_is_a_config_error() {
        use crate::sandbox::firecracker::backend::{BackendSettings, FirecrackerBackend};
        use crate::sandbox::policy::SandboxPolicy;
        use crate::sandbox::runtime::SandboxRuntime;
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        // The default policy takes 1 MiB of input in up to 64 files.
        config
            .image
            .extra_boot_args
            .push("harvest.work_mib=1".into());
        let launcher = Arc::new(new_launcher(config).unwrap());
        let backend = FirecrackerBackend::new(launcher, BackendSettings::default()).unwrap();
        let err = SandboxRuntime::new(Arc::new(backend), SandboxPolicy::default())
            .expect_err("a 1 MiB work tmpfs");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("/work")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_cgroup_root_without_the_launcher_controllers_is_a_config_error() {
        // The jailer refuses a key whose controller the root does not list.
        let dir = temp_dir();
        let config = jailed(&dir);
        std::fs::create_dir_all(dir.join("cgroup")).unwrap();
        std::fs::write(dir.join("cgroup/cgroup.controllers"), "hugetlb\n").unwrap();
        let err = new_launcher(config).expect_err("no cpu, memory or pids");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("controller")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_guest_memory_that_firecracker_cannot_map_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config.machine.mem_size_mib = u32::MAX;
        let err = new_launcher(config).expect_err("4 PiB of guest memory");
        assert!(matches!(err, SandboxError::Config(_)), "{err:?}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_api_timeout_with_no_bound_keeps_a_finite_launch_budget() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::Api);
        config.api_timeout = Duration::MAX;
        let launcher = new_launcher(config).unwrap();
        assert!(
            launcher.launch_budget() <= 2 * crate::sandbox::MAX_TIMEOUT,
            "{:?}",
            launcher.launch_budget()
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_jail_base_on_a_nodev_or_noexec_mount_is_a_config_error() {
        let base = Path::new("/srv/jail");
        for flags in [libc::ST_NODEV, libc::ST_NOEXEC] {
            assert!(matches!(
                check_mount_flags(base, flags, true),
                Err(SandboxError::Config(_))
            ));
        }
        // An unjailed launcher runs no device node from its root.
        check_mount_flags(base, libc::ST_NODEV, false).unwrap();
        assert!(check_mount_flags(base, libc::ST_NOEXEC, false).is_err());
        check_mount_flags(base, libc::ST_NOSUID, true).unwrap();
    }

    #[test]
    fn the_launcher_reports_the_memory_a_run_can_use() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config.machine = MachineSpec {
            vcpu_count: 1,
            mem_size_mib: 512,
        };
        let launcher = new_launcher(config).unwrap();
        assert_eq!(launcher.memory_capacity(), Some(384 << 20));
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
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
            // The guest kernel and agent keep this much, so a run gets none.
            MachineSpec {
                vcpu_count: 1,
                mem_size_mib: super::super::config::GUEST_MEMORY_RESERVE_MIB,
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
    fn a_firecracker_name_that_the_jail_root_holds_is_a_config_error() {
        // The jailer copies the binary to `<chroot>/<file name>`, over the
        // kernel, the root file system or a directory of its own.
        for name in ["vmlinux", "rootfs.ext4", "vm-config.json", "dev", "run"] {
            let dir = temp_dir();
            let bin = dir.join("bin");
            std::fs::create_dir(&bin).unwrap();
            let mut config = jailed(&dir);
            config.firecracker_bin = script(&bin, name, "exit 0");
            let err = new_launcher(config).expect_err("a colliding name");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("jail root")),
                "{name}: {err:?}"
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test]
    async fn an_oversized_vmm_binary_is_a_config_error_and_is_not_copied() {
        let dir = temp_dir();
        // A path that names another large file, such as an image.
        let big = dir.join("firecracker");
        let file = std::fs::File::create(&big).unwrap();
        file.set_len(MAX_VMM_BINARY_BYTES + 1).unwrap();
        drop(file);
        std::fs::set_permissions(&big, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        let refused = |err: &SandboxError| matches!(err, SandboxError::Config(m) if m.contains("bytes") && m.contains("binary"));
        match new_launcher(unjailed(&dir, big, BootMethod::ConfigFile)) {
            Err(err) => assert!(refused(&err), "{err:?}"),
            Ok(launcher) => {
                let err = launcher.check_versions().await.map(|_| ()).unwrap_err();
                assert!(refused(&err), "{err:?}");
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_vmm_binary_that_grows_past_the_bound_is_refused_at_the_check() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo 'Firecracker v1.17.0'");
        let launcher = new_launcher(unjailed(&dir, fc.clone(), BootMethod::ConfigFile)).unwrap();
        // A deploy puts a large file at the path after the launcher starts.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fc)
            .unwrap()
            .set_len(MAX_VMM_BINARY_BYTES + 1)
            .unwrap();
        let err = launcher.check_versions().await.map(|_| ()).unwrap_err();
        assert!(
            matches!(&err, SandboxError::Config(m) if m.contains("bytes") && m.contains("binary")),
            "{err:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_vmm_file_limit_above_the_worker_hard_limit_is_a_config_error() {
        // Without CAP_SYS_RESOURCE, the jailer cannot raise the hard limit
        // that it inherits, so each launch would fail.
        let dir = temp_dir();
        let worker = Worker {
            euid: 0,
            hard: super::super::jailer::HardCeilings {
                nofile: Some(1024),
                fsize: None,
            },
        };
        // The jailer sets 2048 files when the config gives no limit.
        for limit in [Some(1025), None] {
            let mut config = jailed(&dir);
            let mounts = match &mut config.confinement {
                Confinement::Jailer(jail) => {
                    jail.nofile_limit = limit;
                    format!("cgroup2 {} cgroup2 rw 0 0\n", jail.cgroup_root.display())
                }
                Confinement::Unjailed { .. } => unreachable!(),
            };
            let err = FirecrackerLauncher::new_on_host(config, worker, &mounts, |_| Ok(()))
                .map(|_| ())
                .expect_err("the jailer cannot raise its hard limit");
            assert!(
                matches!(&err, SandboxError::Config(m) if m.contains("no-file") && m.contains("hard")),
                "{limit:?}: {err:?}"
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_sentinel_jail_identity_is_a_config_error() {
        // `u32::MAX` is `(uid_t)-1`, which setuid and setgid refuse.
        for (uid, gid) in [(u32::MAX, 12_345), (12_345, u32::MAX)] {
            let dir = temp_dir();
            let mut config = jailed(&dir);
            if let Confinement::Jailer(jail) = &mut config.confinement {
                jail.uid = uid;
                jail.gid = gid;
            }
            let err = new_launcher(config).expect_err("sentinel jail identity");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("u32::MAX")),
                "{err:?}"
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
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
        // The pinned image and the checked binaries are not jails.
        let pinned = vm_root.join(format!("hsbx-{}-image", launcher.owner()));
        let jails = || -> Vec<PathBuf> {
            std::fs::read_dir(&vm_root)
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.is_dir() && *path != pinned)
                .filter(|path| !path.to_string_lossy().ends_with(VMM_COPIES_SUFFIX))
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
    fn a_relative_launcher_path_is_a_config_error() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        // A relative path under the git-ignored target directory.
        let relative = PathBuf::from(format!("target/hl-rel-{}", uuid::Uuid::new_v4().simple()));
        let mut config = unjailed(&dir, fc.clone(), BootMethod::ConfigFile);
        config.confinement = Confinement::Unjailed {
            work_root: relative.clone(),
        };
        let result = new_launcher(config).map(|_| ());
        let _ = std::fs::remove_dir_all(&relative);
        let err = result.expect_err("a relative work root");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("absolute")),
            "{err:?}"
        );
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        config.image.kernel = PathBuf::from("vmlinux");
        let err = new_launcher(config)
            .map(|_| ())
            .expect_err("a relative kernel");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("absolute")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_launcher_path_that_is_not_utf8_is_a_config_error() {
        use std::os::unix::ffi::OsStrExt as _;
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let mut config = unjailed(&dir, fc, BootMethod::ConfigFile);
        // Firecracker takes each path as a JSON string, so a lossy
        // conversion would name another file.
        let kernel = dir.join(std::ffi::OsStr::from_bytes(b"vmlinux-\xff"));
        std::fs::copy(&config.image.kernel, &kernel).unwrap();
        config.image.kernel = kernel;
        let err = new_launcher(config)
            .map(|_| ())
            .expect_err("a kernel path that is not UTF-8");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("UTF-8")),
            "{err:?}"
        );
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
    fn a_cgroup_root_that_the_jailer_does_not_use_is_a_config_error() {
        let dir = temp_dir();
        let err = FirecrackerLauncher::new_on_host(
            jailed(&dir),
            as_root(),
            "cgroup2 /sys/fs/cgroup cgroup2 rw 0 0\n",
            |_| Ok(()),
        )
        .expect_err("another cgroup root");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("cgroup")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn only_a_network_namespace_handle_passes_the_netns_check() {
        check_netns(Path::new("/proc/self/ns/net")).unwrap();
        let dir = temp_dir();
        let file = dir.join("netns-file");
        std::fs::write(&file, b"").unwrap();
        for path in [
            Path::new("/proc/self/ns/mnt"),
            file.as_path(),
            &dir.join("missing"),
        ] {
            let err = check_netns(path).expect_err("not a network namespace");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("network namespace")),
                "{}: {err:?}",
                path.display()
            );
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_netns_that_is_not_a_namespace_is_a_config_error() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let file = dir.join("netns-file");
        std::fs::write(&file, b"").unwrap();
        let mounts = match &mut config.confinement {
            Confinement::Jailer(jail) => {
                jail.netns = Some(file);
                format!("cgroup2 {} cgroup2 rw 0 0\n", jail.cgroup_root.display())
            }
            Confinement::Unjailed { .. } => unreachable!(),
        };
        // The jailer joins it with setns, so a plain file fails each launch.
        let err = FirecrackerLauncher::new_on_host(config, as_root(), &mounts, check_netns)
            .expect_err("a plain file");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("network namespace")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// When set, a umask test runs in this directory. Its parent run sets the
    /// variable and the umask.
    const UMASK_CHILD: &str = "HARVEST_TEST_UMASK_CHILD";

    /// Run the test `name` again in a child under `umask`, and return `None`.
    /// In that child, return the directory of the test. The umask is per
    /// process, so only a child can change it.
    fn in_child_with_umask(umask: &str, name: &str) -> Option<PathBuf> {
        if let Some(dir) = std::env::var_os(UMASK_CHILD) {
            return Some(PathBuf::from(dir));
        }
        let dir = temp_dir();
        let status = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!("umask {umask} && exec \"$0\" \"$@\""))
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", name])
            .env(UMASK_CHILD, &dir)
            .status()
            .unwrap();
        assert!(status.success(), "{status}");
        std::fs::remove_dir_all(dir).unwrap();
        None
    }

    #[test]
    fn copies_keep_their_mode_under_a_strict_umask() {
        let Some(dir) = in_child_with_umask(
            "077",
            "sandbox::firecracker::launcher::tests::copies_keep_their_mode_under_a_strict_umask",
        ) else {
            return;
        };
        let fc = script(&dir, "firecracker", "exit 0");
        let ids = [file_id(&fc).unwrap()];
        let copies = copy_binaries(&dir.join("copies"), &[("firecracker", fc)], &ids).unwrap();
        // The jail uid runs the jailer's copy, which keeps this mode.
        let mode = std::fs::metadata(&copies[0]).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o555, "{mode:o}");
    }

    #[test]
    fn the_vm_config_is_readable_under_a_strict_umask() {
        let Some(dir) = in_child_with_umask(
            "077",
            "sandbox::firecracker::launcher::tests::the_vm_config_is_readable_under_a_strict_umask",
        ) else {
            return;
        };
        let document = vm_config(
            &image(&dir),
            MachineSpec::default(),
            &VmPaths::jailed(),
            GUEST_AGENT_PORT,
        );
        let path = dir.join(CONFIG_FILE);
        write_config(&path, &document).unwrap();
        // The jailed Firecracker reads the file as the jail uid.
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o644, "{mode:o}");
    }

    #[test]
    fn a_new_vm_root_ignores_a_permissive_umask() {
        let Some(dir) = in_child_with_umask(
            "002",
            "sandbox::firecracker::launcher::tests::a_new_vm_root_ignores_a_permissive_umask",
        ) else {
            return;
        };
        let config = jailed(&dir);
        // Only the VM root may come from the umask, so the files lose group
        // write.
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let mut perms = std::fs::metadata(&path).unwrap().permissions();
            perms.set_mode(perms.mode() & !0o022);
            std::fs::set_permissions(&path, perms).unwrap();
        }
        drop(new_launcher(config).unwrap());
        let mode = std::fs::metadata(dir.join("jail/firecracker"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o022, 0, "{mode:o}");
    }

    #[test]
    fn a_jailed_launcher_needs_root() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let mounts = match &mut config.confinement {
            Confinement::Jailer(jail) => {
                // A limit above the hard limit of a non-root worker does not
                // hide the real cause.
                jail.nofile_limit = Some(4096);
                format!("cgroup2 {} cgroup2 rw 0 0\n", jail.cgroup_root.display())
            }
            Confinement::Unjailed { .. } => unreachable!(),
        };
        let worker = Worker {
            euid: 1000,
            hard: super::super::jailer::HardCeilings {
                nofile: Some(1024),
                fsize: None,
            },
        };
        let err = FirecrackerLauncher::new_on_host(config, worker, &mounts, |_| Ok(()))
            .expect_err("the jailer needs root");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("needs root")),
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
    fn a_vm_root_that_others_can_write_is_a_config_error() {
        let dir = temp_dir();
        let config = jailed(&dir);
        // A sticky ancestor that all can write, such as /tmp, stays allowed.
        let base = dir.join("jail");
        std::fs::create_dir_all(base.join("firecracker")).unwrap();
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o1777)).unwrap();
        drop(new_launcher(config.clone()).unwrap());
        // A local user could add a fake VM directory to the root itself.
        // The sweep would then kill the cgroup of that name.
        std::fs::set_permissions(
            base.join("firecracker"),
            std::fs::Permissions::from_mode(0o1777),
        )
        .unwrap();
        let err = new_launcher(config).expect_err("a VM root that others can write");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("group or others")),
            "{err:?}"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_image_on_another_file_system_is_a_config_error() {
        use std::os::unix::fs::MetadataExt as _;
        let dir = temp_dir();
        let mut config = jailed(&dir);
        // tmpfs at /dev/shm is a file system of its own on Linux.
        let other = Path::new("/dev/shm");
        let same_fs = std::fs::metadata(other)
            .is_ok_and(|meta| meta.dev() == std::fs::metadata(&dir).unwrap().dev());
        if !other.is_dir() || same_fs {
            std::fs::remove_dir_all(dir).unwrap();
            return;
        }
        let kernel = other.join(format!("hl-vmlinux-{}", uuid::Uuid::new_v4().simple()));
        std::fs::copy(&config.image.kernel, &kernel).unwrap();
        std::fs::set_permissions(&kernel, std::fs::Permissions::from_mode(0o644)).unwrap();
        config.image.kernel = kernel.clone();
        let result = new_launcher(config);
        let _ = std::fs::remove_file(&kernel);
        let err = result.expect_err("a kernel on another file system");
        assert!(
            matches!(err, SandboxError::Config(ref m) if m.contains("file system")),
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
        // The dead owner left its lease under this VM root.
        std::fs::write(launcher.vm_root().join("hsbx-deadbeef.lease"), b"").unwrap();
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

    /// The cgroup parent of the jailed test config in `dir`.
    fn cgroup_parent(config: &LauncherConfig) -> PathBuf {
        let Confinement::Jailer(jail) = &config.confinement else {
            unreachable!("a jailed config");
        };
        jail.cgroup_root.join(&jail.cgroup_parent)
    }

    /// A fake cgroup of `owner` in `parent`, with an empty kill file.
    fn fake_cgroup(parent: &Path, owner: &str, tag: char) -> PathBuf {
        let cgroup = parent.join(format!("hsbx-{owner}-{}", tag.to_string().repeat(16)));
        std::fs::create_dir_all(&cgroup).unwrap();
        std::fs::write(cgroup.join("cgroup.kill"), b"").unwrap();
        std::fs::write(cgroup.join("cgroup.events"), b"populated 0\n").unwrap();
        cgroup
    }

    #[test]
    fn the_sweep_leaves_the_cgroups_of_a_launcher_with_another_vm_root() {
        let dir = temp_dir();
        let config = jailed(&dir);
        // Another launcher shares the cgroup parent, but its lease is under
        // another VM root.
        let other = fake_cgroup(&cgroup_parent(&config), "0badf00d", 'a');
        let launcher = new_launcher(config).unwrap();
        launcher.sweep_stale();
        assert_eq!(
            std::fs::read(other.join("cgroup.kill")).unwrap(),
            b"",
            "the sweep killed a cgroup of another VM root"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_vm_directory_that_teardown_cannot_remove_is_tried_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        // A file in place of the VM directory makes the removal fail.
        let vm_dir = launcher.vm_root().join(new_vm_id(launcher.owner()));
        std::fs::write(&vm_dir, b"").unwrap();
        let mut command = tokio::process::Command::new("sleep");
        command.arg("30");
        let prepared = Prepared {
            vsock: dir.join("v.sock"),
            vm_dir: vm_dir.clone(),
            api_socket: None,
            cgroup: None,
            command,
            vmm: None,
        };
        let mut vm =
            RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy)).unwrap();
        vm.teardown().await;
        drop(vm);
        // The sweep skips the VMs of a live owner, so only the launcher
        // can try the directory again.
        std::fs::remove_file(&vm_dir).unwrap();
        std::fs::create_dir(&vm_dir).unwrap();
        std::fs::write(vm_dir.join("file"), b"left").unwrap();
        launcher.sweep_stale();
        assert!(
            !vm_dir.exists(),
            "the launcher did not try the directory again"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Make the removal of `dir` fail, then let a sweep try it again.
    /// Return whether the sweep removed it.
    fn swept_again(launcher: &FirecrackerLauncher, dir: &Path, drop_owner: impl FnOnce()) -> bool {
        // A file in place of the directory makes the removal fail.
        std::fs::write(dir, b"").unwrap();
        drop_owner();
        std::fs::remove_file(dir).unwrap();
        std::fs::create_dir(dir).unwrap();
        std::fs::write(dir.join("file"), b"left").unwrap();
        launcher.sweep_stale();
        !dir.exists()
    }

    #[test]
    fn vmm_copies_that_cannot_be_removed_are_tried_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let copies = launcher.vm_root().join(format!(
            "{}{VMM_COPIES_SUFFIX}",
            new_vm_id(launcher.owner())
        ));
        let vmm = CheckedVmm {
            dir: copies.clone(),
            firecracker: PathBuf::new(),
            jailer: None,
            version: (1, 15, 0),
            sources: Vec::new(),
            tenancy: Arc::clone(&launcher.tenancy),
        };
        // The sweep skips the directories of a live owner.
        assert!(
            swept_again(&launcher, &copies, || drop(vmm)),
            "the launcher did not try the copies again"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn the_vm_directory_of_a_failed_spawn_is_tried_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let vm_dir = launcher.vm_root().join(new_vm_id(launcher.owner()));
        let swept = swept_again(&launcher, &vm_dir, || {
            let prepared = Prepared {
                vsock: dir.join("v.sock"),
                vm_dir: vm_dir.clone(),
                api_socket: None,
                cgroup: None,
                command: tokio::process::Command::new(dir.join("missing")),
                vmm: None,
            };
            let spawned =
                RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy));
            assert!(spawned.is_err(), "a missing VMM must not start");
        });
        assert!(swept, "the launcher did not try the directory again");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_abandoned_jail_that_cannot_be_removed_is_tried_again() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let jail = launcher.vm_root().join(new_vm_id(launcher.owner()));
        let guard = JailGuard::new(jail.clone(), Arc::clone(&launcher.tenancy));
        assert!(
            swept_again(&launcher, &jail, || drop(guard)),
            "the launcher did not try the jail again"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Whether the process `pid` runs. A zombie is dead.
    fn is_alive(pid: i32) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| !rest.starts_with('Z') && !rest.starts_with('X'))
        })
    }

    #[tokio::test]
    async fn an_unjailed_teardown_kills_each_process_of_the_vmm_group() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "exit 0");
        let launcher = new_launcher(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let vm_dir = dir.join("vm");
        std::fs::create_dir_all(&vm_dir).unwrap();
        // A wrapper starts a helper in its group, then exits.
        let pid_file = dir.join("helper.pid");
        let mut command = tokio::process::Command::new("sh");
        command
            .arg("-c")
            .arg(format!("sleep 30 & echo $! > {}", pid_file.display()));
        let prepared = Prepared {
            vsock: vm_dir.join("v.sock"),
            vm_dir,
            api_socket: None,
            cgroup: None,
            command,
            vmm: None,
        };
        let mut vm =
            RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while vm.exit_status().is_none() {
            assert!(Instant::now() < deadline, "the wrapper did not exit");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let helper: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(is_alive(helper), "the helper must outlive the wrapper");
        vm.teardown().await;
        let deadline = Instant::now() + Duration::from_secs(2);
        while is_alive(helper) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            !is_alive(helper),
            "the teardown left a process of the VMM group"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_vmm_file_limit_too_small_is_a_config_error() {
        let dir = temp_dir();
        let with_limit = |limit: Option<u64>| {
            let mut config = jailed(&dir);
            let Confinement::Jailer(jail) = &mut config.confinement else {
                unreachable!("a jailed config");
            };
            jail.nofile_limit = limit;
            config
        };
        // The VMM could not open its image, its sockets or its vCPUs.
        for limit in [0, super::super::jailer::MIN_NOFILE_LIMIT - 1] {
            let err = new_launcher(with_limit(Some(limit))).expect_err("too few files");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("no-file")),
                "{limit}: {err:?}"
            );
        }
        drop(new_launcher(with_limit(Some(super::super::jailer::MIN_NOFILE_LIMIT))).unwrap());
        drop(new_launcher(with_limit(None)).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_vmm_file_limit_above_the_host_ceiling_is_a_config_error() {
        let dir = temp_dir();
        let with_limit = |limit: u64| {
            let mut config = jailed(&dir);
            let Confinement::Jailer(jail) = &mut config.confinement else {
                unreachable!("a jailed config");
            };
            jail.nofile_limit = Some(limit);
            config
        };
        // Linux refuses an `RLIMIT_NOFILE` above `fs.nr_open`, even to root.
        let nr_open: u64 = std::fs::read_to_string("/proc/sys/fs/nr_open")
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        for limit in [nr_open + 1, u64::MAX] {
            let err = new_launcher(with_limit(limit)).expect_err("over fs.nr_open");
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("nr_open")),
                "{limit}: {err:?}"
            );
        }
        drop(new_launcher(with_limit(nr_open)).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_cgroup_scan_that_fails_keeps_the_dead_leases() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let parent = cgroup_parent(&config);
        let launcher = new_launcher(config).unwrap();
        let lease = launcher.vm_root().join("hsbx-deadbeef.lease");
        std::fs::write(&lease, b"").unwrap();
        // A file in place of the cgroup parent makes the scan fail.
        let _ = std::fs::remove_dir_all(&parent);
        std::fs::create_dir_all(parent.parent().unwrap()).unwrap();
        std::fs::write(&parent, b"").unwrap();
        launcher.sweep_stale();
        assert!(lease.exists(), "a failed scan dropped a dead lease");
        // A cgroup parent that is absent holds no cgroup.
        std::fs::remove_file(&parent).unwrap();
        launcher.sweep_stale();
        assert!(!lease.exists(), "the sweep kept the lease of a dead owner");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_dead_lease_stays_until_its_cgroups_are_gone() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let dead = fake_cgroup(&cgroup_parent(&config), "deadbeef", 'b');
        let launcher = new_launcher(config).unwrap();
        let lease = launcher.vm_root().join("hsbx-deadbeef.lease");
        std::fs::write(&lease, b"").unwrap();
        launcher.sweep_stale();
        assert_eq!(std::fs::read(dead.join("cgroup.kill")).unwrap(), b"1");
        assert!(lease.exists(), "the lease went before its cgroup");
        std::fs::remove_dir_all(&dead).unwrap();
        launcher.sweep_stale();
        assert!(!lease.exists(), "the sweep kept the lease of a dead owner");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_launcher_dropped_with_leftover_cgroups_keeps_its_lease() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let parent = cgroup_parent(&config);
        let launcher = new_launcher(config).unwrap();
        let left = fake_cgroup(&parent, launcher.owner(), 'c');
        launcher.tenancy.keep_if_left(&left);
        let lease = launcher.lease_path().to_path_buf();
        drop(launcher);
        assert!(lease.exists(), "the lease went before a leftover cgroup");
        // The next launcher sees a dead owner and kills its cgroup.
        std::fs::write(left.join("cgroup.kill"), b"").unwrap();
        let sweeper = new_launcher(jailed(&dir)).unwrap();
        sweeper.sweep_stale();
        assert_eq!(std::fs::read(left.join("cgroup.kill")).unwrap(), b"1");
        drop(sweeper);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_cgroup_that_cannot_be_checked_is_kept() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        // A path below a file cannot be checked, so it may still be there.
        let file = dir.join("file");
        std::fs::write(&file, b"").unwrap();
        let unknown = file.join("cgroup");
        launcher.tenancy.keep_if_left(&unknown);
        assert!(!launcher.tenancy.is_empty(), "the cgroup was not kept");
        launcher.tenancy.retry();
        assert!(!launcher.tenancy.is_empty(), "the retry dropped the cgroup");
        let lease = launcher.lease_path().to_path_buf();
        drop(launcher);
        assert!(lease.exists(), "the lease went before the cgroup");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_vm_that_outlives_its_launcher_keeps_the_lease() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let parent = cgroup_parent(&config);
        let launcher = new_launcher(config).unwrap();
        // A fake cgroup holds files, so its removal fails.
        let left = fake_cgroup(&parent, launcher.owner(), 'd');
        let vm_dir = dir.join("vm");
        std::fs::create_dir_all(&vm_dir).unwrap();
        let mut command = tokio::process::Command::new("sleep");
        command.arg("30");
        let prepared = Prepared {
            vsock: vm_dir.join("v.sock"),
            vm_dir,
            api_socket: None,
            cgroup: Some(left.clone()),
            command,
            vmm: None,
        };
        let mut vm =
            RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy)).unwrap();
        let lease = launcher.lease_path().to_path_buf();
        // A detached teardown can run after the launcher is gone.
        drop(launcher);
        assert!(
            lease.exists(),
            "the launcher removed the lease of a live VM"
        );
        vm.teardown().await;
        drop(vm);
        assert!(lease.exists(), "the lease went before a leftover cgroup");
        // The next launcher sees a dead owner and kills its cgroup.
        std::fs::write(left.join("cgroup.kill"), b"").unwrap();
        let sweeper = new_launcher(jailed(&dir)).unwrap();
        sweeper.sweep_stale();
        assert_eq!(std::fs::read(left.join("cgroup.kill")).unwrap(), b"1");
        drop(sweeper);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_nul_in_the_vmm_stderr_does_not_reach_the_report() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let vm_dir = dir.join("vm");
        std::fs::create_dir_all(&vm_dir).unwrap();
        let mut command = tokio::process::Command::new("sh");
        command.args(["-c", "printf 'boom\\000bad' >&2; exit 3"]);
        let prepared = Prepared {
            vsock: vm_dir.join("v.sock"),
            vm_dir,
            api_socket: None,
            cgroup: None,
            command,
            vmm: None,
        };
        let mut vm =
            RunningVm::spawn("hsbx-test".into(), prepared, Arc::clone(&launcher.tenancy)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let report = loop {
            if let Some(report) = vm.exit_status() {
                break report;
            }
            assert!(Instant::now() < deadline, "the VMM did not exit");
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(report.contains("boom"), "{report:?}");
        assert!(!report.contains('\0'), "{report:?}");
        vm.teardown().await;
        drop((vm, launcher));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Replace `path` by a rename, as `publish-image.sh` does, and return
    /// the old bytes.
    fn publish_over(path: &Path, bytes: &[u8]) -> Vec<u8> {
        let old = std::fs::read(path).unwrap();
        let next = path.with_extension("next");
        std::fs::write(&next, bytes).unwrap();
        std::fs::rename(&next, path).unwrap();
        old
    }

    #[tokio::test]
    async fn a_launch_keeps_the_image_of_the_launcher_start() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let (kernel, rootfs) = (config.image.kernel.clone(), config.image.rootfs.clone());
        let launcher = new_launcher(config).unwrap();
        // A new image lands one file at a time while the launcher runs.
        let old_kernel = publish_over(&kernel, b"new kernel");
        let old_rootfs = publish_over(&rootfs, b"new rootfs");
        let id = new_vm_id(launcher.owner());
        let vmm = launcher.check_versions().await.unwrap();
        let prepared = launcher
            .prepare(&id, &BootMethod::ConfigFile, &vmm)
            .await
            .unwrap();
        let root = prepared.vm_dir.join("root");
        assert_eq!(std::fs::read(root.join(KERNEL_FILE)).unwrap(), old_kernel);
        assert_eq!(std::fs::read(root.join(ROOTFS_FILE)).unwrap(), old_rootfs);
        drop(prepared);
        // The pinned image goes with the launcher and the last of its copies.
        let pinned = launcher
            .vm_root()
            .join(format!("hsbx-{}-image", launcher.owner()));
        assert!(pinned.is_dir(), "{}", pinned.display());
        drop(vmm);
        drop(launcher);
        assert!(!pinned.exists(), "the launcher left its pinned image");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_write_in_place_after_the_pin_fails_each_launch() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let kernel = config.image.kernel.clone();
        // The image is old, as at a worker start.
        let old = std::time::SystemTime::now() - Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(&kernel)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let size = std::fs::metadata(&kernel).unwrap().len();
        let launcher = new_launcher(config).unwrap();
        // `cp` onto the kernel writes the pinned inode, with the same size.
        let bytes = vec![b'x'; usize::try_from(size).unwrap()];
        std::fs::write(&kernel, &bytes).unwrap();
        let id = new_vm_id(launcher.owner());
        let vmm = launcher.check_versions().await.unwrap();
        let err = launcher
            .prepare(&id, &BootMethod::ConfigFile, &vmm)
            .await
            .map(|_| ())
            .expect_err("the pinned kernel changed in place");
        assert!(
            matches!(&err, SandboxError::Config(m) if m.contains("in place")),
            "{err:?}"
        );
        drop((vmm, launcher));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_write_in_place_that_keeps_the_stamp_fails_each_launch() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let rootfs = config.image.rootfs.clone();
        let launcher = new_launcher(config).unwrap();
        // `cp -p` of a new image with the same size and modification time,
        // as two images from the Nix store have.
        let old = std::fs::metadata(&rootfs).unwrap();
        let stamp = FileStamp::of(&rootfs).unwrap();
        let bytes = vec![b'x'; usize::try_from(old.len()).unwrap()];
        std::fs::write(&rootfs, &bytes).unwrap();
        std::fs::File::options()
            .write(true)
            .open(&rootfs)
            .unwrap()
            .set_modified(old.modified().unwrap())
            .unwrap();
        // The stamp check passes, so only the watch can refuse the launch.
        assert_eq!(FileStamp::of(&rootfs).unwrap(), stamp);
        let vmm = launcher.check_versions().await.unwrap();
        // The watch keeps the write, so each later launch fails too.
        for _ in 0..2 {
            let id = new_vm_id(launcher.owner());
            let err = launcher
                .prepare(&id, &BootMethod::ConfigFile, &vmm)
                .await
                .map(|_| ())
                .expect_err("the pinned root file system changed in place");
            assert!(
                matches!(&err, SandboxError::Config(m) if m.contains("had a write in place")),
                "{err:?}"
            );
        }
        drop((vmm, launcher));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_checks_after_a_write_both_see_it() {
        let dir = temp_dir();
        let file = dir.join("image");
        std::fs::write(&file, b"image").unwrap();
        // The read takes the event from the queue. A second check must not
        // pass before the first one sets the flag. The race lost about one
        // round in 30, and closing a watch takes a few milliseconds.
        for _ in 0..300 {
            let watch = WriteWatch::new(&[file.as_path()]).unwrap();
            std::fs::write(&file, b"IMAGE").unwrap();
            let barrier = std::sync::Barrier::new(2);
            let check = || {
                barrier.wait();
                watch.written()
            };
            let (a, b) = std::thread::scope(|scope| {
                let a = scope.spawn(check);
                let b = scope.spawn(check);
                (a.join().unwrap(), b.join().unwrap())
            });
            assert!(a && b, "a check passed after a write: {a} {b}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_watch_knows_a_hard_link_of_a_watched_file_from_another_file() {
        let dir = temp_dir();
        let (file, link, other) = (dir.join("a"), dir.join("b"), dir.join("c"));
        std::fs::write(&file, b"image").unwrap();
        std::fs::hard_link(&file, &link).unwrap();
        std::fs::write(&other, b"image").unwrap();
        let watch = WriteWatch::new(&[file.as_path()]).unwrap();
        assert!(watch.watches(&[link.as_path()]).unwrap());
        assert!(!watch.watches(&[other.as_path()]).unwrap());
        assert!(!watch.watches(&[]).unwrap());
        assert!(!watch.written());
        std::fs::write(&link, b"IMAGE").unwrap();
        assert!(watch.written());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_write_during_the_pin_that_keeps_the_stamp_fails_the_pin() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let rootfs = config.image.rootfs.clone();
        let vm_root = dir.join("pins");
        std::fs::create_dir(&vm_root).unwrap();
        // The write lands after the stamps, while the pin checks and links.
        let err = PinnedImage::pin_with(&config, &vm_root, "owner", || {
            let old = std::fs::metadata(&rootfs).unwrap();
            let bytes = vec![b'x'; usize::try_from(old.len()).unwrap()];
            std::fs::write(&rootfs, &bytes).unwrap();
            std::fs::File::options()
                .write(true)
                .open(&rootfs)
                .unwrap()
                .set_modified(old.modified().unwrap())
                .unwrap();
        })
        .map(|_| ())
        .expect_err("the write keeps the stamp, but the watch sees it");
        assert!(
            matches!(&err, SandboxError::Config(m) if m.contains("in place")),
            "{err:?}"
        );
        assert_eq!(
            std::fs::read_dir(&vm_root).unwrap().count(),
            0,
            "the pin left files"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn launches_that_link_the_pinned_image_do_not_trip_the_write_check() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let vmm = launcher.check_versions().await.unwrap();
        // Each launch links the pinned files into its jail, and its teardown
        // unlinks them. Neither counts as a write.
        for _ in 0..3 {
            let id = new_vm_id(launcher.owner());
            let prepared = launcher
                .prepare(&id, &BootMethod::ConfigFile, &vmm)
                .await
                .unwrap();
            let jail = prepared.vm_dir.clone();
            drop(prepared);
            crate::sandbox::exec::remove_work_dir(&jail).unwrap();
            assert!(!jail.exists(), "{}", jail.display());
        }
        launcher.tenancy.pinned.as_ref().unwrap().check().unwrap();
        drop((vmm, launcher));
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Lock `dir` with `op`, as a publish or a pin does.
    fn hold_lock(dir: &Path, op: libc::c_int) -> std::fs::File {
        use std::os::fd::AsRawFd as _;
        let file = std::fs::File::open(dir).unwrap();
        // SAFETY: flock reads no memory, and the descriptor is open.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), op) }, 0);
        file
    }

    #[test]
    fn a_launcher_does_not_pin_half_a_publish() {
        let dir = temp_dir();
        let config = jailed(&dir);
        let (kernel, rootfs) = (config.image.kernel.clone(), config.image.rootfs.clone());
        // A publish holds the image directory and has moved one file.
        let publish = hold_lock(&dir, libc::LOCK_EX);
        publish_over(&kernel, b"new kernel");
        let start = std::thread::spawn(move || new_launcher(config).unwrap());
        std::thread::sleep(Duration::from_millis(200));
        publish_over(&rootfs, b"new rootfs");
        drop(publish);
        let launcher = start.join().unwrap();
        let pinned = launcher.tenancy.pinned.as_ref().unwrap();
        assert_eq!(std::fs::read(&pinned.kernel).unwrap(), b"new kernel");
        assert_eq!(std::fs::read(&pinned.rootfs).unwrap(), b"new rootfs");
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_lock_that_stays_held_ends_the_wait() {
        let dir = temp_dir();
        let file = dir.join("vmlinux");
        std::fs::write(&file, b"kernel").unwrap();
        let publish = hold_lock(&dir, libc::LOCK_EX);
        let started = Instant::now();
        let err = lock_dirs(&[&file], libc::LOCK_SH, Duration::from_millis(100))
            .expect_err("the directory stays locked");
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock, "{err}");
        assert!(started.elapsed() < Duration::from_secs(2));
        drop(publish);
        drop(lock_dirs(&[&file], libc::LOCK_SH, Duration::from_millis(100)).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_snapshot_publish_waits_for_a_pin() {
        let dir = temp_dir();
        let out = dir.join("golden");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("state"), b"old state").unwrap();
        std::fs::write(out.join("memory"), b"old memory").unwrap();
        std::fs::write(dir.join("new-state"), b"new state").unwrap();
        std::fs::write(dir.join("new-memory"), b"new memory").unwrap();
        // SAFETY: getegid has no preconditions.
        let gid = unsafe { libc::getegid() };
        let copies = [
            (dir.join("new-state"), out.join("state")),
            (dir.join("new-memory"), out.join("memory")),
        ];
        let pin = hold_lock(&out, libc::LOCK_SH);
        let publish = std::thread::spawn(move || publish_snapshot(&copies, gid));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(std::fs::read(out.join("state")).unwrap(), b"old state");
        assert_eq!(std::fs::read(out.join("memory")).unwrap(), b"old memory");
        drop(pin);
        publish.join().unwrap().unwrap();
        assert_eq!(std::fs::read(out.join("state")).unwrap(), b"new state");
        assert_eq!(std::fs::read(out.join("memory")).unwrap(), b"new memory");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Write the snapshot manifest of `config` for `files`.
    fn write_snapshot_meta(config: &LauncherConfig, files: &SnapshotFiles) {
        let image = (config.image.kernel.as_path(), config.image.rootfs.as_path());
        write_meta(files, &SnapshotMeta::of(config, image, (1, 17, 0)).unwrap());
    }

    /// Write `meta` as the snapshot manifest of `files`.
    fn write_meta(files: &SnapshotFiles, meta: &SnapshotMeta) {
        let meta = serde_json::to_vec(meta).unwrap();
        std::fs::write(SnapshotMeta::path(&files.state), meta).unwrap();
    }

    #[test]
    fn a_snapshot_must_keep_the_boot_args_and_machine_of_the_launcher() {
        let dir = temp_dir();
        let files = SnapshotFiles {
            state: dir.join("snap.state"),
            memory: dir.join("snap.mem"),
        };
        std::fs::write(&files.state, b"state").unwrap();
        std::fs::write(&files.memory, b"memory").unwrap();
        let mut config = jailed(&dir);
        config.boot = BootMethod::Snapshot(files.clone());
        let refused = |what: &str| {
            let err = new_launcher(config.clone()).expect_err(what);
            assert!(
                matches!(err, SandboxError::Config(ref m) if m.contains("snapshot")),
                "{what}: {err:?}"
            );
        };
        refused("a snapshot without a manifest");
        // The guest agent of the snapshot listens on its own port.
        let mut other = config.clone();
        other.guest_port = GUEST_AGENT_PORT + 1;
        write_snapshot_meta(&other, &files);
        refused("a snapshot with another guest port");
        let mut other = config.clone();
        other.machine.mem_size_mib = 1024;
        write_snapshot_meta(&other, &files);
        refused("a snapshot with another machine");
        // The memory of the snapshot holds the kernel that booted it.
        let mut other = config.clone();
        other.image.kernel = dir.join("other-vmlinux");
        std::fs::write(&other.image.kernel, b"another kernel").unwrap();
        write_snapshot_meta(&other, &files);
        refused("a snapshot of another kernel");
        let mut other = config.clone();
        other.image.rootfs = dir.join("other-rootfs.ext4");
        std::fs::write(&other.image.rootfs, b"another rootfs").unwrap();
        write_snapshot_meta(&other, &files);
        refused("a snapshot of another root file system");
        // A snapshot holds CPU state, and the host kernel restores it.
        let image = (config.image.kernel.as_path(), config.image.rootfs.as_path());
        let meta = SnapshotMeta::of(&config, image, (1, 17, 0)).unwrap();
        write_meta(
            &files,
            &SnapshotMeta {
                cpu: "another CPU".into(),
                ..meta.clone()
            },
        );
        refused("a snapshot of another CPU model");
        write_meta(
            &files,
            &SnapshotMeta {
                host_kernel: "another kernel".into(),
                ..meta
            },
        );
        refused("a snapshot of another host kernel");
        write_snapshot_meta(&config, &files);
        drop(new_launcher(config).unwrap());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_snapshot_launch_keeps_the_pair_of_the_launcher_start() {
        let dir = temp_dir();
        let files = SnapshotFiles {
            state: dir.join("snap.state"),
            memory: dir.join("snap.mem"),
        };
        std::fs::write(&files.state, b"old state").unwrap();
        std::fs::write(&files.memory, b"old memory").unwrap();
        let mut config = jailed(&dir);
        config.boot = BootMethod::Snapshot(files.clone());
        write_snapshot_meta(&config, &files);
        let launcher = new_launcher(config).unwrap();
        // Only the memory file of a new pair has landed.
        let old_memory = publish_over(&files.memory, b"new memory");
        let id = new_vm_id(launcher.owner());
        let boot = launcher.config().boot.clone();
        let vmm = launcher.check_versions().await.unwrap();
        let prepared = launcher.prepare(&id, &boot, &vmm).await.unwrap();
        let root = prepared.vm_dir.join("root");
        assert_eq!(
            std::fs::read(root.join(SNAPSHOT_MEMORY_FILE)).unwrap(),
            old_memory
        );
        assert_eq!(
            std::fs::read(root.join(SNAPSHOT_STATE_FILE)).unwrap(),
            b"old state"
        );
        drop((prepared, launcher));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_golden_snapshot_needs_an_api_timeout() {
        // A config-file launcher may have no API timeout, but a golden
        // microVM always boots through the API.
        let dir = temp_dir();
        let mut config = jailed(&dir);
        config.boot = BootMethod::ConfigFile;
        config.api_timeout = Duration::ZERO;
        let launcher = new_launcher(config).unwrap();
        let result = launcher
            .create_golden_snapshot(&dir.join("golden"), Duration::from_secs(5))
            .await;
        assert!(
            matches!(result, Err(SandboxError::Config(ref m)) if m.contains("API timeout")),
            "{result:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_golden_snapshot_needs_room_for_the_guest_memory() {
        let dir = temp_dir();
        let mut config = jailed(&dir);
        let Confinement::Jailer(jail) = &mut config.confinement else {
            unreachable!("a jailed config");
        };
        // The memory file of a 2176 MiB guest does not fit 1 GiB.
        jail.fsize_limit = Some(1 << 30);
        let launcher = new_launcher(config).unwrap();
        let result = launcher
            .create_golden_snapshot(&dir.join("golden"), Duration::from_secs(5))
            .await;
        assert!(
            matches!(result, Err(SandboxError::Config(ref m)) if m.contains("fsize")),
            "{result:?}"
        );
        drop(launcher);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_golden_snapshot_needs_a_boot_timeout() {
        let dir = temp_dir();
        let launcher = new_launcher(jailed(&dir)).unwrap();
        let result = launcher
            .create_golden_snapshot(&dir.join("golden"), Duration::ZERO)
            .await;
        assert!(
            matches!(result, Err(SandboxError::Config(ref m)) if m.contains("boot timeout")),
            "{result:?}"
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
        let launcher = new_launcher(config.clone()).unwrap();
        let out = dir.join("golden");
        let files = launcher
            .create_golden_snapshot(&out, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(files.state, out.join("snapshot.state"));
        assert_eq!(files.memory, out.join("snapshot.mem"));
        assert_eq!(std::fs::read(&files.state).unwrap(), b"state");
        assert_eq!(std::fs::read(&files.memory).unwrap(), b"memory");
        // The manifest lands with the pair, and a restore with this config
        // passes its check.
        let meta = SnapshotMeta::check(&config, &files).unwrap();
        meta.check_firecracker((1, 17, 0)).unwrap();
        let manifest = SnapshotMeta::path(&files.state);
        for file in [&files.state, &files.memory, &manifest] {
            let meta = std::fs::metadata(file).unwrap();
            assert_eq!(meta.mode() & 0o777, 0o640, "{}", file.display());
            assert_eq!(meta.gid(), 23_456, "the jailed VMM reads it by group");
        }
        // The pinned image and the checked binaries are not jails.
        let pinned = format!("hsbx-{}-image", launcher.owner());
        let jails = std::fs::read_dir(launcher.vm_root())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()) && e.file_name() != *pinned)
            .filter(|e| !e.file_name().to_string_lossy().ends_with(VMM_COPIES_SUFFIX))
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
