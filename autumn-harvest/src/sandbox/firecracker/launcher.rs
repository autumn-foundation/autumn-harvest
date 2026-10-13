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

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::config::{GuestImage, MachineSpec};
use super::jailer::JailerConfig;
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
        let _ = (dir, owner);
        todo!("InstanceLease::acquire")
    }

    /// The lease file.
    #[must_use]
    pub fn path(&self) -> &Path {
        let _ = &self.file;
        &self.path
    }
}

impl FirecrackerLauncher {
    /// A launcher for `config`, with a new owner tag and its lease.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Config`] when a path is missing or the lease
    /// cannot be taken.
    pub fn new(config: LauncherConfig) -> Result<Self, SandboxError> {
        let _ = config;
        todo!("FirecrackerLauncher::new")
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

    /// The directory that holds the VM directories and the leases.
    #[must_use]
    pub fn vm_root(&self) -> PathBuf {
        let _ = (&self.exec_name, &self.lease);
        todo!("FirecrackerLauncher::vm_root")
    }

    /// Remove the VMs of each owner whose lease is not locked. Returns the
    /// number of VM directories removed.
    pub fn sweep_stale(&self) -> usize {
        todo!("FirecrackerLauncher::sweep_stale")
    }
}

impl VmLauncher for FirecrackerLauncher {
    fn launch(&self) -> BoxFuture<'_, Result<Box<dyn LaunchedVm>, SandboxError>> {
        todo!("FirecrackerLauncher::launch")
    }

    fn image_name(&self) -> Option<String> {
        self.config.image.name.clone()
    }
}

/// Kill every process in the cgroup at `dir`, wait for it to empty, and
/// remove it.
pub async fn kill_cgroup(dir: &Path) {
    let _ = dir;
    todo!("kill_cgroup")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;
    use std::time::Instant;

    use super::*;
    use crate::sandbox::firecracker::config::GUEST_AGENT_PORT;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("harvest-launch-{}", uuid::Uuid::new_v4()));
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
            assert!(Instant::now() < deadline, "{} never appeared", path.display());
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
        let launcher = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        let vm = launcher.launch().await.unwrap();
        let vm_dir = launcher.vm_root().join(vm.id());
        assert!(vm.id().starts_with(&format!("hsbx-{}-", launcher.owner())));
        assert_eq!(vm.vsock_path(), vm_dir.join("v.sock"));
        wait_for(&vm_dir.join("env.txt")).await;
        let args = std::fs::read_to_string(vm_dir.join("args.txt")).unwrap();
        let config_path = vm_dir.join("vm-config.json");
        assert_eq!(
            args.lines().collect::<Vec<_>>(),
            ["--id", vm.id(), "--config-file", config_path.to_str().unwrap(), "--no-api"]
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
        assert_eq!(config["vsock"]["uds_path"], vm_dir.join("v.sock").to_str().unwrap());
        vm.shutdown().await;
        assert!(!vm_dir.exists(), "teardown must remove the VM directory");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn shutdown_kills_the_vmm_process() {
        let dir = temp_dir();
        let fc = script(&dir, "firecracker", "echo $$ > pid.txt; exec sleep 60");
        let launcher = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        let launcher = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
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
        if std::process::Command::new("python3").arg("-V").output().is_err() {
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
        let log = std::fs::read_to_string(launcher.vm_root().join(vm.id()).join("requests.log"))
            .unwrap();
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
        let live = FirecrackerLauncher::new(unjailed(&dir, fc.clone(), BootMethod::ConfigFile)).unwrap();
        let root = live.vm_root();
        let live_vm = root.join(format!("hsbx-{}-{}", live.owner(), "a".repeat(32)));
        std::fs::create_dir_all(&live_vm).unwrap();
        // A dead owner with an unlocked lease, and one with no lease at all.
        std::fs::write(root.join("hsbx-deadbeef.lease"), b"").unwrap();
        let dead_vm = root.join(format!("hsbx-deadbeef-{}", "b".repeat(32)));
        std::fs::create_dir_all(dead_vm.join("root")).unwrap();
        let orphan_vm = root.join(format!("hsbx-0badc0de-{}", "c".repeat(32)));
        std::fs::create_dir_all(&orphan_vm).unwrap();
        let unrelated = root.join("keep-me");
        std::fs::create_dir_all(&unrelated).unwrap();

        let sweeper = FirecrackerLauncher::new(unjailed(&dir, fc, BootMethod::ConfigFile)).unwrap();
        assert_eq!(sweeper.sweep_stale(), 2);
        assert!(live_vm.exists(), "a live owner keeps its VMs");
        assert!(live.lease.path().exists());
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
        assert!(InstanceLease::acquire(&dir, "0123abcd").is_err(), "the lock is held");
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
        assert!(args.ends_with("--\n--config-file\nvm-config.json\n--no-api\n"), "{args}");
        use std::os::unix::fs::MetadataExt as _;
        let original = std::fs::metadata(dir.join("rootfs.ext4")).unwrap().ino();
        assert_eq!(std::fs::metadata(root.join("rootfs.ext4")).unwrap().ino(), original);
        assert!(root.join("vmlinux").exists());
        assert!(root.join("vm-config.json").exists());
        vm.shutdown().await;
        assert!(!jail_dir.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
