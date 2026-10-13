//! Tests that boot real Firecracker microVMs.
//!
//! The tests are ignored by default, because they need `/dev/kvm`, a
//! Firecracker binary and a guest image. The `Sandbox (KVM)` workflow runs
//! them. Set these variables to run them on a KVM host:
//!
//! - `HARVEST_FIRECRACKER_BIN`: the Firecracker binary.
//! - `HARVEST_GUEST_KERNEL` and `HARVEST_GUEST_ROOTFS`: the guest image from
//!   `scripts/sandbox/build-guest-image.sh`.
//! - `HARVEST_JAILER_BIN`: the jailer binary. The jailer tests also need
//!   root, and they skip without both.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::backend::{BackendSettings, FirecrackerBackend};
use super::config::{GUEST_AGENT_PORT, GuestImage, MachineSpec, VM_ID_PREFIX};
use super::jailer::JailerConfig;
use super::launcher::{BootMethod, Confinement, FirecrackerLauncher, LauncherConfig};
use super::posture::PosturePolicy;
use crate::sandbox::outcome::{RunStatus, SandboxOutcome};
use crate::sandbox::policy::{IsolationLevel, SandboxPolicy};
use crate::sandbox::request::SandboxRequest;
use crate::sandbox::runtime::SandboxRuntime;

const IGNORE: &str = "needs /dev/kvm, Firecracker and a guest image";

fn env_path(name: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("set {name}: {IGNORE}"))
}

fn short_dir() -> PathBuf {
    let dir = PathBuf::from("/tmp").join(format!(
        "hk-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).expect("a work root");
    dir
}

fn config(boot: BootMethod, confinement: Confinement) -> LauncherConfig {
    LauncherConfig {
        firecracker_bin: env_path("HARVEST_FIRECRACKER_BIN"),
        image: GuestImage::new(
            env_path("HARVEST_GUEST_KERNEL"),
            env_path("HARVEST_GUEST_ROOTFS"),
        )
        .with_name("kvm-test-image"),
        machine: MachineSpec::default(),
        boot,
        confinement,
        guest_port: GUEST_AGENT_PORT,
        api_timeout: Duration::from_secs(5),
    }
}

fn unjailed(boot: BootMethod) -> LauncherConfig {
    config(
        boot,
        Confinement::Unjailed {
            work_root: short_dir(),
        },
    )
}

/// The jailer configuration, or `None` when the test cannot use it.
fn jailed() -> Option<LauncherConfig> {
    let jailer = std::env::var_os("HARVEST_JAILER_BIN")?;
    // SAFETY: geteuid has no preconditions.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipped: the jailer needs root");
        return None;
    }
    let mut jail = JailerConfig::new(jailer, 10_000, 10_000);
    jail.chroot_base = short_dir();
    Some(config(BootMethod::ConfigFile, Confinement::Jailer(jail)))
}

struct Harness {
    launcher: Arc<FirecrackerLauncher>,
    backend: Arc<FirecrackerBackend>,
    runtime: SandboxRuntime,
}

impl Harness {
    async fn new(config: LauncherConfig, pool: usize) -> Self {
        let launcher = Arc::new(FirecrackerLauncher::new(config).expect("a launcher"));
        launcher
            .verify_host(PosturePolicy::Report)
            .await
            .expect("a supported Firecracker");
        let mut backend = FirecrackerBackend::new(launcher.clone(), BackendSettings::default());
        if pool > 0 {
            backend = backend.with_warm_pool(pool);
        }
        let backend = Arc::new(backend);
        let runtime =
            SandboxRuntime::new(backend.clone(), SandboxPolicy::default()).expect("a runtime");
        Self {
            launcher,
            backend,
            runtime,
        }
    }

    async fn run(&self, request: &SandboxRequest) -> SandboxOutcome {
        self.runtime
            .run(request, &CancellationToken::new())
            .await
            .expect("an outcome")
    }

    /// Stop the pool and wait until no VM directory is left.
    async fn assert_clean(&self) {
        self.backend.shutdown().await;
        let root = self.launcher.vm_root();
        let mut left = Vec::new();
        for _ in 0..50 {
            left = vm_dirs(&root);
            if left.is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("teardown left VM directories: {left:?}");
    }
}

fn vm_dirs(root: &Path) -> Vec<String> {
    std::fs::read_dir(root)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with(VM_ID_PREFIX))
                .collect()
        })
        .unwrap_or_default()
}

fn exited(outcome: &SandboxOutcome) {
    assert_eq!(
        outcome.status,
        RunStatus::Exited { code: 0 },
        "stdout: {}\nstderr: {}",
        outcome.stdout,
        outcome.stderr
    );
}

const PYTHON_OUTPUT: &str = "\
import os, sys
os.makedirs('out', exist_ok=True)
with open('out/sum.txt', 'w') as f:
    f.write(str(sum(map(int, sys.argv[1:]))))
print('uid', os.getuid())
";

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_microvm_runs_python_and_returns_an_output_file() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let request = SandboxRequest::new("python3", PYTHON_OUTPUT)
        .with_arg("40")
        .with_arg("2")
        .with_output("out/sum.txt");
    let outcome = harness.run(&request).await;
    exited(&outcome);
    assert_eq!(outcome.stdout, "uid 1000\n");
    assert_eq!(outcome.isolation, IsolationLevel::MicroVm);
    assert_eq!(outcome.image.as_deref(), Some("kvm-test-image"));
    assert_eq!(outcome.outputs.len(), 1, "{outcome:?}");
    assert_eq!(outcome.outputs[0].content, b"42");
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_microvm_runs_node_and_shell() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let node = harness
        .run(&SandboxRequest::new("node", "console.log(6 * 7)"))
        .await;
    exited(&node);
    assert_eq!(node.stdout, "42\n");
    let shell = harness
        .run(&SandboxRequest::new(
            "bash",
            "echo \"$((6 * 7))\" >&2; exit 3",
        ))
        .await;
    assert_eq!(shell.status, RunStatus::Exited { code: 3 });
    assert_eq!(shell.stderr, "42\n");
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_microvm_has_no_network() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let code = "\
import socket
try:
    socket.create_connection(('1.1.1.1', 53), timeout=2)
    print('connected')
except OSError as e:
    print('refused', type(e).__name__)
print(sorted(socket.if_nameindex()))
";
    let outcome = harness.run(&SandboxRequest::new("python3", code)).await;
    exited(&outcome);
    assert!(outcome.stdout.starts_with("refused"), "{}", outcome.stdout);
    assert!(
        outcome.stdout.ends_with("[(1, 'lo')]\n"),
        "{}",
        outcome.stdout
    );
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_microvm_times_out_and_is_torn_down() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let request = SandboxRequest::new("python3", "while True: pass").with_timeout_ms(2_000);
    let outcome = harness.run(&request).await;
    assert_eq!(outcome.status, RunStatus::TimedOut, "{outcome:?}");
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_cancelled_run_tears_the_microvm_down() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let cancel = CancellationToken::new();
    let request = SandboxRequest::new("sh", "sleep 60");
    let run = harness.runtime.run(&request, &cancel);
    let stop = async {
        tokio::time::sleep(Duration::from_secs(3)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(run, stop);
    assert!(
        matches!(result, Err(crate::sandbox::SandboxError::Cancelled)),
        "{result:?}"
    );
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn each_microvm_gets_fresh_entropy() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 0).await;
    let code = "import os; print(os.urandom(16).hex())";
    let first = harness.run(&SandboxRequest::new("python3", code)).await;
    let second = harness.run(&SandboxRequest::new("python3", code)).await;
    exited(&first);
    exited(&second);
    assert_ne!(first.stdout, second.stdout);
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn the_api_boot_runs_code() {
    let harness = Harness::new(unjailed(BootMethod::Api), 0).await;
    let outcome = harness
        .run(&SandboxRequest::new("sh", "cat /proc/cmdline"))
        .await;
    exited(&outcome);
    for arg in ["reboot=k", "panic=1", "8250.nr_uarts=0"] {
        assert!(outcome.stdout.contains(arg), "{}", outcome.stdout);
    }
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn a_warm_pool_serves_each_run_from_a_new_microvm() {
    let harness = Harness::new(unjailed(BootMethod::ConfigFile), 2).await;
    let code = "cat /proc/sys/kernel/random/boot_id";
    let first = harness.run(&SandboxRequest::new("sh", code)).await;
    let second = harness.run(&SandboxRequest::new("sh", code)).await;
    exited(&first);
    exited(&second);
    assert_ne!(first.stdout, second.stdout, "a microVM served two runs");
    harness.assert_clean().await;
}

#[tokio::test]
#[ignore = "needs /dev/kvm, Firecracker and a guest image"]
async fn the_jailer_runs_code_and_removes_the_jail() {
    let Some(config) = jailed() else {
        return;
    };
    let Confinement::Jailer(jail) = config.confinement.clone() else {
        unreachable!("jailed() returns a jailer config");
    };
    let harness = Harness::new(config, 0).await;
    let outcome = harness
        .run(&SandboxRequest::new("python3", "print(6 * 7)"))
        .await;
    exited(&outcome);
    assert_eq!(outcome.stdout, "42\n");
    harness.assert_clean().await;
    let cgroups = jail.cgroup_root.join(&jail.cgroup_parent);
    let left: Vec<_> = std::fs::read_dir(&cgroups)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|e| e.file_name())
                .filter(|name| name.to_string_lossy().starts_with(VM_ID_PREFIX))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(left, Vec::<std::ffi::OsString>::new(), "cgroups left");
}
