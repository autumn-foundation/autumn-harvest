//! A fake VMM for tests. No KVM needed.
//!
//! Each fake microVM is a task on a Unix socket. It acts like the
//! Firecracker vsock muxer: it reads `CONNECT <port>\n` and answers `OK`.
//! Then it hands the stream to the real
//! [`crate::sandbox::guest::serve_connection`]. So a test runs the real host
//! backend and the real guest code, and only the VMM is fake.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::task::JoinHandle;

use super::launcher::{LaunchedVm, VmLauncher};

/// Write an executable file for a test, with mode `0755`.
///
/// A test that writes a file and then runs it can fail with ETXTBSY. Another
/// test thread can fork while the write descriptor is open, and the child
/// keeps the descriptor until its own exec. So `install` writes the file in
/// its own process, and no thread of the test binary holds a descriptor.
pub fn write_executable(path: &Path, contents: &str) {
    let source = path.with_extension("src");
    std::fs::write(&source, contents).expect("write the source");
    let status = std::process::Command::new("install")
        .args(["-m", "0755"])
        .arg(&source)
        .arg(path)
        .status()
        .expect("run install");
    assert!(status.success(), "install failed for {}", path.display());
    std::fs::remove_file(&source).expect("remove the source");
}
use crate::sandbox::error::SandboxError;
use crate::sandbox::guest::{PlanExecutor, Served, UnmanagedKernel, serve_connection};
use crate::sandbox::protocol::{HostMessage, MAX_FRAME_BYTES, read_message};
use crate::sandbox::runtime::BoxFuture;

/// How a fake guest behaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuestMode {
    /// Serve pings, then one run.
    Serve,
    /// Never accept: close each connection.
    NeverListen,
    /// Accept, then send a frame that is not JSON.
    Garbage,
    /// Accept, read the request, and never answer.
    Hang,
    /// The VMM process has exited.
    Exited,
}

/// A launcher of fake microVMs.
pub struct FakeLauncher {
    pub dir: PathBuf,
    pub mode: GuestMode,
    pub executor: Arc<dyn PlanExecutor>,
    pub live: Arc<AtomicUsize>,
    pub launched: Arc<AtomicUsize>,
    pub failures_left: AtomicUsize,
    /// The ids of the VMs whose VMM has exited.
    pub crashed: Arc<std::sync::Mutex<Vec<String>>>,
    /// The answer of `guest_port`.
    pub port: Option<u32>,
    /// The number of `guest_port` calls.
    pub port_queries: AtomicUsize,
    /// The answer of `launch_budget`.
    pub launch_budget: std::time::Duration,
}

impl FakeLauncher {
    pub fn new(mode: GuestMode, executor: Arc<dyn PlanExecutor>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "hfv-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        Self {
            dir,
            mode,
            executor,
            live: Arc::new(AtomicUsize::new(0)),
            launched: Arc::new(AtomicUsize::new(0)),
            failures_left: AtomicUsize::new(0),
            crashed: Arc::default(),
            port: None,
            port_queries: AtomicUsize::new(0),
            launch_budget: std::time::Duration::ZERO,
        }
    }

    pub fn live(&self) -> usize {
        self.live.load(Ordering::SeqCst)
    }

    pub fn launched(&self) -> usize {
        self.launched.load(Ordering::SeqCst)
    }
}

impl Drop for FakeLauncher {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl VmLauncher for FakeLauncher {
    fn launch(&self) -> BoxFuture<'_, Result<Box<dyn LaunchedVm>, SandboxError>> {
        Box::pin(async move {
            if take_one(&self.failures_left) {
                return Err(SandboxError::Unavailable("fake launch failure".into()));
            }
            let n = self.launched.fetch_add(1, Ordering::SeqCst);
            let id = format!("fake-{n}");
            let vm_dir = self.dir.join(&id);
            std::fs::create_dir_all(&vm_dir).expect("vm dir");
            let vsock = vm_dir.join("v.sock");
            let task = (self.mode != GuestMode::Exited)
                .then(|| spawn_muxer(&vsock, self.mode, Arc::clone(&self.executor)));
            self.live.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(FakeVm {
                id,
                dir: vm_dir,
                vsock,
                mode: self.mode,
                task,
                live: Arc::clone(&self.live),
                down: false,
                crashed: Arc::clone(&self.crashed),
            }) as Box<dyn LaunchedVm>)
        })
    }

    fn image_name(&self) -> Option<String> {
        Some("fake-image".into())
    }

    fn guest_port(&self) -> Option<u32> {
        self.port_queries.fetch_add(1, Ordering::SeqCst);
        self.port
    }

    fn launch_budget(&self) -> std::time::Duration {
        self.launch_budget
    }
}

/// Take one from a positive counter. Returns `false` at zero.
fn take_one(counter: &AtomicUsize) -> bool {
    let mut current = counter.load(Ordering::SeqCst);
    while current > 0 {
        match counter.compare_exchange(current, current - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
    false
}

/// A guest executor that runs each plan with the host network.
///
/// A real microVM has no network device, and its executor adds an empty
/// network namespace as a second layer. The fake microVM shares the host,
/// where a test may not create a namespace. So the fake skips that layer.
pub struct HostNetworkExecutor<E>(pub E);

impl<E: PlanExecutor> PlanExecutor for HostNetworkExecutor<E> {
    fn execute<'a>(
        &'a self,
        plan: &'a crate::sandbox::request::ExecPlan,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> crate::sandbox::runtime::BoxFuture<
        'a,
        Result<crate::sandbox::outcome::SandboxOutcome, crate::sandbox::error::SandboxError>,
    > {
        Box::pin(async move {
            let mut plan = plan.clone();
            plan.network = crate::sandbox::policy::NetworkPolicy::Allow;
            self.0.execute(&plan, cancel).await
        })
    }
}

fn spawn_muxer(path: &Path, mode: GuestMode, executor: Arc<dyn PlanExecutor>) -> JoinHandle<()> {
    let listener = UnixListener::bind(path).expect("bind fake vsock");
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut line = [0u8; 32];
            let n = stream.read(&mut line).await.unwrap_or(0);
            if !line[..n].starts_with(b"CONNECT ") {
                continue;
            }
            match mode {
                GuestMode::NeverListen | GuestMode::Exited => {}
                GuestMode::Garbage => {
                    let _ = stream.write_all(b"OK 1073741824\n").await;
                    let _: Result<HostMessage, _> =
                        read_message(&mut stream, MAX_FRAME_BYTES).await;
                    let _ = stream.write_all(&[0, 0, 0, 3, b'{', b'x', b'}']).await;
                }
                GuestMode::Hang => {
                    let _ = stream.write_all(b"OK 1073741824\n").await;
                    let mut sink = Vec::new();
                    let _ = stream.read_to_end(&mut sink).await;
                }
                GuestMode::Serve => {
                    if stream.write_all(b"OK 1073741824\n").await.is_err() {
                        continue;
                    }
                    let served =
                        serve_connection(&mut stream, executor.as_ref(), &UnmanagedKernel).await;
                    if matches!(served, Ok(Served::Run)) {
                        // A real guest reboots after one run, and the VMM exits.
                        return;
                    }
                }
            }
        }
    })
}

/// One fake microVM.
pub struct FakeVm {
    id: String,
    dir: PathBuf,
    vsock: PathBuf,
    mode: GuestMode,
    task: Option<JoinHandle<()>>,
    live: Arc<AtomicUsize>,
    down: bool,
    crashed: Arc<std::sync::Mutex<Vec<String>>>,
}

impl FakeVm {
    fn tear_down(&mut self) {
        if self.down {
            return;
        }
        self.down = true;
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LaunchedVm for FakeVm {
    fn id(&self) -> &str {
        &self.id
    }

    fn vsock_path(&self) -> &Path {
        &self.vsock
    }

    fn exit_status(&mut self) -> Option<String> {
        let crashed = self
            .crashed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(&self.id);
        (self.mode == GuestMode::Exited || crashed)
            .then(|| "fake vmm exited with status 1".to_owned())
    }

    fn shutdown(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        self.tear_down();
        Box::pin(std::future::ready(()))
    }
}

impl Drop for FakeVm {
    fn drop(&mut self) {
        self.tear_down();
    }
}
