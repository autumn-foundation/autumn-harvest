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
use crate::sandbox::error::SandboxError;
use crate::sandbox::guest::{PlanExecutor, Served, serve_connection};
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
            }) as Box<dyn LaunchedVm>)
        })
    }

    fn image_name(&self) -> Option<String> {
        Some("fake-image".into())
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
                        serve_connection(&mut stream, executor.as_ref(), &|_: &[u8; 32]| Ok(()))
                            .await;
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
        (self.mode == GuestMode::Exited).then(|| "fake vmm exited with status 1".to_owned())
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
