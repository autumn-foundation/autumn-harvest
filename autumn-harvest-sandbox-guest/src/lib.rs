//! The guest agent of the autumn-harvest sandbox microVMs.
//!
//! The agent is process 1 in the microVM. It serves the host protocol of
//! `autumn_harvest::sandbox::guest` on a vsock port. Each connection carries
//! one ping or one run. After one run the agent reboots the microVM, and
//! Firecracker exits, so a microVM never serves two runs.
//!
//! The agent takes its settings from the kernel command line:
//!
//! - `harvest.port=<n>`: the vsock port. The default is 52.
//! - `harvest.uid=<n>` and `harvest.gid=<n>`: the ids of the code. The
//!   default is 1000.
//! - `harvest.work_mib=<n>`: the size of the `/work` tmpfs. The default is
//!   256.

use std::path::PathBuf;
use std::time::Duration;

use autumn_harvest::sandbox::guest::{PlanExecutor, ReseedFn, Served, serve_connection};
use tokio::io::{AsyncRead, AsyncWrite};

/// The default vsock port.
pub const DEFAULT_PORT: u32 = 52;

/// The default uid and gid of the code.
pub const DEFAULT_RUN_AS: (u32, u32) = (1000, 1000);

/// The default size of the `/work` tmpfs, in MiB.
pub const DEFAULT_WORK_MIB: u64 = 256;

/// The time that the agent waits for the host to close the connection
/// after a run, before it reboots. The wait lets the vsock device deliver
/// the last bytes of the outcome.
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// The settings of the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestConfig {
    /// The vsock port.
    pub port: u32,
    /// The uid and gid of the code.
    pub run_as: (u32, u32),
    /// The directory that holds the work directories.
    pub work_dir: PathBuf,
    /// The size of the `/work` tmpfs, in MiB.
    pub work_mib: u64,
}

impl Default for GuestConfig {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            run_as: DEFAULT_RUN_AS,
            work_dir: PathBuf::from("/work"),
            work_mib: DEFAULT_WORK_MIB,
        }
    }
}

/// Read the settings from a kernel command line. Unknown and malformed
/// values keep their defaults.
#[must_use]
pub fn parse_cmdline(cmdline: &str) -> GuestConfig {
    let mut config = GuestConfig::default();
    for arg in cmdline.split_whitespace() {
        let Some((key, value)) = arg.split_once('=') else {
            continue;
        };
        match key {
            "harvest.port" => {
                if let Ok(port) = value.parse() {
                    config.port = port;
                }
            }
            // Root is never the uid or the gid of the code.
            "harvest.uid" => {
                if let Ok(uid) = value.parse::<u32>()
                    && uid != 0
                {
                    config.run_as.0 = uid;
                }
            }
            "harvest.gid" => {
                if let Ok(gid) = value.parse::<u32>()
                    && gid != 0
                {
                    config.run_as.1 = gid;
                }
            }
            "harvest.work_mib" => {
                if let Ok(mib) = value.parse::<u64>()
                    && mib > 0
                {
                    config.work_mib = mib;
                }
            }
            _ => {}
        }
    }
    config
}

/// Serve connections from `accept` until one run is done, or forever when
/// `single_use` is `false`.
///
/// # Errors
///
/// Returns the I/O error of `accept`.
pub async fn serve<S, A, F>(
    mut accept: A,
    executor: &dyn PlanExecutor,
    reseed: &ReseedFn,
    single_use: bool,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    A: FnMut() -> F,
    F: std::future::Future<Output = std::io::Result<S>>,
{
    use tokio::io::AsyncReadExt as _;
    loop {
        let mut stream = accept().await?;
        match serve_connection(&mut stream, executor, reseed).await {
            Ok(Served::Ping) => {}
            Ok(Served::Run) => {
                // Wait until the host closes, so the vsock device delivers
                // the last bytes of the outcome before a reboot.
                let mut sink = [0u8; 256];
                let _ = tokio::time::timeout(CLOSE_WAIT, async {
                    while let Ok(n) = stream.read(&mut sink).await {
                        if n == 0 {
                            break;
                        }
                    }
                })
                .await;
                if single_use {
                    return Ok(());
                }
            }
            Err(e) => eprintln!("harvest-guest-agent: a connection failed: {e}"),
        }
    }
}

#[cfg(target_os = "linux")]
pub mod linux;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use autumn_harvest::sandbox::guest::{ping, request_run};
    use autumn_harvest::sandbox::runtime::SandboxJob;
    use autumn_harvest::sandbox::{NetworkPolicy, RunStatus, SandboxPolicy, SandboxRequest};
    use tokio::net::{UnixListener, UnixStream};

    use super::*;

    #[test]
    fn the_cmdline_sets_the_port_ids_and_tmpfs_size() {
        let config = parse_cmdline(
            "reboot=k panic=1 init=/sbin/harvest-guest-agent harvest.port=1024 \
             harvest.uid=2000 harvest.gid=3000 harvest.work_mib=64 root=/dev/vda ro",
        );
        assert_eq!(config.port, 1024);
        assert_eq!(config.run_as, (2000, 3000));
        assert_eq!(config.work_mib, 64);
    }

    #[test]
    fn missing_or_malformed_values_keep_the_defaults() {
        assert_eq!(parse_cmdline(""), GuestConfig::default());
        let config = parse_cmdline("harvest.port=abc harvest.uid=-1 harvest.work_mib=");
        assert_eq!(config, GuestConfig::default());
    }

    #[test]
    fn root_is_never_the_uid_of_the_code() {
        let config = parse_cmdline("harvest.uid=0 harvest.gid=0");
        assert_eq!(config.run_as, DEFAULT_RUN_AS);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_agent_serves_pings_then_one_run_then_stops() {
        let dir = std::env::temp_dir().join(format!("harvest-guest-{}", uuid_like()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("agent.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        let agent = tokio::spawn(async move {
            let executor = autumn_harvest::sandbox::exec::ProcessExecutor::default();
            serve(
                || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let listener = &listener;
                    async move { listener.accept().await.map(|(stream, _)| stream) }
                },
                &executor,
                &|_: &[u8; 32]| Ok(()),
                true,
            )
            .await
        });

        for _ in 0..2 {
            let mut stream = UnixStream::connect(&path).await.unwrap();
            ping(&mut stream).await.unwrap();
        }
        let plan = SandboxPolicy::default()
            .with_network(NetworkPolicy::Allow)
            .plan(&SandboxRequest::new("sh", "echo guest"))
            .unwrap();
        let job = SandboxJob {
            run_id: "r-1".into(),
            entropy: [9; 32],
            plan,
        };
        let mut stream = UnixStream::connect(&path).await.unwrap();
        let outcome = request_run(&mut stream, &job).await.unwrap();
        drop(stream);
        assert_eq!(outcome.status, RunStatus::Exited { code: 0 });
        assert_eq!(outcome.stdout, "guest\n");
        agent.await.unwrap().unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 3, "the agent stops after one run");
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn uuid_like() -> String {
        format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
