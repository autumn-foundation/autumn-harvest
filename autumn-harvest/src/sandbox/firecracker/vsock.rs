//! The host side of Firecracker vsock: connect to a guest port.
//!
//! Firecracker maps the guest vsock device to a Unix socket on the host. The
//! host connects to that socket, writes `CONNECT <port>\n` in one write and
//! reads `OK <host port>\n`. After that line the stream carries raw bytes.
//! When no guest process listens on the port, Firecracker closes the
//! connection, so the host retries until its deadline.

use std::path::Path;
use std::time::{Duration, Instant};

use tokio::net::UnixStream;

use crate::sandbox::error::SandboxError;

/// The longest reply line that the host reads.
pub const MAX_CONNECT_REPLY_BYTES: usize = 32;

/// The first wait between two connect attempts.
pub const FIRST_RETRY_DELAY: Duration = Duration::from_millis(5);

/// The longest wait between two connect attempts.
pub const MAX_RETRY_DELAY: Duration = Duration::from_millis(100);

/// The time limit of one attempt.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// Connect to `port` in the guest through the vsock socket at `uds`.
///
/// `exited` reports a VMM that has stopped. The loop then ends at once, with
/// the reason, instead of waiting for the deadline.
///
/// # Errors
///
/// Returns [`SandboxError::Unavailable`] when the deadline passes or the
/// VMM stops before the guest accepts.
pub async fn connect_guest(
    uds: &Path,
    port: u32,
    deadline: Instant,
    exited: &mut (dyn FnMut() -> Option<String> + Send),
) -> Result<UnixStream, SandboxError> {
    let _ = (uds, port, deadline, exited);
    todo!("connect_guest")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    use super::*;

    /// How the fake muxer answers.
    #[derive(Clone, Copy)]
    pub(crate) enum Muxer {
        /// Close the first `n` connections, then answer `OK` and echo.
        RefuseThenEcho(usize),
        /// Reply with a line that is not `OK`.
        Garbage,
        /// Reply with more than the line cap and no newline.
        Endless,
    }

    pub(crate) fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("harvest-vsock-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Start a fake Firecracker muxer on `path`. It counts connections.
    fn fake_muxer(path: &Path, mode: Muxer) -> Arc<AtomicUsize> {
        let listener = UnixListener::bind(path).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&count);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let n = seen.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    // The CONNECT line must arrive in one read.
                    let mut buf = [0u8; 64];
                    let len = stream.read(&mut buf).await.unwrap_or(0);
                    assert_eq!(&buf[..len], b"CONNECT 52\n");
                    match mode {
                        Muxer::RefuseThenEcho(refuse) if n < refuse => {}
                        Muxer::RefuseThenEcho(_) => {
                            stream.write_all(b"OK 1073741824\n").await.unwrap();
                            let mut data = [0u8; 5];
                            stream.read_exact(&mut data).await.unwrap();
                            stream.write_all(&data).await.unwrap();
                        }
                        Muxer::Garbage => {
                            let _ = stream.write_all(b"FAILURE\n").await;
                        }
                        Muxer::Endless => {
                            let _ = stream.write_all(&[b'x'; 100]).await;
                        }
                    }
                });
            }
        });
        count
    }

    fn never_exited() -> impl FnMut() -> Option<String> + Send {
        || None
    }

    #[tokio::test]
    async fn the_handshake_hands_back_a_raw_stream() {
        let dir = temp_dir();
        let path = dir.join("v.sock");
        fake_muxer(&path, Muxer::RefuseThenEcho(0));
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = connect_guest(&path, 52, deadline, &mut never_exited())
            .await
            .unwrap();
        stream.write_all(b"hello").await.unwrap();
        let mut back = [0u8; 5];
        stream.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"hello");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_closed_attempt_is_retried_until_the_guest_listens() {
        let dir = temp_dir();
        let path = dir.join("v.sock");
        let count = fake_muxer(&path, Muxer::RefuseThenEcho(3));
        let deadline = Instant::now() + Duration::from_secs(5);
        connect_guest(&path, 52, deadline, &mut never_exited())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 4);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_socket_that_appears_late_is_found() {
        let dir = temp_dir();
        let path = dir.join("v.sock");
        let late = path.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            fake_muxer(&late, Muxer::RefuseThenEcho(0));
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        connect_guest(&path, 52, deadline, &mut never_exited())
            .await
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn a_guest_that_never_listens_is_unavailable_at_the_deadline() {
        let dir = temp_dir();
        let path = dir.join("v.sock");
        fake_muxer(&path, Muxer::RefuseThenEcho(usize::MAX));
        let started = Instant::now();
        let deadline = started + Duration::from_millis(300);
        let err = connect_guest(&path, 52, deadline, &mut never_exited())
            .await
            .expect_err("never");
        assert!(matches!(err, SandboxError::Unavailable(_)));
        assert!(started.elapsed() < Duration::from_secs(3));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn garbage_and_endless_replies_never_succeed() {
        for mode in [Muxer::Garbage, Muxer::Endless] {
            let dir = temp_dir();
            let path = dir.join("v.sock");
            fake_muxer(&path, mode);
            let deadline = Instant::now() + Duration::from_millis(300);
            let err = connect_guest(&path, 52, deadline, &mut never_exited())
                .await
                .expect_err("bad reply");
            assert!(matches!(err, SandboxError::Unavailable(_)));
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test]
    async fn a_stopped_vmm_ends_the_wait_at_once() {
        let dir = temp_dir();
        let path = dir.join("v.sock");
        let started = Instant::now();
        let deadline = started + Duration::from_secs(30);
        let mut exited = || Some("firecracker exited with status 1".to_owned());
        let err = connect_guest(&path, 52, deadline, &mut exited)
            .await
            .expect_err("stopped");
        assert!(matches!(err, SandboxError::Unavailable(ref m) if m.contains("status 1")));
        assert!(started.elapsed() < Duration::from_secs(5));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
