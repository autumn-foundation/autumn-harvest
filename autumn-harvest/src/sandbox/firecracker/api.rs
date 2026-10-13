//! A small Firecracker API client: HTTP/1.1 over a Unix socket.
//!
//! Each request opens a connection, sends one request with a
//! `Content-Length` body, and reads one response. The client bounds the
//! header and the body before it reads them, so a compromised VMM cannot make
//! the host allocate without limit. Firecracker answers `204` on success,
//! `200` with JSON for a `GET`, and `400` with `{"fault_message": ...}` on an
//! error.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use super::config::{ROOT_DRIVE_ID, VmConfigFile};
use crate::sandbox::error::SandboxError;

/// The largest response header that the client reads.
pub const MAX_API_HEADER_BYTES: usize = 16 * 1024;

/// The largest response body that the client reads.
pub const MAX_API_BODY_BYTES: usize = 64 * 1024;

/// The default time limit of one request.
pub const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(5);

/// The API socket, relative to the chroot root.
pub const JAILED_API_SOCKET: &str = "run/firecracker.socket";

/// One response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiResponse {
    /// The HTTP status code.
    pub status: u16,
    /// The body.
    pub body: Vec<u8>,
}

/// A client for one Firecracker API socket.
#[derive(Debug, Clone)]
pub struct ApiClient {
    socket: PathBuf,
    timeout: Duration,
}

impl ApiClient {
    /// A client for the socket at `socket`.
    #[must_use]
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            timeout: DEFAULT_API_TIMEOUT,
        }
    }

    /// Set the time limit of one request.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Send one request and read the response.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Unavailable`] when the connection fails, the
    /// time limit passes, or the response is malformed or over a bound.
    pub async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
    ) -> Result<ApiResponse, SandboxError> {
        let _ = (method, path, body);
        todo!("ApiClient::request")
    }

    /// Send `body` as JSON and expect a 2xx status.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Unavailable`] that holds the `fault_message`
    /// of an error response.
    pub async fn send<T: Serialize + ?Sized>(
        &self,
        method: &str,
        path: &str,
        body: &T,
    ) -> Result<(), SandboxError> {
        let _ = (method, path, serde_json::to_vec(body));
        todo!("ApiClient::send")
    }

    /// `GET` a JSON document.
    ///
    /// # Errors
    ///
    /// Returns [`SandboxError::Unavailable`] for an error status or a body
    /// that is not JSON.
    pub async fn get(&self, path: &str) -> Result<serde_json::Value, SandboxError> {
        let _ = path;
        todo!("ApiClient::get")
    }

    /// Configure a fresh process from `config`: the machine, the kernel, the
    /// root drive, vsock and entropy, in that order.
    ///
    /// # Errors
    ///
    /// Returns the error of the first failed `PUT`.
    pub async fn configure(&self, config: &VmConfigFile) -> Result<(), SandboxError> {
        let _ = (config, ROOT_DRIVE_ID);
        todo!("ApiClient::configure")
    }

    /// Start the configured microVM.
    ///
    /// # Errors
    ///
    /// Returns the error of the `PUT /actions`.
    pub async fn start(&self) -> Result<(), SandboxError> {
        todo!("ApiClient::start")
    }

    /// Load a snapshot into a fresh process and resume it.
    ///
    /// # Errors
    ///
    /// Returns the error of the `PUT /snapshot/load`. Firecracker ends the
    /// process after a failed load.
    pub async fn load_snapshot(&self, state: &str, memory: &str) -> Result<(), SandboxError> {
        let _ = (state, memory);
        todo!("ApiClient::load_snapshot")
    }

    /// Pause the running microVM.
    ///
    /// # Errors
    ///
    /// Returns the error of the `PATCH /vm`.
    pub async fn pause(&self) -> Result<(), SandboxError> {
        todo!("ApiClient::pause")
    }

    /// Write a full snapshot of the paused microVM.
    ///
    /// # Errors
    ///
    /// Returns the error of the `PUT /snapshot/create`.
    pub async fn create_snapshot(&self, state: &str, memory: &str) -> Result<(), SandboxError> {
        let _ = (state, memory);
        todo!("ApiClient::create_snapshot")
    }
}

/// Parse one HTTP/1.1 response head: the status code and the content length.
///
/// # Errors
///
/// Returns a message for a malformed status line or header.
pub fn parse_head(head: &[u8]) -> Result<(u16, Option<usize>), String> {
    let _ = head;
    todo!("parse_head")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    use super::*;
    use crate::sandbox::firecracker::config::{GuestImage, MachineSpec, VmPaths, vm_config};

    /// A fake API server: it records each request and plays back replies.
    pub(crate) struct FakeApi {
        pub dir: PathBuf,
        pub requests: Arc<Mutex<Vec<String>>>,
    }

    impl FakeApi {
        pub(crate) fn socket(&self) -> PathBuf {
            self.dir.join("api.sock")
        }
    }

    impl Drop for FakeApi {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Serve `replies` in order, one per connection. A reply of `None` reads
    /// the request and never answers.
    pub(crate) fn fake_api(replies: Vec<Option<Vec<u8>>>) -> FakeApi {
        let dir = std::env::temp_dir().join(format!("harvest-api-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let listener = UnixListener::bind(dir.join("api.sock")).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        tokio::spawn(async move {
            for reply in replies {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let request = read_request(&mut stream).await;
                seen.lock().unwrap().push(request);
                match reply {
                    Some(bytes) => {
                        let _ = stream.write_all(&bytes).await;
                    }
                    None => {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            }
        });
        FakeApi { dir, requests }
    }

    /// Read one request with a `Content-Length` body.
    async fn read_request(stream: &mut tokio::net::UnixStream) -> String {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        while !buf.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await.unwrap_or(0) == 0 {
                break;
            }
            buf.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&buf).to_string();
        let length = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .map_or(0, |v| v.trim().parse::<usize>().unwrap());
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).await.unwrap();
        format!("{head}{}", String::from_utf8_lossy(&body))
    }

    pub(crate) fn no_content() -> Option<Vec<u8>> {
        Some(b"HTTP/1.1 204 No Content\r\nServer: Firecracker API\r\nConnection: keep-alive\r\n\r\n".to_vec())
    }

    fn json_reply(status: &str, body: &str) -> Option<Vec<u8>> {
        Some(
            format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .into_bytes(),
        )
    }

    #[tokio::test]
    async fn a_put_sends_one_request_with_a_content_length_body() {
        let api = fake_api(vec![no_content()]);
        ApiClient::new(api.socket())
            .send("PUT", "/actions", &serde_json::json!({"action_type": "InstanceStart"}))
            .await
            .unwrap();
        let requests = api.requests.lock().unwrap();
        let request = &requests[0];
        assert!(request.starts_with("PUT /actions HTTP/1.1\r\n"), "{request}");
        assert!(request.contains("\r\nHost: localhost\r\n"));
        assert!(request.contains("\r\nContent-Type: application/json\r\n"));
        assert!(request.contains("\r\nContent-Length: 31\r\n"));
        assert!(request.ends_with("\r\n\r\n{\"action_type\":\"InstanceStart\"}"));
    }

    #[tokio::test]
    async fn an_error_status_reports_the_fault_message() {
        let api = fake_api(vec![json_reply(
            "400 Bad Request",
            r#"{"fault_message":"The kernel file cannot be opened"}"#,
        )]);
        let err = ApiClient::new(api.socket())
            .send("PUT", "/boot-source", &serde_json::json!({}))
            .await
            .expect_err("400");
        assert!(
            matches!(err, SandboxError::Unavailable(ref m) if m.contains("400") && m.contains("kernel file cannot be opened")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_get_returns_the_json_body() {
        let api = fake_api(vec![json_reply(
            "200 OK",
            r#"{"app_name":"Firecracker","state":"Running"}"#,
        )]);
        let value = ApiClient::new(api.socket()).get("/").await.unwrap();
        assert_eq!(value["state"], "Running");
    }

    #[tokio::test]
    async fn an_oversized_body_is_refused() {
        let reply = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            MAX_API_BODY_BYTES + 1
        );
        let api = fake_api(vec![Some(reply.into_bytes())]);
        let err = ApiClient::new(api.socket()).get("/").await.expect_err("too big");
        assert!(matches!(err, SandboxError::Unavailable(_)));
    }

    #[tokio::test]
    async fn an_oversized_header_is_refused() {
        let mut reply = b"HTTP/1.1 200 OK\r\nX-Junk: ".to_vec();
        reply.extend(std::iter::repeat_n(b'a', MAX_API_HEADER_BYTES + 10));
        let api = fake_api(vec![Some(reply)]);
        let err = ApiClient::new(api.socket()).get("/").await.expect_err("too big");
        assert!(matches!(err, SandboxError::Unavailable(_)));
    }

    #[tokio::test]
    async fn a_server_that_never_answers_times_out() {
        let api = fake_api(vec![None]);
        let started = std::time::Instant::now();
        let err = ApiClient::new(api.socket())
            .with_timeout(Duration::from_millis(200))
            .get("/")
            .await
            .expect_err("timeout");
        assert!(matches!(err, SandboxError::Unavailable(_)));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_missing_socket_is_unavailable() {
        let err = ApiClient::new("/nonexistent/harvest/api.sock")
            .get("/")
            .await
            .expect_err("no socket");
        assert!(matches!(err, SandboxError::Unavailable(_)));
    }

    #[test]
    fn response_heads_are_parsed() {
        assert_eq!(
            parse_head(b"HTTP/1.1 204 No Content\r\nServer: x\r\n\r\n").unwrap(),
            (204, None)
        );
        assert_eq!(
            parse_head(b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\n").unwrap(),
            (200, Some(12))
        );
        assert!(parse_head(b"SSH-2.0-OpenSSH\r\n\r\n").is_err());
        assert!(parse_head(b"HTTP/1.1 abc OK\r\n\r\n").is_err());
        assert!(parse_head(b"HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n").is_err());
    }

    #[tokio::test]
    async fn configure_puts_each_part_in_order_then_start_boots() {
        let api = fake_api(vec![no_content(); 6]);
        let client = ApiClient::new(api.socket());
        let config = vm_config(
            &GuestImage::new("/k", "/r"),
            MachineSpec::default(),
            &VmPaths::jailed(),
            52,
        );
        client.configure(&config).await.unwrap();
        client.start().await.unwrap();
        let requests = api.requests.lock().unwrap();
        let lines: Vec<&str> = requests.iter().map(|r| r.lines().next().unwrap()).collect();
        assert_eq!(
            lines,
            [
                "PUT /machine-config HTTP/1.1",
                "PUT /boot-source HTTP/1.1",
                "PUT /drives/rootfs HTTP/1.1",
                "PUT /vsock HTTP/1.1",
                "PUT /entropy HTTP/1.1",
                "PUT /actions HTTP/1.1",
            ]
        );
        assert!(requests[2].contains(r#""is_read_only":true"#));
        assert!(requests[5].ends_with(r#"{"action_type":"InstanceStart"}"#));
    }

    #[tokio::test]
    async fn a_snapshot_load_resumes_from_a_file_backend() {
        let api = fake_api(vec![no_content()]);
        ApiClient::new(api.socket())
            .load_snapshot("snapshot.state", "snapshot.mem")
            .await
            .unwrap();
        let requests = api.requests.lock().unwrap();
        let body = requests[0].split("\r\n\r\n").nth(1).unwrap();
        let json: serde_json::Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "snapshot_path": "snapshot.state",
                "mem_backend": {"backend_type": "File", "backend_path": "snapshot.mem"},
                "resume_vm": true
            })
        );
    }

    #[tokio::test]
    async fn pause_and_snapshot_create_use_the_documented_bodies() {
        let api = fake_api(vec![no_content(), no_content()]);
        let client = ApiClient::new(api.socket());
        client.pause().await.unwrap();
        client
            .create_snapshot("snapshot.state", "snapshot.mem")
            .await
            .unwrap();
        let requests = api.requests.lock().unwrap();
        assert!(requests[0].starts_with("PATCH /vm HTTP/1.1"));
        assert!(requests[0].ends_with(r#"{"state":"Paused"}"#));
        assert!(requests[1].starts_with("PUT /snapshot/create HTTP/1.1"));
        assert!(requests[1].contains(r#""snapshot_type":"Full""#));
    }
}
