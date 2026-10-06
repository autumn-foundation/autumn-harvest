use autumn_harvest_cli::{Cli, CliError, execute, run_cli};
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn execute_sends_expected_http_request() {
    let (base_url, request_task) =
        spawn_one_response_server("202 Accepted", r#"{"ok":true}"#).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "--token",
        "secret-token",
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
        "--reason",
        "operator request",
    ])
    .expect("CLI args should parse");

    let response = execute(&cli).await.expect("request should succeed");
    let raw_request = request_task.await.expect("server task should finish");

    assert_eq!(response["ok"], true);
    assert!(raw_request.starts_with(
        "POST /api/harvest/workflows/00000000-0000-0000-0000-000000000001/cancel HTTP/1.1"
    ));
    assert!(raw_request.contains("authorization: Bearer secret-token"));
    assert!(raw_request.contains(r#"{"reason":"operator request"}"#));
}

#[tokio::test]
async fn execute_returns_api_errors_with_response_body() {
    let (base_url, request_task) =
        spawn_one_response_server("500 Internal Server Error", "database is haunted").await;
    let cli = Cli::try_parse_from(["harvest", "--base-url", &base_url, "health"])
        .expect("CLI args should parse");

    let error = execute(&cli).await.expect_err("request should fail");
    let _raw_request = request_task.await.expect("server task should finish");

    match error {
        CliError::Http { status, body } => {
            assert_eq!(status.as_u16(), 500);
            assert_eq!(body, "database is haunted");
        }
        other => panic!("expected HTTP error, got {other:?}"),
    }
}

async fn spawn_one_response_server(
    status: &'static str,
    body: &'static str,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server should have addr");
    let base_url = format!("http://{addr}/api/harvest");
    let body_len = body.len();
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {body_len}\r\nconnection: close\r\n\r\n{body}"
    );

    let request_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("server should accept");
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = socket.read(&mut buf).await.expect("server should read");
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
            if request_is_complete(&request) {
                break;
            }
        }
        socket
            .write_all(response.as_bytes())
            .await
            .expect("server should respond");
        String::from_utf8(request).expect("request should be UTF-8")
    });

    (base_url, request_task)
}

#[tokio::test]
async fn mutating_command_sends_harvest_source_cli_header() {
    let (base_url, request_task) = spawn_one_response_server("200 OK", r#"{"ok":true}"#).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let _ = execute(&cli).await.expect("request should succeed");
    let raw_request = request_task.await.expect("server task should finish");

    assert!(
        raw_request.contains("x-harvest-source: cli"),
        "mutating request must carry x-harvest-source: cli; got:\n{raw_request}"
    );
}

#[tokio::test]
async fn mutating_command_with_actor_flag_sends_actor_header() {
    let (base_url, request_task) = spawn_one_response_server("200 OK", r#"{"ok":true}"#).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "--actor",
        "ops-bot@example.com",
        "--request-id",
        "incident-123",
        "workflow",
        "cancel",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let _ = execute(&cli).await.expect("request should succeed");
    let raw_request = request_task.await.expect("server task should finish");

    assert!(
        raw_request.contains("x-harvest-actor: ops-bot@example.com"),
        "expected x-harvest-actor header; got:\n{raw_request}"
    );
    assert!(
        raw_request.contains("x-request-id: incident-123"),
        "expected x-request-id header; got:\n{raw_request}"
    );
}

#[tokio::test]
async fn read_only_command_does_not_send_source_header() {
    let (base_url, request_task) = spawn_one_response_server("200 OK", r#"{"status":"ok"}"#).await;
    let cli = Cli::try_parse_from(["harvest", "--base-url", &base_url, "health"])
        .expect("CLI args should parse");

    let _ = execute(&cli).await.expect("request should succeed");
    let raw_request = request_task.await.expect("server task should finish");

    assert!(
        !raw_request.contains("x-harvest-source"),
        "GET requests must not carry x-harvest-source; got:\n{raw_request}"
    );
}

#[tokio::test]
async fn execute_sends_explicit_json_accept_header() {
    // Issue #1579: a bare `Accept: */*` (curl's own default) makes the
    // server's error-page negotiation treat the request as browser
    // navigation. It then returns an HTML error page, not the
    // documented JSON body, on a validation error. An explicit
    // `Accept: application/json` avoids that regardless of what a
    // client library's own default would have sent.
    let (base_url, request_task) = spawn_one_response_server("200 OK", r#"{"ok":true}"#).await;
    let cli = Cli::try_parse_from(["harvest", "--base-url", &base_url, "health"])
        .expect("CLI args should parse");

    let _ = execute(&cli).await.expect("request should succeed");
    let raw_request = request_task.await.expect("server task should finish");

    assert!(
        raw_request.contains("accept: application/json"),
        "expected an explicit accept: application/json header; got:\n{raw_request}"
    );
}

#[tokio::test]
async fn events_tail_still_sends_event_stream_accept_header() {
    // Guard against issue #1579's fix widening by accident. `events tail`
    // is a separate streaming code path with its own `Accept` value. It
    // must never pick up the JSON `execute()` path's header instead.
    let (base_url, request_task) = spawn_one_response_server("200 OK", "").await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "events",
        "tail",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let _ = run_cli(cli).await;
    let raw_request = request_task.await.expect("server task should finish");

    assert!(
        raw_request.contains("accept: text/event-stream"),
        "events tail must send accept: text/event-stream; got:\n{raw_request}"
    );
    assert!(
        !raw_request.contains("accept: application/json"),
        "events tail must not send the JSON execute() Accept header; got:\n{raw_request}"
    );
}

fn request_is_complete(request: &[u8]) -> bool {
    let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let header_end = header_end + 4;
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    request.len() >= header_end + content_length
}

// ── HTTP timeouts (issue #1832) ──────────────────────────────────────────────

/// Accept TCP connections and never answer. Returns the base URL.
async fn spawn_black_hole() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("black hole should bind");
    let addr = listener.local_addr().expect("black hole should have addr");
    // Keep each accepted socket open, and never answer.
    let task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _open = socket;
                std::future::pending::<()>().await;
            });
        }
    });
    (format!("http://{addr}/api/harvest"), task)
}

// Issue #1832 AC: a CLI call to a black-hole endpoint fails within the timeout.
#[tokio::test]
async fn execute_fails_within_the_timeout_on_a_black_hole_endpoint() {
    let (base_url, black_hole) = spawn_black_hole().await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "--http-timeout-secs",
        "1",
        "health",
    ])
    .expect("CLI args should parse");

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), execute(&cli))
        .await
        .expect("execute must give up on its own, not hang");
    let elapsed = started.elapsed();
    black_hole.abort();

    match outcome {
        Err(CliError::Timeout { seconds }) => assert_eq!(seconds, 1),
        other => panic!("expected CliError::Timeout, got {other:?}"),
    }
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "a 1 s timeout took {elapsed:?}"
    );
}

#[test]
fn timeout_errors_name_what_to_check() {
    let message = CliError::Timeout { seconds: 7 }.to_string();
    assert!(message.contains("7 s"), "{message}");
    assert!(message.contains("--http-timeout-secs"), "{message}");
    let message = CliError::ConnectTimeout { seconds: 10 }.to_string();
    assert!(message.contains("10 s"), "{message}");
    assert!(message.contains("--base-url"), "{message}");
}

#[test]
fn http_timeout_defaults_to_thirty_seconds_and_rejects_zero() {
    // An exported HARVEST_HTTP_TIMEOUT_SECS changes the default.
    if std::env::var_os("HARVEST_HTTP_TIMEOUT_SECS").is_none() {
        let cli = Cli::try_parse_from(["harvest", "health"]).expect("CLI args should parse");
        assert_eq!(cli.http_timeout(), std::time::Duration::from_secs(30));
    }
    assert!(
        Cli::try_parse_from(["harvest", "--http-timeout-secs", "0", "health"]).is_err(),
        "a zero timeout would fail every request"
    );
}

// The events tail bounds the wait for headers by the timeout.
#[tokio::test]
async fn events_tail_fails_within_the_timeout_on_a_black_hole_endpoint() {
    let (base_url, black_hole) = spawn_black_hole().await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "--http-timeout-secs",
        "1",
        "events",
        "tail",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), run_cli(cli))
        .await
        .expect("events tail must give up on its own, not hang");
    black_hole.abort();

    assert!(
        matches!(outcome, Err(CliError::Timeout { seconds: 1 })),
        "expected CliError::Timeout, got {outcome:?}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

// A live stream outlasts the timeout. Only the header wait is bounded.
#[tokio::test]
async fn events_tail_stream_may_outlast_the_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server should have addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("server should accept");
        let mut buf = [0_u8; 4096];
        let _ = socket.read(&mut buf).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\
                  connection: close\r\n\r\n",
            )
            .await
            .expect("server should send headers");
        // Three keepalives, 0.6 s apart, then the end. That is 1.8 s in all.
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
            socket
                .write_all(b": keepalive\n\n")
                .await
                .expect("server should send keepalive");
        }
        socket
            .write_all(b"event: stream-end\ndata: {}\n\n")
            .await
            .expect("server should end the stream");
    });
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &format!("http://{addr}/api/harvest"),
        "--http-timeout-secs",
        "1",
        "events",
        "tail",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), run_cli(cli))
        .await
        .expect("the stream should end");
    server.await.expect("server task should finish");
    assert!(
        outcome.is_ok(),
        "a 1.8 s stream must survive a 1 s timeout: {outcome:?}"
    );
}

fn request_timeout_of(command: &[&str]) -> std::time::Duration {
    let mut line = vec!["harvest", "--http-timeout-secs", "30"];
    line.extend_from_slice(command);
    Cli::try_parse_from(line)
        .expect("CLI args should parse")
        .request_timeout()
}

// A command that waits on the server by design gets a longer timeout.
#[test]
fn request_timeout_outlasts_a_server_side_wait() {
    let id = "00000000-0000-0000-0000-000000000001";
    assert_eq!(
        request_timeout_of(&["health"]),
        std::time::Duration::from_secs(30)
    );
    // `update --wait completed --timeout-secs 60` waits 60 s on the server.
    assert_eq!(
        request_timeout_of(&["workflow", "update", id, "approve", "--timeout-secs", "60"]),
        std::time::Duration::from_secs(70)
    );
    // `--wait admitted` returns at once, so the base timeout applies.
    assert_eq!(
        request_timeout_of(&["workflow", "update", id, "approve", "--wait", "admitted"]),
        std::time::Duration::from_secs(30)
    );
    // A bulk DLQ write acts on up to 1000 rows. A dry run reads only.
    assert_eq!(
        request_timeout_of(&["dlq", "redrive", "--queue", "q"]),
        std::time::Duration::from_secs(300)
    );
    assert_eq!(
        request_timeout_of(&["dlq", "redrive", "--queue", "q", "--dry-run"]),
        std::time::Duration::from_secs(30)
    );
}

// A non-2xx error body that stalls is bounded by the timeout too. Only a
// successful stream may run past it.
#[tokio::test]
async fn events_tail_bounds_a_stalled_error_body() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server should have addr");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("server should accept");
        let mut buf = [0_u8; 4096];
        let _ = socket.read(&mut buf).await;
        // Promise 100 bytes, send 1, then stall.
        socket
            .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 100\r\n\r\nx")
            .await
            .expect("server should send headers");
        std::future::pending::<()>().await;
    });
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &format!("http://{addr}/api/harvest"),
        "--http-timeout-secs",
        "1",
        "events",
        "tail",
        "00000000-0000-0000-0000-000000000001",
    ])
    .expect("CLI args should parse");

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), run_cli(cli))
        .await
        .expect("events tail must give up on its own, not hang");
    server.abort();

    assert!(
        matches!(outcome, Err(CliError::Timeout { seconds: 1 })),
        "expected CliError::Timeout, got {outcome:?}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}
