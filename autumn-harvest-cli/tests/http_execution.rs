use autumn_harvest_cli::{Cli, CliError, execute};
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
