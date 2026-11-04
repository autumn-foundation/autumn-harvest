//! `harvest agents profile` against a scripted server (issue #2374).

use autumn_harvest_cli::{Cli, CliError, run_cli};
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Answer each request in turn with the next scripted status and body.
/// Each answer closes its connection. The task returns every request.
async fn scripted_server(
    script: Vec<(&'static str, String)>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server should have addr");
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in script {
            let (mut socket, _) = listener.accept().await.expect("server should accept");
            let mut request = Vec::new();
            let mut buf = [0_u8; 4096];
            loop {
                let read = socket.read(&mut buf).await.expect("server should read");
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..read]);
                if complete(&request) {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
                 connection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("server should respond");
            requests.push(String::from_utf8(request).expect("request should be UTF-8"));
        }
        requests
    });
    (format!("http://{addr}/api/harvest"), task)
}

fn complete(request: &[u8]) -> bool {
    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let head = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
    let length = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0);
    request.len() >= end + 4 + length
}

fn first_line(request: &str) -> &str {
    request.lines().next().unwrap_or_default()
}

fn body(request: &str) -> serde_json::Value {
    let (_, body) = request.split_once("\r\n\r\n").expect("a body");
    serde_json::from_str(body).expect("a JSON body")
}

/// With no `--expected`, `activate` reads the active version, then swaps
/// over it.
#[tokio::test]
async fn activate_reads_the_active_version_then_swaps_over_it() {
    let (base_url, task) = scripted_server(vec![
        (
            "200 OK",
            r#"{"kind":"support","active":1,"versions":[]}"#.to_owned(),
        ),
        (
            "200 OK",
            r#"{"kind":"support","from":1,"to":2,"actor":"ops"}"#.to_owned(),
        ),
    ])
    .await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "profile",
        "activate",
        "support",
        "v2",
    ])
    .expect("parse");
    run_cli(cli).await.expect("activate");
    let requests = task.await.expect("server");
    assert_eq!(
        first_line(&requests[0]),
        "GET /api/harvest/agents/support/profile/versions HTTP/1.1"
    );
    assert_eq!(
        first_line(&requests[1]),
        "POST /api/harvest/agents/support/profile/activate HTTP/1.1"
    );
    assert_eq!(
        body(&requests[1]),
        serde_json::json!({"version": 2, "expected_active": 1})
    );
}

/// A refused rollback exits non-zero, and the error holds the message of
/// the server.
#[tokio::test]
async fn a_refused_rollback_fails_with_the_message() {
    let message = r#"{"error":"the kind has no earlier activation to roll back to"}"#;
    let (base_url, task) = scripted_server(vec![("409 Conflict", message.to_owned())]).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "profile",
        "rollback",
        "support",
    ])
    .expect("parse");
    let error = run_cli(cli).await.expect_err("a 409 fails");
    let requests = task.await.expect("server");
    assert_eq!(
        first_line(&requests[0]),
        "POST /api/harvest/agents/support/profile/rollback HTTP/1.1"
    );
    assert_ne!(error.exit_code(), 0);
    assert!(
        error.to_string().contains("no earlier activation"),
        "{error}"
    );
    assert!(matches!(error, CliError::Http { status, .. } if status.as_u16() == 409));
}

/// A stale `--expected` exits non-zero, and the error names the version
/// that is active.
#[tokio::test]
async fn a_stale_activate_fails_with_the_active_version() {
    let message = r#"{"error":"the active overlay is v3, not the expected version"}"#;
    let (base_url, task) = scripted_server(vec![("409 Conflict", message.to_owned())]).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "profile",
        "activate",
        "support",
        "2",
        "--expected",
        "1",
    ])
    .expect("parse");
    let error = run_cli(cli).await.expect_err("a 409 fails");
    let requests = task.await.expect("server");
    assert_eq!(requests.len(), 1, "--expected skips the read");
    assert_eq!(
        body(&requests[0]),
        serde_json::json!({"version": 2, "expected_active": 1})
    );
    assert_ne!(error.exit_code(), 0);
    assert!(error.to_string().contains("v3"), "{error}");
}

/// Issue #2394: `policy set` reads the policy, changes only the named keys,
/// then sends a `PUT` over the revision that it read.
#[tokio::test]
async fn policy_set_reads_then_sends_the_changed_keys() {
    let (base_url, task) = scripted_server(vec![
        (
            "200 OK",
            r#"{"kind":"support","revision":3,"custom":true,"policy":{"auto_adopt":{"parts":[],"max_per_day":1},"approval_timeout":"7d"}}"#
                .to_owned(),
        ),
        (
            "200 OK",
            r#"{"kind":"support","revision":4,"custom":true,"policy":{}}"#.to_owned(),
        ),
    ])
    .await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "profile",
        "policy",
        "support",
        "set",
        "--auto-adopt-parts",
        "system",
        "--max-per-day",
        "unlimited",
    ])
    .expect("parse");
    run_cli(cli).await.expect("set");
    let requests = task.await.expect("server");
    assert_eq!(
        first_line(&requests[0]),
        "GET /api/harvest/agents/support/profile/policy HTTP/1.1"
    );
    assert_eq!(
        first_line(&requests[1]),
        "PUT /api/harvest/agents/support/profile/policy HTTP/1.1"
    );
    assert_eq!(
        body(&requests[1]),
        serde_json::json!({
            "policy": {
                "auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"},
                "approval_timeout": "7d"
            },
            "expected_revision": 3
        })
    );
}

/// A policy set that loses a race exits non-zero with the message of the
/// server. `show`, `reset` and `history` send one request each.
#[tokio::test]
async fn policy_actions_send_their_requests() {
    let conflict = r#"{"error":"the refine policy is at revision 5, not the expected one"}"#;
    let (base_url, task) = scripted_server(vec![
        (
            "200 OK",
            r#"{"kind":"support","revision":null,"custom":false,"policy":{}}"#.to_owned(),
        ),
        ("409 Conflict", conflict.to_owned()),
        ("200 OK", r#"{"kind":"support","revision":6}"#.to_owned()),
        ("200 OK", r#"{"kind":"support","changes":[]}"#.to_owned()),
    ])
    .await;
    let run = |args: &[&str]| {
        let mut argv = vec![
            "harvest",
            "--base-url",
            &base_url,
            "agents",
            "profile",
            "policy",
        ];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv).expect("parse")
    };
    let error = run_cli(run(&["support", "set", "--proposal-attempts", "3"]))
        .await
        .expect_err("a 409 fails");
    assert_ne!(error.exit_code(), 0);
    assert!(error.to_string().contains("revision 5"), "{error}");
    run_cli(run(&["support", "reset"])).await.expect("reset");
    run_cli(run(&["support", "history"]))
        .await
        .expect("history");
    let requests = task.await.expect("server");
    assert_eq!(
        body(&requests[1]),
        serde_json::json!({"policy": {"proposal_attempts": 3}, "expected_revision": null})
    );
    assert_eq!(
        first_line(&requests[2]),
        "POST /api/harvest/agents/support/profile/policy/reset HTTP/1.1"
    );
    assert_eq!(
        first_line(&requests[3]),
        "GET /api/harvest/agents/support/profile/policy/changes HTTP/1.1"
    );
}
