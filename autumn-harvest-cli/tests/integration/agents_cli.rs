//! `harvest agents erase` against a scripted server (issue #2267).

use autumn_harvest_cli::{Cli, run_cli};
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const E0: &str = "00000000-0000-0000-0000-0000000000e0";
const E1: &str = "00000000-0000-0000-0000-0000000000e1";

/// Answer each request in turn with the next scripted body. Each answer
/// closes its connection. The task returns every request it read.
async fn scripted_server(script: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test server should bind");
    let addr = listener.local_addr().expect("test server should have addr");
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for body in script {
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
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
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

const E2: &str = "00000000-0000-0000-0000-0000000000e2";

/// The by-id answer of the API: the run sits under `execution`.
fn run(id: &str, state: &str, extra: &str) -> String {
    format!(r#"{{"execution":{{"id":"{id}","state":"{state}"{extra}}},"history":[]}}"#)
}

fn first_lines(requests: &[String]) -> Vec<&str> {
    requests
        .iter()
        .map(|r| r.lines().next().unwrap_or_default())
        .collect()
}

const BY_ID: &str = "/api/harvest/workflows/by-id/agent/support%2Fada";

/// Each line from `from` is an entity op signal with the body part in turn.
fn assert_signals(lines: &[&str], requests: &[String], from: usize, bodies: &[&str]) {
    for (n, body) in bodies.iter().enumerate() {
        let at = from + n;
        assert!(
            lines[at].starts_with(&format!("POST {BY_ID}/signal/harvest.entity.op ")),
            "{lines:?}"
        );
        assert!(requests[at].contains(body), "{}", requests[at]);
    }
}

#[tokio::test]
async fn erase_pauses_resets_deletes_waits_and_erases_every_run() {
    let script = vec![
        run(E1, "RUNNING", r#","queue_name":"agents""#),
        "{}".to_owned(),
        format!(r#"{{"execution_id":"{E1}","result":{{"failures":1}}}}"#),
        "{}".to_owned(),
        "{}".to_owned(),
        "{}".to_owned(),
        run(E1, "COMPLETED", r#","output":{"failures":1}"#),
        format!(r#"{{"workflows":[{{"id":"{E0}"}},{{"id":"{E1}"}}],"next_cursor":null}}"#),
        r#"{"events_scrubbed":2,"ledger_rows_deleted":1}"#.to_owned(),
        r#"{"events_scrubbed":3,"stream_chunks_deleted":4}"#.to_owned(),
    ];
    let (base_url, server) = scripted_server(script).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "erase",
        "support/ada",
        "--reason",
        "gdpr-7",
    ])
    .expect("CLI args should parse");

    run_cli(cli).await.expect("erase should succeed");
    let requests = server.await.expect("server task should finish");
    let lines = first_lines(&requests);
    assert!(lines[0].starts_with(&format!("GET {BY_ID} ")), "{lines:?}");
    assert_signals(&lines, &requests, 1, &[r#""action":"pause""#]);
    assert!(
        lines[2].starts_with(&format!("GET {BY_ID}/query/harvest.entity.state ")),
        "{lines:?}"
    );
    assert_signals(
        &lines,
        &requests,
        3,
        &[
            r#""action":"reset_memory""#,
            r#""action":"reset_workspace""#,
            r#"{"kind":"delete"}"#,
        ],
    );
    assert!(lines[6].starts_with(&format!("GET {BY_ID} ")), "{lines:?}");
    assert!(
        lines[7].starts_with("GET /api/harvest/workflows?filter="),
        "{lines:?}"
    );
    assert!(lines[7].contains("workflow_id"), "{lines:?}");
    for (n, exec) in [(8, E0), (9, E1)] {
        assert!(
            lines[n].starts_with(&format!(
                "POST /api/harvest/workflows/{exec}/erase-payloads "
            )),
            "{lines:?}"
        );
        assert!(
            requests[n].contains(r#""reason":"gdpr-7""#),
            "{}",
            requests[n]
        );
    }
}

#[tokio::test]
async fn erase_of_an_ended_agent_restarts_it_to_reset_its_stores() {
    let script = vec![
        run(E1, "COMPLETED", r#","queue_name":"agents""#),
        "{}".to_owned(),
        "{}".to_owned(),
        "{}".to_owned(),
        "{}".to_owned(),
        run(E2, "COMPLETED", r#","output":{"failures":0}"#),
        format!(r#"{{"workflows":[{{"id":"{E1}"}},{{"id":"{E2}"}}],"next_cursor":null}}"#),
        r#"{"events_scrubbed":1}"#.to_owned(),
        r#"{"events_scrubbed":1}"#.to_owned(),
    ];
    let (base_url, server) = scripted_server(script).await;
    let cli = Cli::try_parse_from([
        "harvest",
        "--base-url",
        &base_url,
        "agents",
        "erase",
        "support/ada",
    ])
    .expect("CLI args should parse");

    run_cli(cli).await.expect("erase should succeed");
    let requests = server.await.expect("server task should finish");
    let lines = first_lines(&requests);
    assert_eq!(requests.len(), 9, "{lines:?}");
    assert!(
        lines[1].starts_with("POST /api/harvest/workflows/agent/signal-with-start "),
        "{lines:?}"
    );
    for part in [
        r#""workflow_id":"support/ada""#,
        r#""signal_name":"harvest.entity.op""#,
        r#""queue":"agents""#,
        r#""action":"pause""#,
    ] {
        assert!(requests[1].contains(part), "{}", requests[1]);
    }
    assert_signals(
        &lines,
        &requests,
        2,
        &[
            r#""action":"reset_memory""#,
            r#""action":"reset_workspace""#,
            r#"{"kind":"delete"}"#,
        ],
    );
    for (n, exec) in [(7, E1), (8, E2)] {
        assert!(
            lines[n].starts_with(&format!(
                "POST /api/harvest/workflows/{exec}/erase-payloads "
            )),
            "{lines:?}"
        );
    }
}
