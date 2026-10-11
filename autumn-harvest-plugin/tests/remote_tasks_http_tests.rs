//! The JSON-RPC remote task client against a stub server (issue #2006).
//!
//! No database. A local axum server plays the remote MCP or A2A server.

#![cfg(feature = "mcp")]

use std::sync::{Arc, Mutex};

use autumn_harvest::remote_task::{
    RemoteProtocol, RemoteTaskHandle, RemoteTaskOutcome, RemoteTaskRequest, RemoteTaskStart,
    RemoteTaskState, RemoteTaskTransport,
};
use autumn_harvest_plugin::remote_tasks::{HttpRemoteTasks, RemoteServer};
use autumn_web::reexports::axum::extract::State;
use autumn_web::reexports::axum::http::{HeaderMap, StatusCode};
use autumn_web::reexports::axum::response::{IntoResponse, Response};
use autumn_web::reexports::axum::routing::post;
use autumn_web::reexports::axum::{Json, Router};
use futures::StreamExt as _;
use serde_json::{Value, json};

/// One request that the stub saw.
#[derive(Debug, Clone)]
struct Seen {
    headers: HeaderMap,
    body: Value,
}

type Reply = Arc<dyn Fn(&Value) -> Response + Send + Sync>;

#[derive(Clone)]
struct Stub {
    seen: Arc<Mutex<Vec<Seen>>>,
    reply: Reply,
}

async fn rpc(State(stub): State<Stub>, headers: HeaderMap, Json(body): Json<Value>) -> Response {
    stub.seen.lock().expect("seen").push(Seen {
        headers,
        body: body.clone(),
    });
    (stub.reply)(&body)
}

/// Start a stub server. It answers each request with `reply`.
async fn serve(reply: Reply) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let stub = Stub {
        seen: Arc::clone(&seen),
        reply,
    };
    let app = Router::new().route("/rpc", post(rpc)).with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        autumn_web::reexports::axum::serve(listener, app)
            .await
            .expect("serve");
    });
    (format!("http://{addr}/rpc"), seen)
}

fn result(body: &Value, result: Value) -> Response {
    let mut reply = json!({"jsonrpc": "2.0", "id": body["id"]});
    reply["result"] = result;
    Json(reply).into_response()
}

fn request(protocol: RemoteProtocol, tool: &str) -> RemoteTaskRequest {
    RemoteTaskRequest {
        server: "reports".into(),
        protocol,
        tool: tool.into(),
        arguments: json!({"year": 2026}),
    }
}

fn header(seen: &Seen, name: &str) -> Option<String> {
    seen.headers
        .get(name)
        .map(|v| v.to_str().expect("ascii").to_string())
}

#[tokio::test]
async fn mcp_start_sends_a_task_tools_call_with_its_headers() {
    let (url, seen) = serve(Arc::new(|body| {
        result(
            body,
            json!({"resultType": "task", "taskId": "t-1", "status": "working"}),
        )
    }))
    .await;
    let client =
        HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url).bearer_token("secret"));

    let start = client
        .start(&request(RemoteProtocol::Mcp, "export"), "key-1")
        .await;
    assert_eq!(
        start,
        Ok(RemoteTaskStart::Task(RemoteTaskHandle {
            server: "reports".into(),
            protocol: RemoteProtocol::Mcp,
            task_id: "t-1".into(),
        }))
    );

    let seen = seen.lock().expect("seen")[0].clone();
    assert_eq!(seen.body["jsonrpc"], "2.0");
    assert_eq!(seen.body["method"], "tools/call");
    assert_eq!(seen.body["params"]["name"], "export");
    assert_eq!(
        seen.body["params"]["_meta"]["io.autumn-harvest/idempotencyKey"],
        "key-1"
    );
    assert_eq!(header(&seen, "idempotency-key").as_deref(), Some("key-1"));
    assert_eq!(
        header(&seen, "authorization").as_deref(),
        Some("Bearer secret")
    );
    assert_eq!(
        header(&seen, "mcp-protocol-version").as_deref(),
        Some("2026-07-28")
    );
    assert_eq!(header(&seen, "mcp-method").as_deref(), Some("tools/call"));
    assert_eq!(header(&seen, "mcp-name").as_deref(), Some("export"));
}

#[tokio::test]
async fn mcp_name_that_is_not_ascii_uses_the_base64_sentinel() {
    let (url, seen) = serve(Arc::new(|body| {
        result(body, json!({"resultType": "task", "taskId": "t-2"}))
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    client
        .start(&request(RemoteProtocol::Mcp, "exportér"), "key-2")
        .await
        .expect("start");
    let seen = seen.lock().expect("seen")[0].clone();
    let name = header(&seen, "mcp-name").expect("mcp-name");
    assert!(
        name.starts_with("=?base64?") && name.ends_with("?="),
        "got {name}"
    );
}

#[tokio::test]
async fn mcp_get_reads_an_is_error_result_as_completed() {
    let tool = json!({"content": [{"type": "text", "text": "bad"}], "isError": true});
    let reply = tool.clone();
    let (url, seen) = serve(Arc::new(move |body| {
        result(
            body,
            json!({"resultType": "complete", "taskId": "t-1", "status": "completed", "result": reply}),
        )
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let handle = RemoteTaskHandle {
        server: "reports".into(),
        protocol: RemoteProtocol::Mcp,
        task_id: "t-1".into(),
    };

    let state = client.get(&handle).await;
    assert_eq!(
        state,
        Ok(RemoteTaskState::Completed(RemoteTaskOutcome {
            result: tool,
            is_error: true,
        }))
    );
    let seen = seen.lock().expect("seen")[0].clone();
    assert_eq!(seen.body["method"], "tasks/get");
    assert_eq!(seen.body["params"]["taskId"], "t-1");
    assert_eq!(header(&seen, "mcp-method").as_deref(), Some("tasks/get"));
    assert_eq!(header(&seen, "mcp-name").as_deref(), Some("t-1"));
}

#[tokio::test]
async fn a_json_rpc_error_on_start_is_not_retryable() {
    let (url, _) = serve(Arc::new(|body| {
        Json(json!({"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32602, "message": "unknown tool"}}))
            .into_response()
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "nope"), "k")
        .await
        .expect_err("rpc error");
    assert!(!err.retryable);
    assert!(err.message.contains("unknown tool"), "got {}", err.message);
}

#[tokio::test]
async fn a_server_error_status_is_retryable() {
    let (url, _) = serve(Arc::new(|_| {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("503");
    assert!(err.retryable);
}

#[tokio::test]
async fn a_client_error_status_is_not_retryable() {
    let (url, _) = serve(Arc::new(|_| StatusCode::UNAUTHORIZED.into_response())).await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("401");
    assert!(!err.retryable);
}

/// A loopback URL with no listener: bind a port, then free it.
async fn closed_port_url() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    drop(listener);
    format!("http://{addr}/rpc")
}

#[tokio::test]
async fn an_unreachable_server_is_retryable() {
    let url = closed_port_url().await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("refused");
    assert!(err.retryable);
    assert!(
        !err.message.contains("127.0.0.1"),
        "no URL in {}",
        err.message
    );
}

#[tokio::test]
async fn a_json_rpc_error_behind_http_400_keeps_its_message() {
    let (url, _) = serve(Arc::new(|body| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32602, "message": "arguments.body is required"}})),
        )
            .into_response()
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("400");
    assert!(!err.retryable);
    assert!(
        err.message.contains("arguments.body is required"),
        "got {}",
        err.message
    );
}

#[tokio::test]
async fn a_response_over_the_limit_is_refused() {
    let (url, _) = serve(Arc::new(|_| {
        let big = "x".repeat(autumn_harvest_plugin::remote_tasks::MAX_RESPONSE_BYTES + 1);
        Json(json!({"jsonrpc": "2.0", "id": 1, "result": big})).into_response()
    }))
    .await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("too large");
    assert!(!err.retryable);
    assert!(err.message.contains("bytes"), "got {}", err.message);
}

#[tokio::test]
async fn a_redirect_to_another_origin_is_refused() {
    let (other, _) = serve(Arc::new(|body| {
        result(body, json!({"resultType": "task", "taskId": "t-9"}))
    }))
    .await;
    let target = other.clone();
    let (url, _) = serve(Arc::new(move |_| {
        (
            StatusCode::TEMPORARY_REDIRECT,
            [(
                autumn_web::reexports::axum::http::header::LOCATION,
                target.clone(),
            )],
        )
            .into_response()
    }))
    .await;
    // A caller client that follows redirects.
    let client = HttpRemoteTasks::with_client(reqwest::Client::new())
        .server("reports", RemoteServer::mcp(&url));
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("other origin");
    assert!(
        err.message.contains("another origin"),
        "got {}",
        err.message
    );
}

#[tokio::test]
async fn an_unknown_server_or_a_protocol_mismatch_is_not_retryable() {
    let url = closed_port_url().await;
    let client = HttpRemoteTasks::new().server("reports", RemoteServer::a2a(&url));
    let mut unknown = request(RemoteProtocol::Mcp, "export");
    unknown.server = "other".into();
    let err = client.start(&unknown, "k").await.expect_err("unknown");
    assert!(!err.retryable);
    let err = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect_err("mismatch");
    assert!(!err.retryable);
}

#[tokio::test]
async fn a2a_start_and_get_use_message_send_and_tasks_get() {
    let (url, seen) = serve(Arc::new(|body| match body["method"].as_str() {
        Some("message/send") => result(
            body,
            json!({"kind": "task", "id": "a-1", "status": {"state": "submitted"}}),
        ),
        _ => result(
            body,
            json!({"kind": "task", "id": "a-1", "status": {"state": "failed", "message": {"parts": [{"kind": "text", "text": "no"}]}}}),
        ),
    }))
    .await;
    let client = HttpRemoteTasks::new().server("agent", RemoteServer::a2a(&url));
    let mut req = request(RemoteProtocol::A2a, "summarise");
    req.server = "agent".into();

    let start = client.start(&req, "key-3").await.expect("start");
    let RemoteTaskStart::Task(handle) = start else {
        panic!("expected a task, got {start:?}");
    };
    assert_eq!(handle.task_id, "a-1");
    let state = client.get(&handle).await.expect("get");
    assert!(
        matches!(state, RemoteTaskState::Failed(ref m) if m.contains("no")),
        "got {state:?}"
    );

    let seen = seen.lock().expect("seen").clone();
    assert_eq!(seen[0].body["method"], "message/send");
    assert_eq!(seen[0].body["params"]["message"]["messageId"], "key-3");
    assert_eq!(
        header(&seen[0], "idempotency-key").as_deref(),
        Some("key-3")
    );
    assert_eq!(seen[1].body["method"], "tasks/get");
    assert_eq!(seen[1].body["params"]["id"], "a-1");
}

/// A server may keep an event stream open after the response. The client
/// stops reading at the first full response with its request id.
#[tokio::test]
async fn an_open_event_stream_returns_at_the_matching_response() {
    use autumn_web::reexports::axum::body::Body;
    let (url, _) = serve(Arc::new(|body| {
        let event = format!(
            "event: message\ndata: {}\n\n",
            json!({"jsonrpc": "2.0", "id": body["id"], "result": {"resultType": "task", "taskId": "t-s"}})
        );
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(event.into_bytes())])
            .chain(futures::stream::pending());
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream))
            .expect("response")
    }))
    .await;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let client = HttpRemoteTasks::with_client(client).server("reports", RemoteServer::mcp(&url));

    let began = std::time::Instant::now();
    let start = client
        .start(&request(RemoteProtocol::Mcp, "export"), "k")
        .await
        .expect("the response arrived before the stream ends");
    assert!(
        matches!(start, RemoteTaskStart::Task(ref h) if h.task_id == "t-s"),
        "{start:?}"
    );
    assert!(began.elapsed() < std::time::Duration::from_secs(4));
}
