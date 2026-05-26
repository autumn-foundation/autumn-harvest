import re
with open("autumn-harvest-plugin/tests/dlq_bulk_integration.rs", "r") as f:
    content = f.read()

new_func = """
#[allow(dead_code)]
async fn post_large_json(
    app: &axum::Router,
    path: &str,
    body: serde_json::Value,
) -> (axum::http::StatusCode, serde_json::Value) {
    let mut s = serde_json::to_string(&body).unwrap();
    s.push_str(&" ".repeat(3 * 1024 * 1024)); // > 2MB

    let request = axum::http::Request::builder()
        .method(axum::http::Method::POST)
        .uri(path)
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(s))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}
"""

if "async fn post_large_json(" not in content:
    content = content.replace("#[tokio::test]\nasync fn bulk_replay_with_empty_filter_returns_400() {", new_func + "\n#[tokio::test]\nasync fn bulk_replay_with_empty_filter_returns_400() {")

test = """
#[tokio::test]
async fn bulk_replay_with_large_payload_returns_400() {
    let (database_url, _container) = setup_test_database_url().await;
    let app = build_dlq_app(build_test_pool(&database_url));

    let (status, _) = post_large_json(&app, "/dead-letters/replay", json!({ "activity_name": "preview_task" })).await;

    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
}
"""

if "async fn bulk_replay_with_large_payload_returns_400() {" not in content:
    content = content.replace("#[tokio::test]\nasync fn bulk_replay_with_invalid_json_returns_400() {", test + "\n#[tokio::test]\nasync fn bulk_replay_with_invalid_json_returns_400() {")

with open("autumn-harvest-plugin/tests/dlq_bulk_integration.rs", "w") as f:
    f.write(content)
