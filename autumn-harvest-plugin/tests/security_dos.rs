use autumn_harvest_plugin::api::{HarvestApiState, harvest_api_router};
use autumn_web::AppState;
use autumn_web::reexports::axum::body::Body;
use autumn_web::reexports::http::{Method, Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn test_dos_memory_exhaustion_vulnerability() {
    let api_state = HarvestApiState::new();
    api_state.set_admin_auth_boundary(true); // Bypass 401

    let app = harvest_api_router(api_state).with_state(AppState::for_test());

    // Test payload larger than default limits to ensure memory exhaustion is prevented.
    let massive_body = vec![b'x'; 26 * 1024 * 1024];

    let request = Request::builder()
        .method(Method::POST)
        .uri("/workflows/batch_start")
        .header("content-type", "application/json")
        .body(Body::from(massive_body))
        .unwrap();

    let res = app.oneshot(request).await.unwrap();
    let status = res.status();

    let body_bytes = autumn_web::reexports::axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    // We expect it to be protected, so it should NOT return the specific vulnerable message
    // that assumes it read the whole stream ("exceeds max_total_bytes").
    // It should instead return an error indicating `to_bytes` failed.
    assert!(
        !body_str.contains("exceeds max_total_bytes"),
        "Vulnerable to memory exhaustion because it read the entire body before rejecting it!"
    );
}
