#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autumn_web::reexports::axum::Router;
use autumn_web::reexports::axum::extract::{Path, State};
use autumn_web::reexports::axum::http::{HeaderMap, StatusCode};
use autumn_web::reexports::axum::routing::get;

use super::super::ObjectBackend;
use super::{
    GceMetadataToken, GcsBackend, GcsTokenSource, NoAuth, StaticToken, encode_object_name,
};

#[derive(Clone, Default)]
struct Seen {
    auth: Arc<Mutex<Vec<Option<String>>>>,
    hits: Arc<AtomicUsize>,
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        autumn_web::reexports::axum::serve(listener, router)
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

/// A fake GCS download route: `ok` returns bytes, any other name is 404.
fn fake_gcs(seen: Seen) -> Router {
    Router::new()
        .route(
            "/storage/v1/b/{bucket}",
            get(|Path(bucket): Path<String>| async move {
                match bucket.as_str() {
                    "b" => (StatusCode::OK, "{}"),
                    "denied" => (StatusCode::FORBIDDEN, "denied"),
                    _ => (StatusCode::NOT_FOUND, ""),
                }
            }),
        )
        .route(
            "/storage/v1/b/{bucket}/o/{name}",
            get(
                |State(seen): State<Seen>,
                 Path((_, name)): Path<(String, String)>,
                 headers: HeaderMap| async move {
                    let auth = headers
                        .get("authorization")
                        .map(|v| v.to_str().unwrap().to_string());
                    seen.auth.lock().unwrap().push(auth);
                    match name.as_str() {
                        "ok" => (StatusCode::OK, "bytes".to_string()),
                        "boom" => (StatusCode::FORBIDDEN, "denied".to_string()),
                        _ => (StatusCode::NOT_FOUND, String::new()),
                    }
                },
            ),
        )
        .with_state(seen)
}

#[test]
fn object_names_are_percent_encoded() {
    assert_eq!(
        encode_object_name("history/a b.json"),
        "history%2Fa%20b.json"
    );
    assert_eq!(encode_object_name("blobs/abc-_.~"), "blobs%2Fabc-_.~");
    assert_eq!(encode_object_name("../x?y#z"), "..%2Fx%3Fy%23z");
}

#[tokio::test]
async fn backend_sends_the_bearer_token() {
    let seen = Seen::default();
    let endpoint = serve(fake_gcs(seen.clone())).await;
    let backend = GcsBackend::new("b", StaticToken::new("tok-1")).with_endpoint(&endpoint);
    assert_eq!(backend.get("ok").await.unwrap().unwrap(), b"bytes");
    let auth = seen.auth.lock().unwrap().clone();
    assert_eq!(auth, vec![Some("Bearer tok-1".to_string())]);
}

#[tokio::test]
async fn no_auth_sends_no_header() {
    let seen = Seen::default();
    let endpoint = serve(fake_gcs(seen.clone())).await;
    let backend = GcsBackend::new("b", NoAuth).with_endpoint(&endpoint);
    backend.get("ok").await.unwrap();
    assert_eq!(seen.auth.lock().unwrap().clone(), vec![None]);
}

#[tokio::test]
async fn a_missing_object_is_none_and_other_errors_fail() {
    let endpoint = serve(fake_gcs(Seen::default())).await;
    let backend = GcsBackend::new("b", NoAuth).with_endpoint(&endpoint);
    assert!(backend.get("missing").await.unwrap().is_none());
    let err = backend.get("boom").await.unwrap_err();
    assert!(err.to_string().contains("403"), "{err}");
}

#[tokio::test]
async fn a_missing_bucket_is_an_error_not_a_missing_object() {
    let endpoint = serve(fake_gcs(Seen::default())).await;
    let backend = GcsBackend::new("no-such-bucket", NoAuth).with_endpoint(&endpoint);
    let err = backend.get("missing").await.unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test]
async fn a_failed_bucket_check_is_an_error_not_a_missing_object() {
    let endpoint = serve(fake_gcs(Seen::default())).await;
    let backend = GcsBackend::new("denied", NoAuth).with_endpoint(&endpoint);
    let err = backend.get("missing").await.unwrap_err();
    assert!(err.to_string().contains("403"), "{err}");
}

#[tokio::test]
async fn metadata_token_is_fetched_once_and_cached() {
    let seen = Seen::default();
    let router = Router::new()
        .route(
            "/computeMetadata/v1/instance/service-accounts/default/token",
            get(|State(seen): State<Seen>, headers: HeaderMap| async move {
                seen.hits.fetch_add(1, Ordering::SeqCst);
                if headers.get("metadata-flavor").is_none_or(|v| v != "Google") {
                    return (StatusCode::FORBIDDEN, String::new());
                }
                (
                    StatusCode::OK,
                    r#"{"access_token":"meta-tok","expires_in":3600,"token_type":"Bearer"}"#
                        .to_string(),
                )
            }),
        )
        .with_state(seen.clone());
    let endpoint = serve(router).await;
    let source = GceMetadataToken::new().with_endpoint(&endpoint);
    assert_eq!(source.token().await.unwrap().as_deref(), Some("meta-tok"));
    assert_eq!(source.token().await.unwrap().as_deref(), Some("meta-tok"));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1, "the token is cached");
}

#[tokio::test]
async fn metadata_token_refreshes_near_expiry() {
    let seen = Seen::default();
    let router = Router::new()
        .route(
            "/computeMetadata/v1/instance/service-accounts/default/token",
            get(|State(seen): State<Seen>| async move {
                seen.hits.fetch_add(1, Ordering::SeqCst);
                r#"{"access_token":"short","expires_in":0,"token_type":"Bearer"}"#
            }),
        )
        .with_state(seen.clone());
    let endpoint = serve(router).await;
    let source = GceMetadataToken::new().with_endpoint(&endpoint);
    source.token().await.unwrap();
    source.token().await.unwrap();
    assert_eq!(
        seen.hits.load(Ordering::SeqCst),
        2,
        "an expired token is not reused"
    );
}

#[tokio::test]
async fn bounded_get_refuses_a_large_object() {
    let endpoint = serve(fake_gcs(Seen::default())).await;
    let backend = GcsBackend::new("b", NoAuth).with_endpoint(&endpoint);
    assert_eq!(
        backend.get_bounded("ok", 5).await.unwrap().unwrap(),
        b"bytes"
    );
    let err = backend.get_bounded("ok", 4).await.unwrap_err();
    assert!(err.to_string().contains("read limit"), "{err}");
}
