//! Cross-site request rejection for cookie-authenticated mutations (issue #1278).
//!
//! Vantage forms submit as `application/x-www-form-urlencoded`. A browser
//! sends that content type as a CORS-simple request: no preflight, and the
//! operator's session cookie rides along automatically. A hostile page can
//! therefore submit a cross-origin `<form>` at a Vantage mutation and the
//! request arrives authenticated. This layer closes that gap.
//!
//! # Method
//!
//! Modern browsers send `Sec-Fetch-Site` on every request. Only
//! `same-origin` passes; `cross-site`, `same-site`, and `none` do not, since
//! any of them can carry a forged form submission. A browser that omits
//! `Sec-Fetch-Site` falls back to the `Origin` header, compared against the
//! request's own `Host`. A request with neither header is rejected — it is
//! never admitted by default.
//!
//! # Exemptions
//!
//! Two request shapes skip the check, because neither can be produced by a
//! bare cross-site `<form>` submission:
//!
//! - **A non-CORS-simple `Content-Type`.** A `<form>` can only emit
//!   `application/x-www-form-urlencoded`, `multipart/form-data`, or
//!   `text/plain`. Anything else — `application/json`, most of all — needs a
//!   CORS preflight the attacker's page cannot pass.
//! - **A request carrying a [`TokenPrincipal`](crate::api_token::TokenPrincipal).**
//!   A verified scoped API token is an explicit credential a browser never
//!   attaches on its own, so it carries none of the ambient-cookie risk this
//!   layer defends against. [`require_harvest_admin`](crate::api::require_harvest_admin)
//!   draws the same line.

use autumn_web::reexports::axum;
use axum::extract::Request;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::api_token::TokenPrincipal;

const REJECTION_MESSAGE: &str = "cross-site request rejected";

/// Axum middleware: reject a mutating request with no same-origin evidence.
///
/// Mount with `axum::middleware::from_fn`. Carries no state, so it composes
/// with `require_harvest_admin` in either order.
pub(crate) async fn require_same_origin(request: Request, next: Next) -> Response {
    if is_exempt(&request) || passes_same_origin_check(request.headers()) {
        return next.run(request).await;
    }
    (StatusCode::FORBIDDEN, REJECTION_MESSAGE).into_response()
}

/// Requests this layer never inspects: safe methods, non-CORS-simple bodies,
/// and callers already holding an explicit bearer credential.
fn is_exempt(request: &Request) -> bool {
    is_safe_method(request.method())
        || !is_cors_simple_content_type(request.headers())
        || request.extensions().get::<TokenPrincipal>().is_some()
}

const fn is_safe_method(method: &Method) -> bool {
    matches!(
        method,
        &Method::GET | &Method::HEAD | &Method::OPTIONS | &Method::TRACE
    )
}

/// Whether `headers` carries one of the three content types a plain HTML
/// `<form>` can send without a CORS preflight (or carries none at all — a
/// bodyless form submission still counts as simple).
fn is_cors_simple_content_type(headers: &HeaderMap) -> bool {
    let Some(raw) = headers.get(header::CONTENT_TYPE) else {
        return true;
    };
    let Ok(raw) = raw.to_str() else {
        return true;
    };
    let media_type = raw.split(';').next().unwrap_or(raw).trim();
    media_type.eq_ignore_ascii_case("application/x-www-form-urlencoded")
        || media_type.eq_ignore_ascii_case("multipart/form-data")
        || media_type.eq_ignore_ascii_case("text/plain")
}

/// The Fetch Metadata / `Origin` same-origin check. Pure function of the
/// request headers, so it is unit-testable without building a full request.
fn passes_same_origin_check(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("same-origin"));
    }

    let (Some(origin), Some(host)) = (headers.get(header::ORIGIN), headers.get(header::HOST))
    else {
        return false;
    };
    let (Ok(origin), Ok(host)) = (origin.to_str(), host.to_str()) else {
        return false;
    };
    let authority = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"));
    authority.is_some_and(|a| a.eq_ignore_ascii_case(host))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::{get, post};
    use tower::ServiceExt as _;

    fn app() -> Router {
        Router::new()
            .route("/mutate", post(|| async { "ok" }))
            .route("/read", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_same_origin))
    }

    async fn post_with_headers(headers: &[(&str, &str)]) -> StatusCode {
        let mut builder = HttpRequest::builder().method("POST").uri("/mutate");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = app()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        response.status()
    }

    #[tokio::test]
    async fn same_origin_fetch_metadata_passes() {
        let status = post_with_headers(&[("sec-fetch-site", "same-origin")]).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn cross_site_fetch_metadata_is_rejected() {
        let status = post_with_headers(&[("sec-fetch-site", "cross-site")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn same_site_fetch_metadata_is_rejected() {
        // A sibling subdomain can still hold a copy of a `Domain`-scoped
        // session cookie, so `same-site` gets no more trust than `cross-site`.
        let status = post_with_headers(&[("sec-fetch-site", "same-site")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn none_fetch_metadata_is_rejected() {
        let status = post_with_headers(&[("sec-fetch-site", "none")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn matching_origin_passes_without_fetch_metadata() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "dashboard.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn mismatched_origin_is_rejected() {
        let status = post_with_headers(&[
            ("origin", "https://attacker.example"),
            ("host", "dashboard.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn neither_header_is_rejected() {
        let status = post_with_headers(&[]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn origin_without_host_is_rejected() {
        let status = post_with_headers(&[("origin", "https://dashboard.example")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn opaque_null_origin_is_rejected() {
        let status = post_with_headers(&[("origin", "null"), ("host", "dashboard.example")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn safe_method_is_never_checked() {
        let response = app()
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/read")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn json_content_type_skips_the_check() {
        // application/json cannot be sent by a bare cross-site <form> without
        // a CORS preflight, so it carries none of the risk this layer guards.
        let status = post_with_headers(&[("content-type", "application/json")]).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn urlencoded_content_type_is_checked() {
        let status =
            post_with_headers(&[("content-type", "application/x-www-form-urlencoded")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn text_plain_content_type_is_checked() {
        // A JSON-API polyglot can be smuggled through `text/plain`, so it
        // gets the same enforcement as urlencoded and multipart bodies.
        let status = post_with_headers(&[("content-type", "text/plain")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn multipart_content_type_is_checked() {
        let status =
            post_with_headers(&[("content-type", "multipart/form-data; boundary=----x")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn token_principal_bypasses_the_check() {
        async fn mutate_with_token(mut req: Request, next: Next) -> Response {
            req.extensions_mut().insert(TokenPrincipal {
                id: uuid::Uuid::nil(),
                scope: crate::api_token::TokenScope::Mutate,
            });
            next.run(req).await
        }

        let app = Router::new()
            .route("/mutate", post(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_same_origin))
            .layer(axum::middleware::from_fn(mutate_with_token));

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/mutate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
