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
//! `Sec-Fetch-Site` falls back to the `Origin` header instead. That is
//! compared against the request's own host, and against its scheme when a
//! trusted reverse proxy reports one. `X-Forwarded-Host` and
//! `X-Forwarded-Proto` take precedence over `Host` and a bare request's own
//! scheme, respectively. A request with neither `Sec-Fetch-Site` nor
//! `Origin` is rejected — it is never admitted by
//! default.
//!
//! # Exemptions
//!
//! Four request shapes skip the check. None can be produced by a bare
//! cross-site `<form>` submission or a cross-site `no-cors` fetch. None
//! carries the ambient credential a forged request relies on, either:
//!
//! - **A method other than `POST`.** A `<form>` can only ever submit `GET`
//!   or `POST`. `PUT`, `PATCH`, and `DELETE` are not CORS-simple methods at
//!   all. A cross-site `fetch`/XHR in `no-cors` mode cannot send them, and
//!   any other mode needs a preflight this server does not answer. A route
//!   reached over `POST` through autumn-web's HTML method-override
//!   convention is still safe. That convention rewrites `POST` to
//!   `PUT`/`PATCH`/`DELETE` on a hidden `_method` form field, and the
//!   rewrite carries its own, stricter same-origin check upstream of this
//!   layer.
//! - **A non-CORS-simple `Content-Type`.** A `<form>` can only emit
//!   `application/x-www-form-urlencoded`, `multipart/form-data`, or
//!   `text/plain`. Anything else — `application/json`, most of all — needs a
//!   CORS preflight the attacker's page cannot pass.
//! - **No `Cookie` header at all.** CSRF is specifically the forgery of a
//!   request that rides on a cookie the browser attaches automatically. A
//!   cross-site page cannot set a `Cookie` header itself; the browser
//!   reserves it. So a forged request either carries the victim's real
//!   session cookie, or this layer never sees it as a threat at all. A
//!   non-browser caller — the `harvest` CLI among them — never sends a
//!   cookie either. This keeps first-party tooling working no matter which
//!   auth mechanism, if any, is configured.
//! - **A request carrying a [`TokenPrincipal`](crate::api_token::TokenPrincipal).**
//!   A verified scoped API token is an explicit credential. A browser never
//!   attaches one on its own, so it carries none of the ambient-cookie risk
//!   this layer defends against.
//!   [`require_harvest_admin`](crate::api::require_harvest_admin) draws the
//!   same line.

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
    if is_exempt(&request) || is_same_origin(request.headers()) {
        return next.run(request).await;
    }
    (StatusCode::FORBIDDEN, REJECTION_MESSAGE).into_response()
}

/// Requests this layer never inspects: non-`POST` methods, non-CORS-simple
/// bodies, cookieless callers, and callers already holding an explicit
/// bearer credential.
fn is_exempt(request: &Request) -> bool {
    request.method() != Method::POST
        || !is_cors_simple_content_type(request.headers())
        || !request.headers().contains_key(header::COOKIE)
        || request.extensions().get::<TokenPrincipal>().is_some()
}

/// Whether `headers` carries one of the three content types a plain HTML
/// `<form>` can send without a CORS preflight. A body-less form submission
/// carries no `Content-Type` at all, and counts as simple too.
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
fn is_same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("same-origin"));
    }

    let Some(origin) = headers.get(header::ORIGIN) else {
        return false;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let Some((origin_scheme, origin_authority)) = split_origin(origin) else {
        return false;
    };
    // A reverse proxy in front of this server rewrites `Host` to its own
    // upstream authority and forwards the browser's public host via
    // `X-Forwarded-Host`. Prefer that when present, falling back to `Host`
    // otherwise — the same precedence autumn-web's own method-override
    // same-origin check uses.
    let Some(host) = forwarded_host(headers).or_else(|| headers.get(header::HOST)?.to_str().ok())
    else {
        return false;
    };
    if !origin_authority.eq_ignore_ascii_case(host) {
        return false;
    }
    // A TLS-terminating proxy is the only reliable source for the scheme the
    // browser actually used; a plain request carries none. When it is
    // absent, fall back to the authority match alone — the same trade-off
    // autumn-web's own method-override same-origin check documents and
    // accepts.
    forwarded_scheme(headers).is_none_or(|expected| origin_scheme.eq_ignore_ascii_case(expected))
}

/// The public host a trusted reverse proxy reports via `X-Forwarded-Host`,
/// or `None` when the header is absent. Takes the leftmost value of a
/// comma-separated proxy chain, matching `X-Forwarded-For` convention.
fn forwarded_host(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get("x-forwarded-host")?.to_str().ok()?;
    let host = raw.split(',').next().unwrap_or(raw).trim();
    (!host.is_empty()).then_some(host)
}

/// Split an `Origin` header value into its scheme and authority
/// (`host[:port]`), e.g. `"https://dashboard.example"` -> `("https",
/// "dashboard.example")`. Returns `None` for an opaque origin (`"null"`) or
/// any value that is not `http://` or `https://`.
fn split_origin(origin: &str) -> Option<(&str, &str)> {
    origin
        .strip_prefix("https://")
        .map(|authority| ("https", authority))
        .or_else(|| {
            origin
                .strip_prefix("http://")
                .map(|authority| ("http", authority))
        })
}

/// The scheme a TLS-terminating proxy reports via `X-Forwarded-Proto`, or
/// `None` when the header is absent. Takes the leftmost value of a
/// comma-separated proxy chain, matching `X-Forwarded-For` convention.
fn forwarded_scheme(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get("x-forwarded-proto")?.to_str().ok()?;
    let scheme = raw.split(',').next().unwrap_or(raw).trim();
    (!scheme.is_empty()).then_some(scheme)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::{delete, get, post};
    use tower::ServiceExt as _;

    fn app() -> Router {
        Router::new()
            .route("/mutate", post(|| async { "ok" }))
            .route("/read", get(|| async { "ok" }))
            .route("/delete", delete(|| async { "ok" }))
            .layer(axum::middleware::from_fn(require_same_origin))
    }

    /// Every case below models a browser that already holds a session
    /// cookie, so `Cookie` is always present here. The one exemption
    /// covered separately is `cookieless_request_is_exempt`.
    async fn post_with_headers(headers: &[(&str, &str)]) -> StatusCode {
        let mut builder = HttpRequest::builder()
            .method("POST")
            .uri("/mutate")
            .header("cookie", "harvest_session=abc123");
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
    async fn http_origin_is_rejected_when_proxy_reports_https() {
        // A TLS-terminating proxy reports the real scheme via
        // `X-Forwarded-Proto`. An `http://` Origin on the same host is a
        // scheme downgrade, not the same origin, even though the host
        // matches.
        let status = post_with_headers(&[
            ("origin", "http://dashboard.example"),
            ("host", "dashboard.example"),
            ("x-forwarded-proto", "https"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn https_origin_passes_when_proxy_reports_https() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "dashboard.example"),
            ("x-forwarded-proto", "https"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn forwarded_proto_takes_the_leftmost_value_of_a_chain() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "dashboard.example"),
            ("x-forwarded-proto", "https, http"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn http_origin_passes_without_a_proxy_scheme_signal() {
        // No `X-Forwarded-Proto` means the scheme cannot be observed. Falling
        // back to the authority match alone matches autumn-web's own
        // method-override same-origin check.
        let status = post_with_headers(&[
            ("origin", "http://dashboard.example"),
            ("host", "dashboard.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn forwarded_host_is_preferred_over_a_rewritten_host() {
        // A reverse proxy can rewrite `Host` to its own internal upstream
        // authority while still forwarding the browser's public host.
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "harvest-upstream:3000"),
            ("x-forwarded-host", "dashboard.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn forwarded_host_takes_the_leftmost_value_of_a_chain() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "harvest-upstream:3000"),
            ("x-forwarded-host", "dashboard.example, edge.internal"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn mismatched_forwarded_host_is_rejected() {
        let status = post_with_headers(&[
            ("origin", "https://attacker.example"),
            ("host", "harvest-upstream:3000"),
            ("x-forwarded-host", "dashboard.example"),
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
    async fn host_without_origin_is_rejected() {
        let status = post_with_headers(&[("host", "dashboard.example")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn opaque_null_origin_is_rejected() {
        let status = post_with_headers(&[("origin", "null"), ("host", "dashboard.example")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn get_is_never_checked() {
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
    async fn delete_is_never_checked() {
        // DELETE is not a CORS-simple method. A cross-site `<form>` cannot
        // send it, and a cross-site `fetch`/XHR needs a preflight this
        // server does not answer. So this layer only ever checks POST.
        let response = app()
            .oneshot(
                HttpRequest::builder()
                    .method("DELETE")
                    .uri("/delete")
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
                    // A cookie is present too, so this proves the token
                    // bypass itself, not the separate cookieless exemption.
                    .header("cookie", "harvest_session=abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cookieless_request_is_exempt() {
        // The `harvest` CLI, and any other non-browser caller, never sends
        // a `Cookie` header. With no ambient credential to ride on, this is
        // not the threat this layer defends against.
        let response = app()
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
