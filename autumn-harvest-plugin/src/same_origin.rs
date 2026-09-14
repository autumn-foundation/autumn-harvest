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
//! compared against the request's own host (`X-Forwarded-Host`, when a
//! trusted reverse proxy reports one, takes precedence over `Host`) and
//! its scheme. A proxy-reported `X-Forwarded-Proto` must match `Origin`'s
//! scheme exactly. Without one, only `https` is accepted: a browser cannot
//! lie about `Origin`, but an unconfirmed `http` claim is indistinguishable
//! from a downgrade attack. A request with neither
//! `Sec-Fetch-Site` nor `Origin` is rejected — it is never admitted by
//! default.
//!
//! `Sec-Fetch-Site`, when present, is always decisive — checked before
//! anything else below, including `Content-Type`. A browser sends it on
//! *every* request it originates, not only `fetch`/XHR. Hyperlink auditing
//! (`<a ping>`) and `navigator.sendBeacon` are two mechanisms that reach a
//! server with a non-CORS-simple `Content-Type` and no preflight. So
//! treating "not CORS-simple" as proof of safety on its own would be
//! wrong. `Sec-Fetch-Site` closes that gap: a truthful `cross-site` (or
//! `same-site`, or `none`) rejects the request regardless of what its body
//! looks like.
//!
//! # Exemptions
//!
//! Three request shapes skip the check outright, before `Sec-Fetch-Site` is
//! even read. None can be produced by a bare cross-site `<form>`
//! submission or a cross-site `no-cors` fetch:
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
//! - **The `X-Harvest-Source: cli` header the `harvest` CLI sends on every
//!   mutation** (`autumn-harvest-cli/src/lib.rs`). A custom header is not
//!   CORS-safelisted. A bare cross-site `<form>` cannot add it at all, and a
//!   cross-site `fetch`/XHR that tries needs a preflight this server does
//!   not answer.
//! - **A request carrying a [`TokenPrincipal`](crate::api_token::TokenPrincipal).**
//!   A verified scoped API token is an explicit credential. A browser never
//!   attaches one on its own, so it carries none of the ambient-credential
//!   risk this layer defends against.
//!   [`require_harvest_admin`](crate::api::require_harvest_admin) draws the
//!   same line.
//!
//! A fourth shape is admitted only as a last resort. This applies after
//! `Sec-Fetch-Site` and `Origin` have both been checked, and neither was
//! present. The shape is a `Content-Type` of exactly `application/json`.
//! Documented, headerless `curl`-based API clients this router also serves
//! use exactly that value.
//!
//! A cross-site `fetch`/XHR sending `application/json` needs a CORS
//! preflight this server does not answer. `<a ping>` cannot send it at
//! all. The HTML standard fixes its content type to `text/ping`. So this
//! fallback only ever admits the one shape a real direct API client
//! produces, not an open set of "anything non-simple."
//!
//! A cross-site `sendBeacon` can set an attacker-chosen content type,
//! `application/json` included. It always carries `Origin` on a
//! cross-site call, though, regardless of `Sec-Fetch-Site` support. The
//! `Origin` check above catches it first, so it never falls through to
//! here.

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

/// Requests this layer never inspects: non-`POST` methods, the first-party
/// CLI's own requests, and callers already holding an explicit bearer
/// credential. Checked before `Sec-Fetch-Site`/`Origin`, unconditionally.
fn is_exempt(request: &Request) -> bool {
    request.method() != Method::POST
        || is_first_party_cli(request.headers())
        || request.extensions().get::<TokenPrincipal>().is_some()
}

/// Whether `headers` carries the `X-Harvest-Source: cli` header the
/// `harvest` CLI sends on every mutating request. Not CORS-safelisted, so
/// a cross-site forgery cannot add it.
fn is_first_party_cli(headers: &HeaderMap) -> bool {
    headers
        .get("x-harvest-source")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("cli"))
}

/// Whether `headers` declares `Content-Type: application/json`, the one
/// value a documented direct API client sends with no browser-origin
/// evidence at all. Admitted only as the last resort in [`is_same_origin`],
/// after `Sec-Fetch-Site` and `Origin` were both checked and neither was
/// present.
fn is_json_content_type(headers: &HeaderMap) -> bool {
    let Some(raw) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(raw) = raw.to_str() else {
        return false;
    };
    let media_type = raw.split(';').next().unwrap_or(raw).trim();
    media_type.eq_ignore_ascii_case("application/json")
}

/// The Fetch Metadata / `Origin` same-origin check. Pure function of the
/// request headers, so it is unit-testable without building a full request.
///
/// `Sec-Fetch-Site`, when present, is always the final word — checked
/// before `Origin` and never overridden by it. Only when *neither* header
/// is present does an `application/json` `Content-Type` admit the
/// request. See the module docs for why that order, and that one exact
/// value, matter (`<a ping>`/`sendBeacon`).
fn is_same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site
            .to_str()
            .is_ok_and(|s| s.eq_ignore_ascii_case("same-origin"));
    }

    let Some(origin) = headers.get(header::ORIGIN) else {
        return is_json_content_type(headers);
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
    // A TLS-terminating proxy is the only reliable source for the scheme
    // this server was actually reached over; a plain request carries none.
    // A confirmed scheme must match `Origin`'s claim exactly. Unconfirmed,
    // only `https` may proceed. A browser cannot lie about `Origin`, so
    // `https://dashboard.example` is proof the requesting page really was
    // HTTPS, regardless of what this server independently knows. `http`
    // gets no such benefit. Unconfirmed, it is indistinguishable from a
    // downgrade attack against an HTTPS deployment, so it is rejected
    // rather than assumed legitimate.
    let proxy_scheme = forwarded_scheme(headers);
    let scheme_confirmed = proxy_scheme.map_or_else(
        || origin_scheme.eq_ignore_ascii_case("https"),
        |expected| origin_scheme.eq_ignore_ascii_case(expected),
    );
    if !scheme_confirmed {
        return false;
    }
    let effective_scheme = proxy_scheme.unwrap_or(origin_scheme);
    // A browser never includes a default port in `Origin`. A `Host` /
    // `X-Forwarded-Host` value that spells one out explicitly
    // (`dashboard.example:443` over `https`) is still the same origin.
    strip_default_port(origin_authority, effective_scheme)
        .eq_ignore_ascii_case(strip_default_port(host, effective_scheme))
}

/// Strip a trailing `:80` (`http`) or `:443` (`https`) from `authority` when
/// it matches `scheme`'s default port, so `"dashboard.example:443"` and
/// `"dashboard.example"` compare equal under `https`.
fn strip_default_port<'a>(authority: &'a str, scheme: &str) -> &'a str {
    let default_port_suffix = if scheme.eq_ignore_ascii_case("https") {
        ":443"
    } else if scheme.eq_ignore_ascii_case("http") {
        ":80"
    } else {
        return authority;
    };
    authority
        .strip_suffix(default_port_suffix)
        .unwrap_or(authority)
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
    async fn http_origin_is_rejected_without_a_proxy_scheme_signal() {
        // Regression (Codex): with no `X-Forwarded-Proto`, this server
        // cannot independently confirm the scheme. An `http` claim gets no
        // benefit of the doubt here, since it is indistinguishable from a
        // downgrade attack against an HTTPS deployment.
        let status = post_with_headers(&[
            ("origin", "http://dashboard.example"),
            ("host", "dashboard.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn https_origin_passes_without_a_proxy_scheme_signal() {
        // A browser cannot lie about `Origin`. `https://dashboard.example`
        // is proof the requesting page really was HTTPS, so it needs no
        // proxy confirmation the way an `http` claim does.
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
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
    async fn ping_content_type_without_fetch_metadata_or_origin_is_rejected() {
        // Regression (Codex): the last-resort content-type fallback used to
        // admit any non-CORS-simple type, not only `application/json`. An
        // `<a ping>` request can reach this server with neither
        // `Sec-Fetch-Site` nor `Origin` present. An old browser, or an
        // intermediary that strips both headers, can cause this. Such a
        // request carries `Content-Type: text/ping`, a value no real
        // direct API client ever sends. That must stay rejected rather
        // than fall through as "not simple, so safe."
        let status = post_with_headers(&[("content-type", "text/ping")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cross_site_fetch_metadata_beats_a_non_simple_content_type() {
        // Regression (Codex): hyperlink auditing (`<a ping>`) and
        // `navigator.sendBeacon` reach a server with a non-CORS-simple
        // `Content-Type` and no preflight, credentialed, and still carry a
        // truthful `Sec-Fetch-Site`. That must reject the request even
        // though its content type alone would otherwise admit it.
        let status = post_with_headers(&[
            ("sec-fetch-site", "cross-site"),
            ("content-type", "text/ping"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn same_origin_fetch_metadata_beats_a_non_simple_content_type() {
        let status = post_with_headers(&[
            ("sec-fetch-site", "same-origin"),
            ("content-type", "text/ping"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn mismatched_origin_beats_a_non_simple_content_type() {
        let status = post_with_headers(&[
            ("origin", "https://attacker.example"),
            ("host", "dashboard.example"),
            ("content-type", "text/ping"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
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

    #[tokio::test]
    async fn first_party_cli_header_is_exempt() {
        let status = post_with_headers(&[("x-harvest-source", "cli")]).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn first_party_cli_header_match_is_case_insensitive() {
        let status = post_with_headers(&[("x-harvest-source", "CLI")]).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn unrecognised_source_header_value_is_still_checked() {
        let status = post_with_headers(&[("x-harvest-source", "browser")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cookieless_request_without_the_cli_header_is_still_checked() {
        // A cross-site `<form>` submission to a target guarded by
        // browser-cached HTTP Basic/Digest auth carries that ambient
        // credential too, with no `Cookie` header at all. So cookielessness
        // alone must never be enough to skip this check.
        let status = post_with_headers(&[]).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn origin_with_explicit_default_port_matches_host_without_one() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "dashboard.example:443"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn origin_with_explicit_default_port_matches_forwarded_host() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "harvest-upstream:3000"),
            ("x-forwarded-host", "dashboard.example:443"),
            ("x-forwarded-proto", "https"),
        ])
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn non_default_port_still_must_match() {
        let status = post_with_headers(&[
            ("origin", "https://dashboard.example"),
            ("host", "dashboard.example:8443"),
        ])
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
}
