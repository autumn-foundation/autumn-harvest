//! The optional API rate limiter on a standalone mount (issue #1827).
//!
//! No database is required. Each client is a peer address that a test layer
//! stamps as `ConnectInfo`, or a resolved autumn-web `ClientAddr`.
//! `api_rate_limit_integration.rs` carries the database-backed proof for
//! verified tokens, the metric and the audit row.

use std::net::{IpAddr, SocketAddr};

use autumn_harvest_plugin::api::{HarvestApiState, StandaloneAdminAuth, harvest_api_router};
use autumn_harvest_plugin::api_rate_limit::{ApiRateLimit, BucketRate};
use autumn_harvest_plugin::harvest_ui_router;
use autumn_web::reexports::axum;
use autumn_web::security::ResolvedClientIdentity;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request};
use axum::http::{Method, StatusCode};
use axum::middleware::Next;
use tower::ServiceExt;

const START: &str = "/api/harvest/workflows/billing/start";
const READ: &str = "/api/harvest/workflows/registered";

/// Ten mutating and ten read requests a second, with a burst of ten.
const fn ten_per_second() -> ApiRateLimit {
    ApiRateLimit::new(BucketRate::per_second(10), BucketRate::per_second(10))
}

/// A standalone mount with an embedder boundary, so auth never answers first.
fn app(auth: StandaloneAdminAuth) -> axum::Router {
    let api_state = HarvestApiState::new();
    let router =
        harvest_api_router(api_state.clone()).nest("/ui", harvest_ui_router(api_state.clone()));
    let mounted = auth.with_admin_auth_boundary().mount(router, &api_state);
    axum::Router::new().nest("/api/harvest", mounted)
}

/// How a test request names its client.
#[derive(Clone, Copy)]
enum Peer {
    /// A `ConnectInfo` peer address, as a plain TCP listener sets it.
    Connect(&'static str),
    /// An autumn-web `ClientAddr` from the trusted-proxy layer, over a proxy
    /// peer address.
    Resolved {
        client: &'static str,
        proxy: &'static str,
    },
}

fn request(method: &Method, uri: &str, peer: Peer) -> Request {
    let mut request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .expect("request should build");
    let (connect, resolved) = match peer {
        Peer::Connect(addr) => (addr, None),
        Peer::Resolved { client, proxy } => (proxy, Some(client)),
    };
    let ip: IpAddr = connect.parse().expect("test address should parse");
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::new(ip, 40_000)));
    if let Some(client) = resolved {
        request.extensions_mut().insert(ResolvedClientIdentity {
            addr: Some(client.parse().expect("test address should parse")),
            host: None,
            scheme: None,
        });
    }
    request
}

async fn send(
    app: &axum::Router,
    method: &Method,
    uri: &str,
    peer: Peer,
) -> axum::response::Response {
    app.clone()
        .oneshot(request(method, uri, peer))
        .await
        .expect("router should serve the request")
}

/// Send `n` requests and count the `429` answers.
async fn count_429(app: &axum::Router, method: &Method, uri: &str, peer: Peer, n: usize) -> usize {
    let mut limited = 0;
    for _ in 0..n {
        if send(app, method, uri, peer).await.status() == StatusCode::TOO_MANY_REQUESTS {
            limited += 1;
        }
    }
    limited
}

/// With no limiter declared, a burst is never refused with 429.
#[tokio::test(start_paused = true)]
async fn the_limiter_is_off_by_default() {
    let app = app(StandaloneAdminAuth::new());

    let limited = count_429(&app, &Method::POST, START, Peer::Connect("192.0.2.1"), 100).await;

    assert_eq!(limited, 0);
}

/// AC1 for clients with no token: one client address is limited, another is
/// not.
#[tokio::test(start_paused = true)]
async fn a_burst_from_one_address_is_limited_and_another_address_is_not() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));

    let limited = count_429(&app, &Method::POST, START, Peer::Connect("192.0.2.1"), 100).await;
    let other = count_429(&app, &Method::POST, START, Peer::Connect("192.0.2.2"), 10).await;

    assert_eq!(limited, 90, "a bucket of 10 admits 10 of 100");
    assert_eq!(other, 0, "another client keeps its own bucket");
}

/// The 429 carries `Retry-After` in whole seconds and a JSON body.
#[tokio::test(start_paused = true)]
async fn a_rejection_carries_retry_after_and_a_json_body() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let peer = Peer::Connect("192.0.2.1");
    count_429(&app, &Method::POST, START, peer, 10).await;

    let response = send(&app, &Method::POST, START, peer).await;

    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let retry_after: u64 = response
        .headers()
        .get(axum::http::header::RETRY_AFTER)
        .expect("a 429 carries Retry-After")
        .to_str()
        .expect("Retry-After is ASCII")
        .parse()
        .expect("Retry-After is whole seconds");
    assert!(retry_after >= 1, "Retry-After is never zero");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("body is JSON");
    assert_eq!(json["error"], "rate limited");
    assert_eq!(json["route_class"], "mutating");
    assert_eq!(json["retry_after_secs"], retry_after);
}

/// A bucket refills with time, so a client that waits is served again.
#[tokio::test(start_paused = true)]
async fn a_limited_client_is_served_after_it_waits() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let peer = Peer::Connect("192.0.2.1");
    assert_eq!(count_429(&app, &Method::POST, START, peer, 11).await, 1);

    tokio::time::advance(std::time::Duration::from_secs(1)).await;

    assert_eq!(count_429(&app, &Method::POST, START, peer, 10).await, 0);
}

/// Mutating and read routes use separate buckets.
#[tokio::test(start_paused = true)]
async fn read_routes_have_their_own_bucket() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let peer = Peer::Connect("192.0.2.1");
    count_429(&app, &Method::POST, START, peer, 20).await;

    let reads = count_429(&app, &Method::GET, READ, peer, 10).await;
    let read_burst = count_429(&app, &Method::GET, READ, peer, 10).await;

    assert_eq!(reads, 0, "an empty mutating bucket leaves reads alone");
    assert_eq!(read_burst, 10, "the read bucket has its own limit");
}

/// A Vantage page has no route class. A `GET` counts as a read.
#[tokio::test(start_paused = true)]
async fn an_unclassified_get_counts_as_a_read() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let peer = Peer::Connect("192.0.2.1");
    count_429(&app, &Method::POST, START, peer, 20).await;

    let limited = count_429(&app, &Method::GET, "/api/harvest/ui/", peer, 10).await;

    assert_eq!(limited, 0);
}

/// Health probes and `OPTIONS` preflights are never limited.
#[tokio::test(start_paused = true)]
async fn health_probes_and_preflights_are_exempt() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let peer = Peer::Connect("192.0.2.1");

    for uri in [
        "/api/harvest/health",
        "/api/harvest/health/live",
        "/api/harvest/health/ready",
    ] {
        assert_eq!(
            count_429(&app, &Method::GET, uri, peer, 50).await,
            0,
            "{uri}"
        );
    }
    assert_eq!(count_429(&app, &Method::OPTIONS, START, peer, 50).await, 0);
}

/// Behind a trusted proxy, the resolved client address is the key, not the
/// proxy address.
#[tokio::test(start_paused = true)]
async fn the_resolved_client_address_is_the_key() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let first = Peer::Resolved {
        client: "198.51.100.1",
        proxy: "10.0.0.1",
    };
    let second = Peer::Resolved {
        client: "198.51.100.2",
        proxy: "10.0.0.1",
    };

    let limited = count_429(&app, &Method::POST, START, first, 20).await;
    let other = count_429(&app, &Method::POST, START, second, 10).await;

    assert_eq!(limited, 10);
    assert_eq!(
        other, 0,
        "two clients behind one proxy keep separate buckets"
    );
}

/// Addresses in one IPv6 /64 share a bucket.
#[tokio::test(start_paused = true)]
async fn one_ipv6_prefix_shares_a_bucket() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));

    count_429(&app, &Method::POST, START, Peer::Connect("2001:db8::1"), 10).await;
    let limited = count_429(&app, &Method::POST, START, Peer::Connect("2001:db8::2"), 10).await;

    assert_eq!(limited, 10);
}

/// Without any address, every caller shares one bucket rather than going
/// unlimited.
#[tokio::test(start_paused = true)]
async fn a_request_with_no_address_is_still_limited() {
    let app = app(StandaloneAdminAuth::new().with_rate_limit(ten_per_second()));
    let mut limited = 0;
    for _ in 0..20 {
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(START)
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .expect("request should build");
        let status = app
            .clone()
            .oneshot(request)
            .await
            .expect("router should serve the request")
            .status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            limited += 1;
        }
    }

    assert_eq!(limited, 10);
}

/// The limiter runs before the route handlers. A tagged inner layer proves no
/// rejected request reaches them.
#[tokio::test(start_paused = true)]
async fn a_rejected_request_never_reaches_a_handler() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let reached = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reached);
    let api_state = HarvestApiState::new();
    let router = harvest_api_router(api_state.clone()).layer(axum::middleware::from_fn(
        move |request: Request, next: Next| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                next.run(request).await
            }
        },
    ));
    let mounted = StandaloneAdminAuth::new()
        .with_admin_auth_boundary()
        .with_rate_limit(ten_per_second())
        .mount(router, &api_state);
    let app = axum::Router::new().nest("/api/harvest", mounted);

    let limited = count_429(&app, &Method::POST, START, Peer::Connect("192.0.2.1"), 30).await;

    assert_eq!(limited, 20);
    assert_eq!(reached.load(Ordering::SeqCst), 10);
}
