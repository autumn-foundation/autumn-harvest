//! The scoped API token checks that both backends keep (issue #942, #2469).
//!
//! `tests/token_auth_integration.rs` runs them on Postgres. The SQLite suite
//! runs them on a SQLite app. Each harness builds its own apps and reads its
//! own store. The checks read audit rows through `GET /admin/audit`, so the
//! audit route is under test on both backends.
//!
//! The checks cover:
//!   - create returns the secret exactly once; the store keeps only the hash;
//!   - `GET /admin/tokens` returns metadata only;
//!   - a minted `hvst_` bearer authenticates a read route;
//!   - revocation and expiry take effect on the next request;
//!   - a `read` token gets `403` on every mutating route and reaches the
//!     read routes;
//!   - a `mutate` token reaches an admin-gated mutation with no boundary;
//!   - a token-authed mutation audits as `token:{id}`, and a spoofed
//!     `x-harvest-actor` cannot change it;
//!   - an unknown `hvst_` bearer is `401`;
//!   - an offline-seeded token works as a minted one does.

// Each test binary uses a different subset of the checks.
#![allow(dead_code)]
// Each check runs on the test task. No future crosses a thread.
#![allow(clippy::future_not_send)]
// A trait in a test binary needs no `Send` bound on its futures.
#![allow(async_fn_in_trait)]
// "actor must be token:{{id}}" holds literal braces, not a format argument.
#![allow(clippy::literal_string_with_formatting_args)]

use autumn_harvest::audit::{CLASSIFIED_ROUTES, RouteClass};
use autumn_web::reexports::axum;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

/// The `Host` of each request. An app in `prod` refuses a request with none.
pub const HOST: &str = "harvest.test";

/// One backend that serves the token checks.
///
/// Both apps share one token store. Each starts empty.
pub trait TokenBackend {
    /// The path that each route is under, or `""`.
    fn prefix(&self) -> &str;
    /// The app with tokens on and a declared admin boundary.
    async fn boundary(&mut self) -> axum::Router;
    /// The app with tokens on and no boundary, in a non-`dev` profile.
    async fn standalone(&mut self) -> axum::Router;
    /// The `(name, token_hash)` of every stored token.
    async fn stored_hashes(&self) -> Vec<(String, String)>;
    /// Store an `admin` token as the offline bootstrap does. Returns its id.
    async fn seed_admin_token(&self, name: &str, token_hash: &str) -> String;
}

/// One request.
pub struct Call<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub body: Option<Value>,
    pub bearer: Option<&'a str>,
    pub actor: Option<&'a str>,
}

impl<'a> Call<'a> {
    pub const fn new(method: &'a str, path: &'a str) -> Self {
        Self {
            method,
            path,
            body: None,
            bearer: None,
            actor: None,
        }
    }

    #[must_use]
    pub fn body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    #[must_use]
    pub const fn bearer(mut self, bearer: &'a str) -> Self {
        self.bearer = Some(bearer);
        self
    }

    #[must_use]
    pub const fn actor(mut self, actor: &'a str) -> Self {
        self.actor = Some(actor);
        self
    }
}

/// Send `call` under `prefix`.
///
/// Each call also carries `x-harvest-admin: true`, as the original suite
/// did. No Harvest code reads that header. It shows that a header never
/// grants admin access.
pub async fn send(app: &axum::Router, prefix: &str, call: Call<'_>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(call.method)
        .uri(format!("{prefix}{}", call.path))
        .header("host", HOST)
        .header("x-harvest-admin", "true");
    if call.body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    if let Some(bearer) = call.bearer {
        builder = builder.header("authorization", format!("Bearer {bearer}"));
    }
    if let Some(actor) = call.actor {
        builder = builder.header("x-harvest-actor", actor);
    }
    let request = builder
        .body(
            call.body
                .map_or_else(Body::empty, |b| Body::from(b.to_string())),
        )
        .unwrap();
    let response = app.clone().oneshot(request).await.expect("request");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
    };
    (status, json)
}

/// Send a `GET` with a raw `Authorization` value. Returns the status.
async fn send_with_raw_auth(
    app: &axum::Router,
    prefix: &str,
    path: &str,
    authorization: &str,
) -> StatusCode {
    let request = Request::builder()
        .method("GET")
        .uri(format!("{prefix}{path}"))
        .header("host", HOST)
        .header("authorization", authorization)
        .body(Body::empty())
        .unwrap();
    app.clone()
        .oneshot(request)
        .await
        .expect("request")
        .status()
}

/// Mint a token on `app`, and return its secret.
pub async fn mint(
    app: &axum::Router,
    prefix: &str,
    name: &str,
    scope: &str,
    expires_at: Option<&str>,
) -> String {
    let mut body = json!({ "name": name, "scope": scope });
    if let Some(e) = expires_at {
        body["expires_at"] = json!(e);
    }
    let (status, resp) = send(app, prefix, Call::new("POST", "/admin/tokens").body(body)).await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201: {resp:?}");
    resp["secret"]
        .as_str()
        .expect("mint response carries a secret")
        .to_string()
}

/// The id of the listed token named `name`.
async fn token_id(app: &axum::Router, prefix: &str, name: &str) -> String {
    let (status, list) = send(app, prefix, Call::new("GET", "/admin/tokens")).await;
    assert_eq!(status, StatusCode::OK, "{list:?}");
    list.as_array()
        .expect("a token list")
        .iter()
        .find(|t| t["name"] == name)
        .unwrap_or_else(|| panic!("no token {name}: {list}"))["id"]
        .as_str()
        .unwrap()
        .to_string()
}

/// The actors of the audit rows of `operation`, read through the API.
pub async fn audit_actors(app: &axum::Router, prefix: &str, operation: &str) -> Vec<String> {
    let (status, rows) = send(
        app,
        prefix,
        Call::new(
            "GET",
            &format!("/admin/audit?operation={operation}&limit=500"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "audit read: {rows:?}");
    rows.as_array()
        .expect("an audit list")
        .iter()
        .map(|row| {
            assert_eq!(row["operation"], operation, "{row}");
            row["actor"].as_str().expect("an actor").to_string()
        })
        .collect()
}

/// The `{param}` segments of a route template, filled with `x`.
fn concrete(template: &str) -> String {
    template
        .split('/')
        .map(|seg| {
            if seg.starts_with('{') && seg.ends_with('}') {
                "x"
            } else {
                seg
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

pub async fn create_returns_secret_once_and_stores_only_hash(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let (status, resp) = send(
        &app,
        &prefix,
        Call::new("POST", "/admin/tokens").body(json!({ "name": "ci-bot", "scope": "read" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "resp: {resp:?}");
    let secret = resp["secret"].as_str().expect("secret in create response");
    assert!(secret.starts_with("hvst_"), "secret must be hvst_-prefixed");
    assert_eq!(resp["scope"], "read");
    assert_eq!(resp["name"], "ci-bot");
    assert!(resp.get("token_hash").is_none(), "{resp}");

    let rows = backend.stored_hashes().await;
    assert_eq!(rows.len(), 1);
    assert_ne!(rows[0].1, secret, "hash must not equal the secret");
    assert!(
        !rows[0].1.contains(secret),
        "stored hash must not contain the plaintext"
    );
    assert_eq!(
        rows[0].1,
        autumn_harvest::api_token::hash_secret(secret),
        "the store holds the shared hash of the secret"
    );
}

pub async fn list_returns_metadata_only(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "lister", "read", None).await;

    let (status, list) = send(&app, &prefix, Call::new("GET", "/admin/tokens")).await;
    assert_eq!(status, StatusCode::OK);
    let arr = list.as_array().expect("list is an array");
    assert_eq!(arr.len(), 1);
    let entry = &arr[0];
    assert!(entry.get("id").is_some());
    assert_eq!(entry["name"], "lister");
    assert_eq!(entry["scope"], "read");
    let text = entry.to_string();
    assert!(!text.contains(&secret), "list must not contain the secret");
    assert!(!text.contains("token_hash"), "list must not expose hash");
    assert!(entry.get("secret").is_none(), "list must not expose secret");
    for field in [
        "created_at",
        "expires_at",
        "last_used_at",
        "revoked_at",
        "tenant",
    ] {
        assert!(entry.get(field).is_some(), "list carries {field}: {entry}");
    }
}

pub async fn verify_with_secret_authenticates_a_read_route(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "reader", "read", None).await;
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("GET", "/workflows").bearer(&secret),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    assert_ne!(status, StatusCode::FORBIDDEN);
}

/// RFC 7235 2.1: the `Authorization` scheme is case-insensitive. A valid
/// token with a mixed-case scheme authenticates as with `Bearer`.
pub async fn mixed_case_bearer_scheme_authenticates_identically(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "case", "read", None).await;
    let canonical =
        send_with_raw_auth(&app, &prefix, "/workflows", &format!("Bearer {secret}")).await;
    assert_ne!(canonical, StatusCode::UNAUTHORIZED);
    assert_ne!(canonical, StatusCode::FORBIDDEN);
    for scheme in ["BEARER", "bEaReR", "bearer"] {
        let status =
            send_with_raw_auth(&app, &prefix, "/workflows", &format!("{scheme} {secret}")).await;
        assert_eq!(status, canonical, "scheme `{scheme}`");
    }
}

pub async fn unknown_hvst_bearer_is_401(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("GET", "/workflows").bearer("hvst_this_was_never_minted"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

pub async fn revoke_denies_next_request(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "revokee", "read", None).await;
    let (ok, _) = send(
        &app,
        &prefix,
        Call::new("GET", "/workflows").bearer(&secret),
    )
    .await;
    assert_ne!(ok, StatusCode::UNAUTHORIZED);

    let id = token_id(&app, &prefix, "revokee").await;
    let (status, body) = send(
        &app,
        &prefix,
        Call::new("DELETE", &format!("/admin/tokens/{id}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["revoked"], true);
    let (again, body) = send(
        &app,
        &prefix,
        Call::new("DELETE", &format!("/admin/tokens/{id}")),
    )
    .await;
    assert_eq!(again, StatusCode::OK, "a revoke is idempotent");
    assert_eq!(body["revoked"], false);

    let (denied, _) = send(
        &app,
        &prefix,
        Call::new("GET", "/workflows").bearer(&secret),
    )
    .await;
    assert_eq!(denied, StatusCode::UNAUTHORIZED);
}

pub async fn expired_token_is_401(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "short", "read", Some("2000-01-01T00:00:00Z")).await;
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("GET", "/workflows").bearer(&secret),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// A `read` token gets `403` on every mutating route and never on a read.
pub async fn read_token_403_on_all_mutations_and_reaches_reads(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "reader", "read", None).await;

    for (template, class) in CLASSIFIED_ROUTES {
        let Some((method, tmpl)) = template.split_once(' ') else {
            continue;
        };
        let path = concrete(tmpl);
        let (status, _) = send(&app, &prefix, Call::new(method, &path).bearer(&secret)).await;
        match class {
            RouteClass::Mutating => assert_eq!(
                status,
                StatusCode::FORBIDDEN,
                "read token must be 403 on mutating route {template}"
            ),
            RouteClass::ReadOnly | RouteClass::PublicSafe => assert_ne!(
                status,
                StatusCode::FORBIDDEN,
                "read token must NOT be 403 on read route {template}"
            ),
        }
    }
}

/// A `mutate` token reaches an admin-gated mutation with no boundary.
pub async fn mutate_token_reaches_admin_route_standalone(backend: &mut impl TokenBackend) {
    let boundary = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&boundary, &prefix, "deployer", "mutate", None).await;

    let standalone = backend.standalone().await;
    let cancel = || Call::new("POST", "/workflows/some-wf/cancel").body(json!({}));
    let (unauth, _) = send(&standalone, &prefix, cancel()).await;
    assert_eq!(unauth, StatusCode::UNAUTHORIZED, "no creds: 401");

    let (authed, _) = send(&standalone, &prefix, cancel().bearer(&secret)).await;
    assert_ne!(
        authed,
        StatusCode::UNAUTHORIZED,
        "mutate token must pass admin"
    );
    assert_ne!(
        authed,
        StatusCode::FORBIDDEN,
        "mutate token is not read-scoped"
    );
}

/// A token-authed mutation records `actor = token:{id}`. A spoofed
/// `x-harvest-actor` cannot change it.
pub async fn token_authed_mutation_audit_actor_is_token_id(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    // Only an admin token may revoke (issue #1803).
    let secret = mint(&app, &prefix, "actor-test", "admin", None).await;
    let id = token_id(&app, &prefix, "actor-test").await;
    mint(&app, &prefix, "throwaway", "read", None).await;
    let throwaway_id = token_id(&app, &prefix, "throwaway").await;

    let (status, _) = send(
        &app,
        &prefix,
        Call::new("DELETE", &format!("/admin/tokens/{throwaway_id}"))
            .bearer(&secret)
            .actor("attacker@evil"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let actors = audit_actors(&app, &prefix, "token.revoke").await;
    assert!(!actors.is_empty(), "a revoke audit row must exist");
    for actor in &actors {
        assert_eq!(actor, &format!("token:{id}"), "actor must be token:{{id}}");
        assert!(!actor.contains(&secret), "actor must not carry the secret");
    }
}

/// With the token layer on, a caller with no valid `hvst_` token cannot
/// forge a `token:` actor. A plain actor passes through unchanged.
pub async fn passthrough_cannot_forge_token_namespaced_actor(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "spoofer", "scope": "read" }))
            .actor("token:deadbeef-0000-0000-0000-000000000000"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "mint should 201");
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "legit", "scope": "read" }))
            .actor("release-eng"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "second mint should 201");

    let actors = audit_actors(&app, &prefix, "token.create").await;
    assert_eq!(actors.len(), 2, "two mints must be audited");
    for actor in &actors {
        assert!(
            !actor.starts_with("token:"),
            "the token: namespace must be stripped on the pass-through path: {actor}"
        );
    }
    assert!(
        actors.iter().any(|a| a == "release-eng"),
        "a plain actor must pass through unchanged"
    );
}

/// Create and revoke each write an audit row with a non-empty actor.
pub async fn create_and_revoke_are_audited_with_actor(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "audited", "scope": "read" }))
            .actor("release-eng"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(backend.stored_hashes().await.len(), 1);
    let id = token_id(&app, &prefix, "audited").await;
    let (status, _) = send(
        &app,
        &prefix,
        Call::new("DELETE", &format!("/admin/tokens/{id}")).actor("release-eng"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    for operation in ["token.create", "token.revoke"] {
        let actors = audit_actors(&app, &prefix, operation).await;
        assert_eq!(actors, ["release-eng"], "{operation} is audited");
    }
}

/// A token seeded offline, with the shared helpers, authenticates and
/// audits as a minted one does.
pub async fn bootstrap_seeded_token_authenticates_identically_to_route_minted(
    backend: &mut impl TokenBackend,
) {
    let boundary = backend.boundary().await;
    let prefix = backend.prefix().to_owned();

    // The route stores exactly `hash_secret(secret)`.
    let route_secret = mint(&boundary, &prefix, "route-minted", "read", None).await;
    let hashes = backend.stored_hashes().await;
    let route_hash = hashes
        .iter()
        .find(|(name, _)| name == "route-minted")
        .map(|(_, hash)| hash.clone())
        .expect("the minted row");
    assert_eq!(
        route_hash,
        autumn_harvest::api_token::hash_secret(&route_secret),
        "the mint route must store hash_secret(secret)"
    );

    // Seed an `admin` token as `harvest token bootstrap` does.
    let seed_secret = autumn_harvest::api_token::mint_secret();
    let seed_hash = autumn_harvest::api_token::hash_secret(&seed_secret);
    assert!(seed_secret.starts_with("hvst_"));
    assert_ne!(seed_hash, seed_secret);
    let seed_id = backend.seed_admin_token("bootstrap-seed", &seed_hash).await;

    // It satisfies `require_admin` with no boundary.
    let standalone = backend.standalone().await;
    let (authed, _) = send(
        &standalone,
        &prefix,
        Call::new("POST", "/workflows/some-wf/cancel")
            .body(json!({}))
            .bearer(&seed_secret),
    )
    .await;
    assert_ne!(authed, StatusCode::UNAUTHORIZED, "seeded admin must pass");
    assert_ne!(authed, StatusCode::FORBIDDEN, "seeded admin is not read");

    // A mutation by the seeded token audits as `token:{id}`. A backend
    // with one writer per file runs one app at a time, so ask again.
    let boundary = backend.boundary().await;
    mint(&boundary, &prefix, "throwaway", "read", None).await;
    let throwaway_id = token_id(&boundary, &prefix, "throwaway").await;
    let (status, _) = send(
        &boundary,
        &prefix,
        Call::new("DELETE", &format!("/admin/tokens/{throwaway_id}"))
            .bearer(&seed_secret)
            .actor("attacker@evil"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "the seeded token revokes");
    let actors = audit_actors(&boundary, &prefix, "token.revoke").await;
    assert!(!actors.is_empty(), "a revoke audit row must exist");
    for actor in &actors {
        assert_eq!(actor, &format!("token:{seed_id}"));
        assert!(!actor.contains(&seed_secret));
    }
}

/// A `mutate` token is denied the admin-only token routes. The deny writes
/// one `authz.deny` row with the token actor (issue #1803).
pub async fn mutate_token_is_denied_token_admin_and_audited(backend: &mut impl TokenBackend) {
    let app = backend.boundary().await;
    let prefix = backend.prefix().to_owned();
    let secret = mint(&app, &prefix, "deployer", "mutate", None).await;
    let id = token_id(&app, &prefix, "deployer").await;
    let (status, body) = send(
        &app,
        &prefix,
        Call::new("POST", "/admin/tokens")
            .body(json!({ "name": "escalate", "scope": "admin" }))
            .bearer(&secret),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");
    let actors = audit_actors(&app, &prefix, "authz.deny").await;
    assert_eq!(actors, [format!("token:{id}")]);
    assert_eq!(backend.stored_hashes().await.len(), 1, "nothing minted");
}
