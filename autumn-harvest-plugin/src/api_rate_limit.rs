//! Optional per-client rate limiting for the management API (issue #1827).
//!
//! One misbehaving client or leaked token can flood the start, signal and
//! query routes. This layer gives each client a token bucket per route class.
//! A client over its limit gets `429` with `Retry-After`. The layer is off by
//! default. Turn it on with [`crate::HarvestPlugin::with_api_rate_limit`] or
//! [`crate::api::StandaloneAdminAuth::with_rate_limit`].
//!
//! See `docs/security-posture.md#api-rate-limiting`.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use autumn_harvest::audit::{OP_API_RATE_LIMIT_SUSTAINED, RouteClass};
use autumn_web::extract::ClientAddr;
use autumn_web::reexports::axum::Json;
use autumn_web::reexports::axum::extract::{ConnectInfo, Request, State};
use autumn_web::reexports::axum::http::header::RETRY_AFTER;
use autumn_web::reexports::axum::http::{HeaderValue, Method, StatusCode};
use autumn_web::reexports::axum::middleware::Next;
use autumn_web::reexports::axum::response::{IntoResponse, Response};
use tokio::time::Instant;
use uuid::Uuid;

use crate::api::HarvestApiState;
use crate::api_token::{TOKEN_ACTOR_PREFIX, TokenPrincipal};

/// The refill rate and burst of one token bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketRate {
    per_second: u32,
    burst: u32,
}

impl BucketRate {
    /// A bucket that refills `per_second` tokens each second.
    ///
    /// The burst is equal to the rate. A zero rate counts as one.
    #[must_use]
    pub const fn per_second(per_second: u32) -> Self {
        let per_second = if per_second == 0 { 1 } else { per_second };
        Self {
            per_second,
            burst: per_second,
        }
    }

    /// Set the bucket size. A zero burst counts as one.
    #[must_use]
    pub const fn with_burst(mut self, burst: u32) -> Self {
        self.burst = if burst == 0 { 1 } else { burst };
        self
    }

    /// Tokens added each second.
    #[must_use]
    pub const fn rate(&self) -> u32 {
        self.per_second
    }

    /// The bucket size.
    #[must_use]
    pub const fn burst(&self) -> u32 {
        self.burst
    }
}

/// The configuration of the API rate limiter.
///
/// Each client has one bucket for mutating routes and one for read routes.
/// The client is the verified API token. Without a token, it is the client
/// IP address.
#[derive(Clone, Debug)]
pub struct ApiRateLimit {
    mutating: BucketRate,
    read: BucketRate,
    max_clients: usize,
    sustained_rejections: u32,
    sustained_window: Duration,
}

impl Default for ApiRateLimit {
    /// 20 mutating and 100 read requests a second, each with a burst of twice
    /// the rate.
    fn default() -> Self {
        Self::new(
            BucketRate::per_second(20).with_burst(40),
            BucketRate::per_second(100).with_burst(200),
        )
    }
}

impl ApiRateLimit {
    /// A limiter with these rates for mutating and read routes.
    #[must_use]
    pub const fn new(mutating: BucketRate, read: BucketRate) -> Self {
        Self {
            mutating,
            read,
            max_clients: DEFAULT_MAX_CLIENTS,
            sustained_rejections: DEFAULT_SUSTAINED_REJECTIONS,
            sustained_window: DEFAULT_SUSTAINED_WINDOW,
        }
    }

    /// Cap the number of buckets the limiter keeps. A zero cap counts as one.
    ///
    /// At the cap, a new client shares one overflow bucket per route class.
    #[must_use]
    pub const fn with_max_clients(mut self, max_clients: usize) -> Self {
        self.max_clients = if max_clients == 0 { 1 } else { max_clients };
        self
    }

    /// Write an audit row when one bucket rejects `rejections` requests in
    /// `window`. Zero values count as one.
    ///
    /// The limiter writes at most one row per bucket per window.
    #[must_use]
    pub const fn with_sustained_audit(mut self, rejections: u32, window: Duration) -> Self {
        self.sustained_rejections = if rejections == 0 { 1 } else { rejections };
        self.sustained_window = if window.is_zero() {
            Duration::from_secs(1)
        } else {
            window
        };
        self
    }

    /// The rate for mutating routes.
    #[must_use]
    pub const fn mutating(&self) -> BucketRate {
        self.mutating
    }

    /// The rate for read routes.
    #[must_use]
    pub const fn read(&self) -> BucketRate {
        self.read
    }

    const fn rate_for(&self, class: LimitClass) -> BucketRate {
        match class {
            LimitClass::Mutating => self.mutating,
            LimitClass::Read => self.read,
        }
    }
}

const DEFAULT_MAX_CLIENTS: usize = 10_000;
const DEFAULT_SUSTAINED_REJECTIONS: u32 = 100;
const DEFAULT_SUSTAINED_WINDOW: Duration = Duration::from_secs(60);

/// The route class a bucket counts.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum LimitClass {
    Mutating,
    Read,
}

impl LimitClass {
    /// The `route_class` metric label value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Mutating => "mutating",
            Self::Read => "read",
        }
    }
}

/// The identity a bucket belongs to.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub(crate) enum ClientKey {
    /// A verified API token.
    Token(Uuid),
    /// A client IP address. An IPv6 address is cut to its /64 prefix.
    Ip(IpAddr),
    /// No token and no known address.
    Unknown,
    /// A new client when the limiter is at its bucket cap.
    Overflow,
}

impl ClientKey {
    /// The `client_kind` metric label value.
    pub(crate) const fn kind(self) -> &'static str {
        match self {
            Self::Token(_) => "token",
            Self::Ip(_) => "ip",
            Self::Unknown => "unknown",
            Self::Overflow => "overflow",
        }
    }
}

/// What the limiter decided for one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Allow,
    Reject {
        /// The client the limiter charged. It is [`ClientKey::Overflow`] at
        /// the bucket cap.
        key: ClientKey,
        /// Whole seconds until the bucket holds one token. Never zero.
        retry_after_secs: u64,
        /// The rejection count when this rejection makes the bucket
        /// sustained. Set once per bucket per window.
        sustained: Option<u32>,
    },
}

/// The shared limiter state behind the layer.
#[derive(Clone, Debug)]
pub(crate) struct ApiRateLimiter {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: ApiRateLimit,
    state: Mutex<Buckets>,
}

#[derive(Debug, Default)]
struct Buckets {
    buckets: HashMap<(ClientKey, LimitClass), Bucket>,
    /// The last time a full map was pruned. Pruning walks every bucket, so it
    /// runs at most once per [`PRUNE_INTERVAL`].
    last_prune: Option<Instant>,
    /// The audit budget window (see [`MAX_SUSTAINED_AUDITS_PER_WINDOW`]).
    audit_window_start: Option<Instant>,
    audits_in_window: u32,
}

/// The shortest time between two prune passes over a full bucket map.
const PRUNE_INTERVAL: Duration = Duration::from_secs(1);

/// The most sustained-rejection audit rows that all buckets together write in
/// one window.
///
/// Many clients can become sustained at once. Each audit row needs a pool
/// connection, so this cap stops a wide flood from starving real work.
const MAX_SUSTAINED_AUDITS_PER_WINDOW: u32 = 100;

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    refilled_at: Instant,
    window_start: Instant,
    window_rejections: u32,
    window_reported: bool,
}

impl Bucket {
    fn full(rate: BucketRate, now: Instant) -> Self {
        Self {
            tokens: f64::from(rate.burst),
            refilled_at: now,
            window_start: now,
            window_rejections: 0,
            window_reported: false,
        }
    }

    /// The tokens the bucket holds at `now`, capped at the burst.
    fn tokens_at(&self, rate: BucketRate, now: Instant) -> f64 {
        let elapsed = now
            .saturating_duration_since(self.refilled_at)
            .as_secs_f64();
        (elapsed.mul_add(f64::from(rate.per_second), self.tokens)).min(f64::from(rate.burst))
    }

    fn refill(&mut self, rate: BucketRate, now: Instant) {
        self.tokens = self.tokens_at(rate, now);
        self.refilled_at = now;
    }

    /// Whole seconds until the bucket holds one token, rounded up, at least 1.
    fn retry_after_secs(&self, rate: BucketRate) -> u64 {
        let wait = (1.0 - self.tokens) / f64::from(rate.per_second);
        let wait = Duration::try_from_secs_f64(wait).unwrap_or(Duration::from_secs(1));
        let secs = wait.as_secs() + u64::from(wait.subsec_nanos() > 0);
        secs.max(1)
    }

    /// Count one rejection. Return the count when the bucket first reaches
    /// `threshold` in the current window.
    fn count_rejection(&mut self, now: Instant, threshold: u32, window: Duration) -> Option<u32> {
        if now.saturating_duration_since(self.window_start) >= window {
            self.window_start = now;
            self.window_rejections = 0;
            self.window_reported = false;
        }
        self.window_rejections = self.window_rejections.saturating_add(1);
        if self.window_reported || self.window_rejections < threshold {
            return None;
        }
        self.window_reported = true;
        Some(self.window_rejections)
    }

    /// A full bucket past its window holds no state worth keeping.
    fn is_idle(&self, rate: BucketRate, now: Instant, window: Duration) -> bool {
        self.tokens_at(rate, now) >= f64::from(rate.burst)
            && now.saturating_duration_since(self.window_start) >= window
    }
}

impl Buckets {
    /// The key to charge. At the bucket cap, a new client is charged to the
    /// overflow bucket, unless a prune frees room.
    fn admit(
        &mut self,
        key: ClientKey,
        class: LimitClass,
        now: Instant,
        config: &ApiRateLimit,
    ) -> ClientKey {
        if self.buckets.len() < config.max_clients || self.buckets.contains_key(&(key, class)) {
            return key;
        }
        let prune_due = self
            .last_prune
            .is_none_or(|at| now.saturating_duration_since(at) >= PRUNE_INTERVAL);
        if prune_due {
            self.last_prune = Some(now);
            self.buckets.retain(|(_, class), bucket| {
                !bucket.is_idle(config.rate_for(*class), now, config.sustained_window)
            });
            if self.buckets.len() < config.max_clients {
                return key;
            }
        }
        ClientKey::Overflow
    }

    /// Take one audit row from the shared budget.
    fn take_audit(&mut self, now: Instant, window: Duration) -> bool {
        let expired = self
            .audit_window_start
            .is_none_or(|at| now.saturating_duration_since(at) >= window);
        if expired {
            self.audit_window_start = Some(now);
            self.audits_in_window = 0;
        }
        if self.audits_in_window >= MAX_SUSTAINED_AUDITS_PER_WINDOW {
            if self.audits_in_window == MAX_SUSTAINED_AUDITS_PER_WINDOW {
                tracing::warn!(
                    budget = MAX_SUSTAINED_AUDITS_PER_WINDOW,
                    "harvest: api rate limit audit budget spent for this window"
                );
                self.audits_in_window += 1;
            }
            return false;
        }
        self.audits_in_window += 1;
        true
    }
}

impl ApiRateLimiter {
    pub(crate) fn new(config: ApiRateLimit) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                state: Mutex::new(Buckets::default()),
            }),
        }
    }

    /// The configuration this limiter enforces.
    pub(crate) fn config(&self) -> &ApiRateLimit {
        &self.inner.config
    }

    /// Charge one request to the bucket of `key` and `class` at `now`.
    pub(crate) fn check(&self, key: ClientKey, class: LimitClass, now: Instant) -> Decision {
        let config = &self.inner.config;
        let rate = config.rate_for(class);
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let key = state.admit(key, class, now, config);
        let bucket = state
            .buckets
            .entry((key, class))
            .or_insert_with(|| Bucket::full(rate, now));
        bucket.refill(rate, now);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return Decision::Allow;
        }
        let retry_after_secs = bucket.retry_after_secs(rate);
        let sustained = bucket
            .count_rejection(now, config.sustained_rejections, config.sustained_window)
            .filter(|_| state.take_audit(now, config.sustained_window));
        Decision::Reject {
            key,
            retry_after_secs,
            sustained,
        }
    }

    /// The number of buckets the limiter holds.
    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .buckets
            .len()
    }
}

/// The client key for an address. An IPv6 address keeps its /64 prefix only.
///
/// One host often owns a whole /64, so a per-address key would let it mint
/// new buckets without limit. An IPv4-mapped address maps back to IPv4.
pub(crate) fn ip_key(addr: IpAddr) -> ClientKey {
    let addr = match addr {
        IpAddr::V4(v4) => IpAddr::V4(v4),
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or_else(
            || IpAddr::V6(Ipv6Addr::from(u128::from(v6) & (u128::MAX << 64))),
            IpAddr::V4,
        ),
    };
    ClientKey::Ip(addr)
}

/// The bucket class of a request, or `None` when the request is exempt.
///
/// `OPTIONS` and `PublicSafe` routes, such as the health probes, are exempt. A
/// route outside `CLASSIFIED_ROUTES`, such as a Vantage page, counts by its
/// method.
pub(crate) fn limit_class(method: &Method, path: &str) -> Option<LimitClass> {
    if *method == Method::OPTIONS {
        return None;
    }
    match crate::api::classified_route(method, path) {
        Some(RouteClass::PublicSafe) => None,
        Some(RouteClass::ReadOnly) => Some(LimitClass::Read),
        Some(RouteClass::Mutating) => Some(LimitClass::Mutating),
        None if matches!(*method, Method::GET | Method::HEAD) => Some(LimitClass::Read),
        None => Some(LimitClass::Mutating),
    }
}

/// The client a request is charged to.
///
/// A verified token comes first. Next is the autumn-web `ClientAddr`, which
/// applies `[security.trusted_proxies]`. Next is the socket peer. A request
/// with none of these shares the `unknown` bucket.
fn client_key(request: &Request, client_addr: Option<ClientAddr>) -> ClientKey {
    let extensions = request.extensions();
    if let Some(principal) = extensions.get::<TokenPrincipal>() {
        return ClientKey::Token(principal.id);
    }
    if let Some(addr) = client_addr {
        return ip_key(addr.ip());
    }
    if let Some(ConnectInfo(peer)) = extensions.get::<ConnectInfo<SocketAddr>>() {
        return ip_key(peer.ip());
    }
    ClientKey::Unknown
}

/// Refuse a request over its client's limit with `429` (issue #1827).
///
/// The layer runs inside the token layer, so a verified token is known. It
/// runs before the read-only, authorizer and admin layers, so a refused
/// request does no further work.
pub(crate) async fn enforce_api_rate_limit(
    State((api_state, limiter)): State<(HarvestApiState, ApiRateLimiter)>,
    client_addr: Option<ClientAddr>,
    request: Request,
    next: Next,
) -> Response {
    let Some(class) = limit_class(request.method(), request.uri().path()) else {
        return next.run(request).await;
    };
    let key = client_key(&request, client_addr);
    match limiter.check(key, class, Instant::now()) {
        Decision::Allow => next.run(request).await,
        Decision::Reject {
            key,
            retry_after_secs,
            sustained,
        } => {
            if let Ok(runtime) = api_state.runtime() {
                runtime
                    .registry()
                    .telemetry()
                    .metrics
                    .record_api_rate_limited(class.as_str(), key.kind());
            }
            if let Some(rejections) = sustained {
                audit_sustained(&api_state, &request, &limiter, key, class, rejections);
            }
            rate_limited_response(class, retry_after_secs)
        }
    }
}

/// The `429` answer: `Retry-After` in whole seconds and a JSON body.
fn rate_limited_response(class: LimitClass, retry_after_secs: u64) -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(serde_json::json!({
            "error": "rate limited",
            "route_class": class.as_str(),
            "retry_after_secs": retry_after_secs,
        })),
    )
        .into_response();
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from(retry_after_secs));
    response
}

/// The client named in a sustained-rejection summary. It never holds a secret.
fn describe(key: ClientKey) -> String {
    match key {
        ClientKey::Token(id) => format!("token {id}"),
        ClientKey::Ip(IpAddr::V6(v6)) => format!("ip {v6}/64"),
        ClientKey::Ip(IpAddr::V4(v4)) => format!("ip {v4}"),
        ClientKey::Unknown => "a client with no address".to_owned(),
        ClientKey::Overflow => "the overflow bucket".to_owned(),
    }
}

/// Log a sustained client and write one `api.rate_limit_sustained` row.
///
/// The write runs on its own task, so the `429` never waits on the database.
/// A failed write is logged.
fn audit_sustained(
    api_state: &HarvestApiState,
    request: &Request,
    limiter: &ApiRateLimiter,
    key: ClientKey,
    class: LimitClass,
    rejections: u32,
) {
    let summary = format!(
        "rate limit sustained: {rejections} rejections in {}s on {} routes from {}",
        limiter.config().sustained_window.as_secs(),
        class.as_str(),
        describe(key),
    );
    tracing::warn!(
        route_class = class.as_str(),
        client_kind = key.kind(),
        rejections,
        "harvest: {summary}"
    );
    let actor = match key {
        ClientKey::Token(id) => format!("{TOKEN_ACTOR_PREFIX}{id}"),
        _ => ANONYMOUS_ACTOR.to_owned(),
    };
    let (_, source, request_id) = crate::api::audit_context(request.headers(), api_state);
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let Ok(pool) = api_state.storage_pool() else {
        tracing::error!(path = %path, "harvest: no audit store for api rate limit");
        return;
    };
    tokio::spawn(async move {
        let write = async {
            let mut conn = crate::api::acquire_conn(pool.default_pool())
                .await
                .map_err(|e| e.to_string())?;
            crate::authz::audit_route_event(
                &mut conn,
                OP_API_RATE_LIMIT_SUSTAINED,
                &crate::authz::DenyAudit {
                    actor: &actor,
                    method: &method,
                    path: &path,
                    request_id: request_id.as_deref(),
                    source: &source,
                    shard: None,
                    summary: &summary,
                },
            )
            .await;
            Ok::<(), String>(())
        };
        match tokio::time::timeout(AUDIT_WRITE_TIMEOUT, write).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(error = %error, "harvest: failed to audit api rate limit");
            }
            Err(_) => tracing::error!("harvest: api rate limit audit write timed out"),
        }
    });
}

/// The actor of a sustained row with no verified token.
const ANONYMOUS_ACTOR: &str = "anonymous";

/// The longest a sustained-rejection audit write may take.
const AUDIT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn token() -> ClientKey {
        ClientKey::Token(Uuid::new_v4())
    }

    fn limiter(rate: u32) -> ApiRateLimiter {
        ApiRateLimiter::new(ApiRateLimit::new(
            BucketRate::per_second(rate),
            BucketRate::per_second(rate),
        ))
    }

    fn rejects(decision: Decision) -> bool {
        matches!(decision, Decision::Reject { .. })
    }

    #[test]
    fn zero_rates_count_as_one() {
        let rate = BucketRate::per_second(0).with_burst(0);
        assert_eq!((rate.rate(), rate.burst()), (1, 1));
    }

    #[test]
    fn a_burst_over_the_limit_is_rejected() {
        let limiter = limiter(10);
        let key = token();
        let now = Instant::now();
        let rejected = (0..100)
            .filter(|_| rejects(limiter.check(key, LimitClass::Mutating, now)))
            .count();
        assert_eq!(rejected, 90, "a bucket of 10 admits 10 of 100");
    }

    #[test]
    fn another_client_has_its_own_bucket() {
        let limiter = limiter(10);
        let (a, b) = (token(), token());
        let now = Instant::now();
        for _ in 0..100 {
            limiter.check(a, LimitClass::Mutating, now);
        }
        assert_eq!(limiter.check(b, LimitClass::Mutating, now), Decision::Allow);
    }

    #[test]
    fn read_and_mutating_buckets_are_separate() {
        let limiter = limiter(1);
        let key = token();
        let now = Instant::now();
        assert_eq!(
            limiter.check(key, LimitClass::Mutating, now),
            Decision::Allow
        );
        assert!(rejects(limiter.check(key, LimitClass::Mutating, now)));
        assert_eq!(limiter.check(key, LimitClass::Read, now), Decision::Allow);
    }

    #[test]
    fn a_bucket_refills_at_its_rate() {
        let limiter = limiter(10);
        let key = token();
        let start = Instant::now();
        for _ in 0..10 {
            limiter.check(key, LimitClass::Mutating, start);
        }
        assert!(rejects(limiter.check(key, LimitClass::Mutating, start)));
        let later = start + Duration::from_millis(100);
        assert_eq!(
            limiter.check(key, LimitClass::Mutating, later),
            Decision::Allow
        );
        assert!(rejects(limiter.check(key, LimitClass::Mutating, later)));
    }

    #[test]
    fn retry_after_rounds_up_and_is_never_zero() {
        let limiter = ApiRateLimiter::new(ApiRateLimit::new(
            BucketRate::per_second(10),
            BucketRate::per_second(1).with_burst(1),
        ));
        let key = token();
        let now = Instant::now();
        limiter.check(key, LimitClass::Mutating, now);
        for _ in 0..9 {
            limiter.check(key, LimitClass::Mutating, now);
        }
        let Decision::Reject {
            retry_after_secs, ..
        } = limiter.check(key, LimitClass::Mutating, now)
        else {
            panic!("an empty bucket rejects");
        };
        assert_eq!(retry_after_secs, 1, "0.1 s rounds up to 1 s");

        limiter.check(key, LimitClass::Read, now);
        let later = now + Duration::from_millis(400);
        let Decision::Reject {
            retry_after_secs, ..
        } = limiter.check(key, LimitClass::Read, later)
        else {
            panic!("an empty bucket rejects");
        };
        assert_eq!(retry_after_secs, 1, "0.6 s rounds up to 1 s");
    }

    #[test]
    fn a_rejection_names_the_charged_client() {
        let limiter = ApiRateLimiter::new(ApiRateLimit::new(
            BucketRate::per_second(1).with_burst(1),
            BucketRate::per_second(1),
        ));
        let key = token();
        let now = Instant::now();
        limiter.check(key, LimitClass::Mutating, now);
        let Decision::Reject {
            retry_after_secs,
            key: charged,
            ..
        } = limiter.check(key, LimitClass::Mutating, now)
        else {
            panic!("an empty bucket rejects");
        };
        assert_eq!(retry_after_secs, 1);
        assert_eq!(charged, key);
    }

    #[test]
    fn sustained_rejections_are_reported_once_per_window() {
        let limiter = ApiRateLimiter::new(
            ApiRateLimit::new(BucketRate::per_second(1), BucketRate::per_second(1))
                .with_sustained_audit(5, Duration::from_secs(60)),
        );
        let key = token();
        let start = Instant::now();
        limiter.check(key, LimitClass::Mutating, start);
        let sustained: Vec<Option<u32>> = (0..20)
            .map(|_| match limiter.check(key, LimitClass::Mutating, start) {
                Decision::Reject { sustained, .. } => sustained,
                Decision::Allow => panic!("an empty bucket rejects"),
            })
            .collect();
        assert_eq!(
            sustained.iter().flatten().copied().collect::<Vec<_>>(),
            vec![5],
            "the fifth rejection is reported, and only once"
        );

        let next_window = start + Duration::from_secs(61);
        limiter.check(key, LimitClass::Mutating, next_window);
        let reports = (0..5)
            .filter(|_| {
                matches!(
                    limiter.check(key, LimitClass::Mutating, next_window),
                    Decision::Reject {
                        sustained: Some(5),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(reports, 1, "a new window reports again");
    }

    #[test]
    fn a_few_rejections_are_not_sustained() {
        let limiter = ApiRateLimiter::new(
            ApiRateLimit::new(BucketRate::per_second(1), BucketRate::per_second(1))
                .with_sustained_audit(5, Duration::from_secs(1)),
        );
        let key = token();
        let start = Instant::now();
        for step in 0..20_u64 {
            // Four rejections each window never reach the threshold of five.
            let now = start + Duration::from_secs(step);
            for _ in 0..5 {
                if let Decision::Reject { sustained, .. } =
                    limiter.check(key, LimitClass::Mutating, now)
                {
                    assert_eq!(sustained, None);
                }
            }
        }
    }

    #[test]
    fn the_bucket_cap_sends_new_clients_to_overflow() {
        let limiter = ApiRateLimiter::new(
            ApiRateLimit::new(BucketRate::per_second(1), BucketRate::per_second(1))
                .with_max_clients(2),
        );
        let now = Instant::now();
        limiter.check(token(), LimitClass::Mutating, now);
        limiter.check(token(), LimitClass::Mutating, now);
        // Both buckets are empty and young, so pruning frees nothing.
        let third = token();
        assert_eq!(
            limiter.check(third, LimitClass::Mutating, now),
            Decision::Allow
        );
        match limiter.check(token(), LimitClass::Mutating, now) {
            Decision::Reject { key, .. } => assert_eq!(key, ClientKey::Overflow),
            Decision::Allow => panic!("the overflow bucket is shared and empty"),
        }
        assert!(limiter.bucket_count() <= 4, "the cap bounds memory");
    }

    #[test]
    fn idle_buckets_are_pruned_at_the_cap() {
        let limiter = ApiRateLimiter::new(
            ApiRateLimit::new(BucketRate::per_second(1), BucketRate::per_second(1))
                .with_max_clients(2)
                .with_sustained_audit(1, Duration::from_secs(1)),
        );
        let start = Instant::now();
        limiter.check(token(), LimitClass::Mutating, start);
        limiter.check(token(), LimitClass::Mutating, start);
        let later = start + Duration::from_secs(5);
        let fresh = token();
        limiter.check(fresh, LimitClass::Mutating, later);
        match limiter.check(fresh, LimitClass::Mutating, later) {
            Decision::Reject { key, .. } => assert_eq!(key, fresh, "no overflow after a prune"),
            Decision::Allow => panic!("a bucket of one rejects its second request"),
        }
        assert_eq!(limiter.bucket_count(), 1);
    }

    #[test]
    fn ipv6_keys_keep_the_64_bit_prefix() {
        let a: IpAddr = Ipv6Addr::new(0x2001, 0xdb8, 1, 2, 3, 4, 5, 6).into();
        let b: IpAddr = Ipv6Addr::new(0x2001, 0xdb8, 1, 2, 9, 9, 9, 9).into();
        let c: IpAddr = Ipv6Addr::new(0x2001, 0xdb8, 1, 3, 3, 4, 5, 6).into();
        assert_eq!(ip_key(a), ip_key(b));
        assert_ne!(ip_key(a), ip_key(c));
    }

    #[test]
    fn ipv4_mapped_keys_match_ipv4_keys() {
        let v4 = Ipv4Addr::new(192, 0, 2, 7);
        assert_eq!(ip_key(v4.to_ipv6_mapped().into()), ip_key(v4.into()));
        assert_ne!(
            ip_key(v4.into()),
            ip_key(Ipv4Addr::new(192, 0, 2, 8).into())
        );
    }
}
