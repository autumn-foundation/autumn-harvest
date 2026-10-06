## Feature — optional per-client API rate limiting (issue #1827)

The management API can now rate-limit each client. A client over its limit
gets `429 Too Many Requests` with a `Retry-After` header in whole seconds.
This closes OWASP API4 (Unrestricted Resource Consumption) without a proxy.

The limiter is off by default. Turn it on with
`HarvestPlugin::with_api_rate_limit` or `StandaloneAdminAuth::with_rate_limit`.
`ApiRateLimit::default()` allows 20 mutating and 100 read requests a second for
each client, each with a burst of twice the rate. The production checklist in
`docs/security-posture.md` now recommends it.

Design decisions:

- One in-process token bucket per client per route class. The class comes
  from `CLASSIFIED_ROUTES`. A route with no class counts by its method.
  `PublicSafe` routes and `OPTIONS` are exempt, so health probes are never
  limited.
- The client is the verified API token. The layer sits directly inside the
  token layer in `apply_admin_auth_layers`, so an unverified bearer cannot open
  a bucket. Both mount paths share that function.
- Without a token, the client is the autumn-web `ClientAddr` (trusted-proxy
  aware), then the socket peer. An IPv6 address counts by its /64 prefix.
- Memory is bounded. The limiter keeps at most 10,000 buckets, prunes idle
  ones, and sends new clients past the cap to one overflow bucket.
- autumn-web `RateLimitLayer` was considered. It has no rejection hook for the
  metric and audit row, and it runs outside the nested router, so it cannot see
  the verified token. Its Redis backend remains the answer for one fleet-wide
  limit, because these buckets are per replica.

Observability:

- New counter `harvest.api.rate_limited` with labels `route_class` and
  `client_kind`. The metrics-rs adapter and the built-in scrape both export it.
  The starter dashboard has a panel for it.
- New audit operation `api.rate_limit_sustained`. One row per bucket per
  window when the rejections reach a threshold (default 100 in 60 s), and at
  most 100 rows per window in total. The write runs off the request path.

No migration, no new `WorkflowEvent` variant, and no change to
`harvest_events`. No new dependency.

Test evidence:

- `api_rate_limit.rs` unit tests: bucket math, `Retry-After`, class split,
  IPv6 keys, cap and overflow, prune, sustained window.
- `tests/api_rate_limit.rs` (no database): off by default, address keys,
  `ClientAddr`, exemptions, read and mutating buckets, refill, handler never
  reached.
- `tests/api_rate_limit_integration.rs` (Postgres): at 10 req/s, a burst of
  100 starts from one token gets 429s while another token is unaffected; one
  metric sample per rejection; one sustained audit row with the token actor.
- `metrics_rs_adapter` and `metrics_scrape` tests for the new counter.
