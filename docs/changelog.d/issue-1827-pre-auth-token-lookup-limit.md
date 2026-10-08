## Fix — rate-limit claimed API tokens before the token lookup (issue #1827)

The API rate limiter ran inside the token layer. The token layer looks up
every `hvst_` bearer in the database first. A made-up token got `401` from
that lookup and never reached the limiter. So an unauthenticated sender could
flood the default pool with made-up tokens, with rate limiting turned on.

With API tokens and the limiter both on, the same limiter now also runs
outside the token layer:

- Each request with an `hvst_` bearer pays one request from its client
  address bucket before the lookup. A client over that limit gets `429` and
  takes no pool connection.
- When the token verifies, the limiter gives the address charge back. Then
  the token pays from its own bucket, as before. Valid tokens that share one
  address therefore do not share its budget.
- A request with no `hvst_` bearer skips this charge. The token layer does no
  lookup for it. An `OPTIONS` request skips it for the same reason.
- An exempt route, such as `/health/live`, still pays from the read bucket.
  The token layer looks up a claimed token there too.
- A valid token that its scope refuses also gets the charge back. The token
  layer marks that `403`, and the pre-auth layer refunds on the mark.

A flood of made-up tokens from one address can now hold back valid tokens
from that address until its bucket refills. Behind a proxy, configure
`[security.trusted_proxies]` so each caller has its own address.
`docs/security-posture.md` documents the new layer order and this trade-off.

Tests: `made_up_tokens_are_limited_before_the_token_lookup` (no database)
fails on the old layer order. All 30 made-up tokens reached the lookup there.
`made_up_tokens_are_limited_before_the_lookup_and_valid_tokens_are_not`
(Postgres) also proves the refund. Without it, a second valid token from the
same address got 9 refusals.
`made_up_tokens_on_a_health_probe_are_limited_before_the_lookup` and
`a_scope_denied_token_does_not_empty_its_address_bucket` cover the exempt
route and the scope deny. Each fails without its part of the fix.
