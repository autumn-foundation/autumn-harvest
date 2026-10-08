## Phase — SSRF guard rejects IPv4-compatible IPv6 loopback/private embeds (issue #1390)

🪝 Snag exploratory QA finding: `completion_callback::is_ipv6_non_routable`
unwrapped an embedded IPv4 address via `Ipv6Addr::to_ipv4_mapped()`, which
recognizes only the modern IPv4-*mapped* form (`::ffff:a.b.c.d`, RFC 4291
`::ffff:0:0/96`). The deprecated IPv4-*compatible* form (`::a.b.c.d`, the
bare `::/96` prefix) encodes the identical address but `to_ipv4_mapped()`
returns `None` for it, so `validate_target_url` classified
`https://[::127.0.0.1]/hook`, `https://[::10.0.0.5]/hook`,
`https://[::192.168.1.1]/hook`, and `https://[::169.254.1.1]/hook` as
**routable** — bypassing the exact guarantee the function's own doc comment
states ("even then is rejected if it is loopback/private/link-local/etc").
Reachable only when an operator opts into `SsrfPolicy::allow_ip_literals(true)`
(default `false`), but the same guard was already hardened against narrower
IP-literal obfuscations in #1006 (`0.0.0.0/8`, `198.18.0.0/15`).

Fix is a one-line, non-widening swap: `ip.to_ipv4_mapped()` → `ip.to_ipv4()`
in `is_ipv6_non_routable`. `to_ipv4()` unwraps both the mapped and compatible
forms while still returning `None` for a genuine global-unicast IPv6 address
(verified against `2001:db8::1` and a real public IPv6 literal) — no other
branch of the function changes, and nothing previously rejected is now
accepted. No new `WorkflowEvent` variant, no migration, no replay impact —
`completion_callback.rs` is a pure validation module.

Regression test:
`completion_callback::ssrf_tests::rejects_ipv4_compatible_ipv6_embedding_a_loopback_or_private_address`
— RED pre-fix (all four IPv4-compatible addresses parsed as routable), GREEN
post-fix; all 74 pre-existing `completion_callback` tests still pass.
