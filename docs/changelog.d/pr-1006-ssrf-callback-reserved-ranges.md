## Phase 3.47 — SSRF guard: block `0.0.0.0/8` and `198.18.0.0/15` callback targets (issue #605 hardening / PR #1004 review notes)

Closed two IP-literal SSRF bypasses in the completion-callback URL validator
(`completion_callback::validate_target_url` → `is_ipv4_non_routable`), surfaced
by the `validate_target_url` internal-IP property/fuzz coverage added in PR #1004.

**Before:** the classifier blocked the bare `0.0.0.0` (via `is_unspecified()`)
but not the rest of `0.0.0.0/8` ("this host on this network", RFC 1122) — on
Linux the whole range routes to localhost, so `http(s)://0.0.0.1/`,
`http(s)://0.1.2.3/`, `http(s)://0.255.255.255/` all passed the callback host
allowlist. It also missed the entire `198.18.0.0/15` benchmarking range
(RFC 2544), so `198.18.0.1` … `198.19.255.255` validated.

**After:** both ranges are rejected as `SsrfRejection::IpNotRoutable` (the
existing machine-readable code — no new variant), alongside the previously
covered loopback (`127.0.0.0/8`), private (`10/8`, `172.16/12`, `192.168/16`),
link-local (`169.254/16`), CGNAT/shared (`100.64.0.0/10`), unspecified,
multicast, documentation, and broadcast ranges. IPv6 v4-mapped forms
(`::ffff:0.0.0.1`, `::ffff:198.18.0.1`) inherit the fix through the existing
`to_ipv4_mapped()` canonicalization.

**Blast radius / call sites that inherit the fix for free** (all four reuse the
one pure `validate_target_url`): builder-default callback registration
(`try_build`), the per-start HTTP registration route (rejects with `422`),
enqueue-time re-validation (defense in depth), and the scanner's fire-time
re-validation before each POST. The inbound webhook receiver
(`autumn-harvest-plugin`) does **not** reuse this guard — it verifies inbound
deliveries via autumn-web's `[security.webhooks]` — so it is unaffected.

**Scope:** validator + tests only. No new `SsrfRejection` code, no new
`WorkflowEvent` variant, no migration, no API/wire change. TDD red→green: the
two new range tests and the v4-mapped test failed against the old classifier
(addresses returned `Ok`); the fix makes them pass while the added
already-covered-range audit tests and the just-outside-`198.18.0.0/15` neighbor
tests (`198.17.255.255`, `198.20.0.0` still pass validation) stay green. Verified
with `cargo test -p autumn-harvest {--no-default-features,--features db} ssrf_tests`
(33 passed), `cargo clippy -p autumn-harvest --all-features --tests -- -D warnings`
and `-p autumn-harvest-plugin` clean, `cargo fmt --check` clean.
