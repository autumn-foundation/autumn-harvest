## Fix sqs feature's legacy TLS/HTTP stack pull (PR #1393)

`deny.toml` (from the dependency-ledger harness, PR #1303) deferred four
RUSTSEC advisories — RUSTSEC-2026-0098/0099/0104 (rustls-webpki 0.101.7
certificate-parsing bugs) and the h2@0.3.27 half of RUSTSEC-2026-0258
(unbounded empty DATA frames) — as reachable via the optional `sqs`
feature but stuck pending an `aws-config`/`aws-sdk-sqs` manifest bump. That
turned out to be wrong: it was never a version problem.

`aws-sdk-sqs`'s own `rustls` feature (which `autumn-harvest-plugin`'s `sqs`
feature requested) is defined upstream as `rustls =
["aws-smithy-runtime/tls-rustls"]`, and `tls-rustls` unconditionally pulls
`connector-hyper-0-14-x` — the legacy hyper 0.14 / h2 0.3.x / rustls 0.21 /
rustls-webpki 0.101.7 connector — *alongside* the modern stack, not instead
of it. `default-https-client` (`aws-smithy-runtime/default-https-client` ->
`aws-smithy-http-client/rustls-aws-lc`, hyper 1.x + aws-lc-rs) is an
independent feature that already selects a fully working modern client on
its own and was already enabled in the manifest, so `rustls` was pure extra
weight. Traced with `cargo tree --workspace --all-features -e features -i
aws-smithy-runtime@1.10.0` and confirmed against the actual published
`Cargo.toml` `[features]` sections of `aws-config`, `aws-sdk-sqs`, and
`aws-smithy-runtime`. Also confirmed no version bump would have fixed this
at any MSRV: the `rustls -> tls-rustls` mapping is unchanged as of
`aws-sdk-sqs`'s newest release (1.109.0), and 1.93.0 (already in this
lockfile) is in fact the newest release compatible with this workspace's
`rust-version = "1.88.0"`.

Fix: `autumn-harvest-plugin/Cargo.toml`'s `aws-sdk-sqs` dependency now
requests `default-https-client` instead of `rustls` — a feature-flag
change only, no version bump, no MSRV change. `aws-config`'s own `rustls`
feature is untouched (it already aliases to `client-hyper` ->
`default-https-client`, the same modern target).

Rehearsal: `cargo tree --workspace --all-features` now has zero instances
of hyper 0.14, h2 0.3.x, rustls 0.21.x, rustls-webpki 0.101.7, hyper-rustls
0.24.x, or tokio-rustls 0.24.x anywhere in the graph (759 -> 747 unique
(name, version) entries under `-e normal`; no new packages added, only
removals). `cargo check -p autumn-harvest-plugin --no-default-features
--features sqs` and `cargo test -p autumn-harvest-plugin
--no-default-features --features sqs --lib` both green (1263 passed, 0
failed) — the sqs connector only calls the high-level
`aws_config::load_defaults`/`Client::new`/`Client::from_conf` API, never a
connector type directly, so the swap is behavior-invisible to it.

`deny.toml`'s ignore list drops all four now-moot entries, with the
mechanism recorded in their place.
