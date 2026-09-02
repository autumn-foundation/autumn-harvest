## Dependency ledger harness + wasmtime/metrics security fix (PR #1303)

No lockfile-verified dependency audit previously existed in this repo — no
`cargo-audit`/`cargo-deny`, no advisory scan, no license gate in CI. This PR
adds one (`deny.toml` + a new `dependency-audit` CI job running
`cargo deny check` via `EmbarkStudios/cargo-deny-action@v2`) and, using it,
fixes the one reachable finding it turned up: four wasmtime advisories
(RUSTSEC-2026-0222/0223/0268/0269 — VM-state/type-index corruption, guest-
controlled host heap allocation, a filesystem sandbox escape) against
`wasmtime` 46.0.1, the direct dependency behind the `wasm-activities`
sandbox feature. All four are fixed within the already-declared
`wasmtime = "46"` range (→ 46.0.3): a lockfile-only `cargo update`, no
manifest change, no new MSRV. `metrics` 0.24.5, independently yanked
upstream, moves to 0.24.6 the same way.

Eleven other advisory-adjacent findings (crossbeam-epoch, lru, h2, rsa,
proc-macro-error2, instant, rustls-webpki x3, two yanked crates) are none of
them fixable from this repo's own manifest today — each carries a
reachability verdict and a revisit trigger in `deny.toml` rather than being
silently ignored. `lru` is a direct dependency and does call the advisory's
vulnerable `LruCache::pop()` (in `autumn-harvest/src/cache.rs`), but our key
(`Uuid`) and value (`CachedWorkflowState`) types have no custom `Drop`, so
the panic-during-drop precondition the advisory describes can't occur —
verdict unreachable, flagged as a candidate for the first scheduled
dependency batch.

Also fixed: `autumn-harvest-sqlite`'s dev-dependency on `autumn-harvest` was
missing the `version = "0.6.0"` every sibling path-dependency in this
workspace carries, which `deny.toml`'s `bans.wildcards = "deny"` now catches.

No `WorkflowEvent` variant, no migration, no engine-runtime change — this
touches only dependency pins and CI configuration. Test evidence: `cargo
build`/`clippy --all-features -D warnings` clean; all 70 wasm-activities
unit tests (sandbox denial, capability grants, fuel/memory limits) pass;
`cargo run --example wasm_activity` (the DB-free sandbox contract demo)
passes; `cargo deny check` green across advisories/licenses/bans/sources.
