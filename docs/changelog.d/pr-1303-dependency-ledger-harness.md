## Dependency ledger harness + wasmtime/h2/crossbeam-epoch security fixes (PR #1303)

No lockfile-verified dependency audit previously existed in this repo — no
`cargo-audit`/`cargo-deny`, no advisory scan, no license gate in CI. This PR
adds one (`deny.toml` + a new `dependency-audit` CI job running
`cargo deny check` via `EmbarkStudios/cargo-deny-action@v2`) and, using it,
fixes the reachable findings it turned up:

- Four wasmtime advisories (RUSTSEC-2026-0222/0223/0268/0269 — VM-state/
  type-index corruption, guest-controlled host heap allocation, a
  filesystem sandbox escape) against `wasmtime` 46.0.1, the direct
  dependency behind the `wasm-activities` sandbox feature. Fixed in
  `Cargo.lock` (→ 46.0.3) and, after review pointed out that a workspace's
  own lockfile never propagates to a downstream consumer, in the manifest
  floor too: `wasmtime = { version = "46", ... }` → `{ version = "46.0.3",
  ... }`, matching the `tokio-postgres = { version = "0.7.18", ... }`
  exact-floor precedent already in the same file. `metrics` (below) got the
  same manifest-floor treatment for the same reason.
- RUSTSEC-2026-0258 (h2 unbounded empty DATA frames) against `h2` 0.4.13,
  reached via hyper 1.x → axum → autumn-web, our production HTTP server
  path. Fixed the same way: `cargo update -p h2@0.4.13` → 0.4.19. A second
  h2 instance (0.3.27, on the old hyper-0.14 line behind the optional
  `sqs`/AWS-SDK feature) has no patched 0.3.x release to move to and stays
  deferred.
- RUSTSEC-2026-0204 (crossbeam-epoch invalid pointer dereference) against
  0.9.18, reached via `moka` (autumn-web's production cache) and
  `criterion` (dev-only benchmarks). Fixed: `cargo update -p
  crossbeam-epoch` → 0.9.20.
- Two yanked crates with non-yanked releases available that the first pass
  missed: `chacha20` 0.10.0→0.10.2, `spin` 0.9.8→0.9.9.
- `metrics` 0.24.5, independently yanked upstream, moves to 0.24.6 the same
  way.

Review caught three real mistakes across this PR's history, each fixed in
its own follow-up commit: (1) h2 and (2) crossbeam-epoch were both first
wrongly deferred as "transitive through autumn-web, so the fix needs an
autumn-web release" — that conflated "not a direct dependency" with "not
fixable via `cargo update`," which doesn't follow, and which also surfaced
two more wrongly-deferred yanked crates once every remaining entry was
re-verified with `cargo update --dry-run` instead of assumed; (3) even the
correctly-applied `cargo update -p wasmtime` in `Cargo.lock` doesn't help a
downstream consumer of the published `autumn-harvest` crate, whose own
resolution reads the manifest constraint, not this workspace's lockfile —
fixed by raising the manifest floor (see above). h2, crossbeam-epoch,
chacha20, and spin didn't need a manifest-floor fix: none of them are
direct dependencies in any published crate's manifest in this workspace.

Five findings remain genuinely deferred in `deny.toml` (each confirmed
stuck via dry-run, not assumed): `lru` (direct dependency — `cache.rs` does
call the vulnerable `LruCache::pop()`, but our key `Uuid` and value
`CachedWorkflowState` types have no custom `Drop`, so the advisory's
panic-during-drop precondition can't occur; candidate for the first
scheduled batch), `h2@0.3.27` and `rustls-webpki` x3 (same old
hyper-0.14/AWS-SDK cluster behind the optional `sqs` feature, needs an
aws-config/aws-sdk-sqs manifest bump), `rsa`/Marvin Attack (undetermined —
depends on autumn-web's own JWT usage, no upstream patch either way), and
`proc-macro-error2`/`instant` (unmaintained, no safe upgrade exists).

Also fixed: `autumn-harvest-sqlite`'s dev-dependency on `autumn-harvest` was
missing the `version = "0.6.0"` every sibling path-dependency in this
workspace carries, which `deny.toml`'s `bans.wildcards = "deny"` now catches.

No `WorkflowEvent` variant, no migration, no engine-runtime change — this
touches only dependency pins and CI configuration. Test evidence: `cargo
check --workspace --all-features` clean; `cargo clippy --all-features -D
warnings` clean for both `autumn-harvest` and `autumn-harvest-plugin`;
`cargo check -p autumn-harvest --bench metrics_noop` clean (the
crossbeam-epoch/criterion path); `cargo test -p autumn-harvest --lib
--features wasm-activities` — 3310 passed, including all 70 wasm-activities
sandbox tests (denial, capability grants, fuel/memory limits); `cargo run
--example wasm_activity` (the DB-free sandbox contract demo) passes;
`cargo deny check` green across advisories/licenses/bans/sources.
