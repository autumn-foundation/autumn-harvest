## Dependency ledger: `unsound` advisory scope gap + anyhow RUSTSEC-2026-0190 fix (PR #1556)

Routine Ballast ledger sweep. `cargo deny check` came back fully green (no
errors), but with one warning: the `RUSTSEC-2026-0253` (lru) ignore entry in
`deny.toml`, previously verified reachable-and-deferred in PR #1417, now
reported "advisory was not encountered" — cargo-deny no longer evaluates it
at all.

The underlying lru instance (ratatui-core 0.1.0's own `lru = "0.16"` pin,
resolving to 0.16.4) is unchanged and still present in the graph; its own
revisit trigger ("ratatui-core bumps past 0.18.2 or starts calling `pop()`")
had not fired. The real cause: cargo-deny's `[advisories] unsound` field
defaults to `Scope::Workspace`, which only evaluates an
`informational = "unsound"` advisory when a *workspace member* directly
depends on the affected instance. Once autumn-harvest's own `lru` dependency
moved to 0.18 (PR #1417), the only remaining affected instance — reached
only via ratatui-core, never a direct workspace dependency — dropped out of
scope entirely, silently, regardless of the ignore entry.

`deny.toml` didn't declare `unsound` at all, so it inherited that narrower
default — unlike `unmaintained`, which already defaults to `Scope::All` and
was unaffected. Setting `unsound = "all"` explicitly (matching `unmaintained`)
closes the gap. Turning it on immediately re-surfaced two things:

1. The lru ignore entry now matches cleanly again (comment updated in
   `deny.toml` to record why it had gone stale and that the underlying
   verdict is unchanged).
2. A second, previously invisible finding: **RUSTSEC-2026-0190** (`anyhow`
   1.0.102, unsoundness in `Error::downcast_mut()` — a Stacked-Borrows
   violation when downcasting after `.context()`). `anyhow` is never a
   direct workspace dependency either, so it had been silently out of scope
   the whole time this workspace has had a `cargo-deny` gate at all — not a
   new regression, a pre-existing gap this sweep closed by turning the light
   on.

Reachability, not just presence: grepped the actual dependency sources
(`~/.cargo/registry/src`, fetched via `cargo fetch --locked`) for
`downcast_mut` across every direct dependent of `anyhow` in the graph
(`ittapi`, `prost`/`prost-derive`, `reqwest-middleware`/`-retry`/`-tracing`,
`wasm-compose`, `wasmprinter`, `wasmtime-environ`,
`wasmtime-internal-component-macro`, `wasmtime-internal-core`,
`wasmtime-internal-wit-bindgen`, `wit-parser`). Found the real call:
`wasmtime-internal-core`'s `error/error.rs`, `Error::downcast_mut<E>()`,
falls through to `self.inner.downcast_mut::<anyhow::Error>().and_then(|a|
a.downcast_mut::<E>())` when its own chain lookup misses — gated
`#[cfg(feature = "anyhow")]`, which is in wasmtime's own `default` feature
set and therefore active whenever this workspace's `wasm-activities` feature
is built (nothing here disables wasmtime's defaults). Verdict: **reachable**
— the vulnerable call exists in code we actually compile, behind wasmtime's
own general-purpose error-introspection API. Every other dependent checked
(prost/tonic path, reqwest-middleware/postgresql_archive dev-runtime path,
the rest of the wasmtime-internal-* crates) had zero `anyhow::Error`
downcast call sites.

Fix: `cargo update -p anyhow` (1.0.102 → 1.0.104; advisory patched range is
`>= 1.0.103`). Lockfile-only — `anyhow` was never a direct dependency in any
manifest in this workspace, so there was no floor to raise. No ignore entry
needed once patched.

Rehearsal: `cargo deny check` clean (advisories/bans/licenses/sources all
ok, zero warnings, zero notes needing action); `cargo check --workspace`
(default features) clean; `cargo check -p autumn-harvest --features
wasm-activities,hot-code-swap` clean (the reachable wasmtime/anyhow path);
`cargo check -p autumn-harvest-plugin --features dev-runtime-managed` clean
(the other anyhow path, via `postgresql_archive`/`reqwest-middleware`);
`cargo test -p autumn-harvest --lib` — 3490 passed, 0 failed, 1 ignored.
(`--all-features` itself doesn't build in this sandbox — the `kafka` feature
needs system `curl` headers unavailable here — so the two feature-scoped
checks above stand in for it; both are the actual paths that touch `anyhow`
in this workspace.)

No `WorkflowEvent` variant, no migration, no engine-runtime change — this
touches only `deny.toml` policy and one transitive lockfile entry.
