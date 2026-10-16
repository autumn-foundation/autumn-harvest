## Fix — wasmtime 48.0.6 and rustls 0.23.46 for new RustSec advisories

**Security fix.** New RustSec advisories failed the `Dependency ledger
(cargo-deny)` job on every pull request against `trunk-dev`.

- `wasmtime` 48.0.5, the dependency behind the optional `wasm-activities`
  sandbox feature: RUSTSEC-2026-0353 to 0359 and RUSTSEC-2026-0361 to 0364.
  One example is RUSTSEC-2026-0364 (GHSA-p79w-pv2r-29qp), where exception
  tags can make host data structures unaligned. 48.0.6 fixes them all.
- `rustls` 0.23.45, behind the `tls` feature, the CLI and the Redis
  dispatch channel: RUSTSEC-2026-0346 to 0349. These cover TLS secrets in
  `Debug` output, data sent after an ECH rejection, a Brotli certificate
  decompression of up to 1 GiB, and a ServerHello downgrade. 0.23.46 fixes
  them all.

The changes:

- `Cargo.lock` moves the `wasmtime` 48.0.x family to 48.0.6, with its
  `cranelift` crates at 0.135.6, and `rustls` to 0.23.46. `cargo update
  --precise` changed nothing else.
- The manifest floors move too: `wasmtime` 48.0.4 to 48.0.6 in
  `autumn-harvest`, and `rustls` 0.23 to 0.23.46 in `autumn-harvest`,
  `autumn-harvest-cli` and `autumn-harvest-redis`. A downstream consumer
  reads the manifest, not this lockfile, so only the floor keeps it off a
  vulnerable release (as in PR #1303).

**No migration. No new `WorkflowEvent` variant. No public API change.** The
core MSRV stays 1.88, and `rustls` 0.23.46 builds on it. The
`wasm-activities` feature still needs Rust 1.95, as `wasmtime` 48 does.

**Checks.** `cargo deny --all-features check` with cargo-deny 0.20.2, the CI
version, reports `advisories ok, bans ok, licenses ok, sources ok`. Before
the change it failed on the 15 advisories above. The `autumn-harvest`
library suite passes with `wasm-activities` on 48.0.6: 4865 tests, 122 of
them wasm tests. `autumn-harvest` with `tls`, `autumn-harvest-cli` and
`autumn-harvest-redis` build on `rustls` 0.23.46.
