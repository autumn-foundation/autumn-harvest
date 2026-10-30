## Fix — wasmtime 48.0.8 for three new RustSec advisories (issue #2434)

**Security fix.** The daily advisory scan failed on `wasmtime` 48.0.7. It
affects the optional `wasm-activities` sandbox feature.

- RUSTSEC-2026-0422: a typed function with `i31` or `nullref` arguments can
  panic, because the type check is too weak.
- RUSTSEC-2026-0423: host-created struct types with supertypes can cause a
  type confusion between engines.
- RUSTSEC-2026-0424: a transfer of borrowed resources between components
  does not check memory use.

The changes:

- `Cargo.lock` moves the `wasmtime` 48.0.x family to 48.0.8, with its
  `cranelift` crates at 0.135.8. The resolver also re-points a few
  dependency edges inside the allowed semver ranges: `windows-sys`,
  `itertools` and `getrandom`.
- The manifest floor in `autumn-harvest` moves from 48.0.7 to 48.0.8. A
  downstream consumer reads the manifest, not this lockfile, so only the
  floor keeps it off a vulnerable release.

**Checks.** `cargo deny check advisories` failed with the three ids before
the change. It reports `advisories ok` after the change.

**No migration. No new `WorkflowEvent` variant. No public API change.** The
`wasm-activities` feature still needs Rust 1.95, as `wasmtime` 48 does.
