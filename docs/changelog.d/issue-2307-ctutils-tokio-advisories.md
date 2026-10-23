## Fix — ctutils 0.4.5 and tokio 1.53.3 for RUSTSEC-2026-0385 and -0386 (issue #2307)

**Security fix.** The daily advisory scan failed on two new advisories.

- `ctutils` 0.4.2 builds mask generation that can branch on secret values
  (RUSTSEC-2026-0385). It is a transitive dependency of the RustCrypto
  `digest` 0.11 family. 0.4.5 fixes it.
- `tokio` 1.53.2 has unsound `AsyncFd` constructors (RUSTSEC-2026-0386).
  1.53.3 fixes it.

The changes:

- `Cargo.lock` moves `ctutils` to 0.4.5 and `tokio` to 1.53.3. Cargo also
  drops the `windows-sys` 0.60 chain, which nothing needs any more.
- The workspace `tokio` floor moves from 1 to 1.53.3. A downstream consumer
  reads the manifest, not this lockfile (as in PR #1303).

**No migration. No new `WorkflowEvent` variant. No public API change.**

**Checks.** `cargo deny --all-features check` reports `advisories ok, bans ok,
licenses ok, sources ok`. Before the change it failed on both advisories.
