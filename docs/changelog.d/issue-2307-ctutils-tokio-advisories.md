## Fix — ctutils 0.4.5 and tokio 1.53.3 for RUSTSEC-2026-0385 and -0386 (issue #2307)

**Security fix.** The daily advisory scan failed on two new advisories.

- `ctutils` 0.4.2 builds mask generation code that can branch on secret
  values (RUSTSEC-2026-0385). It is a transitive dependency of the RustCrypto
  `digest` 0.11 family. Version 0.4.5 fixes it.
- `tokio` 1.53.2 has unsound `AsyncFd` constructors (RUSTSEC-2026-0386).
  Version 1.53.3 fixes it.

The changes:

- `Cargo.lock` moves `ctutils` to 0.4.5 and `tokio` to 1.53.3. The new
  `tokio` also changes which `windows-sys` versions some crates resolve to.
  Cargo drops `windows-sys` 0.60 and `windows-targets` 0.53, which nothing
  needs any more. These changes affect Windows targets only.
- The workspace `tokio` floor moves from 1 to 1.53.3. A downstream consumer
  reads the manifest, not this lockfile, so only the floor keeps it off
  1.53.2 (as in PR #1303).

**No migration. No new `WorkflowEvent` variant. No public API change.** The
lockfiles under `docs/assays/apparatus/` are separate. The advisory scan does
not read them, so they keep their old versions.

**Checks.** `cargo deny --all-features check` with cargo-deny 0.20.2, the CI
version, reports `advisories ok, bans ok, licenses ok, sources ok`. Before
the change it failed on both advisories.
