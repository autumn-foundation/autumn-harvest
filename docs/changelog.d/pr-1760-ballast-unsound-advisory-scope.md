## Dependency ledger: restore unsound-advisory transitive scope (PR #1760)

`deny.toml`'s `[advisories]` section had documented `unsound` advisory
scoping as a solved problem: cargo-deny 0.18 removed the
`[advisories.unsound]` field and unconditionally denied `unsound`
advisories for every reachable instance, workspace or transitive. That
stopped being true in cargo-deny 0.19.0 ([PR#826](https://github.com/EmbarkStudios/cargo-deny/pull/826)),
which reintroduced the field defaulting to `Scope::Workspace` — silently
narrowing coverage back to workspace-direct dependencies only, with no
deprecation warning or config-validation failure. This repo's pinned
cargo-deny tracked the 0.18.9 -> 0.20.2 upgrade without `deny.toml`
changing, so the gap reopened unannounced: `cargo deny check` kept exiting
0, with only a low-signal `advisory-not-detected` warning on the
RUSTSEC-2026-0253 ignore entry that looked identical to the entry simply
going stale.

Fix: `unsound = "all"` restored under `[advisories]`. Rehearsal:
`cargo deny check advisories -s` goes from "0 errors, 1 warnings, 6 notes"
to "0 errors, 0 warnings, 8 notes" — the two new notes are
RUSTSEC-2026-0253 being evaluated against `lru` 0.16.4 (ratatui-core's
transitive pin) again, confirming the existing documented "unreachable"
verdict rather than silently skipping the check. No dependency graph
change: `Cargo.lock` untouched, 776 packages before and after. `cargo deny
check` (advisories, bans, licenses, sources) green both before and after.
