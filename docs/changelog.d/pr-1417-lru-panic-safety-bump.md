## Bump `lru` 0.16 -> 0.18.4, closing our own RUSTSEC-2026-0253 exposure (PR #1417)

`deny.toml` (PR #1393) deferred RUSTSEC-2026-0253 — a use-after-free in
`LruCache::pop()` when a stored key's `Drop` panics mid-pop — as
unreachable for `autumn-harvest/src/cache.rs`'s `WorkflowCache` (its key
and value types have no custom `Drop`), and flagged the fix (`lru` >=
0.18.2) as a candidate for "the next scheduled batch" once a changelog
read confirmed no breaking API surface. This is that scheduled batch: it
was the only routine update due this cadence window (`cargo update
--dry-run --workspace` found nothing else within existing manifest
ranges).

Read `lru`'s `CHANGELOG.md` end to end from 0.16.4 to 0.18.4 (the current
release): every entry between them is additive (new methods, an MSRV
bump, and the 0.18.2 panic-safety fix itself). Nothing touches `new`,
`put`, `get`, `pop`, `len`, `is_empty`, or `cap` — the entire surface
`cache.rs` calls — so the bump carries no migration.

Change: `autumn-harvest/Cargo.toml`'s `lru = "0.16"` -> `"0.18"`; lockfile
now at 0.18.4. This also unified with `autumn-web`'s own transitive `lru`
dependency, which independently resolved to 0.18.0 before this PR — a
version still inside the vulnerable range (< 0.18.2) — onto the same
patched 0.18.4 instance, closing `autumn-web`'s exposure as a side effect,
not just ours.

One instance of the advisory remains in the graph and keeps its
`deny.toml` ignore entry: `ratatui-core` 0.1.0 (pulled in via
`autumn-harvest-cli` -> `ratatui`) pins `lru = "0.16"` in its own
manifest — external, not ours to bump — resolving to 0.16.4. Traced its
`LAYOUT_CACHE` usage in `ratatui-core`'s source: it never calls `pop()`,
only `resize()` -> `pop_lru()`, and read the two methods side by side in
`lru` 0.16.4's source — only `pop()` drops the key in place before
detaching the list node, which is the exact defect; `pop_lru()` doesn't
do that sequence, so it cannot trigger this bug regardless of the stored
key/value types' `Drop` behavior (which, separately, also have none:
`(Rect, Layout)`, neither with a custom `Drop` impl). Verdict:
unreachable, on two independent grounds. `deny.toml`'s ignore entry is
rewritten to record this rather than repeat the now-fixed reasoning about
our own call site.

Rehearsal: `cargo check -p autumn-harvest --all-features` and `cargo test
-p autumn-harvest --lib cache::` (8/8 passed) both green; `cargo check
--workspace` (default features) and `cargo check -p autumn-harvest-cli
--all-features` (the `ratatui`/lru-0.16.4 consumer) both green too.

Measurement: `cargo tree --workspace --all-features -e normal` unique
(name, version) count unchanged (566 before/after — this was a version
swap on an existing edge, not a graph-shape change); duplicate-version
crate-name count unchanged (36) since `ratatui-core`'s independent 0.16
pin still coexists with the now-shared 0.18.4 instance. The improvement
is entirely in which version is reachable from code we own: our own and
`autumn-web`'s instances both move off the vulnerable range entirely.
