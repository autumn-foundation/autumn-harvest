## Dependency ledger: croner 2.2 -> 3.0 dedup rehearsed, pinned at 2.2 (Ballast)

`deny.toml`'s `[bans]` comment had flagged `croner` as the one duplicate-version
warning where both sides are in reach: our own direct `croner = "2.2"`
(`autumn-harvest/Cargo.toml`) vs. autumn-web's transitive `^3.0`, resolving to
two separate instances (2.2.0 and 3.0.1) in the same graph.

Rehearsed the bump on this branch. Usage is narrow — four call sites across
`autumn-harvest/src/policy.rs` and `autumn-harvest/src/scheduler.rs`, all
`Cron::new(expr).with_seconds_optional().parse()`. croner 3.0 removed the
`Cron::new`/builder API in favor of `FromStr` (`expr.parse::<Cron>()`), with
`Seconds::Optional` as the parser default — confirmed from both crates'
source (crates.io tarballs for 2.2.0 and 3.0.1) that this default matches our
explicit `with_seconds_optional()` call, so the port is mechanical. No new
crates would enter the graph either: 3.0.1's new direct deps
(`derive_builder` 0.20.2, `strum` 0.27.2) are already in `Cargo.lock` via
autumn-web's existing 3.0.1 instance.

`cargo check -p autumn-harvest --all-features` was clean after the port. The
rehearsal caught the problem anyway: `cargo test -p autumn-harvest --lib
--all-features` failed three scheduler tests that pass under 2.2 --
`catchup_run_plan_window_cron_jump_starts_at_cutoff` (returned 2 of 3 expected
hourly slots, the last at `22:00:00.999999999Z` instead of `22:00:00Z`),
`plan_backfill_timestamps_7_day_hourly_cron_within_default_limit` (168 vs. 169
expected), and `plan_backfill_timestamps_hourly_cron_inclusive_bounds`. All
three exercise `window_eligible_slots` (issue #484 / Codex #3069), which
leans on exact inclusive-boundary semantics at a window cutoff --
`find_next_occurrence`'s public signature is unchanged 2.2 -> 3.0.1, but its
internal boundary handling evidently is not.

Reverted the bump. Pinned `croner = "2.2"` with the rehearsal recorded inline
in `autumn-harvest/Cargo.toml`, and updated `deny.toml`'s `[bans]` comment
(previously calling this "a candidate for a dedicated dedup PR") to match.
Revisit trigger: a croner release whose changelog calls out an
inclusive-boundary/occurrence-count fix relative to 3.0.1, or a repro that
pins the exact internal change -- rehearse again from there, starting with
`scheduler.rs`'s three failing tests as the check.

Also recorded in the same `[bans]` comment: the duplicate-crate-name count
has grown from the documented baseline of 33 to 43 (`cargo deny check bans`,
0 errors either way, still `warn` not `deny`) since this workspace grew
(codec rotation, formal/Kani proofs, loom/Shuttle model checking, chaos
tooling). No action taken on that count itself -- it isn't this PR's finding,
just re-baselined for the next person who reads the comment.

No dependency graph change: `Cargo.lock` untouched. `cargo deny check`
(advisories, bans, licenses, sources) green before and after -- unchanged at
`advisories ok: 0 errors, 0 warnings, 8 notes`, `bans ok: 0 errors, 43
warnings, 0 notes`, `licenses ok: 0 errors, 0 warnings, 696 notes`, `sources
ok: 0 errors, 0 warnings, 0 notes`.
