## Dependency ledger: croner dedup rehearsed, deferred with a migration sketch

`deny.toml`'s `[bans]` comment already named `croner` as the one duplicate-version
finding in this repo's dependency graph where both sides are in reach: our own
direct `croner = "2.2"` vs. autumn-web/tokio-cron-scheduler's `^3.0` (resolving
to 3.0.1), flagged by a prior Ballast run as "a candidate for a dedicated dedup
PR, not bundled into this harness." This run took up that candidate.

Rehearsed on a branch: bumped `autumn-harvest/Cargo.toml`'s `croner` to
`"3.0.1"` and migrated the four `Cron::new(expr).with_seconds_optional()
.parse()` call sites (`policy.rs` x2, `scheduler.rs` x2) to 3.x's builder,
`CronParser::builder().seconds(Seconds::Optional).build().parse(expr)` —
confirmed via docs.rs as 3.x's own documented equivalent, since `Cron::new`
and `with_seconds_optional` were removed from `Cron` itself in the 3.0 major.
`cargo check -p autumn-harvest --all-features` came back clean and
`Cargo.lock` collapsed to a single `croner` instance (3.0.1).

`cargo test -p autumn-harvest --lib --features tls` caught a real behavior
change the API migration alone didn't reveal: 3 of 98 `scheduler::tests`
failed (`plan_backfill_timestamps_hourly_cron_inclusive_bounds`,
`catchup_run_plan_window_cron_jump_starts_at_cutoff`,
`plan_backfill_timestamps_7_day_hourly_cron_within_default_limit`).

**Correction**: a Codex review on the PR that rehearsed this caught the
mechanism as first written up here being wrong, and it was. All three
failing tests use a plain `Schedule::Cron`, never `CronInTimezone`, so they
never reach the one branch that calls `find_next_occurrence` with
`inclusive = true`. They go through the strict (`inclusive = false`) branch
instead, called with a `reference` deliberately undercut below the real
boundary of interest (`from - 1ms` in `plan_backfill_timestamps`,
`cutoff - 1ns` in the window-jump cron fallback) so that a strict search
lands exactly on that boundary. Under croner 2.2.0 it does; under 3.0.1 the
returned instant instead carries the original sub-second remainder forward,
landing roughly a full second later than 2.2.0's result (not milliseconds
earlier, as this entry first said) — and that drift drops one occurrence
from each multi-step walk. `policy::tests` (90 tests) were unaffected — they
only validate expressions, never call `find_next_occurrence` at all.
Upstream's `CHANGELOG.md` documents the 3.0.1 and 4.x releases but has no
entry for the 3.0.0 major itself, so this was not something reading the
changelog alone would have surfaced.

Reverted rather than shipped: both affected call sites' "undercut the
boundary to turn a strict search into an effectively-inclusive one" trick is
load-bearing catchup/backfill scheduling logic (issue #484 / Codex #3069's
window-cutoff jump-start depends on the window-fallback instance of it), and
the rehearsal only has full-integration evidence, not a minimal croner-only
repro confirming the mechanism is croner's own change rather than an
artifact of this migration. `deny.toml`'s `croner` comment now carries the
corrected, full rehearsal (numbers and all) plus a migration sketch —
isolate a minimal repro against both versions, then either pursue an
upstream fix/clarification or stop relying on the undercut-reference trick
— and a revisit trigger (an upstream fix lands, or someone completes the
sketch). No dependency graph change: `Cargo.lock` and
`autumn-harvest/Cargo.toml` are back to their pre-rehearsal state; only
`deny.toml`'s comment changed. `cargo deny --all-features check` green
before and after (advisories ok, bans ok, licenses ok, sources ok); the one
pre-existing warning (`RUSTSEC-2026-0253` "advisory was not encountered") is
unrelated prior history, already documented in that ignore entry's own
comment.
