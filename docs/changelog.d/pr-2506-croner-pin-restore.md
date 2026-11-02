## Dependency ledger: restore the lost `croner` pin rehearsal, correct a stale duplicate count

Routine Ballast ledger sweep (2026-11-02). `cargo deny check` came back
fully green: 0 errors, 0 warnings on advisories, 66 warnings on bans (all
duplicate-version, `multiple-versions = "warn"`), 0 on licenses and sources.
All 12 advisory notes matched the 6 documented `[advisories] ignore` entries
exactly — no new RUSTSEC advisory, no stale ignore entry, no duplicate
ignore entries. No security-class action needed this run.

Two documentation defects found and fixed, both in `deny.toml`'s `[bans]`
comment, neither a dependency-graph change:

1. **A finished rehearsal never landed.** PR #2142 (2026-10-12) rehearsed
   bumping our direct `croner = "2.2"` to 3.0.1 (the version
   tokio-cron-scheduler pulls in transitively through autumn-web), migrated
   all 4 call sites to the 3.x builder API, and ran the full
   `scheduler::tests` suite. It found a real correctness regression: 3 of 98
   tests failed, because two call sites rely on a strict (exclusive) search
   landing exactly on a reference 1ms/1ns below a whole-second boundary, and
   croner 3.0.1 instead carries the sub-second remainder forward, landing a
   full second late and dropping one occurrence from the walk. The PR
   reverted the rehearsal and was meant to record the finding as a
   documented pin in `deny.toml` — but its one CI run hit an infra loss
   (runner shutdown, exit 143) on an unrelated test, unrelated to its
   docs-only diff, and the PR went stale and was closed unmerged on
   2026-10-26 with no follow-up push. The rehearsal itself is still valid
   evidence; the `deny.toml` comment never got the update it described.
   This sweep restores it in full, with the regression mechanism, the two
   call sites, and a revisit trigger.

2. **A stale graph fact.** The same comment still said "33 duplicated crate
   names," the count from this harness's original baseline (PR #1303,
   2026-09-02). It's 66 now — `cargo deny check -s` is the reproduction
   command. Restated the sentence to point at that command instead of a
   literal, per this repo's own "a live count in prose is a conflict
   magnet" rule (`CLAUDE.md` § Keep docs guards from becoming conflict
   magnets), so the next sweep doesn't have to fix the same drift again.

New graph fact recorded while restoring the comment: `croner` now resolves
to **three** instances, not two — 2.2.0 (ours, direct), 3.0.1
(tokio-cron-scheduler's own pin, unchanged), and a newly-appeared 4.0.2
(autumn-web's own direct use). `autumn-web` is still locked at exactly
0.9.0 in our `Cargo.lock`, so the third instance is Cargo's resolver
picking up a newer croner release against autumn-web's own requirement
string on a lockfile refresh, not an autumn-web version bump. Nobody has
rehearsed croner 4.x against the same regression; that's an open question
for a future sweep, not a finding either way. Our own pin stays at 2.2
until either the two external instances converge on one version, or
someone rehearses 4.x clean the same way PR #2142 rehearsed 3.0.1.

Rehearsal for this PR's own diff: `cargo deny check -s` before and after the
comment edit — identical output (0 errors, 0/66/0/0 warnings, 12/0/747/0
notes). No `Cargo.toml`/`Cargo.lock` change, so no `cargo check`/`cargo
test` rehearsal applies; this is a documented-pin restoration, not a
dependency bump, and the impact floor doesn't apply to it.

No `WorkflowEvent` variant, no migration, no engine-runtime change — this
touches only `deny.toml`'s comment and this fragment.
