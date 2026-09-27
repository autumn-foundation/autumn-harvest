# 🚦 Semaphore CI health — refresh the stale shard-weight-drift harness (PR #1703), no new failure signature found

**Status:** harness PR (refresh), superseding a 5-day-stale open PR. No new
flake or product bug diagnosed this session; `trunk-dev`'s head is green and
the SOURCE-completion hang series (closed 2026-09-25) has no recurrence.

## 🎯 Verdict path

Unchanged: `ci.yml`'s `pull_request` trigger against `trunk-dev`,
`test-db-linux` (now 21-shard, Docker-backed Postgres). The `lint` job's
`docs/audits/*.py` checks are the ungated, always-run report layer this
session's change lives in.

## 🌡️ Symptom

The last report in this series (`docs/rnd/2026-09-25-...-series-closed.md`)
flagged PR #1703 (`shard-weight-drift.py`, opened 2026-09-22) as stale: three
days old then, branch `mergeable_state: unknown`, and nothing had re-run its
`--self-test`/`--sweep` against the manifest after the shard rebalance
(11→21, PR #1707) and whatever suites landed since. This session checked: the
PR is still open and unmerged five days on, still un-rebased.

Pulling PR #1703's `shard-weight-drift.py` and running it against today's
checkout surfaces two concrete drift effects, not just staleness in the
abstract:

1. **The self-test itself now fails.** `enabled_features_for_row("autumn-harvest", "-")`
   asserted `{"db", "unified-dag-execution"}`; the crate's real default
   feature set (read from `autumn-harvest/Cargo.toml`, not hand-copied) is
   now `{"db", "unified-dag-execution", "tls"}` — `tls` was added by
   `f53bb36` (issue #1717, merged 2026-09-24, two days after PR #1703 opened).
   The harness's own production code was never wrong; only the self-test's
   hand-written expected value, which is exactly the kind of fixture that
   goes stale the moment a crate's defaults change and nobody re-runs it.
2. **The findings table is stale, not just old.** Re-running the full script
   against today's manifest (156 `linux` rows, `SEMAPHORE_SHARD_COUNT = 21`)
   finds a completely different set of colliding shards than the 09-22
   report recorded (151 rows, `SEMAPHORE_SHARD_COUNT = 11`). None of the
   seven 09-22 collisions still collide — shard 0's `integration_e2e`/
   `quota_enforcement_tests` pair, the PR's headline example, now land on
   shards 12 and 2 respectively, un-collided. Today's five collisions are a
   different set entirely:

   | Shard | Colliding rows (ordinal, weight) |
   |---|---|
   | 4  | `shard_rebalance_db_tests` (88, 111), `workflow_rerun_integration` (151, 68) |
   | 8  | `event_partitioning_tests` (29, 155), `rate_limit_bucket_gc_tests` (50, 30) |
   | 10 | `interface_schema_integration` (115, 31), `stall_diagnosis_integration` (136, 76) |
   | 13 | `capability_miss_tests` (13, 46), `pacing_override_integration` (118, 46) |
   | 19 | `backup_verify_tests` (82, 57), `ui_integration` (145, 149) |

   Re-running `--sweep` (N=9..24) against today's manifest confirms the
   PR's core claim still holds under today's data, not just 09-22's: no
   swept count reaches zero heavy-suite collisions (floor is 4, at N=18,
   19, 20, 24); `SEMAPHORE_SHARD_COUNT = 21` (today's actual value) carries
   5 collisions but the smallest max/min shard-weight spread (214) of any
   count tried. The rebalance (PR #1707) moved the collision, as the
   script's own docstring already predicted it would, rather than removing
   it — this is now demonstrated on two different manifests five days apart,
   not asserted once.

## 🔍 Diagnosis

Test-vs-product verdict: **neither** — this is not a flake, it's a
report-only static-analysis harness whose own fixture (the self-test's
hand-written expected feature set) drifted out of sync with the crate it
inspects. Mechanism: `Cargo.toml`'s `default` feature list is live,
externally-owned state from this harness's point of view; a self-test that
hard-codes a snapshot of it will break every time that list changes, whether
or not the harness's own logic has a bug. The harness's actual counting
logic (weight-per-row, ordinal assignment, sweep) is unaffected and was not
touched beyond the self-test fixture and the stale docstring numbers.

Root-cause category for *why nobody caught this in 5 days*: the PR was never
merged, so it never ran in `ci.yml`'s `lint` job (which would have caught
the self-test regression the next time any PR touched `autumn-harvest`'s
default features) — an unmerged report-only harness protects nothing.

## 🔧 Treatment

This session cannot push to PR #1703's branch (different session's ongoing
work, and this session's own instructions bind it to a different branch), so
rather than leave a second stale review cycle on that PR, this change
re-lands the same harness, corrected, as a fresh PR from this session's own
branch:

1. **Superseded within this same PR (Codex review, round 1 on this PR):**
   the first push here fixed the self-test by adding `"tls"` to its
   hard-coded expected feature set. Codex correctly flagged that as the same
   defect one level down — `main()` runs `self_test()` unconditionally
   (including on every real, non-`--self-test` invocation), so the *next*
   legitimate default-feature change on `autumn-harvest` would break this
   PR's own newly-wired `lint` step the same way `tls` just broke PR #1703's.
   The actual fix, landed second: `self_test()` now redirects the module's
   `REPO_ROOT` to a disposable fixture crate tree (a temp `Cargo.toml` with
   an invented `default = ["alpha", "beta"]`) for the duration of the
   `crate_default_features`/`enabled_features_for_row` assertions, so the
   self-test no longer depends on `autumn-harvest`'s real feature list at
   all — nothing to hand-copy, nothing to go stale.
2. Replaced the docstring's 09-22 findings table and sweep claim with a
   dated update reflecting today's manifest and shard count, explicitly
   marking the old table as historical (kept for the three rounds of
   Codex-review corrections to the counting logic it documents) rather than
   a claim about current state — so the next session doesn't have to
   re-discover that the numbers moved.
3. Same `ci.yml`/`docs/audits/README.md` wiring PR #1703 proposed: one new
   `lint`-job step, report-only (always exits 0), catalogued.

No behavior change to the counting algorithm itself — this is a data
refresh plus a self-test fix, not a new diagnosis.

**Carried forward again, still not fixed:** the migration-bundle-completeness
gap first named in `docs/rnd/2026-09-24-...-confirmed-fixed.md:254-266` and
re-flagged in the 09-25 closure report. Verified again this session:
`migration_hygiene.rs`'s `no_new_handrolled_migration_bundles_outside_allowlist`
(autumn-harvest/tests/integration/migration_hygiene.rs:533) still only checks
(1) no new file outside `ALLOWED_HANDROLLED_MIGRATION_INCLUDES` reintroduces a
hand-rolled bundle and (2) no allowlisted entry has gone stale — neither
direction diffs an allowlisted bundle's included migrations against the full
`migrations/` directory. This is the actual root cause class behind the
now-closed `integration_e2e.rs:1383` hang (a hand-rolled `INIT_SQL` silently
missing a migration), and it can recur through the same allowlist today. Not
fixed here: it is a real product/test-harness change, one mechanism per PR,
and this session's PR is already the harness refresh above.

## 📊 Measurement

Before (PR #1703's branch, as opened 2026-09-22, run against today's
checkout): `--self-test` fails with an `AssertionError` on
`enabled_features_for_row`. After (this PR's branch): `--self-test` passes;
a full run resolves all 156 `linux` rows with no unresolved-file warnings;
`--sweep` completes N=9..24 and reproduces the "floor is 4, not 0" claim
above. `audit-catalog-coverage.py` and `comment-hygiene.py --base
origin/trunk-dev` both pass clean (0 changed `.rs` files, so comment hygiene
is a no-op; the new script is catalogued). `corpus-link-check.py` exits 0
(its pre-existing local-Windows-path findings in
`docs/plans/2026-06-22-canary-swarm-fixes.md` are unrelated and unchanged by
this diff).

Separately, this session checked `trunk-dev`'s own CI health since the
09-25 closure report: the current head (`8924bfc9`) is green, and the last
15 `pull_request`-event `ci.yml` runs sampled show only `success` or
`cancelled` (superseded-by-a-later-push on the same PR, not a failure)
conclusions — no new `Test DB (linux, *)` failure signature to report.

## 🔬 Reproduce

```sh
# Confirm PR #1703's harness fails its own self-test against today's
# Cargo.toml (fetch its branch, copy the script into place to get
# REPO_ROOT resolution right, run from the repo root):
git fetch origin claude/youthful-mendel-oykopw
git show origin/claude/youthful-mendel-oykopw:docs/audits/shard-weight-drift.py \
  > docs/audits/shard-weight-drift.py
python3 docs/audits/shard-weight-drift.py --self-test   # -> AssertionError

# Confirm the real default feature set has "tls" now:
grep -A3 '^\[features\]' autumn-harvest/Cargo.toml

# After checking out this session's fix instead:
python3 docs/audits/shard-weight-drift.py --self-test   # -> self-test: ok
python3 docs/audits/shard-weight-drift.py                # -> 156 rows, 5 collisions, N=21
python3 docs/audits/shard-weight-drift.py --sweep         # -> floor 4 collisions, N=18/19/20/24

# Confirm trunk-dev head is green and no new failure signature:
# (via actions_list/list_workflow_runs, event=pull_request, status=completed,
# on ci.yml) — sample the last ~15 runs and grep conclusions for anything
# other than success/cancelled.
```
