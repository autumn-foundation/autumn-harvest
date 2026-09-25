# 🚦 Semaphore CI health — the `quota_enforcement_tests`/`integration_e2e.rs:1383` SOURCE-completion hang series closes; two more independently-diagnosed flakes and the shard rebalance landed alongside it; one open PR's harness needs a fresh run against today's manifest before merging

**Status:** health report — no PR opened against `ci.yml` or any test file. This
role's hard gate is not met because there is no new mechanism to diagnose: the
one item the last report treated as an open bug — the `integration_e2e.rs:1383`
hang itself — is fixed and merged, and this session's own check of the
post-merge CI history found no new failure signature to investigate.

**Correction (post-review, Codex on this PR).** An earlier draft of this
paragraph said "every open item the series was tracking... has already been
fixed," which overstates it. The last report's own Treatment section (line
254-266) also flagged a recurring bug-class follow-up as a candidate for a
*future* session, explicitly not attempted there: nothing checks that an
allowlisted hand-rolled migration bundle (like `integration_e2e.rs`'s
`INIT_SQL`) stays complete as new migrations land, which is exactly how this
hang's root cause was introduced in the first place. `676e79c3` only appended
the one missing migration; it did not add that guard, and
`migration_hygiene.rs`'s `no_new_handrolled_migration_bundles_outside_allowlist`
still only checks for new offenders and stale allowlist entries (verified this
session), not bundle completeness. That item is still open — carried forward
below, not closed by this report. Nothing here changes the tolerance of any
test; this is confirmation of the hang's fix plus two procedural flags.

## 🎯 Verdict path

Unchanged from the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, `Test DB (linux, shard N)`. What changed is the shard count itself
(11 → 21, below) and the schema/config each shard's containers boot from.

## 🌡️ Symptom — closed, not just quiet

The prior report counted 9 confirmed occurrences of the byte-identical
`integration_e2e.rs:1383:6` panic
(`workflow should reach expected state within timeout: Elapsed(())`) across
this series, tracing to the same root cause this session already verified twice
independently (Docker-based, in PR #1713's own report, and Docker-free, in the
prior session's local-Postgres rerun harness: 5/5 fail unpatched, 20/20 pass
patched). That fix, plus two related-but-distinct flake fixes, are all on
`trunk-dev` now, though not all landed where their own PR number would
suggest:

| Commit | Time (UTC) | PR | Mechanism |
|---|---|---|---|
| `676e79c3` | 09-24 14:28:22 | #1713 | `INIT_SQL`'s hand-rolled bundle in `integration_e2e.rs` was missing migration `20260920215812_harvest_completion_trigger_fires_target` — this series' own tracked defect |
| `49d1a56c` | 09-23 13:01:20 | #1710 | `quota_enforcement_tests`'s outbox-retry path didn't wire `sharded_pool` into the test's own worker config (issue #1685) — a **different** mechanism hitting the same test module, tracked separately since `docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md` |
| `f08842e0` | 09-24 14:29:37 | #1719 | `sharded_runtime_tests`'s timeout-pass hold-and-wait wedges on an exhausted shard pool — a third, distinct suite |

**Correction (post-review, Codex on this PR).** An earlier draft attributed
the outbox-retry wiring fix to `5a3cf148` (PR #1706, titled for exactly this
fix) and grouped all three as landing within two minutes of each other.
Checking `5a3cf148`'s own diff shows it touches only one file —
`docs/rnd/2026-09-22-ci-health-semaphore-quota-outbox-root-cause.md` — no
code. The actual `worker_cfg.sharded_pool = Some(sharded_pool)` line, with a
comment citing issue #1685 by name, is in `49d1a56` (`⚡ Bolt: own payload
fields instead of cloning them`, PR #1710), an otherwise-unrelated
payload-ownership performance PR that happened to touch the same test file
and landed a full day earlier, on 09-23. PR #1706 appears to have been
opened for the same fix independently, then merged after Bolt's PR already
carried the identical change on `trunk-dev`, leaving only its own docs
artifact as new content by the time it landed. Corrected above: the table
now cites `49d1a56` and its real timestamp, not `5a3cf148`'s.

Then `285c7fa0` (09-24 17:27:14 UTC, PR #1707) rebalanced `test-db-linux` from
11 to 21 shards, for the slowest-shard latency this series' shard-weight-drift
harness (open PR #1703, still unmerged) had already flagged, not primarily to
un-collide `integration_e2e`/`quota_enforcement_tests` (that collision's
*symptom* is now moot since the underlying test no longer hangs, but the
collision itself was never the root cause — the missing migration was).

**Post-fix verification (this session, from CI history, not a fresh rerun
harness), corrected for docs-only skips (see below):** two trunk-dev pushes
have completed a genuine full `Test DB (linux, *)` matrix since `676e79c3`
merged, confirmed by per-job run duration (real execution: ~15-30 minutes
per shard; a docs-only push completes the same step in single-digit
seconds because `ci.yml`'s `Run Linux Docker-backed manifest suites` step
is gated on `needs.changes.outputs.code == 'true'` and skips instantly
otherwise). Neither shows this signature or any
`quota_enforcement_tests`/`sharded_runtime_tests` failure:

- `285c7fa0` (run `36034407319`, first 21-shard run): 21/21 shards green, ~18-28 min/shard.
- `b0f5a4d1` (run `36047203664`, current `trunk-dev` head): 21/21 shards green, ~20-28 min/shard.

**Correction (post-review, Codex on this PR).** An earlier draft listed
`5a3cf148` (run `36013321172`) as a third full sweep. That commit is
docs-only (see the correction above), so `needs.changes.outputs.code` was
`false` for its run and every `Test DB (linux, *)` job's actual suite step
was skipped — the jobs still report `success` (a skipped step doesn't fail
a job), but none of them exercised the code under test. Removed from the
count; the real number is 2 full sweeps, not 3.

That is 2 full sweeps and 0 recurrences — real signal, but explicitly
**not** this role's ≥20x same-commit rerun protocol (each sweep is a
different commit, not 20 reruns of one), so it is reported as corroboration
of the prior session's already-rigorous local verification, not as an
independent rerun-protocol result in its own right. Of the `pull_request`-event
runs sampled from the last 24 hours, three ran the suite for real and none
showed the tracked panic (`#1727`, `#1731`, `#1734`, each confirmed by the
same duration check, all green, all post-dating the fix); a fourth
(`#1732`) is also docs-only and skipped the suite the same way `5a3cf148`
did, so it is not counted as evidence either way.

**Series closed.** Nine confirmed occurrences, one root cause, one merged fix,
zero recurrences across every sampled post-merge run. No further action item
carries forward from this specific signature.

## 🔍 Diagnosis note — nothing new, one correction to the record

Nothing in this session changes the diagnosis already rendered and twice
verified in the prior two reports. One bookkeeping note for future sessions
reading this series: the shard rebalance and the three flake fixes are
independent changes — `49d1a56` landed a day ahead of the other two
(09-23), which landed within two minutes of each other (09-24 14:28-14:29
UTC), and the rebalance followed three hours after those.
Do not attribute the hang's fix to the rebalance — the collision
(`integration_e2e`/`quota_enforcement_tests` both landing on shard 0 under the
old 11-shard layout) only ever explained why both suites' failures showed up
in the *same* CI job; it was never the reason either suite failed on its own.

## 🔧 Treatment — two procedural flags, neither actioned

**Carried forward, not fixed: the allowlist-completeness gap.** The prior
report (`docs/rnd/2026-09-24-...-confirmed-fixed.md:254-266`) named this as
a real, recurring bug class — an allowlisted hand-rolled migration bundle
(`integration_e2e.rs`'s `INIT_SQL`, on `ALLOWED_HANDROLLED_MIGRATION_INCLUDES`)
has no check that it stays complete as new migrations land, which is exactly
how this series' whole tracked hang got introduced on 09-20. `676e79c3`
fixed the one missing migration; it did not add that check. Verified this
session: `migration_hygiene.rs`'s `no_new_handrolled_migration_bundles_outside_allowlist`
still only asserts (1) no new file outside the allowlist reintroduces a bundle
and (2) no allowlisted entry has gone stale — neither direction compares an
allowlisted bundle's included migrations against the full `migrations/`
directory. Not fixed here either, for the same reason the prior report gave:
this is a real product/test-harness improvement, not a health-report action.
Carrying it forward again so a future session doesn't have to re-discover it.

**Correction (post-review, Codex on this PR).** An earlier draft of this
section claimed merging PR #1703 as-is "would reintroduce a shard-count
assumption the codebase has already moved past" and that its harness "was
authored and self-tested against the 11-shard manifest." That is wrong:
`docs/rnd/2026-09-23-ci-health-semaphore-shard-0-rebalance.md:143-161`
already fetched PR #1703's `shard-weight-drift.py` from its branch and ran
its `--sweep` (which covers `SEMAPHORE_SHARD_COUNT` 9 through 24, so 21 is
inside its range, not outside it) against that day's manifest — and that
same report's final choice of N=21 (line 168) is the value `285c7fa0`
shipped. The harness is N-agnostic by construction and was not blind to
21; it was, if anything, the evidence base the rebalance drew on.

**PR #1703** (`🚦 Semaphore: shard-weight drift harness (report-only, 2
collisions found)`, opened 09-22, a prior session in this same series) is
still worth a fresh look before merging, for a narrower reason than
originally stated here: its branch is three days old and unmerged, its
`mergeable_state` reads `unknown`, and `docs/audits/shard-weight-drift.py`
does not exist on `trunk-dev` today, so nothing has re-run its
`--self-test`/`--sweep` against whatever the manifest looks like *now* —
after the rebalance and after however many test files other PRs have added
or removed since 09-22. That is a staleness concern about the manifest's
current content, not about the shard-count range the harness already
covers. This is not this session's fix to make unilaterally (the PR
belongs to a different session); flagging here so the next session in this
series rebases it, re-runs both commands against the current manifest, and
confirms the 2 previously-found collisions either persist or have moved,
before merging.

**Ledger:** still no quarantine ledger in this repository (unchanged from
every prior report in this series).

## 📊 Measurement

No new rerun harness executed this session. Evidence is entirely from the
CI history table in Symptom above (GitHub Actions run/job data for the two
genuine full-matrix `trunk-dev` pushes and three genuine sampled
`pull_request` runs since the fix merged, each confirmed by per-job
duration rather than by conclusion alone), which is reproducible by any
session with API access, no Docker/Postgres environment needed.

## 🔬 Reproduce

```sh
# Confirm the three fix commits and the rebalance commit, in order.
# --no-walk is required: without it, git log treats four revisions as a
# range/set to walk ancestry from, printing every reachable ancestor
# (6ddbc976, 90f5c714, ...) instead of just these four rows.
git log --no-walk --format='%H %ad %s' --date=iso 676e79c3 49d1a56c f08842e0 285c7fa0

# Confirm 49d1a56, not 5a3cf148, carries the outbox-retry code fix:
git diff 5a3cf148^ 5a3cf148 --stat   # -> one docs/rnd file, no code
git diff 49d1a56^ 49d1a56 -- autumn-harvest/tests/integration/quota_enforcement_tests.rs \
  | grep -n "sharded_pool"           # -> worker_cfg.sharded_pool = Some(sharded_pool)

# Confirm zero recurrences: pull job results for the two genuine full
# trunk-dev sweeps since the fix (via actions_list/list_workflow_jobs on
# runs 36034407319, 36047203664) and grep for anything other than
# "success" among jobs named "Test DB (linux, shard *)" or
# "sharded_runtime_tests"/"quota_enforcement_tests" in their logs.

# Confirm a run's Test DB jobs actually executed the suite rather than
# skipping it: compare each job's started_at/completed_at. A skip
# completes in single-digit seconds (e.g. run 36013321172, 5a3cf148's
# docs-only push); real execution takes ~15-30 minutes per shard (e.g.
# runs 36034407319, 36047203664, or any of #1727/#1731/#1734's own CI).

# Confirm PR #1703 is stale:
git merge-base --is-ancestor 285c7fa04c18e8862c7601f24e8fae7667db88f9 \
  <PR #1703 head sha>   # -> not an ancestor
ls docs/audits/shard-weight-drift.py   # -> absent on trunk-dev today
```
