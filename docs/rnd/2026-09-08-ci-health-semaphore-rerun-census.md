# 🚦 Semaphore CI health — fresh rerun census and failure-signature clustering

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
up on `docs/rnd/2026-09-03-ci-health-semaphore.md` through
`docs/rnd/2026-09-06-ci-health-semaphore-cache-audit-followup.md`. This report runs
the rerun-button census and, new this round, clusters every red `pull_request`-event
`ci.yml` run in the sample by failure signature — the prior reports established the
cache-correctness and windows-`no-db` timing findings but had not yet checked whether
any of the reds in this repo's history are actually flaky.

## 🎯 Verdict path

Branch-protection status for `test-db-linux`'s 10 shards and `test-nodb`'s 12 shards
is still unconfirmed from this session — no branch-protection-read tool is exposed
here, checked again today. Not re-diagnosing; flagging that the gap is still open.

**Not unchanged, per a Codex review comment on this PR:** `ci.yml` now also defines
`openapi-client-smoke` (`ci.yml:984-990`), added after the 09-06 report and carrying
the identical "not yet enforced" comment as `test-nodb`/`test-db-linux`. This report's
own earlier draft said the verdict path was "same as all four prior reports," which
missed that a third unenforced check now exists — a failure in generated-client
generation or execution may also not block merging, unconfirmed the same way as the
other two families.

## 🌡️ Symptom

### 1. Rerun-button census, repeated

100 most recent completed `pull_request`-event `ci.yml` runs, `run_attempt` field
checked directly: **0/100 show `run_attempt > 1`.** Identical to the 09-03 and 09-06
reports' findings — still no reflexive-rerun culture, no whole-job or workflow-level
rerun in the sample. Conclusion split across the same 100: 25 success, 66 cancelled
(superseded by a later push to the same PR — expected under a cancel-in-progress
concurrency group, not a CI-health signal), 9 failure.

**Narrowed per a Codex review comment on this PR:** `run_attempt` cannot see a
retry that happens *inside* a single job step, and one exists —
`openapi-client-smoke` (`ci.yml:1057-1059`) runs `npm ci --no-audit --no-fund ||
(sleep 10 && npm ci --no-audit --no-fund)`, an in-step sleep-and-retry around the npm
registry specifically, with a comment at the call site limiting it to that one
external boundary ("the registry is the one thing here that fails for unrelated
reasons. Two tries, then it is real."). That is the shape this role's own charter
calls sanctioned — declared, bounded, at a genuinely external boundary — not the
banned ambient/suite-level kind. But it does mean this census cannot support "no
retry wrapper anywhere in this pipeline" as a blanket claim, and it cannot observe
this class of transient failure (a first-attempt `npm ci` failure recovered by the
second attempt still shows as `run_attempt == 1` and a green conclusion). Narrowed
to what the census actually measures: no workflow- or job-level reruns.

### 2. Every one of the 9 failures, root-caused, not just tallied

The prior reports never checked whether this repo's reds are flaky or legitimate.
This round pulled the job logs for all 9 failed runs in the sample and clustered
them by signature:

| signature | count | mechanism |
|---|---:|---|
| `Comment hygiene (docs/audits/comment-hygiene.py)` | 4 | Tier B ratchet catching real new violations (review-round archaeology / long sentences) introduced by that commit's own diff — different files and lines every time |
| `docs/rnd/sqlite-feasibility.md guards` → `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` | 2 | live count of migration directories (99, then 100 four hours later) no longer matches the frozen count string in the doc — a doc/code sync gate, correctly firing on two different commits that each added a migration without updating the doc |
| `docs/performance.md guards` → `performance_docs::claim_transaction_statements_are_all_named_in_the_docs` | 1 | same doc/code sync pattern: a new transaction statement not yet named in `docs/performance.md` |
| `Clippy autumn-harvest-plugin` (`clippy::result_large_err`) | 1 | a genuine 128-byte `Err` variant introduced by that commit — real lint violation |
| runner infra | 1 | `Test (no-db, ubuntu-latest, shard 2)`, run `34059892793`: `##[error]The runner has received a shutdown signal` mid-compile, exit 143 — a GitHub-hosted-runner preemption, not a suite defect |

**9/9 root-caused; 0/8 suite-attributable flakes; 1/9 external infra transient.** A
Codex review comment on this PR correctly caught an earlier draft's unqualified
"0/9 flaky," which contradicted this same section's own identification of the runner
shutdown as nondeterministic — a known root cause does not make a failure
deterministic, and an external preemption is a form of nondeterminism, just not a
suite defect. Corrected: 8 of the 9 are a deterministic function of that commit's own
diff (no same-commit disagreement possible, since the defect is in the diff itself);
the 9th (run `34059892793`) is nondeterministic but external to the suite — the
runner disappeared mid-compile, not mid-assertion, and does not recur elsewhere in
this 9-run sample (it does recur elsewhere in the fuller sample — see §3). No evidence
of shared-state leakage, timing races, or order dependence in any of the 9.

### 3. The 9-run sample undercounted real failures — a Codex review comment on this PR caught the gap, and it's real

**A Codex review comment on this PR correctly flagged a methodology hole:** §1's
census only classified runs by their overall `conclusion`, and both matrix jobs run
`fail-fast: false` (`ci.yml:494-501`, `ci.yml:782-788`). Under GitHub's
cancel-in-progress concurrency group, a shard can fail while sibling shards are still
running; if a newer push arrives before they finish, the whole run's `conclusion`
becomes `cancelled`, silently absorbing the earlier failure. §1's own "66 cancelled,
not a CI-health signal" framing assumed every cancelled run's jobs were themselves
clean cancellations — untested.

Pulled `list_workflow_jobs` for 10 of the 66 cancelled runs in the original 100-run
sample (not all 66 — see caveat below) and checked every job's own `conclusion`, not
just the run's:

| run | hidden job failure | mechanism |
|---|---|---|
| `34066621806` | `Lint` → `Check formatting` | a real `cargo fmt` diff in `autumn-harvest-plugin/tests/openapi_spec.rs` — deterministic, that commit's own formatting violation |
| `34057554518` | `Test (windows-latest)` and `Test (macos-latest)`, identically | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` panics: migration `20260906164501_shard_migration_legal_hold_guard` missing from `docs/upgrading/0.5.0.md` — the same doc/code-sync class as `sqlite_feasibility_docs`/`performance_docs` in §2, deterministic, that commit's own omission |
| `34165189395` | `Test (macos-latest)`, `Test (ubuntu-latest)`, `Test (windows-latest)`, identically | compile error `E0433`: `MemoryDispatch` is used unconditionally in `autumn-harvest/src/worker.rs:37779` but the type is gated behind `#[cfg(feature = "testing")]` — a real, deterministic feature-flag mismatch in that commit, reproducing identically on all three OSes because it's a compile-time defect in shared code, not a runtime race |
| `34177004263` | `Test DB (linux, shard 8)` → `business_day_timer_tests::business_day_calendar_loaded_from_db_resolves` | panics with `failed to start Postgres container: Client(PullImage { descriptor: "postgres:16", ... "bytes remaining on stream" })` — a Docker registry image-pull transient, the "unpinned external service" category this role's own charter names. **This is a flake**, not a suite defect: the same commit would very likely pass on a retry, since nothing about the test or the code under test caused a corrupted registry stream. |

6 of the 10 sampled cancelled runs (`34201689353`, `34150872938`, `34107996449`,
`34089954017`, `34075787182`, `34055805573`) had no hidden failures — every job in
them was either a clean `success` or a clean `cancelled` with no prior `failure`.

**A Codex review comment on this PR correctly flagged that the first pass of this
check didn't request `perPage=100`, and this workflow's matrices can expand past the
default 30-job page for a full code PR.** Re-ran `list_workflow_jobs` with
`perPage=100` for all 6 "clean" runs: each returns `total_count` 12 or 13, matching
the number of jobs actually listed both times — the matrix jobs in each of these 6
runs never expanded past their single unexpanded placeholder entry, because
cancellation landed early (during `Lint` or before), before the gating jobs that
unlock matrix expansion had finished. No pagination gap existed for these 6, and the
4/10 hit-rate tally is unchanged. (The 4 runs where a hidden failure *was* found each
had a `total_count` of 13–35 that matched their returned array length too, checked at
the time — including the two fetched with an explicit `perPage=50`.)

**Revised tally across the 19 runs actually inspected (9 explicit-failure + 10
sampled cancelled):** 13 real failures found, not 9. Of those 13: 11 are deterministic,
commit-specific defects (comment hygiene ×4, doc/code-sync gates ×4 across three
different guard tests, a real Clippy lint ×1, a real formatting violation ×1, a real
compile-time feature-flag defect ×1 — counting the 3-OS compile failure as one
defect, not three); **2 are external-infra nondeterminism** — the original runner
shutdown (§2) and this section's Docker-registry pull failure — not one. Both are
outside this pipeline's own configuration (a GitHub-hosted-runner preemption and a
third-party registry hiccup), and this role's charter treats that class as a
"genuinely external boundary," not a suite flake to fix.

**This is not exhaustive, and the correct denominator is unmeasured.** Only 10 of the
66 cancelled runs were checked — a 4/10 hit rate for a hidden failure in this small
sample, not a rate that safely extrapolates to "26 of 66." The honest statement this
report can make: the original rerun census's "66 cancelled, no CI-health signal"
framing understated real failures by at least 4, and the true count across all 66 is
unknown without checking the rest. That gap is itself a harness problem worth naming:
**a rerun-flakiness census on this pipeline needs to inspect job-level conclusions
inside cancelled runs, not just each run's own `conclusion`, or it will silently
undercount both deterministic defects and genuine flakes.** Building that harness (a
scheduled job that pulls every job's conclusion for every completed run, cancelled or
not) is the correct next step for whoever picks this up — flagged here, not built in
this report, since a report is this role's own gate for anything short of a
measured, ≥20-rerun-verified fix.

One incidental observation, not a flake and not actioned here: the
`sqlite_feasibility_docs` panic message repeats the same live-count number on both
sides of the sentence (`"the report should state \"**99 migrations**\"; a live count
finds 99 migration directories"` — both slots show 99, not the doc's stale value vs.
the live value). That's a diagnostics quality issue in the assertion message, not a
mechanism for the failure itself; the test's pass/fail behavior is unaffected, so it
does not meet this role's bar to open a fix PR on its own.

## 🔍 Diagnosis

**Category: none of the 13 found failures is a suite-level flake needing treatment;
two are external-infra nondeterminism worth tracking, not fixing here.** The
doc/code-sync gates (`sqlite_feasibility_docs`, `performance_docs`,
`migration_hygiene`) are working as designed: they fail deterministically on any
commit that changes a counted or listed quantity (migrations, transaction
statements) without updating the paired doc, and every observed failure traces to
that specific commit's own omission, not to suite state or interaction with other
PRs. The comment-hygiene failures are the same shape as the 09-03 report already
described this repo's Tier B ratchet doing. The Clippy and `cargo fmt` failures are
real, deterministic lint/format regressions. The compile-time feature-flag defect
(§3) is a real bug in that commit, reproducing identically across all three OSes
because a compile error has no runtime timing component to race on.

The two external-infra failures (the runner shutdown in §2, the Docker `postgres:16`
pull failure in §3) are genuinely nondeterministic, but neither is this pipeline's
to fix: one is a GitHub-hosted-runner preemption, the other a third-party registry
hiccup pulling a fixed, unpinned-by-digest image tag. Per this role's own root-cause
taxonomy, "unpinned external service" is a named flake category — but the sanctioned
response is a bounded, declared retry at that exact call site (as `ci.yml` already
does for the npm registry in `openapi-client-smoke`, per §1), not a suite-level
change, and two instances in a partial sample is nowhere near the ≥20-rerun bar this
role's own hard gate requires before treating it as a measured problem rather than
background noise.

## 🔧 Treatment

None shipped. Nothing found clears the impact floor: no *suite-level* flaky test to
make deterministic (none found — both external-infra failures are outside this
pipeline's configuration, and each has exactly one observed instance, far short of
the ≥20-rerun bar this role's own gate sets before calling something a measured
flake worth fixing), no product bug surfaced (11 of 13 failures are correctly
attributed to their own commit's defect, 2 are external infra), no timing win
identified beyond what
`docs/rnd/2026-09-06-ci-health-semaphore-cache-audit-followup.md` already measured
and credited, no quarantine entries to retire — there is still no quarantine ledger
in this repo, and no unowned flake quarantine was found. A Codex review comment on
this PR correctly caught an earlier draft overstating this as "nothing is skipped":
a repo-wide search at this commit's parent finds 31 `#[ignore = "..."]` tests
(Docker-only, performance, and manual-probe cases), each carrying a documented reason
at the point of use — owned, intentional skips, not a flake quarantine, but still
skipped by a normal test run. Per this role's own gate, a report is the correct
outcome here, not a PR against `ci.yml` or any test file.

Four items are now routed for whoever picks this thread up next — three carried
forward unchanged from prior reports, one new from §3:

1. **Cache-usage API access** (09-05/09-06 reports) — still no `cache/usage` or
   `caches`-listing method among the GitHub MCP tools available here.
2. **Branch-protection confirmation** (09-04/09-05/09-06 reports) — still no
   branch-protection-read tool available here; `test-db-linux` (10 shards),
   `test-nodb` (12 shards), and now `openapi-client-smoke` (see 🎯 Verdict path)
   all carry "not yet enforced" comments in `ci.yml`, unconfirmed against the live
   Settings → Branches state.
3. **Windows `no-db` long pole** (09-06 report) — unchanged; not re-measured this
   round since this session's sample was aimed at failure clustering, not timing.
4. **New: a cancelled-run job-conclusion harness** (§3) — the remaining 56 of 66
   cancelled runs in this sample are unaudited; someone should either extend the
   rerun-flakiness census to check every job's own conclusion (not just each run's
   overall conclusion) across the full set, or accept the 4/10 hit rate found here
   as directional evidence that this pipeline's cancelled-run bucket is hiding a
   non-trivial number of real failures and prioritize the harness accordingly.

## 📊 Measurement

- **Rerun-button census:** 0/100 workflow/job-level reruns, unchanged from
  09-03/09-06 (narrowed per §1 to what `run_attempt` actually observes).
- **Failure clustering, explicit-failure runs:** 9/100 sampled completed runs
  concluded `failure`; 9/9 root-caused; 0/8 suite-attributable flakes; 1/9 external
  infra transient.
- **Failure clustering, sampled cancelled runs (new, per a Codex review comment on
  this PR):** 10 of the 66 `cancelled`-conclusion runs inspected at job level; 4/10
  hid at least one real job failure (13 real failures total across both samples);
  of the 4 newly found, 3 are deterministic commit-specific defects and 1 is a
  second external-infra transient (Docker registry pull failure).
- **Combined:** 13 real failures found across 19 runs actually inspected; 11/13
  deterministic and commit-specific; 2/13 external-infra nondeterminism; 0
  suite-level flakes (no test disagreeing with itself on an unchanged commit). The
  remaining 56 cancelled runs are unaudited — this is a sample, not a census, of
  that bucket.
- No revert check applies — there is no fix in this report to verify red-then-green
  on.

## 🔬 Reproduce

```sh
# Rerun-button census:
# via actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event: "pull_request", status: "completed"}, perPage=100)
# then filter conclusion == "failure" and check run_attempt > 1

# Per-failure root cause:
# via actions_list(method="list_workflow_jobs", resource_id=<run_id>) to find the
# failed job/step, then get_job_logs(job_id=<id>, return_content=true,
# tail_lines=40-100) to read the panic/assertion or clippy diagnostic directly.

# Cancelled-run hidden-failure audit (§3): for each run with overall
# conclusion == "cancelled", pull list_workflow_jobs(resource_id=<run_id>,
# perPage=100) — this workflow's matrices can expand past the API's default
# 30-job page for a full code PR, so always pass perPage=100 (or page through
# the rest) and check total_count against the returned array length before
# trusting a run as "clean." Then check every job's own conclusion — a job
# conclusion of "failure" inside an overall "cancelled" run is the case this
# report's original sample missed.
```
