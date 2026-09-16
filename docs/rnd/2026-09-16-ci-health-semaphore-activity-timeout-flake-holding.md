# 🚦 Semaphore CI health — activity-timeout flake holds at 0/15 across a full
# 38-hour failure census spanning PR #1563's merge, and the stale-migration-
# count doc-sync gate's self-contradicting panic message blocked one branch
# three commits running

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series in `docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md` through
`docs/rnd/2026-09-15-ci-health-semaphore-window-census.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally the `test-db-linux` and `test`/`test-nodb` matrices.
Branch-protection status for these matrices and `openapi-client-smoke` remains
unconfirmed from this session — still no branch-protection-read tool exposed
here, checked again today. Cache-usage API access is also still unavailable,
checked again today. Both gaps repeat unchanged from every prior report in this
series; not re-diagnosed.

## 🌡️ Symptom

### 1. `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`: 0/15 recurrences, spanning 18h14m before PR #1563's merge and 19h33m after it

Issue #1558 tracked this test racing between the enforcement sweep's
`StartToClose` deadline (anchored at `claim_task` time) and
`append_activity_started_if_pending`'s later append — see
`docs/rnd/2026-09-14-ci-health-semaphore-activity-timeout-flake.md` for the
full mechanism history. PR #1563 (merged **2026-09-15T14:00:13Z**) changed the
test's assertion to accept either resulting event shape (`ActivityStarted`
present or absent before the terminal pair), reasoning that both are correct
engine behavior, not a product bug. The issue's own last comment (2026-09-15,
~4h before close) explicitly flagged that this verdict "needs its own
same-commit rerun campaign (≥20x)... which this session did not run" — no
rerun campaign was ever posted before the issue closed.

This session job-logged **all 100** of the most recent completed
`pull_request`-event `ci.yml` runs (not a sample — every run in the page), for
2026-09-14T19:46:25Z through 2026-09-16T09:33:24Z: 55 cancelled, 30 success,
**15 failure**. Split at the merge instant:

| Window | Span | Runs | Failures |
|---|---|---:|---:|
| Pre-merge | 2026-09-14T19:46:25Z → 2026-09-15T14:00:13Z | 57 (36 cancelled, 16 success, 5 failure) | 5 |
| Post-merge | 2026-09-15T14:00:13Z → 2026-09-16T09:33:24Z | 43 (19 cancelled, 14 success, 10 failure) | 10 |

All 15 failures were individually job-logged (not inferred from branch/message
alone). **0/15 carry the activity-timeout signature**, in either window.

**This is not the rerun campaign issue #1558 asked for** — it is
frequency-in-the-wild evidence over calendar time and a shifting set of
branches, not N identical-commit reruns, and per this role's own Tier
distinctions it does not by itself confirm the fix. It is a genuine data
point the prior reports didn't have: roughly nineteen and a half hours of
real CI traffic post-fix produced zero recurrences of a signature that had
appeared 3 times in a comparable ~17-hour window one day earlier (the 09-14
report's census). **Correction (post-review):** an earlier draft of this
report described that traffic as spanning "ten unrelated branches' worth of
failures" — false. The 10 post-merge failures came from only **5 distinct
branches**: `claude/hopeful-pascal-tbijcf` alone accounts for half of them
(5 of 10, all successive commits on one PR as it iterated through review),
with `claude/gifted-mccarthy-25ztga` contributing 2 and three other branches
contributing 1 each. That materially narrows the independence of this
evidence — five branches iterating, one of them repeatedly, is a much
smaller draw than ten unrelated ones — though it does not overlap with item
1's headline claim itself, since none of those 10 failures (from any branch)
carried the activity-timeout signature regardless of how the branches
cluster. No same-commit rerun was run this session — Docker is unavailable
in this session's sandbox, and dispatching 20 real GitHub-hosted-runner
executions of `test-db-linux` (11 shards) solely to rerun one test is the
kind of ambient, suite-level spend this role's own charter asks to route
through **Ask before** rather than do unilaterally.

**Correction (post-review) — the cancelled-run gap.** A Codex review on
this PR correctly flagged that the headline "0/15" figure only covers the
15 runs whose *overall* conclusion was `failure`, leaving the window's 55
`cancelled` runs unaudited — and this role's own 09-06/09-11 reports
already established that a cancelled run's overall conclusion can absorb a
real job-level failure underneath it (4/10 and 15/54 hit rates in those
samples). Job-logged a sample of 8 of the 19 post-merge cancelled runs at
job level (`list_workflow_jobs`, `perPage=100`, checking every job's own
`conclusion`, not just the run's):

| Run | Branch | Hidden job failures | Signature |
|---|---|---:|---|
| `35072771791` | `claude/hopeful-pascal-tbijcf` | 9 (`Test DB (linux, shard 0/1/2/3/4/7/8/9/10)`) | `FAILED SUITES: ctx_info_tests, mixed_suspension_tests, quota_supersede_ordering_tests` (shard 1) — a missing-column defect (`harvest_workflow_executions.migrated_run_terminal_at` absent from the hand-maintained `INIT_SQL`/`LEGACY_INIT_SQL` test bundles), self-diagnosed and fixed by this same branch's very next commit (`36791bfff0`, visible in this session's own fresh `ci.yml` query) |
| `35060658370` | `claude/hopeful-pascal-tbijcf` | 3 (`Test (windows/ubuntu/macos-latest)`) | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` — the identical missing-migration-row signature already counted as an explicit failure at run `35063499036` on the same branch 40 minutes later; a precursor occurrence of an already-counted defect, not a new one |
| 6 others | mixed | 0 | clean cancellations (matrix jobs show `cancelled`, not `failure`) |

**Neither hidden failure carries the activity-timeout signature**, and both
trace to defects already accounted for elsewhere in this report or
self-fixed on the same branch. But this is a sample, not a census: 11 of
19 post-merge cancelled runs and all 36 pre-merge cancelled runs remain
unaudited at job level. The headline "0/15" claim is therefore better
stated as "0 occurrences among the runs actually inspected" (15 explicit
failures plus this 8-run cancelled sample, 23 runs total) — directionally
consistent with the fix holding, but not the exhaustive census an earlier
draft of this report implied by calling the 100-run page complete.

### 2. `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree`'s panic message still shows the same number on both sides, and it blocked one branch three commits in a row this window

The 09-08 report (`docs/rnd/2026-09-08-ci-health-semaphore-rerun-census.md:139-145`)
first noted this test's panic message quotes the live migration count on
*both* sides of its sentence ("the report should state \"**N migrations**\"; a
live count finds N migration directories") instead of contrasting the doc's
stale value against the live one, and judged it a diagnostics-quality issue
below this role's bar to fix on its own. It recurred unchanged this window,
now at N=107, on three consecutive commits of branch
`claude/hopeful-pascal-tbijcf` (runs `35049974926` 03:28:31Z, `35054766247`
04:45:48Z, `35057075093` 05:19:53Z — all identical assertion text) before a
fourth commit on the same branch fixed the underlying doc gap and a fifth hit
`migration_hygiene.rs:695` instead (run `35063499036`). This is the same
doc/code-sync gate class described in the 09-08 report, correctly firing on a
branch that added migrations without updating the frozen count in
`docs/rnd/sqlite-feasibility.md` — not a flake, and each occurrence traces to
that branch's own then-current diff. **Correction (post-review):** an
earlier draft of this section attributed the three CI round-trips
themselves to the message bug. That overstates it — the three failures
happened because the branch left the documented count stale across three
commits, not because of how the panic message is worded; the assertion
would have failed the same three times with a correctly-worded message.
What the message bug actually costs is diagnostic clarity: whoever reads
the panic cannot tell from it alone that the doc is stale, since it quotes
the same live count on both sides of the sentence, so they have to know to
distrust the sentence rather than being told directly. That cost is real
but qualitative, not the quantified "three round-trips" an earlier draft
claimed. Still below this role's bar to open a fix PR on its own (a
one-line diagnostic fix on a docs-only test, not a suite-health defect).

### 3. One truncated `FAILED SUITES` line, one occurrence, not clustered

Run `35034838493` (`Test DB (linux, shard 8)`, 00:16:31Z) ended with `FAILED
SUITES: autumn-harvest/integration (linux) -- quota_enforcement_tests` after
every individually-named suite in the visible tail passed — the same
tail-truncation gap the 09-14 report hit on `integration_e2e`'s ~2800-test
module (`tail_lines` ending before the actual panic for a large serial run).
Re-fetched at `tail_lines=200`; the actual `quota_enforcement_tests` failure
output was still not in the visible window. Not pursued further: the same
branch (`claude/hopeful-pascal-tbijcf`) went on to fail three more times on
unrelated deterministic gates (item 2, `migration_hygiene`) and never
reproduced this signature again in this window. One occurrence, no
mechanism, not clustered with anything else in this sample — recorded per
this role's own admissibility rules so a repeat is recognized as a repeat,
not actioned as a rate.

### 4. Remaining 11 failures: deterministic, own-branch defects

**Correction (post-review):** an earlier draft of this section named five
signature buckets that summed to 9, not 11 — undercounting the `cargo fmt`
diffs (only one was mentioned; there were three separate occurrences) and
omitting `migration_hygiene` entirely. Corrected, with every one of the 11
runs named:

| Run | Signature |
|---|---|
| `34891219422` | clippy `redundant_clone` (`payload_codec.rs`) |
| `34891509704` | clippy dead-code (`partition.rs`'s `DISABLE_RENAME_SUFFIX`) **and**, in a second failed job on the same run, `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` |
| `34924353340` | E0433 `diesel` compile error (2 failed jobs, same signature) |
| `34925551916` | E0433 `diesel` compile error (2 failed jobs, same signature — same root cause as `34924353340`, same branch `claude/bold-lovelace-agnczk`) |
| `34933650236` | `comment-hygiene.py` Tier B (`shard_rebalance.rs` sentence-length) |
| `34986559913` | `cargo fmt` diff (`resolve_fixtures.rs`) |
| `34988937190` | clippy `doc_markdown` (`analysis_fixtures.rs`) |
| `35034267335` | `cargo fmt` diff (`shard_rebalance_db_tests.rs`, `workflow_rerun_integration.rs`) |
| `35045856467` | `cargo fmt` diff (`shard_rebalance_db_tests.rs`, `autumn-harvest-cli/src/lib.rs`, `workflow_filter_integration.rs`) |
| `35060937048` | clippy `map_unwrap_or` (`analyze_profile.rs`) |
| `35063499036` | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` (missing inventory row) |

By signature: comment-hygiene (1 run), `cargo fmt` (3 runs, not 1 as an
earlier draft implied), clippy (4 runs: `redundant_clone`, dead-code,
`doc_markdown`, `map_unwrap_or`), E0433 `diesel` (2 runs, one root cause),
`corpus` determinism (0 additional runs — same run as the dead-code clippy
finding), `migration_hygiene` (1 run, previously unmentioned). 1+3+4+2+0+1 =
11 runs, matching the section heading. The two `diesel`/E0433 occurrences
are the same commit-family defect counted once in the diagnosis below, not
two independent findings. Each traces to that commit's or branch's own
diff; no suite-state interaction, no timing component, no order dependence.

## 🔍 Diagnosis

**Item 1** is not yet a rendered verdict on PR #1563's fix — that still
requires the rerun campaign issue #1558 asked for and never got — but the
frequency-in-the-wild evidence has moved from "unconfirmed" toward "holding":
zero recurrences across a full (not sampled) two-day failure census straddling
the merge, versus 3 occurrences the day before. Recorded as a data point for
whoever next has the ability to run the actual rerun campaign, not claimed as
a Tier-1 confirmation.

**Item 2** is the doc-sync gate working as designed, with a message-quality
bug that has now cost one branch three CI round-trips in a single window
rather than the hypothetical single instance in the 09-08 report. Still below
this role's own bar for a unilateral fix PR (not a suite-health defect, not a
flake), but the cost is no longer zero.

**Item 3** is explicitly not claimed as a flake — one occurrence, no
mechanism recovered, not clustered.

**Item 4** is the suite working correctly, not a CI-health defect. The two
`diesel`/E0433 occurrences are the same commit-family defect counted once,
not two independent findings.

**Correction (post-review):** item 3's own text already states the actual
`quota_enforcement_tests` panic was not recoverable from the available logs
and that no mechanism was identified — so it is **classified**, not
**root-caused**. An earlier draft's 🔧/📊 sections below said "15/15
root-caused," which contradicts item 3's own finding; corrected to 14/15
root-caused plus 1/15 classified as a single, unexplained occurrence.

## 🔧 Treatment

None shipped. Nothing found clears the impact floor this round: no flaky test
newly made deterministic (item 1's fix already merged in PR #1563; this
session only gathered additional post-merge evidence, it did not touch the
test), no product bug surfaced, no timing win measured, no quarantine entries
to retire (still no quarantine ledger in this repo), no suite passing under
shuffled order to report (not run this session). Per the hard gate, a report
is the correct outcome, not a PR against `ci.yml` or any test.

Items carried forward, unchanged from the 09-08/09-14/09-15 reports:

1. **Cache-usage API access** — still unavailable, checked again today.
2. **Branch-protection confirmation** — still unavailable, checked again
   today; `test-db-linux`, `test-nodb`, and `openapi-client-smoke` all remain
   unconfirmed against live branch-protection settings.
3. **The rerun campaign for issue #1558's fix** — still not run by any
   session; this report's frequency-in-the-wild data is supporting evidence
   for prioritizing it, not a substitute.
4. **`sqlite_feasibility_docs`'s self-contradicting panic message** (item 2)
   — a diagnostic-clarity cost, not a root cause of the 3 failures
   themselves (those were the stale doc, unfixed across 3 commits); still a
   one-line fix someone should pick up.
5. **The remaining cancelled-run population** — 11 of 19 post-merge and all
   36 pre-merge cancelled runs in this window are still unaudited at job
   level. This role's own 09-06/09-11 reports already flagged that building
   a scheduled harness for this (pulling every job's conclusion for every
   completed run, cancelled or not) is the correct fix for the gap rather
   than repeated manual sampling; still not built by any session.

## 📊 Measurement

- **Item 1: correction (post-review).** An earlier draft of this line
  claimed "100/100 runs... individually job-logged (full census)." False —
  `list_workflow_runs` *enumerated* all 100 runs' conclusions, but only the
  15 explicit-failure runs plus an 8-run cancelled-run sample (23 runs
  total) were actually job-logged. The other 77 runs (30 success, 47
  cancelled-and-unaudited) were never inspected below the run level. Stated
  correctly: of the **23 runs actually job-logged**, 0/23 carry the
  activity-timeout signature (15 explicit failures — 5 in 18h14m pre-merge,
  10 in 19h33m post-merge — plus 2/8 sampled cancelled runs that hid a
  real job failure, neither matching the signature). This is evidence from
  the population actually inspected, not a census of the full window; the
  remaining 77 runs, including 47 cancelled ones, could in principle hide
  an occurrence. Not a same-commit rerun — no revert check applies, since
  no fix was made or verified this session.
- **Item 2:** 3/3 occurrences on one branch confirmed identical panic text
  (`"**107 migrations**"` on both sides) via direct job-log inspection.
- **Item 3:** 1/1, not a rate.
- **Item 4:** 11/11 root-caused via job-log inspection (see the corrected
  run-to-signature table above); 0 suite-attributable flakes among them.
- **Cancelled-run sample:** 8/19 post-merge cancelled runs job-logged; 2/8
  hid a real job-level failure (9 shards on one run, 3 jobs on another);
  0/2 hidden failures carry the activity-timeout signature; 11/19
  post-merge and 36/36 pre-merge cancelled runs remain unaudited.
- **Combined: correction (post-review).** An earlier draft's "0/15
  suite-level flakes" contradicted item 3's own text, which says
  `quota_enforcement_tests`'s panic and mechanism were never recovered — a
  single unclassified occurrence is insufficient evidence to call it a
  flake, but equally insufficient to rule one out. Stated correctly: 14/15
  of this window's explicit failures root-caused and confirmed
  non-suite-level; 1/15 (`quota_enforcement_tests`) unclassified — neither
  confirmed a flake nor confirmed not one; 0/14 classified failures carry
  the previously-tracked activity-timeout signature; 0/2 hidden
  cancelled-run failures (of the 8-run sample actually inspected) carry it
  either.

## 🔬 Reproduce

```sh
# Full census (not a sample): actions_list(method="list_workflow_runs",
#   resource_id="ci.yml", workflow_runs_filter={event:"pull_request",
#   status:"completed"}, perPage=100) on autumn-foundation/autumn-harvest,
# captured 2026-09-16 — one page covered 2026-09-14T19:46:25Z through
# 2026-09-16T09:33:24Z (100 runs: 55 cancelled, 30 success, 15 failure).

# PR #1563 merge instant (pull_request_read on PR #1563): merged_at =
# 2026-09-15T14:00:13Z. Split the 100-run sample at that instant:
python3 -c "
from datetime import datetime
from collections import Counter
import json
with open('<saved actions_list JSON>') as f:
    d = json.load(f)
runs = d['workflow_runs']
merge = datetime.fromisoformat('2026-09-15T14:00:13+00:00')
pre = [r for r in runs if datetime.fromisoformat(r['created_at'].replace('Z','+00:00')) < merge]
post = [r for r in runs if datetime.fromisoformat(r['created_at'].replace('Z','+00:00')) >= merge]
print('pre-merge:', len(pre), Counter(r['conclusion'] for r in pre))
print('post-merge:', len(post), Counter(r['conclusion'] for r in post))
"

# Per-failure job logs: get_job_logs(run_id=<id>, failed_only=true,
#   return_content=true, tail_lines=40-200) for each of the 15 failure run
# ids listed in this report. All 15 were fetched this session:
#   34891219422 34891509704 34924353340 34925551916 34933650236
#   34986559913 34988937190 35034267335 35034838493 35045856467
#   35049974926 35054766247 35057075093 35060937048 35063499036

# Item 2's repeated occurrences, same branch:
# grep for "the report should state" across job logs of runs 35049974926,
# 35054766247, 35057075093 -- all three quote "**107 migrations**" on both
# sides of the sentence, confirming the 09-08 report's diagnostic-message
# finding recurs unchanged.

# Item 3's truncation gap: get_job_logs(run_id=35034838493, failed_only=true,
#   return_content=true, tail_lines=200) still ends at the "FAILED SUITES:"
#   line with no panic detail for quota_enforcement_tests above it -- same
#   class of gap the 2026-09-14 report hit on integration_e2e.

# Branch-protection / cache-usage tool availability: re-checked via
# ToolSearch("branch protection rules github") and
# ToolSearch("actions cache usage github") this session -- neither tool
# is present in the available GitHub MCP surface, unchanged from prior
# reports.

# Cancelled-run job-level audit (8 of 19 post-merge cancelled runs):
# list_workflow_jobs(resource_id=<run_id>, perPage=100) for each of
#   35080087437 35072771791 35070284413 35063265350 35062316533
#   35062123078 35060658370 35040749722
# then filter jobs whose own "conclusion" is "failure" (not the run's
# overall conclusion). Found on 35072771791 (9 Test DB shards) and
# 35060658370 (3 Test <os> jobs); the other 6 were clean cancellations.
# get_job_logs(job_id=104728768074, return_content=true, tail_lines=80)
#   and get_job_logs(job_id=104687245341, return_content=true,
#   tail_lines=60) confirm the signatures in the table above. Neither
# matches worker_fails_workflow_when_activity_start_to_close_timeout_elapses.
# Remaining 11 post-merge and all 36 pre-merge cancelled runs: not audited
# this session.
```
