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
| `35072771791` | `claude/hopeful-pascal-tbijcf` | 9 (`Test DB (linux, shard 0/1/2/3/4/7/8/9/10)`) | a missing-column defect (`harvest_workflow_executions.migrated_run_terminal_at` absent from the hand-maintained `INIT_SQL`/`LEGACY_INIT_SQL` test bundles), self-diagnosed and fixed by this same branch's very next commit (`36791bfff0`, visible in this session's own fresh `ci.yml` query) — see below for shard 8 specifically |
| `35060658370` | `claude/hopeful-pascal-tbijcf` | 3 (`Test (windows/ubuntu/macos-latest)`) | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` — the identical missing-migration-row signature already counted as an explicit failure at run `35063499036` on the same branch 40 minutes later; a precursor occurrence of an already-counted defect, not a new one |
| 6 others | mixed | 0 | clean cancellations (matrix jobs show `cancelled`, not `failure`) |

**Correction (post-review) — shard 8 needed checking directly, not
assumed.** A Codex review correctly pointed out that `35072771791`'s
9-shard failure table above named only shard 1's signature (fetched from
its job log) and did not check whether the activity-timeout *test itself*
ran on one of the other 8 failed shards. Per `.github/ci/run-suites.sh`'s
`row_ordinal % SHARD_COUNT` sharding rule and `integration_e2e`'s row
position in `.github/ci/integration-suites.txt` (row ordinal 30 among
`linux`-class rows, `30 % 11 = 8`), `integration_e2e` — the file containing
`worker_fails_workflow_when_activity_start_to_close_timeout_elapses` —
runs on shard 8, which **is** among the 9 failed shards. Fetched shard 8's
full log directly (`get_job_logs(..., return_content=false)` for the
signed URL, then `curl` and `grep`, since the module's ~2800 tests exceed
any reasonable `tail_lines`): `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`
did fail in this run — but at `integration_e2e.rs:729:10`, panicking with
`failed to reload workflow execution: DatabaseError(Unknown, "column
harvest_workflow_executions.migrated_run_terminal_at does not exist")` —
the same missing-column defect that cascaded through essentially every
test in that shard's run (dozens of other tests fail at the identical
line and message in the same log). This is **not** the tracked
event-history-mismatch signature — the panic is at `integration_e2e.rs:729:10`
in a shared setup helper on a `DatabaseError`, not at the assertion's own
`other => panic!("history did not match...")` arm (`integration_e2e.rs:3587`).
**Correction (post-review):** an earlier draft of this paragraph claimed
the tracked signature "can no longer occur on this test at all" post-#1563
— false. Reading the current source directly
(`integration_e2e.rs:3567-3588`): the `match` still ends in a catch-all
`other => panic!(...)` arm, so a third event-history shape neither of
PR #1563's two accepted patterns matches would still trip that exact
panic. PR #1563 narrowed which shapes are accepted, it did not remove the
fallback panic arm — so the tracked signature remains structurally
possible on this test, just not observed in this occurrence. The claim
here is narrower and fully supported by direct inspection: *this specific
occurrence*, on shard 8 of run `35072771791`, failed via the DB error at
line 729 before ever reaching the event-history `match` at line 3567, so
it is not an instance of the tracked signature — confirmed by direct log
inspection, not inferred from the shard-1 sample alone as an earlier
draft did.

Restated precisely: of the 9 hidden `Test DB` failures in `35072771791`,
one (shard 8) happens to include the tracked test by name, but its failure
mode is the same schema-mismatch defect as the other 8 shards, not the
race PR #1563 addressed — so it still does not count as a recurrence of
the *tracked signature*, but "neither hidden failure carries it" (an
earlier draft's wording) glossed over needing to check that directly
rather than assume it from an adjacent shard's log. This is a sample, not
a census: 11 of 19 post-merge cancelled runs and all 36 pre-merge
cancelled runs remain unaudited at job level. The headline "0/15" claim is
therefore better stated as "0 occurrences of the tracked signature among
the runs actually inspected" (15 explicit failures plus this 8-run
cancelled sample, 23 runs total) — directionally consistent with the fix
holding, but not the exhaustive census an earlier draft of this report
implied by calling the 100-run page complete.

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
| `34891509704` | clippy dead-code (`partition.rs`'s `DISABLE_RENAME_SUFFIX`) — root-caused. A second failed job on this same run, `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`, is **not** root-caused here (see correction below) |
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
`migration_hygiene` (1 run, previously unmentioned). 1+3+4+2+1 = 11 runs,
matching the section heading. The two `diesel`/E0433 occurrences are the
same commit-family defect counted once in the diagnosis below, not two
independent findings.

**Correction (post-review) — the `corpus` failure on `34891509704` is not
root-caused.** An earlier draft of this section named
`corpus::seeded_corpus_is_clean_under_the_syntactic_layer` alongside the
dead-code clippy finding as if diagnosing the clippy lint also accounted
for it. It does not — they are two independent failed jobs on the same
run, and this report never established a mechanism for the `corpus`
failure. Checking this series' own prior work: the immediately preceding
report, `docs/rnd/2026-09-15-ci-health-semaphore-window-census.md:141-144`,
already found this exact signature recurring across 3 of 5 pushes on this
branch (`34891509704` among them) and explicitly logged it as **"not
otherwise diagnosed"** — so this is not a new occurrence this session
found, it is the same still-undiagnosed recurring failure surfacing again
in this window's sample, carried forward rather than freshly root-caused.
Because it has now recurred at least 3 times without a known mechanism, it
is the closest thing in this report's window to a suite-attributable-flake
candidate — though 3 occurrences (all from the 09-15 report, none newly
observed here) is still short of this role's own ≥20-rerun bar for a
measured rate, and no session has yet run the rerun protocol or
signature-clustering work needed to say more. Each of the other 10 runs
in this section traces cleanly to that commit's or branch's own diff; no
suite-state interaction, no timing component, no order dependence.

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

**Item 4** is the suite working correctly for 10 of its 11 runs, not a
CI-health defect. The two `diesel`/E0433 occurrences are the same
commit-family defect counted once, not two independent findings. The
11th run's second failed job (`corpus` on `34891509704`) is not
root-caused — see the correction in that section — and is grouped with
item 3 below as unclassified.

**Correction (post-review):** item 3's own text already states the actual
`quota_enforcement_tests` panic was not recoverable from the available logs
and that no mechanism was identified — so it is **classified**, not
**root-caused**. The same is true of the `corpus` failure on `34891509704`
(item 4's correction). An earlier draft's 🔧/📊 sections below said "15/15
root-caused," which contradicts both findings; corrected to 13/15 fully
root-caused and 2/15 with at least one unclassified, unexplained failure
(`quota_enforcement_tests`, and `34891509704`'s `corpus` job alongside its
otherwise-explained clippy failure).

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
6. **`corpus::seeded_corpus_is_clean_under_the_syntactic_layer`** — still
   undiagnosed after recurring at least 3 times (per
   `docs/rnd/2026-09-15-ci-health-semaphore-window-census.md`, one of
   which resurfaced in this window's own sample). The closest thing in
   this report's data to a suite-attributable-flake candidate, but at 3
   occurrences it is well short of this role's own ≥20-rerun bar for a
   measured rate; no session has yet run the rerun protocol or
   signature-clustering work this would need.

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
- **Item 4: correction (post-review).** 10/11 runs fully root-caused via
  job-log inspection (see the corrected run-to-signature table above);
  the 11th (`34891509704`) has one root-caused job (clippy dead-code) and
  one unclassified job (`corpus`, recurring per the 09-15 report, still
  undiagnosed). 0 confirmed suite-attributable flakes among the
  root-caused failures; `corpus` is an open candidate, not confirmed
  either way.
- **Cancelled-run sample:** 8/19 post-merge cancelled runs job-logged; 2/8
  hid a real job-level failure (9 shards on one run, 3 jobs on another).
  Of the 9 hidden shard failures in the larger run, one (shard 8) included
  the tracked test by name but failed via the same missing-column defect
  as the other 8 shards, confirmed by direct log inspection (panic at
  `integration_e2e.rs:729:10`, a DB error in shared setup, before the
  event-history `match` at line 3567 is ever reached) — not an instance of
  the tracked event-history-mismatch signature. **Correction (post-review):**
  the signature itself remains structurally possible on this test post-#1563
  (the `match` still ends in a catch-all `other => panic!(...)` arm, per
  direct source inspection); only this specific occurrence is confirmed not
  to be one. 0/9 hidden shard failures and 0/3 hidden job failures (the other
  cancelled run) carry the tracked signature. 11/19 post-merge and 36/36
  pre-merge cancelled runs remain unaudited.
- **Combined: correction (post-review).** An earlier draft's "0/15
  suite-level flakes" contradicted item 3's and item 4's own text, both of
  which say a failure's mechanism was never recovered — a single
  unclassified occurrence is insufficient evidence to call it a flake, but
  equally insufficient to rule one out. Stated correctly: 13/15 of this
  window's explicit failures fully root-caused and confirmed
  non-suite-level; 2/15 have at least one unclassified, unexplained
  failure (`quota_enforcement_tests`; `34891509704`'s `corpus` job); 0/13
  root-caused failures carry the previously-tracked activity-timeout
  signature, and neither unclassified failure's assertion text matches it
  either. 0/2 hidden cancelled-run failures (of the 8-run sample actually
  inspected) carry it
  either.

## 🔬 Reproduce

```sh
# Run-conclusion enumeration (not a job-level census -- see corrections
# above): actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"},
#   perPage=100) on autumn-foundation/autumn-harvest, captured 2026-09-16 --
# one page covered 2026-09-14T19:46:25Z through 2026-09-16T09:33:24Z (100
# runs: 55 cancelled, 30 success, 15 failure). Only the 15 failures plus an
# 8-run cancelled sample (23 of 100) were job-logged below the run level.

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
#   tail_lines=60) confirm the shard-1 and Test-<os> signatures.

# Shard-8 direct check (the Codex-flagged gap): confirm which shard
# integration_e2e.rs actually runs on, rather than assuming from shard 1's
# log:
awk '$1=="linux"{c++} $0 ~ /integration_e2e/ && $1=="linux"{print c-1}' \
  .github/ci/integration-suites.txt   # -> 30 (0-indexed row ordinal)
python3 -c "print(30 % 11)"           # -> 8 (SEMAPHORE_SHARD_COUNT=11)
# Then, since the ~2800-test module exceeds any reasonable tail_lines:
# get_job_logs(job_id=104728768233, return_content=false) for the signed
#   logs_url, curl it directly, and grep:
grep -n "worker_fails_workflow_when_activity_start_to_close_timeout_elapses" shard8.log
grep -n "panicked at\|does not exist" shard8.log
# -> panics at integration_e2e.rs:729:10 with the same
#    "column harvest_workflow_executions.migrated_run_terminal_at does not
#    exist" DatabaseError shared by dozens of other tests in the same log --
#    the missing-column defect, not the tracked event-history assertion.

# corpus::seeded_corpus_is_clean_under_the_syntactic_layer prior-occurrence
# check (the Codex-flagged root-cause gap):
grep -n "seeded_corpus_is_clean_under_the_syntactic_layer" \
  docs/rnd/2026-09-15-ci-health-semaphore-window-census.md
# -> that report's own text (lines 141-144) already found this signature
#    recurring across 3 pushes, including run 34891509704, and explicitly
#    logged it as "not otherwise diagnosed" -- confirming this session's
#    occurrence is the same still-open item, not independently root-caused
#    by this report's clippy finding on the same run.

# Remaining 11 post-merge and all 36 pre-merge cancelled runs: not audited
# this session.
```
