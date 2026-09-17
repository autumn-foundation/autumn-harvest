# 🚦 Semaphore CI health — the census tool itself is non-deterministic:
# four identical `list_workflow_runs` calls returned four different
# 100-run samples spanning different multi-day windows, while the
# unfiltered list and single-run lookups stayed stable; today's window
# (09-16T09:33Z→09-17T07:39Z, gathered via the reliable method) shows
# zero recurrence of the tracked activity-timeout, quota-enforcement, or
# `corpus` signatures, plus a fully root-caused `benchmarks_docs` defect
# that left trunk-dev red against its own gate for 4h13m

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series from `docs/rnd/2026-09-16-ci-health-semaphore-activity-timeout-flake-holding.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally the `test-db-linux` and `test`/`test-nodb` matrices.
Branch-protection status for these matrices and `openapi-client-smoke` remains
unconfirmed — no branch-protection-read tool is exposed in this session, same
gap every prior report in this series has logged. Cache-usage API access is
also still unavailable. Item **g** below adds a concrete data point to the
branch-protection gap, not a resolution of it.

## 🌡️ Symptom

### 0. The measurement instrument itself is non-deterministic — this is today's headline finding, and it bears on every prior report in this series

This role's own charter is explicit that Tier-1 evidence must be reproducible:
"anyone can rerun it." The tool this entire series has used for every census —
`actions_list(method="list_workflow_runs", ..., workflow_runs_filter={event:
"pull_request", status:"completed"})` — fails that bar, verified directly this
session, not inferred.

Four back-to-back calls with **identical parameters** (same `owner`, `repo`,
`resource_id="ci.yml"`, `perPage=100`, `page=1`, same filter object) returned
four different result sets:

| Call | `total_count` | Newest run in page | Oldest run in page |
|---|---:|---|---|
| 1 | 1973 | `34316264290` (run 4575, 2026-09-09T05:46:48Z) | run 4447, 2026-09-06T20:43:13Z |
| 2 | 3911 | `34623184036` (run 4707, 2026-09-11T16:38:44Z) | run 4451, 2026-09-06T20:50:46Z |
| 3 | 4854 | `35195626323` (run 5650, 2026-09-17T07:39:28Z) | run 5535, 2026-09-16T10:53:08Z |
| 4 | 2301 | `34316264290` (run 4575, 2026-09-09T05:46:48Z) | run 4447, 2026-09-06T20:43:13Z |

Only call 3 lands anywhere near "now" (this session's wall clock is
2026-09-17). Calls 1, 2 and 4 return **stale windows over a week old**, and
call 1 and call 4 are near-identical to each other despite being separated by
two other calls that returned completely different data — this is not
monotonic drift or eventual pagination catch-up, it looks like the tool is
drawing from a re-randomized or re-cached population each call.

Isolated the cause by varying which filter keys were supplied, one call each:

| Filter | `total_count` | Newest run | Matches ground truth? |
|---|---:|---|---|
| none | 5650 | run 5650, 2026-09-17T07:39:28Z | yes |
| `{status:"completed"}` only | 5645 | run 5650, 2026-09-17T07:39:28Z | yes |
| `{event:"pull_request"}` only | 4783 | run 5631, 2026-09-16T20:07:37Z | stale but plausible (~11h old) |
| `{event, status}` together | 1973–4854, varying per call | varying, up to 8 days stale | **no — non-deterministic** |

Ground truth confirmed independently: `get_workflow_run(resource_id=
35195626323)`, called twice with identical arguments, returned identical,
stable data both times (run 5650, `conclusion: "success"`, `created_at:
2026-09-17T07:39:28Z`). Single-run lookups and the unfiltered list are
reliable; only the **combined** `event`+`status` filter on
`list_workflow_runs` is not.

**This matters retroactively.** Every prior report in this series —
09-03 through 09-16 — built its census with exactly this combined filter and
described the result as "the N most recent completed pull_request runs." Per
today's finding, that call does not reliably return the most recent runs at
all; it can return an arbitrary, possibly week-old slice. This does not mean
those reports' *internal* arithmetic was wrong — each report fetched one
JSON snapshot and reasoned about it consistently — but the claim that the
snapshot represented "the current census window" is unconfirmed for all of
them, and demonstrably false for 3 of the 4 calls made this session alone.

**Workaround verified and used for the rest of this report.** Paginating the
*unfiltered* `list_workflow_runs` (3 pages, `perPage=100`, no
`workflow_runs_filter`) and filtering for `event=="pull_request" and
status=="completed"` client-side in Python produced a set that: (a) has
monotonically contiguous run numbers across all 3 pages with zero gaps or
duplicates (5351–5650, 300 runs, `total_count: 5650` identical on every
page), and (b) reconstructs the exact same 15 explicit-failure runs the
09-16 report already found and named for the shared part of the window —
cross-validated, not just internally consistent. Recommended for every
future session in this series until the underlying tool defect is fixed:
**page the unfiltered list and filter client-side; never trust the combined
`event`+`status` filter's result as "the current window."**

This is not this repository's bug to fix — the defect is in the GitHub MCP
server's `list_workflow_runs` tool, outside `autumn-harvest`'s own `ci.yml`
or test suite, so no PR against this repo's harness applies. Recorded here
per this role's own "Ask before... adding CI plugins/dependencies" and
because every future census in this series depends on knowing it.

### 1. Activity-timeout flake (`worker_fails_workflow_when_activity_start_to_close_timeout_elapses`, issue #1558/PR #1563): zero new occurrences in today's window

Using the reliable method, built the census for the window since the 09-16
report's cutoff (2026-09-16T09:33:24Z) through this session's wall clock
(2026-09-17T07:39:28Z): **109 `pull_request`+`completed` runs — 86 cancelled,
13 success, 10 failure.**

Job-logged all 10 explicit failures (`get_job_logs`, `failed_only=true`).
None touched `test-db-linux` (shard 8, where `integration_e2e.rs` and this
test live) at all — every one of the 10 failures this window was a `Lint`,
`Test (<os>)`, or `Test (no-db, <os>, shard N)` job. Zero opportunities for
the tracked signature to appear in this window's explicit failures, and
issue #1558 remains closed (`closed_at: 2026-09-15T14:00:25Z`, no reopening,
confirmed via `issue_read` this session). This is consistent with — not
proof of — the fix holding; the ≥20x same-commit rerun campaign issue #1558
originally asked for has still never been run by any session in this series.

### 2. `quota_enforcement_tests`'s unexplained 10-second target-row timeout (09-16 report item 3): not recurred

Zero occurrences among today's 10 job-logged failures or the 6 job-logged
cancelled-run samples (below). Still 1/1 total across the series — not a
rate, not clustered.

### 3. `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` (open candidate, 3 prior occurrences per the 09-15 report): not recurred

Zero occurrences among today's 10 job-logged failures. Still short of this
role's own ≥20-rerun bar for a measured rate; no session has yet run the
rerun protocol this would need.

### 4. `sqlite_feasibility_docs`'s self-contradicting panic message (09-08 report, diagnostic-quality issue): recurred once more, count now 108

Run `35122304096` (2026-09-16T16:30:11Z, `claude/pensive-brahmagupta-2tsiz3`)
panicked with `"the report should state \"**108 migrations**\"; a live
count finds 108 migration directories"` — the same live-count-quoted-on-both-
sides defect first logged in the 09-08 report, now recurring at N=108 (up
from N=107 in the 09-16 report). One occurrence this window, on an unrelated
branch (a `split_top` byte-check fix), correctly gated on that branch's own
stale doc count — not a flake. Still below this role's bar for a unilateral
fix PR (diagnostic clarity, not a suite-health defect).

### 5. New: `benchmarks_docs::the_doc_names_no_competitor_engine` — 3 occurrences, fully root-caused to a single ~4h13m window where trunk-dev itself failed its own committed gate

| Run | When (UTC) | Branch | Job(s) |
|---|---|---|---|
| `35129116518` | 17:35:18Z | `claude/fervent-einstein-92qrxx` ("Assays #10 and #11... harvest vs Temporal on one box") | `Test (ubuntu/windows/macos-latest)`, `Test (no-db, *, shard 0/3)` |
| `35145049656` | 20:11:30Z | `claude/vigilant-hopper-moyhnr` ("Add batched seek-and-refine claim path") | same 7-job pattern |
| `35152977594` | 21:32:46Z | `claude/magical-gauss-gn4qd2` ("Ledger: batch the outbox start relay") | same 6-job pattern |

All three carry the identical panic: `docs/benchmarks.md names a competitor
engine (temporal); issue #1309 asks this page to explain the comparison
methodology, never quote a competitor's own figure`
(`benchmarks_docs.rs:231`).

Root cause, confirmed directly against source rather than inferred from the
panic text alone:

```
git show 266ac9c:docs/benchmarks.md | grep -i temporal   # 4 literal hits
git diff 266ac9c f01a448 -- docs/benchmarks.md            # shows their removal
git log -1 --format=%cI 266ac9c   # 2026-09-16T12:35:57-05:00 = 17:35:57Z
git log -1 --format=%cI f01a448   # 2026-09-16T16:49:14-05:00 = 21:49:14Z
```

Commit `266ac9c` ("🔬 Assays #10 and #11: cross-mode throughput, and harvest
vs Temporal on one box", merged 17:35:57Z) put literal "Temporal" text
directly into `docs/benchmarks.md`'s prose — not just its own commit
message. Commit `f01a448` ("📐 Correct the assay #10 and #11 numbers...",
merged 21:49:14Z, same day) removed it. **`trunk-dev` itself failed its own
committed `the_doc_names_no_competitor_engine` gate for 4h13m** between
those two merges. My local checkout (on `f01a448` and later) has zero
"temporal" matches in `docs/benchmarks.md` today — the doc is currently
clean.

Two of the three occurrences (`35145049656`, `35152977594`) are on branches
with nothing to do with the assay work — `vigilant-hopper-moyhnr`'s
claim-path change and `magical-gauss-gn4qd2`'s outbox-batching change. Both
inherited a real, deterministic, correctly-firing red purely from being
based on `trunk-dev` during the broken window — the same "stale-branch
echo of a defect in the base, not in the branch" pattern the 09-14 report
first documented for the activity-timeout test's pre-hardening pair, here
running the other direction (the defect was introduced on the base, not
fixed there yet).

**A concrete data point for the still-unconfirmed branch-protection gap.**
Run `35129116518` was created at 17:35:18Z — **39 seconds before** `266ac9c`
merged at 17:35:57Z — and it already shows this exact failure on `Test
(ubuntu-latest)` and 5 other legs. If that run was PR #1617's own
pre-merge CI (plausible: same branch, same defect, right before merge), the
PR merged into `trunk-dev` with this test failing on its own head commit or
one immediately prior. This session cannot confirm branch-protection
settings (the recurring, still-open gap), so it cannot say whether that
check was required and bypassed, not required at all, or the merge used a
different/later commit whose own CI passed — but it is exactly the kind of
concrete instance this series has been missing to make the branch-protection
gap actionable rather than abstract. Recorded here, not treated as
confirmed policy failure.

### 6. Cancelled-run sample this window: 6/7 job-logged, 0 hid a failure; 1/7 not inspected

Job-logged 7 of this window's 86 cancelled runs, spread across the window
(`list_workflow_jobs`, `perPage=100`, checking every job's own `conclusion`):
`35081223941`, `35087852092`, `35105355850`, `35117694170`, `35127884735`,
`35136021958`, `35138666154`, `35141974598` — 8 attempted, 6 fully read
(`35081223941`'s job list exceeded this session's inline tool-output limit
and was not re-fetched via the signed-URL fallback this series has used
before; not inspected). All 6 read runs show ordinary
`concurrency.cancel-in-progress` behavior: every job cancelled at the same
instant mid-step, consistent with a newer push superseding an
in-progress run — including one run (`35105355850`) whose `Lint` job shows
a genuine `cargo fmt` **failure** before the rest of the run was cancelled
by a later push, which is a real, correctly-firing gate on that branch's
own diff, not a hidden CI-health defect. 0/6 sampled cancelled runs hid an
unrelated job-level failure this window, against 2/8 in the 09-16 report's
sample — too small a sample on both sides to compare rates, and the
remaining 79 of 86 cancelled runs in this window are unaudited.

## 🔍 Diagnosis

**Item 0** is a tooling defect external to this repository: the GitHub MCP
server's `list_workflow_runs`, when given both `event` and `status` filter
keys together, returns a non-deterministic result on identical repeated
calls. Root cause not diagnosable from this side (client-observable
behavior only — no visibility into the server implementation). Neither a
test-vs-product verdict nor a flake-vs-product verdict applies; it is
neither this repo's test suite nor its product code. It is, however, the
instrument every report in this series has relied on for its Tier-1
"reproducible protocol" claim, so it is diagnosed and routed here rather
than silently worked around.

**Items 1–3** show continued absence of recurrence for three previously
open items, over a reliably-constructed ~22-hour window. None of the three
clears this role's own bar for "confirmed holding" (no rerun campaign has
ever been run for any of them), but none regressed either.

**Item 4** is the same known diagnostic-message defect recurring on
schedule, as expected — not a new finding, not actioned for the same
reason as every prior report.

**Item 5** is a fully root-caused, deterministic, single-commit-window
defect — not a suite flake. `trunk-dev` was briefly red against its own
gate; a same-day follow-up fixed it. The two downstream "stale-branch echo"
occurrences are not independent findings, they are the same root cause
propagating to any branch built on the broken window. Filed here as a
health-report item, not a fix PR, because the defect is already fixed on
`trunk-dev` (confirmed by direct inspection of the current file) — there is
nothing left to fix. The branch-protection data point is exactly that: a
data point, not a confirmed policy conclusion.

**Item 6** is ordinary CI mechanics for the 6 runs actually inspected; too
small and too narrow a sample to say anything about the other 79 cancelled
runs this window.

## 🔧 Treatment

None shipped against `ci.yml` or any test. Per the hard gate: no flake was
located, diagnosed, and verified fixed this session (items 1–3 show absence
of new occurrences, not a completed rerun campaign); item 4 is a known,
unactioned diagnostic-quality issue; item 5 is already fixed upstream by
its own follow-up commit; item 0 is not this repository's code to patch. A
health report is the correct outcome.

**The one recommendation this report does act on**, because it costs
nothing and changes how every future session in this series gathers
evidence: **stop using `list_workflow_runs`'s combined `{event, status}`
filter for census work.** Page the unfiltered list (or the `status`-only
filter, which stayed reliable in this session's testing) and filter
client-side. This is a change in *how this role gathers evidence*, not a
change to the repository's CI configuration, so it needs no PR — but it
should be treated as binding methodology for the next session that opens
one of these reports.

Items carried forward, unchanged:

1. **Cache-usage API access** — still unavailable, checked again today via
   `ToolSearch`.
2. **Branch-protection confirmation** — still unavailable; item 5 above adds
   one concrete, timestamp-based data point (a run that may have been PR
   #1617's own pre-merge CI, failing 39s before merge) without resolving the
   gap.
3. **The rerun campaign for issue #1558's fix** — still not run by any
   session in this series.
4. **`sqlite_feasibility_docs`'s self-contradicting panic message** — still
   a one-line fix someone should pick up; recurred again this window at
   N=108.
5. **The remaining cancelled-run population** — 79 of this window's 86
   cancelled runs, plus the entire backlog from every prior report's window,
   remain unaudited at job level. A scheduled harness for this (this
   series' own repeated recommendation since 09-06/09-11) is still not
   built by any session.
6. **`corpus::seeded_corpus_is_clean_under_the_syntactic_layer`** — still
   undiagnosed, still short of a measured rate (3 occurrences total, none
   new this window).
7. **New: the `list_workflow_runs` combined-filter non-determinism (item 0)**
   — not this repo's to fix, but every future session in this series should
   read this report before trusting that tool call's result as a "current
   window" census.

## 📊 Measurement

- **Item 0:** 4/4 identical calls to the combined filter returned 4 distinct
  `total_count` values (1973, 3911, 4854, 2301) and 4 distinct date windows,
  one of which repeated near-identically after two intervening different
  results. 2/2 identical calls to `get_workflow_run` for the same run ID
  returned identical data. 1/1 unfiltered paginated fetch (3 pages) was
  internally consistent (`total_count: 5650` on every page, contiguous run
  numbers 5351–5650, zero gaps/duplicates) and reconstructed the exact same
  15 failures the 09-16 report already named for the overlapping portion of
  the window — cross-validated against a prior, independently-gathered
  report, not just self-consistent.
- **Items 1–3:** 0 new occurrences each, over a 109-run / ~22-hour window
  built via the verified-reliable method. Not rerun-campaign confirmations.
- **Item 4:** 1/1 occurrence this window, consistent with the known,
  unfixed defect.
- **Item 5:** 3/3 occurrences confirmed to share one root cause via direct
  commit inspection (`git show`, `git diff`, `git log --format=%cI`), not
  inferred from timing alone. 0/3 required a rerun — the fix already
  shipped upstream 4h13m after the defect was introduced; confirmed absent
  from the current `docs/benchmarks.md` by direct grep.
- **Item 6:** 6/7 attempted cancelled-run job-logs read; 0/6 hid a failure
  beyond one real, correctly-firing `cargo fmt` red already superseded by a
  later push on the same branch.
- No revert check applies — no test or suite code was changed this session.

## 🔬 Reproduce

```sh
# Item 0 — the non-determinism, reproduced by calling identically 4 times:
# actions_list(method="list_workflow_runs", owner="autumn-foundation",
#   repo="autumn-harvest", resource_id="ci.yml", perPage=100, page=1,
#   workflow_runs_filter={event:"pull_request", status:"completed"})
# Compare total_count and the newest/oldest run in each response — expect
# them to differ across calls with byte-identical parameters.

# Isolating the cause (single calls, no page param needed):
# actions_list(..., workflow_runs_filter={event:"pull_request"})   # stale but stable-looking
# actions_list(..., workflow_runs_filter={status:"completed"})     # matches ground truth
# actions_list(..., resource_id="ci.yml")                          # no filter — matches ground truth
# actions_get(method="get_workflow_run", resource_id=35195626323)  # called 2x, identical both times

# The reliable substitute used for this report's census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   perPage=100, page=1)   # then page=2, page=3 — no workflow_runs_filter
python3 -c "
import json
pages = ['page1.json','page2.json','page3.json']  # saved tool-result files
allruns = {}
for p in pages:
    with open(p) as f:
        d = json.load(f)
    for r in d['workflow_runs']:
        allruns[r['id']] = r
runs = list(allruns.values())
pr_completed = [r for r in runs if r['event']=='pull_request' and r['status']=='completed']
cutoff = '2026-09-16T09:33:24Z'   # the 09-16 report's own cutoff
new = [r for r in pr_completed if r['created_at'] > cutoff]
from collections import Counter
print(len(new), Counter(r['conclusion'] for r in new))
"
# -> 109 runs: {'cancelled': 86, 'success': 13, 'failure': 10}

# Item 5 — the benchmarks_docs root cause:
git show 266ac9c:docs/benchmarks.md | grep -ni "temporal\|dbos\|cadence\|zeebe\|conductor\|restate"
git diff 266ac9c f01a448 -- docs/benchmarks.md | grep -i temporal
git log -1 --format=%cI 266ac9c    # 2026-09-16T12:35:57-05:00
git log -1 --format=%cI f01a448    # 2026-09-16T16:49:14-05:00
grep -ni temporal docs/benchmarks.md   # current tree: no matches

# Per-failure job logs, this window's 10 explicit failures:
# get_job_logs(run_id=<id>, failed_only=true, return_content=true, tail_lines=50-60)
#   35096353603 35100756217 35117297810 35122304096 35129116518
#   35132957731 35140963232 35145049656 35145054552 35152977594

# Cancelled-run sample, this window:
# actions_list(method="list_workflow_jobs", resource_id=<run_id>, perPage=100)
#   35081223941 35087852092 35105355850 35117694170 35127884735
#   35136021958 35138666154 35141974598
# then filter for any job whose own "conclusion" is "failure" rather than
# "cancelled"/"skipped"/"success".

# Issue #1558 status check:
# issue_read(owner="autumn-foundation", repo="autumn-harvest", issue_number=1558)
# -> state: closed, closed_at: 2026-09-15T14:00:25Z, no reopening.
```
