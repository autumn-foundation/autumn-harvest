# 🚦 Semaphore CI health — the census tool itself is non-deterministic:
# four identical `list_workflow_runs` calls returned four different
# `total_count`s across 3 distinct date windows, while a repeated
# single-run lookup stayed stable and the unfiltered list (untested for
# call-to-call repetition, but internally consistent and content-
# cross-validated) is this report's best provisional substitute; today's
# audited population — 10 explicit failures and a 7-run
# cancelled-run sample, not the full 109-run window — shows no recurrence
# of the tracked activity-timeout, quota-enforcement, or `corpus`
# signatures, with 15 confirmed passing shard-8 executions as positive
# exposure evidence for the activity-timeout fix, plus a fully
# root-caused `benchmarks_docs` defect that left trunk-dev red against
# its own gate for 4h13m

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series from `docs/rnd/2026-09-16-ci-health-semaphore-activity-timeout-flake-holding.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally the `test-db-linux` and `test`/`test-nodb` matrices.
Branch-protection status for these matrices and `openapi-client-smoke` remains
unconfirmed — no branch-protection-read tool is exposed in this session, same
gap every prior report in this series has logged. Cache-usage API access is
also still unavailable. Item **5** below adds a concrete data point to the
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
four different `total_count`s:

| Call | `total_count` | Newest run in page | Oldest run in page |
|---|---:|---|---|
| 1 | 1973 | `34316264290` (run 4575, 2026-09-09T05:46:48Z) | run 4447, 2026-09-06T20:43:13Z |
| 2 | 3911 | `34623184036` (run 4707, 2026-09-11T16:38:44Z) | run 4451, 2026-09-06T20:50:46Z |
| 3 | 4854 | `35195626323` (run 5650, 2026-09-17T07:39:28Z) | run 5535, 2026-09-16T10:53:08Z |
| 4 | 2301 | `34316264290` (run 4575, 2026-09-09T05:46:48Z) | run 4447, 2026-09-06T20:43:13Z |

**Correction (post-review):** an earlier draft of this passage (and this
report's own title block) described the 4 calls as returning "4 different
100-run samples spanning different multi-day windows." A Codex review
correctly caught that this overstates the date-window count: calls 1 and 4
share the identical newest run (`34316264290`) and oldest run boundary
(2026-09-06T20:43:13Z) — by that measure there are only **3 distinct date
windows** across the 4 calls, not 4. What *is* still 4-for-4 different,
and remains sufficient on its own to prove non-determinism, is
`total_count`: 1973 and 2301 are different numbers despite calls 1 and 4
reporting the same page boundaries, so those two calls were not proven to
be identical samples either — only that their pages' min/max timestamps
coincided. Restated precisely: **4 identical calls, 4 different
`total_count`s, 3 distinct date windows** — not "4 different 100-run
samples."

Only call 3 lands anywhere near "now" (this session's wall clock is
2026-09-17). Calls 1, 2 and 4 return **stale windows over a week old**, and
call 1 and call 4 share the same window despite being separated by two other
calls that returned completely different windows — this is not monotonic
drift or eventual pagination catch-up, it looks like the tool is drawing
from a re-randomized or re-cached population each call.

Varied which filter keys were supplied to see which combination triggers
the defect, **one call each** for the single-key filters:

| Filter | `total_count` | Newest run | This one call's result |
|---|---:|---|---|
| none | 5650 | run 5650, 2026-09-17T07:39:28Z | matches ground truth |
| `{status:"completed"}` only | 5645 | run 5650, 2026-09-17T07:39:28Z | matches ground truth |
| `{event:"pull_request"}` only | 4783 | run 5631, 2026-09-16T20:07:37Z | stale but plausible (~11h old) |
| `{event, status}` together | 1973–4854, varying per call | varying, up to 8 days stale | **confirmed non-deterministic (4 calls)** |

**Correction (post-review), two rounds.** An earlier draft of this
section's closing sentence claimed "single-run lookups and the unfiltered
list are reliable; only the combined `event`+`status` filter... is not" —
phrasing that implicitly extended to the single-key filters too. A Codex
review correctly caught that overreach for the single-key filters: only
the **combined** filter was called repeatedly (4 times, confirmed broken)
and only `get_workflow_run` was called repeatedly (2 times, confirmed
reliable); the `status`-only and `event`-only rows above are each **one
call**, exactly the evidentiary gap that sank this same report's own
first-draft recommendation of the `status`-only filter (see the correction
further below) — one passing call proves nothing about determinism, since
the combined filter's own defect was only detectable by repetition. A
second Codex review then caught that the fix still overclaimed the
**unfiltered** list as equally "confirmed": paginating pages 1, 2, and 3 is
three different requests, not one request repeated, so this session never
actually tested whether an identical unfiltered call returns the same
answer twice — see the fuller correction further below. Correctly stated:
this session **confirms** the combined filter is broken and **confirms**
`get_workflow_run` is reliable (both via genuine repetition); it leaves
both the single-key filters' and the unfiltered list's own call-to-call
determinism **untested** — the unfiltered list has other evidence in its
favor (internal cross-page consistency, content cross-validation), just
not that specific test.

**This matters retroactively.** Every prior report in this series —
09-03 through 09-16 — built its census with exactly this combined filter and
described the result as "the N most recent completed pull_request runs." Per
today's finding, that call does not reliably return the most recent runs at
all; it can return an arbitrary, possibly week-old slice. This does not mean
those reports' *internal* arithmetic was wrong — each report fetched one
JSON snapshot and reasoned about it consistently — but the claim that the
snapshot represented "the current census window" is unconfirmed for all of
them, and demonstrably false for 3 of the 4 calls made this session alone.

**Workaround used for the rest of this report, provisionally.** Paginating
the *unfiltered* `list_workflow_runs` (3 pages, `perPage=100`, no
`workflow_runs_filter`) and filtering for `event=="pull_request" and
status=="completed"` client-side in Python produced a set that: (a) has
monotonically contiguous run numbers across all 3 pages with zero gaps or
duplicates (5351–5650, 300 runs, `total_count: 5650` identical on every
page), and (b) reconstructs the exact same 15 explicit-failure runs the
09-16 report already found and named for the shared part of the window.
Recommended for every future session in this series until the underlying
tool defect is fixed: **page the unfiltered list and filter client-side;
never trust the combined `event`+`status` filter's result as "the current
window."**

**Correction (post-review), two rounds.** An earlier draft of the 🔧
Treatment section below also recommended the `status`-only filter as a
lighter-weight alternative, on the strength of the single call in the
isolation table above. A Codex review correctly caught that this
overclaims: the combined filter's defect was only ever detectable by
*repeating* an identical call, so a single passing `status`-only call is
no evidence that it is deterministic — it could fail the same way on a
second call, untested. A second Codex review then caught that the
paragraph's own replacement reasoning — calling the unfiltered path
"actually repeated and cross-checked" — overreaches by the same standard:
`page=1`, `page=2`, and `page=3` are three *different* requests, not the
same request repeated, so this session never actually tested whether
calling `list_workflow_runs` with no filter and `page=1` twice returns the
same answer twice. Points (a) and (b) above are real evidence, but of a
different and weaker kind than the combined filter's 4-identical-calls
test: (a) is internal cross-page consistency (which pagination has to
satisfy to work at all, and which the combined filter's own broken
behavior gives no reason to assume for free), and (b) shows this
particular fetch's *content* isn't fabricated, not that a repeated
identical unfiltered call reliably returns the same window — and the
09-16 report it matches against was itself built with the defective
combined filter, so matching it doesn't independently validate
current-window selection either, only that the named failing runs are
real. **Correctly stated: the unfiltered-list path is this report's
best-evidenced option, not a confirmed-deterministic one** — recommended
as the provisional default until a future session actually repeats
identical unfiltered calls (or the underlying tool defect is fixed),
not asserted as proven reliable.

This is not this repository's bug to fix — the defect is in the GitHub MCP
server's `list_workflow_runs` tool, outside `autumn-harvest`'s own `ci.yml`
or test suite, so no PR against this repo's harness applies. Recorded here
per this role's own "Ask before... adding CI plugins/dependencies" and
because every future census in this series depends on knowing it.

### 1. Activity-timeout flake (`worker_fails_workflow_when_activity_start_to_close_timeout_elapses`, issue #1558/PR #1563): no occurrence among the runs actually audited

Using the provisional-but-best-evidenced unfiltered method (see item 0),
built the census for the window since the 09-16 report's cutoff
(2026-09-16T09:33:24Z) through this session's wall clock
(2026-09-17T07:39:28Z): **109 `pull_request`+`completed` runs — 86 cancelled,
13 success, 10 failure.**

Job-logged all 10 explicit failures (`get_job_logs`, `failed_only=true`).
None of the 10 failures' own **failed** jobs was `test-db-linux` — every
one of the 10 was a `Lint`, `Test (<os>)`, or `Test (no-db, <os>, shard N)`
job. Zero opportunities for the tracked signature to appear *as a
failure* in the 10 explicit failures, and issue #1558 remains closed
(`closed_at: 2026-09-15T14:00:25Z`, no reopening, confirmed via
`issue_read` this session). **This does not mean shard 8 never ran in
these 10 runs** — see the third correction below.

**Correction (post-review), two rounds.** An earlier draft of this
section's heading and this report's own title block said "zero new
occurrences in today's window," language a Codex review correctly flagged
as broader than what was actually checked — item 6 below shows cancelled
runs can hide job-level failures behind their overall `cancelled`
conclusion, and 79 of this window's 86 cancelled runs were never read at
job level, so "zero occurrences" could only honestly cover the 10 explicit
failures and the 7-run cancelled sample (item 6), not the full window.

A second Codex review then made the more useful catch: reporting "zero
opportunities" from the failure side alone materially understates the
fix's actual exposure this window, because `test-db-linux` runs
independently on every non-draft, non-docs-only PR whose `Lint` job
succeeds (`needs: [lint, changes]`, `fail-fast: false`) — a run's overall
`success` conclusion says nothing by itself about whether shard 8 (where
this test lives) ran and passed; it has to be checked directly. Checked
directly: all 13 of this window's `success`-conclusion runs ran `Test DB
(linux, shard 8)` with job-level `conclusion: "success"` —
`35089473138`, `35100701044`, `35100891367`, `35105421682`,
`35105776046`, `35108239864`, `35113095127`, `35122845094`,
`35145046673`, `35149088615`, `35174612204`, `35188629910`,
`35195626323`. Directly log-verified for one of the 13 (`35105776046`,
fetched via the job's signed log URL and `curl`ed):
`test integration_e2e::worker_fails_workflow_when_activity_start_to_close_timeout_elapses
... ok`.

**A third Codex review then caught that this still undercounted**: the
reproduction only ran `list_workflow_jobs` for the 13 `success`-conclusion
runs, leaving the 10 `failure`-conclusion runs checked solely via
`get_job_logs(..., failed_only=true)` — which shows only failed jobs, not
whether `test-db-linux` ran independently and *passed* alongside the
failure. Per the same `needs: [lint, changes]` / `fail-fast: false` wiring,
a run whose overall conclusion is `failure` can still have run and passed
`test-db-linux`, as long as `Lint` itself succeeded (the failure came from
some other, independent job). Checked directly: of the 10 explicit
failures, `Lint` succeeded on exactly 4 — the `benchmarks_docs` cluster
(item 5): `35129116518`, `35145049656`, `35145054552`, `35152977594`
(the 6 others all failed inside `Lint` itself, which skips `test-db-linux`
entirely, per the mechanism the 09-16 report first established). All 4
of those ran `Test DB (linux, shard 8)` with job-level `conclusion:
"success"` too.

**A fourth Codex review then caught that a job-level `success` on this
shard is not itself proof the suite ran.** `ci.yml`'s own docs-only-skip
design (see the file's header comment) keeps the `Test DB (linux, shard N)`
*job* present and green even when `needs.changes.outputs.code == 'false'`
— it only gates the actual `Run Linux Docker-backed manifest suites
(shard)` *step* inside that job. A docs-only PR's shard-8 job reports
`success` having run nothing. Checked the step-level conclusion for all 17
job-level successes above (the `steps` array `list_workflow_jobs` already
returned, not a new fetch): **15 of the 17 show the suite step itself as
`success`; 2 — `35089473138` (this series' own 09-16 report PR, a
docs-only change) and `35188629910` (a Folio corpus-index change, also
docs-only) — show the suite step as `skipped`.** Those 2 are trivial
no-op passes, not executions, and do not belong in the exposure count.

Correctly stated: **15 confirmed passing shard-8 *executions* this
window** (11 from `success`-conclusion runs after excluding the 2
docs-only no-ops, 1 of those 11 log-verified to the exact assertion text;
4 more from `failure`-conclusion runs whose `Lint` job independently
succeeded), plus zero occurrences of the tracked failure signature among
the 10 explicit failures and the 7-run cancelled sample (item 6) — not a
clean census of the full 109-run window, since the 79
unaudited cancelled runs could still hide a shard-8 execution this report
never checked (in either direction — pass or fail). This is real,
positive exposure evidence for the fix holding, well beyond this report's
earlier "zero opportunities" framing, though still short of — and a much
smaller sample than — the ≥20x same-commit rerun campaign issue #1558
originally asked for, which no session in this series has run.

### 2. `quota_enforcement_tests`'s unexplained 10-second target-row timeout (09-16 report item 3): not recurred among the runs audited

Zero occurrences among today's 10 job-logged failures or the 7 job-logged
cancelled-run samples (item 6 below) — not the full window; see item 1's
correction above for why that distinction matters. Still 1/1 total across
the series — not a rate, not clustered.

### 3. `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` (open candidate, 3 prior occurrences per the 09-15 report): not recurred among the runs audited

Zero occurrences among today's 10 job-logged failures or the 7-run
cancelled sample — again, the audited subset, not the full window. Still
short of this role's own ≥20-rerun bar for a measured rate; no session has
yet run the rerun protocol this would need.

### 4. `sqlite_feasibility_docs`'s self-contradicting panic message (09-08 report, diagnostic-quality issue): recurred once more, count now 108

Run `35122304096` (2026-09-16T16:30:11Z, `claude/pensive-brahmagupta-2tsiz3`)
panicked with `"the report should state \"**108 migrations**\"; a live
count finds 108 migration directories"` — the same live-count-quoted-on-both-
sides defect first logged in the 09-08 report, now recurring at N=108 (up
from N=107 in the 09-16 report). One occurrence this window, on an unrelated
branch (a `split_top` byte-check fix), correctly gated on that branch's own
stale doc count — not a flake. Still below this role's bar for a unilateral
fix PR (diagnostic clarity, not a suite-health defect).

### 5. New: `benchmarks_docs::the_doc_names_no_competitor_engine` — 4 occurrences, fully root-caused to a single ~4h13m window where trunk-dev itself failed its own committed gate

**Correction (post-review):** an earlier draft of this section counted only
3 occurrences, omitting `35145054552` — the same run this report's item-1
shard-8 check (above) had already pulled in and correctly labeled part of
"the benchmarks_docs cluster" in a reply comment, without this section
itself ever being updated to match. A Codex review caught the mismatch
between that label and this table's own row count. Corrected to all 4,
confirmed by directly grepping `35145054552`'s own job log for the
signature (below).

| Run | When (UTC) | Branch | Failed jobs (of 30 total) |
|---|---|---|---|
| `35129116518` | 17:35:18Z | `claude/fervent-einstein-92qrxx` ("Assays #10 and #11... harvest vs Temporal on one box") | **7**: `Test (ubuntu/windows/macos-latest)` (3), `Test (no-db, ubuntu/windows/macos-latest, shard 0)` (3), `Test (no-db, ubuntu-latest, shard 3)` (1) |
| `35145049656` | 20:11:30Z | `claude/vigilant-hopper-moyhnr` ("Add batched seek-and-refine claim path") | **7**: `Test (ubuntu/windows/macos-latest)` (3), `Test (no-db, ubuntu-latest, shard 0)`, `Test (no-db, windows-latest, shard 0)`, `Test (no-db, ubuntu-latest, shard 3)`, `Test (no-db, macos-latest, shard 3)` |
| `35145054552` | 20:11:33Z | `claude/hopeful-pascal-tbijcf` ("Fix shard-rebalancing follow-ups from issue #1317") | **9**: `Test (ubuntu/windows/macos-latest)` (3), `Test (no-db, ubuntu/windows/macos-latest, shard 0)` (3), `Test (no-db, ubuntu/windows/macos-latest, shard 3)` (3) |
| `35152977594` | 21:32:46Z | `claude/magical-gauss-gn4qd2` ("Ledger: batch the outbox start relay") | **6**: `Test (ubuntu/windows/macos-latest)` (3), `Test (no-db, ubuntu/windows/macos-latest, shard 0)` (3) |

All four carry the identical panic: `docs/benchmarks.md names a competitor
engine (temporal); issue #1309 asks this page to explain the comparison
methodology, never quote a competitor's own figure`
(`benchmarks_docs.rs:231`) — directly grepped from each run's own job log,
not inferred from job names alone.

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

Three of the four occurrences (`35145049656`, `35145054552`,
`35152977594`) are on branches with nothing to do with the assay work —
`vigilant-hopper-moyhnr`'s claim-path change, `hopeful-pascal-tbijcf`'s
shard-rebalancing follow-ups (the same branch this series' 09-16 report
already tracked for 5 other, unrelated failures in this same window), and
`magical-gauss-gn4qd2`'s outbox-batching change. All three inherited a
real, deterministic, correctly-firing red purely from being based on
`trunk-dev` during the broken window — the same "stale-branch echo of a
defect in the base, not in the branch" pattern the 09-14 report first
documented for the activity-timeout test's pre-hardening pair, here
running the other direction (the defect was introduced on the base, not
fixed there yet).

**A concrete data point for the still-unconfirmed branch-protection gap.**
Run `35129116518` was created at 17:35:18Z — **39 seconds before** `266ac9c`
merged at 17:35:57Z — and it already shows this exact failure on 7 legs
(**correction, post-review**: an earlier draft said "`Test (ubuntu-latest)`
and 5 other legs," 6 total; a Codex review correctly caught that this
contradicts the table above, which lists 7 failed jobs for this run — the
full 7-job count is used consistently now). If that run was PR #1617's own
pre-merge CI (plausible: same branch, same defect, right before merge), the
PR merged into `trunk-dev` with this test failing on its own head commit or
one immediately prior. This session cannot confirm branch-protection
settings (the recurring, still-open gap), so it cannot say whether that
check was required and bypassed, not required at all, or the merge used a
different/later commit whose own CI passed — but it is exactly the kind of
concrete instance this series has been missing to make the branch-protection
gap actionable rather than abstract. Recorded here, not treated as
confirmed policy failure.

### 6. Cancelled-run sample this window: 7/8 job-logged, 1/7 hid a job-level failure; 1/8 not inspected

Attempted `list_workflow_jobs` (`perPage=100`, checking every job's own
`conclusion`, not just the run's) on 8 of this window's 86 cancelled runs,
spread across the window: `35081223941`, `35087852092`, `35105355850`,
`35117694170`, `35127884735`, `35136021958`, `35138666154`, `35141974598`.
**Correction (post-review):** an earlier draft of this section miscounted
its own sample as "8 attempted, 6 fully read" — a Codex review correctly
flagged the mismatch against the section heading and the 8 listed IDs.
Rechecked directly against this session's own tool results: `35081223941`'s
job list exceeded the inline tool-output size limit and was not re-fetched
via the signed-URL fallback this series has used before (not inspected);
the other **7** — not 6 — were fully read.

Of those 7, 6 show ordinary `concurrency.cancel-in-progress` behavior:
every job cancelled at the same instant mid-step, consistent with a newer
push superseding an in-progress run, with no job-level `"failure"`
conclusion anywhere in the run. **Correction (post-review):** an earlier
draft additionally miscategorized the 7th, `35105355850`, as "not a
hidden CI-health defect" because its `cargo fmt` failure was a real,
correctly-firing gate on that branch's own diff — true on the merits, but
beside the point this series' own 09-06/09-11 methodology actually tracks:
a cancelled *run* with a `"failure"`-conclusion *job* underneath it is a
hidden failure by that mechanical definition regardless of whether the
failure is itself legitimate, because the run's own overall conclusion
(`cancelled`) would otherwise hide it from any census that only reads run-
level conclusions. Corrected: **1/7 sampled cancelled runs hid a job-level
failure** (`35105355850`'s `Lint` job, `Check formatting` step, real and
deterministic — not a suite-health defect, but a hidden failure by this
series' own counting convention), against 2/8 in the 09-16 report's
sample — both samples too small to compare rates. 79 of this window's 86
cancelled runs remain unaudited (86 total minus the 7 actually read;
`35081223941` counts as unaudited, not as a completed read).

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

**Item 1** additionally has real positive exposure evidence, not just
absence of failure: 15 confirmed passing shard-8 *executions* this window
(1 log-verified, 14 step-conclusion-verified — 11 from the `success`-
conclusion runs after excluding 2 docs-only no-ops that reported job-level
`success` having run nothing, plus 4 more from `failure`-conclusion runs
whose `Lint` job independently succeeded), on top of zero occurrences
among the 10 explicit failures and the 7-run cancelled sample. **Items
1–3** together show continued absence of recurrence for three previously
open items,
among the population this session actually audited (10 explicit failures
plus a 7-run cancelled sample, out of 109 runs in a provisionally-
constructed ~22-hour window — "provisional" per item 0's own correction,
not a claim this construction method is itself confirmed reliable). None
of the three clears this
role's own bar for "confirmed holding" (no rerun campaign has ever been
run for any of them), and none of the three is confirmed absent from the
window's remaining 79 unaudited cancelled runs either — but none regressed
in what was checked.

**Item 4** is the same known diagnostic-message defect recurring on
schedule, as expected — not a new finding, not actioned for the same
reason as every prior report.

**Item 5** is a fully root-caused, deterministic, single-commit-window
defect — not a suite flake. `trunk-dev` was briefly red against its own
gate; a same-day follow-up fixed it. The three downstream "stale-branch
echo" occurrences are not independent findings, they are the same root
cause propagating to any branch built on the broken window. Filed here as a
health-report item, not a fix PR, because the defect is already fixed on
`trunk-dev` (confirmed by direct inspection of the current file) — there is
nothing left to fix. The branch-protection data point is exactly that: a
data point, not a confirmed policy conclusion.

**Item 6** is ordinary CI mechanics for 6 of the 7 runs actually inspected,
plus one real, correctly-firing `cargo fmt` gate hidden under a cancelled
run's overall conclusion — a genuine hidden failure by this series' own
counting convention, though not a suite-health defect on its own merits.
Too small and too narrow a sample to say anything about the other 79
cancelled runs this window.

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
filter for census work.** Page the unfiltered list and filter client-side —
this session's best-evidenced option (internal cross-page consistency plus
content cross-validation against the 09-16 report), though **not** proven
deterministic under this report's own bar of repeating an identical call
(see the correction above; the `status`-only filter is not recommended
either, on the strength of a single untested call). This is a change in
*how this role gathers evidence*, not a change to the repository's CI
configuration, so it needs no PR — but it should be treated as the
provisional default for the next session that opens one of these reports,
until that session (or a future one) actually repeats an identical
unfiltered call to confirm it, or the underlying tool defect is fixed.

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
  `total_count` values (1973, 3911, 4854, 2301) across **3 distinct date
  windows** (corrected from an earlier draft's "4 distinct date windows" —
  calls 1 and 4 share the same newest/oldest run boundary, though their
  `total_count`s still differ) — confirmed non-deterministic by genuine
  repetition. 2/2 identical calls to `get_workflow_run` for the same run ID
  returned identical data — confirmed reliable by genuine repetition. 1/1
  unfiltered paginated fetch (3 pages — 3 *different* requests, not one
  repeated) was internally consistent (`total_count: 5650` on every page,
  contiguous run numbers 5351–5650, zero gaps/duplicates) and reconstructed
  the exact same 15 failures the 09-16 report already named for the
  overlapping portion of the window. **Correction (post-review):** an
  earlier draft called this "cross-validated... not just self-consistent"
  and treated it as confirmed on a par with the combined filter's and
  `get_workflow_run`'s repetition tests. A Codex review correctly caught
  that paginating 3 different pages is not repeating an identical request,
  so this session never tested whether the unfiltered list itself is
  deterministic call-to-call — the evidence above is real but of a
  different, weaker kind (internal consistency + content validation, not
  repetition), and the 09-16 report it matches was itself built with the
  defective combined filter, so the match validates that report's named
  runs' authenticity, not "current-window selection." Corrected: the
  unfiltered list is this session's best-evidenced option and this
  report's provisional recommendation, **not** a confirmed-deterministic
  one. The single-key `event`-only and `status`-only filters were each
  called once and are **untested** for determinism, not confirmed reliable
  either (corrected from an earlier draft's stronger claim).
- **Item 1:** 15 confirmed shard-8 *executions* ran and passed this
  window (11/13 of the `success`-conclusion runs — 2 excluded as
  docs-only no-ops whose suite step was `skipped`, not run, a third
  Codex-review catch on a job-level-only reading; 1 of the 11
  log-verified; plus 4/4 of the `failure`-conclusion runs whose own
  `Lint` job succeeded, a second catch after the first established that
  "zero opportunities" understated exposure by only counting the failure
  side). Plus 0/10 explicit failures and 0/7 cancelled sample carry the
  failure signature.
- **Items 2–3:** 0 occurrences each among the 10 explicit failures and the
  7-run cancelled sample actually audited this window (corrected from an
  earlier draft's "0 new occurrences... over a 109-run window" — the other
  79 cancelled runs were not checked). Not rerun-campaign confirmations.
- **Item 4:** 1/1 occurrence this window, consistent with the known,
  unfixed defect.
- **Item 5:** 4/4 occurrences confirmed to share one root cause (corrected
  from an earlier draft's 3/3 — a Codex review caught that `35145054552`
  was omitted from this section despite already being named in this
  report's own item-1 reply comment; its panic text was directly grepped
  from its job log to confirm the same signature) via direct commit
  inspection (`git show`, `git diff`, `git log --format=%cI`), not
  inferred from timing alone. 0/4 required a rerun — the fix already
  shipped upstream 4h13m after the defect was introduced; confirmed absent
  from the current `docs/benchmarks.md` by direct grep.
- **Item 6:** 7/8 attempted cancelled-run job-logs fully read; 1/7 hid a
  job-level failure (`35105355850`'s real, correctly-firing `cargo fmt`
  red, already superseded by a later push on the same branch — a hidden
  failure by this series' counting convention, not a suite-health defect).
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

# The provisional (not proven call-to-call deterministic — see item 0)
# substitute used for this report's census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   perPage=100, page=1)   # then page=2, page=3 — no workflow_runs_filter
python3 -c "
import json
from collections import Counter
pages = ['page1.json','page2.json','page3.json']  # saved tool-result files

# Correction (post-review): an earlier draft of this script deduplicated
# straight into a dict (allruns[r['id']] = r) without ever checking
# whether a run repeated across a page boundary -- a Codex review
# correctly caught that this would silently overwrite, not surface, the
# exact pagination instability this workaround exists to guard against,
# and that the script never actually asserted the invariants ('zero
# gaps/duplicates', 'total_count identical on every page') the prose
# cites as evidence. Rewritten to check the raw rows BEFORE deduplicating.
total_counts = []
raw_ids = []
allruns = {}
for p in pages:
    with open(p) as f:
        d = json.load(f)
    total_counts.append(d['total_count'])
    for r in d['workflow_runs']:
        raw_ids.append(r['id'])
        allruns[r['id']] = r

assert len(set(total_counts)) == 1, f'total_count differs across pages: {total_counts}'
dupes = {k: v for k, v in Counter(raw_ids).items() if v > 1}
assert not dupes, f'duplicate run IDs across pages: {dupes}'
run_numbers = sorted(r['run_number'] for r in allruns.values())
assert run_numbers == list(range(run_numbers[0], run_numbers[-1] + 1)), 'run_number sequence has gaps'
print('total_count (all pages):', total_counts[0])
print('run_number range:', run_numbers[0], '-', run_numbers[-1], f'({len(run_numbers)} contiguous, no gaps, no duplicates)')

runs = list(allruns.values())
pr_completed = [r for r in runs if r['event']=='pull_request' and r['status']=='completed']
cutoff = '2026-09-16T09:33:24Z'   # the 09-16 report's own cutoff
new = [r for r in pr_completed if r['created_at'] > cutoff]
print(len(new), Counter(r['conclusion'] for r in new))
"
# -> total_count 5650 on all 3 pages; run_number range 5351-5650 (300
#    contiguous, no gaps, no duplicates); 109 runs:
#    {'cancelled': 86, 'success': 13, 'failure': 10}
# Re-run against this session's own saved pages 2026-09-17: all three
# assertions pass.

# Item 5 — the benchmarks_docs root cause:
git show 266ac9c:docs/benchmarks.md | grep -ni "temporal\|dbos\|cadence\|zeebe\|conductor\|restate"
git diff 266ac9c f01a448 -- docs/benchmarks.md | grep -i temporal
git log -1 --format=%cI 266ac9c    # 2026-09-16T12:35:57-05:00
git log -1 --format=%cI f01a448    # 2026-09-16T16:49:14-05:00
grep -ni temporal docs/benchmarks.md   # current tree: no matches

# Item 5's 4th occurrence (the one an earlier draft omitted): confirm
# 35145054552 carries the identical signature, not just a structurally
# similar job-name pattern:
# get_job_logs(run_id=35145054552, failed_only=true, return_content=true)
# -> job "Test (ubuntu-latest)" contains:
#    "thread 'benchmarks_docs::the_doc_names_no_competitor_engine' ...
#     panicked at autumn-harvest/tests/integration/benchmarks_docs.rs:231:9:
#     docs/benchmarks.md names a competitor engine (temporal); ..."

# Per-failure job logs, this window's 10 explicit failures:
# get_job_logs(run_id=<id>, failed_only=true, return_content=true, tail_lines=50-60)
#   35096353603 35100756217 35117297810 35122304096 35129116518
#   35132957731 35140963232 35145049656 35145054552 35152977594

# Item 1's shard-8 positive-exposure check, this window's 13 success runs:
# actions_list(method="list_workflow_jobs", resource_id=<run_id>, perPage=100)
#   35089473138 35100701044 35100891367 35105421682 35105776046
#   35108239864 35113095127 35122845094 35145046673 35149088615
#   35174612204 35188629910 35195626323
# then grep each job list for name contains "shard 8" and read its
# "conclusion" -- all 13 report "success".
# Log-verify one directly: get_job_logs(job_id=104835185301,
#   return_content=false) for the signed logs_url, then curl it and grep:
grep "worker_fails_workflow_when_activity_start_to_close_timeout_elapses" \
  /tmp/shard8_35105776046.log
# -> "test integration_e2e::worker_fails_workflow_when_activity_start_to_close_timeout_elapses ... ok"

# Correction (post-review), the same check extended to the 10 FAILURE-
# conclusion runs: get_job_logs(..., failed_only=true) alone can't show a
# job that ran and PASSED alongside an unrelated failure. Checked each of
# the 10 for whether its own Lint job succeeded (test-db-linux needs
# [lint, changes], so Lint failing skips it entirely -- 6 of the 10 fail
# inside Lint itself, no opportunity). The other 4 (all benchmarks_docs,
# item 5) have Lint succeeding:
# actions_list(method="list_workflow_jobs", resource_id=<run_id>, perPage=100)
#   35129116518 35145049656 35145054552 35152977594
# then check the "Lint" job's own conclusion and the "Test DB (linux,
# shard 8)" job's own conclusion -- all 4 show Lint: success and
# shard 8: success.

# Correction (post-review), a fourth pass: a job-level "success" on
# "Test DB (linux, shard 8)" is not proof the suite ran -- ci.yml's
# docs-only-skip design keeps the JOB green while gating only the
# "Run Linux Docker-backed manifest suites (shard)" STEP inside it on
# needs.changes.outputs.code == 'true'. Re-checked the steps[] array
# already returned by the list_workflow_jobs calls above (no new fetch)
# for all 17 job-level successes:
python3 -c "
import json
files = {
    35089473138: 'run1.json', 35100701044: 'run2.json', 35100891367: 'run3.json',
    35105421682: 'run4.json', 35108239864: 'run5.json', 35113095127: 'run6.json',
    35122845094: 'run7.json', 35145046673: 'run8.json', 35149088615: 'run9.json',
    35174612204: 'run10.json', 35188629910: 'run11.json', 35195626323: 'run12.json',
    35129116518: 'run13.json', 35145049656: 'run14.json', 35145054552: 'run15.json',
    35152977594: 'run16.json',
}  # saved list_workflow_jobs results, one file per run
for run_id, path in files.items():
    with open(path) as f:
        jobs = json.load(f)['jobs']['jobs']
    shard8 = next(j for j in jobs if 'shard 8' in j['name'] and 'DB' in j['name'])
    step = next(s for s in shard8['steps'] if 'Run Linux Docker-backed manifest suites' in s['name'])
    print(run_id, 'job:', shard8['conclusion'], '| suite step:', step['conclusion'])
"
# -> 15 of 17 show suite step "success" (a real execution); 35089473138
#    (this series' own 09-16 report PR) and 35188629910 (a Folio
#    corpus-index change) show "skipped" -- both docs-only, no-op passes,
#    excluded from the exposure count.

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
