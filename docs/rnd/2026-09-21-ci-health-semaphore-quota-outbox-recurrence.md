# 🚦 Semaphore CI health — `quota_enforcement_tests`' unexplained outbox-retry
# timeout recurs a 2nd confirmed time (byte-identical panic, new branch, 6
# days later), `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`
# recurs again undiagnosed, and one branch's worker-wide wait-timeout
# cascade — not a suite defect on the evidence gathered

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series from `docs/rnd/2026-09-20-ci-health-semaphore-migration-count-message-fix.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally `test-db-linux`/`test`/`test-nodb`, plus the ungated
`lint` job. Branch-protection status and cache-usage API access remain
unavailable from this session (`ToolSearch` re-checked today for both; neither
tool is present, unchanged from every prior report in this series).

## 🌡️ Symptom

### 1. Today's census: window since the 09-20 report's cutoff (2026-09-20T06:06:33Z) through this session's wall clock (2026-09-21T09:28:48Z) — 78 runs: 44 cancelled, 19 failure, 15 success

`list_workflow_runs` with the combined `{event: "pull_request", status:
"completed"}` filter, one page (100 runs, `2026-09-20T01:33:40Z` through
`2026-09-21T09:28:48Z`), filtered to `created_at >= 2026-09-20T06:06:33Z`.
Job-logged all 19 explicit failures (not a sample this time — the window was
small enough to cover fully):

| Run | Branch | Signature |
|---|---|---|
| `35525986977` | `claude/magical-gauss-10blzb` | clippy `cast_precision_loss` in `audit_log_unexported_idx_write_cost_perf.rs` (own diff) |
| `35527850309` | `claude/magical-gauss-10blzb` | same clippy `cast_precision_loss`, same branch, unfixed across 2 pushes |
| `35531289080` | `claude/magical-gauss-10blzb` | `E0061`: `PendingRequeueChangeset::new` called with 2 args, takes 1 (`queue.rs:10251`/`3012`) — own diff |
| `35531826444` | `claude/magical-gauss-10blzb` | same `E0061`, unfixed, 3rd push |
| `35532005601` | `claude/exciting-babbage-r3nfok` | same `E0061`, same lines, different branch name |
| `35533403023` | `claude/exciting-babbage-r3nfok` | comment-hygiene Tier B (`activity.rs`/`workflow.rs` sentence length, own diff) |
| `35534123956` | `claude/magical-gauss-10blzb` | same `E0061`, unfixed, 4th push |
| `35534264738` | `claude/lucid-pasteur-9m45n7` | `sqlite_feasibility_docs`: 2 failures (own doc/schema mismatch) |
| `35535026904` | `claude/kind-hopper-wbrak0` | clippy dead-code (`backup_verify.rs::absence_is_decisive_loss`, own diff) **and** `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` — see item 3 |
| `35535358141` | `claude/fix-pending-requeue-changeset-arity` | `Test DB (linux, shard 1)`: `cross_region_dr_tests` — see below, not root-caused this session |
| `35536291030` | `claude/magical-gauss-10blzb` | same `E0061`, unfixed, 5th push |
| `35549811155` | `claude/exciting-babbage-r3nfok` | run conclusion `failure`, but `get_job_logs(failed_only=true)` returns 0 failed jobs of 30 — see the data-quality note below |
| `35551323906` | `claude/kind-hopper-wbrak0` | `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` (own doc staleness; the 09-20 report's diagnostic-message fix is visibly working — panic text now reads "the report states..." rather than repeating the live count) |
| `35552427919` | `claude/laughing-maxwell-9t78rp` | comment-hygiene Tier B (`shard_rebalance.rs`, own diff) |
| `35553863066` | `claude/laughing-maxwell-9t78rp` | `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded` — see item 2, a 2nd confirmed occurrence |
| `35557880574` | `claude/kind-hopper-wbrak0` | comment-hygiene Tier B (`execution.rs`, own diff) |
| `35563198153` | `claude/keen-bardeen-amm1ft` | 6 shards fail; nearly all failures panic at the same shared polling helper — see item 4 |
| `35566461177` | `claude/kind-hopper-wbrak0` | `cargo fmt` diff (own diff) |
| `35523524313` | `claude/kind-clarke-ef1yn2` | run conclusion `failure`, but 0 failed jobs of 30 — same data-quality note as `35549811155` |

The `E0061` compile break (5 occurrences above, spanning two branch names,
19:07Z–20:45Z) is `requeue_workflow_task_for_quota_retry`'s call site
supplying an extra `chrono::Utc::now()` argument to
`PendingRequeueChangeset::new`, which takes only `previous_error: String`.
This is already fixed on `trunk-dev`: `bdf7d58` ("Fix
requeue_workflow_task_for_quota_retry compile break (unblocks issue #1610
verification)", #1669) is the HEAD-most commit in this session's own branch
history. Not actioned further here — it is a deterministic own-branch compile
error, caught correctly by `Lint`/`MSRV` every time, already resolved.

**Data-quality note (2 of 19):** `35549811155` and `35523524313` both report
overall conclusion `failure` while `get_job_logs(run_id=..., failed_only=true)`
returns zero failed jobs across all 30. Not root-caused this session — a
plausible mechanism is a job that failed on an earlier attempt within the
run before succeeding, or a conclusion computed from a check the jobs API
does not expose (e.g. a required status check outside the workflow's own
job graph) — but this is not confirmed, only a candidate. Recorded as a
census gap the way the 09-06/09-11 reports recorded the cancelled-run
job-hiding gap: `list_workflow_runs`' `conclusion` field is not always
reconcilable with `list_workflow_jobs`, and no session has yet built the
harness to systematically check the gap's size.

### 2. `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`: 2nd confirmed occurrence, byte-identical panic text, 6 days later, different branch

The 09-16 report (item 3) found one occurrence of this test failing with
`"target row was never created by the outbox retry; last count was 1"` at
`quota_enforcement_tests.rs:3329:9` (run `35034838493`,
`claude/too-many-lines-followup`, 2026-09-15T23:15:28Z) and explicitly
declined to call it a flake at n=1, describing it instead as "an unexplained
10-second target-row timeout with a plausible timing-category candidate, not
a confirmed mechanism."

Today's run `35553863066` (`Test DB (linux, shard 10)`,
`claude/laughing-maxwell-9t78rp`, 2026-09-21T02:20:38Z) failed the identical
test, and direct log inspection (`get_job_logs` → signed URL → `curl` + `grep`,
since the shard's suite exceeds any reasonable `tail_lines`) shows the panic
text is **byte-identical** to the 09-16 occurrence, save for the line number
tracking six days of unrelated edits to the file:

```
thread 'quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded' (37104) panicked at autumn-harvest/tests/integration/quota_enforcement_tests.rs:3461:9:
target row was never created by the outbox retry; last count was 1
```

Every other test in the same suite passed cleanly in this run (48/49 in the
visible log before the panic), so this is not a suite-wide DB or
infrastructure problem in this run — the failure is scoped to this one test,
consistent with the 09-16 occurrence.

**This raises the count from 1 to 2 confirmed occurrences**, spanning two
different branches (`claude/too-many-lines-followup`,
`claude/laughing-maxwell-9t78rp` — no shared ancestry visible from branch
names alone, not verified further) six days apart, with the identical
mechanism candidate each time: the test frees a quota slot, then polls every
50ms up to a 10-second deadline for a background
`enforce_completion_triggers_outbox` sweep (folded into the worker's
timeout-checker loop) to retry a deferred completion trigger and create the
target row — and the row count never leaves 1. As the 09-16 report already
established, the recovered panic text proves the row count stayed at 1; it
does not by itself prove whether the sweep ran and no-op'd, ran and failed to
create the row, or never ran at all — no worker/tracing output is captured
in `cargo test`'s stdout, only in this test binary's own assertions. That
gap is unchanged; this session did not obtain worker-level logging either.

Two occurrences with an identical, specific panic text is a meaningfully
stronger signal than the single occurrence the 09-16 report logged, but it
is still far short of this role's own ≥20-rerun bar for a measured rate, and
still has no confirmed mechanism (test-side timing bug vs. a genuine
product-side sweep failure) — the hard gate's requirement 3 (product/test
verdict rendered first) is not met. **This is the strongest documented
argument yet, across the whole series, for spending a same-commit rerun
campaign on this specific test** rather than on the closed activity-timeout
item (#1558, now at 0/1 confirmed exposure per the 09-16 report) — 2
independent real-world occurrences beats 0 confirmed exposures as a signal
of where to spend the ≥20x budget this role has not yet had the means to
run (no Docker in this session's sandbox; see the 09-16 report's identical
constraint).

### 3. `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`: recurs again, still undiagnosed

Run `35535026904` (`Semantic determinism analysis (harvest-verify)`,
`claude/kind-hopper-wbrak0`, 2026-09-20T20:16:44Z) failed this same test
again: `test result: FAILED. 5 passed; 1 failed` — the identical pass/fail
split the 09-15 and 09-16 reports already recorded for this signature's
prior occurrences. The 09-18 report (`docs/rnd/2026-09-18-ci-health-semaphore-corpus-gate-diagnostic-fix.md`)
shipped a diagnostic-clarity fix for a *different* self-contradicting panic
message in this same test file (rustc diagnostics dropped on the floor under
`--message-format=json`), explicitly noting it was closing a diagnostic gap,
**not** the recurring `seeded_corpus_is_clean_under_the_syntactic_layer`
failure itself, which the 09-16 report already flagged as "the closest thing
in this report's data to a suite-attributable-flake candidate" at 3
occurrences. Today's is at least a 4th confirmed occurrence (09-15 report:
3 of 5 pushes on one branch; 09-16 report: one of those resurfacing in its
own window's sample; today: a fresh occurrence on a fifth, unrelated branch).
Not root-caused this session — this report did not have time to pull the
09-18 fix's improved diagnostic output for this specific run (the improved
panic message should now surface the actual `rustc` finding rather than a
bare `FAILED`), which would be the fastest next step for whoever picks this
up.

### 4. `claude/keen-bardeen-amm1ft`: one run, 6 of 30 shards fail, most panics at one shared polling helper — own-branch, not a suite defect on this evidence

Run `35563198153` (2026-09-21T05:04:24Z) failed 6 of its 30 `Test DB (linux,
shard N)` legs (shards 0, 3, 4, 5, 8, 10). Full-log inspection of shard 3
(the worst-hit, `curl` of the signed log URL since the module exceeds any
`tail_lines`) shows the large majority of its ~40 failing tests — spanning
four otherwise-unrelated suites (`chain_timeout_tests`, `ctx_info_tests`,
`mixed_suspension_tests`, `dispatch_tests`) — panic at the identical
location, `integration_e2e.rs:1383:6`, inside a shared test helper,
`wait_for_execution_state_with_timeout`:

```
workflow should reach expected state within timeout: Elapsed(())
```

That helper polls the database every 50ms for a workflow to reach an
expected state, and gives up after a fixed `tokio::time::timeout`. A handful
of the shard's failures instead panic at an assertion inside their own test
file (`chain_timeout_tests.rs:736`/`880`, `mixed_suspension_tests.rs:508`),
not the shared helper — not checked further this session. This shape (one
shared wait-helper's timeout firing across many otherwise-independent tests
in one shard) is consistent with either a systemic problem in that run's
environment (worker never processes tasks, DB contention) or a genuine
regression in this branch's own diff that broke workflow dispatch broadly
enough that nothing reaches its expected state in time. **Not
root-caused, and not rendered as a test-vs-product verdict** — this
session did not fetch this branch's diff, did not check whether a later
commit on the same branch fixed it, and does not have the worker-level
logging that would distinguish "this branch's own code broke dispatch" from
"this run's shard 3 environment was starved." Recorded because the *shape*
(a shared blocking-wait helper's timeout cascading across many unrelated
suites in one run) is a pattern worth recognizing if it recurs on other
branches — a single occurrence on one branch's own in-progress work does not
clear this role's evidentiary bar for anything beyond a note.

### 5. `cross_region_dr_tests`: one occurrence, new signature, not root-caused

Run `35535358141` (`claude/fix-pending-requeue-changeset-arity`,
2026-09-20T20:22:52Z — the branch that went on to ship the `E0061` fix
merged as `bdf7d58`/#1669) failed `Test DB (linux, shard 1)` with `FAILED
SUITES: ... cross_region_dr_tests`, but the visible tail (150 lines) ends at
the `FAILED SUITES:` summary line without the individual test's panic
reaching the window — the same log-truncation gap this series has hit
repeatedly (09-14, 09-16 reports) on large serial suites. Not pursued to the
full log this session; recorded as a single, unclassified occurrence, not
clustered with anything else in this window.

## 🔍 Diagnosis

**Item 1 (census)** is the suite working as designed for 15 of 19 explicit
failures — deterministic own-branch defects (compile breaks, clippy, `cargo
fmt`, comment-hygiene, doc staleness), all correctly gated. 2 of 19
(`35549811155`, `35523524313`) are a census/tooling gap, not a suite defect
claim. The remaining 2 (items 4 and 5) are unclassified single occurrences.

**Item 2** cannot yet be given a test-vs-product verdict — this role's own
hard gate (requirement 3) blocks a fix PR until the nondeterminism is shown
to live in the test rather than the product it exercises, and this session
did not obtain the worker/tracing evidence that would decide it. What
changed today is the evidentiary weight: 1 occurrence was "recorded, not
actioned"; 2 independent occurrences with byte-identical panic text six days
apart is the strongest recurrence signal this series has produced for any
single candidate since the (now-closed, 0/1-exposure) activity-timeout item.
This is the report's headline recommendation: **whoever next has Docker
available in-session should point the ≥20x rerun campaign at
`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
before any other candidate in this series' backlog.**

**Item 3** remains undiagnosed after a 4th occurrence. Below this role's
≥20-rerun bar, but now recurring often enough (roughly weekly, across five
distinct branches over the series) that it is worth someone spending 20
minutes reading the 09-18 fix's improved diagnostic output against a fresh
occurrence, which might resolve it far faster than a rerun campaign.

**Item 4** is explicitly not rendered as a test-vs-product verdict — the
evidence gathered this session (one run, one branch, no diff read, no
later-commit check) is insufficient to say whether this is a real
dispatch-path regression in that branch's own code or an environment issue,
and guessing either way would be exactly the kind of overclaim this series'
own correction history (09-16 report) warns against repeating.

**Item 5** has no mechanism recovered; a single, unclustered occurrence.

## 🔧 Treatment

None shipped. Nothing found clears the impact floor this round: no flaky
test made deterministic (0/N verified), no product bug rendered with a
mechanism, no timing win measured, no quarantine ledger entries to retire
(still no quarantine ledger in this repository — checked again), no suite
passing under shuffled order to report (not run this session). Per the hard
gate, a report is the correct outcome, not a PR against `ci.yml` or any test.

Carried forward, unchanged from prior reports in this series:

1. **Cache-usage API access** — still unavailable, checked again today.
2. **Branch-protection confirmation** — still unavailable, checked again
   today.
3. **The rerun campaign for `quota_enforcement_tests`** (this report's new
   top priority; the 09-16 report's activity-timeout `#1558` campaign is
   comparatively less urgent now, at 0/1 confirmed exposure and no fresh
   occurrences since) — still not run by any session; no Docker available
   in this session's sandbox.
4. **`corpus::seeded_corpus_is_clean_under_the_syntactic_layer`** — a 4th
   occurrence, still undiagnosed; the 09-18 report's improved diagnostic
   output for this file has not yet been checked against a fresh occurrence.
5. **The `list_workflow_runs` conclusion vs. `list_workflow_jobs` gap**
   (item 1's data-quality note) — 2 of 19 runs this window report `failure`
   overall with 0 job-level failures found; not previously logged in this
   series in exactly this form (distinct from the cancelled-run-hides-a-
   failure direction the 09-06/09-11 reports found — this is the reverse:
   an explicit `failure` conclusion the jobs API cannot account for).
6. **The remaining cancelled-run population** — the 44 cancelled runs in
   this window were not job-logged at all this session (time budget went to
   the 19 explicit failures instead, all 19 of which were checked, a
   improvement over prior reports' partial samples).

## 📊 Measurement

- **Census:** 78 runs in window, 19 explicit failures, all 19 job-logged
  (100% of explicit failures this window, versus partial samples in most
  prior reports). 15/19 deterministic own-branch defects, root-caused. 2/19
  a census/tooling gap (conclusion/job mismatch). 2/19 unclassified single
  occurrences (items 4, 5).
- **Item 2:** 2/2 confirmed occurrences of
  `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
  carry byte-identical panic text (`"target row was never created by the
  outbox retry; last count was 1"`), 6 days apart, 2 different branches. Not
  a rate (n=2, no rerun protocol run). No revert check applies — no fix was
  made or attempted this session.
- **Item 3:** 4th confirmed occurrence (this session's own count from this
  series' prior reports plus today's), still no rate, no mechanism.
- **Item 4:** 1 occurrence, 6/30 shards, ~40 failing tests in the worst
  shard, large majority sharing one panic site
  (`integration_e2e.rs:1383:6`, the `wait_for_execution_state_with_timeout`
  helper). Not a rate; not clustered against any other occurrence in this
  session's data.
- **Item 5:** 1 occurrence, unclassified.
- **Ledger:** no quarantine ledger exists in this repository to update.

## 🔬 Reproduce

```sh
# Today's window census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml", owner=
#   "autumn-foundation", repo="autumn-harvest",
#   workflow_runs_filter={event:"pull_request", status:"completed"},
#   perPage=100) -> one page, 2026-09-20T01:33:40Z .. 2026-09-21T09:28:48Z,
# total_count 5090. Filtered to created_at >= 2026-09-20T06:06:33Z (the
# 09-20 report's cutoff): 78 runs (44 cancelled, 19 failure, 15 success).

# Per-failure job logs, all 19:
# get_job_logs(run_id=<id>, failed_only=true, return_content=true,
#   tail_lines=20-40) for 35525986977 35527850309 35531289080 35531826444
#   35532005601 35533403023 35534123956 35534264738 35535026904
#   35535358141 35536291030 35549811155 35551323906 35552427919
#   35553863066 35557880574 35563198153 35566461177 35523524313

# Item 2's byte-identical-panic check:
# get_job_logs(job_id=106196807563, return_content=false) -> signed URL
curl -sS -o shard10.log '<signed logs_url>'
grep -n "quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded\|panicked at" shard10.log
# -> panics at quota_enforcement_tests.rs:3461:9, "target row was never
#    created by the outbox retry; last count was 1" -- identical text to
#    the 09-16 report's run 35034838493 (there at line 3329, six days of
#    unrelated edits explain the line-number drift).

# Item 4's shared-helper check:
# get_job_logs(job_id=106225838152, return_content=false) -> signed URL
curl -sS -o shard3.log '<signed logs_url>'
grep -n "panicked at\|FAILED\|error\[" shard3.log | head -60
# -> the large majority of ~40 failures across 4 unrelated suites panic at
#    integration_e2e.rs:1383:6 ("workflow should reach expected state
#    within timeout: Elapsed(())"), inside wait_for_execution_state_with_timeout:
sed -n '1370,1384p' autumn-harvest/tests/integration/integration_e2e.rs

# The E0061 compile break, already fixed on trunk-dev:
git log --oneline -1 --grep="requeue_workflow_task_for_quota_retry compile break"
# -> bdf7d58 (#1669), already in this session's branch history (HEAD~2 at
#    time of writing).

# Tool-availability re-checks (unchanged from every prior report):
# ToolSearch("branch protection rules github") -> no matching tool
# ToolSearch("actions cache usage github") -> no matching tool
```
