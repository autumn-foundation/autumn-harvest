# 🚦 Semaphore CI health — a same-day "no race left to lose" fix that
# still failed post-merge, a stale-branch echo of the bug it fixed, the
# dashboard panel-id root-cause fix holding, and two single-occurrence
# candidates not yet worth a rate claim

**Status:** health report / issue filed (#1558, corrected after review) — no
PR opened against `ci.yml`, no test changed. Continues the series in
`docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md` and
`docs/rnd/2026-09-1[1-3]-ci-health-semaphore*.md`.

**Correction (post-publication):** this report's first version grouped three
occurrences of `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`
into one cluster with one candidate mechanism, and misreported the span
between the two outer timestamps as 3h11m. A Codex review on PR #1559 caught
both errors: the span is 3h20m38s (21:45:08 to 01:05:46, arithmetic error in
the original), and — materially — two of the three occurrences ran a
pre-hardening version of the test that a same-day fix (`7fbfebb`) had already
addressed, while the third ran the **post-hardening** version and still
failed the identical assertion about an hour after that fix merged. Verified
directly against each commit's blob for `integration_e2e.rs` (below); section
1 is rewritten to reflect this. Issue #1558 has been corrected to match.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request`/`push`
triggers against `trunk`/`trunk-dev`, principally the `test-db-linux` (11
shards) and `test`/`test-nodb` matrices.

## 🌡️ Symptom

Sampled the 100 most recent completed `pull_request`-event `ci.yml` runs via
`actions_list(method="list_workflow_runs", ...)`, covering 2026-09-13T16:45:19Z
through 2026-09-14T09:39:43Z (~17 hours): 63 cancelled, 20 success, **17
explicit failure**. Job-logged all 17 failures (`get_job_logs`, `failed_only`,
falling back to the signed `logs_url` + direct fetch when a job's log exceeded
the tool's inline size limit, and to `list_workflow_jobs` with pagination when
`get_job_logs`'s own job enumeration undercounted — see 🔬 below).

### 1. `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`: two occurrences of an already-fixed bug, and one occurrence of the fix itself still failing

| Run | Job | When (UTC) | Branch | Commit | Panic line | Test revision at that commit |
|---|---|---|---|---|---|---|
| `34781980082` | Test DB (linux, shard 0) | 21:45:08 | `claude/gracious-noether-a7mahx` | `403c0652` | `integration_e2e.rs:3529:5` | **pre-hardening** (blob `8eeeb87`) |
| `34787239282` | Test DB (linux, shard 0) | 01:05:46 | `claude/vibrant-hamilton-s6oknw` | `97969f2f` | `integration_e2e.rs:3529:5` | **pre-hardening** (blob `8eeeb87`) |
| `34789424377` | Test DB (linux, shard 7) | 23:56:26 | `claude/hopeful-rubin-rlkl1n` | `874784b9` | `integration_e2e.rs:3541:5` | **post-hardening** (blob `27509aa`) |

Identical assertion in all three:

```
assertion failed: matches!(history.events.as_slice(),
    [WorkflowEvent::WorkflowStarted { .. }, WorkflowEvent::ActivityScheduled
    { .. }, WorkflowEvent::ActivityStarted { .. },
    WorkflowEvent::ActivityTimedOut
    { timeout_type: TimeoutType::StartToClose, .. },
    WorkflowEvent::WorkflowFailed { .. },])
```

**The line number difference is not incidental.** `7fbfebb` (merged
2026-09-13T22:53:08Z, changing this file's blob from `8eeeb87` to `27509aa`)
hardened this exact test against this exact assertion, in two steps within
the same PR: first widening `slow_activity`'s sleep from 250ms to 600ms
against a 50ms→100ms `default_start_to_close` (still racy — it failed twice
in that PR's own CI), then abandoning the margin approach entirely and
setting the sleep to 30 seconds, past the test's own 10-second wait-loop
bound, with the commit message stating "no race left to lose" — the activity
can no longer complete before the test gives up, so the only way to reach
`FAILED` is meant to be the `StartToClose` timeout, deterministically.

Fetched each occurrence's exact commit tree via `get_file_contents(...,
sha=<commit>)` and compared blob SHAs against `7fbfebb`'s diff:

- `403c0652` and `97969f2f` both carry blob `8eeeb87` — the **pre-hardening**
  test (250ms sleep, 50ms timeout). These are not a new bug: they are the
  exact race `7fbfebb` already fixed, recurring only because those two
  branches had not yet merged `trunk-dev` past that fix (`97969f2f`'s CI run
  at 01:05:46Z postdates the fix's 22:53:08Z merge by over 2 hours, but the
  branch's own tree still predates it — a stale-branch echo, not a live
  defect). **Not routed forward as an open item**; a rebase closes it.
- `874784b9` carries blob `27509aa` — the **post-hardening** test (30s
  sleep, 100ms timeout, the "no race left to lose" version) — and still hit
  the identical event-history assertion, 1h03m18s after `7fbfebb` merged.
  This is the one finding worth carrying forward from this test, and it is
  more concerning than the original (uncorrected) report's framing: a fix
  whose own commit message asserts the race is structurally impossible
  ("the activity then cannot complete before the wait loop gives up") failed
  the same way anyway, on its very next occurrence. Whatever produced the
  mismatched event history here cannot be "the activity finished before the
  timeout sweep noticed" — that path is closed by construction. The
  mechanism is unknown.

**Test-vs-product verdict: not rendered — and now for a narrower, more
consequential question.** Issue #1558 is corrected to ask specifically: given
`874784b9`'s config makes the original race provably impossible, what
produced the same wrong event-history shape anyway? Candidates not yet
distinguished: a *different* timeout type firing first (`ScheduleToStart` or
`Heartbeat`, both present in `timeout.rs` alongside `StartToClose`), the
timeout-enforcement sweep firing more than once for the same task, or an
interaction with this test's own 10-second `workflow_task_timeout` config
(also 10s, the same bound the wait loop uses — worth checking whether that is
coincidence or coupling). This session did not have the diagnostic dump
needed to tell which; the `matches!` assertion does not print the actual
event sequence on failure, so the next occurrence should capture
`history.events` before asserting, not just after it panics. One occurrence
is not a rate. No same-commit rerun was run this session.

### 2. Dashboard panel-id collision: the same-day "root cause" fix (#1550) has not recurred in ~7 hours of post-merge sample

4 of the 17 failures, plus one indirect case, were the already-known,
already-being-actively-fixed Grafana panel-id collision
(`dashboard_pack_docs::panel_structure_is_grafana10_clean`, panel id
duplicated) — trunk-dev's own tip had this bug repeatedly during this window
(runs `34794754199`, `34791486024`, `34776014650`, `34773156359`,
`34772990914`, `34772453914`, `34770812085`, all timestamped **before**
2026-09-14T02:29:41Z). That is not a new finding: commits `b3f8e84`,
`9fec854`, `e98a676` (merged at exactly that timestamp) already diagnosed and
fixed the recurring pattern, the last one adding
`docs/dashboards/next-panel-id.py` specifically so the collision stops
recurring by construction (drawing from a wide, mostly-empty id range instead
of hand-picking "the next" id).

One run in this sample, `34800863625` (02:56:13Z, **after** the root-cause
fix), carried a commit that *itself* was a port of the pre-fix collision fix
onto a branch that predated it (`"docs(dashboards): carry duplicate panel id
960 to 961 (CI red on #1551)"`) — a no-op once merged, correctly identified by
its own author as porting a fix that the base branch would soon carry
anyway. Its CI failure was unrelated (see item 4 below), not a recurrence of
the collision.

**No occurrence of the collision signature after 02:29:41Z** in this sample.
Given the sample only extends to 09:39:43Z (~7 hours post-fix) this is a
short window, not a clean bill of health, but it is a positive signal that
`next-panel-id.py` is holding rather than a fourth instance of "two PRs picked
the same id 37 seconds apart."

### 3. Two single-occurrence candidates — not clustered, not claimed as rates

- `retention_summary_tests::summary_gc_deletes_expired_and_emits_metric`
  (run `34799721202`, 02:35:29Z, `Test DB (linux, shard 0)`) — an `i64`
  `assert_eq!` failure at `retention_summary_tests.rs:572`. One occurrence.
- `autumn-harvest-sqlite`'s
  `fleet_fault_isolation::poll_once_as_of_drives_the_rest_of_the_fleet_past_an_unsupported_command`
  (run `34800863625`, 02:56:13Z, `Test (no-db, ubuntu-latest, shard 3)`). One
  occurrence.
- `webhook_receiver_integration::blank_delivery_id_is_rejected_as_missing_for_signals_target`
  (run `34772800206`, 17:50:26Z, `Test DB (linux, shard 10)`). One
  occurrence.

Per this role's own admissibility rules, a single occurrence is not a rate
and is not clustered with anything else in this sample — noted here so a
future session can check whether any of the three recurs, not routed forward
as an open item requiring action yet.

### 4. Remaining failures: deterministic, own-branch defects, not a CI-health signal

The other failures in the 17 were each a deterministic gate correctly doing
its job on that branch's own change, not suite nondeterminism: unused-import
compile errors (`partition.rs`), a `seeded_corpus_is_clean_under_the_syntactic_layer`
determinism-analysis failure, `cargo clippy`'s `large_futures` lint, and three
separate comment-hygiene Tier B regressions (`dev_runtime_lifecycle.rs`,
`poison_pill_reclaim_perf.rs`, `rate_limit_bucket_gc_tests.rs`) — each a real,
correct red on a PR that added a violation, not something this role's rerun
protocol or revert-check machinery applies to.

## 🔍 Diagnosis

**Item 1** splits into two: two occurrences are a stale-branch echo of a bug
already fixed by `7fbfebb` (no action needed — a rebase resolves it), and one
occurrence is a genuinely new, unexplained failure of the fix itself, on a
test configuration where the originally-diagnosed race is provably
impossible. The test-vs-product verdict for that third occurrence is
explicitly **not rendered** in this report — issue #1558 (corrected) carries
the evidence and the narrowed question instead of guessing which side to fix.
Per the hard gate, opening a fix PR without a same-commit rerun protocol and
a rendered verdict would be exactly the "retry in disguise" this role exists
to refuse — doubly so here, since the obvious-looking fix (widen the margin
further, or add another `sleep`) has already been tried once and did not
hold.

**Item 2** is not a new finding — it is confirmation that a same-day
root-cause fix already shipped by this repo's own maintainers is (so far)
holding.

**Item 3** is explicitly not claimed as flakes — one occurrence each, no
mechanism, no clustering. Recorded so a repeat is recognized as a repeat.

**Item 4** is the suite working correctly, not a CI-health defect.

## 🔧 Treatment

- **Filed, then corrected:** issue #1558 — originally described a 3-occurrence
  cluster with one candidate mechanism; edited after this PR's own review
  caught that two occurrences were a stale-branch echo of an already-fixed
  bug, to instead ask the narrower, more consequential question about the
  one occurrence on the already-hardened test.
- **No PR opened against `ci.yml` or any test** — correct per the hard gate,
  since no rerun protocol has been run and no verdict has been rendered for
  item 1, and items 2-4 need no suite change.

## 📊 Measurement

- **Item 1:** 3/3 occurrences confirmed same assertion via direct job-log
  inspection; 3/3 commit trees fetched and their `integration_e2e.rs` blob
  SHAs compared against `7fbfebb`'s diff, confirming 2 pre-hardening / 1
  post-hardening as stated above (not inferred from panic line numbers
  alone — the line numbers were what prompted checking, not what proved it).
  Not a same-commit rerun for either group — this is frequency-in-the-wild
  evidence per this role's own Tier distinctions, not a Tier-1 measured rate.
  1 occurrence of the post-hardening failure is offered as grounds to
  prioritize issue #1558's narrowed question, not as a rate claim.
- **Item 2:** 0/17 sampled failures after 2026-09-14T02:29:41Z (the
  root-cause fix's merge time) carried the collision signature, against 6
  direct occurrences before it in the same 17-hour window. ~7h post-fix
  sample only.
- **Item 3:** 1/1 for each of three distinct signatures — explicitly not a
  rate.
- No revert check applies — no fix in this report to verify red-then-green
  on.

## 🔬 Reproduce

```sh
# Full-window failure census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# on autumn-foundation/autumn-harvest — one page covered 2026-09-13T16:45:19Z
# through 2026-09-14T09:39:43Z (100 runs: 63 cancelled, 20 success, 17 failure).

# Per-failure job logs: get_job_logs(run_id=<id>, failed_only=true,
#   return_content=true, tail_lines=<40-150>) for each of the 17 failure run
# ids. Two tooling gaps hit while doing this, both worked around directly:
#   - get_job_logs's own job enumeration undercounts on runs with >30 jobs
#     (observed on run 34800863625: it reported "0 failed jobs" against a
#     30-job page while the run's actual 36-job list, fetched via
#     list_workflow_jobs with page=2, showed one real failure). Cross-check
#     with list_workflow_jobs(perPage=100) (or page through) when a run's
#     conclusion is "failure" but get_job_logs reports zero.
#   - A large job's `tail_lines` window can end before the actual panic line
#     for a big serial suite (observed on `integration_e2e`'s ~2800-test
#     module) -- get_job_logs(job_id=<id>, return_content=false) for the
#     signed logs_url, then curl it directly and grep, when tail_lines=500
#     still shows only "FAILED SUITES:" with no panic detail above it.

# Item 1 -- pre/post-hardening split (the correction):
git show 7fbfebb -- autumn-harvest/tests/integration/integration_e2e.rs
# diff header: index 8eeeb87..27509aa -- the pre/post blobs.
# Then, per occurrence commit, confirm which blob it carries:
# get_file_contents(owner, repo, path="autumn-harvest/tests/integration/integration_e2e.rs",
#   sha=<403c0652|97969f2f|874784b9>) -- each result's "successfully
#   downloaded text file (SHA: ...)" line names the blob directly.
python3 -c "
from datetime import datetime
a = datetime.fromisoformat('2026-09-13T21:45:08')
b = datetime.fromisoformat('2026-09-14T01:05:46')
print('pre-hardening pair span:', b - a)          # 3:20:38, not 3:11 as first reported
c = datetime.fromisoformat('2026-09-13T22:53:08')  # 7fbfebb merge (UTC)
d = datetime.fromisoformat('2026-09-13T23:56:26')  # post-hardening failure
print('fix merge to post-hardening failure:', d - c)
"

# Item 1 mechanism corroboration (still applies to the pre-hardening pair,
# not confirmed for the post-hardening occurrence):
grep -n "pool acquisition exceeded the tick interval" \
  <(curl -s "$SIGNED_LOG_URL_FOR_A_PASSING_workflow_retry_tests_LEG")
sed -n '4594,4650p' autumn-harvest/src/timeout.rs
sed -n '3386,3557p' autumn-harvest/tests/integration/integration_e2e.rs

# Item 2 fix-holding check:
git log --oneline -- docs/dashboards/starter-pack-v0.1.0.json | head -8
# e98a676's merge timestamp (2026-09-13T21:29:41-05:00 = 2026-09-14T02:29:41Z)
# vs. the timestamps of the 17 sampled failures.

# search_issues("worker_fails_workflow_when_activity_start_to_close_timeout_elapses flaky")
# on autumn-foundation/autumn-harvest before filing #1558 -- 5 hits, all
# issue #1459 (a different test), confirming this is not a re-report.
```
