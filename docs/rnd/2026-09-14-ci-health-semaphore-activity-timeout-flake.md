# 🚦 Semaphore CI health — a new clustered flake
# (`worker_fails_workflow_when_activity_start_to_close_timeout_elapses`),
# the dashboard panel-id root-cause fix holding, and two single-occurrence
# candidates not yet worth a rate claim

**Status:** health report / issue filed (#1558) — no PR opened against `ci.yml`,
no test changed. Continues the series in `docs/rnd/2026-09-0[3-8]-ci-health-
semaphore*.md` and `docs/rnd/2026-09-1[1-3]-ci-health-semaphore*.md`.

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

### 1. New clustered flake: `integration_e2e::worker_fails_workflow_when_activity_start_to_close_timeout_elapses` — 3/17 occurrences, 3 branches, 3h11m span

| Run | Job | When (UTC) | Branch | Commit | Panic line |
|---|---|---|---|---|---|
| `34781980082` | Test DB (linux, shard 0) | 21:45:08 | `claude/gracious-noether-a7mahx` | `403c0652` | `integration_e2e.rs:3529:5` |
| `34787239282` | Test DB (linux, shard 0) | 01:05:46 | `claude/vibrant-hamilton-s6oknw` | `97969f2f` | `integration_e2e.rs:3529:5` |
| `34789424377` | Test DB (linux, shard 7) | 23:56:26 | `claude/hopeful-rubin-rlkl1n` | `874784b9` | `integration_e2e.rs:3541:5` |

Identical assertion in all three (the line number differs only because
unrelated edits elsewhere in the file shifted it on those branches):

```
assertion failed: matches!(history.events.as_slice(),
    [WorkflowEvent::WorkflowStarted { .. }, WorkflowEvent::ActivityScheduled
    { .. }, WorkflowEvent::ActivityStarted { .. },
    WorkflowEvent::ActivityTimedOut
    { timeout_type: TimeoutType::StartToClose, .. },
    WorkflowEvent::WorkflowFailed { .. },])
```

In all three the test's first assertion (`execution.state == "FAILED"`,
polled for up to 10s) already passed — only the exact 5-event history-shape
check afterward failed. Filed as **issue #1558** with the full mechanism
candidate; not summarized again here beyond the headline, per this role's own
rule against re-litigating a filed finding in a second document. Not
previously reported by this series (checked via `search_issues` before
filing — the 5 existing matches are all issue #1459's ten-child-fan-out
test, a different test).

**Test-vs-product verdict: not rendered.** 3 occurrences across 3 unrelated
branches in 3h11m is enough to call this a real cluster, not enough to say
which side the nondeterminism lives on — the hard gate's own "must show the
nondeterminism lives in the test, not the product, before touching either"
requirement is not met by frequency-in-the-wild data alone. Corroborating
(not confirming) evidence for a wall-clock-margin mechanism: the test uses a
100ms `default_start_to_close` against a 25ms worker `poll_interval`, and the
timeout-enforcement sweep that detects `StartToClose` breaches
(`autumn-harvest/src/timeout.rs:4594-4644`) explicitly skips a tick under
pool contention, logging `"pool acquisition exceeded the tick interval;
skipping this tick"` (`timeout.rs:4645`) — a line seen firing routinely in
this same 17-hour sample's **passing** `workflow_retry_tests` legs (at
`interval=50ms`), so tick-skipping under load is confirmed to be happening on
these runners generally, just not yet confirmed as the specific cause of
*this* test's failure. No same-commit rerun was run this session — see 📋 in
issue #1558 for the requested follow-up before any fix.

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

**Item 1** is a genuine new cluster (3 same-signature failures, 3 unrelated
branches, 3h11m span) but the test-vs-product verdict is explicitly **not
rendered** in this report — filed as issue #1558 with the evidence and a
named candidate mechanism instead of guessing which side to fix. Per the hard
gate, opening a fix PR without a same-commit rerun protocol and a rendered
verdict would be exactly the "retry in disguise" this role exists to refuse.

**Item 2** is not a new finding — it is confirmation that a same-day
root-cause fix already shipped by this repo's own maintainers is (so far)
holding.

**Item 3** is explicitly not claimed as flakes — one occurrence each, no
mechanism, no clustering. Recorded so a repeat is recognized as a repeat.

**Item 4** is the suite working correctly, not a CI-health defect.

## 🔧 Treatment

- **Filed:** issue #1558 (the new clustered flake, with mechanism candidate
  and requested rerun-protocol follow-up).
- **No PR opened against `ci.yml` or any test** — correct per the hard gate,
  since no rerun protocol has been run and no verdict has been rendered for
  item 1, and items 2-4 need no suite change.

## 📊 Measurement

- **Item 1:** 3/3 occurrences confirmed same assertion, same event-history
  shape check, via direct job-log inspection (not inferred from job
  conclusion alone). Not a same-commit rerun — 3 different commits — so this
  is frequency-in-the-wild clustering evidence per this role's own Tier
  distinctions, not a Tier-1 measured rate. Offered as grounds to prioritize
  issue #1558's requested rerun protocol, not as a standalone rate claim.
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

# Item 1 mechanism corroboration:
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
