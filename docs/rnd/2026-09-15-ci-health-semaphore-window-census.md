# 🚦 Semaphore CI health — fresh 16-hour census: known flake recurs twice
# more (still pre-fix), a security-advisory gate's stale-branch echo, no new
# signals

**Status:** health report, evidence added to existing issue #1558 — no PR
opened against `ci.yml`, no test changed. Continues the series in
`docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md` and
`docs/rnd/2026-09-1[1-4]-ci-health-semaphore*.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request`/`push`
triggers against `trunk`/`trunk-dev`, principally the `test-db-linux` (11
shards) and `test`/`test-nodb` matrices, plus the `Dependency ledger
(cargo-deny)` advisories gate.

## 🌡️ Symptom

Sampled the 100 most recent completed `pull_request`-event `ci.yml` runs
(`actions_list(method="list_workflow_runs", ...)`), covering
2026-09-14T17:01:38Z through 2026-09-15T09:13:43Z (~16 hours): 66 cancelled,
17 success, **17 explicit failure**. Job-logged all 17 (`get_job_logs`,
`failed_only`, falling back to `list_workflow_jobs(perPage=100)` plus a
direct `curl` of the signed `logs_url` when `get_job_logs`'s own job
enumeration undercounted — this tooling gap, first noted in the 09-14
report, recurred twice this session on the same two `Test DB (linux, shard
0)` jobs discussed below).

### 1. `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`: two more occurrences, both pre-#1563

Two of the 17 failures are the exact flake tracked in issue #1558 (and now
being fixed in open PR #1563):

| Run | Branch | Commit | When (UTC) |
|---|---|---|---|
| `34888311292` | `claude/issue-1555-tdd-refactor-26zumg` | `d4238f90` | 2026-09-14T20:44:09Z |
| `34922392508` | `claude/gracious-noether-5mk8d5` (PR #1576) | `cb150a47` | 2026-09-15T03:47:14Z |

Both panicked at the identical pre-#1563 assertion
(`integration_e2e.rs:3541:5`); both branches predate #1563 landing on
`trunk-dev` (#1563 is still open). Full detail and the tooling-gap
workaround posted to issue #1558 directly (comment
[5678456772](https://github.com/autumn-foundation/autumn-harvest/issues/1558#issuecomment-5678456772))
rather than duplicated here. **Total known occurrences of this assertion:
5.** This session did not run a same-commit rerun campaign against a commit
carrying #1563's fix — that verification is still owed before #1563's own
hard-gate measurement section can be called complete, and is #1563's own
work to finish, not a new item this report opens.

### 2. `Dependency ledger (cargo-deny)`: RUSTSEC-2026-0285, a stale-branch echo already fixed on `trunk-dev`

3 of the 17 failures (`34879949306` 18:18:00Z, `34875349496` 17:32:17Z,
`34874959976` 17:28:27Z) failed `cargo-deny check advisories` on
**RUSTSEC-2026-0285** (a rustls TLS 1.3 handshake vulnerability, fixed
upstream in rustls `>=0.23.45`; `Cargo.lock` on the affected branches still
pinned `0.23.40`). `git log -p -- Cargo.lock` shows the bump
(`0.23.40`→`0.23.45`) landed on `trunk-dev` in `05c3826` at
2026-09-14T19:34:02Z, incidental to an unrelated PR (#1564). All three
failures above are timestamped **before** that merge. `trunk-dev`'s current
`Cargo.lock` already carries `0.23.45` (confirmed directly:
`git show origin/trunk-dev:Cargo.lock | grep -A2 'name = "rustls"'`). Same
shape as this series' own recurring stale-branch-echo finding (see the
09-14 report's item 1): the gate is correctly, deterministically failing on
branches that have not rebased past the fix. **No action needed** — a
rebase resolves it for each affected branch, and no currently-open PR was
found still carrying `0.23.40` as of this pass (not exhaustively checked
against every open PR's own `Cargo.lock`; routed forward only as a
"if this recurs after checking rebase status, escalate" note, not a
confirmed open gap).

### 3. Remaining 12 failures: own-branch WIP, not a CI-health signal

The rest were deterministic gates correctly failing on each branch's own
in-progress change: a `diesel` import/feature-gating compile error in
`partition.rs` recurring across 4 pushes to the same branch
(`claude/bold-lovelace-agnczk`, issue #1270 work), a comment-hygiene Tier B
regression (`shard_rebalance.rs`, one sentence over 25 words), a
`clippy::redundant_clone` lint catch, and 3 pushes to
`claude/determined-brahmagupta-j0vmw3` plus 2 to
`claude/issue-1555-tdd-refactor-26zumg` iterating on their own fixes (one of
the latter's failures is item 1 above; the rest are a Windows-only
`fleet_fault_isolation::poll_once_still_attempts_every_broken_execution_in_one_pass`
failure not otherwise investigated this session — single occurrence, not
clustered with anything, noted here so a repeat is recognized as a repeat
rather than re-diagnosed from scratch).

No occurrence of the previously-tracked Grafana dashboard panel-id
collision (`docs/rnd/2026-09-14-...md` item 2) in this window.

## 🔍 Diagnosis

**Item 1** is not a new mechanism — it is two more real-world confirmations
of the already-diagnosed, already-being-fixed race in issue #1558/PR #1563.
Test-vs-product verdict already rendered in #1563 (test-side: the race
between `claim_task`'s `started_at` stamp and
`append_activity_started_if_pending` is real but both resulting event
shapes are correct engine behavior). Not re-litigated here.

**Item 2** is a stale-branch echo of an already-merged fix, structurally
identical to this series' recurring finding about the activity-timeout
test's pre-hardening pair and the dashboard panel-id collision: a
deterministic gate correctly red on branches whose tree predates the fix.

**Item 3** is the suite working correctly, not a CI-health defect.

## 🔧 Treatment

- Evidence for item 1 added to issue #1558 as a comment rather than a new
  issue — it is the same tracked defect, not a new one.
- No PR opened against `ci.yml` or any test. Per the hard gate: item 1's fix
  already exists and is under review in #1563; item 2 and item 3 need no
  suite change.

## 📊 Measurement

- **Item 1:** 2/2 new occurrences confirmed via direct job-log inspection
  (with the `get_job_logs` undercount workaround); both pre-#1563 by branch
  ancestry, not merely by timestamp. Not a same-commit rerun — frequency-
  in-the-wild evidence per this role's Tier distinctions, added to the
  existing n=3 for a running total of n=5. No rerun campaign run this
  session; that remains owed on #1563 before it can claim the hard gate's
  after-measurement.
- **Item 2:** 3/3 failures confirmed to share the `RUSTSEC-2026-0285`
  signature via direct log inspection; all 3 timestamps confirmed before
  the fix commit's merge timestamp (not just "before now") via
  `git log -p -- Cargo.lock` and `git show <fix-commit> -- Cargo.lock`.
- No revert check applies — no fix in this report to verify red-then-green
  on.

## 🔬 Reproduce

```sh
# Full-window failure census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# on autumn-foundation/autumn-harvest — 2026-09-14T17:01:38Z through
# 2026-09-15T09:13:43Z (100 runs: 66 cancelled, 17 success, 17 failure).

# Per-failure job logs, with the undercount workaround:
# get_job_logs(run_id=<id>, failed_only=true, return_content=true, tail_lines=80)
# then, when it reports 0 failed jobs against a run whose conclusion is
# "failure": actions_list(method="list_workflow_jobs", resource_id=<run_id>,
# perPage=100) to find the actual failed job id, then
# get_job_logs(job_id=<id>, return_content=false) for the signed logs_url,
# curl it directly, and grep for "... FAILED$" / "thread '.*' panicked".
# Hit this on runs 34888311292 and 34922392508 (item 1), both 30-job runs.

# Item 2's fix-timing check:
git log -p --all -- Cargo.lock | grep -B5 '^\+version = "0.23.45"'
git show -s --format="%H %cI %s" 05c3826a0cb6b2503776a5f15b187f0a06845b8e
git show origin/trunk-dev:Cargo.lock | grep -A2 '^name = "rustls"$'

# search_issues("RUSTSEC-2026-0285") on this repo before writing this
# report -- 0 hits, confirming this is not a re-report of a filed advisory
# gap.
```
