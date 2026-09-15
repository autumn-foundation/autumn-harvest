# 🚦 Semaphore CI health — fresh 16-hour census: known flake recurs twice
# more (still pre-fix), a security-advisory gate's stale-branch echo (8
# occurrences, not 3), an already-fixed SQLite flake's last occurrence, no
# confirmed new signal

**Corrected after Codex review on PR #1580 (one pass, three findings).** The first
version of this report (a) claimed "no occurrence of the dashboard
panel-id collision in this window" from only the 17 explicit failures,
leaving the 66 cancelled runs unaudited — this series' own 09-11 report
found cancelled runs can hide real failures, so that claim is narrowed
below; (b) miscounted the `RUSTSEC-2026-0285` cargo-deny cluster as 3
occurrences when it was 8, because 9 of the 17 failing runs were never
individually job-logged, only inferred by branch-name pattern; and (c)
filed a Windows-only `fleet_fault_isolation` failure as "not otherwise
investigated" when it is in fact the last, already-fixed occurrence of a
flake diagnosed and fixed (20,000-try bound) later in that same PR's own
commit history (`c652867`). All 17 failing runs are now individually
job-logged below; every category is a verified count, not an inference
from a branch name.

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

### 2. `Dependency ledger (cargo-deny)`: RUSTSEC-2026-0285, a stale-branch echo already fixed on `trunk-dev` — 8 occurrences, not 3

**Corrected count.** 8 of the 17 failures failed `cargo-deny check
advisories` on **RUSTSEC-2026-0285** (a rustls TLS 1.3 handshake
vulnerability, fixed upstream in rustls `>=0.23.45`; `Cargo.lock` on the
affected branches still pinned `0.23.40`):

| Run | Branch | When (UTC) |
|---|---|---|
| `34883968010` | `claude/bold-lovelace-agnczk` | 18:57:37Z |
| `34879949306` | `claude/determined-brahmagupta-j0vmw3` | 18:18:00Z |
| `34877128464` | `claude/optimistic-sagan-qunu40` | 17:50:08Z |
| `34876736763` | `claude/determined-brahmagupta-j0vmw3` | 17:46:06Z |
| `34875349496` | `claude/tender-planck-a500l0` | 17:32:17Z |
| `34874959976` | `claude/sleepy-volta-pjudme` | 17:28:27Z |
| `34874351972` | `claude/determined-brahmagupta-j0vmw3` | 17:22:26Z |
| `34873735733` | `claude/issue-1555-tdd-refactor-26zumg` | 17:16:21Z |

`git log -p -- Cargo.lock` shows the bump (`0.23.40`→`0.23.45`) landed on
`trunk-dev` in `05c3826` at 2026-09-14T19:34:02Z, incidental to an unrelated
PR (#1564). All 8 failures above are timestamped **before** that merge.
`trunk-dev`'s current `Cargo.lock` already carries `0.23.45` (confirmed
directly: `git show origin/trunk-dev:Cargo.lock | grep -A2 'name =
"rustls"'`). Same shape as this series' own recurring stale-branch-echo
finding (see the 09-14 report's item 1): the gate is correctly,
deterministically failing on branches that have not rebased past the fix.
**No action needed** — a rebase resolves it for each affected branch, and
no currently-open PR was found still carrying `0.23.40` as of this pass
(not exhaustively checked against every open PR's own `Cargo.lock`; routed
forward only as a "if this recurs after checking rebase status, escalate"
note, not a confirmed open gap).

### 3. The SQLite `start_after` flake's last occurrence — already diagnosed, already fixed, in the same PR

Run `34888311292` (`claude/issue-1555-tdd-refactor-26zumg`, commit
`d4238f90`, 2026-09-14T20:44:09Z) failed
`fleet_fault_isolation::poll_once_still_attempts_every_broken_execution_in_one_pass`
on Windows with `did not land an execution after the target one in 50
tries`. This is not an unexplained occurrence: this exact branch's own
later commit (`c652867`, merged 2026-09-14T23:27:13Z, 2h43m after this
failure) root-causes it in its own commit message — `ExecutionId::new()`
fixes the id's first two bytes to the `ShardId::UNENCODED` sentinel, so a
~1-in-256 draw for the `after` argument collapses a fresh candidate's odds
of sorting higher, and 50 tries is not a safe bound for that case — and
fixes it by raising `start_after`'s retry bound to 20,000
(`autumn-harvest-sqlite/tests/integration/fleet_fault_isolation.rs:129`,
confirmed already at `20_000i64` on `trunk-dev`), verified there with 20
consecutive full-suite runs at 0 failures. This run is simply the last
live occurrence of that already-fixed flake, on the same PR that fixed it,
before the fix commit landed. No new action needed.

### 4. Own-branch WIP: 6 failures, at least 8 distinct own-code signatures across 4 branches

The rest were deterministic gates correctly failing on each branch's own
in-progress change — not a CI-health signal, but itemized here (rather
than summarized by branch-name pattern) since an earlier draft of this
report undercounted this bucket by inferring repeat branches shared one
signature without checking each push individually:

- `claude/bold-lovelace-agnczk` (issue #1270 work), 5 failing pushes, at
  least 4 distinct signatures across them: a `diesel` import/feature-gating
  compile error in `partition.rs` (`34925551916`, `34924353340`, identical
  both times), an unrelated `dead_code` compile error
  (`DISABLE_RENAME_SUFFIX`, `34891509704`), an `rustfmt` diff in
  `event_partitioning_tests.rs` (`34888038964`, `34883968010`, identical
  both times, the second run adding cargo-deny per item 2 above), and a
  `seeded_corpus_is_clean_under_the_syntactic_layer` determinism-analysis
  failure recurring across 3 of the 5 pushes (`34891509704`,
  `34888038964`, `34883968010`) — not otherwise diagnosed this session,
  noted so a future pass recognizes the repeat rather than re-investigating
  from zero.
- `claude/determined-brahmagupta-j0vmw3` (dev/reaper.rs work), 3 failing
  pushes: a comment-hygiene CH007 regression (`34876736763`) and a
  `clippy::too_many_lines` catch (`34874351972`) in `reaper.rs`, both
  pushes also carrying item 2's cargo-deny failure; the third push
  (`34879949306`) failed only on cargo-deny.
- `claude/issue-1555-tdd-refactor-26zumg`'s earliest push (`34873735733`,
  17:16:21Z) failed only on item 2's cargo-deny cluster — its later push
  (`34888311292`) is items 1 and 3 above.
- `claude/gracious-carson-lygokt` (codec envelope PR #1569): a
  `clippy::redundant_clone` catch in `payload_codec.rs` (`34891219422`).
- `claude/pensive-brahmagupta-htddux` (Bolt PR #1577): a comment-hygiene
  CH007 regression in `shard_rebalance.rs` (`34933650236`).
- `claude/sharp-feynman-w310nk` (codec-rotation deadline PR #1573): a
  `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree`
  failure (`34883440260`) — a docs/code total-count mismatch this PR's own
  migration likely introduced; not previously listed in this report at all,
  found only once every run was individually job-logged. Single occurrence,
  own-branch, not investigated further this session.

**Run-count check.** Some runs failed more than one job, so items 1-4's
tables/lists overlap by run id (not by count): item 1's 2 runs
(`34888311292`, `34922392508`); item 3 is `34888311292` again (a second,
distinct failed job on that same run); item 2's 8 cargo-deny runs, 5 of
which (`34883968010`, `34879949306`, `34876736763`, `34874351972`,
`34873735733`) also carry a second, distinct own-code failure listed under
item 4, and 3 of which (`34877128464`, `34875349496`, `34874959976`) failed
*only* on cargo-deny; item 4's remaining rows
(`34925551916`, `34924353340`, `34891509704`, `34888038964`,
`34891219422`, `34933650236`, `34883440260`) each failed on an own-code
issue with no cargo-deny component. Counting each of the 17 run **ids**
once regardless of how many jobs it failed: 2 (item 1, `34888311292` +
`34922392508`) + 3 (item 2's cargo-deny-only runs) + 12 (item 4's distinct
run ids, five of which double as item 2 rows) = 17, with `34888311292`
counted once under item 1 despite also being item 3. Every one of the 17
run ids sampled this session (`34933650236`, `34925551916`, `34924353340`,
`34922392508`, `34891509704`, `34891219422`, `34888311292`, `34888038964`,
`34883968010`, `34883440260`, `34879949306`, `34877128464`, `34876736763`,
`34875349496`, `34874959976`, `34874351972`, `34873735733`) was
individually job-logged, not inferred from a branch name.

**No occurrence of the previously-tracked Grafana dashboard panel-id
collision among the 17 explicit failures** (`docs/rnd/2026-09-14-...md`
item 2) in this window. That is narrower than "did not recur": the window
also held 66 cancelled runs, none of which this session job-logged, and
this series' own 09-11 report found cancelled runs can hide a real failure
underneath their overall conclusion (15/54, 28%, in that sample). Routed
forward, not claimed as confirmation, per that same gap this series has
hit twice before.

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
8, not 3, once every failing run was individually job-logged instead of
inferred from a repeated branch name.

**Item 3** is the same stale-commit-echo shape as item 2, just within one
branch's own iteration rather than needing a rebase against `trunk-dev`:
the fix landed later in the same PR that hit the flake.

**Item 4** is the suite working correctly, not a CI-health defect — but
undercounted in this report's first draft by assuming repeat branch names
meant repeat signatures, which items 2-4 above show was not reliably true
(`bold-lovelace-agnczk`'s 5 pushes alone carry at least 4 distinct
signatures).

## 🔧 Treatment

- Evidence for item 1 added to issue #1558 as a comment rather than a new
  issue — it is the same tracked defect, not a new one.
- No PR opened against `ci.yml` or any test. Per the hard gate: item 1's fix
  already exists and is under review in #1563; items 2-4 need no suite
  change.
- This report itself corrected after Codex review (see the note at top):
  the dashboard-collision claim narrowed to "among audited failures," the
  cargo-deny count corrected 3→8 with every occurrence individually
  verified, and the SQLite `start_after` failure reclassified from
  "uninvestigated" to "already fixed later in the same PR."

## 📊 Measurement

- **Item 1:** 2/2 new occurrences confirmed via direct job-log inspection
  (with the `get_job_logs` undercount workaround); both pre-#1563 by branch
  ancestry, not merely by timestamp. Not a same-commit rerun — frequency-
  in-the-wild evidence per this role's Tier distinctions, added to the
  existing n=3 for a running total of n=5. No rerun campaign run this
  session; that remains owed on #1563 before it can claim the hard gate's
  after-measurement.
- **Item 2:** 8/8 failures confirmed to share the `RUSTSEC-2026-0285`
  signature via direct log inspection of every one of the 8 runs (not
  inferred from branch name); all 8 timestamps confirmed before the fix
  commit's merge timestamp (not just "before now") via `git log -p --
  Cargo.lock` and `git show <fix-commit> -- Cargo.lock`.
- **Item 3:** 1/1 occurrence confirmed against the fix commit's own message
  and the current value of `start_after`'s retry bound on `trunk-dev`
  (`20_000i64`, matching the fix commit's stated change and its own
  20-consecutive-run verification, which this session did not independently
  rerun).
- **Item 4:** all 12 remaining run-rows individually job-logged; the
  run-count check above accounts for all 17 total failures against items
  1-4 with no run left uncategorized and none double-counted.
- No revert check applies — no fix in this report to verify red-then-green
  on.

## 🔬 Reproduce

```sh
# Full-window failure census:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# on autumn-foundation/autumn-harvest — 2026-09-14T17:01:38Z through
# 2026-09-15T09:13:43Z (100 runs: 66 cancelled, 17 success, 17 failure).

# Per-failure job logs, with the undercount workaround -- run against all
# 17 run ids listed in items 1-4 above, not a sample:
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

# Item 3's already-fixed check:
git show -s --format="%H %cI %s" c65286713cc8cf8d0d2a55fa72151821e64a4709
sed -n '108,136p' autumn-harvest-sqlite/tests/integration/fleet_fault_isolation.rs
# confirms start_after's retry bound is 20_000i64 on trunk-dev, matching
# c652867's own commit message ("Fix a real, provable flake in
# start_after's 50-try bound").

# search_issues("RUSTSEC-2026-0285") on this repo before writing this
# report -- 0 hits, confirming this is not a re-report of a filed advisory
# gap.
```
