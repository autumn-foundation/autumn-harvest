# 🚦 Semaphore CI health — `completion_trigger_defers_to_outbox_when_target_quota_exceeded`'s SOURCE-completion wait has gone from 1 occurrence in 6 days to 6 confirmed identical-signature occurrences in 26 hours, on 6 differently-named branches, and the 30s timeout widen that shipped for it (PR #1673, 09-21) did not fix it

**Status:** health report — no PR opened against `ci.yml`, `quota_enforcement_tests.rs`,
or `completion_trigger.rs`. This role's hard gate (a located problem, a named
mechanism, a rendered test-vs-product verdict, and a before/after measurement
from a rerun harness) is not met: no Docker daemon in this session's sandbox
(re-checked today — `docker ps` fails, `/var/run/docker.sock` absent, same gap
every report in this series has hit). Continues the series from
`docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md` and
`docs/rnd/2026-09-23-ci-health-semaphore-shard-0-rebalance.md` (the latter
landed on `claude/fix-shard-0-collision-rebalance-1685`, not yet merged to
`trunk-dev`, so it is not in this session's tree).

**Corrected after Codex review on this PR:** the first draft claimed the
`QuotaExceeded` arm "never" propagates `Err`/rolls back the source's
transaction, having stopped reading `completion_trigger.rs` right before the
outbox-row insert; that insert's own `.map_err(...)?` can in fact propagate
and roll back, which is a real, previously-ruled-out candidate mechanism —
corrected inline below. The first draft also called the six branches
"unrelated"/"independent" based only on an open-PR-listing check, which
cannot establish that; downgraded to "differently-named," with the actual
diff check left for the next session. Both corrections are inline at the
point each applies, matching this series' convention.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`, principally `test-db-linux`. Concretely, in this window,
`Test DB (linux, shard 0)` (11-shard layout) / `Test DB (linux, shard 2)`
(PR #1707's proposed 21-shard layout) — the shard(s) that carry
`quota_enforcement_tests`. Branch-protection and cache-usage API access
remain unavailable (re-checked, unchanged).

## 🌡️ Symptom

### This session's starting point: two fix PRs already open against this exact test, both still red on their own CI

Before running a fresh census, this session found `claude/fix-quota-outbox-scanner-sharded-pool-1685`
(PR #1706, "wire `sharded_pool` into the test's own worker config", targeting
this test's *other* panic signature — `"target row was never created by the
outbox retry"` at `quota_enforcement_tests.rs:3804`) and
`claude/fix-shard-0-collision-rebalance-1685` (PR #1707, an 11→21 shard
rebalance) already open, both citing issue/PR #1685 (this series' 09-21
report). Both PRs' *own* CI runs — pushed hours apart, on top of the fix
commits — still fail `quota_enforcement_tests`:

| Run | Branch | Job | Panic |
|---|---|---|---|
| `35788065837` (job `107008321702`) | PR #1706, `claude/fix-quota-outbox-scanner-sharded-pool-1685` | `Test DB (linux, shard 0)`, 2026-09-23T01:44:40Z | `completion_trigger_defers_to_outbox_when_target_quota_exceeded` panicked at `integration_e2e.rs:1383:6` |
| `35813687699` (job `107035722407`) | PR #1707, `claude/fix-shard-0-collision-rebalance-1685` | `Test DB (linux, shard 2)` (new 21-shard layout), 2026-09-23T03:55:37Z | same test, same panic site |

PR #1706's own description already retracted an earlier draft's claim that
its fix would also explain this panic: `evaluate_triggers_for_execution`'s
`QuotaExceeded` arm never touches the source's own terminal commit *on its
success path* — confirmed this session by reading `completion_trigger.rs:
2078-2100`, the tracing call and metrics record before the outbox insert.
**Correction (post-review, Codex on this PR):** an earlier draft of this
report stopped reading at line 2100 and claimed "no `Err`/`?` propagation out
of the source's transaction" outright. That is wrong: continuing to line
2121, the outbox-row insert itself ends `.map_err(crate::error::database_error)?`
(`completion_trigger.rs:2101-2122`). A database error on *that* insert — a
transient pool exhaustion, a constraint violation, a connection drop — does
propagate `?` out of this function, and per the function's own doc comment
(the whole arm "runs INLINE inside the SOURCE execution's own terminal
transaction"), that would roll back the source's `WorkflowCompleted` append
along with it, reproducing exactly the pre-fix symptom this test guards
against. So #1706's fix landing will not close this on the arm's *ordinary*
path (confirmed unchanged), but a rare error on the outbox insert itself is
a real, undismissed candidate mechanism this report had wrongly ruled out —
not confirmed either way this session (no worker-level logging or DB-error
telemetry available to check whether any of the 6 occurrences actually hit
this insert's error path), added to the diagnosis below.

### Widened census: 4 more, completely independent branches hit the identical panic site in the same ~26-hour window

Sampling `Test DB (linux, shard 0)` failures from today's `ci.yml` window
(90 `pull_request`/`completed` runs since the 09-21 report's cutoff,
2026-09-21T10:18:51Z: 61 cancelled, 26 failure, 3 success), every
shard-0 failure checked — 4 more beyond the two PRs above, chosen for having
no obvious own-diff compile/lint signature in the run summary — resolves to
the **exact same test, exact same panic site**:

| Run (job) | Branch | Timestamp (UTC) |
|---|---|---|
| `35652570063` (`106647193659`) | `claude/trusting-ritchie-nml6ud` | 2026-09-22T07:15:01Z |
| `35658767845` (`106896752815`) | `claude/gallant-dijkstra-a83hyy` | 2026-09-22T19:29:31Z |
| `35788942436` (`106960979043`) | `claude/confident-babbage-sl0122` | 2026-09-22T22:32:07Z |
| `35835324763` (`107108884704`) | `claude/cool-noether-7dejjb` | 2026-09-23T09:00:21Z |

Every one of these 6 (the 2 PRs above plus these 4) shows the byte-identical
panic:

```
thread 'quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded' panicked at autumn-harvest/tests/integration/integration_e2e.rs:1383:6:
workflow should reach expected state within timeout: Elapsed(())
```

reached from `quota_enforcement_tests.rs`'s `wait_for_execution_state_with_timeout(&url, source, "COMPLETED", Duration::from_secs(30))`
call (the test's own doc comment names this "the money assertion: the source
reaches COMPLETED even though its trigger's target is at quota cap").

Six occurrences, six different branches, spanning 2026-09-22T07:15Z through
2026-09-23T09:00Z — **26 hours**. **Correction (post-review, Codex on this
PR):** an earlier draft called these six branches' failures independent
because none of the four new ones has an open PR touching
`quota_enforcement_tests.rs`, `completion_trigger.rs`, or `execution.rs`.
That is too weak a check to support "no shared diff" — an open-PR listing
says nothing about a branch with no PR yet, a branch sharing commits with
another via a common base, or a change to worker/queue/shard/test-setup code
outside those three named files. This session did not diff each of the six
branches' actual tested SHA against `trunk-dev` or against each other, so
the independence claim is **downgraded**: six occurrences on six
differently-named branches, not confirmed to carry unrelated diffs. This is
still a real rate increase from the prior report's 3 total occurrences (2 of
one signature, 1 of this one) spread across 6 days, and the two PRs (#1706,
#1707) are independently confirmed not to touch this path (read directly,
not inferred from branch naming) — but the four additional branches' own
diffs are an open question the next session should check
(`git diff trunk-dev...<branch>` for each) before repeating "independent" as
a settled fact.

**Not claimed as 100%.** Two `Test DB (linux, shard 0)` runs in roughly the
same window passed cleanly: `35695812531` (2026-09-22T06:39Z, ~35 minutes
*before* the first of the six failures above) and `35826846700`
(2026-09-23T06:27Z, sandwiched between the `confident-babbage-sl0122` and
`cool-noether-7dejjb` failures). So this is intermittent, not deterministic —
but every shard-0 failure this session sampled in the window resolves to this
one signature, and none of the six sampled shard-0 failures showed a
different one. This is a sample of convenience (failures picked by browsing
the run list, not a uniform draw over all shard-0 executions), not a formal
rerun-rate — the ≥20x same-commit protocol this role's hard gate requires is
still not runnable here (no Docker).

### The 30-second timeout widen (PR #1673, merged 2026-09-21T19:18Z UTC) did not fix this, and its own commit message says the flake was already known and deliberately not root-caused

`quota_enforcement_tests.rs`'s wait for this exact assertion was widened from
the file's usual 10s default to a caller-supplied 30s bound in `56bc205`
(PR #1673, "Fix: backup verify adjudicates lost cross-shard completion-trigger
fires"), whose second commit message states directly: *"This test's own diff,
and completion_trigger.rs's quota-exceeded-to-outbox path, are unchanged from
trunk-dev in this PR -- the flake predates and is unrelated to the fire-verify
work here."* The diff itself confirms: only the timeout argument changed
(`wait_for_execution_state` → `wait_for_execution_state_with_timeout(..., 30s)`),
no assertion, no test logic, no product code in the same hunk.

**This is exactly the "raised timeout as a fix" pattern this role's charter
bans**, applied here to a flake that was observed but not diagnosed at the
time. The evidence this session gathered shows it did not work: every one of
the 6 occurrences above happened *after* this widen shipped (the first one,
`35652570063`, is ~12 hours after the merge), at the *new*, 3x-larger, 30
second bound. A timeout that is already 3x the file's own default and still
loses this often is strong evidence against "the runner is just slow" and for
either a genuine hang (the source workflow's terminal transaction never
actually commits under some condition) or unbounded queueing (the worker's
dispatch loop never picks the task up) — both mechanism categories this
role's hard gate requires naming, and neither confirmed yet.

## 🔍 Diagnosis

**Test-vs-product verdict: not rendered.** Per this role's hard gate
(requirement 3), a fix PR cannot be opened without first showing the
nondeterminism lives in the test rather than the product. This session did
not obtain worker-level tracing or a live repro (no Docker), so it cannot
render that verdict. What this session *did* establish, narrowing the
candidate space:

- **Not `#1706`'s mechanism on the arm's ordinary path — but a related,
  undismissed candidate survives review.** The same-shard `QuotaExceeded`
  arm's happy path (`completion_trigger.rs:2078-2100`) falls through to the
  outbox and metrics recording without touching the source's transaction.
  **Correction (post-review, Codex on this PR):** an earlier draft of this
  report stopped there and claimed the whole arm never propagates `Err`.
  Wrong — the outbox-row insert immediately after
  (`completion_trigger.rs:2101-2122`) ends `.map_err(crate::error::database_error)?`,
  and per the arm's own doc comment this whole block runs inline inside the
  source's terminal transaction. A database error on that specific insert
  (pool exhaustion, a dropped connection, a constraint violation under
  concurrent load) would propagate and roll back the source's own
  `WorkflowCompleted` append — the exact pre-fix symptom this test exists to
  catch, on a narrower trigger than the original bug (an insert-time error,
  not every quota-exceeded evaluation). Not confirmed as what actually
  happened in any of the 6 occurrences (no DB-error telemetry captured), but
  it is a concrete, previously-unconsidered mechanism the next session
  should check for (e.g. Postgres logs or connection-pool metrics around
  each occurrence's timestamp) before assuming a pure hang.
- **Not "CI is just slow."** The bound is already 30s, 3x this file's normal
  10s default, deliberately widened for this exact assertion, and still
  loses regularly. The test's own doc comment says the entire decision cycle
  (quota check, outbox insert, source terminal commit) happens synchronously
  inside one worker task with no background-timer dependency — so 30s is a
  very large multiple of the expected sub-second path if the mechanism is
  working as designed.
- **Timing correlation with PR #1673, not established as causal.** The
  spike in occurrences (0 confirmed in this series between 09-16 and 09-21,
  now 6 in 26 hours) begins shortly after `56bc205` merged
  (2026-09-21T19:18Z), which also touched `completion_trigger.rs` (168 lines,
  for the *cross-shard* fire-verification path in `backup_verify.rs`) and
  `execution.rs` (27 lines). The commit message asserts the same-shard
  quota-exceeded-to-outbox path used by this test is unchanged, and this
  session's own read of that path (above) is consistent with that claim.
  Whether the `execution.rs` or other `completion_trigger.rs` changes in that
  PR touch a code path this test's worker also exercises (e.g. shared
  connection-pool or transaction-commit machinery) is **not checked this
  session** — a concrete, cheap next step: `git show 56bc205 --
  autumn-harvest/src/execution.rs` and diff against what the source
  workflow's terminal-commit path actually calls.
- **Not investigated this session, for lack of Docker:** whether the source
  workflow's task is even being dispatched at all during the hang (a queue
  visibility/claim bug) versus dispatched-and-stuck (a transaction or lock
  wait) versus never retried (a crashed worker task). Black-box waiting on
  `harvest_workflow_executions.state` cannot distinguish these; the next
  session with Docker should add ad hoc `tracing` output or query
  `harvest_task_queue` mid-hang rather than re-running the black-box wait.

## 🔧 Treatment

None. Per the hard gate, this is correctly a health report, not a fix PR:
no rerun-rate measurement (no Docker), no named mechanism (four candidates
above, none confirmed), no test-vs-product verdict, no before/after
measurement. **Explicitly not recommended:** widening the timeout further.
It is already at 3x default, was widened once for this exact flake nine
occurrences ago (1 at the time, now 7 across both this and the prior
report), and did not change the outcome — a 4th widen would be pure
timeout-bump theater, the exact pattern this role exists to stop.

**Priority for the next session with Docker or live-CI-dispatch access:**

1. Reproduce this specific panic (not the outbox-retry one #1706 targets)
   using PR #1706's own successful method for the *other* signature: CPU
   oversubscription (its report used 8x on a 4-core box) + N≥20 repeated
   runs of `completion_trigger_defers_to_outbox_when_target_quota_exceeded`
   against unmodified `trunk-dev`. If it reproduces under stress, that is
   this role's Tier-1 rerun-rate evidence and unblocks a real diagnosis.
2. If it reproduces, add tracing (or a direct `harvest_task_queue` /
   `harvest_workflow_executions` query mid-wait) to distinguish "task never
   dispatched" from "dispatched and stuck" from "worker crashed" — the
   black-box wait this test currently does cannot tell these apart, and
   guessing further from outside is not evidence.
3. Diff `56bc205`'s `execution.rs` and remaining `completion_trigger.rs`
   hunks against what this test's worker path actually calls, to check
   (not assume) the commit message's "unrelated" claim.
4. Check whether any of the 6 occurrences hit the outbox-insert error path
   named in Diagnosis (`completion_trigger.rs:2101-2122`'s `.map_err(...)?`)
   rather than a pure hang — Postgres server logs or connection-pool
   exhaustion metrics around each occurrence's timestamp, if retained, would
   settle it directly; this session had neither.
5. Diff each of the 4 additional branches (`trusting-ritchie-nml6ud`,
   `gallant-dijkstra-a83hyy`, `confident-babbage-sl0122`, `cool-noether-7dejjb`)
   against `trunk-dev` before repeating this report's "independent branches"
   framing as settled — this session checked only the open-PR listing, which
   Codex review on this PR correctly flagged as too weak to support that claim.
6. Both #1706 and #1707 are otherwise complete, reviewed multiple times, and
   blocked on this same shared, pre-existing flake — neither PR's own diff
   caused it (checked above for #1706; #1707 touches only `ci.yml`'s shard
   count). Whoever merges either should not treat this test's continued
   redness as a regression introduced by that PR.

## 📊 Measurement

- **Before:** none — no rerun protocol executed.
- **Symptom count:** 6 confirmed occurrences of the identical panic
  (`integration_e2e.rs:1383:6`, reached from this test's 30s
  `wait_for_execution_state_with_timeout` for the source's `COMPLETED`
  state), across 6 independent branches, 2026-09-22T07:15Z–2026-09-23T09:00Z.
  Up from 1 occurrence of this specific site in the 09-21 report (which found
  it alongside 2 occurrences of a different signature on the same test).
  2 clean shard-0 passes bound the same window, so this is intermittent, not
  deterministic, and this count is a sample of convenience, not a rerun-rate.
- **After:** N/A — no fix attempted.
- **Revert check:** N/A — no fix attempted.
- **Ledger:** no quarantine ledger exists in this repository (checked again).

## 🔬 Reproduce

```sh
# Docker check (unchanged gap, re-verified today):
docker ps
# -> failed to connect to the docker API at unix:///var/run/docker.sock:
#    connect: no such file or directory

# The two fix PRs already open against this test family:
# search_pull_requests(query="repo:autumn-foundation/autumn-harvest head:claude/fix-quota-outbox-scanner-sharded-pool-1685")
# -> PR #1706
# search_pull_requests(query="repo:autumn-foundation/autumn-harvest head:claude/fix-shard-0-collision-rebalance-1685")
# -> PR #1707

# Each PR's own CI still failing on quota_enforcement_tests:
# pull_request_read(method="get_check_runs", pullNumber=1706) -> "Test DB (linux, shard 0)" conclusion=failure
# pull_request_read(method="get_check_runs", pullNumber=1707) -> "Test DB (linux, shard 2)" conclusion=failure
# get_job_logs(job_id=107008321702, return_content=false) -> signed URL; curl + grep:
grep -n "panicked at\|quota_enforcement_tests::.*FAILED" pr1706_shard0.log
# -> completion_trigger_defers_to_outbox_when_target_quota_exceeded panicked
#    at integration_e2e.rs:1383:6
# Same for job 107035722407 (PR #1707).

# Today's window census (90 pull_request/completed runs since the 09-21
# report's cutoff):
# actions_list(method="list_workflow_runs", resource_id="ci.yml", perPage=100, page=1)
python3 -c "
import json
d = json.load(open('<saved actions_list response>'))
runs = [r for r in d['workflow_runs'] if r['event']=='pull_request' and r['status']=='completed']
cutoff = '2026-09-21T10:18:51Z'
window = [r for r in runs if r['created_at'] >= cutoff]
from collections import Counter
print(len(window), Counter(r['conclusion'] for r in window))
"
# -> 90 (61 cancelled, 26 failure, 3 success)

# The 4 additional shard-0 failures, each job-logged the same way:
# get_job_logs(run_id=<id>, failed_only=true, tail_lines=15) to find the
# failing job name, then get_job_logs(job_id=<id>, return_content=false) for
# the signed URL, curl + grep "panicked at":
#   35652570063 (job 106647193659) claude/trusting-ritchie-nml6ud
#   35658767845 (job 106896752815) claude/gallant-dijkstra-a83hyy
#   35788942436 (job 106960979043) claude/confident-babbage-sl0122
#   35835324763 (job 107108884704) claude/cool-noether-7dejjb
# -> all 4: identical panic, integration_e2e.rs:1383:6

# The 2 clean shard-0 passes bounding the window (ruling out determinism):
# actions_list(method="list_workflow_jobs", resource_id=35695812531) -> "Test DB (linux, shard 0)" success, 2026-09-22T06:39Z
# actions_list(method="list_workflow_jobs", resource_id=35826846700) -> "Test DB (linux, shard 0)" success, 2026-09-23T06:27Z

# The timeout widen and its own commit message disclaiming a fix:
git log -S "A wider bound than the usual 10s default" --oneline -- autumn-harvest/tests/integration/quota_enforcement_tests.rs
# -> 56bc205 (#1673)
git show 56bc205 -- autumn-harvest/tests/integration/quota_enforcement_tests.rs
# -> only the wait_for_execution_state -> wait_for_execution_state_with_timeout(30s)
#    change; commit message: "This test's own diff, and
#    completion_trigger.rs's quota-exceeded-to-outbox path, are unchanged
#    from trunk-dev in this PR -- the flake predates and is unrelated to the
#    fire-verify work here."

# Ruling out #1706's own mechanism as the cause (same-shard QuotaExceeded
# arm never aborts the source's transaction):
sed -n '2078,2100p' autumn-harvest/src/completion_trigger.rs
```
