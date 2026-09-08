# 🚦 Semaphore CI health — fresh rerun census and failure-signature clustering

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
up on `docs/rnd/2026-09-03-ci-health-semaphore.md` through
`docs/rnd/2026-09-06-ci-health-semaphore-cache-audit-followup.md`. This report runs
the rerun-button census and, new this round, clusters every red `pull_request`-event
`ci.yml` run in the sample by failure signature — the prior reports established the
cache-correctness and windows-`no-db` timing findings but had not yet checked whether
any of the reds in this repo's history are actually flaky.

## 🎯 Verdict path (unchanged)

Same as all four prior reports. Branch-protection status for `test-db-linux`'s 10
shards and `test-nodb`'s 12 shards is still unconfirmed from this session — no
branch-protection-read tool is exposed here, checked again today. Not re-diagnosing;
flagging that the gap is still open.

## 🌡️ Symptom

### 1. Rerun-button census, repeated

100 most recent completed `pull_request`-event `ci.yml` runs, `run_attempt` field
checked directly: **0/100 show `run_attempt > 1`.** Identical to the 09-03 and 09-06
reports' findings — still no reflexive-rerun culture, still no ambient retry wrapper
anywhere in this pipeline. Conclusion split across the same 100: 25 success, 66
cancelled (superseded by a later push to the same PR — expected under a
cancel-in-progress concurrency group, not a CI-health signal), 9 failure.

### 2. Every one of the 9 failures, root-caused, not just tallied

The prior reports never checked whether this repo's reds are flaky or legitimate.
This round pulled the job logs for all 9 failed runs in the sample and clustered
them by signature:

| signature | count | mechanism |
|---|---:|---|
| `Comment hygiene (docs/audits/comment-hygiene.py)` | 4 | Tier B ratchet catching real new violations (review-round archaeology / long sentences) introduced by that commit's own diff — different files and lines every time |
| `docs/rnd/sqlite-feasibility.md guards` → `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` | 2 | live count of migration directories (99, then 100 four hours later) no longer matches the frozen count string in the doc — a doc/code sync gate, correctly firing on two different commits that each added a migration without updating the doc |
| `docs/performance.md guards` → `performance_docs::claim_transaction_statements_are_all_named_in_the_docs` | 1 | same doc/code sync pattern: a new transaction statement not yet named in `docs/performance.md` |
| `Clippy autumn-harvest-plugin` (`clippy::result_large_err`) | 1 | a genuine 128-byte `Err` variant introduced by that commit — real lint violation |
| runner infra | 1 | `Test (no-db, ubuntu-latest, shard 2)`, run `34059892793`: `##[error]The runner has received a shutdown signal` mid-compile, exit 143 — a GitHub-hosted-runner preemption, not a suite defect |

**9/9 root-caused, 0/9 flaky.** None of these are a same-commit disagreement — each
is a deterministic function of that commit's own diff (or, for the one infra case, an
external runner event outside this pipeline's control). No evidence of shared-state
leakage, timing races, or order dependence in any of the 9. This confirms and
sharpens the 09-03 report's "quarantine is clean, no orphaned skips" finding with an
actual failure-by-failure audit rather than a source-grep for `#[ignore]`.

One incidental observation, not a flake and not actioned here: the
`sqlite_feasibility_docs` panic message repeats the same live-count number on both
sides of the sentence (`"the report should state \"**99 migrations**\"; a live count
finds 99 migration directories"` — both slots show 99, not the doc's stale value vs.
the live value). That's a diagnostics quality issue in the assertion message, not a
mechanism for the failure itself; the test's pass/fail behavior is unaffected, so it
does not meet this role's bar to open a fix PR on its own.

## 🔍 Diagnosis

**Category: none — this is a null result for flakiness, not a finding requiring
treatment.** The two doc/code sync gates (`sqlite_feasibility_docs`,
`performance_docs`) are working as designed: they fail deterministically on any
commit that changes a counted quantity (migrations, transaction statements) without
updating the paired doc, and both observed failures trace to that in-diff omission,
not to suite state. The comment-hygiene failures are the same shape as the 09-03
report already described this repo's Tier B ratchet doing. The one Clippy failure is
a real lint regression. The one infra failure is a GitHub-hosted-runner shutdown
signal, outside this pipeline's configuration entirely, and does not recur elsewhere
in the sample.

## 🔧 Treatment

None. Nothing in this sample clears the impact floor: no flaky test to make
deterministic (none found), no product bug surfaced (all 9 failures are correctly
attributed to their own commit's defect), no timing win identified beyond what
`docs/rnd/2026-09-06-ci-health-semaphore-cache-audit-followup.md` already measured
and credited, no quarantine entries to retire (there is still no quarantine ledger
in this repo — nothing is skipped). Per this role's own gate, a report is the
correct outcome here, not a PR against `ci.yml` or any test file.

The three items already routed in prior reports remain open and unchanged by this
session, which had no more tool access than the previous ones:

1. **Cache-usage API access** (09-05/09-06 reports) — still no `cache/usage` or
   `caches`-listing method among the GitHub MCP tools available here.
2. **Branch-protection confirmation** (09-04/09-05/09-06 reports) — still no
   branch-protection-read tool available here; `test-db-linux` (10 shards) and
   `test-nodb` (12 shards) both still carry "not yet enforced" comments in
   `ci.yml`, unconfirmed against the live Settings → Branches state.
3. **Windows `no-db` long pole** (09-06 report) — unchanged; not re-measured this
   round since this session's sample was aimed at failure clustering, not timing.

## 📊 Measurement

- **Rerun-button census:** 0/100, unchanged from 09-03/09-06.
- **Failure clustering (new this round):** 9/100 sampled completed runs failed;
  9/9 root-caused; 0/9 flaky; 5 distinct signatures, none recurring with contradictory
  verdicts on the same commit. No revert check applies — there is no fix in this
  report to verify red-then-green on.

## 🔬 Reproduce

```sh
# Rerun-button census:
# via actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event: "pull_request", status: "completed"}, perPage=100)
# then filter conclusion == "failure" and check run_attempt > 1

# Per-failure root cause:
# via actions_list(method="list_workflow_jobs", resource_id=<run_id>) to find the
# failed job/step, then get_job_logs(job_id=<id>, return_content=true,
# tail_lines=40-100) to read the panic/assertion or clippy diagnostic directly.
```
