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
distinctions it does not by itself confirm the fix. But it is a genuine data
point the prior reports didn't have: roughly nineteen and a half hours of
real CI traffic post-fix, across ten unrelated branches' worth of failures,
produced zero recurrences of a signature that had appeared 3 times in a
comparable ~17-hour window one day earlier (the 09-14 report's census). No
same-commit rerun was run this session — Docker is unavailable in this
session's sandbox, and dispatching 20 real GitHub-hosted-runner executions of
`test-db-linux` (11 shards) solely to rerun one test is the kind of ambient,
suite-level spend this role's own charter asks to route through **Ask
before** rather than do unilaterally.

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
that branch's own then-current diff. But the message bug means whoever reads
the panic cannot tell from it alone that the doc is stale; they have to know
to distrust the sentence. Three round-trips through CI on one branch is a
real, if small, cost of that message bug. Still below this role's bar to open
a fix PR on its own (a one-line diagnostic fix on a docs-only test, not a
suite-health defect), but now recorded with a concrete cost instead of as a
hypothetical.

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

`comment-hygiene.py` Tier B (1, `shard_rebalance.rs` sentence-length), a
`cargo fmt` diff (`workflow_filter_integration.rs`/`shard_rebalance_db_tests.rs`),
clippy `map_unwrap_or` / `doc_markdown` / `redundant_clone` / dead-code
(4, all `autumn-harvest-verify` or `autumn-harvest-sqlite`), an E0433
`diesel` compile error under a `#[cfg(feature = "db")]` gate (2 occurrences,
both on branch `claude/bold-lovelace-agnczk`, same root cause both times —
`partition.rs` using `diesel::` unconditionally while the re-export is
feature-gated), and `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`
(1, `autumn-harvest-verify`'s determinism-analysis gate). Each traces to that
commit's or branch's own diff; no suite-state interaction, no timing
component, no order dependence.

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

**Item 4** is the suite working correctly, not a CI-health defect. The two
`diesel`/E0433 occurrences are the same commit-family defect counted once,
not two independent findings.

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
   — now with a concrete cost (3 CI round-trips on one branch), still a
   one-line fix someone should pick up.

## 📊 Measurement

- **Item 1:** 100/100 completed `pull_request`-event `ci.yml` runs in the
  sample individually job-logged (full census of this page, not a sample);
  15/15 failures classified; 0/15 carry the activity-timeout signature, 5 in
  18h14m pre-merge and 10 in 19h33m post-merge. Not a same-commit rerun — no
  revert check applies, since no fix was made or verified this session.
- **Item 2:** 3/3 occurrences on one branch confirmed identical panic text
  (`"**107 migrations**"` on both sides) via direct job-log inspection.
- **Item 3:** 1/1, not a rate.
- **Item 4:** 11/11 root-caused via job-log inspection; 0 suite-attributable
  flakes among them.
- **Combined:** 15/15 of this window's failures root-caused; 0/15 suite-level
  flakes; 0/15 the previously-tracked activity-timeout signature.

## 🔬 Reproduce

```sh
# Full census (not a sample): actions_list(method="list_workflow_runs",
#   resource_id="ci.yml", workflow_runs_filter={event:"pull_request",
#   status:"completed"}, perPage=100) on autumn-foundation/autumn-harvest,
# captured 2026-09-16 — one page covered 2026-09-14T19:46:25Z through
# 2026-09-16T09:33:24Z (100 runs: 55 cancelled, 30 success, 15 failure).

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
```
