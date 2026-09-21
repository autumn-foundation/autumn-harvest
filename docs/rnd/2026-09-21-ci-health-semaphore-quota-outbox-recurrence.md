# 🚦 Semaphore CI health — `quota_enforcement_tests`' unexplained outbox-retry
# timeout recurs a 3rd time, and its 3rd occurrence shares a panic site with
# a separate shard-10 wait-timeout cascade, tying two candidates this
# report's first draft treated as unrelated; `corpus::seeded_corpus_is_
# clean_under_the_syntactic_layer`'s 5th occurrence confirms the 09-18
# report's diagnosis still holds (own-diff dead code, not a flake)

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series from `docs/rnd/2026-09-20-ci-health-semaphore-migration-count-message-fix.md`.

**Corrected three times after review** (Codex on this PR, two rounds): the
initial draft of item 3 reopened
`corpus::seeded_corpus_is_clean_under_the_syntactic_layer` as "still
undiagnosed" and miscounted it as a 4th occurrence on a fifth branch,
without checking today's occurrence against the 09-18 report's own
diagnostic-clarity fix — which was live in the log this session already had
and, once grepped, shows the exact same deterministic dead-code mechanism
that report closed as "not a flake." Second, the census math in the
Diagnosis and Measurement sections omitted `35553863066` (item 2's quota
recurrence) from its 15/2/2 breakdown entirely. Third, a follow-up review
caught that the census itself was built from the combined `{event, status}`
filter the 09-17 report confirmed is non-deterministic, without the
validation or workaround that finding requires — rebuilding it via the
documented method recovered one more failure (`35576291757`), which turned
out to be a 3rd occurrence of item 2's test with a *different* signature
that ties it directly to item 4's cascade. All three corrected below,
inline at the point each applies; item 2 and item 4 are now materially
different findings than the first draft reported.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally `test-db-linux`/`test`/`test-nodb`, plus the ungated
`lint` job. Branch-protection status and cache-usage API access remain
unavailable from this session (`ToolSearch` re-checked today for both; neither
tool is present, unchanged from every prior report in this series).

**Correction (post-review, second round):** the census below was first built
from a single call to the combined `{event, status}` filter, which the
09-17 report confirmed is non-deterministic and unsafe to trust without
repetition or the documented unfiltered-list workaround — this report did
neither on the first pass. Redone properly: two back-to-back identical
unfiltered `list_workflow_runs` calls (no filter, `page=1`) returned
identical `total_count` (5926) and identical 100-run id sets, confirming
determinism today; filtering client-side for `event=="pull_request" and
status=="completed"` against the same window found **one additional
failure the original combined-filter call had silently dropped**
(`35576291757`) — itself a significant finding, added as item 2's 3rd
occurrence below. The corrected census and every downstream count reflect
this.

## 🌡️ Symptom

### 1. Today's census: window since the 09-20 report's cutoff (2026-09-20T06:06:33Z) through this session's wall clock — 80 runs: 45 cancelled, 20 failure, 15 success

**Correction (post-review):** rebuilt via the 09-17 report's documented
workaround (unfiltered `list_workflow_runs`, filtered client-side), after
two identical calls confirmed determinism today (see the Verdict path
correction above). One page (100 runs, `2026-09-20T04:29:44Z` through
`2026-09-21T10:18:51Z`) covered the whole window; filtered to `created_at >=
2026-09-20T06:06:33Z`. Every one of the original 19 explicit failures is
still present and unchanged; the corrected method adds exactly one more
(`35576291757`), job-logged below alongside the rest:

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
| `35535026904` | `claude/kind-hopper-wbrak0` | clippy dead-code (`backup_verify.rs::absence_is_decisive_loss`) **and** `corpus::seeded_corpus_is_clean_under_the_syntactic_layer` — same defect, two independent gates, root-caused, see item 3 |
| `35535358141` | `claude/fix-pending-requeue-changeset-arity` | `Test DB (linux, shard 1)`: `cross_region_dr_tests` — see below, not root-caused this session |
| `35536291030` | `claude/magical-gauss-10blzb` | same `E0061`, unfixed, 5th push |
| `35549811155` | `claude/exciting-babbage-r3nfok` | run conclusion `failure`, but `get_job_logs(failed_only=true)` returns 0 failed jobs of 30 — see the data-quality note below |
| `35551323906` | `claude/kind-hopper-wbrak0` | `sqlite_feasibility_docs::derived_totals_agree_with_the_table_and_the_tree` (own doc staleness; the 09-20 report's diagnostic-message fix is visibly working — panic text now reads "the report states..." rather than repeating the live count) |
| `35552427919` | `claude/laughing-maxwell-9t78rp` | comment-hygiene Tier B (`shard_rebalance.rs`, own diff) |
| `35553863066` | `claude/laughing-maxwell-9t78rp` | `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded` — see item 2, a 2nd confirmed occurrence |
| `35576291757` | `claude/kind-hopper-wbrak0` (run_attempt 2) | same test as item 2, but a 3rd, differently-signatured occurrence found only after the census correction above — see item 2 |
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
stronger signal than the single occurrence the 09-16 report logged.

**Added after review (Codex's census correction, above, surfaced this): a
3rd occurrence, with a different signature that reframes the finding.** Run
`35576291757` (`Test DB (linux, shard 10)`, `claude/kind-hopper-wbrak0`,
2026-09-21T08:07:53Z, `run_attempt: 2`) failed the same test again, but this
time the panic is **not** the specific "target row" assertion — it is the
generic shared-helper timeout:

```
thread 'quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded' panicked at autumn-harvest/tests/integration/integration_e2e.rs:1383:6:
workflow should reach expected state within timeout: Elapsed(())
```

This is the identical panic site and text as item 4's mass cascade
(`wait_for_execution_state_with_timeout`). Checked whether that is
coincidence: `.github/ci/integration-suites.txt`'s row-ordinal sharding
(`row_ordinal % 11`) puts **both** `integration_e2e` (row 32, `32 % 11 =
10`) and `quota_enforcement_tests` (row 43, `43 % 11 = 10`) on shard 10 —
confirmed by direct calculation, not assumed. Shard 10 is not an arbitrary
one-in-eleven sample; it is specifically the shard carrying the two largest
serial suites in the manifest. Item 4's own shard-10 leg (same run,
`35563198153`) independently confirms this: its full log shows this exact
test — `completion_trigger_defers_to_outbox_when_target_quota_exceeded` —
also panicking at `integration_e2e.rs:1383:6`, alongside roughly 60 other
`integration_e2e`/`quota_enforcement_tests` tests in the same shard, nearly
all at the identical site.

**This changes the finding.** What looked like one recurring, specific
mechanism (a background outbox-retry sweep race) is now 2 occurrences of
that specific signature plus a 3rd, differently-signatured occurrence that
directly ties this test to item 4's shard-10-wide cascade. The two
signatures may be genuinely independent (a real outbox-retry race *and* a
separate shard-10 contention problem, both hitting the same test because it
happens to poll twice — once via its own bespoke wait loop, once via the
shared helper), or the "target row" signature could itself be an earlier,
partial stage of the same contention problem (the background sweep merely
delayed, not absent) — this session cannot distinguish the two without
worker-level tracing, which remains unavailable. Both readings are recorded,
neither claimed as confirmed.

Three occurrences (2 of one signature, 1 of another, on the identical test)
is still far short of this role's own ≥20-rerun bar for a measured rate, and
still has no confirmed mechanism — the hard gate's requirement 3
(product/test verdict rendered first) is not met. **This is the strongest
documented argument yet, across the whole series, for spending a same-commit
rerun campaign on this specific test — and, given the shard-10 connection,
possibly on the whole shard-10 leg's timing** — rather than on the closed
activity-timeout
item (#1558, now at 0/1 confirmed exposure per the 09-16 report) — 3
real-world occurrences beats 0 confirmed exposures as a signal
of where to spend the ≥20x budget this role has not yet had the means to
run (no Docker in this session's sandbox; see the 09-16 report's identical
constraint).

### 3. `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`: 5th confirmed occurrence, and it is the same deterministic dead-code defect the 09-18 report already diagnosed and closed — not a flake

**Correction (post-review):** the first draft of this section reopened this
test as "still undiagnosed" and miscounted the occurrence total. Both wrong.
The 09-18 report
(`docs/rnd/2026-09-18-ci-health-semaphore-corpus-gate-diagnostic-fix.md`)
already root-caused this exact recurring signature: three sessions (09-15,
09-16, 09-17) had carried it forward as an undiagnosed flake candidate
because the test's own panic message threw away the actual `rustc`
diagnostic under `--message-format=json`. That report fixed the message (not
the test's assertion — a diagnostic-clarity change only) and, on direct log
inspection of 4 known occurrences at the time (3 on `claude/bold-lovelace-agnczk`,
1 on `claude/pensive-brahmagupta-jafcou`), found every one traced to real,
own-branch dead code under `-D warnings` — explicitly concluding "this item
is not a flake, was never a flake."

Run `35535026904` (`Semantic determinism analysis (harvest-verify)`,
`claude/kind-hopper-wbrak0`, 2026-09-20T20:16:44Z) is a **5th** occurrence, on
a **third** distinct branch — this report's first draft undercounted it as a
4th occurrence on a fifth branch, not having cross-checked the 09-18 report's
own tally. This session already had this run's job log in hand (fetched for
the census above) and, re-grepped after review for the 09-18 fix's `---
rustc diagnostics ---` section rather than just the tail:

```
error: function `route_trigger_fires` is never used
error: function `absence_is_decisive_loss` is never used
error: could not compile `autumn-harvest` (lib) due to 4 previous errors
```

`absence_is_decisive_loss` is the identical function the census table's
clippy dead-code row (same run, `Lint` job) already names — confirming this
is the exact mechanism the 09-18 report closed: one real dead-code defect in
this branch's own diff, caught independently and correctly by two gates
(`Lint`'s clippy step and the corpus test's own nested `-D warnings`
rebuild), not a suite-level flake and not requiring any further diagnosis.
**No action needed; not carried forward.**

### 4. `claude/keen-bardeen-amm1ft`: one run, 6 of 11 `test-db-linux` shards fail, most panics at one shared polling helper — connects to item 2, still not root-caused

**Correction (post-review):** the first draft said "6 of 30 shards." Wrong
denominator — `test-db-linux`'s own matrix (`ci.yml:1002`,
`SEMAPHORE_SHARD_COUNT: "11"`) defines shards `0` through `10`, 11 total;
30 was this run's *total job count* across every job type (`Lint`, `MSRV`,
the OS-matrix `test`/`test-nodb` legs, etc.), not the shard denominator.
Corrected: **6 of 11** `Test DB (linux, shard N)` legs failed.

Run `35563198153` (2026-09-21T05:04:24Z) failed 6 of its 11 `Test DB (linux,
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
not the shared helper — not checked further this session. Shard 3 was the
worst-hit but not the only shard 10 hit either: **added after review**, this
same run's shard 10 independently shows ~60 `integration_e2e`/
`quota_enforcement_tests` failures, nearly all at the identical
`integration_e2e.rs:1383:6` site — including the exact test item 2 tracks,
`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
(see item 2's own correction above). `.github/ci/integration-suites.txt`'s
row-ordinal sharding puts both `integration_e2e` (row 32) and
`quota_enforcement_tests` (row 43) on shard 10 by construction (`32 % 11 =
43 % 11 = 10`), so shard 10 carries the two largest serial suites in the
manifest — a candidate reason shard 10 specifically is where this pattern
keeps surfacing, on two different branches (`claude/keen-bardeen-amm1ft`
here, `claude/kind-hopper-wbrak0` in item 2's 3rd occurrence).

This shape (one shared wait-helper's timeout firing across many
otherwise-independent tests in a shard, concentrated on shard 10) is
consistent with either a systemic problem in that run's environment
(worker never processes tasks, DB contention, shard 10's own wall-clock
being long enough to make it disproportionately exposed to any transient
slowdown) or a genuine regression in a branch's own diff that broke
workflow dispatch broadly enough that nothing reaches its expected state in
time. **Not root-caused, and not rendered as a test-vs-product verdict** —
this session did not fetch either branch's diff, did not check whether a
later commit fixed either occurrence, and does not have the worker-level
logging that would distinguish "a branch's own code broke dispatch" from
"shard 10's environment was starved" from "shard 10's own suite is long
enough that transient CI contention hits it more often than other shards."
Now recorded as **2 occurrences across 2 branches**, not 1 — still short of
a measured rate, but no longer a single-occurrence note: this shape is
recurring, and the shard-10 mechanism above is a concrete, checkable
hypothesis for why, worth timing-decomposition work (per this role's own
Tier-1 evidence toolkit) rather than another log-reading pass.

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

**Item 1 (census). Corrected twice (post-review):** first, the "15/2/2"
breakdown silently dropped `35553863066` (item 2's quota recurrence) from
the count. Second, the combined-filter census itself was rebuilt via the
09-17 report's unfiltered-list method (see the Verdict path and item 1
corrections above), which recovered one more failure, `35576291757` — the
3rd occurrence of item 2's test. Corrected: of 20 explicit failures in the
window, 14 are the suite working as designed — deterministic own-branch
defects (compile breaks, clippy, `cargo fmt`, comment-hygiene, doc
staleness, and, per item 3's correction, `35535026904`'s corpus failure),
all correctly gated and root-caused. 2 of 20 (`35549811155`, `35523524313`)
are a census/tooling gap, not a suite defect claim. The remaining 4 of 20
are unresolved: item 2 (`35553863066`, `35576291757` — 2 of item 2's 3
occurrences; the 1st, `35034838493`, predates this window), item 4
(`35563198153`), and item 5 (`35535358141`).

**Item 2** cannot yet be given a test-vs-product verdict — this role's own
hard gate (requirement 3) blocks a fix PR until the nondeterminism is shown
to live in the test rather than the product it exercises, and this session
did not obtain the worker/tracing evidence that would decide it. What
changed today is the evidentiary weight, twice over: 1 occurrence was
"recorded, not actioned"; then 2 occurrences with byte-identical panic text
six days apart became the series' strongest single-candidate signal since
the closed activity-timeout item; then a 3rd occurrence, found only after
correcting the census method, turned out to share its panic site with item
4's shard-10 cascade — connecting two candidates this report's first draft
treated as unrelated. This is the report's headline recommendation:
**whoever next has Docker available in-session should point the ≥20x
rerun campaign at
`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`,
and separately consider a timing decomposition of the `test-db-linux`
shard-10 leg specifically** (it carries both `integration_e2e` and
`quota_enforcement_tests` by the sharding formula, making it this suite's
single longest-running, most contention-exposed shard) — both ahead of any
other candidate in this series' backlog.

**Item 3** is closed, per the correction above: the 09-18 report's diagnosis
holds on this 5th occurrence too — real, own-branch dead code, correctly
caught by two independent gates, not a suite defect. No further action, and
not carried forward to the next report.

**Item 4** is explicitly not rendered as a test-vs-product verdict, but is
no longer an isolated note either: it is now 2 occurrences (its own run,
`claude/keen-bardeen-amm1ft`, and item 2's 3rd occurrence, on
`claude/kind-hopper-wbrak0`), both concentrated on shard 10, both sharing
the identical panic site. The evidence gathered this session (2 runs, 2
branches, no diff read on either, no later-commit check, no worker-level
logging) is still insufficient to say whether this is a real dispatch-path
regression, DB/runner contention, or simply shard 10's own long wall-clock
making it the shard most exposed to any transient slowdown — but "insufficient
to render a verdict" is now paired with "a concrete, checkable next
experiment" (the timing decomposition above), which is a stronger position
than a single occurrence's shrug.

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
4. **The `list_workflow_runs` conclusion vs. `list_workflow_jobs` gap**
   (item 1's data-quality note) — 2 of 20 runs this window report `failure`
   overall with 0 job-level failures found; not previously logged in this
   series in exactly this form (distinct from the cancelled-run-hides-a-
   failure direction the 09-06/09-11 reports found — this is the reverse:
   an explicit `failure` conclusion the jobs API cannot account for).
5. **A timing decomposition of `test-db-linux` shard 10** — new this report
   (item 4's correction): shard 10 carries both `integration_e2e` and
   `quota_enforcement_tests` by the sharding formula, and is now implicated
   in both item 2's 3rd occurrence and item 4's cascade. Whether its own
   wall-clock time is disproportionate versus the other 10 shards has not
   been measured by any session.
6. **The remaining cancelled-run population** — the 45 cancelled runs in
   this window were not job-logged at all this session (time budget went to
   the 20 explicit failures instead, all 20 of which were checked, an
   improvement over prior reports' partial samples).

## 📊 Measurement

- **Census: corrected twice (post-review).** First, "15/2/2" omitted
  `35553863066` (item 2) from the count. Second, the combined-filter method
  itself was replaced with the 09-17 report's validated unfiltered-list
  workaround (two identical calls confirmed determinism today), which
  recovered one more failure. Corrected: 80 runs in window, 20 explicit
  failures, all 20 job-logged (100% of explicit failures this window, versus
  partial samples in most prior reports). 14/20 deterministic own-branch
  defects, root-caused — including `35535026904`'s corpus/clippy pair, per
  item 3's correction. 2/20 a census/tooling gap (conclusion/job mismatch).
  4/20 unresolved, with no rendered test-vs-product verdict: items 2 (2 of
  its 3 occurrences fall in this window), 4, and 5.
- **Item 2:** 3 confirmed occurrences of
  `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
  across the whole series — 2 carry byte-identical panic text (`"target row
  was never created by the outbox retry; last count was 1"`), 6 days apart,
  2 different branches; the 3rd (found only after the census correction)
  panics at a different, shared-helper site that is byte-identical to item
  4's cascade. Not a rate (n=3, no rerun protocol run). No revert check
  applies — no fix was made or attempted this session.
- **Item 3: correction (post-review).** 5th confirmed occurrence (not a 4th,
  per the correction above), on a 3rd distinct branch — direct log
  inspection (re-grepped after review for the `--- rustc diagnostics ---`
  section this session already had in hand) confirms the identical
  dead-code mechanism the 09-18 report closed. Root-caused, not a flake, no
  rate needed.
- **Item 4: correction (post-review).** Denominator fixed: 6/11 shards, not
  6/30 (`test-db-linux`'s own matrix is 11 shards; 30 was this run's total
  job count across every job type). ~40 failing tests in the worst shard
  (shard 3), large majority sharing one panic site
  (`integration_e2e.rs:1383:6`, `wait_for_execution_state_with_timeout`).
  Now 2 occurrences across 2 branches (this run, and item 2's 3rd
  occurrence), both on shard 10 specifically — confirmed by direct
  calculation that shard 10 uniquely carries both `integration_e2e` and
  `quota_enforcement_tests` under the sharding formula (`32 % 11 = 43 % 11
  = 10`). Not a rate; a shard-10-specific timing decomposition is the
  concrete next step, not yet run by any session.
- **Item 5:** 1 occurrence, unclassified.
- **Ledger:** no quarantine ledger exists in this repository to update.

## 🔬 Reproduce

```sh
# Today's window census -- POST-REVIEW CORRECTION, do not use the combined
# {event, status} filter (confirmed non-deterministic, 09-17 report). Use
# the unfiltered list and filter client-side:
# actions_list(method="list_workflow_runs", resource_id="ci.yml", owner=
#   "autumn-foundation", repo="autumn-harvest", perPage=100, page=1)
# -- call twice back-to-back first, to confirm determinism today:
python3 -c "
import json
a = json.load(open('call1.json')); b = json.load(open('call2.json'))
print(a['total_count'] == b['total_count'])
print({r['id'] for r in a['workflow_runs']} == {r['id'] for r in b['workflow_runs']})
"
# -> True, True (total_count 5926 both times) -- confirmed deterministic
#    today; the 09-17 report's finding was about the COMBINED filter, not
#    this unfiltered call, and this session did not retest that specific
#    combination -- see that report for why it's still the one to avoid.

python3 -c "
import json
from datetime import datetime
from collections import Counter
d = json.load(open('call1.json'))
runs = [r for r in d['workflow_runs'] if r['event']=='pull_request' and r['status']=='completed']
cutoff = datetime.fromisoformat('2026-09-20T06:06:33+00:00')
window = [r for r in runs if datetime.fromisoformat(r['created_at'].replace('Z','+00:00')) >= cutoff]
print(len(window), Counter(r['conclusion'] for r in window))
"
# -> 80 (45 cancelled, 20 failure, 15 success) -- one more than the original
#    combined-filter call's 78/44/19/15, confirming that call silently
#    dropped a real failure (35576291757).

# Per-failure job logs, all 20 (19 from the original pass, plus
# 35576291757 recovered by the corrected method):
# get_job_logs(run_id=<id>, failed_only=true, return_content=true,
#   tail_lines=20-40) for 35525986977 35527850309 35531289080 35531826444
#   35532005601 35533403023 35534123956 35534264738 35535026904
#   35535358141 35536291030 35549811155 35551323906 35552427919
#   35553863066 35557880574 35563198153 35566461177 35523524313
#   35576291757

# Item 2's byte-identical-panic check:
# get_job_logs(job_id=106196807563, return_content=false) -> signed URL
curl -sS -o shard10.log '<signed logs_url>'
grep -n "quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded\|panicked at" shard10.log
# -> panics at quota_enforcement_tests.rs:3461:9, "target row was never
#    created by the outbox retry; last count was 1" -- identical text to
#    the 09-16 report's run 35034838493 (there at line 3329, six days of
#    unrelated edits explain the line-number drift).

# Item 2's 3rd occurrence, different signature, found only after the
# census correction recovered run 35576291757:
# get_job_logs(job_id=106285217766, return_content=false) -> signed URL
curl -sS -o shard10_kindhopper.log '<signed logs_url>'
grep -n "panicked at" shard10_kindhopper.log | grep completion_trigger
# -> panics at integration_e2e.rs:1383:6 ("workflow should reach expected
#    state within timeout: Elapsed(())") -- NOT the target-row assertion of
#    the first two occurrences.

# Item 4's shared-helper check, shard 3 (worst-hit):
# get_job_logs(job_id=106225838152, return_content=false) -> signed URL
curl -sS -o shard3.log '<signed logs_url>'
grep -n "panicked at\|FAILED\|error\[" shard3.log | head -60
# -> the large majority of ~40 failures across 4 unrelated suites panic at
#    integration_e2e.rs:1383:6 ("workflow should reach expected state
#    within timeout: Elapsed(())"), inside wait_for_execution_state_with_timeout:
sed -n '1370,1384p' autumn-harvest/tests/integration/integration_e2e.rs

# Item 4's shard 10 (same run 35563198153), confirming item 2's test is
# in the cascade too:
# get_job_logs(job_id=106225838228, return_content=false) -> signed URL
curl -sS -o shard10_keenbardeen.log '<signed logs_url>'
grep -n "quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded.*FAILED\|panicked at" shard10_keenbardeen.log | grep -A1 completion_trigger
# -> same test, same integration_e2e.rs:1383:6 site, ~60 other tests in the
#    same shard fail identically.

# The shard-10 sharding calculation (why both suites land there):
awk '$1=="linux"{print NR": "c" "$0; c++}' .github/ci/integration-suites.txt \
  | grep -n "quota_enforcement_tests\|integration_e2e\b"
python3 -c "print(32 % 11, 43 % 11)"   # -> 10 10

# Item 3's re-grep (post-review correction): the run's log was already
# fetched for the census; re-checking it for the 09-18 fix's diagnostic
# section instead of just the tail:
# get_job_logs(job_id=106142430329, return_content=false) -> signed URL
curl -sS -o corpus_job.log '<signed logs_url>'
grep -n "never used\|could not compile\|rustc diagnostics\|dead-code" corpus_job.log
# -> "error: function `absence_is_decisive_loss` is never used" -- the
#    identical function this run's Lint/clippy job independently flagged,
#    confirming the 09-18 report's diagnosed mechanism, not a fresh flake.

# The E0061 compile break, already fixed on trunk-dev:
git log --oneline -1 --grep="requeue_workflow_task_for_quota_retry compile break"
# -> bdf7d58 (#1669), already in this session's branch history (HEAD~2 at
#    time of writing).

# Tool-availability re-checks (unchanged from every prior report):
# ToolSearch("branch protection rules github") -> no matching tool
# ToolSearch("actions cache usage github") -> no matching tool
```
