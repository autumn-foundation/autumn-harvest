# 🚦 Semaphore CI health — a shard-weight-drift harness (report-only), a
# second, previously undetected 11-apart manifest collision, fresh evidence
# against "shard weight causes the flakes" and for a run-wide contention
# event instead, and a real product bug: a claim-task query's `MATERIALIZED`
# CTE syntax breaks against the webhook suites' still-default Postgres 11

**Status:** harness PR (`docs/audits/shard-weight-drift.py`, report-only) +
health report. Continues the series from
`docs/rnd/2026-09-21-ci-health-semaphore-quota-outbox-recurrence.md`.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger
against `trunk-dev`, principally `test-db-linux`/`test`/`test-nodb`, plus the
ungated `lint` job. No Docker daemon in this session's sandbox (`docker ps`
fails with "no such file or directory" on `/var/run/docker.sock`), matching
every prior report in this series — the ≥20x rerun campaign this series has
recommended since 09-16 is still not run by any session.

## 🌡️ Symptom, part 1 — the manifest-weight harness finding

The 09-21 report closed by asking for "a full per-shard test-weight
accounting across the full manifest" before touching `SEMAPHORE_SHARD_COUNT`
again. This session built that accounting as a script
(`docs/audits/shard-weight-drift.py`) rather than a one-off, so it stops
requiring hand archaeology each time.

**Methodological correction, affecting this whole report series.** Every
prior report's row-ordinal arithmetic (the "row 32", "row 43", "row 28"
figures in the 09-04 through 09-21 reports) was computed from a plain line
count over the *entire* manifest file — comments, blank lines, and
`allos`/`compileonly` rows included. `run-suites.sh`'s actual `do_run`
function counts `row_ordinal` only among rows whose osclass matches the mode
being run (`linux`, for `test-db-linux`). The two schemes agree on the
*distance* between two rows (a fixed offset survives either indexing), which
is why the headline `integration_e2e`/`quota_enforcement_tests` finding
still held across every report — but they disagree on *which shard number* a
row lands in. This script counts the way the real script counts, so its
shard numbers are the ones that match a real job's name in the Actions UI.

Running it against today's manifest (151 `linux` rows, `SEMAPHORE_SHARD_COUNT
= 11`, both read out of the actual files rather than hand-copied):

```
shard 0: total= 418  top=integration_e2e(120), api_scheduler_integration(80), backup_verify_tests(57)
shard 7: total= 435  top=event_partitioning_tests(155), audit_export_tests(88), claim_budget_tests(36)
```

**Correction (Codex review on this PR):** the first version of this report
and the script it ships weighed each row by column-0 `^#\[tokio::test` only,
silently missing every indented test (nested-module tests) and every plain
`#[test]`. Two examples Codex's review cited directly: `worker_session_tests`
weighed 0 (its 12 tests are all indented) and `hot_code_swap_tests` weighed
40 instead of 79 (missing 38 plain `#[test]`s and one indented
`#[tokio::test]`). Fixed to match `#[test]`/`#[tokio::test]` with optional
leading whitespace; every weight number in this report reflects the
corrected count (`integration_e2e` moved 118→120,
`event_partitioning_tests` 148→155, `audit_export_tests` 87→88,
`claim_budget_tests` 34→36, `hot_code_swap_tests` 40→79 — see the diffs
above and below).

**Second correction (Codex review, same PR):** the first version of this
report also singled out only two of the collisions the script itself
reports, which read as if they were the whole finding. Under today's
`SEMAPHORE_SHARD_COUNT = 11`, the harness finds **seven** shards carrying
2+ suites at or above the 30-test heavy threshold, not two — every one of
them is a genuine `SEMAPHORE_SHARD_COUNT`-apart-ordinal collision:

| Shard | Colliding rows (ordinal, weight) |
|---|---|
| 0  | `integration_e2e` (33, 120), `quota_enforcement_tests` (44, 47), `backup_verify_tests` (77, 57), `api_scheduler_integration` (88, 80), `interface_schema_integration` (110, 31) |
| 1  | `transactional_start_tests` (67, 36), `codec_rotation_db_tests` (78, 56) |
| 2  | `capability_miss_tests` (13, 46), `cross_region_dr_tests` (79, 31) |
| 3  | `pacing_override_integration` (113, 46), `workflow_rerun_integration` (146, 68) |
| 6  | `admission_gate_authoritative` (6, 34), `shard_rebalance_db_tests` (83, 111) |
| 7  | `audit_export_tests` (7, 88), `claim_budget_tests` (18, 36), `event_partitioning_tests` (29, 155) |
| 10 | `queue_pause_tests` (43, 44), `hot_code_swap_tests` (76, 79), `stall_diagnosis_integration` (131, 76) |

Of these, only shard 0's `integration_e2e`/`quota_enforcement_tests` pair was
already known (the 09-21 report). Shards 0 and 7 remain the two worst by
both row count and total weight — shard 7's three-way stack
(`event_partitioning_tests` is the single heaviest row in the entire
manifest) is worse than shard 0's — but the other five collisions (shards 1,
2, 3, 6, 10) are new to this script and were not called out in the first
version of this report. The 09-21 report's own manifest-wide check placed
`event_partitioning_tests` on "shard 6" (using the whole-file line count,
the discrepancy the paragraph above describes) — coincidentally shard 6 does
also carry a real collision today, just a different one
(`admission_gate_authoritative`/`shard_rebalance_db_tests`), not the one
that report meant.

**A sweep of `SEMAPHORE_SHARD_COUNT` from 9 through 24 finds no value with
zero heavy-suite (≥30 tests) collisions** — every count in that range stacks
at least 5 heavy rows somewhere (re-run after both corrections above), and
*which* rows collide changes unpredictably with the count (see the script's
own docstring for the full table). The 2026 issue #1267 fix picked 11 by
comparing spread across a handful of candidate counts, but did not check for
multi-suite stacking specifically, and — per the correction above — used the
wrong row-ordinal scheme to do it. **Bumping the count again would very
likely just relocate today's collisions to new shards, not remove the
failure mode**: with ~20 rows at or above the heavy threshold and single- or
low-double-digit shard counts, pigeonhole makes some stacking unavoidable
under a modulo assignment. The real fix is a weight-aware assignment (e.g.
greedy longest-processing-time bin-packing) replacing `row_ordinal % N`,
which this session did not attempt — it would change `run-suites.sh`'s
sharding algorithm itself, a bigger, harder-to-validate-without-live-CI
change than one session should make unilaterally.

**What this session shipped instead:** `docs/audits/shard-weight-drift.py`,
wired into the ungated `lint` job, **report-only** (always exits 0 — gating
now would redden every open PR on a pre-existing structural property this
step cannot itself fix). It recomputes the accounting above on every CI run,
so the next session (or the next manifest edit) sees current numbers
instead of needing to re-derive them by hand, and self-tests its own parser
against a fixture before running for real. Registered in
`docs/audits/README.md`'s catalog table.

## 🌡️ Symptom, part 2 — fresh occurrences since the 09-21 report's cutoff

Census: `list_workflow_runs` for `ci.yml` (unfiltered, two identical calls
confirming determinism — 100 runs, `2026-09-21T10:29:20Z` through
`2026-09-22T06:39:18Z`), filtered client-side to
`event=="pull_request" and status=="completed"` per the 09-17 report's
documented method (the combined server-side filter is known non-deterministic).
85 runs in the window since the 09-21 report's cutoff (`2026-09-21T10:18:51Z`):
57 cancelled, 19 failure, 9 success. Sampled the failures for signatures
relevant to this series' open items (not every one of the 19 was job-logged
this session — time went to the three items below instead).

### 1. A 4th confirmed occurrence of the `integration_e2e.rs:1383:6` timeout panic, isolated (1 test)

Run `35693877462` (`Test DB (linux, shard 0)`, `claude/pensive-brahmagupta-xj28n0`,
2026-09-22T06:49–06:50Z): `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
panics at `integration_e2e.rs:1383:6`, `"workflow should reach expected state
within timeout: Elapsed(())"` — the identical site as the 09-21 report's item
2's 3rd occurrence. Every other test in the run (`grep -c '\.\.\. FAILED$'` on
the full log: 1) passed. This is the same shard (0) this session's harness
confirms carries the `integration_e2e`/`quota_enforcement_tests` collision.

### 2. An 8-of-11-shard cascade on `claude/keen-bardeen-amm1ft` — worse than the 09-21 report's 6-of-11 cascade on the *same branch*

Run `35680554771` (2026-09-22T02:20–03:57Z): shards 0, 1, 3, 4, 5, 6, 8, 9 of
11 failed. The 09-21 report's item 4 found a 6-of-11 cascade on this exact
branch name the day before (run `35563198153`); this is either a later push
or a rerun on the same branch, and the cascade widened rather than resolved.

**Shard 0** (the harness-confirmed `integration_e2e`/`quota_enforcement_tests`
collision shard) shows both suites failing, and not narrowly: `integration_e2e`
alone has roughly 60 of its ~120 tests fail, spanning entirely unrelated
categories (activity retry, claim-task sticky routing, concurrency caps,
search attributes, worker completion, schedule baselines). The great majority
panic at the identical `integration_e2e.rs:1383:6` site, in linear sequence
from `03:18:49` to `03:31:57` (consistent with `--test-threads=1` — these are
independent sequential failures, not a simultaneous crash the timestamps
might suggest at first read; the full backtraces print together only because
`cargo test` batches its failure-detail section after the whole binary
finishes). `quota_enforcement_tests`, which runs immediately after on the
same shard, then *also* cascades for another ~4 minutes once it starts.

**Shard 9** — the *lightest* shard under today's manifest (harness total:
100, no suite above the heavy threshold) — also failed in this same run,
with a
**new, previously undocumented failure signature**: `workflow_retry_tests`
(3 of 18 tests) — `workflow_retry_carries_chain_deadline_verbatim`,
`workflow_retry_inherits_predecessor_start_source`,
`workflow_retry_stays_on_the_predecessor_shard` — all panic at
`workflow_retry_tests.rs:606:5`, inside a different polling helper
(`wait_for_retry_state`, a 400×50ms = 20s poll for a retry execution row to
appear) than the one item 2 and the shard-0 cascade use, but the same
*shape*: a fixed-deadline poll for a background worker action never
completing.

**This is significant evidence against, not for, the shard-weight-causes-
exposure hypothesis this series has carried since 09-21.** Shard 9 carries
none of the manifest's heavy suites and finished well within normal time,
yet it hit the same class of worker-fell-behind timeout as shard 0's
heavily-loaded run. The 09-21 report's item 4 already flagged this exact
tension ("shard 3... failed simultaneously... not explained by the shard-10
collision") without a second data point; this run supplies one. **Read
together, both this run's data point and the corrected accounting above
support treating the manifest-gap collision as a real, confirmed, worth-
fixing structural defect on its own terms (shard 0/shard 7 do carry
disproportionate total work, which is worth fixing for CI-cost reasons
regardless), while treating "it explains the observed timeout cascades" as
not merely unconfirmed but now actively undercut** — a cascade hitting the
fleet's lightest shard in the same run as its heaviest points at a run-wide
resource-contention event (a busy runner host, a saturated Docker daemon
across the whole job matrix, or a worker/DB-level backlog independent of any
one shard's manifest weight), not at any individual shard's own workload.
This session has no runner-level or worker-level telemetry to name the
mechanism further.

### 3. A real product bug: `queue.rs`'s claim-task query uses `MATERIALIZED` CTE syntax (Postgres 12+), and the webhook suites still default to Postgres 11

In the same run's shard 9 log, immediately after the `workflow_retry_tests`
failures, `autumn-harvest-plugin/tests/webhook_receiver_integration.rs`'s
suite ran and logged, on every one of its 6 tests:

```
ERROR autumn_harvest::worker: failed to claim task error=database error: syntax error at or near "MATERIALIZED"
```

All 6 tests still report `ok` — this is not currently failing any assertion
in this suite, only being logged — but the mechanism is real and reproduces
on demand, not intermittently. `autumn-harvest/src/queue.rs` builds its
task-claim query with four `AS MATERIALIZED` CTEs (`paused_queues`,
`paused_activities`, `concurrency_pending_keys`,
`concurrency_running_counts`, lines 970–988, plus a fifth at line 1206) —
the `[NOT] MATERIALIZED` CTE modifier is Postgres 12+ syntax; it does not
exist in Postgres 11 and produces exactly the observed
`syntax error at or near "MATERIALIZED"`. The code comments at
`queue.rs:858-989` and `:1182-1206` explain these were added deliberately,
for a real performance reason (forcing single evaluation of a lookup set
rather than once per row) — this is recent, intentional work, not leftover
debug code, and matches several `docs/assays/000{3,4,5}-*.md` performance
write-ups' subject matter (concurrency-gate cardinality, claim-batched
seek-and-refine).

`webhook_receiver_integration.rs` uses `autumn_web::test::TestDb` with no
`.with_tag(...)` override, so it gets whichever Postgres version that
fixture defaults to. `ci.yml`'s own pre-pull step (the `Pre-pull Docker test
images` step, `test-db-linux` job) documents the answer directly: *"the
unoverridden default -- `autumn-web`'s own `TestDb`, used by e.g. the
webhook suites"* is `postgres:11-alpine`. Every other suite that opts out of
that default does so explicitly (`grep -c 'with_tag("16")'` finds the
pattern repeated across `workflow_retry_tests.rs`,
`workflow_logs_integration.rs`, and others) — the webhook suite simply never
picked one up.

**Test-vs-product verdict:** this is a product bug, not a test bug. The
query itself, not the test, breaks against Postgres 11. Whether Postgres 11
needs to keep working is a product decision this session cannot make: the
codebase has no single documented "minimum supported Postgres version" this
session could find (`docs/partitioned-events.md` requires "Postgres 14 or
later" for one *opt-in* layout only; most performance docs test against
Postgres 16; one changelog fragment notes `DEFAULT now()` behavior "which
PostgreSQL 11+ applies", implying 11 is at least sometimes treated as a
floor). Two fixes are available and this session is not picking one: (a)
if Postgres 11 must keep working, gate the `MATERIALIZED` hint behind a
server-version check or drop it, since a hint that fails outright is worse
than no hint; (b) if Postgres 11 support has already been dropped
elsewhere and this is simply a stale test fixture, give
`webhook_receiver_integration.rs` the same `.with_tag("16")` (or later)
every other suite in this position already carries, and audit for any other
caller of `autumn_web::test::TestDb` with no override.

**Why this session did not just bump the tag:** doing so without knowing
whether Postgres 11 is a supported target would silently convert a real,
currently-detectable product defect into an invisible one — exactly the
"making a test tolerant of a product bug" pattern this role is chartered to
never do, just aimed at a fixture instead of an assertion. Filed here with
full reproduction instead.

## 🔍 Diagnosis

**Harness finding:** confirmed by direct computation, matching
`run-suites.sh`'s actual algorithm (self-tested). Mechanism: round-robin
(`row_ordinal % N`) sharding of an alphabetically-grown, weight-skewed
manifest cannot avoid collisions at any of the 16 shard counts checked;
`SEMAPHORE_SHARD_COUNT` bumps have fixed one collision at a time
historically (issue #1267) while the underlying failure mode persists.

**Item 2 (4th `integration_e2e.rs:1383:6` occurrence):** signature-confirmed,
not newly diagnosed — same open mechanism candidate as every prior report in
this series (worker/background-sweep behavior under load, vs. a genuine
product race per the 09-21 report's still-undismissed candidate). Still
blocked on the hard gate's requirement 3 (test-vs-product verdict) pending
the ≥20x rerun campaign no session has been able to run.

**Item 4 (8/11 cascade + lightest-shard hit):** the shard-weight-causes-
exposure hypothesis this series has carried is weakened by direct evidence
in this session (the lightest shard cascaded too, in the same run). A
run-wide contention event is now the better-supported reading, though not
proven — this session has no runner-level telemetry. The manifest-gap
collision (item 1) remains a confirmed, independently real defect regardless
of which explanation for the cascades turns out to be right.

**Item 3 (MATERIALIZED):** root-caused with a name, a line, and a
reproduction — genuinely closed as a diagnosis, open as a fix decision that
requires product input this session does not have authority to make.

## 🔧 Treatment

Shipped this session: `docs/audits/shard-weight-drift.py` (report-only
harness), `docs/audits/README.md` catalog row, and a `lint`-job wiring in
`ci.yml`. No test was made deterministic, no flake was fixed, no CI-cost
number was reduced — this session's contribution is visibility (the
harness), a corrected and expanded diagnosis (the second collision, the
run-wide-contention counter-evidence), and one product bug filed with
reproduction (item 3).

Carried forward, in priority order:

1. **File and fix the `MATERIALIZED`/Postgres-11 product bug** (item 3) —
   the highest-value single item in this report. Needs a product decision
   (drop Postgres 11, or make the query version-tolerant) this session
   cannot make; either fix is small once decided.
2. **The ≥20x rerun campaign for the `integration_e2e.rs:1383:6` timeout
   panic** — now at 4 confirmed real-world occurrences across 4 sessions
   and at least 3 branches, still not run by any session (no Docker in any
   session's sandbox so far).
3. **A weight-aware (not modulo) shard assignment for `test-db-linux`** —
   the sweep in this report shows no shard count avoids collisions; a
   different algorithm, not a different N, is the durable fix. Bigger
   change, needs live-CI validation this session cannot provide.
4. **Runner/worker-level telemetry for the run-wide contention hypothesis**
   (item 2 of this report's fresh evidence) — this session's biggest open
   question, and unreachable without either Docker locally or a live CI
   dispatch with additional instrumentation.
5. Cache-usage API access, branch-protection confirmation, the
   `list_workflow_runs` conclusion/jobs-API gap — unchanged, still
   unavailable, carried from every prior report.

## 📊 Measurement

- **Harness:** self-test passes; full run against today's manifest resolves
  all 151 `linux` rows (5 rows needed a `_tests`-suffix fallback the first
  version of the script missed — fixed and re-verified) and reproduces the
  seven collisions above. **Corrected twice post-review (Codex, this PR):**
  first, the weight regex undercounted indented and plain `#[test]`
  attributes (`worker_session_tests` 0→12, `hot_code_swap_tests` 40→79,
  every other weight in this report shifted accordingly); second, the first
  draft named only 2 of the 7 collisions the script's own output shows.
  Both fixed, re-run, and the corrected numbers are what this report now
  carries throughout. `--sweep` output (N=9..24, re-run after both fixes:
  still no collision-free N) archived in the script's own docstring.
- **Item 2:** 1 occurrence this window, 4th cumulative — a count, not yet a
  rate; the ≥20x bar this role requires is still unmet.
- **Item 4:** 1 run, 8/11 shards — worse than the 09-21 report's 6/11 on the
  same branch. No before/after timing comparison possible (no fix shipped
  for this item).
- **Item 3:** reproduces on every one of the 6 `webhook_receiver_integration`
  tests in the observed run, 6/6 — not a rate, a deterministic defect. No
  revert check applies (no fix shipped; filing only).
- **Ledger:** no quarantine ledger exists in this repository, unchanged.

## 🔬 Reproduce

```sh
# Harness (self-test + full report + sweep):
python3 docs/audits/shard-weight-drift.py --self-test
python3 docs/audits/shard-weight-drift.py --sweep

# Census (unfiltered list, client-side filter — see 09-17 report for why):
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   owner="autumn-foundation", repo="autumn-harvest", perPage=100, page=1)
# filter client-side: event=="pull_request" and status=="completed",
# created_at >= 2026-09-21T10:18:51Z

# Item 1 (4th occurrence):
# get_job_logs(job_id=106641230294, return_content=false) -> signed URL
curl -sS -o shard0.log '<signed logs_url>'
grep -n "panicked at" shard0.log
# -> quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded
#    panicked at integration_e2e.rs:1383:6, "workflow should reach expected
#    state within timeout: Elapsed(())"

# Item 2 (cascade + lightest-shard hit):
# get_job_logs(run_id=35680554771, failed_only=true) -> 8 of 11 shard jobs
# shard 0:
curl -sS -o shard0_cascade.log '<signed logs_url for shard-0 job>'
grep -c "\.\.\. FAILED$" shard0_cascade.log   # integration_e2e portion
grep -n "panicked at" shard0_cascade.log | sort | uniq -c -f2 | sort -rn | head
# -> the overwhelming majority at integration_e2e.rs:1383:6
# shard 9 (lightest shard, harness total=100):
curl -sS -o shard9.log '<signed logs_url for shard-9 job>'
grep -n "panicked at\|test result" shard9.log
# -> workflow_retry_tests.rs:606:5, 3/18 failed, a NEW signature; and,
#    later in the same shard's log, the MATERIALIZED error (item 3)

# Item 3 (MATERIALIZED / Postgres 11):
grep -n "AS MATERIALIZED" autumn-harvest/src/queue.rs
grep -n "with_tag" autumn-harvest/tests/integration/workflow_retry_tests.rs \
  autumn-harvest-plugin/tests/webhook_receiver_integration.rs
# workflow_retry_tests.rs:229 pins "16"; webhook_receiver_integration.rs has
# no override, so it gets autumn_web::test::TestDb's own default.
grep -n "the unoverridden default" .github/workflows/ci.yml
# -> "postgres:11-alpine (the unoverridden default -- autumn-web's own
#    TestDb, used by e.g. the webhook suites)"
# PostgreSQL 12 release notes: AS [NOT] MATERIALIZED introduced in CTEs;
# postgres 11 has no such syntax, matching the exact error text observed.
```
