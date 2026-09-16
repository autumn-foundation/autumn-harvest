# 🚦 Semaphore CI health — activity-timeout flake holds at 0/1 confirmed
# assertion exposure (not 0/3, not 0/15, not 0/23 — most job-logged
# failures never ran the tracked test, and two of three shard-8 runs
# crashed before reaching its assertion) spanning PR #1563's merge, and a
# stale migration count blocked one branch three commits running (its
# panic message's own self-contradiction is a separate, unrelated bug)

**Status:** health report — no PR opened against `ci.yml` or any test. Continues
the series in `docs/rnd/2026-09-0[3-8]-ci-health-semaphore*.md` through
`docs/rnd/2026-09-15-ci-health-semaphore-window-census.md`.

**Corrected eleven times after review** (Codex on PR #1599): the initial
draft overclaimed a full 100-run job-level census when only 23 runs were
actually job-logged (restated twice more after later drafts reintroduced
the same "full census" wording at other locations); overstated branch
diversity in the post-merge failures; miscounted item 4's failure
signatures (9 vs. 11) and omitted `migration_hygiene`; counted a
truncated, mechanism-unknown failure and a separately-recurring
`corpus` failure as root-caused; attributed CI round-trips to a
panic-message bug rather than the stale doc that actually caused them
(also reintroduced and refixed at a second location, including in this
report's own section heading); claimed the tracked event-history
assertion could no longer fire post-#1563, when the source still has a
catch-all panic arm; conflated two entirely different branches in item 3,
crediting one branch's later unrelated failures to a different branch's
single, isolated occurrence; treated "job-logged" as equivalent to
"actually ran the tracked test" — `test-db-linux` needs `[lint, changes]`
and is skipped, not run, whenever `lint` fails, which it did in 13 of the
15 explicit failures this report counted, narrowing confirmed shard-8
executions from 15 or 23 down to 3; and, most substantively of all, that
same fix reintroduced the branch-conflation error (crediting two branches'
executions to one) and still overstated exposure at "0/3" — two of those
three executions crashed in shared setup at `integration_e2e.rs:729:10`,
before ever reaching the tracked assertion at line 3567, so only **one**
execution (`35034838493`) actually exercised it. The honestly supported
figure is **0/1**, not 0/3, 0/15, or 0/23. A ninth round then found that
the stale-heading habit and the "mechanism unknown" label on item 3 had
both survived every prior fix: this report had itself downloaded and
`curl`'d item 3's full failure log (to check item 1's tracked test) without
ever grepping that same log for `quota_enforcement_tests`'s own panic —
which was sitting there the whole time (`quota_enforcement_tests.rs:3329:9`,
an unexplained 10-second target-row timeout, not an unrecoverable
truncation — though a tenth round then caught this report overreaching
past that recovered text into an unverified "the sweep didn't fire"
claim the log doesn't actually support). Each correction
is called out inline below at the point it applies, verified against
source, raw job logs, or direct `curl` of signed log URLs rather than
taken on faith — the pattern this series' own prior reports already
follow.

## 🎯 Verdict path

Same verdict path as the whole series: `ci.yml`'s `pull_request` trigger against
`trunk-dev`, principally the `test-db-linux` and `test`/`test-nodb` matrices.
Branch-protection status for these matrices and `openapi-client-smoke` remains
unconfirmed from this session — still no branch-protection-read tool exposed
here, checked again today. Cache-usage API access is also still unavailable,
checked again today. Both gaps repeat unchanged from every prior report in this
series; not re-diagnosed.

## 🌡️ Symptom

### 1. `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`: 0/1 confirmed assertion exposure, post-merge only — no confirmed pre-merge exposure exists (corrected from an initial "0/15 spanning both windows" — see below)

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

**Correction (post-review):** this paragraph originally claimed all 100 of
the most recent completed `pull_request`-event `ci.yml` runs were
job-logged. False, and this role's own 09-06/09-11 reports already
established why that matters: `list_workflow_runs` enumerated all 100
runs' overall conclusions (55 cancelled, 30 success, 15 failure, for
2026-09-14T19:46:25Z through 2026-09-16T09:33:24Z), but only the 15
explicit-failure runs plus an 8-run cancelled-run sample (23 of 100) were
actually job-logged — see the cancelled-run audit below. The other 77
runs, including 47 cancelled ones never inspected at job level, could in
principle hide a failure this census would miss; a cancelled run's overall
conclusion can absorb a real job failure underneath it, per this series'
own prior findings. Split the 15 explicit failures at the merge instant:

| Window | Span | Runs | Failures |
|---|---|---:|---:|
| Pre-merge | 2026-09-14T19:46:25Z → 2026-09-15T14:00:13Z | 57 (36 cancelled, 16 success, 5 failure) | 5 |
| Post-merge | 2026-09-15T14:00:13Z → 2026-09-16T09:33:24Z | 43 (19 cancelled, 14 success, 10 failure) | 10 |

All 15 failures were individually job-logged (not inferred from branch/message
alone). **0/15 carry the activity-timeout signature**, in either window.

**Correction (post-review) — most of those 15 runs never gave the tracked
test a chance to run at all.** A Codex review correctly pointed out that
"0/15" (and this report's later "0/23") measures how many *job-logged
failures* carry the signature, not how many runs actually **executed**
the test — and those are very different denominators here.
`test-db-linux` (`ci.yml:948-950`) declares `needs: [lint, changes]`, and
GitHub Actions skips a job when a `needs` job fails (no `always()`
override is present for this job). Checked directly via
`list_workflow_jobs` on run `34891219422` (a `Lint`-only failure): its
`Test DB (linux, shard N)` legs all show `conclusion: "skipped"`, not
`"failure"` or `"success"` — confirming the mechanism. Of the 15 explicit
failures, **13 failed inside the `Lint` job itself** (comment-hygiene,
clippy, `cargo fmt`, `sqlite_feasibility_docs`) — for every one of these,
shard 8 (where `integration_e2e.rs`, and thus the tracked test, actually
runs — see the shard-sharding math below) never executed. Those 13 runs
say nothing about whether the tracked signature recurred; they are not
zero-evidence-for-the-flake, they are **no-evidence-at-all**.

Only **2 of the 15** explicit failures had `Lint` succeed and `test-db-linux`
actually run: `35034838493` and `35063499036`. Fetched both runs' shard-8
logs directly (`get_job_logs(..., return_content=false)` for the signed
URL, `curl`, `grep`) to check the tracked test's own outcome, not just the
run's overall conclusion:

| Run | Shard 8 outcome | Tracked test (`worker_fails_workflow_when_activity_start_to_close_timeout_elapses`) |
|---|---|---|
| `35034838493` (2026-09-15T23:15:28Z, post-merge) | failed (`quota_enforcement_tests`, item 3) | **`... ok`** — passed cleanly |
| `35063499036` (2026-09-16T06:23:34Z, post-merge) | failed (12 jobs total — see item 4's correction) | failed, but via `DatabaseError(..., "column harvest_workflow_executions.migrated_run_terminal_at does not exist")` at `integration_e2e.rs:729:10`, the same missing-column defect as `35072771791` below — **not** the tracked signature |

Combined with the cancelled-run audit's `35072771791` (shard 8 also ran,
also hit the identical missing-column defect, not the tracked signature —
see below), this window's **actually-confirmed shard-8 executions total
3, not 15 or 23**.

**Correction (post-review), two more errors in the paragraph above at
first draft.** First — reintroducing a branch-conflation this report's
own item 3 already corrected — `35034838493` runs on
`claude/too-many-lines-followup`; only `35063499036` and `35072771791`
are `claude/hopeful-pascal-tbijcf`'s successive commits. The three
executions span **two branches**, not one: a single sampled run from one
branch, and two successive commits from a different branch. Second, and
more importantly: two of the three (`35063499036`, `35072771791`) panic
at `integration_e2e.rs:729:10`, in the shared workflow-reload helper the
test calls **before** ever reaching the event-history `match` at line
3567 (confirmed directly in both logs above). A test that aborts before
its own assertion runs had **zero opportunity** to exhibit that
assertion's tracked signature — counting those two as "non-recurrences"
overstates the evidence exactly the way an untested code path would.
**Only `35034838493` actually exercised the assertion** (it passed
cleanly, meaning it reached line 3567 and matched one of the two accepted
event shapes). The honestly supported exposure count is **0/1**, not 0/3
— one confirmed opportunity for the signature to appear, and it didn't.
This is a far thinner evidentiary base than any of this report's earlier
"0/15" or "0/23" framings implied: most of the window's 100 runs (all 13
`Lint`-failing explicit failures, most of the 55 cancelled runs per the
audit below, all 30 successful runs — unverified this session whether
their `test-db-linux` step actually executed rather than being skipped as
docs-only — and now also the 2 of 3 "executions" that crashed before
reaching the assertion) give no information either way about whether the
tracked signature recurred.

**This is not the rerun campaign issue #1558 asked for**, and per the
correction above it is much less than the "frequency-in-the-wild over
calendar time" evidence this report originally claimed. **Correction
(post-review):** an earlier draft of this paragraph said "roughly
nineteen and a half hours of real CI traffic post-fix produced zero
recurrences... versus 3 occurrences the day before" — that treats every
hour of elapsed CI time as if it were an opportunity for the signature to
appear, which the correction above already shows is false: the sole
confirmed assertion exposure (`35034838493`, 23:15:28Z) sits over nine
hours *after* the merge, and this report has **no confirmed pre-merge
exposure at all** to compare it against. There is no "19.5 hours of
traffic," only one confirmed data point, well inside the post-merge
window. The comparison to "3 occurrences the day before" (the 09-14
report's genuine, confirmed occurrences) is not undermined by this
correction, but the implied symmetry — hours of quiet traffic on one side
against hours of confirmed occurrences on the other — is. Separately, an
earlier draft of this same paragraph described the 15 explicit failures'
branch spread as "ten unrelated branches' worth of failures" — also
false, though this is about the failure population generally, not about
assertion exposure specifically. The 10 post-merge failures came from only
**5 distinct branches**: `claude/hopeful-pascal-tbijcf` alone accounts for
half of them (5 of 10, all successive commits on one PR as it iterated
through review), with `claude/gifted-mccarthy-25ztga` contributing 2 and
three other branches
contributing 1 each. That materially narrows the independence of this
evidence — five branches iterating, one of them repeatedly, is a much
smaller draw than ten unrelated ones — though it does not overlap with item
1's headline claim itself, since none of those 10 failures (from any branch)
carried the activity-timeout signature regardless of how the branches
cluster. No same-commit rerun was run this session — Docker is unavailable
in this session's sandbox, and dispatching 20 real GitHub-hosted-runner
executions of `test-db-linux` (11 shards) solely to rerun one test is the
kind of ambient, suite-level spend this role's own charter asks to route
through **Ask before** rather than do unilaterally.

**Correction (post-review) — the cancelled-run gap.** A Codex review on
this PR correctly flagged that the headline "0/15" figure only covers the
15 runs whose *overall* conclusion was `failure`, leaving the window's 55
`cancelled` runs unaudited — and this role's own 09-06/09-11 reports
already established that a cancelled run's overall conclusion can absorb a
real job-level failure underneath it (4/10 and 15/54 hit rates in those
samples). Job-logged a sample of 8 of the 19 post-merge cancelled runs at
job level (`list_workflow_jobs`, `perPage=100`, checking every job's own
`conclusion`, not just the run's):

| Run | Branch | Hidden job failures | Signature |
|---|---|---:|---|
| `35072771791` | `claude/hopeful-pascal-tbijcf` | 9 (`Test DB (linux, shard 0/1/2/3/4/7/8/9/10)`) | a missing-column defect (`harvest_workflow_executions.migrated_run_terminal_at` absent from the hand-maintained `INIT_SQL`/`LEGACY_INIT_SQL` test bundles), self-diagnosed and fixed by this same branch's very next commit (`36791bfff0`, visible in this session's own fresh `ci.yml` query) — see below for shard 8 specifically |
| `35060658370` | `claude/hopeful-pascal-tbijcf` | 3 (`Test (windows/ubuntu/macos-latest)`) | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` — the identical missing-migration-row signature already counted as an explicit failure at run `35063499036` on the same branch 40 minutes later; a precursor occurrence of an already-counted defect, not a new one |
| 6 others | mixed | 0 | clean cancellations (matrix jobs show `cancelled`, not `failure`) |

**Correction (post-review) — shard 8 needed checking directly, not
assumed.** A Codex review correctly pointed out that `35072771791`'s
9-shard failure table above named only shard 1's signature (fetched from
its job log) and did not check whether the activity-timeout *test itself*
ran on one of the other 8 failed shards. Per `.github/ci/run-suites.sh`'s
`row_ordinal % SHARD_COUNT` sharding rule and `integration_e2e`'s row
position in `.github/ci/integration-suites.txt` (row ordinal 30 among
`linux`-class rows, `30 % 11 = 8`), `integration_e2e` — the file containing
`worker_fails_workflow_when_activity_start_to_close_timeout_elapses` —
runs on shard 8, which **is** among the 9 failed shards. Fetched shard 8's
full log directly (`get_job_logs(..., return_content=false)` for the
signed URL, then `curl` and `grep`, since the module's ~2800 tests exceed
any reasonable `tail_lines`): `worker_fails_workflow_when_activity_start_to_close_timeout_elapses`
did fail in this run — but at `integration_e2e.rs:729:10`, panicking with
`failed to reload workflow execution: DatabaseError(Unknown, "column
harvest_workflow_executions.migrated_run_terminal_at does not exist")` —
the same missing-column defect that cascaded through essentially every
test in that shard's run (dozens of other tests fail at the identical
line and message in the same log). This is **not** the tracked
event-history-mismatch signature — the panic is at `integration_e2e.rs:729:10`
in a shared setup helper on a `DatabaseError`, not at the assertion's own
`other => panic!("history did not match...")` arm (`integration_e2e.rs:3587`).
**Correction (post-review):** an earlier draft of this paragraph claimed
the tracked signature "can no longer occur on this test at all" post-#1563
— false. Reading the current source directly
(`integration_e2e.rs:3567-3588`): the `match` still ends in a catch-all
`other => panic!(...)` arm, so a third event-history shape neither of
PR #1563's two accepted patterns matches would still trip that exact
panic. PR #1563 narrowed which shapes are accepted, it did not remove the
fallback panic arm — so the tracked signature remains structurally
possible on this test, just not observed in this occurrence. The claim
here is narrower and fully supported by direct inspection: *this specific
occurrence*, on shard 8 of run `35072771791`, failed via the DB error at
line 729 before ever reaching the event-history `match` at line 3567, so
it is not an instance of the tracked signature — confirmed by direct log
inspection, not inferred from the shard-1 sample alone as an earlier
draft did.

Restated precisely: of the 9 hidden `Test DB` failures in `35072771791`,
one (shard 8) happens to include the tracked test by name, but its failure
mode is the same schema-mismatch defect as the other 8 shards, not the
race PR #1563 addressed — so it still does not count as a recurrence of
the *tracked signature*, but "neither hidden failure carries it" (an
earlier draft's wording) glossed over needing to check that directly
rather than assume it from an adjacent shard's log. This is a sample, not
a census: 11 of 19 post-merge cancelled runs and all 36 pre-merge
cancelled runs remain unaudited at job level. The headline "0/15" claim is
therefore better stated as "0 occurrences of the tracked signature among
the runs actually inspected" (15 explicit failures plus this 8-run
cancelled sample, 23 runs total) — directionally consistent with the fix
holding, but not the exhaustive census an earlier draft of this report
implied by calling the 100-run page complete.

### 2. A stale migration count blocked one branch three commits in a row this window, and its panic message still shows the same number on both sides (a separate, unrelated bug)

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
that branch's own then-current diff. **Correction (post-review):** an
earlier draft of this section attributed the three CI round-trips
themselves to the message bug. That overstates it — the three failures
happened because the branch left the documented count stale across three
commits, not because of how the panic message is worded; the assertion
would have failed the same three times with a correctly-worded message.
What the message bug actually costs is diagnostic clarity: whoever reads
the panic cannot tell from it alone that the doc is stale, since it quotes
the same live count on both sides of the sentence, so they have to know to
distrust the sentence rather than being told directly. That cost is real
but qualitative, not the quantified "three round-trips" an earlier draft
claimed. Still below this role's bar to open a fix PR on its own (a
one-line diagnostic fix on a docs-only test, not a suite-health defect).

### 3. `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`: an unexplained 10-second target-row timeout, one occurrence, not clustered

Run `35034838493` (`Test DB (linux, shard 8)`, 00:16:31Z) ended with `FAILED
SUITES: autumn-harvest/integration (linux) -- quota_enforcement_tests` after
every individually-named suite in the visible tail passed — the same
tail-truncation gap the 09-14 report hit on `integration_e2e`'s ~2800-test
module (`tail_lines` ending before the actual panic for a large serial run).
Re-fetched at `tail_lines=200`; the actual failure output was still not in
the visible window.

**Correction (post-review), two passes.** First: a Codex review pointed
out that this report separately downloaded and `curl`'d this exact run's
shard-8 full log (job `104609028589`, to verify the tracked test's
outcome for item 1) without checking that same log for
`quota_enforcement_tests`'s own failure. Grepping it:
`quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
panics at `quota_enforcement_tests.rs:3329:9` with `"target row was never
created by the outbox retry; last count was 1"`. Reading the test source
(`quota_enforcement_tests.rs:3313-3330`): after freeing a quota slot, the
test polls every 50ms, up to a 10-second deadline, for a background
`enforce_completion_triggers_outbox` sweep (folded into the worker's
timeout-checker loop, ticking every `poll_interval`) to retry a deferred
completion trigger and create the target row.

Second: a follow-up Codex review correctly caught that the first pass
overreached. The recovered panic proves only that the target-row count
stayed at 1 until the deadline — `quota_enforcement_tests.rs:3323-3333`
never observes whether the sweep itself ran. The log this session has is
`cargo test`'s own stdout (test names and panic text only; checked
directly — no worker/tracing output is captured for this run), so there
is no evidence either way about whether the sweep executed and failed to
create the row, executed and no-op'd, or never ran at all. Calling this
"a background timer that did not fire" promoted a symptom (the row count
never reached 2) into an unverified mechanism. **The honest description
is an unexplained 10-second target-row timeout** — a real, reproducible
symptom with a real deadline and polling structure, but not yet a
confirmed mechanism. One occurrence is not a rate, and this report does
not render a test-vs-product verdict on it — that would need both the
rerun-campaign rigor this report already declined to substitute for on
item 1, and worker/outbox-level logging this session did not have. It is
a symptom with a plausible timing-category candidate mechanism (a
wait-loop's deadline racing an unobserved background sweep), not
"mechanism unknown" in the sense of a truncated, unrecoverable log — the
failure text and test structure are fully recovered — but it should be
tracked as an open, unverified candidate, not a confirmed root cause.

**Correction (post-review):** an earlier draft of this paragraph said "the
same branch (`claude/hopeful-pascal-tbijcf`) went on to fail three more
times on unrelated deterministic gates... and never reproduced this
signature again" — factually wrong on two counts, verified against this
session's own raw data. First, `35034838493` runs on branch
`claude/too-many-lines-followup`, not `claude/hopeful-pascal-tbijcf` — a
different branch entirely; the two were conflated. Second,
`claude/too-many-lines-followup` has exactly one run in this 100-run
sample (`35034838493` itself), so there is no later activity on *that*
branch to say anything about, "not pursued further" or otherwise.
(`claude/hopeful-pascal-tbijcf` did separately fail 5 times later in this
same window on unrelated gates — item 4's table and the fmt/sqlite/
migration_hygiene entries — but that is a different branch's story, not
evidence about whether `quota_enforcement_tests` recurred.) Corrected: one
occurrence, an unexplained 10-second target-row timeout (above, not a
confirmed mechanism), no branch history
to compare it against, not clustered with anything else in this sample —
recorded per this role's own admissibility rules so a repeat is
recognized as a repeat, not actioned as a rate.

### 4. Remaining 11 failures: 10 deterministic own-branch defects, 1 still unclassified

**Correction (post-review):** an earlier draft of this section named five
signature buckets that summed to 9, not 11 — undercounting the `cargo fmt`
diffs (only one was mentioned; there were three separate occurrences) and
omitting `migration_hygiene` entirely. Corrected, with every one of the 11
runs named:

| Run | Signature |
|---|---|
| `34891219422` | clippy `redundant_clone` (`payload_codec.rs`) |
| `34891509704` | clippy dead-code (`partition.rs`'s `DISABLE_RENAME_SUFFIX`) — root-caused. A second failed job on this same run, `corpus::seeded_corpus_is_clean_under_the_syntactic_layer`, is **not** root-caused here (see correction below) |
| `34924353340` | E0433 `diesel` compile error (2 failed jobs, same signature) |
| `34925551916` | E0433 `diesel` compile error (2 failed jobs, same signature — same root cause as `34924353340`, same branch `claude/bold-lovelace-agnczk`) |
| `34933650236` | `comment-hygiene.py` Tier B (`shard_rebalance.rs` sentence-length) |
| `34986559913` | `cargo fmt` diff (`resolve_fixtures.rs`) |
| `34988937190` | clippy `doc_markdown` (`analysis_fixtures.rs`) |
| `35034267335` | `cargo fmt` diff (`shard_rebalance_db_tests.rs`, `workflow_rerun_integration.rs`) |
| `35045856467` | `cargo fmt` diff (`shard_rebalance_db_tests.rs`, `autumn-harvest-cli/src/lib.rs`, `workflow_filter_integration.rs`) |
| `35060937048` | clippy `map_unwrap_or` (`analyze_profile.rs`) |
| `35063499036` | `migration_hygiene::every_release_migration_is_in_the_upgrade_guide` (missing inventory row) on `Test (macos/windows/ubuntu-latest)` — **correction (post-review):** this run has 12 failed jobs total, not 1. `Lint` itself succeeded on this run (unlike the other 13 rows in this table), which let `test-db-linux` actually execute — and 8 of its shards (including shard 8) then failed on the unrelated missing-column defect from the cancelled-run audit below, confirmed by direct log inspection of shard 8. Counted once here for `migration_hygiene` (the `Lint`-adjacent `Test (<os>)` jobs' signature); the missing-column failures on this same run are the same defect as `35072771791`'s, not a new signature |

By signature: comment-hygiene (1 run), `cargo fmt` (3 runs, not 1 as an
earlier draft implied), clippy (4 runs: `redundant_clone`, dead-code,
`doc_markdown`, `map_unwrap_or`), E0433 `diesel` (2 runs, one root cause),
`migration_hygiene` (1 run, previously unmentioned — though that run also
independently carries the missing-column defect on 8 other jobs, not
counted as a 12th run since it's the same run already in this table).
1+3+4+2+1 = 11 runs,
matching the section heading. The two `diesel`/E0433 occurrences are the
same commit-family defect counted once in the diagnosis below, not two
independent findings.

**Correction (post-review) — the `corpus` failure on `34891509704` is not
root-caused.** An earlier draft of this section named
`corpus::seeded_corpus_is_clean_under_the_syntactic_layer` alongside the
dead-code clippy finding as if diagnosing the clippy lint also accounted
for it. It does not — they are two independent failed jobs on the same
run, and this report never established a mechanism for the `corpus`
failure. Checking this series' own prior work: the immediately preceding
report, `docs/rnd/2026-09-15-ci-health-semaphore-window-census.md:141-144`,
already found this exact signature recurring across 3 of 5 pushes on this
branch (`34891509704` among them) and explicitly logged it as **"not
otherwise diagnosed"** — so this is not a new occurrence this session
found, it is the same still-undiagnosed recurring failure surfacing again
in this window's sample, carried forward rather than freshly root-caused.
Because it has now recurred at least 3 times without a known mechanism, it
is the closest thing in this report's window to a suite-attributable-flake
candidate — though 3 occurrences (all from the 09-15 report, none newly
observed here) is still short of this role's own ≥20-rerun bar for a
measured rate, and no session has yet run the rerun protocol or
signature-clustering work needed to say more. Each of the other 10 runs
in this section traces cleanly to that commit's or branch's own diff; no
suite-state interaction, no timing component, no order dependence.

## 🔍 Diagnosis

**Item 1** is not yet a rendered verdict on PR #1563's fix — that still
requires the rerun campaign issue #1558 asked for and never got.
**Correction (post-review), superseding three earlier drafts of this
sentence:** the first draft called the window a "full (not sampled)
two-day failure census"; the second walked that back to "23 runs actually
inspected"; the third to "3 runs confirmed to execute `test-db-linux`,"
still crediting all three to one branch's successive commits when
`35034838493` is actually a different branch's sole sampled run (two
branches, not one). All three overstate the evidence, and the third
missed a further gap: two of those three runs (`35063499036`,
`35072771791`) panic at `integration_e2e.rs:729:10`, in shared setup,
**before** the test ever reaches the event-history assertion at line
3567 — they had no opportunity to exhibit the tracked signature, so
counting them as non-recurrences is itself an overclaim. Only
`35034838493` actually exercised the assertion, and it passed. The
correct figure is **0/1 confirmed exposure**, not 0/3, 0/15, or 0/23.
That is directionally consistent with the fix holding — the one real
data point does not contradict it — but it is an extremely thin base,
nowhere close to the frequency-in-the-wild framing this report originally
claimed, and nowhere near issue #1558's own ≥20x rerun-campaign bar.
Recorded as a data point for whoever next has the ability to run
the actual rerun campaign, not
claimed as a Tier-1 confirmation.

**Item 2** is the doc-sync gate working as designed: three round-trips in
one window, caused by the branch leaving the documented count stale across
three commits, not by the message wording. **Correction (post-review):**
an earlier draft of this paragraph attributed the three round-trips to the
message-quality bug itself — wrong, as the section above already
corrects; repeated here for consistency. The message bug's actual cost is
diagnostic clarity (the panic quotes the live count on both sides of its
sentence, so a reader can't tell from it alone that the doc is stale), not
the failure count. Still below this role's own bar for a unilateral fix PR
(not a suite-health defect, not a flake).

**Item 3** is explicitly not claimed as a flake — one occurrence.
**Correction (post-review), two passes:** the failure text was
recoverable (above, found by checking a log this session had already
downloaded but not searched for this signature) — a wait-loop polling up
to a 10-second deadline for a background outbox-retry sweep, timing out.
But a second pass caught that recovering the failure text is not the
same as confirming a mechanism: the log proves the target row count
stayed at 1, not that the sweep failed to run. This is correctly an
unexplained target-row timeout with a plausible timing-category
candidate — not "no mechanism" as one earlier draft said, and not a
confirmed mechanism as a later draft overclaimed either. Not clustered
with anything else in this sample.

**Item 4** is the suite working correctly for 10 of its 11 runs, not a
CI-health defect. The two `diesel`/E0433 occurrences are the same
commit-family defect counted once, not two independent findings. The
11th run's second failed job (`corpus` on `34891509704`) is not
root-caused — see the correction in that section — and, unlike item 3,
genuinely has no mechanism recovered (this series' own 09-15 report
already tried and came up empty across 3 occurrences).

**Correction (post-review):** item 3's own text originally said the
`quota_enforcement_tests` panic's mechanism was unrecoverable, then
(overreaching) that it was a confirmed background-timer-didn't-fire
mechanism; corrected above to an unexplained target-row timeout with a
plausible but unconfirmed candidate. The `corpus` failure on
`34891509704` (item 4's correction) genuinely has no mechanism, confirmed
or candidate. Neither is "root-caused" in the sense the 13 deterministic
defects below are. An earlier draft's 🔧/📊 sections below said "15/15
root-caused," which is wrong either way; corrected to 13/15 fully
root-caused and 2/15 with at least one failure that isn't
(`quota_enforcement_tests` — unexplained timeout, no confirmed
mechanism, no verdict; `34891509704`'s `corpus` job — no mechanism at
all — alongside its otherwise-explained clippy failure).

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
   — a diagnostic-clarity cost, not a root cause of the 3 failures
   themselves (those were the stale doc, unfixed across 3 commits); still a
   one-line fix someone should pick up.
5. **The remaining cancelled-run population** — 11 of 19 post-merge and all
   36 pre-merge cancelled runs in this window are still unaudited at job
   level. This role's own 09-06/09-11 reports already flagged that building
   a scheduled harness for this (pulling every job's conclusion for every
   completed run, cancelled or not) is the correct fix for the gap rather
   than repeated manual sampling; still not built by any session.
6. **`corpus::seeded_corpus_is_clean_under_the_syntactic_layer`** — still
   undiagnosed after recurring at least 3 times (per
   `docs/rnd/2026-09-15-ci-health-semaphore-window-census.md`, one of
   which resurfaced in this window's own sample). The closest thing in
   this report's data to a suite-attributable-flake candidate, but at 3
   occurrences it is well short of this role's own ≥20-rerun bar for a
   measured rate; no session has yet run the rerun protocol or
   signature-clustering work this would need.

## 📊 Measurement

- **Item 1: correction (post-review), fourth pass.** Three earlier drafts
  of this line claimed, in turn, "100/100 runs... individually job-logged
  (full census)," then "23 runs actually job-logged, 0/23 carry the
  signature," then "3 runs confirmed to execute `test-db-linux`, 0/3
  carry the signature" (while also wrongly crediting all 3 to one
  branch's successive commits — `35034838493` is actually a different
  branch's sole sampled run). Each overstated the evidence. `job-logged`
  is not `executed the tracked test`: `test-db-linux` needs
  `[lint, changes]` (`ci.yml:948-950`) and is skipped, not run, when
  `lint` fails — confirmed directly via `list_workflow_jobs` on run
  `34891219422`, whose `Test DB` legs all show `conclusion: "skipped"`.
  Of the 15 explicit failures, 13 failed inside `lint` itself, leaving
  only 3 runs confirmed to execute shard 8 at all: `35034838493`
  (`claude/too-many-lines-followup`) and `35063499036` plus the cancelled
  run `35072771791` (both `claude/hopeful-pascal-tbijcf`, successive
  commits). But `executed shard 8` is still not `exercised the tracked
  assertion`: the latter two panic at `integration_e2e.rs:729:10`, in
  shared setup, before the test ever reaches the event-history `match` at
  line 3567 — direct log inspection confirms this, not inference. Only
  `35034838493` actually reached that assertion, and passed. **The
  correct figure is 0/1, not 0/3, 0/15, or 0/23.** The 30 successful runs
  and the remaining 47 unaudited cancelled runs in this window were not
  checked for whether `test-db-linux` actually executed versus was
  skipped, so they add no confirmed exposure either way. Not a
  same-commit rerun — no revert check applies, since no fix was made or
  verified this session.
- **Item 2:** 3/3 occurrences on one branch confirmed identical panic text
  (`"**107 migrations**"` on both sides) via direct job-log inspection.
- **Item 3: correction (post-review), two passes.** 1/1, not a rate. The
  failure text and test structure were recovered by re-checking a log
  this session already had (not "mechanism unknown" as an earlier draft
  said), but a second pass caught that the recovered text only proves the
  target-row count stayed at 1 for 10 seconds — it does not show whether
  the background sweep ran. Correctly stated as an unexplained 10-second
  target-row timeout with a plausible timing-category candidate, not a
  confirmed "sweep didn't fire" mechanism.
- **Item 4: correction (post-review).** 10/11 runs fully root-caused via
  job-log inspection (see the corrected run-to-signature table above);
  the 11th (`34891509704`) has one root-caused job (clippy dead-code) and
  one unclassified job (`corpus`, recurring per the 09-15 report, still
  undiagnosed). 0 confirmed suite-attributable flakes among the
  root-caused failures; `corpus` is an open candidate, not confirmed
  either way.
- **Cancelled-run sample:** 8/19 post-merge cancelled runs job-logged; 2/8
  hid a real job-level failure (9 shards on one run, 3 jobs on another).
  Of the 9 hidden shard failures in the larger run, one (shard 8) included
  the tracked test by name but failed via the same missing-column defect
  as the other 8 shards, confirmed by direct log inspection (panic at
  `integration_e2e.rs:729:10`, a DB error in shared setup, before the
  event-history `match` at line 3567 is ever reached) — not an instance of
  the tracked event-history-mismatch signature. **Correction (post-review):**
  the signature itself remains structurally possible on this test post-#1563
  (the `match` still ends in a catch-all `other => panic!(...)` arm, per
  direct source inspection); only this specific occurrence is confirmed not
  to be one. 0/9 hidden shard failures and 0/3 hidden job failures (the other
  cancelled run) carry the tracked signature. 11/19 post-merge and 36/36
  pre-merge cancelled runs remain unaudited.
- **Combined: correction (post-review).** An earlier draft's "0/15
  suite-level flakes" contradicted item 3's and item 4's own findings —
  a single occurrence, named mechanism or not, is insufficient evidence to
  call it a flake, but equally insufficient to rule one out. Stated
  correctly: 13/15 of this window's explicit failures fully root-caused
  and confirmed non-suite-level; 2/15 have at least one failure short of
  that bar (`quota_enforcement_tests` — an unexplained target-row timeout,
  no confirmed mechanism, no rendered verdict; `34891509704`'s `corpus`
  job — no mechanism at all); 0/13 root-caused failures carry the previously-tracked
  activity-timeout signature, and neither of the other two failures'
  assertion text matches it either. 0/2 hidden cancelled-run failures (of
  the 8-run sample actually inspected) carry it either. **None of this
  changes item 1's own
  denominator correction above**: these 15/2 counts are over runs that
  failed for *some* reason, not runs that actually reached the tracked
  assertion — the operative figure for "did the flake recur" is item 1's
  0/1 confirmed exposure, not any count phrased in fifteenths (or thirds).

## 🔬 Reproduce

```sh
# Run-conclusion enumeration (not a job-level census -- see corrections
# above): actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"},
#   perPage=100) on autumn-foundation/autumn-harvest, captured 2026-09-16 --
# one page covered 2026-09-14T19:46:25Z through 2026-09-16T09:33:24Z (100
# runs: 55 cancelled, 30 success, 15 failure). Only the 15 failures plus an
# 8-run cancelled sample (23 of 100) were job-logged below the run level.

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

# Item 3's mechanism (recovered, not left as an unclassified truncation
# gap): the tail_lines=200 API view still ends at "FAILED SUITES:" with no
# panic detail -- same class of gap the 2026-09-14 report hit on
# integration_e2e -- but this report separately downloaded shard 8's full
# log (job 104609028589) via get_job_logs(..., return_content=false) + curl
# to check item 1's tracked test. Grepping that same full log:
grep -n "completion_trigger_defers_to_outbox_when_target_quota_exceeded" \
  /tmp/run35034838493_shard8.log
# -> panics at quota_enforcement_tests.rs:3329:9, "target row was never
#    created by the outbox retry; last count was 1" -- a 10s poll-loop
#    that never observed count reach 2. Source: quota_enforcement_tests.rs:3313-3330.
# NOTE: this log is cargo test's own stdout only (checked directly --
# no worker/tracing output captured), so it proves the row count stayed
# at 1, not whether the background sweep itself ran. Do not overclaim a
# confirmed "sweep didn't fire" mechanism from this alone.

# Branch-protection / cache-usage tool availability: re-checked via
# ToolSearch("branch protection rules github") and
# ToolSearch("actions cache usage github") this session -- neither tool
# is present in the available GitHub MCP surface, unchanged from prior
# reports.

# Cancelled-run job-level audit (8 of 19 post-merge cancelled runs):
# list_workflow_jobs(resource_id=<run_id>, perPage=100) for each of
#   35080087437 35072771791 35070284413 35063265350 35062316533
#   35062123078 35060658370 35040749722
# then filter jobs whose own "conclusion" is "failure" (not the run's
# overall conclusion). Found on 35072771791 (9 Test DB shards) and
# 35060658370 (3 Test <os> jobs); the other 6 were clean cancellations.
# get_job_logs(job_id=104728768074, return_content=true, tail_lines=80)
#   and get_job_logs(job_id=104687245341, return_content=true,
#   tail_lines=60) confirm the shard-1 and Test-<os> signatures.

# Shard-8 direct check (the Codex-flagged gap): confirm which shard
# integration_e2e.rs actually runs on, rather than assuming from shard 1's
# log:
awk '$1=="linux"{c++} $0 ~ /integration_e2e/ && $1=="linux"{print c-1}' \
  .github/ci/integration-suites.txt   # -> 30 (0-indexed row ordinal)
python3 -c "print(30 % 11)"           # -> 8 (SEMAPHORE_SHARD_COUNT=11)
# Then, since the ~2800-test module exceeds any reasonable tail_lines:
# get_job_logs(job_id=104728768233, return_content=false) for the signed
#   logs_url, curl it directly, and grep:
grep -n "worker_fails_workflow_when_activity_start_to_close_timeout_elapses" shard8.log
grep -n "panicked at\|does not exist" shard8.log
# -> panics at integration_e2e.rs:729:10 with the same
#    "column harvest_workflow_executions.migrated_run_terminal_at does not
#    exist" DatabaseError shared by dozens of other tests in the same log --
#    the missing-column defect, not the tracked event-history assertion.

# corpus::seeded_corpus_is_clean_under_the_syntactic_layer prior-occurrence
# check (the Codex-flagged root-cause gap):
grep -n "seeded_corpus_is_clean_under_the_syntactic_layer" \
  docs/rnd/2026-09-15-ci-health-semaphore-window-census.md
# -> that report's own text (lines 141-144) already found this signature
#    recurring across 3 pushes, including run 34891509704, and explicitly
#    logged it as "not otherwise diagnosed" -- confirming this session's
#    occurrence is the same still-open item, not independently root-caused
#    by this report's clippy finding on the same run.

# Remaining 11 post-merge and all 36 pre-merge cancelled runs: not audited
# this session.

# The "job-logged != actually ran" denominator check (the Codex-flagged
# gap): confirm test-db-linux's needs graph, then verify skip-on-failure
# directly on one Lint-only failure:
grep -n "needs:" .github/workflows/ci.yml
sed -n '940,955p' .github/workflows/ci.yml   # test-db-linux: needs: [lint, changes]
# list_workflow_jobs(resource_id=34891219422, perPage=100) -> every
#   "Test DB (linux, shard N)" leg shows conclusion "skipped", confirming
#   Lint failures give zero exposure to the tracked test.
# Repeated the same list_workflow_jobs check for the other 12 Lint-failing
# runs of this report's 15 explicit failures (all skip test-db-linux the
# same way); the only 2 explicit failures where Lint succeeded and
# test-db-linux actually ran are 35034838493 and 35063499036.
# get_job_logs(job_id=104609028589, return_content=false) -> shard 8 of
#   35034838493; curl + grep "worker_fails_workflow_when_activity_start_to_close_timeout_elapses"
#   -> "... ok" (clean pass).
# get_job_logs(job_id=104694905315, return_content=false) -> shard 8 of
#   35063499036; curl + grep "does not exist" -> same missing-column
#   defect as 35072771791, 106 occurrences in the log.

# The "executed shard 8" != "exercised the assertion" check (a further
# Codex-flagged gap): confirm exactly where each of the 3 executions'
# panic/pass actually happened relative to the test's own assertion.
grep -n "panicked at" /tmp/run35063499036_shard8.log | \
  grep "worker_fails_workflow_when_activity_start_to_close_timeout_elapses"
# -> integration_e2e.rs:729:10 (the shared reload helper), same for
#    35072771791's shard8.log. Compare against the assertion's own
#    location and catch-all arm:
sed -n '3560,3588p' autumn-harvest/tests/integration/integration_e2e.rs
# -> the match starts at 3567, the catch-all panic arm is at 3587-3588 --
#    both occurrences panicked at 729, well before reaching 3567, so
#    neither had the opportunity to hit 3587. Only 35034838493's shard 8
#    (branch claude/too-many-lines-followup, not hopeful-pascal-tbijcf --
#    corrected after this report initially credited it to the wrong
#    branch) actually reached and passed the assertion.
```
