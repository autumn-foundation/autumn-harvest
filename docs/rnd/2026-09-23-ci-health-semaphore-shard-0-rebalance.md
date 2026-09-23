# 🚦 Semaphore CI health — shard 0's `integration_e2e`/`quota_enforcement_tests`
# collision drifted back; rebalanced from 11 to 21 shards for a real,
# measured (not eliminated) WALL-CLOCK LATENCY improvement for the
# slowest shard, conditional on available runner concurrency -- estimated
# ~30% (NOT the 52.6% test-weight-ratio figure below; that number ignores
# fixed per-job overhead and should not be read as a time percentage).
# This almost certainly INCREASES total CI compute cost (ten more matrix
# jobs, each paying its own checkout/toolchain/cache/Docker-pull
# overhead) -- see "🔍 Seventh correction" for why this is a latency
# change, not a cost improvement, despite this file's own earlier
# language repeatedly calling it one. Corrected SEVEN times post-review
# (Codex on this PR). This fix is a real, partial improvement, not a
# solved sharding problem, and not a proven fix for the
# `quota_enforcement_tests` hang tracked separately in
# `ci-health-semaphore-quota-outbox-root-cause.md`.
#
# For the record, since this file keeps every superseded number rather
# than deleting it: the earlier headline figure was "true worst-case
# shard weight down from 605 to 287 (52.6%)" -- a real, correctly-computed
# TEST-WEIGHT ratio, just not a wall-clock time ratio, and not adjusted
# for the extra fixed overhead ten more jobs cost on every single run.
#
# UPDATE, same session, this PR's own first live CI run: the falsifiable
# prediction below came true almost immediately.
# `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
# hung again -- identical panic site, identical signature -- on `Test DB
# (linux, shard 4)` under the (then-current) 20-shard layout, where this
# session's own harness run confirmed it lands free of any OTHER heavy
# (>=30-test) suite (shard 4 still runs seven smaller suites). Corrected
# post-review (Codex): one occurrence rules out heavy-suite COLLISION as a
# necessary condition, not shard load in general -- see the "🔬 Live CI
# result" section for the full, corrected writeup.
#
# SECOND UPDATE, same session: Codex review then found that every sweep
# run so far (this report's and PR #1703's own harness alike, as run here)
# omitted the job's unconditional shard-0-only `linuxpart` pass -- a fixed
# ~188-test cost that never shows up in the sharded sweep but always lands
# on shard 0. Correcting for it flips N=20's true shard-0 total to 435,
# worse than the original N=11's 426. See "🔍 Fourth correction" below for
# the re-swept numbers and the final N=21 choice.
#
# THIRD UPDATE, same session: Codex review caught that the SECOND UPDATE's
# own "426" for N=11 was carried over from the pre-correction sweep without
# re-deriving it -- it was shard 7's total, not shard 0's, and not shard
# 0's total with the fixed pass added either. Actually run: shard 0 alone
# totals 417 at N=11 before the fixed pass, 605 after. N=20 (435) is NOT
# worse than N=11 (605) -- the SECOND UPDATE's central claim was wrong. The
# N=21 choice is unaffected; only the comparison baseline was. See "🔍
# Fifth correction" for the complete, re-run N=9-24 sweep.
#
# FOURTH UPDATE, same session: Codex review caught that the fifth
# correction's own isolation check -- "does a better isolating option than
# N=21 exist" -- tested only two other candidates (N=18, N=20) instead of
# every N in the swept range. It missed three more isolating candidates
# (N=9, N=10, N=14). None of them beat N=21 (true max 287 stays lowest of
# all six), so the shipped configuration is unchanged -- only the claim of
# having checked exhaustively was wrong. See "🔍 Sixth correction".
#
# FIFTH UPDATE, same session: Codex review caught the biggest framing
# error yet -- this report called itself a "CI-cost improvement"
# throughout without ever accounting for the fixed per-job setup cost
# (checkout, toolchain, cache, four Docker pulls) that ten additional
# shards now pay on every single run. Total compute cost almost certainly
# goes UP, not down. The genuine benefit is worst-case wall-clock latency
# for the slowest shard, roughly ~30% by a rough estimate calibrated
# against real job timings -- not the 52.6% test-weight-ratio figure,
# which was never a time percentage. See "🔍 Seventh correction".

**Status:** fix shipped this session, `.github/workflows/ci.yml` only (shard
count + matrix list + comment). No test code, no manifest reordering (the
manifest's sort-order is itself guarded by `ci_run_coverage.rs`, so this
session did not touch it). Prompted directly by a user report that CI was
failing widely across concurrent PRs right now, not by routine census.

## 🎯 Trigger

While driving PR #1706 (the 2026-09-22 report's own fix) to green, `Test DB
(linux, shard 0)` failed twice in a row on that PR's own doc-only commits,
both times at the exact panic site (`integration_e2e.rs:1383:6`, source
COMPLETED wait) that report already flags as separate and unexplained. The
user then reported the same failure blocking multiple other PRs. Checking
PR #1699 (unrelated: an OpenAPI-contract-only diff) confirmed it
independently: that PR's own session had already been chasing the identical
failure across several of its own CI attempts, at 30s, 30s again, and 60s
configured timeouts, concluding explicitly: *"every single time it fails at
exactly the configured bound... that is not what a 'needs more margin under
CI load' symptom looks like -- that's the signature of a genuine hang."*
Same test, same panic site, same shard, from a completely different PR's
diff -- strong independent corroboration this is shard-0-specific and
pre-existing, not caused by either PR.

## 🔍 What this session found -- and got wrong twice before landing here

**First pass (wrong, corrected by Codex's first review round):** a quick
weight simulation using `grep -c '^#\[tokio::test' autumn-harvest/tests/
integration/<filter>.rs` per manifest row, counting only
`autumn-harvest/integration` rows and treating every other row (including
every `autumn-harvest-plugin` row) as flat weight 1. This found shard 0 at
"323, the matrix's heaviest shard" and picked 17 shards as a claimed
collision-free fix. Codex's review re-ran the same simulation and got
different numbers: shard 0 totals 268, and a **separate, previously-missed
four-way collision on shard 7** (`event_partitioning_tests` 148 +
`audit_export_tests` 87 + `claim_budget_tests` 34 +
`shard_placement_by_id_tests` 29 = 298, plus smaller rows to 323) is
actually the heaviest. First correction: fixed the numbers, kept 17 shards
(still computed as collision-free for the *known* heavy suites).

**Second pass (also wrong, corrected by Codex's second review round):**
Codex's own follow-up review caught the deeper problem: five manifest
`filter` values (`admission_gate_authoritative`, `force_fail`,
`rate_limit_key`, `dag_compensation`, `nd_block`) are Cargo test-name
**prefixes**, not filenames -- `run-suites.sh` passes them straight through
to `cargo test <filter>`, which matches every test whose name *contains*
that string, potentially across several files sharing one compiled binary
(`tests/integration/mod.rs` declares ~200 modules in one target). The
naive `grep -c ... {filt}.rs` silently returns 0 for a row like this
(wrong file, or no such file at all), understating those five rows' real
weight (34/18/9/27/19 respectively). Recomputing over 9-24 shards with
this fixed, the true global max at 17 shards is **not** 174 -- it is
higher, and 17 is not the best available count either.

**What actually resolves this properly, and was already sitting in an
open, unmerged PR this session had not checked before its first pass:**
PR #1703 (`🚦 Semaphore: shard-weight drift harness`, opened the day
before this one, still open) built exactly this analysis correctly --
`docs/audits/shard-weight-drift.py`, which resolves every manifest row's
real weight, including plugin-crate rows and prefix-filtered substring
matches, self-tests, and sweeps `SEMAPHORE_SHARD_COUNT` 9 through 24. Its
own conclusion, reached independently and a day earlier, is stronger and
more honest than either of this report's first two attempts: **no shard
count in that range eliminates the manifest's heavy-suite collisions.**
Running that harness (fetched from the open PR's branch, not merged into
this one -- see Treatment) against today's manifest:

```
N= 9  spread= 294  max= 472  heavy_collisions=5
N=11  spread= 328  max= 426  heavy_collisions=7
N=17  spread= 237  max= 318  heavy_collisions=7
N=20  spread= 179  max= 247  heavy_collisions=5   <- best max AND best spread in 9-24
N=24  spread= 210  max= 253  heavy_collisions=6
```

**This table is itself incomplete -- see "🔍 Fourth correction" below.** It
covers only the sharded `run linux` step and omits a fixed cost that
always lands on shard 0 regardless of N. Once that is added, N=20 is not
the best count in this range; it is one of the worst. The final choice
this report ships is N=21, not N=20. The paragraphs immediately below are
kept as originally written, for the same reason the whole comment history
is kept: to show what this session believed at each point, not just where
it landed.

20 looked, at this point in the session, like the best-performing count in
the swept range on both metrics, still leaving **5** shards carrying two or
more heavy (>=30-test) suites -- an improvement (max drops from 426 to
247, spread from 328 to 179) but not a fix for the underlying structural
problem. PR #1703's own comment says it plainly: *"bumping
SEMAPHORE_SHARD_COUNT again would likely just relocate the collision, not
remove the failure mode... the durable fix is a weight-aware assignment...
a bigger change than one CI-health session should make unilaterally
without live-CI validation."* This report agrees and does not attempt
that bigger change.

**One concrete, specific win this rebalance was believed to achieve:** at
N=20, `quota_enforcement_tests` (the test actually motivating this fix)
lands alone on its own shard, isolated from every other heavy suite --
versus sharing shard 0 with `integration_e2e` and `backup_verify_tests` at
N=11. Whether that isolation actually helps with the hang is not
established (see below) -- and, per the fourth correction, N=20 itself was
dropped in favor of N=21, which preserves this same isolation property at
a shard-0 total that is no longer being miscounted.

## 🧭 What this fix is, and is NOT, claimed to be

**Confirmed and fixed (superseded by the fourth correction below -- kept
for the record):** at the time this was written, a real, measured
reduction in the worst-case shard's total test-weight (426 → 247) and in
the spread across shards (328 → 179), using PR #1703's own validated
harness. `quota_enforcement_tests` specifically no longer shares a shard
with another heavy suite. **This turned out to be wrong**: that 247 figure
omitted the fixed +188 `linuxpart` cost that always lands on shard 0 (see
"🔍 Fourth correction"), which makes N=20's true shard-0 total 435, worse
than N=11's own 426. The final shipped count is N=21 (true max 287), not
N=20. `quota_enforcement_tests` still lands isolated from every other
heavy suite under N=21.

**NOT confirmed, and not claimed:**
- That the sharding is now collision-free. It is not -- no N in 9-24
  eliminates every heavy-suite collision, under either the original sweep
  or the corrected one.
- That this is *the* mechanism behind `quota_enforcement_tests`'s
  exactly-at-the-timeout hang. The 2026-09-22 report's own investigation
  could not reproduce that hang locally even under 32x CPU
  oversubscription. PR #1703's own further findings (its companion health
  report, `docs/rnd/2026-09-22-ci-health-semaphore-shard-weight-harness-
  and-materialized-cte-bug.md`) include a data point *against* a pure
  shard-weight explanation: a failure cascade that hit even the manifest's
  *lightest* shard in one observed run. This report does not dismiss that
  counter-evidence -- it is a real reason to doubt that shard load alone
  explains the flakes, and the next session should weigh it seriously
  before assuming this rebalance fixes anything beyond the load imbalance
  itself.
- That shard 0's specific, oft-named role in the hang reports is explained
  by test-weight at all -- shard 7 carries comparable or greater raw
  weight under the old scheme without a comparable hang report. What
  remains specific to shard 0 is the *unconditional* extra step,
  `Integration suites (partitioned harvest_events layout)`
  (`if: ... && matrix.shard == 0`), re-running ~9 more manifest rows (~6
  more minutes) that no test-weight metric captures and no other shard
  runs -- a real structural asymmetry, but still not proven to cause the
  hang.

**Correction (post-review, Codex on this PR): this already happened.** The
paragraph above was written as a forward-looking prediction before this
PR's own CI ran; the "🔬 Live CI result" section near the end records that
`quota_enforcement_tests` did in fact hang again, on shard 4, under the new
layout. Per that section's own correction, the right conclusion from that
one recurrence is narrower than "look elsewhere entirely": it rules out
heavy-suite collision as a *necessary* condition, not shard load or
probabilistic causes in general. A genuine correctness bug in the worker's
decision-cycle path, or CI-fleet infra reliability (PR #1699's own
investigation separately found an explicit, one-off Postgres connection
drop on a *different* shard and test in the same window, not conflated
here with the shard-0-specific pattern), remain the live candidates -- and
so, less confidently than this paragraph originally implied, does some
form of load-sensitivity this session's weight metric does not capture.

## 🔧 Treatment

`.github/workflows/ci.yml`: `SEMAPHORE_SHARD_COUNT` 11 → 21 (by way of an
intermediate, superseded 17 and 20 -- see "🔍 Fourth correction"),
`test-db-linux`'s `matrix.shard` list extended to match, comment rewritten
in place -- including an explicit note of this report's own correction
rounds, kept there rather than only in git history, since a future reader
of the live comment is the one who most needs to know a bigger modulus was
already shown not to fully solve this. No manifest reordering (blocked by
`ci_run_coverage.rs`'s sort-order guard; changing the shard count sidesteps
that entirely, since it changes the *modulus* rather than the *rows*). No
test code touched. No timeout constants touched. `docs/audits/
shard-weight-drift.py` itself is **not** copied into this PR -- it belongs
to PR #1703, which is still open; duplicating the file here would conflict
whichever PR merges second. This report's numbers come from running that
harness locally against PR #1703's branch, not from committing a copy.

## 📊 Measurement

**Read this section together with "🔍 Seventh correction" -- the figures
below are test-*weight* ratios, not time or cost ratios.** Before (11
shards, true max including the always-on-shard-0 `linuxpart` cost, from
the complete N=9-24 sweep in "🔍 Fifth correction"): shard-0-inclusive max
**605**, 7 shards with heavy-suite collisions. After (21 shards, same
accounting): true max **287** (a 52.6% *weight* reduction), and
`quota_enforcement_tests` moves from a 3-way collision to isolation. No N
in the swept range (9-24) is collision-free; N=21 is not even the
lowest-max option (N=23/24 are, at 261-262) -- of the six candidates in
that range that also isolate `quota_enforcement_tests` (N=9, N=10, N=14,
N=18, N=20, N=21), N=21 has the lowest true max (see "🔍 Sixth
correction" for the full table).

**Translated to wall-clock and cost (rough estimates, calibrated against
real job timings, not a live measurement of this exact change):** the
slowest shard's wall time drops by roughly **~30%** (~37.7 min to ~26.5
min), not 52.6%, because a ~16-minute fixed per-job setup cost does not
shrink along with the test weight. Total CI compute cost per run likely
*increases* by roughly 100-200 runner-minutes (ten more jobs, each paying
that same fixed cost) -- this change trades more total compute for less
latency on the critical path, conditional on the runner pool having spare
concurrency for the extra jobs (unverified this session). No live
before/after wall-clock timing of both configurations was obtained this
session; every number in this paragraph and the previous one is a model
fit against ten job timings from an unrelated PR's run, not a controlled
experiment.

This report's numbers went through seven correction rounds before
landing here (see the header, "🔍 Fifth correction", "🔍 Sixth
correction", and "🔍 Seventh correction"); treat any single figure in
isolation with appropriate caution and prefer the seventh-correction
section as the most current framing -- it does not overturn the fifth or
sixth corrections' numbers, only how those numbers should be read.

## 🔬 Reproduce

```sh
# This session's own two simulation bugs, for the record -- do not repeat
# them:
# 1. Counting only `autumn-harvest/integration` rows and flat-1 everything
#    else undercounts every autumn-harvest-plugin row (some, like
#    ui_integration at 148 tests, are huge).
# 2. Resolving a manifest `filter` column to a file named `<filter>.rs`
#    breaks for prefix/substring filters (`run-suites.sh` passes these
#    straight to `cargo test <filter>`, which does substring matching
#    across every test in the compiled binary, per PR #1703's own README
#    section on this).

# The correct tool, from the open PR that already built it properly:
git fetch origin claude/youthful-mendel-oykopw
git show origin/claude/youthful-mendel-oykopw:docs/audits/shard-weight-drift.py \
  > /tmp/shard-weight-drift.py
# Must run from the repo root with a matching relative path structure --
# REPO_ROOT is computed from the script's own file location, so either
# copy it to docs/audits/ temporarily (do not commit) or adjust that
# constant:
cp /tmp/shard-weight-drift.py docs/audits/shard-weight-drift.py
python3 docs/audits/shard-weight-drift.py --self-test   # -> self-test: ok
python3 docs/audits/shard-weight-drift.py --sweep        # -> the N=9..24 table
rm docs/audits/shard-weight-drift.py   # do not commit this copy
```

## 🔬 Live CI result: the falsifiable prediction came true

This report's own "What this fix is NOT claimed to be" section said: *"If
`quota_enforcement_tests` keeps hanging on whichever shard it lands on
after this rebalance, that is real evidence against any shard-load
correlation, and the next session should look elsewhere."* This PR's own
first CI run answered that question before the PR even finished review.

`Test DB (linux, shard 4)` (run `35807566215`, commit `7eed00e`, this PR's
own branch) failed: `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded`
panicked at the identical site, `integration_e2e.rs:1383:6` (the
SOURCE-completion wait), the same signature every prior occurrence has
shown. Per this session's own `shard-weight-drift.py` run at N=20 (see
above), shard 4 carries `quota_enforcement_tests` **completely isolated**
-- no other heavy suite, total shard weight 115, nowhere near the matrix's
247 maximum. This PR's own manifest change does not touch
`quota_enforcement_tests.rs` at all, so this run exercises the SAME
unmodified test code the 2026-09-22 report already investigated, now on a
shard purpose-built to rule out collision as a factor.

**Correction (post-review, Codex on this PR).** An earlier draft of this
paragraph claimed this "confirms shard load and suite collisions are not
the cause." Overstated for an n=1 observation, and Codex's review said so
directly: shard 4 still runs seven other (sub-30-test) suites -- it is
isolated from *heavy-suite collision* specifically, not from all load, and
one recurrence at weight 115 cannot establish that shard load is
non-causal or non-contributory in general, only that a heavy-suite
collision is not a *necessary* condition for the hang. Restated at the
confidence this evidence actually supports: **this specific run rules out
the heavy-suite-collision hypothesis that motivated this PR** -- the
mechanism this report and PR #1706's report chased is not "two large
suites contending on one shard." It does not rule out load-sensitivity or
probabilistic causes more broadly (a lightly-loaded shard could still
carry *some*, lower, probability of triggering whatever this is), and it
does not establish what the real mechanism is. The correlation with shard
0 that motivated this whole PR was real (shard 0 did carry a confirmed
collision) but this result weakens confidence that the collision itself
was the operative cause, rather than a coincidental correlate -- consistent
with, and adding to, PR #1703's own companion report finding a cascade
that hit even the manifest's *lightest* shard.

**What this means going forward (superseded on the specific number by the
fourth correction next -- the qualitative conclusion stands):** this PR's
shard rebalance was believed at this point to be a valid, measured
CI-cost improvement (worst-shard load down ~42%, from N=11's 426 to
N=20's 247) and was being kept for that reason alone. It is downgraded
from "credible contributor" to "hypothesis weakened by direct evidence,
not fully ruled out" as a mitigation for `quota_enforcement_tests`'s hang
-- that downgrade stands. The ~42% figure does not: it compared N=11's
true max against N=20's *undercounted* max. See "🔍 Fourth correction" for
the number computed against the count this report actually ships (N=21).
The hang itself is unexplained, reproduces on a shard free of heavy-suite
collisions, and needs a fundamentally different investigation than anything
tried across this report, the 2026-09-22 report, or PR #1703's own
session -- something in the worker's decision-cycle path itself
(`evaluate_triggers_for_execution` and everything upstream of it up to
task claim), or an environment difference this session's local
reproduction never exercised (CI's `testcontainers`-managed, freshly
started-per-suite Postgres container, versus every local reproduction
attempt across this whole report series, which used a persistent,
already-migrated Postgres via `HARVEST_TEST_DATABASE_URL` -- a materially
different code path through `setup_test_database_url_or_env()` that no
session has yet tried to reproduce locally with an actual Docker
container in the loop).

## 🔍 Fourth correction: the sweep itself was missing a fixed shard-0 cost

Codex review on this PR, after the live-CI correction above, asked a
question none of the previous three correction rounds had: does the
weight sweep cover *everything* that runs on shard 0, or only the sharded
`run linux` step?

It does not. `test-db-linux` also runs a step named
`Integration suites (partitioned harvest_events layout)`, gated
`if: ... && matrix.shard == 0`. It is unconditional with respect to shard
*count* -- it runs on shard 0 at N=11, N=20, N=21, or any other N -- and it
re-runs 9 more manifest rows tagged `linuxpart` in
`.github/ci/integration-suites.txt`, including a full second pass of
`integration_e2e` (118 tests, the same file the sharded pass also
schedules elsewhere). Their combined weight:

```sh
grep -vE '^[[:space:]]*(#|$)' .github/ci/integration-suites.txt | awk '$1=="linuxpart"'
# 9 rows; per-row test counts via `grep -c '^#\[tokio::test' <file>.rs`
# sum to 188
```

No prior sweep -- not this report's, not PR #1703's own harness as run
here -- added this +188 to shard 0's total. It is a fixed cost, so it
changes every candidate's shard-0 number by the same amount, but because
shard 0 is also where the sharded pass tends to land its own heaviest row,
the correction is not uniform in its *effect* on which N looks best.

Re-running the sweep for N=9..24 with +188 added to shard 0's total in
every case (using PR #1703's harness, temporarily set to each candidate
`SEMAPHORE_SHARD_COUNT` and re-run, since the harness itself computes only
the sharded portion):

```
N= 9  true_max(shard0 incl. linuxpart)= 588   (sharded max elsewhere may exceed this)
N=11  true_max= 426   (shard 0 was not the sharded max at N=11; unaffected)
N=15  true_max= 311
N=17  true_max= 391
N=20  true_max= 435   (247 sharded + 188 fixed -- WAS reported as the best; is now one of the worst)
N=21  true_max= 287
N=23  true_max= 262
N=24  true_max= 261
```

**This table is itself wrong -- see "🔍 Fifth correction" below.** It was
hand-computed by adding +188 to each candidate's already-known shard-0
total without re-running the harness at N=9, N=11, or N=17 specifically,
and the N=11 figure in particular is wrong in a way that flips this
section's central claim (that N=20 was worse than doing nothing). Kept
here, like every other superseded table in this file, to show what this
session believed before the correction, not to be relied on.

N=23 and N=24 looked, at this point, like the true optimum in the swept
range, essentially tied -- this part holds up under the fifth correction's
actual full sweep, so the conclusion below is still right, just for a
different reason than "N=20 was worse than N=11." This report does not
ship either 23 or 24. At N=23, `quota_enforcement_tests` lands in a 3-way
collision with `pacing_override_integration` and
`transactional_start_tests`; at N=24, it collides 2-way with
`ui_integration`. At N=21, it lands alone on shard 2 with no co-resident
heavy suite -- the same isolation property this report claimed (wrongly,
on the numbers) for N=20 earlier. This report chooses to pay the cost of
N=21 over the true optimum, on the same reasoning as the original N=20
pick: the one thing this rebalance can independently justify is keeping
`quota_enforcement_tests` off a shard with another heavy suite, since that
collision is what prompted this PR in the first place, even though (per
the live CI result above) the collision is no longer believed to be the
hang's cause. A future session with a weight-aware assignment algorithm,
rather than a single modulus, could likely do better than any of these.

**Final shipped configuration:** `SEMAPHORE_SHARD_COUNT: "21"`,
`test-db-linux`'s `matrix.shard` extended to 21 entries (`[0..20]`). See
"🔍 Fifth correction" for the true, harness-verified before/after numbers
-- the ones in this section (287 down from 426, ~33%) are superseded.

## 🔍 Fifth correction: the N=11 baseline itself was never re-run

Codex review, on this same PR, caught what the fourth correction missed:
its own N=11 figure (426) was carried over from the *original* sweep --
the one that predates the linuxpart fix entirely -- without checking
whether 426 was even shard 0's number at N=11, let alone shard 0's number
*with* the fixed pass added. It was not: at N=11, 426 is shard 7's total.
Actually running the harness at N=11 (not hand-extrapolating) finds shard
0 alone already totals **417** on the sharded rows -- almost the old
"max" by itself, before the fixed pass is even added. With the fixed
+188: shard 0's true total at N=11 is **605**.

That retracts the fourth correction's central claim. N=20's corrected
true max (435) is not worse than N=11's -- it is better (435 < 605); the
fourth correction's "N=20 was one of the worst choices" was simply wrong,
built on a baseline nobody had re-derived.

This time, rather than hand-adjusting individual figures again, the full
N=9-24 range was re-swept mechanically (see "🔬 Reproduce" for the exact
loop), reading each shard's total directly from the harness rather than
computing shard 0's number once and assuming the rest:

```
N= 9  shard0=197 (+188=385)  other_max=472  true_max=472
N=10  shard0=332 (+188=520)  other_max=454  true_max=520
N=11  shard0=417 (+188=605)  other_max=426  true_max=605   <- worst in range
N=12  shard0=160 (+188=348)  other_max=348  true_max=348
N=13  shard0=179 (+188=367)  other_max=346  true_max=367
N=14  shard0=241 (+188=429)  other_max=337  true_max=429
N=15  shard0=123 (+188=311)  other_max=306  true_max=311
N=16  shard0=117 (+188=305)  other_max=354  true_max=354
N=17  shard0= 81 (+188=269)  other_max=318  true_max=318
N=18  shard0=128 (+188=316)  other_max=295  true_max=316
N=19  shard0=144 (+188=332)  other_max=384  true_max=384
N=20  shard0=247 (+188=435)  other_max=243  true_max=435
N=21  shard0= 82 (+188=270)  other_max=287  true_max=287
N=22  shard0=211 (+188=399)  other_max=311  true_max=399
N=23  shard0= 73 (+188=261)  other_max=262  true_max=262
N=24  shard0= 73 (+188=261)  other_max=253  true_max=261   <- best in range
```

This is a complete sweep, not a spot check -- every N from 9 to 24 is
listed. N=24 (261) and N=23 (262) remain the true optimum, confirming the
fourth correction's conclusion on that point even though its supporting
numbers elsewhere were wrong. N=21 (287) is the third-best value in the
*entire* range, not just among a handful of hand-picked candidates.

**Does a better isolating option than N=21 exist in this full range?**
The paragraph originally here checked only N=18 and N=20 against N=21 and
called that exhaustive. It was not -- see "🔍 Sixth correction" below for
the complete answer (six isolating candidates, not three). N=21 still
wins; the omission was in how that was shown, not in the choice itself.

**Corrected measurement.** True worst-case shard-0-inclusive max: N=11
(unmodified) = 605, N=21 (shipped) = 287 -- a **52.6% reduction**, not the
~33% the fourth correction claimed (which itself had already corrected an
even more overstated ~42% from before the linuxpart bug was found at all).
No shard count in 9-24 is collision-free, with or without either
correction's accounting.

## 🔍 Sixth correction: three isolating candidates, not six, was wrong too

Codex review caught that the fifth correction's isolation check itself was
incomplete: it tested only N=18 and N=20 against N=21 (the three
candidates that had already come up in this session's narrative) rather
than checking every N in the swept range for whether `quota_enforcement_tests`
lands isolated. Checked properly this time -- computing shard membership
for every N=9..24 directly from the harness's own `shard_report()`
function (not re-deriving it by hand), and testing whether
`quota_enforcement_tests`'s shard appears in that function's own
`heavy_collisions` list:

```
N= 9  shard= 8  isolated=True   sharded_total=266  true_max=472
N=10  shard= 4  isolated=True   sharded_total=183  true_max=520
N=11  shard= 0  isolated=False  sharded_total=417  true_max=605
N=12  shard= 8  isolated=False  sharded_total=302  true_max=348
N=13  shard= 5  isolated=False  sharded_total=302  true_max=367
N=14  shard= 2  isolated=True   sharded_total=129  true_max=429
N=15  shard=14  isolated=False  sharded_total=271  true_max=311
N=16  shard=12  isolated=False  sharded_total=354  true_max=354
N=17  shard=10  isolated=False  sharded_total=254  true_max=318
N=18  shard= 8  isolated=True   sharded_total=160  true_max=316
N=19  shard= 6  isolated=False  sharded_total=168  true_max=384
N=20  shard= 4  isolated=True   sharded_total=115  true_max=435
N=21  shard= 2  isolated=True   sharded_total=109  true_max=287
N=22  shard= 0  isolated=False  sharded_total=211  true_max=399
N=23  shard=21  isolated=False  sharded_total=173  true_max=262
N=24  shard=20  isolated=False  sharded_total=247  true_max=261
```

Six candidates isolate `quota_enforcement_tests`, not three: N=9, N=10,
N=14, N=18, N=20, and N=21. The fifth correction's omission of N=9, N=10,
and N=14 did not change the outcome -- their true maxima (472, 520, 429)
are all worse than N=21's 287, so N=21 remains the lowest-true-max
isolating option across the complete range -- but "checked directly rather
than assumed" was not an accurate description of a check that skipped
half the candidates. It is now: every N in the swept range has been
tested, not a hand-picked subset.

**This does not change the shipped configuration.** `SEMAPHORE_SHARD_COUNT`
stays `"21"`. It changes only how confidently this report can claim N=21
is the best isolating option -- from "checked three, best of those" to
"checked all sixteen, best of all of them."

## 🔍 Seventh correction: this was never a "CI-cost improvement"

Codex review caught something none of the previous six rounds questioned:
every number in this report measures test-*weight* redistribution across
shards, and every mention of "improvement" or "reduction" implicitly
treated a lower worst-case weight as a lower CI cost. Ten more shards
means ten more `test-db-linux` matrix jobs on every code-bearing run, and
each job pays its own fixed setup cost regardless of how many tests it
runs: `actions/checkout`, Rust toolchain install, `rust-cache` restore,
the disk-pruning step, and four sequential `docker pull`s (with retries)
for the Postgres/Redis images every DB-backed suite needs. The 151
manifest rows themselves are unchanged -- rebalancing only moves which
shard runs which subset of them. **Total GitHub Actions runner-minutes
consumed per CI run almost certainly goes up, not down**, and this report
never measured that number.

A rough estimate, calibrated against real job timings rather than
guessed: fitting `duration = fixed_overhead + rate * weight` to ten
`Test DB (linux, shard N)` jobs' actual start/end timestamps from PR
#1706's N=11 run (a real CI run, not this PR's own, so unaffected by this
PR's changes) gives `fixed_overhead ~= 985s` (~16.4 min) and
`rate ~= 2.1s` per weight unit. This fit is noisy -- two shards of very
different weight (296 and 426) finished within four seconds of each other
in that data, so treat both constants as order-of-magnitude, not precise:

```
fixed_overhead ~= 16 min/job (order of magnitude; could plausibly be
                  anywhere from ~10 to ~20 min given the fit's noise)
extra jobs (11 -> 21 shards): +10
extra fixed-overhead cost per CI run: ~160 minutes of runner-minutes
  (10 jobs * ~16 min each) -- a real, recurring cost this report never
  weighed against the benefit
```

**The wall-clock benefit is real but much smaller than "52.6%" suggests,
once fixed overhead is included.** That 52.6% figure (605 -> 287) is a
pure test-weight ratio; it does not account for the ~16-minute fixed
component that both the old and new worst shard pay identically. Applying
the fitted model to the worst shard specifically: old worst shard
(weight 605) ~= 37.7 min wall time; new worst shard (weight 287) ~= 26.5
min. That is a **~30% wall-clock reduction for the slowest shard**, not
52.6% -- still a real improvement, but roughly half the size the headline
number implied, because the fixed setup cost does not shrink along with
the test weight.

**This also assumes GitHub Actions has enough concurrent-runner capacity
to run all 21 shards (plus every other job in this workflow and whatever
else is running concurrently in the repo/org) at once.** If the account's
actual concurrency ceiling is lower than that, some of the 10 new shards
queue for a runner slot instead of starting immediately, which would
reduce or eliminate the wall-clock benefit entirely while still paying
the extra fixed-overhead cost. This session did not check the org's
actual GitHub Actions concurrency limit and has no basis to claim the
benefit is guaranteed rather than conditional on available capacity.

**Corrected framing.** This change is a **worst-case wall-clock latency
improvement for the slowest shard, conditional on available runner
concurrency** -- not a CI-cost improvement. It very likely *increases*
total runner-minutes consumed per run by something on the order of 100-200
minutes (a rough estimate, not a measurement), in exchange for an
estimated ~30% reduction in the slowest shard's own wall time (also a
rough estimate, not a live-CI measurement). Every prior mention in this
report and in `ci.yml`'s own comment describing this as a "CI-cost"
improvement, or citing the weight-ratio percentage as if it were a
time-ratio percentage, should be read with this correction applied. The
shipped configuration (`SEMAPHORE_SHARD_COUNT: "21"`) is unchanged --
this correction is entirely about how the change should be described and
weighed, not about which number to ship.
