# 🚦 Semaphore CI health report

**Status:** health report — no PR opened; no test changed. The two candidate
findings below did not clear the fix gate (a measured baseline flake rate or a
safe, zero-added-cost speed win), so per the CI-health charter this is a
findings report handed to the team, not a fix.

**Scope examined:** `.github/workflows/ci.yml` (the `test` job matrix, the
only path a PR into `trunk-dev`/`trunk` waits on), `.github/ci/run-suites.sh`
+ `integration-suites.txt` (the manifest-driven integration-suite runner),
and Actions run/job history for the `CI` workflow via the GitHub API —
~150 `pull_request`-event runs and 30 `push`-event (`trunk-dev`) runs sampled
for the rerun-button census (of the 30 push runs, 7 completed with a
non-cancelled conclusion — the rest were superseded by `cancel-in-progress`
before finishing), plus full step-level timing and per-suite log timestamps
pulled for three of those 7 completed `push`-event runs on `trunk-dev`
(2026-08-31, 2026-09-02, 2026-09-03).

## 🎯 Verdict path

A PR into `trunk-dev` waits on the `test` matrix (`ubuntu-latest`,
`macos-latest`, `windows-latest`, `fail-fast: false`), gated behind the
`lint` and `changes` jobs. There is no merge queue (`on:` has no
`merge_group` trigger) and no ambient retry of any kind: no
`continue-on-error`, no rerun action, no retry wrapper anywhere in
`ci.yml`. `concurrency.cancel-in-progress: true` cancels a PR's own
superseded runs on a new push — that is cost control, not a trust-lowering
retry, since a cancelled run never reports a conclusion the wrong way.
Draft-PR and docs-only skips are already implemented (see the comment block
at the top of `ci.yml`): the `changes` job's `needs.changes.outputs.code`
output gates every expensive step behind `if:`, so a docs-only commit still
reports every `test` leg green while running none of the compile/test work —
by design (a skipped step reports success so branch-protection-required
checks aren't left permanently pending on a docs-only PR; see the `ci.yml`
header comment), not by accident, but it means **green is honest only for the
commits that actually exercised it**: a code-touching commit's green means
the full matrix ran once, on that exact commit, and passed once; a docs-only
commit's green (this PR's own CI run included) means the matrix legs
completed with the code-dependent steps skipped, and asserts nothing about
the test suite. The distinction is visible in the run itself (every
code-dependent step shows `skipped`, not `success`), so nothing is hidden —
but it does mean "green" is not a single verdict shape across this pipeline's
runs, and a reader diffing runs needs to check which case they're looking
at.

What it does not mean is "fast": the `ubuntu-latest` leg of `test` is
consistently the slowest of the three, at 132–155 minutes end to end across
the three sampled runs (table below), and it is the leg every PR is actually
gated on.

## 🌡️ Symptom — timing decomposition

Step-level timing for `Test (ubuntu-latest)`, three independent `push` runs
on `trunk-dev` (all green):

| run (date) | job total | `Run Linux Docker-backed manifest suites (serial)` | % of job |
|---|---:|---:|---:|
| `33407865159` (2026-08-31) | 132.0 min | 76.9 min | 58.2% |
| `33574550163` (2026-09-02) | 148.2 min | 82.0 min | 55.3% |
| `33712626699` (2026-09-03) | 155.3 min | 84.3 min | 54.3% |

One step — `bash .github/ci/run-suites.sh run linux`, 119 sequential
`cargo test` invocations each internally `--test-threads=1` (see Diagnosis
below for what that serialization actually buys, which is more mixed than
"shared Postgres" alone) — consistently owns **54–58% of the slowest leg's
wall
time**, and that leg is on the path every PR waits on end to end. No other
step comes close: the next-largest ubuntu-leg step across all three runs is
`Compile plugin integration test suites (manifest, all OSes)` at 5–8 minutes.

Per-suite breakdown of the 2026-09-03 run (parsed from the `::group::cargo
...` timestamps inside that one step, which sum to the step's own 5061s
exactly — see Reproduce below):

- 119 suites, median 27s, mean 42.5s per suite.
- Only 9/119 finish in under 5s (i.e. this is not mostly per-process
  start-up overhead being paid 119 times — most rows are real test work).
- Heaviest single suites: `dag_compensation` 489s, `backup_verify_tests`
  289s, `integration_e2e` 253s, `transactional_start_tests` 152s,
  `wasm_activities_tests` 148s, `capability_miss_tests` 128s.

## 🔍 Diagnosis

**Not a flake.** All three sampled runs were green with identical suite sets;
this is deterministic, reproducible wall time, not nondeterminism — it does
not fit the flake-rate protocol and isn't being reported as one.

**Root-cause category: forced full serialization across an unbounded suite
count — but the constraint is not one uniform reason.** (An earlier draft of
this section cited it as more uniform than it is — corrected below at the
`capability_miss_tests.rs` citation.) The runner script
pins `--test-threads=1` on every `linux`/`linuxpart` row "serial for shared
Postgres" (`run-suites.sh` comment), and drives all 119 matching rows through
one `while read` loop — one `cargo test` process at a time, full stop, for
the whole step. What's actually behind that constraint, per-suite:

1. **Genuine cross-test shared mutable state (a minority, confirmed).**
   `mutex_tests.rs` ("several tests set the process-global mutex lease TTL")
   and `transactional_start_tests.rs` (a lock held "safe under
   `--test-threads=1` because it always...") depend on process-global state,
   not just the database — these need in-process serialization regardless of
   DB isolation.
2. **Documented CI-runner resource contention — the best-evidenced mechanism
   here, and the one this report under-weighted originally.**
   `integration_e2e.rs` carries an extensive incident writeup (five rounds of
   CI failures, PR #901 and issue #604) concluding the flakiness was "pure
   scheduling-contention on a shared, 2-vCPU CI runner asked to make progress
   on 11 genuinely concurrent, DB-heavy decision cycles at once" — not a
   logic bug, and not fundamentally a database-sharing problem. That test
   now runs 12 OS threads *inside itself*, explicitly relying on the rest of
   the suite being serialized around it ("this whole suite runs with
   `--test-threads=1`, so this is the only test using this many OS threads
   at any given moment"). This is evidence for *runner capacity*, not
   *shared-database correctness*, as (at least one) real constraint.
3. **Many manifest suites already run per-test-isolated, not against a
   shared database, in CI as configured today.** `HARVEST_TEST_DATABASE_URL`
   is not set anywhere in the `test` job (confirmed: absent from `ci.yml`,
   no matching secret reference) — so every suite whose `setup_db()`
   falls back to booting a fresh testcontainers Postgres when that variable
   is unset (36 files match this pattern by grep, `capability_miss_tests.rs`
   and `integration_e2e.rs` included) is *already* getting a throwaway,
   per-test database in the CI runs this report measured, not one shared
   instance. `capability_miss_tests.rs`'s "the whole module shares one
   database" comment describes the *other* branch of `setup_db()` — a
   developer pointing it at a real shared Postgres locally via
   `HARVEST_TEST_DATABASE_URL` — not what happens in CI. I cited that
   comment as evidence for the shared-Postgres constraint in the original
   version of this section; that citation was wrong, and I've removed it.

**Test-vs-product verdict: this is a test/harness-architecture question, not
a product bug.** Nothing here suggests the execution engine itself is slow;
it's that 119 independent `cargo test` invocations are being walked one at a
time, for a mix of real correctness reasons (category 1) and a documented,
previously-litigated runner-capacity reason (category 2) — with category 3
meaning the DB-sharing story specifically is weaker evidence than the
runner-capacity story for a meaningful chunk of the manifest.

## 🔧 Treatment — routed, not applied

I did not open a fix PR for this. The two paths that would cut this time are
both outside what this report can ship on its own authority, and the
category-2 finding above raises the bar on the second one:

1. **More parallelism** (matrix-shard the manifest across additional runner
   legs, or raise `--test-threads` for suites that don't need category-1
   serialization) is *new CI spend* if it means more concurrent runners —
   which the CI-health charter this report follows requires asking a human
   before doing, not inferring from a timing chart. It may not even help
   without that spend: `integration_e2e.rs`'s own incident history says the
   2-vCPU shared-runner class chokes on DB-heavy concurrency well short of
   119 suites' worth, so raising thread/shard count on the *same* runner
   class risks reproducing the exact failure mode that test's five CI
   incidents already diagnosed and fixed around.
2. **Removing the serialization constraint per-suite** would require
   auditing all 119 rows against the three categories above (which need
   category-1 process-global-state serialization, which are already
   per-test-isolated per category 3 and could potentially move to a higher
   `--test-threads` *if* runner capacity allows it, and which are actually
   category-2-constrained by the runner class itself) — a correctness- and
   capacity-sensitive per-suite audit, not a mechanical CI change. `36` of
   the 119 rows match the per-test-isolation pattern by grep (a superset
   estimate, not a verified count of which are safe to parallelize);
   `chaos_tests.rs` (feature-gated, outside the manifest, run only via the
   separate `chaos.yml`) is a precedent that the pattern works in this
   codebase, not evidence about any manifest suite specifically.

Both are legitimate next steps; neither is a same-day CI-config edit, and
the second is more promising than my original version of this report
credited (some suites may parallelize for free, no new runner spend, once
audited) but is gated on confirming runner capacity actually tolerates it
for each suite. **This report's recommendation is a ranked next step, not a
change:** an owner should decide whether to (a) fund an audit of the
already-isolated suites' actual parallelization headroom on the current
2-vCPU runner class before touching CI config, (b) spend the extra runner
budget on sharding regardless, or (c) accept 132–155 minutes as the honest
cost of this suite's actual work and leave it alone. Either is a legitimate
call; this report only supplies the number.

## 🔬 Flake census (secondary finding, not actioned)

Searched for `flaky`/`flake`/`quarantine` in source, comments, and `#[ignore]`
reasons: 25 actual `#[ignore = "..."]` attributes repo-wide
(`grep -rEn '^\s*#\[ignore(\s*=|\])'`, syntax-anchored so it excludes the ~20
textual `#[ignore` mentions inside comments/doc-strings, mostly in
`ci_run_coverage.rs`'s own guard-test descriptions), zero of the 25 without a
reason string — no orphaned skips, nothing resembling an un-owned quarantine.

Two prior CI-timing-flake fixes exist in `slot_tuner.rs`, both from real
incidents: `tuner_loop_samples_pool_pressure_once_per_tick_for_both_slot_types`
was converted to `#[tokio::test(start_paused = true)]` after flaking on a
slow Windows runner (comment: "only 2 real ticks elapsed in 110ms of wall
time"). Its sibling in the same file,
`tuner_loop_applies_decision_each_tick_for_both_slot_types` (line 1448),
still uses the pre-fix pattern: a real `tokio::time::sleep(200ms)` racing a
background loop on a 5ms tick interval. Same file, same mechanism, not yet
converted.

I ran this test's own baseline before treating it as a finding:
30x under ~2x CPU oversubscription (8 busy loops on 4 cores) and 15x inside
the full 2,320-test `--lib` suite at default parallelism (closer to actual
CI contention) — **0/45 failures**. That's baseline evidence *against* this
test being currently flaky at any rate my harness could detect on this
runner class, not evidence it is safe on GitHub's shared Windows runners.
Per the hard gate this report is built to (a fix needs a measured baseline
rate, not a resemblance to a past incident), 0/45 does not license a "fix" —
there is no rate to drive to zero. Flagging it as a watch item only: if this
test is ever seen red on `windows-latest`, its sibling three tests below it
in the same file is the fix to copy, and the mechanism (real-time
`sleep()` racing a background timer, "sleep() as synchronization") is
already named.

## 📊 Escape analysis & rerun-click trend

- **Reverts:** `git log --oneline origin/trunk-dev | grep -i revert` finds
  one commit reachable from `trunk-dev`: `08b1207`, a `Bolt`
  micro-optimization reverted for a negative benchmark result — not a CI
  escape, not a production bug. The same query against `origin/trunk` (the
  production branch, and this report's first pass only had `trunk-dev`
  fetched locally, which understated this — corrected here against a full
  `git fetch` of all ~400 branches this repository carries) finds a second:
  `26f6632`, `Revert "fix: gate shard rollouts on readiness"`, reverting
  `b960a15` 54 minutes after it was committed (2026-05-07). Checked whether
  either commit reached a tagged release before the revert:
  `git merge-base --is-ancestor b960a15 v0.2.0` is false (v0.2.0 predates it)
  and `v0.3.0` (the next tag) only contains the *revert*, not the original —
  so, like `08b1207`, this is a same-day catch-and-revert that never shipped,
  not a production escape. A third candidate, `d4b149a` ("DLQ aggregate
  grouping — extract + profile (negative result, reverted after review)"),
  exists as a git object in this checkout but `git merge-base --is-ancestor
  d4b149a origin/trunk` (checked against both branches now) is false and no
  ref among the ~400 fetched contains it — unreachable pre-squash history
  from a PR that squashed to a different, reverted-before-merge commit, not
  part of either branch's real history. Two further SHAs a reviewer cited
  (`db99a5d`, `b3432f3e`) are not valid objects anywhere in this repository
  even after the full fetch (`git cat-file -t` fails on both against every
  fetched ref). Net: two reverts total across `trunk` + `trunk-dev`
  combined, both same-day catch-and-reverts that never reached a release —
  no commit resembling a shipped-then-reverted **production** escape was
  found in either branch's history.
- **Rerun-button usage:** 0 runs with `run_attempt > 1` across ~150 sampled
  `pull_request`-event runs and 30 `push`-event runs. No reflexive-rerun
  culture; consistent with a pipeline whose red is currently trusted enough
  not to be routed around.
- **Failures observed on PR branches:** several, all on branches driven by
  autonomous coding-agent sessions mid-iteration (e.g.
  `claude/issue-964-tdd-implementation-*`, four separate failing runs across
  distinct commits). Spot-checked one (`33713080390`): the failure summary
  names two specific suites (`integration_e2e`,
  `workflow_id_targeted_tests`) with real test assertion failures inside a
  work-in-progress branch, not an infra or timing symptom — normal
  in-progress iteration, not a CI-trust problem.

## Reproduce

Timing decomposition (per-suite, from a `push`-event run's logs):

```sh
# Given a run id for the `CI` workflow on trunk-dev:
gh api repos/autumn-foundation/autumn-harvest/actions/runs/<run_id>/logs \
  > logs.zip   # `gh api` has no -o/--output flag; redirect stdout instead.
              # (or actions_get/get_workflow_run_logs_url via the GitHub API)
unzip logs.zip -d ci_logs
# Isolate the "run linux" step's ##[group]cargo ... timestamps and diff
# consecutive ones -- see this memo's PR description for the ~40-line parser.
```

Rerun-button / escape sampling (`--paginate` to cover the full ~150-run
`pull_request` sample and the ~30-run `push` sample this report used, not
just the first page):

```sh
gh api --paginate \
  "repos/autumn-foundation/autumn-harvest/actions/workflows/ci.yml/runs?event=pull_request&status=completed&per_page=100" \
  | jq '.workflow_runs[] | select(.run_attempt > 1)'

gh api --paginate \
  "repos/autumn-foundation/autumn-harvest/actions/workflows/ci.yml/runs?event=push&status=completed&per_page=100" \
  | jq '.workflow_runs[] | select(.run_attempt > 1)'
```

`tuner_loop_applies_decision_each_tick_for_both_slot_types` baseline (0/45):

```sh
cargo test -p autumn-harvest --no-default-features --lib \
  slot_tuner::tests::tuner_loop_applies_decision_each_tick_for_both_slot_types \
  -- --exact --test-threads=1   # repeat N times under CPU load

cargo test -p autumn-harvest --no-default-features --lib -- --skip zz_
# repeat N times at default parallelism; grep the one test's line each run
```

## What this memo is

A recorded, reproducible measurement, not a change: the pipeline's verdict is
currently honest (no ambient retries, no reflexive reruns, a clean quarantine
posture), and its single largest cost is one already-documented, intentionally
serial integration-test step that eats over half of the leg every PR is
gated on. Fixing that costs either new CI spend or a per-suite isolation
audit, both owner decisions this report is not positioned to make alone. The
one flake-shaped risk found (a real-time-sleep test with a same-file sibling
that already flaked once) has no measured rate behind it yet, so it is
recorded here as a watch item with its fix pre-identified, not shipped as a
guess.
