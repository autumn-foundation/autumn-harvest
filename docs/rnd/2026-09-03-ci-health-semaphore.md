# 🚦 Semaphore CI health report

**Status:** health report — no PR opened; no test changed. The two candidate
findings below did not clear the fix gate (a measured baseline flake rate or a
safe, zero-added-cost speed win), so per the CI-health charter this is a
findings report handed to the team, not a fix.

**Scope examined:** `.github/workflows/ci.yml` (the `test` job matrix, the
only path a PR into `trunk-dev`/`trunk` waits on), `.github/ci/run-suites.sh`
+ `integration-suites.txt` (the manifest-driven integration-suite runner),
and Actions run/job history for the `CI` workflow via the GitHub API —
~150 `pull_request`-event runs and 7 recent `push`-event (`trunk-dev`) runs
sampled, plus full step-level timing and per-suite log timestamps pulled for
three `push`-event runs on `trunk-dev` (2026-08-31, 2026-09-02, 2026-09-03).

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
`cargo test` invocations against a shared Docker Postgres, each internally
`--test-threads=1` — consistently owns **54–58% of the slowest leg's wall
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
count, with the constraint asserted but not decomposed by suite.** The
runner script pins `--test-threads=1` on every `linux`/`linuxpart` row "serial
for shared Postgres" (`run-suites.sh` comment), and drives all 119 matching
rows through one `while read` loop — one `cargo test` process at a time, full
stop, for the whole step. That constraint is real and specifically
documented, repeatedly, in the test sources themselves: `mutex_tests.rs`
("several tests set the process-global mutex lease TTL"),
`transactional_start_tests.rs` (a lock held "safe under `--test-threads=1`
because it always..."), `capability_miss_tests.rs` ("the whole module shares
one database"), `integration_e2e.rs` (explicitly reasons about the 2-vCPU
GitHub runner). This is load-bearing, intentional design, not an oversight.

A per-test-isolated-database pattern does exist in this codebase, just not
inside the 119-row manifest: `chaos_tests.rs` notes that on its
testcontainers path "each test owns its database" — but that suite is gated
behind the `chaos` feature (off by default, never in `default`) and runs
only via the separate, non-gating `chaos.yml` workflow (on-demand +
nightly), not through `run-suites.sh`. It's evidence the pattern is known
and already implemented once in this repository, not evidence that any of
the 119 manifest suites currently avoid the shared-Postgres constraint.

**Test-vs-product verdict: this is a test/harness-architecture question, not
a product bug.** Nothing here suggests the execution engine itself is slow;
it's that 119 independent `cargo test` invocations, sharing one Postgres
instance, are being walked one at a time by design.

## 🔧 Treatment — routed, not applied

I did not open a fix PR for this. The two paths that would cut this time are
both outside what this report can ship on its own authority:

1. **More parallelism** (matrix-shard the manifest across additional runner
   legs, or run suites concurrently against isolated per-shard databases)
   is *new CI spend* — additional concurrent runner-minutes — which the
   CI-health charter this report follows requires asking a human before
   doing, not inferring from a timing chart.
2. **Removing the serialization constraint** without adding runners would
   require auditing all 119 suites' actual isolation requirements (which
   ones truly share mutable global/DB state vs. which ones could move to a
   per-test-database pattern) — a correctness-sensitive, per-suite audit,
   not a mechanical CI change. None of the 119 manifest suites currently use
   such a pattern (see Diagnosis above); `chaos_tests.rs` shows it's already
   implemented once in this repository, outside the manifest, as a
   precedent to adapt rather than a suite to point an owner at directly.

Both are legitimate next steps; neither is a same-day CI-config edit. **This
report's recommendation is a ranked next step, not a change:** an owner
should decide whether to (a) spend the extra runner budget on sharding the
`linux` manifest rows across N parallel jobs against N isolated Postgres
databases, adapting the per-test-database pattern `chaos_tests.rs` already
uses outside the manifest, or (b) accept 132–155 minutes as the honest cost
of this suite's actual work and leave it alone. Either is a legitimate call;
this report only supplies the number.

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

- **Reverts:** one in `trunk-dev`'s actual history (`git log --all --oneline
  | grep -i revert` against the fetched branch refs): `08b1207`, a `Bolt`
  micro-optimization reverted for a negative benchmark result — not a CI
  escape, not a production bug. This is the "reverted before merge, caught by
  review/benchmark rather than by an escaped bug" pattern, and it is the only
  instance of that pattern reachable from `trunk-dev`: a same-shaped commit,
  `d4b149a` ("DLQ aggregate grouping — extract + profile (negative result,
  reverted after review)"), exists as a loose object in this checkout but
  `git merge-base --is-ancestor d4b149a origin/trunk-dev` is false and no ref
  contains it — it is unreachable pre-squash history from a PR that squashed
  to a different, reverted-before-merge commit, not part of the branch this
  census covers, and not a second data point beyond `08b1207`'s pattern.
  No commit resembling a shipped-then-reverted **production** escape was
  found.
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

cargo test -p autumn-harvest --no-default-features --lib --skip zz_
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
