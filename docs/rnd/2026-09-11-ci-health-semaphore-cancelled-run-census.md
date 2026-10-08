# 🚦 Semaphore CI health — the cancelled-run census, finished, plus a new flake candidate

**Status:** health report — no PR opened against `ci.yml`, no test changed. Follows
`docs/rnd/2026-09-08-ci-health-semaphore-rerun-census.md`, which found that this
workflow's `fail-fast: false` matrix jobs under a cancel-in-progress concurrency
group can hide a real job failure inside an overall `cancelled` run, and sampled
only 10 of 66 cancelled runs (4/10 hit rate) before flagging the gap as unmeasured.
This report finishes that census on a fresh 100-run sample, finds one genuine
flake candidate, and attempts — without success — to reproduce it at a measured
rate. It also re-measures the windows-`no-db` long pole first flagged 2026-09-06:
it has gotten worse, not better.

## 🎯 Verdict path

Same verdict path as every prior report in this series: `ci.yml`'s `test-nodb`
(12 shards) and `test-db-linux` (10 shards) matrices, plus `openapi-client-smoke`,
still carry "not yet enforced" comments with branch-protection status unconfirmed
from this session — no branch-protection-read tool is exposed here, checked again
today. No cache-usage/listing tool is exposed either. Both gaps are unchanged
across all six reports in this series (09-03 through today); routing them forward
again below rather than re-describing them.

## 🌡️ Symptom

### 1. Rerun-button census, repeated on a fresh 100-run sample

100 most recent completed `pull_request`-event `ci.yml` runs (2026-09-07 18:15 UTC
through 2026-09-11 07:19 UTC): 32 success, 54 cancelled, 14 failure. **1/100 shows
`run_attempt > 1`** — the first non-zero count in this series (09-03/09-06/09-08 all
found 0/100). Traced to source: run `34467134226`, a manual `rerun_failed_jobs`
issued by a Semaphore session itself while driving PR #1460 (issue #1459) to green,
not a developer reflexive-rerun-button press. **Still 0/100 developer-initiated
reruns** — narrowed, not contradicted.

### 2. The 14 explicit failures

**Correction, per a Codex review comment on this PR:** an earlier draft of this
section reported only an aggregate count for 13 of the 14 runs, disclosing that
a context-compaction boundary in the sub-investigation had lost their individual
IDs and signatures. The comment correctly flagged that an aggregate the reader
cannot verify — against a `list_workflow_runs` window that keeps moving as new
runs land — doesn't belong in a verified total. All 14 run IDs were preserved
outside that sub-investigation's own context (saved to a local file before
delegating), so they were re-pulled directly rather than left as an unverifiable
aggregate:

| Run | Failed job/step | Signature | Class |
|---|---|---|---|
| `34573811773` | `Test (windows-latest)` compile | `error[E0786]: found invalid metadata files for crate 'autumn_harvest'` → `failed to mmap file '...libautumn_harvest-*.rlib': The paging file is too small for this operation to complete. (os error 1455)` | **External infra** — Windows runner memory/paging-file exhaustion. A different infra-failure shape than any prior report in this series (those found a GitHub-runner shutdown and a Docker-registry pull failure; this is the first paging-file exhaustion observed) |
| `34514846121` | Lint → Clippy autumn-harvest | Clippy violation in the core crate | Deterministic |
| `34413355439` | Lint → Clippy autumn-harvest | Clippy violation in the core crate | Deterministic |
| `34161210429` | Lint → Clippy autumn-harvest-plugin | Clippy violation in the plugin crate | Deterministic |
| `34262115891` | Lint → `docs/performance.md` guards | Doc/code-sync gate — same class as `sqlite_feasibility_docs`/`migration_hygiene` in the 09-08 report | Deterministic |
| `34163231968` | Lint → `docs/performance.md` guards | Same doc/code-sync gate, different commit | Deterministic |
| `34411696665` | Lint → Comment hygiene (`docs/audits/comment-hygiene.py`) | Tier B ratchet violation | Deterministic |
| `34325821650` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34316264290` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34268913672` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34192557082` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34163695062` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34155387838` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |
| `34151231594` | Lint → Comment hygiene | Tier B ratchet violation | Deterministic |

**13/14 deterministic, commit-specific `Lint`-job failures (8 comment-hygiene,
3 clippy, 2 doc/code-sync); 1/14 external infra** — matching the aggregate the
prior draft reported, now backed by verifiable run IDs. None reproduce the
`dispatch_tests` flake signature in §4, and none required pulling job logs
beyond the job list itself — the failed step name identifies the check in
every case here (unlike the cancelled-run table in §3, where several hidden
failures needed a log pull to name the specific test or lint that fired).

### 3. The cancelled-run census, finished

All 54 of the 54 cancelled runs in the fresh sample were checked (not a sub-sample
this time): `list_workflow_jobs(perPage=100)` per run, `total_count` confirmed
against the returned array length, every job's own `conclusion` inspected rather
than trusting the run's overall `cancelled` verdict.

**15 of 54 (28%) hid at least one real job failure.** Full list:

| Run | Failed job(s) | Signature | Class |
|---|---|---|---|
| `34511547383` | Lint | `cargo fmt --check` diff | Deterministic |
| `34444753299` | Lint | `cargo fmt --check` diff | Deterministic |
| `34387029856` | 5 jobs, 1 root cause | `ci_run_coverage::every_db_gated_test_has_a_ci_run_step_or_is_allowlisted` panic (manifest-completeness gate) | Deterministic |
| `34263945990` | `Test (no-db)` ×2 | `ui_integration.rs:2826` — `ui_schedules_health_filter_narrows_to_unhealthy_rows`, `left: 200, right: 400` | Deterministic (single occurrence) |
| `34200783185` | Lint | Clippy `match_wild_err_arm` | Deterministic |
| `34200351621` | Lint | Clippy `doc_markdown` | Deterministic |
| `34199661486` | Lint | `cargo fmt --check` diff | Deterministic |
| `34195246573` | `Test`/`Test (no-db)` ×3 OS | `ci_run_coverage::every_db_gated_test_has_a_ci_run_step_or_is_allowlisted` panic — missing manifest row for `scheduler_overdue_pass_perf` | Deterministic |
| `34177004263` | `Test DB (linux, shard 8)` | `failed to start Postgres container: ... PullImage { descriptor: "postgres:16" } ... "bytes remaining on stream"` | **External infra** — same signature the 09-08 report already documented |
| `34172807956` | `Test DB (linux, shard 5)` | `dispatch_tests::the_by_id_claim_honours_the_dr_fence` panics at `dispatch_tests.rs:992`: `relation "harvest_shard_generation" does not exist` | **Flake candidate — see §4** |
| `34170137842` | 5 jobs, 1 root cause | `migration_hygiene::no_new_handrolled_migration_bundles_outside_allowlist` panic | Deterministic |
| `34167481977` | `Test DB (linux, shard 5)` | Same test, `dispatch_tests.rs:938`: `relation "harvest_shard_generation" does not exist` | **Flake candidate — see §4** |
| `34165189395` | `Test` ×3 OS | `error[E0433]: cannot find MemoryDispatch in dispatch` — used unconditionally in `worker.rs:37779` but gated behind `#[cfg(feature = "testing")]` | Deterministic (compile-time; identical on all OSes by construction) |
| `34162176528` | Dependency ledger (`cargo-deny`) | `RUSTSEC-2025-0134`: `rustls-pemfile` unmaintained, no safe upgrade | Dependency-ledger drift, not a CI-reliability issue |
| `34160646952` | Lint | `cargo fmt --check` diff | Deterministic |

The remaining 39/54 showed every job as `success`/`cancelled`/`skipped` — no hidden
failures.

**Combined with §2: 26 of 29 total failure instances found across both sections
trace to a deterministic, commit-specific defect (24) or to external infra
nondeterminism (2); 2 share one flake-candidate signature; 1 is dependency-ledger
drift, not a suite-reliability question.** (§2: 13 deterministic + 1 external
infra = 14. §3: 11 deterministic + 1 external infra + 2 flake-candidate + 1
dependency-ledger = 15. 14 + 15 = 29; 13+11=24 deterministic, 1+1=2 external
infra, 24+2=26.)

### 4. The flake candidate: `dispatch_tests::the_by_id_claim_honours_the_dr_fence`

Two occurrences, two different commits (the panic's source line moved from 938 to
992 between them, so the file changed), same shard slot (`Test DB (linux, shard
5)`), same error: `relation "harvest_shard_generation" does not exist`. Both were
absorbed under an overall `cancelled` verdict — neither would appear in a census
that trusted run-level conclusions, which is exactly the gap this census exists to
close.

**Mechanism attempted:** `harvest_shard_generation` is created by migration
`20260726000000_harvest_shard_generation` and is present in `INIT_SQL`, which
`setup_test_database_url_or_env()` (`integration_e2e.rs:498`) applies via
`Postgres::default().with_init_sql(...)` — a fresh testcontainers Postgres per
test function, not a database shared across the shard's test binary. A real,
independently-verified fact about the dependency stack: `testcontainers-modules`
0.15.0's Postgres module waits on `WaitFor::message_on_stderr` /
`message_on_stdout("database system is ready to accept connections")` with no
`.with_times(2)`, and `testcontainers` 0.27.3's `LogWaitStrategy` defaults `times`
to 1 — i.e., it latches onto the *first* occurrence of that message. The official
Postgres image logs that exact line twice when init scripts are configured (once
for the ephemeral instance that runs them, again for the final server), which is
the class of race this dependency pairing is known to be exposed to in general.

**This is a plausible mechanism, not a confirmed one — say so plainly.** It was
not confirmed against this specific failure (no container logs from the two CI
runs were pulled to check log ordering), and it does not obviously explain how an
external client would even observe "relation does not exist" rather than
"connection refused" if the temporary initdb-scripts instance in the official
image binds only its Unix socket, not the mapped TCP port. Flagging the
verified facts (dependency versions, default wait-strategy behavior) and the
open question (does this specific image/entry point expose the race this way)
separately rather than collapsing them into a single claimed cause.

**Reproduction attempted, did not reproduce:** built the `integration` test binary
with `--features testing` locally (Docker available, 4 vCPUs) and ran, under
`taskset -c 0,1` CPU-pinned contention (matching the method PR #1464 used
successfully on a different flake):

- 60 concurrent reps of the single test alone (10 rounds × 6 concurrent copies): **0/60 failed.**
- 32 concurrent reps of the full `dispatch_tests` module, all ~18 tests per rep,
  serialized internally by `DISPATCH_SERIAL` (8 rounds × 4 concurrent copies):
  **0/32 failed.**

Two Codex review comments on this PR flagged real gaps in how an earlier draft
documented this reproduction (fixed in 🔬 Reproduce below) — confirmed neither
affected the result actually obtained here: `HARVEST_TEST_DATABASE_URL` was
unset in this session's shell for the whole run (checked directly), so every
rep did go through `setup_test_database_url_or_env()`'s testcontainers path,
corroborated by each solo rep taking ~13s (consistent with a fresh container
plus migrations, not an instant shared-DB connection) and by `postgres:16`
appearing in the local Docker image cache only after these runs. And the
failure count was read from per-rep log files via `grep -L "test result: ok"`,
not from the backgrounding loop's own exit status, so the 0/92 count is not
subject to the `wait`-swallows-failures gap the comments correctly identified
in the originally-documented commands.

**92/92 clean locally.** Per this role's own hard gate, that is not a fix-ready
result — it means the local harness (a 4-vCPU sandbox running one or a few
copies of this test concurrently) does not reproduce whatever CI's actual
10-parallel-shard, shared-runner-fleet load produces, or the true rate is low
enough that even 92 reps isn't past the noise floor. Either way, **no measured
rate exists, so no fix is being proposed.**

## 🔍 Diagnosis

**§2/§3 (26 of 29 failures):** deterministic, commit-specific lint/fmt/doc-sync/
migration-hygiene/manifest-coverage/compile-time-feature-gating defects, or
external infra (a new paging-file-exhaustion shape on Windows, plus a repeat of
the already-documented Docker-registry pull failure). None are suite-level
flakes. **1 of 29** (`34162176528`) is dependency-ledger drift — `cargo-deny`
catching up to an advisory published against an existing, unchanged dependency —
neither a suite defect nor infra noise, so it is tracked separately and not
counted toward either bucket. **§4 (2 of 29):** an unconfirmed flake candidate.
Verdict on test-vs-product
cannot be rendered yet — the hard gate's own §3 requirement ("show the
nondeterminism lives in the test, not the thing it tests, before touching any
test") is not met, because no reproduction exists to interrogate. This is
explicitly not being treated as a proven test bug or a proven product bug; it is
an open question with a plausible mechanism and two real occurrences.

**Windows no-db long pole, re-measured (§5 below):** confirmed still present and
measurably worse than the 09-06 baseline, in an uncontrolled sample of four
single runs on four different branches. Not root-caused to any commit — "which
commit, if any single one" is an open question a real bisect has to answer, not
something this report's opportunistic sample can settle either way.

## 🔧 Treatment

None shipped — nothing here clears the impact floor. Specifically:

- No flaky test made deterministic: the one flake candidate has 2 occurrences and
  0/92 local reproductions, nowhere near the ≥20 (≥50 for low-rate) same-commit
  rerun bar this role's own gate requires before calling a rate and naming a fix.
- No product bug filed: the mechanism is unconfirmed, not diagnosed.
- No timing fix: the windows regression (§5) is real and worth someone's time, but
  bisecting it across several merges to find the specific contributor(s) is a
  separate investigation from what this report's session budget covered.
- No quarantine: quarantining `the_by_id_claim_honours_the_dr_fence` would remove
  the only detector of whatever this is before its mechanism is known — exactly
  what this role's charter says not to do "without a diagnosis showing the test
  was testing nothing."

Four items routed forward, two carried from prior reports and two new:

1. **Cache-usage API access** (carried from 09-05 through 09-08) — still no
   `cache/usage` or `caches`-listing method among the tools available here.
2. **Branch-protection confirmation** (carried from 09-04 through 09-08) — still
   no branch-protection-read tool; `test-nodb`, `test-db-linux`, and
   `openapi-client-smoke` all remain unconfirmed against live branch protection.
3. **New: reproduce the `dispatch_tests` flake candidate at CI scale, not
   sandbox scale.** 92/92 clean locally under 2-CPU-pinned contention; CI's
   actual failure mode may need the real 10-parallel-shard runner-fleet load
   (or many more reps) to surface. Whoever has access to a CI-scale
   reproduction environment (or the patience for a much larger rep count) should
   pull the actual container logs from `34172807956`/`34167481977` first, if
   they're still retrievable, to check the ready-message-ordering hypothesis in
   §4 directly before spending compute on blind reruns.
4. **New: bisect the windows-`no-db` timing regression (§5)** across the
   2026-09-08–11 merge window to find which change(s) contributed, rather than
   treating "it's worse" as the final word.

## 📊 Measurement

- **Rerun-button census:** 1/100 `run_attempt > 1`, traced to a Semaphore
  session's own manual rerun — 0/100 developer-initiated, consistent with
  09-03/09-06/09-08.
- **Cancelled-run census:** 54/54 cancelled runs in the fresh sample fully
  audited at job level (vs. 10/66 sampled in 09-08) — 15/54 (28%) hid a real
  failure; 11 deterministic, 1 external infra, 2 flake-candidate instances
  (1 signature), 1 dependency-ledger drift.
- **Explicit-failure census:** 14/14 runs classified with full per-run detail —
  13/14 re-pulled after a mid-investigation context-compaction loss initially
  left only their aggregate (8 comment-hygiene, 3 clippy, 2 doc/code-sync;
  1/14 external infra), per a Codex review comment on this PR.
- **Flake-candidate reproduction:** 0/92 local reps failed (60 isolated-test +
  32 full-module, both under `taskset -c 0,1` contention) — inconclusive, not a
  measured rate, no fix follows from this.
- **Windows no-db timing (§5):**

  | Run | When | `no-db, windows-latest, shard 3` | `no-db, ubuntu-latest, shard 3` | Run wall-clock |
  |---|---|---:|---:|---:|
  | `34166818680` | 2026-09-07 (pre-#1427) | 39.2 min | 23.0 min | — |
  | `34268170391` | 2026-09-08 19:18 (first post-#1427) | 51.6 min | 30.3 min | — |
  | `34553672251` | 2026-09-11 02:11 | 62.9 min | 40.6 min | 92.8 min |
  | `34570712844` | 2026-09-11 06:37 | 63.4 min | 36.2 min | 93.7 min |

  Windows shard 3 grew ~62% (39.2→63.4 min) and ubuntu shard 3 grew ~57–77% in
  the same window. **Attribution caveat, flagged by a Codex review comment on
  this PR and correct:** each row is one opportunistic sample from a different
  PR's own branch and commit, on whatever hosted runner GitHub happened to
  assign, with cache state and runner-fleet load uncontrolled between them.
  That is enough to show a real timing *increase* across the sampled runs — it
  is not a controlled measurement and does not by itself prove the increase is
  spread across several merges rather than concentrated in one, or that
  branch-specific factors (a slower runner class that day, a colder cache on
  that particular branch) aren't contributing. The original draft of this
  section claimed the growth was "spread across several commits ... not a
  single step," which overstated what four uncontrolled single-run samples can
  support; corrected here to: **a real timing increase is observed in this
  sample, its distribution across commits is not yet established, and item 4
  below (an actual bisect, or repeated same-commit runs to control for runner
  variance) is required before attributing it to any specific change.** Overall
  run wall-clock is now ~93 min in the two latest samples, up from the
  58.9–66.5 min range the 09-06 report measured — also reported as an
  observation from this same uncontrolled sample, not a controlled trend.
- No revert check applies — no fix in this report to verify red-then-green on.

## 🔬 Reproduce

```sh
# Rerun-button census + explicit-failure classification:
# actions_list(method="list_workflow_runs", resource_id="ci.yml",
#   workflow_runs_filter={event:"pull_request", status:"completed"}, perPage=100)
# then filter conclusion and run_attempt.

# Cancelled-run hidden-failure audit (full census, not a sample):
# for every run with conclusion=="cancelled": list_workflow_jobs(resource_id=run,
# perPage=100), confirm total_count matches the returned length, then check every
# job's own `conclusion` field (not just the run's).

# Flake-candidate local reproduction. Two Codex review comments on this PR
# caught real gaps in an earlier draft of these commands, both fixed below:
# (1) if HARVEST_TEST_DATABASE_URL is set in the shell, setup_test_database_url_or_env()
#     (integration_e2e.rs:498) returns it without starting a container, silently
#     testing a shared pre-migrated DB instead of the testcontainers startup path
#     this reproduction exists to stress -- unset it explicitly first.
# (2) a bare `wait` after backgrounding jobs with `&` does not propagate any
#     individual job's failure -- redirect each rep to its own log file and grep
#     the logs afterward, don't trust the loop's own exit status.
unset HARVEST_TEST_DATABASE_URL
cargo test -p autumn-harvest --features testing --test integration --no-run
BIN=target/debug/deps/integration-<hash>
mkdir -p /tmp/repro_logs
# isolated-test contention:
for round in $(seq 1 10); do
  for i in 1 2 3 4 5 6; do
    taskset -c 0,1 "$BIN" --test-threads=1 \
      dispatch_tests::the_by_id_claim_honours_the_dr_fence \
      > /tmp/repro_logs/round${round}_${i}.log 2>&1 &
  done
  wait
done
grep -L "test result: ok" /tmp/repro_logs/*.log  # empty output = 0 failures
# full-module contention (DISPATCH_SERIAL still serializes within each process):
rm -f /tmp/repro_logs/*.log
for round in $(seq 1 8); do
  for i in 1 2 3 4; do
    taskset -c 0,1 "$BIN" --test-threads=1 dispatch_tests:: \
      > /tmp/repro_logs/round${round}_${i}.log 2>&1 &
  done
  wait
done
grep -L "test result: ok" /tmp/repro_logs/*.log  # empty output = 0 failures

# Windows no-db timing:
# list_workflow_jobs(resource_id=<run>, perPage=100), compute
# (completed_at - started_at) per job, filter to "no-db, <os>, shard N" names.
```
