# Design — Issue #1818: measure test coverage and gate critical modules

CI does not measure coverage. The stated target is 85–90 %. This change
adds a nightly `cargo llvm-cov` run and a ratchet on seven critical
modules.

**No product code changes. No new `WorkflowEvent` variant. No migration.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- No workflow, script or manifest uses `llvm-cov`, `tarpaulin` or `grcov`.
- `test-db-linux` runs the `linux` rows of `.github/ci/integration-suites.txt`
  through `run-suites.sh`, in shards.
- `run-suites.sh` calls plain `cargo test`. `cargo llvm-cov show-env` sets
  a `RUSTC_WRAPPER`, so a plain `cargo test` builds instrumented binaries.
- Stable Rust gives line and region coverage. Branch coverage needs
  `-Zcoverage-options=branch`, so it needs a nightly.
- `rust-toolchain.toml` pins 1.99.0. `RUSTUP_TOOLCHAIN` overrides that file.
- The critical modules are seven single files in `autumn-harvest/src/`.
- `fuzz_nightly_wiring.rs` and `proptest-nightly.yml` set the pattern: a
  cron, an alert job, and a Rust guard over the workflow YAML.

### 0.2 Brainstorm — how to measure and gate

| # | Idea | Verdict |
|---|------|---------|
| B1 | `cargo tarpaulin`. | Rejected. It is slower, and the issue names `llvm-cov`. |
| B2 | `cargo llvm-cov` on stable. | Rejected. It gives no branch data. |
| B3 | `cargo llvm-cov --branch` on a dated nightly. | **Adopted.** It gives line and branch data. |
| B4 | One job runs every DB suite. | Rejected. The suites take hours in series. |
| B5 | Each shard runs `run-suites.sh run linux` under `show-env`. | **Adopted.** The manifest stays the one list of suites. |
| B6 | Merge the raw `.profraw` files of all shards. | Rejected. Each file needs the exact binary that wrote it. |
| B7 | Each shard exports LCOV. A stdlib script merges by line. | **Adopted.** LCOV is small and needs no binary. |
| B8 | Upload to Codecov or Coveralls. | Rejected. It needs a token and an external service. |
| B9 | A fixed 85 % threshold. | Rejected. The issue asks for a ratchet. |
| B10 | A committed baseline with a tolerance. | **Adopted.** A drop below the floor fails. |
| B11 | The nightly commits a raised floor. | Rejected. It needs a write token on `trunk-dev`. |
| B12 | The report names each floor that can rise. `--update` raises it. | **Adopted.** `--update` never lowers a floor. |
| B13 | Gate every PR on coverage. | Rejected. It doubles the DB shard cost of each PR. |
| B14 | Run the whole pipeline on a PR that changes the workflow. | **Adopted.** A script or baseline change runs only the `lint` self-test, because one full run costs about 15 runner-hours. |
| B15 | A Rust guard over the workflow and the baseline. | **Adopted.** It follows `fuzz_nightly_wiring.rs`. |
| B16 | The script self-test runs in the `lint` job. | **Adopted.** It costs seconds. |

### 0.3 Reverse brainstorm — how to make the gate useless

| # | Way to fail | Counter-measure |
|---|-------------|-----------------|
| R1 | A critical module moves, so it has no data and passes. | A module with no data fails. The guard checks that each file exists. |
| R2 | A failed test cuts a shard short. Coverage drops by noise. | The ratchet runs only when every test job passed. A tolerance absorbs jitter. |
| R3 | Nobody raises the floor. The ratchet becomes a fixed threshold. | The report names each floor that can rise and prints the command. |
| R4 | The `-Z` flag reaches a stable `cargo`. The build fails. | Each job sets `RUSTUP_TOOLCHAIN` to the dated nightly. |
| R5 | A newer nightly fails to build the crate. | The pin is a date. The guard rejects `nightly` with no date. |
| R6 | LCOV paths are absolute and differ per runner. | The script makes each path relative to the workspace root. |
| R7 | The merge counts a line twice. Totals inflate. | The merge keys each line by file and number. The self-test proves it. |
| R8 | A failed shard stops the report. | The report job runs unless the run is cancelled. Its summary then shows "Ratchet skipped". |
| R9 | A scheduled failure goes unseen. | An alert job opens or updates an issue. |
| R10 | A `--update` lowers a floor. | `--update` keeps the larger value. The self-test proves it. |
| R11 | `continue-on-error`, `--update` or `|| true` hides a drop. | The guard compares the ratchet step and its gates exactly. Mutation self-tests pin each case. |
| R12 | Instrumented builds fill the runner disk. | Each `db` shard frees disk first, as `test-db-linux` does. |
| R13 | The LCOV exporter crashes. | Found in the green phase: LLVM 23 `llvm-cov export -format=lcov` crashes on `worker.rs`. Each job exports JSON. The script converts it with the LLVM line rule. |
| R14 | `show-env` fails, so the suites run with no instrumentation. | The step assigns its output to a variable first, so the failure fails the step. |

### 0.4 Six thinking hats

- **White (facts).** On 2026-10-16 the manifest had 187 core, 103 plugin, 4
  Redis and 1 runner `linux` rows. The pinned nightly builds the `db`
  feature. `cargo-llvm-cov` 0.9.1 was the current release.
- **Red (feelings).** A nightly that fails by noise gets ignored. Low noise
  matters more than a tight floor.
- **Black (risks).** 23 jobs each night. Nightly drift. Coverage jitter
  from timing paths. A floor that someone lowers by hand.
- **Yellow (benefits).** The 85–90 % target becomes a number. A drop in a
  hot path shows the next morning, not after a release.
- **Green (ideas).** The summary shows each module against its floor. A
  later change can gate PRs that touch only a critical file.
- **Blue (process).** Red: the guard and the self-test fail. Green: the
  workflow, script and baseline. Refactor: share the guard helpers. Then a
  PR run records the baseline.

## 1. Design

### 1.1 Workflow `.github/workflows/coverage-nightly.yml`

- Triggers: a cron, `workflow_dispatch`, and a PR that changes the workflow.
- `unit`: `cargo llvm-cov --branch` over `autumn-harvest --all-features --lib`.
- `db`: the shard layout of `test-db-linux`. Each shard runs
  `run-suites.sh run linux` under `show-env`.
- Each test job exports `llvm-cov` JSON, converts it into LCOV, and
  uploads the LCOV file as an artifact.
- `report`: merges the LCOV files, writes a JSON and a Markdown summary,
  uploads `coverage-report`, and runs the ratchet.
- `alert`: opens or updates an issue when a scheduled run fails.

### 1.2 Script `.github/ci/coverage.py`

Stdlib only. Subcommands:

- `convert IN.json --root R --output OUT` — applies the LLVM
  `LineCoverageStats` rule to the JSON segments. On `queue.rs`, `erase.rs`,
  `scheduler.rs` and `timeout.rs`, it gives the same lines and counts as the
  LCOV exporter of `nightly-2026-08-14` (LLVM 23). That exporter crashes on
  `worker.rs`, so `worker.rs` has no LCOV output to compare.
- `merge --output OUT IN...` — sums hits per file and line, and per branch.
- `summary LCOV --json J --markdown M [--baseline B]` — line and branch
  data per file. With `--baseline`, it also shows each critical module
  against its floor.
- `ratchet --baseline B --summary J [--update]` — fails when a critical
  module is below its floor. `--update` raises the floors.
- `--self-test` — runs the fixtures.

### 1.3 Baseline `.github/ci/coverage-baseline.json`

Each critical module has a line floor and a branch floor, in percent. The
`tolerance` field sets the allowed jitter in points. A module can set its
own `tolerance`. A drop below `floor - tolerance` fails. A module with no
data fails.

### 1.4 Guard `autumn-harvest/tests/integration/coverage_nightly_wiring.rs`

It checks the cron, the two test jobs, the shard count, the artifact, the
ratchet step, the alert job and the baseline module list. It requires a
recorded floor for each module. Mutation self-tests prove that each check
rejects the softened forms that review found.

### 1.5 Baseline period

The issue asks for a baseline period before the gate. The PR runs of this
change are that period. The floors come from the first complete run. Each
later run checks them, and a floor drops to the lower value if jitter
breaks it.

One branch of `erase.rs` is 1.43 points, and one of `poison_pill.rs` is
1.04 points. A global 0.5-point tolerance fails those modules on one
flipped branch. A global 1.5 lets about 39 branches of `worker.rs` drop in
silence. So the baseline keeps 0.5 and gives those two modules their own
`tolerance` of 1.5.

## 2. Out of scope

- A PR gate on coverage (B13).
- Coverage of the `linuxpart` and `allos` rows, the no-DB and `dst`
  binaries, and the lib tests of the other crates.
