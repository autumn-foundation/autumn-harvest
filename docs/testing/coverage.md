# Test coverage

The nightly workflow `.github/workflows/coverage-nightly.yml` measures line
and branch coverage (issue #1818). A ratchet stops coverage of the critical
modules from dropping.

## What the nightly measures

- **`unit`** runs the `autumn-harvest` lib tests with `--all-features`.
- **`db`** runs every `linux` row of `.github/ci/integration-suites.txt`.
  It uses the shard layout of `test-db-linux`.
- **`report`** merges the LCOV file of each job. It publishes the
  `coverage-report` artifact and the run summary.

The test jobs use a dated nightly, because branch coverage needs
`-Zcoverage-options=branch`. The workflow sets `RUSTUP_TOOLCHAIN`, which
wins over `rust-toolchain.toml`. The pin is the one in `fuzz-nightly.yml`.

The nightly does not measure these tests:

- the `linuxpart` and `allos` manifest rows;
- the no-DB `integration` run and the `dst` simulation binary;
- the lib tests of crates other than `autumn-harvest`;
- the property suites and the doctests.

A crate other than `autumn-harvest` shows only the coverage that the DB
rows give. So the numbers are a lower bound.

## Read the report

Open the run summary, or download the `coverage-report` artifact. It holds
these files:

| File | Content |
|---|---|
| `coverage.lcov` | The merged LCOV data. Load it into an editor plugin or `genhtml`. |
| `summary.json` | Line and branch counts and percentages for each source file. |
| `summary.md` | The critical modules against their floors, each crate, and each file. |

A module is one source file. A line counts as covered when a test runs it
once or more. A branch side counts as covered when a test takes it once or
more.

The branch percent can be lower than `cargo llvm-cov report` shows. The
JSON export does not mark a folded branch side, so the script counts it as
not taken. The difference is the same each night, so the ratchet is not
affected.

## The ratchet

`.github/ci/coverage-baseline.json` records a line floor and a branch floor
for each critical module:

- `autumn-harvest/src/codec_rotation.rs`
- `autumn-harvest/src/erase.rs`
- `autumn-harvest/src/poison_pill.rs`
- `autumn-harvest/src/queue.rs`
- `autumn-harvest/src/scheduler.rs`
- `autumn-harvest/src/timeout.rs`
- `autumn-harvest/src/worker.rs`

The `report` job fails when one of these conditions is true:

- A module is below its floor minus `tolerance` points.
- A module has no coverage data. A moved file must move its floor too.
- A module has a branch floor but no branch data.

The ratchet runs only when every test job passed. A failed test cuts
coverage short, so a drop in that run is noise. The summary of such a run
shows "Ratchet skipped", and the failed job fails the run. A shard that
fails every night therefore turns the gate off. Fix that shard first.

A failed scheduled run opens the issue "Coverage nightly: a scheduled run
failed", or comments on it. The comment names the result of each job.

The first floors are the lowest value of each module over the runs of the
PR that added the nightly. The tolerance covers the jitter between those
runs.

### Raise a floor

The summary shows "A floor can rise" when a module is above its floor. Use
a run in which every test job passed. Download `summary.json` from that run
and run this command:

```sh
python3 .github/ci/coverage.py ratchet --update \
  --baseline .github/ci/coverage-baseline.json --summary summary.json
```

`--update` never lowers a floor. Commit the new baseline in its own PR, with
the run link. Such a PR runs no DB shard. Start the workflow by hand on the
branch to check the new floors.

### Lower a floor

Lower a floor only when code leaves the module, or when a test moves to a
suite that the nightly does not measure. Edit the baseline by hand and
state the reason in the PR.

### Add or remove a critical module

Edit the baseline and `CRITICAL` in `coverage_nightly_wiring.rs` in one PR.
The guard requires the two lists to match.

## Run it locally

The LCOV exporter of LLVM 23 crashes on `worker.rs`. Export JSON and
convert it, as the workflow does.

Install the tools once:

```sh
export RUSTUP_TOOLCHAIN="$(sed -n 's/^  RUSTUP_TOOLCHAIN: //p' .github/workflows/coverage-nightly.yml)"
rustup toolchain install "$RUSTUP_TOOLCHAIN" --component llvm-tools-preview
cargo install cargo-llvm-cov --locked --version 0.9.1
```

Measure the lib tests. The `clean` step removes old profile data:

```sh
cargo llvm-cov clean --workspace
cargo llvm-cov --branch --no-report -p autumn-harvest --all-features --lib
cargo llvm-cov report --branch --json --output-path unit.json
python3 .github/ci/coverage.py convert unit.json --root "$PWD" --output unit.lcov
```

Measure one DB shard too. The DB suites need Docker. Without the shard
variables, `run-suites.sh` runs every row, which takes hours:

```sh
coverage_env="$(cargo llvm-cov show-env --sh --branch)"
eval "$coverage_env"
cargo llvm-cov clean --workspace
SEMAPHORE_SHARD_COUNT=21 SEMAPHORE_SHARD_INDEX=0 \
  HARVEST_DEV_REQUIRE_POSTGRES=1 HARVEST_TEST_REQUIRE_REDIS=1 \
  bash .github/ci/run-suites.sh run linux
cargo llvm-cov report --branch --json --output-path db.json
python3 .github/ci/coverage.py convert db.json --root "$PWD" --output db.lcov
```

Then merge and summarize:

```sh
python3 .github/ci/coverage.py merge --output merged.lcov unit.lcov db.lcov
python3 .github/ci/coverage.py summary merged.lcov \
  --baseline .github/ci/coverage-baseline.json \
  --json summary.json --markdown summary.md
```

Run `python3 .github/ci/coverage.py --self-test` after you change the
script. The CI `lint` job runs it too.

## Guards

`autumn-harvest/tests/integration/coverage_nightly_wiring.rs` checks the
workflow and the baseline. It runs with the no-DB tests. It requires a
recorded floor for each critical module, the toolchain pin of
`fuzz-nightly.yml`, and the shard count of `test-db-linux`.
