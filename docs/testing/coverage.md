# Test coverage

The nightly workflow `.github/workflows/coverage-nightly.yml` measures line
and branch coverage (issue #1818). A ratchet stops coverage of the critical
modules from dropping.

## What the nightly measures

- **`unit`** runs the `autumn-harvest` lib tests with `--all-features`.
- **`db`** runs every `linux` row of `.github/ci/integration-suites.txt`.
  It uses the 21 shards of `test-db-linux`.
- **`report`** merges the LCOV file of each job. It publishes the
  `coverage-report` artifact and the run summary.

The test jobs use a dated nightly, because branch coverage needs
`-Zcoverage-options=branch`. The workflow sets `RUSTUP_TOOLCHAIN`, which
wins over `rust-toolchain.toml`.

The nightly does not measure the `linuxpart` or `allos` rows, the property
suites, or the doctests.

## Read the report

Open the run summary, or download the `coverage-report` artifact. It holds
these files:

| File | Content |
|---|---|
| `coverage.lcov` | The merged LCOV data. Load it into an editor plugin or `genhtml`. |
| `summary.json` | Line and branch counts and percentages for each source file. |
| `summary.md` | The critical modules against their floors, each crate, and each file. |

A module is one source file. A line counts as covered when a test runs it
once or more. A branch counts as covered when a test takes it once or more.

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
coverage short, so a drop in that run is noise. The failed job already fails
the run.

A failed scheduled run opens the issue "Coverage nightly: a scheduled run
failed", or comments on it.

### Raise a floor

The summary tells you when a module is above its floor. To raise the floors,
download `summary.json` from the run and run this command:

```sh
python3 .github/ci/coverage.py ratchet --update \
  --baseline .github/ci/coverage-baseline.json --summary summary.json
```

`--update` never lowers a floor. Commit the new baseline in its own PR, with
the run link.

### Lower a floor

Lower a floor only when code leaves the module, or when a test moves to
another tier. Edit the baseline by hand and state the reason in the PR.

## Run it locally

Install the tools once:

```sh
rustup toolchain install nightly-2026-08-14 --component llvm-tools-preview
cargo install cargo-llvm-cov --locked --version 0.9.1
```

Measure the lib tests:

```sh
export RUSTUP_TOOLCHAIN=nightly-2026-08-14
cargo llvm-cov --branch --no-report -p autumn-harvest --all-features --lib
cargo llvm-cov report --branch --lcov --output-path unit.lcov
python3 .github/ci/coverage.py merge --root "$PWD" --output merged.lcov unit.lcov
python3 .github/ci/coverage.py summary merged.lcov \
  --baseline .github/ci/coverage-baseline.json \
  --json summary.json --markdown summary.md
```

Measure DB suites too. The DB suites need Docker:

```sh
source <(cargo llvm-cov show-env --sh --branch)
cargo llvm-cov clean --workspace
bash .github/ci/run-suites.sh run linux
cargo llvm-cov report --branch --lcov --output-path db.lcov
```

Run `python3 .github/ci/coverage.py --self-test` after you change the
script. The CI `lint` job runs it too.

## Guards

`autumn-harvest/tests/integration/coverage_nightly_wiring.rs` checks the
workflow and the baseline. It runs with the no-DB tests.
