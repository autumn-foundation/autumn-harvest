# ADR 0015: Workspace automation runs through `cargo xtask`

## Status

Accepted.

## Context

- The repository ran its automation as about a hundred shell and Python
  scripts. They lived in `docs/audits/`, `.github/ci/`, `scripts/`,
  `benchmarks/`, `fuzz/` and three crate `scripts/` directories.
- The CI gates were Python. They needed a Python interpreter, and two of
  them needed PyYAML. Nothing type-checked them, so a refactor of the
  Rust code that they parse could break them silently.
- The shell drivers repeated the same boilerplate: find the repository
  root, start a database, export variables, run `cargo`. Each script had
  its own copy.
- Guard tests read the scripts as text to check their behaviour. A test
  that parses a shell script checks spelling, not behaviour.

## Decision

1. All workspace automation is one binary crate, `xtask`, a workspace
   member with `publish = false`. The alias in `.cargo/config.toml` runs
   it as `cargo xtask <group> <command>`.
2. The groups are `audit`, `ci`, `check`, `dev`, `sandbox`, `bench` and
   `perf`. Each former script is one subcommand with the same flags, the
   same output and the same exit codes. `docs/xtask.md` maps each command
   to the script that it replaced.
3. Commands use the [`smop`](https://docs.rs/smop) scripting crate for
   processes, files, paths and the command line. Only the features that
   xtask needs are on, so the crate builds in seconds.
4. A gate with fixtures keeps its `--self-test` flag for CI. Its fixtures
   also run as unit tests under `cargo test -p xtask`.
5. A guard test that once ran a script now runs the built binary through
   `autumn-harvest/tests/integration/xtask_bin.rs`. It checks what the
   command does, not how its source is spelled.
6. Two script sets stay as they are:
   - The Python lab of `examples/autoresearch` is the workload that the
     example runs. It is not automation of this repository.
   - The apparatus in `docs/assays/apparatus/` and `docs/perf-artifacts/`
     is the exact harness behind a published measurement. A rewrite
     would break the claim that the measurement reproduces as recorded.

## Consequences

- CI needs no Python for its gates. The OpenAPI spec validator is an
  external tool and still runs under `setup-python`.
- xtask follows the workspace lints, `cargo fmt` and the comment-hygiene
  gate like every other crate.
- A gate change is a Rust change, so the compiler and `cargo test -p xtask`
  check it before CI runs it.
- The first `cargo xtask` call in a job compiles the crate. The cost is
  under a minute on a cold cache.
