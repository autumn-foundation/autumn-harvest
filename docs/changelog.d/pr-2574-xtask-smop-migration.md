## Tooling: every repo script runs as `cargo xtask` on smop (ADR 0015)

PR #2564 added the `xtask` crate, ADR 0015 and the `docs/xtask.md` command
index, with one stub command per script. This entry ports all 96 scripts and
deletes them. The CI gates, doc guards, check guards, perf evidence
generators, bench drivers, sandbox image tools and dev helpers now run as
`cargo xtask <group> <command>`. The groups are `audit`, `bench`, `check`,
`ci`, `dev`, `perf` and `sandbox`. The crate builds on `smop` 0.3.2 for
process, file system and CLI helpers.

Scope:

- `docs/audits/*.py` became `cargo xtask audit <name>`. Each `--self-test`
  keeps its fixtures, and `audit-catalog-coverage` now reads the command list
  from the clap enum.
- `scripts/check-*.sh` became `cargo xtask check <name>`.
- `.github/ci/*.sh` and `coverage.py` became `cargo xtask ci <name>`. The
  `test-db-linux` sharding arithmetic is unchanged, and 21 of 21 shards print
  the same plan.
- `autumn-harvest/scripts/*_perf_repro.sh` became `cargo xtask perf <name>`.
- `benchmarks/*.sh` became `cargo xtask bench run` and `bench notify-commit`.
- The sandbox, smoke, formal and fuzz scripts became `sandbox` and `dev`
  commands.
- `examples/autoresearch/lab/`, `docs/assays/apparatus/` and
  `docs/perf-artifacts/` keep their scripts. They are frozen evidence or an
  external lab.

Design notes:

- Each port is checked for parity against its script. The checks compare
  stdout, stderr and the exit code on the real tree, on the self-test
  fixtures, and on mutated or fuzzed inputs. Tools such as cargo, docker and
  psql are stubbed on `PATH` to record each call. The accepted differences
  are few. An internal error now exits 2 with `error: ...`, where Python
  printed a traceback and exited 1. clap refuses unknown arguments. Messages
  that named a script now name its command.
- The audits that relied on Python regex lookaround use `fancy-regex`. The
  YAML audits use a line-by-line port of the PyYAML 6 scanner and parser, so
  they keep PyYAML 1.1 semantics with no `unsafe` code. `ledger-census
  plan-sqlite` uses `rusqlite`. Every new dependency was already in
  `Cargo.lock`.
- The guard tests that ran a script now run the built binary through
  `tests/integration/xtask_bin.rs`.
- The `lint` job runs `cargo test -p xtask` before any gate. The workspace
  clippy job already covers the crate.
- CLAUDE.md and AGENTS.md name the new commands. CLAUDE.md now says to add
  an xtask command, not a script.

Test evidence: `cargo test -p xtask` passes 565 tests. Every audit passes on
the tree, with its self-test where it has one. The guard suites that read the
ported modules or run the binary pass. They are `ci_run_coverage`,
`ci_nextest_archive`, `coverage_nightly_wiring`, `release_pr_ci`,
`supply_chain_ci`, `chaos_watchdog`, `sandbox_activities_docs`,
`formal_models_coverage`, `formal_trace_coverage`,
`otel_workflow_semconv_docs`, `safety_report_docs`, `verus_proofs_coverage`,
`benchmarks_docs`, `fuzz_nightly_wiring` and the plugin's `openapi_spec`.
