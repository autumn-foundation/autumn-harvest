## Phase 5.x — the chaos suite runs nightly, with a watchdog (issue #1790)

`chaos.yml` did not parse from its first commit. Two `run:` values held
`chaos:: ` unquoted, and YAML reads `: ` as a mapping value. GitHub ran the file
as a zero-job failure and never fired its cron. So the race reproducers and the
seeded convergence sweep never ran. The `core:chaos_tests` allowlist reason in
`ci_run_coverage` said the suite ran in CI. That was false.

- `chaos.yml`: the two values are now single-quoted. The change matches
  PR #1773, so the two changes merge cleanly.
- `ci_run_coverage` parses every workflow file with `serde_yaml` (YAML 1.2, a
  repeated key fails). It checks that `chaos.yml` has a cron and runs
  `chaos_tests::` with the `chaos` feature. An `if` or a
  `continue-on-error: true` on that step or job fails the check. So does a flag
  such as `--no-run`, or `|| true`, in the step's `run:` text. On the old file,
  both tests failed at the parse step (RED: `line 57 column 72`).
- New `chaos-watchdog.yml` runs `.github/ci/chaos-watchdog.sh` daily. With no
  successful scheduled chaos run in 48 h, it opens one issue, or comments on
  the open one. After a success, it closes that issue. An API error fails the
  run. An `if: failure()` step then opens a separate issue for the failed
  watchdog run, also when checkout failed. The next clean run closes it. A
  concurrency group runs one watchdog job at a time. Thirteen `chaos_watchdog`
  tests run the script or the report step against a stub `gh` and the real `jq`
  on Linux. Four more check the watchdog workflow.
- The chaos suite passes locally against Docker Postgres: 9 integration tests
  (seven race reproducers, the seeded sweep and its seed-set test) and 25
  harness unit tests.
- The allowlist and feature-gate reasons for `core:chaos_tests` now cite the
  check that proves the claim.

No `WorkflowEvent` variant, no migration, no production code change.
`serde_yaml` is a new dev-dependency, at the version already in the lockfile.
