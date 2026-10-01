## Phase 5.x — the chaos suite runs nightly, with a watchdog (issue #1790)

`chaos.yml` did not parse from its first commit. Two `run:` values held
`chaos:: ` unquoted, and YAML reads `: ` as a mapping value. GitHub ran the file
as a zero-job failure and never fired its cron. So the race reproducers and the
seeded convergence sweep never ran. The `core:chaos_tests` allowlist reason in
`ci_run_coverage` said the suite ran in CI. That was false.

- `chaos.yml`: the two values are now single-quoted. The text is the same as in
  PR #1773.
- `ci_run_coverage` parses each workflow that its reasons cite, with
  `serde_yaml` (YAML 1.2, a repeated key fails). It checks that `chaos.yml` has
  a cron and runs `chaos_tests::` with the `chaos` feature. Both checks failed
  on the old file (RED: `line 57 column 72`).
- New `chaos-watchdog.yml` runs `.github/ci/chaos-watchdog.sh` daily. With no
  successful scheduled chaos run in 48 h, it opens one issue, or comments on
  the open one. After a success, it closes that issue. An API error fails the
  run. Seven `chaos_watchdog` tests run the script against a stub `gh`.
- The allowlist and feature-gate reasons for `core:chaos_tests` now cite the
  check that proves the claim.

No `WorkflowEvent` variant, no migration, no production code change.
`serde_yaml` is a new dev-dependency, at the version already in the lockfile.
