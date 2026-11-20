## CI runs the root-only mount tests (issue #2757)

Three tests in `sandbox::fs_isolation` returned early on a non-root worker.
The CI job that runs them is not root. So no CI job ran them, and each one
passed with no sign.

- The tests are now `root_a_pivot_runs_a_dedicated_uid_with_no_network`,
  `root_a_mask_does_not_leak_into_a_shared_root` and
  `root_a_pivot_ignores_a_strict_worker_umask`. Each one is ignored, and
  it asserts root in place of the skip.
- The root step of `sandbox-kvm.yml` runs them with sudo. It passes
  `HARVEST_TEST_REQUIRE_ISOLATION=1` through `sudo env`, so a missing
  namespace fails a root test.
- A local root run of these tests needs `--ignored`.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `no_sandbox_test_skips_without_root` scans the tests in
  `src/sandbox`. A test that is not ignored must not skip on a non-root
  worker. A test that needs root must be an ignored `root_` test.
  `the_sudo_step_requires_isolation` parses `sandbox-kvm.yml`. Design:
  `docs/design/DESIGN-2757.md`.
