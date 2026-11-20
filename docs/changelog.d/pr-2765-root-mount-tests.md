## CI runs the root-only mount tests (issue #2757)

Three tests in `sandbox::fs_isolation` returned early on a non-root worker.
The CI job that ran them was not root. So no CI job ran their checks, and
each one passed silently.

- The tests are now `root_a_pivot_runs_a_dedicated_uid_with_no_network`,
  `root_a_mask_does_not_leak_into_a_shared_root` and
  `root_a_pivot_ignores_a_strict_worker_umask`. Each one has `#[ignore]`,
  and it asserts root in place of the skip.
- The root branches of two tests in `launcher.rs` and `posture.rs` ran
  only as root, so no CI job ran them either. Each branch is now a
  `root_` test of its own.
- The root step of `sandbox-kvm.yml` runs them with sudo. It passes
  `HARVEST_TEST_REQUIRE_ISOLATION=1` through `sudo env`, so a missing
  namespace fails a root test.
- The workflow also runs when a draft becomes ready for review.
- The shared-root test gives its thread a new peer group. A regression
  then cannot leak a mount onto the runner.
- A local root run of these tests needs `--ignored`.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `no_sandbox_test_skips_without_root` scans the functions in
  `src/sandbox`. Only a `root_` test may read the uid or call a helper
  that skips for want of root. A test that needs root must be an ignored
  `root_` test. The scan skips `isolation_gate.rs`, which holds the
  patterns as test data. `the_scan_reads_names_attributes_and_bodies`,
  `a_test_attribute_is_found_in_each_form`, `a_skip_helper_is_a_probe`
  and `the_root_rules_hold` test the scan.
  `the_sudo_step_requires_isolation` parses `sandbox-kvm.yml`. Design: `docs/design/DESIGN-2757.md`.
