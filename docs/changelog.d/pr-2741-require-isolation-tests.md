## CI no longer skips the isolation tests without a sign (issue #2741)

A sandbox test that needs a namespace skipped on a host without one. The
skip passed, and libtest hid its output. So CI could pass with no
isolation test run.

- `HARVEST_TEST_REQUIRE_ISOLATION=1` turns each such skip into a failure.
  The test-only module `sandbox::isolation_gate` holds the rule. It covers
  the mount namespace tests in `sandbox::fs_isolation` and the network
  namespace test in `sandbox::exec`.
- The variable accepts `0`, `1` or an empty value. Any other value fails,
  so a typo cannot turn the gate off.
- The Linux `cargo sandbox-test` step in CI sets the variable. A step
  before it sets `kernel.apparmor_restrict_unprivileged_userns=0` when the
  sysctl exists. It prints the old value first.
- Without the variable, a developer host skips as before.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `sandbox::isolation_gate` tests the rule and the parse of the
  value. `ci_requires_isolation_for_the_sandbox_tests` checks that the CI
  step sets the variable. Design: `docs/design/DESIGN-2741.md`.
