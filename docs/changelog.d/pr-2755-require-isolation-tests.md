## CI fails when it cannot run an isolation test (issue #2741)

A sandbox test that needs a namespace skipped on a host without one. The
skip passed, and libtest hid its output. So CI could pass with no
isolation test run.

- `HARVEST_TEST_REQUIRE_ISOLATION=1` turns each such skip into a failure.
  The test-only module `sandbox::isolation_gate` holds the rule. It covers
  the mount namespace tests in `sandbox::fs_isolation`, and the network
  and user namespace tests in `sandbox::exec`.
- The variable accepts `0`, `1` or an empty value. Any other value fails,
  so a typo cannot turn the gate off.
- The `cargo sandbox-test` step in CI sets the variable. Only the Linux
  tests read it. A Linux step before it sets
  `kernel.apparmor_restrict_unprivileged_userns=0` when the sysctl exists.
  It prints the old value first. The nightly coverage job sets the sysctl
  too, so the isolation code shows as covered.
- Without the variable, a developer host skips as before.
- No migration. No `WorkflowEvent` variant. No replay impact.
- Tests: `sandbox::isolation_gate` tests the rule and the parse of the
  value. `ci_requires_isolation_for_the_sandbox_tests` parses `ci.yml`. It
  checks that each `cargo sandbox-test` step sets the variable. Design: `docs/design/DESIGN-2741.md`.
