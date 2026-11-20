# Design — no silent skip of the isolation tests in CI (issue #2741)

A test that needs a namespace starts with a probe. On a host without the
namespace, the test prints `skipped` and passes. libtest hides the output
of a test that passes. So CI can pass with no isolation test run, and a
regression in `Mask` or `Pivot` stays green.

**No migration. No new `WorkflowEvent` variant. No replay impact.** The
change is in test code, the CI workflow and the docs.

---

## 1. Facts

- Most runtime tests in `sandbox::fs_isolation` skip when
  `fs_isolation_available()` is false.
- `the_probe_matches_the_run` and the network test in `sandbox::exec`
  do not skip. They test the fail-closed path instead. That path is
  correct, but the isolated path then has no test.
- The `Test (no-db, ubuntu-latest, shard 1)` job runs
  `cargo sandbox-test` as a non-root user.
- Recent Ubuntu releases set
  `kernel.apparmor_restrict_unprivileged_userns=1`. A user namespace then
  has no capability to mount. Both probes can fail for that reason.
- The repository already turns a skip into a failure with
  `HARVEST_TEST_REQUIRE_REDIS=1` and `HARVEST_DEV_REQUIRE_POSTGRES=1`.

## 2. Brainstorm: the options

| Option | Result | Cost |
|---|---|---|
| A. An opt-in variable. A probe that fails under it panics | CI goes red. A developer host still skips | Small |
| B. Always panic on Linux when the probe fails | A container without user namespaces cannot run the suite | Small, but hostile |
| C. Mark the tests `#[ignore]`, and run them with `--include-ignored` in CI | A local run never runs them | Small |
| D. Show the skip lines in CI with `--show-output` | Visible, but still green | Small |
| E. One canary test that asserts both probes | Red, but the other tests still skip one by one | Small |

**Decision: A.** One test-only module, `sandbox::isolation_gate`, holds
the rule. Each probe-gated test calls it. CI sets
`HARVEST_TEST_REQUIRE_ISOLATION=1` in the sandbox test step. The name
follows `HARVEST_TEST_REQUIRE_REDIS`. It covers the mount and the network
namespaces, because both probes fail for the same reason.

## 3. Reverse brainstorm: how to make it fail

| Fault | Answer |
|---|---|
| A typo in the variable name | One constant. A test parses the CI workflow for the same name |
| A value such as `true` turns nothing on | Accept `0`, `1` or an empty value only. Any other value panics |
| A value that is not UTF-8 | Panic, as for an unknown value |
| A test calls the probe directly and bypasses the gate | Route each probe call in the tests through the gate |
| A fail-closed branch passes as if it tested the isolated path | Call the gate before that branch |
| A test changes the variable while another test reads it | No test sets the variable. The rule is a pure function of its input |
| Somebody removes the variable from CI | A wiring test fails |
| The runner refuses user namespaces, so CI goes red | A Linux step sets the sysctl to `0` when it exists. It prints the old value first |
| The sysctl does not exist on the kernel | The step checks for the file first |
| The panic does not say what to do | The message names the sysctl |
| Tests that need root skip in CI | Not a probe skip. Out of scope here, see issue #2757 |

## 4. Six hats

- **White (facts).** Most runtime tests in `fs_isolation`, and some tests
  in `exec`, depend on a namespace. CI runs them as a non-root user. Nothing in CI shows whether they ran.
- **Red (feel).** A green job that tested nothing is worse than a red job.
  A developer does not want a local run to fail for a host reason. So the
  rule is opt-in.
- **Black (risk).** The first CI run can go red. The sysctl weakens the
  runner a little, but the runner is temporary. A typo in the value can
  turn the gate off. So an unknown value panics.
- **Yellow (gain).** A return of the `mode=000` mask of #2719 now fails CI.
  The same gate covers the network namespace at no extra cost.
- **Green (new ideas).** A later change can run the root-only tests with
  `sudo`, as the `Sandbox (KVM)` workflow does.
- **Blue (process).** Red: unit tests for the rule and the wiring test.
  Green: the gate module, the test changes and the CI step. Refactor: the
  `MOUNT_NS` constant, a YAML parse in the wiring test, and the docs.

## 5. Design

```rust
// sandbox/isolation_gate.rs, compiled for Linux tests only.
pub const REQUIRE_VAR: &str = "HARVEST_TEST_REQUIRE_ISOLATION";

/// Returns `available`. Panics when it is false and CI requires isolation.
/// Panics also on a value other than `0`, `1` or empty.
pub fn host_has(available: bool, what: &str) -> bool;
```

- `needs_isolation()` in `fs_isolation` calls `host_has`.
- The umask test, the shared-root test and `the_probe_matches_the_run`
  call `host_has` before they skip or take the fail-closed branch.
- The network test in `exec` calls `host_has` before its fail-closed
  branch. The process-limit test calls it before it skips for want of a
  user namespace.
- The Linux CI step relaxes the AppArmor sysctl, then runs
  `cargo sandbox-test` with the variable set.

## 6. Acceptance criteria

1. With `HARVEST_TEST_REQUIRE_ISOLATION=1`, a host without a mount or a
   network namespace fails each probe-gated test.
2. Without the variable, such a host skips as before.
3. A value other than `0`, `1` or an empty value panics.
4. The CI sandbox test step sets the variable, and a test proves it.
5. On a host with namespaces, each test runs, with or without the
   variable.
