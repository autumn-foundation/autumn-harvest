# Design — CI runs the root-only mount tests (issue #2757)

Three tests in `sandbox::fs_isolation` return early when the worker is not
root. The CI job that runs them uses a non-root user. The sudo step of
`sandbox-kvm.yml` runs only tests whose names start with `root_`. So no CI
job runs the three tests, and each one passes with no sign.

**No migration. No new `WorkflowEvent` variant. No replay impact.** The
change is in test code, a CI workflow and the docs.

---

## 1. Facts

- The three tests are `a_pivot_runs_a_dedicated_uid_with_no_network`,
  `a_mask_does_not_leak_into_a_shared_root` and
  `a_pivot_ignores_a_strict_worker_umask`.
- They test the `run_as` uid in a pivot, the private propagation before a
  mask, and the umask fix for a pivot.
- `cargo sandbox-test` in `Test (no-db, ubuntu-latest, shard 1)` runs as
  the `runner` user.
- The root step of `sandbox-kvm.yml` runs `"${bin}" root_ --ignored` with
  sudo. It runs on each pull request that changes `src/sandbox`, and
  weekly.
- `exec.rs`, `launcher.rs` and `kvm_tests.rs` already name their root
  tests `root_`. Each one is `#[ignore]` with a reason that names root.
- `sudo` clears the environment. So the root step does not pass
  `HARVEST_TEST_REQUIRE_ISOLATION` to the tests. A missing namespace then
  skips the root tests with no sign, as in issue #2741.

## 2. Brainstorm: the options

| Option | Result | Cost |
|---|---|---|
| A. Rename to `root_`, mark `#[ignore]`. The KVM root step runs them | Uses the current convention and step | Small |
| B. A new sudo step in `test-nodb` with a name filter | Runs on each code change. A second copy of the sudo logic | Medium |
| C. Run all of `cargo sandbox-test` with sudo | The non-root paths then have no test | Small, but loses coverage |
| D. A gate variable that fails a test on a non-root host | CI has no root host in that job, so CI goes red | Small, but wrong |

**Decision: A.** It follows the `root_` convention of `exec.rs`. The root
step then passes `HARVEST_TEST_REQUIRE_ISOLATION=1` through `sudo env`.

## 3. Reverse brainstorm: how to make a root test never run

| Fault | Answer |
|---|---|
| A test returns early on a non-root worker | A guard fails on a test that is not ignored and checks for root |
| A test needs root but has no `root_` prefix | A guard fails when the ignore reason names root and the name does not start with `root_` |
| A `root_` test is not ignored | The same guard fails. The non-root job would run it and fail |
| `sudo` drops the gate variable | A guard parses `sandbox-kvm.yml`. The root step must set it inside `sudo env` |
| The root step loses its `root_` filter or `--ignored` | The same guard checks both |
| The KVM workflow does not run for a change | Its path filter covers `autumn-harvest/src/sandbox/**` |
| A local root run skips the tests | The docs give the command with `--ignored` |

## 4. Six hats

- **White (facts).** A root step exists, and it runs on each sandbox
  change. Eleven tests already use the `root_` convention.
- **Red (feel).** A green job that tested nothing is worse than a red job.
  One convention for root tests is easier to keep than two.
- **Black (risk).** The KVM job runs only for sandbox paths, not for each
  code change. A local root run now needs `--ignored`. A source scan can
  miss a skip written in a new way.
- **Yellow (gain).** No new CI step. The three tests run as root with the
  gate on. The guards stop the next silent root skip.
- **Green (new ideas).** One guard covers all of `src/sandbox`, not only
  these three tests. The same guard checks the root step wiring.
- **Blue (process).** Red: the guards fail on the current tree. Green: the
  rename, the `#[ignore]`, the root assert and the workflow line.
  Refactor: shared helpers in the guard, and the docs.

## 5. Design

- Each of the three tests gets the `root_` prefix, the reason
  `needs root; the Sandbox (KVM) workflow runs it with sudo`, and an
  assert for root in place of the skip.
- The root step of `sandbox-kvm.yml` adds
  `HARVEST_TEST_REQUIRE_ISOLATION=1` to its `sudo env`.
- `sandbox::isolation_gate` gets two guards:
  - `no_sandbox_test_skips_without_root` scans the test functions in
    `src/sandbox`. A test that is not ignored must not check for a
    non-root worker. A test whose ignore reason names root must start
    with `root_`, and a `root_` test must be ignored.
  - `the_sudo_step_requires_isolation` parses `sandbox-kvm.yml`. The step
    that runs the `root_` tests with sudo must set the gate variable.

## 6. Acceptance criteria

1. No test in `src/sandbox` returns early because the worker is not root.
2. The root step of `sandbox-kvm.yml` runs the three tests.
3. In that step, a missing namespace fails a test.
4. The two guards fail on the tree before this change.
