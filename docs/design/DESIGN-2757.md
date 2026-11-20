# Design — CI runs the root-only mount tests (issue #2757)

Three tests in `sandbox::fs_isolation` return early when the worker is not
root. The CI job that ran them used a non-root user. The sudo step of
`sandbox-kvm.yml` runs only tests whose names start with `root_`. So no CI
job ran their checks, and each one passed silently.

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
  sudo. It runs on each non-draft pull request that changes
  `src/sandbox`, on each such push to `trunk-dev`, and weekly.
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

**Decision: A.** It follows the `root_` convention of `exec.rs`,
`launcher.rs` and `kvm_tests.rs`. The root step then passes `HARVEST_TEST_REQUIRE_ISOLATION=1` through `sudo env`.

## 3. Reverse brainstorm: how to make a root test never run

| Fault | Answer |
|---|---|
| A test returns early on a non-root worker | A guard fails on a test that is not `root_` and reads the uid |
| An ignored KVM test skips on a non-root worker | The same rule. It reads the name, not the `#[ignore]` |
| A test keeps an assert inside `if geteuid() == 0` | The same rule. Two such tests in `launcher.rs` and `posture.rs` become `root_` tests |
| A helper skips for want of root, and a test calls it | A helper that prints `skipped:` and reads the uid counts as a check |
| `is_root_device` or `rootfs` reads as a root check | Match whole identifiers and whole words only |
| A test needs root but has no `root_` prefix | A guard fails when the ignore reason names root and the name does not start with `root_` |
| A `root_` test is not ignored | The same guard fails. The non-root job would run it and fail |
| `sudo` drops the gate variable | A guard parses `sandbox-kvm.yml`. The root step must set it inside `sudo env` |
| The root step loses its `root_` filter or `--ignored` | The same guard checks both |
| The KVM workflow does not run for a change | Its path filter covers `autumn-harvest/src/sandbox/**`. `ready_for_review` runs it for a draft that becomes ready |
| A regression leaks a mount onto the runner | The shared-root test makes `/` private, then shared, so it gets a new peer group |
| A local root run skips the tests | The docs give the command with `--ignored` |

## 4. Six hats

- **White (facts).** A root step exists, and it runs on each sandbox
  change. The jailer, snapshot and cgroup tests already use the `root_`
  convention.
- **Red (feel).** A green job that tested nothing is worse than a red job.
  One convention for root tests is easier to keep than two.
- **Black (risk).** The KVM job runs only for sandbox paths, not for each
  code change. A local root run now needs `--ignored`. A source scan can
  miss a skip written in a new way.
- **Yellow (gain).** No new CI step. The three tests run as root with the
  gate on. The guards stop the next silent root skip.
- **Green (new ideas).** One guard covers all of `src/sandbox`, not only
  these three tests. The same guard checks the root step wiring.
- **Blue (process).** Test first: the guards fail on the tree before this
  change. Then the rename, the `#[ignore]`, the root assert and the
  workflow line make them pass. Last, share the helpers and update the
  docs.

## 5. Design

- Each of the three tests gets the `root_` prefix, the reason
  `needs root; the Sandbox (KVM) workflow runs it with sudo`, and an
  assert for root in place of the skip.
- The root branch of a test in `launcher.rs` and one in `posture.rs`
  moves into a new `root_` test of its own.
- The root step of `sandbox-kvm.yml` adds
  `HARVEST_TEST_REQUIRE_ISOLATION=1` to its `sudo env`.
- `sandbox::isolation_gate` gets two guards:
  - `no_sandbox_test_skips_without_root` scans the functions in
    `src/sandbox`. Only a `root_` test may read the uid or call a
    helper that skips for want of root. One test that skips as root on
    purpose is allowed. A test whose ignore reason names root must
    start with `root_`. A `root_` test must have `#[ignore]` with a
    reason that names root.
  - `the_sudo_step_requires_isolation` parses `sandbox-kvm.yml`. The step
    that runs the `root_` tests with sudo must set the gate variable.

## 6. Acceptance criteria

1. Only a `root_` test in `src/sandbox` reads the uid of the worker, with
   one named exception.
2. The root step of `sandbox-kvm.yml` runs the three tests and the two new
   `root_` tests.
3. In that step, a missing namespace fails a test.
4. The two guards fail on the tree before this change.
