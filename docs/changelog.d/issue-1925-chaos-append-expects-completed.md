## Testing — the chaos append test expects the #1871 fix (issue #1925)

No scheduled `chaos.yml` run passed for 48 h, so the watchdog opened #1925.
Two runs failed:

- 2026-10-04: the integration suite did not compile. A test set
  `resident_workflows` twice. `trunk-dev` already has the fix.
- 2026-10-05: `terminate_backend_mid_commit_append` failed on purpose. It
  required `FAILED`, the outcome of bugs #1871 and #1870. #1788
  (`3405cae`) fixed #1871: the worker writes a lost activity result again
  on a new connection. The workflow now completes.

The test now requires `COMPLETED`. Both append tests also require one
activity attempt: one activity task at attempt 1 and one `ActivityStarted`.
The change removes `Accept::KnownFailure`. The restart test still accepts
the #1870 outcome for in-flight activities. A crash restart can make every
repeat of the write fail, and the claim give-back too.
`docs/testing/chaos.md` records the #1871 fix.

No engine code changes. There is no new `WorkflowEvent` variant and no
migration.

**Evidence.** On `trunk-dev`, `terminate_backend_mid_commit_append` fails
locally with the nightly error. With this change, the six
`terminate_backend_*` tests pass. With the result-write repeat turned off,
the append test fails: the worker gives the claim back, a second activity
attempt runs, and the workflow still completes. Only the attempt check
catches that.
