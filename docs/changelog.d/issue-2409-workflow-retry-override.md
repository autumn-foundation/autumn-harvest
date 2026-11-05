## Feature — override a workflow type's retry policy at runtime (issue #2409)

`WorkflowInfo::retry_policy` decides whether a failed run starts another
attempt. Only a deploy changed it.
`PATCH /admin/workflow-types/{workflow_name}/run-defaults` now takes
`retry_max_attempts`, `retry_initial_interval_ms` and
`retry_max_interval_ms`, with the tri-state, the audit row and the cache of
the other run-default fields. `GET` shows them in `code`, `override` and
`effective`. The backoff coefficient, the jitter and `non_retryable_errors`
stay deploy-only.

**Semantics.** Each start still stores its policy on the execution row. The
failure path applies the override over that stored policy, through
`run_defaults::current_retry_policy`. So a change applies to the next
failure of any run of the type, in flight or new. `retry_max_attempts: 1`
stops the retries. A longer `retry_initial_interval_ms` slows them. An
attempt that already started keeps its row, its fire time and its stored
policy. A retry successor stores the policy of its run, never the effective
one, so a clear restores the code value. The fleet ceiling still caps
`max_attempts`.

**A schedule policy wins.** A schedule's own `retry_policy` still wins for
the runs that the schedule starts. Each schedule path picks its policy
through `workflow_retry::schedule_retry_policy`, which marks a schedule
policy with `_harvest_retry_source: "schedule"` in the stored JSON.
`StartWorkflowParams` gains `workflow_retry_source`. A retry, a same-type
continue-as-new and a re-run keep the mark. The guard
`retry_override_guard` fails on a schedule path that picks its policy by
hand. It also fails on a schedule start that does not take its source from
the helper, and on a decode of the stored JSON that drops the mark.

**Refusals.** The route refuses a retry field for a type with no code
`retry_policy`, a DAG included, and a `retry_max_attempts` above the fleet
ceiling. The write refuses zero, an interval past 365 days and a cap below
the first delay. A retry starts with a delay, and the start refuses a delay
past 365 days. The effective cap never falls below the first delay. The
declared backoff and jitter still apply.

**Migration.** `20261105062317_harvest_run_default_retry_policy` adds three
nullable columns to `harvest_run_default_overrides`, and widens the row
check to ten columns. No `WorkflowEvent` variant and no replay impact.
Design: `docs/design/DESIGN-2409.md`.

**Upgrade guide (for the next release).** Run the migration. During a roll,
an older process refuses the retry fields with a 400, and an older worker
decides each failure with the stored policy. So set a retry field only after
every process runs the new release. See the migration row in
`docs/upgrading/0.5.0.md`.

**Tests.** Unit tests on `RetryLimits`, the stored source, the merge, the
resolver and the ceiling. DB tests on the store and the table checks, and a
worker-driven test for each acceptance criterion, a scheduler fire included.
HTTP tests on the route, each refusal and the audit rows. A source guard over
the schedule paths and the decodes.
