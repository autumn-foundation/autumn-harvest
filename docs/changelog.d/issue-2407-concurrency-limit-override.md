## Feature — override a workflow type's concurrency limit at runtime (issue #2407)

`WorkflowInfo::concurrency` caps the runs that share a key. Its `limit` was
fixed at startup. `PATCH /admin/workflow-types/{workflow_name}/run-defaults`
now takes `concurrency_limit`, with the tri-state, the audit row and the cache
of the other run-default fields. `GET` shows it in `code`, `override` and
`effective`. The key and `on_conflict` stay deploy-only.

**Semantics.** A start stores its limit on its task row. So a lower limit
applies to the next start. A run in flight keeps its task and its cap, and a
lower limit cancels no run. A higher limit raises the stored cap of each live
task row of the type on every shard, so the next claim admits the waiting
starts. Further raises 10 seconds, 1 minute and 5 minutes later lift the
starts that a process with a stale cache stored. The response reports `concurrency_raise.raised_tasks`.
Clearing the override restores the code value. The route refuses a limit for
a type with no concurrency policy, for a latest-wins type
(`cancel_running`), and a limit of zero or above 2147483647. A later drop of
a latest-wins limit would cancel runs in flight.

**`GET /admin/concurrency`.** Each workflow type with a policy in a
`workflow` group now reports `code_limit`, `override_limit` and
`effective_limit`.

**One resolver.** `HandlerRegistry::concurrency_policy` applies the override.
Every start path calls it, or an `apply_concurrency*` helper. The guard
`concurrency_override_guard` fails on a `.concurrency` read that is not an
argument of such a call. A debounce, throttle, event-batch, completion-trigger
or cross-shard child start now resolves the limit when it fires. A same-type
continue-as-new successor and a workflow retry take a raised limit.

**Migration.** `20261104173743_harvest_run_default_concurrency_limit` adds the
nullable column `concurrency_limit` to `harvest_run_default_overrides`, and
widens the row check to four columns. No `WorkflowEvent` variant and no replay
impact. Design: `docs/design/DESIGN-2407.md`.

**Upgrade guide (for the next release).** Run the migration. During a roll,
an older process refuses `concurrency_limit` with a 400 and starts runs with
the code limit. So set a limit only after every process runs the new release,
and send no `PATCH` to an older process once a limit is stored.
See the migration row in `docs/upgrading/0.5.0.md`.

**Tests.** Unit tests on the fields, the merge, the cache and the resolvers.
DB tests on the store, the table check and the raise. HTTP tests for each
acceptance criterion, each refusal and the second raise. A source guard over
the start paths.
