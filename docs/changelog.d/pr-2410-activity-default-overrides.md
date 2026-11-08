## Override an activity type's retry policy and timeouts at runtime (issue #2410)

An operator can now change the defaults of an activity type with no deploy.
`PATCH /admin/activity-types/{activity_name}/defaults` takes
`retry_max_attempts`, `retry_initial_interval_ms`, `retry_max_interval_ms`,
`start_to_close_ms`, `heartbeat_timeout_ms`, `schedule_to_start_ms` and
`schedule_to_close_ms`. `GET` shows `code`, `override` and `effective`. The
design is in `docs/design/DESIGN-2410.md`.

- The precedence is call site, then override, then code, then builder
  default. A field that the override does not set inherits the code value.
- The override applies at schedule time only. The task row stores the
  resolved values, and a retry reuses the row, so a scheduled activity keeps
  them. `ActivityScheduled` stores none of them, so replay is not affected.
- The typed context calls used to send the code default as a call-site
  value. `WorkflowContext::with_activity_overrides` now resolves the
  override first. The worker applies it to the registered defaults when the
  command carries no value, which covers the raw and DAG calls.
- A local activity resolves the override in the context. The worker
  freezes the result in `LocalActivityScheduled`, so a recovered local
  activity keeps its frozen values.
- A retry field and a heartbeat timeout apply only over a code value. The
  route refuses them for an activity that declares none, and refuses the
  heartbeat and schedule fields for a local activity.
- `RunDefaultsCache` holds the activity overrides next to the workflow
  overrides. Each refresh loads both tables. One failed load keeps its own
  map and never blocks the other.
- Each `PATCH` writes one `activity_type.defaults` audit row. A clear
  restores the code values. `GET /admin/overrides` and the `overrides`
  count of `GET /admin/config` add `activity_default_overrides`.
- `RetryPolicy` now implements `PartialEq`.

**Migration.** `20261106145707_harvest_activity_default_overrides` creates
`harvest_activity_default_overrides` on the default shard. Its `CHECK`
constraints match the decode bounds. No `WorkflowEvent` variant, no replay
impact.

**Tests.** Unit tests on the fields, the merge, the resolver and the cache.
Context tests on each typed call, the call-site precedence and the frozen
local recovery. DB tests on the store, the table checks and each acceptance
criterion through a worker: a scheduled row keeps its values, the replay of
a spanning run succeeds, a frozen local activity keeps its budget, and a
clear restores the code values. HTTP tests on the route, each refusal and
the audit rows.
