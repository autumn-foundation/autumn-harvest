## Override an activity type's max_concurrent cap at runtime (issue #2411)

An operator can now lower or raise the fleet-wide cap of an activity type
with no deploy. `PATCH /admin/activity-types/{activity_name}/defaults` takes
`max_concurrent`. `GET` shows it in `code`, `override` and `effective`. The
design is in `docs/design/DESIGN-2411.md`.

- Each task row stores its cap, and each claim reads the stored cap. So a
  `PATCH` that names `max_concurrent` or `clear_override` sets the stored cap
  of each `PENDING` and `RUNNING` row of the activity, on every shard. The
  response reports the first pass in `cap_rewrite`.
- A pending task waits under the new cap at the next claim. A running task
  runs on, because the pass changes the cap column only. Its retry waits
  under the new cap.
- The pass runs again after 10 seconds, 1 minute and 5 minutes, for a row
  that a process with a stale cache stored. Each batch takes the activity
  lock of the default shard and stops when a newer change replaced the cap.
- The worker stores the effective cap on each new row. The key stays the
  code key. `HandlerRegistry::activity_concurrency` resolves both.
- A dead-letter replay or redrive of an activity task now restores its key
  and the current cap. Before, a requeued activity task had no cap at all.
- The route refuses `max_concurrent` for an activity with no code cap, for
  a local activity and for an activity that shares its key with another.
- Each `PATCH` writes one `activity_type.defaults` audit row. A clear
  restores the code cap on the live rows too.

**Migration.** `20261107194243_harvest_activity_default_max_concurrent`
adds `max_concurrent INTEGER CHECK (max_concurrent > 0)` to
`harvest_activity_default_overrides` and widens the row check. The table is
small, and `lock_timeout = '5s'` bounds its brief lock. No `WorkflowEvent`
variant, no replay impact.

**Tests.** Unit tests on the field, the merge, the bound, the refusals and
the key sharer. DB tests on the store, the table check, the rewrite, a stale
pass, a settle pass, the scope of the pass, the retry, the DLQ paths and each
acceptance criterion at the claim and through a worker. HTTP tests on the
route, each refusal, the audit rows, the settle pass and the override list.
