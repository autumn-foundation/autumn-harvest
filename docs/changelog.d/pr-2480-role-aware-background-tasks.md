## Harvest runtimes as Autumn background tasks (issue #2480)

`autumn-harvest-plugin` now needs `autumn-web` 0.11, and Autumn places each
Harvest runtime by role. One `AUTUMN_ROLE` per process is enough. See
[`docs/upgrading/autumn-web-0.11.md`](../upgrading/autumn-web-0.11.md) and
[`docs/operations/process-roles.md`](../operations/process-roles.md).

- **Capabilities.** Harvest declares `harvest.worker` (default from
  `job_workers`), `harvest.scheduler` (from `scheduler`) and
  `harvest.maintenance` (from `maintenance`). A `[roles.<name>]` preset sets
  each one: `capabilities = { "harvest.worker" = false }`. A preset with no
  Autumn job workers can keep the Harvest worker with `"harvest.worker" =
  true`.
- **Background tasks.** The worker, the scheduler, the retention janitor and
  the batch executor run as the Autumn tasks `harvest.worker`,
  `harvest.scheduler`, `harvest.retention` and `harvest.batch`. The startup
  hook builds each loop, and the task runs it until the Autumn drain. No
  loop runs on a detached `tokio::spawn` from `on_startup`. A standalone
  runner spawns its loops as before. `/actuator/tasks` and `autumn graph`
  list each task and the roles that run it.
- **Queues.** The task `harvest.queues` declares the Harvest queues, so the
  role `pin` check, `autumn jobs manifest` and `autumn doctor` include them.
- **Overrides.** `worker_enabled`, `scheduler_enabled` and
  `AUTUMN_HARVEST__WORKER_ENABLED` stay as explicit overrides, with the
  warning against the role. The warning now names the capability. The
  warning for a named role that Harvest did not read is gone.
- **Engine.** `SchedulerRuntime::prepare_sharded` and
  `RetentionRuntime::prepare_with_hooks` build a loop with no spawn.
  `autumn_harvest::prepared_loop::PreparedLoop` runs it under a host token.
- **Dependencies.** `diesel` 2.4, `diesel-async` 0.10, `rusqlite` 0.42 and
  `libsqlite3-sys` 0.40, which fixes RUSTSEC-2026-0459. `split_table!`
  bridges the new `RETURNING` source of `diesel` 2.4. The plugin drops
  `chrono`, because `autumn-web` 0.11 takes `jiff` values.
- **Proof and tests.** `formal/verus/src/process_role.rs` proves that a
  built-in role keeps the 0.10 placement of the janitor, and that a preset
  cannot turn on a capability above its base. `background.rs` tests each
  role through Autumn: built-in roles, a preset that turns the Harvest
  worker off, the reverse, explicit switches and `/actuator/tasks`.
