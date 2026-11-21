# DESIGN-2480: Harvest runtimes on the Autumn background-task API

Part of epic #2476. Upstream: autumn#3545 (background tasks) and autumn#3542
(capabilities). Both ship in `autumn-web` 0.11.0.

## Goal

Harvest declares what it runs. Autumn places it by role. One `AUTUMN_ROLE`
per process is enough.

## Facts

- `autumn-web` 0.11 adds `autumn_web::background`. A plugin declares a
  `PluginCapability` with a built-in parent. It registers a
  `BackgroundTask` with `AppBuilder::background`. Autumn starts the task
  after the startup hooks, on a role that runs its capability.
- `[roles.<name>] capabilities = { "harvest.worker" = false }` sets a plugin
  capability for one named role. A role cannot turn on a capability that
  its base does not run.
- `BackgroundTask::explicit(Some(v))` is the switch of the plugin config.
  `Some(true)` runs on every role, with a warning. `Some(false)` never runs.
- A task with `queues` runs only on a role that runs Autumn job workers and
  whose pin admits one of its queues. `autumn jobs manifest` and the pin
  check read the queues of every task.
- Autumn cancels the task token at shutdown. It waits for at most half of
  the remaining `server.shutdown_timeout_secs`, then aborts. It drains the
  tasks before the `on_shutdown` hooks run.
- `/actuator/tasks` (`background_tasks`) and `autumn graph`
  (`background_task`) list each task and the roles that run it.
- Today `HarvestRunner::start` spawns the worker, the scheduler, the
  retention janitor and the batch executor with `tokio::spawn`, from the
  `on_startup` hook. It reads only the base of a role.

## Brainstorm

1. Register four tasks: `harvest.worker`, `harvest.scheduler`,
   `harvest.retention` and `harvest.batch`. The runner prepares each loop.
   The task runs the loop until its token is cancelled.
2. Let each task call the old `spawn` and hold the handle. Simple, but each
   loop still runs on a detached `tokio::spawn`.
3. Move the whole runner into one background task. The management API then
   has no runtime until the task starts, and an API-only role has none.
4. Compute the placement in Harvest from `Role::runs_plugin`, and use the
   same explicit switch on each task. Then the runner and Autumn agree.
5. Declare the Harvest queues on the worker task.
6. Declare the Harvest queues on a separate marker task, `harvest.queues`.

## Reverse brainstorm: how could this fail?

| Failure | Guard |
|---|---|
| The runner prepares a loop that no task runs, or a task finds no loop. | One function gives both the explicit switch of a task and the runner plan. A test checks the two on each role. A task with no loop fails, so `/actuator/tasks` shows `failed`. |
| A queue on the worker task blocks the worker on a role with `job_workers = false`. The acceptance criterion needs that role. | The worker task has no queues. The `harvest.queues` marker carries them. |
| A detached loop outlives the drain. | Each task awaits its loop. The loop stops on the task token. |
| The worker drain needs more time than Autumn gives it. | The upgrade note says to size `server.shutdown_timeout_secs`. `on_shutdown` still calls `Worker::shutdown`, and the leases recover an aborted task. |
| A standalone runner loses its loops. | With no host, the runner spawns as before. |
| A named role changes the retention placement of an existing fleet. | Before this change, Harvest read only the base of a named role. Built-in roles keep the table of 0.10. Verus proves it. |
| `run_on = "all"` warns on each `web` replica. | That is the Autumn warning for an explicit switch. The guide says so. |

## Six thinking hats

- **White (facts).** The API above. Harvest 0.10.0 is released, so the
  bump goes to the next minor release.
- **Red (feel).** Operators want one knob. A fake task looks odd, but a
  queue pin that silently skips Harvest looks worse.
- **Black (risk).** A public API change: `ProcessSwitches::resolve` takes a
  role. Shutdown order changes: the loops stop before `on_shutdown`.
- **Yellow (value).** Named roles toggle Harvest. `/actuator/tasks` and
  `autumn graph` show where each runtime runs. Doctor sees Harvest queues.
- **Green (options).** Options 1, 4 and 6. Option 2 keeps detached spawns.
  Option 3 breaks the API-only role. Option 5 breaks the reverse toggle.
- **Blue (process).** Bump, then red tests, then green, then refactor.
  Update the Verus model, the guides and the changelog. Review, then map
  each criterion to its evidence.

## Decision

- Three capabilities: `harvest.worker` (parent `JobWorkers`),
  `harvest.scheduler` (parent `Scheduler`) and `harvest.maintenance`
  (parent `Maintenance`).
- Five tasks:

| Task | Capability | Explicit switch |
|---|---|---|
| `harvest.worker` | `harvest.worker` | `worker_enabled` |
| `harvest.scheduler` | `harvest.scheduler` | `scheduler_enabled` |
| `harvest.retention` | `harvest.maintenance` | off when retention is off; on for `run_on = "all"`; else `scheduler_enabled` |
| `harvest.batch` | `harvest.maintenance` | `worker_enabled` |
| `harvest.queues` | `harvest.worker` | `worker_enabled`, with the declared queues |

- An unset switch takes `Role::runs_plugin` of its capability. With no
  role, it is on.
- The explicit switch keeps the old link of retention to the scheduler and
  of the batch executor to the worker. So each built-in role keeps the
  table of 0.10.
- With a host, `HarvestRunner::start` spawns none of the four loops. It
  puts each prepared loop in `HostedLoops`. Each task takes its loop and
  awaits it with the task token. With no host, the runner spawns as before.
- The codec refresh loop, the dispatch-metrics sampler and the notify
  sender stay in the runner. Each process needs them, and no capability
  runs on every role.
- The SQLite backend keeps its one driver. It is the only writer, so a role
  cannot move it.
- Remove the warning for a named role. Harvest now reads the preset.

## Acceptance criteria

1. A process with only `AUTUMN_ROLE` set runs exactly the Harvest runtimes
   that its role allows.
2. A named role can turn off `harvest.worker` and keep Autumn job workers,
   and the reverse.
3. Harvest runtimes appear in `/actuator/tasks` and `autumn graph`.
4. The minimum `autumn-web` version is 0.11, with a compatibility note.
