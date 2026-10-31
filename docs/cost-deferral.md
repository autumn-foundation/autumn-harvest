# Cost-aware deferral of activities

A Harvest worker can hold some activity types while a cost signal is high.
The signal is a live number, such as grid carbon intensity in g/kWh or a
power price. Held work moves out of the expensive window and runs when the
signal falls.

The signal is the autumn-web `CostSignal` (autumn#1720). autumn already uses
it to defer `#[job(deferrable)]` jobs and `#[scheduled(deferrable)]` ticks.
With this feature, one threshold and one runtime-config value drive autumn
jobs and Harvest activities alike.

---

## What the hold does

While the signal is high, the worker claims no task of a held activity type.

- The task stays `PENDING` in `harvest_task_queue`.
- The hold uses no attempt and no retry budget. The first run after the
  window is attempt 1.
- The task holds no worker slot.
- Other activity types on the same queue run as usual.
- Every workflow task runs. A workflow task is cheap, and holding it would
  stop the whole run.

When the signal falls, the next poll claims the task. A worker polls at
least once every `poll_interval` (500 ms by default), so held work starts
within about one poll interval.

The worker reads the signal once before each claim. The hold adds no SQL,
no claim gate and no migration: it puts each held type into the claim's
skip list, the same list the adaptive concurrency limit uses (issue #1836).

---

## Turn it on in an autumn app

1. Set the threshold in the autumn config:

   ```toml
   [cost]
   defer_threshold = 400.0
   ```

2. Name the activity types to hold:

   ```rust,ignore
   use autumn_harvest_plugin::HarvestPlugin;

   let harvest = HarvestPlugin::new()
       .workflows(workflows![nightly_rebuild])
       .activities(activities![rebuild_search_index, train_model, charge_card])
       .cost_deferrable_activities(["rebuild_search_index", "train_model"]);
   ```

3. Feed the signal. Use one of these:

   - Set it from code: `state.extension::<CostSignal>()` and then
     `signal.set(520.0)`.
   - Set it with no redeploy: declare `registry.define_cost_signal()?` in
     the runtime-config registry, then run
     `autumn config set autumn_cost_signal 520`.

   See the autumn guide `docs/guide/cost.md` for both.

The plugin reads the `CostSignal` from the app state at startup. The signal
is high while its value is above `defer_threshold`. With a runtime-config
signal, it is also high until the first value arrives, so held work does not
start before the app knows the signal.

`HarvestPlugin::cost_hold_recheck` sets how long a held dispatch reference
waits before it returns to the dispatch channel (issue #1312). The default
is 5 seconds. A worker with no dispatch channel does not use it.

## Turn it on without the plugin

Build the hold yourself and give it to the worker config. Any
`Fn() -> bool + Send + Sync` is a signal source:

```rust,ignore
use autumn_harvest::builder::WorkerConfig;
use autumn_harvest::cost_hold::CostHold;

let hold = CostHold::new(move || grid.carbon_intensity() > 400.0, ["train_model"]);
let worker = WorkerConfig::default().with_cost_hold(hold);
```

With an autumn `CostSignal` in hand, for example for a `HarvestEmbedding`,
use `autumn_harvest_plugin::cost::autumn_cost_hold(signal, names)`.

---

## Rules that `try_build` checks

- A local activity in the hold fails the build. A local activity runs inline
  in its workflow task, so a hold cannot defer it.
- A name that no activity registers logs a warning and has no effect.
- A held activity with a `schedule_to_start` default logs a warning. See
  the limits below.

---

## See the hold

| Surface | What it shows |
|---|---|
| `harvest.cost.hold` gauge | `1` while a worker holds its deferrable types, `0` otherwise. A worker sets it at its first claim and at each change. Use `max(harvest_cost_hold)` across replicas. The starter dashboard has a panel for it. |
| Worker log | One `info` line when the signal rises, and one when it falls. |
| `GET /admin/config` | `cost_hold_activities` lists the held types of the worker. It does not show the live signal. |
| autumn `GET /actuator/cost` | The signal value, the threshold and whether it is high. |

A rising queue depth or oldest-pending age while `harvest_cost_hold` is `1`
is deferred work that waits for a cheaper window. It is not a capacity
incident.

---

## Limits

- **`schedule_to_start` keeps running.** That timer is a start deadline. A
  window longer than the deadline fails the task with a `ScheduleToStart`
  timeout. A queue pause (issue #619) suspends this timer. A cost hold does
  not, because a high price is not an outage. Do not set `schedule_to_start`
  on a held type unless a deadline must win over the price.
- **The database cannot see the hold.** A held task counts in
  `harvest.queue.depth` and in `harvest.queue.oldest_pending_age`. Load
  shedding (issue #1794) reads the backlog age, so it can trip on held work.
  Put held types on their own queue, and leave that queue out of the
  load-shed config.
- **Each claim scans past held rows.** A large held backlog at the head of a
  queue makes each claim on that queue walk past those rows. The adaptive
  limit has the same cost. A dedicated queue keeps it off the hot queues.
- **A claim before the rise runs.** A worker that claimed a task just before
  the signal rose runs it.
- **The hold is per worker.** Each worker reads its own copy of the signal.
  Replicas that read one runtime-config key agree within
  `signal_refresh_secs`.
- **Postgres only.** The `autumn-harvest-sqlite` backend does not use the
  hold.
