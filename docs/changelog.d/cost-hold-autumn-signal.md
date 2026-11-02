## Phase X.Y — Cost-aware deferral of activities from the autumn cost signal (autumn#1720)

A worker can now hold named activity types while a cost signal is high. The
signal is the autumn-web `CostSignal`: grid carbon intensity or a price,
with a threshold from autumn's `[cost] defer_threshold`. autumn uses the
same signal to defer `#[job(deferrable)]` work, so one threshold and one
runtime-config key now drive autumn jobs and Harvest activities.

- `autumn_harvest::cost_hold`: `CostSignalSource` (any `Fn() -> bool` is
  one) and `CostHold`. Set it with `WorkerConfig::with_cost_hold`.
- `HarvestPlugin::cost_deferrable_activities` reads the app's `CostSignal`
  at startup. `HarvestPlugin::cost_hold_recheck` sets the dispatch-channel
  retry delay. `autumn_harvest_plugin::cost::autumn_cost_hold` builds a hold
  for a `HarvestEmbedding`.
- While the signal is high, the worker puts each held type into the claim
  skip list with `SATURATED_ACTIVITY_MARKER`, the path the adaptive limit
  (issue #1836) uses. A held task stays `PENDING`, uses no attempt and no
  retry budget, and holds no slot. Workflow tasks and other activity types
  run. A held dispatch reference returns after the recheck delay.
- `try_build` rejects a local activity in the hold. An unregistered name and
  a `schedule_to_start` default log a warning. The plugin's `sqlite` build
  refuses a cost hold and names the SQLite epic (#2466).
- New gauge `harvest.cost.hold` (`METRIC_COST_HOLD`, no label), bridged by
  the metrics-rs adapter and the plugin scrape endpoint, with a starter
  dashboard panel. `GET /admin/config` reports `cost_hold_activities`.

No new `WorkflowEvent` variant, no migration, no new SQL. The
`schedule_to_start` timer keeps running for a held task. See
`docs/cost-deferral.md` for that and the other limits.

Test evidence: `cost_hold_tests` (Postgres) holds a type while the signal is
high, runs an unheld type on the same queue, then runs the held task at
attempt 1 after the signal falls. Unit tests cover the skip list, the gauge
edges, the build validation, the effective-config view, the plugin bridge
and the scrape gauge.
