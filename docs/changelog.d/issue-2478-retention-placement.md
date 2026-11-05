## Plugin — place the retention janitor by process role (issue #2478, epic #2476)

**Behaviour change on `web` replicas.** The retention janitor now runs
where the scheduler runs. An `AUTUMN_ROLE=web` replica runs no scheduler,
so it runs no retention sweep. A `worker` or `combined` replica, or a
standalone runner with the scheduler on, still runs it. Set `[harvest.retention] run_on = "all"`
or `AUTUMN_HARVEST_RETENTION__RUN_ON=all` to keep the old behaviour.

- New config: `HarvestRuntimeConfig::retention`, a `HarvestRetentionConfig`
  with `run_on: RetentionRunOn`. The values are `scheduler` (the default)
  and `all`. An unknown value stops the boot. The environment value
  ignores case. `RetentionRunOn` is `#[non_exhaustive]`.
- This is **source-breaking** for a `HarvestRuntimeConfig` struct literal
  with no `..Default::default()`. Add `retention: Default::default()`.
- An explicit `scheduler_enabled` moves retention with it. Retention that
  is off never runs.
- A process with retention on but not placed logs one `info!` that names
  `run_on` and the fix.
- `POST /admin/retention/run-now` on a process with no placed janitor
  returns 503. The message names `run_on = "all"`. On a split fleet the
  `web` replicas serve the API, so `harvest retention run-now` returns 503
  there by default.
- `GET /admin/retention` on such a process adds `placed_elsewhere: true`.
  An empty `per_shard` then means "not here", not "idle". Every other
  body is unchanged.
- A runner with no role and the scheduler off logs a `warn!`, not an
  `info!`. Its retention may then run nowhere.
- `SpawnPlan` also decides the dispatch-metrics sampler (only with no
  worker) and the post-commit notify sender (each process). The test
  `each_role_places_each_runtime` pins each runtime on each role. The
  rationale is in `docs/operations/process-roles.md`.
- The SQLite backend runs retention in its driver on every role, as
  before. It ignores `run_on`.
- Registration as Autumn background tasks waits for autumn#3545. Issue
  #2480 tracks it.

No `WorkflowEvent` variant, no migration, no replay impact.

`formal/verus/src/process_role.rs` proves `retention_placed` and
`runs_retention` for every input. With `run_on = "scheduler"`, retention
runs exactly where the scheduler runs. A `web` role with no explicit value
runs none. Each other role, and no role, runs it. `run_on = "all"` runs it
wherever it is on. The DB test `retention_follows_the_scheduler_placement`
in `api_scheduler_integration.rs` seeds an expired run and checks the
sweep on each placement. The design is in `docs/design/DESIGN-2478.md`.
