## Plugin — worker and scheduler defaults from the Autumn process role (issue #2477, epic #2476)

**Behaviour change on `web` replicas.** An `AUTUMN_ROLE=web` replica no
longer runs the Harvest worker or scheduler. It still serves the management
API and starts workflows. A fleet with only `web` replicas then claims no
task. Add `worker` replicas, or set both switches to `true` on the `web`
replicas before the upgrade.

- `HarvestRuntimeConfig::worker_enabled` and `scheduler_enabled` are now
  `Option<bool>`. `None` is the default and follows the role. This is
  **source-breaking**: a struct literal with `worker_enabled: true` fails to
  compile. Write `Some(true)`, or leave the field unset. Ship it in a
  minor release.
- `HarvestRunner::start` resolves both switches from the role of its
  `AppState`. A `web` role gives `false`. A `worker` or `combined` role
  gives `true`. A runtime with no `AppState`, such as a standalone runner,
  has no role and keeps `true`.
- An explicit value wins. `AUTUMN_HARVEST__WORKER_ENABLED` and
  `AUTUMN_HARVEST__SCHEDULER_ENABLED` still set it. A value against the
  role logs one `warn!` per switch, with the switch, its value and the role.
  An old API replica with `WORKER_ENABLED=false` and no `AUTUMN_ROLE` has
  the `combined` role, so it now warns. Set `AUTUMN_ROLE=web` instead.
- Startup logs one `info!` with each value and its source: `explicit`,
  `role` or `default`.
- New API: `HarvestRuntimeConfig::process_switches`,
  `HarvestRunnerResources::process_role` and the `process_role` module
  (`ProcessSwitches`, `ProcessSwitch`, `SwitchSource`). `SwitchSource` is
  `#[non_exhaustive]`.
- The SQLite backend always runs its driver. The `web` role does not stop
  it, and Harvest logs a warning that names the fix. An explicit `false` is
  still refused. Its startup line gives the source `sqlite`.
- `HarvestRunner::start` takes each spawn decision from one `SpawnPlan`.
  A unit test checks the plan for each role.

No `WorkflowEvent` variant, no migration, no replay impact.

`formal/verus/src/process_role.rs` proves the resolve table for every
input. A `web` role with no value runs no worker and no scheduler. No role
keeps `true`. An explicit value wins. The warning fires only on an override
against a known role. Unit tests in `process_role.rs` cover each role with no value,
and an explicit override on each role with its warning. The guide is
`docs/operations/process-roles.md`. The design is in
`docs/design/DESIGN-2477.md`.
