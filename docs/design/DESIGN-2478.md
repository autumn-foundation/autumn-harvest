# DESIGN-2478: place the retention runtime by process role

Issue #2478. Parent epic #2476. Follows `docs/design/DESIGN-2477.md`.

## 1. Problem

`HarvestRunner::start` starts `RetentionRuntime` when retention is
enabled. It ignores the role. A `web` replica then runs delete sweeps on
the request path. Row leases keep this correct, but the operator cannot
move the load off the web tier.

## 2. Facts

- Since issue #2477, `SpawnPlan` holds each spawn decision of the runner.
  A `web` role resolves the worker and the scheduler to off.
- Retention policy (`RetentionConfig`) is Rust-only.
  `HarvestBuilder::retention` sets it. The placement is a per-process deployment setting, so
  it belongs in `[harvest]`, with an environment variable.
- `GET /admin/retention` and `POST /admin/retention/run-now` read the
  local `RetentionRuntime`. A process with no runtime reported the config
  only, so "not here" and "idle" looked the same. `run-now` returned 503.
- A `worker` replica serves no app routes. So on a split fleet, the API
  replicas never run retention by default. The scheduler monitor already
  reads offline there (issue #2477).
- The dispatch-metrics sampler runs only with no `Worker`. A `Worker`
  runs its own copy. Two copies would emit each gauge twice.
- Each process registers its pools with the post-commit notify sender.
  An API-only process writes rows, so it sends notifications.
- The SQLite backend runs retention in its single driver on every role.
- A test cannot set the role of an `AppState`. `autumn-web` keeps the
  field private, and `AppState::for_test` gives `combined`. A `web` role
  resolves to `scheduler_enabled = false`, so a DB test sets that value
  directly.
- autumn#3545 (the role-aware plugin background-work API) has not landed.
  Issue #2480 registers the tasks with it.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| What the placement follows | a new `maintenance_enabled` switch · the worker switch · **the resolved scheduler switch** |
| Where the setting lives | a field on `RetentionConfig` · **`[harvest.retention] run_on` in `HarvestRuntimeConfig`** |
| The values | `bool` · **`"scheduler"` (default) and `"all"`** |
| Where the decision lives | inline in the runner · **one pure kernel, `retention_placed`, read by `SpawnPlan`, with a Verus proof** |
| What `run-now` says on an unplaced process | the old 503 text · **a 503 that names the placement and the fix** |
| How the sampler and notify sender are pinned | a comment only · **`SpawnPlan` methods that the runner reads, with a table test** |
| How the placement is proven end to end | unit test only · **a DB test that seeds an expired run and checks the sweep on each placement** |

## 4. Reverse brainstorming

"How could this change hurt an operator?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| Retention stops on a split fleet after the upgrade. | Retention follows the scheduler. A `worker` replica runs the scheduler by default, so it runs retention. The changelog states it. |
| A fleet turns the scheduler off on every process. Nothing runs retention. | Each unplaced process logs one `info!` that names `run_on` and the fix. The guide and the changelog state it. |
| `harvest retention run-now` against a `web` replica returns an opaque 503. | The 503 names the placement and `run_on = "all"`. The runbook states it. |
| `harvest retention status` on a `web` replica looks enabled and idle. | The body adds `placed_elsewhere: true`. |
| A runner with no role and the scheduler off runs retention nowhere. | It logs a `warn!`, not an `info!`. |
| A typo such as `run_on = "Al"` turns retention off. | Parsing refuses an unknown value. The boot stops. A test proves it. |
| A later refactor moves the sampler onto worker processes and doubles its gauges. | The runner reads `SpawnPlan::dispatch_metrics_sampler`. The table test pins it. |
| A later refactor drops the notify sender on API-only processes. Waiters then fall back to polling. | The runner reads `SpawnPlan::notify_sender`. The table test pins it. |
| A standalone runner loses retention. | No role keeps the scheduler on, so retention stays on. A test proves it. |
| The placement table drifts from its contract. | Verus proves `retention_placed` and `runs_retention`, and their join with the role default. The mirror guard keeps each copy equal to its source. |
| Existing tests that run retention with both switches off go red. | They set `run_on = "all"`. That also proves that `"all"` keeps the old behaviour. |

## 5. Six thinking hats

- **White (facts).** Section 2.
- **Red (feelings).** Operators expect a sweep to run off the request path.
  A janitor that silently stops would feel worse than one in the wrong
  place. The startup log line answers that.
- **Black (risks).** No process runs retention. `run-now` on a `web`
  replica fails. The status page on a `web` replica shows no shard
  results.
- **Yellow (benefits).** One `AUTUMN_ROLE` per pod keeps delete load off
  the web tier. The table test documents every runtime in one place.
- **Green (ideas).** A DB request flag that lets `run-now` reach a remote
  janitor. This change does not need it. Issue #2480 can expose run-now through the
  Autumn task API. `run_on = "worker"` is not needed: the scheduler
  switch already moves retention.
- **Blue (process).** Verus spec and proof first. Then red tests: the
  table, the config parse and the DB placement test. Then the minimum
  code, the docs and the fragment.

## 6. Decision

- `HarvestRuntimeConfig::retention.run_on` is a `RetentionRunOn`:
  `scheduler` (the default) or `all`. TOML key
  `[harvest.retention] run_on`. Environment variable
  `AUTUMN_HARVEST_RETENTION__RUN_ON`. An unknown value stops the boot.
- `process_role::retention_placed(run_on, scheduler)` is the kernel.
  `scheduler` places retention where the resolved scheduler runs. `all`
  places it on each process.
- `SpawnPlan::retention` is `Off`, `Here` or `Elsewhere`.
  `HarvestRunner::start` reads it. It logs one `info!` when an enabled
  retention is not placed, or a `warn!` when the runner has no role.
- `SpawnPlan::dispatch_metrics_sampler` is `!worker`.
  `SpawnPlan::notify_sender` is always `true`. The runner reads both.
- On an unplaced process, `GET /admin/retention` adds
  `placed_elsewhere: true`. `run-now` returns a 503 that names
  `run_on = "all"`.
- The SQLite driver keeps retention on every role. It ignores `run_on`.

## 7. Acceptance criteria

| AC | Evidence |
|---|---|
| On a `web` role with retention enabled, no retention sweep runs. On `worker`, it does. | `runner.rs` test `each_role_places_each_runtime`. DB test `retention_follows_the_scheduler_placement` in `api_scheduler_integration.rs`. Verus `theorem_a_web_role_runs_no_retention_by_default` and `theorem_other_roles_run_retention_by_default`. |
| `retention.run_on = "all"` restores today's behaviour. | The same DB test with `all` on a `web` placement. The existing retention DB tests with both switches off. Verus `theorem_run_on_all_runs_retention_everywhere`. Config tests for TOML and the environment variable. |
| A table test pins which runtime runs on which role. | `runner.rs` test `each_role_places_each_runtime`. |
