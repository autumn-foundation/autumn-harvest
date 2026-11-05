# DESIGN-2479: worker queues from config and from the role pin

Issue #2479. Parent epic #2476. Follows `docs/design/DESIGN-2478.md`.

## 1. Problem

Code sets the queues that a Harvest worker polls, with
`WorkerConfig::with_queues`. A tier for `priority` workflows needs its own
code path or its own binary. Autumn lets an operator pin `#[job]` workers
with `jobs.pin`. Harvest does not take part.

## 2. Facts

- `HarvestRunner::start` builds the `Worker` from
  `WorkerRuntimeConfig::queues`. The API runtime reads the same list. A
  start with no queue goes to the first queue of that list.
- `WorkerRuntimeConfig::validate` refuses an empty queue list. It warns about a weight
  of a queue that it does not poll.
- Each worker writes its queues to `harvest_workers.queues`.
  `GET /admin/queue-coverage` reads that column across the fleet.
- `autumn-web` 0.10 resolves named roles (autumn#3543). `Role::pin()`
  gives the `pin` of a `[roles.<name>]` preset. A preset refuses unknown
  keys, so `harvest_queues = [...]` is not possible. The pin is a shared
  namespace with Autumn job queues.
- Autumn keeps a pinned queue that it does not know out of the schedule.
  An empty `jobs.pin` means "each queue". `AUTUMN_JOBS__PIN` is
  comma-separated and skips an empty item.
- The doctor API for plugin queues (autumn#3545) is not released. Issue
  #2480 registers Harvest queues with it.
- The SQLite backend runs one driver for each queue.

## 3. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Who owns the full set | config · **code; config only narrows it** |
| An undeclared name | ignore it · warn · **stop the boot and name it** |
| Polled order | config order · **code order** |
| An empty list | refuse it · **"not set", as for `jobs.pin`** |
| The variable | `AUTUMN_HARVEST_WORKER__QUEUES` · **`AUTUMN_HARVEST__WORKER__QUEUES`, as the issue and Autumn name it** |
| The role source | a `harvest_queues` preset key · **the shared `pin`, from `Role::pin()`** |
| A pin that names no Harvest queue | worker off · **keep each declared queue and log a `warn!`** |
| What the API uses | the subset · **the declared list, so the default queue does not move** |
| Where the rule lives | inline in the runner · **one module, `worker_queues`, with a Verus kernel** |
| Coverage | wait for autumn#3545 · **report both lists now, in `/admin/config` and `/admin/queue-coverage`** |

## 4. Reverse brainstorming

"How could this change hurt an operator?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A typo such as `priorty` makes a tier that polls nothing. | An undeclared name stops the boot. The error names it and the declared list. |
| `queue = [...]` narrows nothing in silence. | `[harvest.worker]` refuses an unknown key. |
| A narrowed web tier moves the default start queue. | The API keeps the declared list. Only the worker polls the subset. |
| A `mount` preset drops the Harvest routes, the readiness probe included. | The route group is `harvest`, not the Rust type path. The guide names it. |
| A preset turns off `job_workers` or `scheduler`, but Harvest still runs. | One `warn!` names the preset and the fix. Issue #2480 maps capabilities. |
| A new CLI calls an older server, and `--strict` passes. | A missing or `null` list fails the gate. |
| A drained shard reports every queue unpolled. | Only writable shards count. |
| A jobs-only role pin turns the Harvest worker off. | A pin that names no declared queue changes nothing. One `warn!` names the pin and the fix. |
| A role pin overrides an operator choice. | An explicit list always wins. Verus proves it. |
| No tier polls a declared queue. Its runs wait forever. | `/admin/queue-coverage` lists `unpolled_declared_queues`. `/admin/config` lists both lists and the source. |
| A dropped weight logs a false warning. | The runner drops the weight of each queue that it does not poll. |
| A SQLite app sets a list that does nothing. | The SQLite backend refuses the list. |
| A standalone runner ignores the variable. | `HarvestEmbedding` reads `[harvest.worker]` and the variable, as it reads `[harvest.startup]`. |
| A `queues` case mismatch goes unseen. | A name matches exactly. `Priority` is undeclared. |

## 5. Six thinking hats

- **White (facts).** Section 2.
- **Red (feelings).** An operator wants one variable per pod. A tier
  that boots and drains nothing feels worse than a boot that stops.
- **Black (risks).** A shared pin can name a job queue and a Harvest
  queue with the same name. Both then drain on that tier. That is the
  intent of a shared pin. The upgrade to `autumn-web` 0.10 can change
  other behaviour.
- **Yellow (benefits).** One binary serves each tier. The doctor work of
  #2480 can read the declared and the polled lists that this change
  reports.
- **Green (ideas).** Per-queue weights from config. A `harvest_queues`
  key on Autumn presets. Both are out of scope.
- **Blue (process).** Verus spec and proof first. Then red tests: the
  module, the config and the DB test. Then the minimum code, the docs and
  the fragment.

## 6. Decision

- `HarvestRuntimeConfig::worker` is a `HarvestWorkerConfig`. TOML key
  `[harvest.worker] queues`. Variable `AUTUMN_HARVEST__WORKER__QUEUES`,
  comma-separated. An empty list means "not set".
- `worker_queues::WorkerQueues::resolve(declared, explicit, pin)` picks the
  source and the subset. `queue_source` and `pin_misses` are the kernels.
  `formal/verus/src/worker_queues.rs` proves them.
- `HarvestRunner::start` resolves the queues before any I/O. The worker
  polls the subset. The API keeps the declared list.
- `HarvestRunnerResources::role_pin` reads `Role::pin()` of the app state.
- `GET /admin/config` adds `worker.declared_queues` and
  `worker.queue_source`. `worker.queues` is the polled list.
- `GET /admin/queue-coverage` adds `unpolled_declared_queues`. It checks
  writable shards only. `null` means "unknown". `--strict` fails closed.
- `HarvestPlugin::route_group` is `harvest`, so a `mount` list can name
  the Harvest routes.
- Harvest follows only the `base` of a preset. A preset that turns off a
  capability logs one `warn!`. Issue #2480 maps each capability.
- `HarvestEmbedding` applies the operator `[harvest.worker]` section.
- The SQLite backend refuses a list.
- `autumn-web` moves to 0.10.

## 7. Acceptance criteria

| AC | Evidence |
|---|---|
| `AUTUMN_HARVEST__WORKER__QUEUES=priority` polls only `priority`. An undeclared queue fails the boot with its name. | Config test `worker_queues_from_toml_profile_and_env`. DB tests `a_worker_polls_only_the_queues_that_config_names` and `an_undeclared_worker_queue_fails_the_boot_with_its_name`. Module tests in `worker_queues.rs`. |
| A role queue list applies with no explicit config. | Module test `a_named_role_pin_reaches_the_worker` resolves a `[roles.priority]` preset with `autumn-web` 0.10. Module tests `a_role_pin_narrows_with_no_explicit_list`, `an_explicit_list_wins_over_the_role_pin` and `a_pin_of_job_queues_only_keeps_the_code_set_and_warns`. Runner test `the_resources_give_the_pin_of_their_role` (a built-in role gives no pin). Verus `theorem_the_role_applies_only_below_config`. |
| Queue coverage is visible to Autumn doctor. | Partly. `/admin/config` and `/admin/queue-coverage` report the lists, and `harvest queue coverage --strict` gates on them. The `autumn doctor` hook waits for autumn#3545 and issue #2480. |
