## Plugin — worker queues from config and from the role pin (issue #2479, epic #2476)

Code declares the full set of queues with `WorkerConfig::with_queues`.
Config can now narrow the queues that the worker of one process polls.

- New config: `HarvestRuntimeConfig::worker`, a `HarvestWorkerConfig` with
  `queues`. TOML key `[harvest.worker] queues`. Variable
  `AUTUMN_HARVEST__WORKER__QUEUES`, comma-separated. An empty list means
  "not set". `[harvest.worker]` refuses an unknown key.
- An undeclared queue stops the boot. The error names it and the declared
  queues. The check runs before any I/O.
- With no explicit list, the `pin` of a named Autumn role narrows the
  worker. Autumn job queues share the pin, so Harvest ignores a name that
  it does not declare. A pin that names no declared queue changes nothing
  and logs one `warn!`. An explicit list always wins.
- The worker polls in code order. The management API keeps the declared
  list, so the default start queue does not move. A weight of a queue that
  the worker does not poll is dropped.
- Startup logs the declared queues, the polled queues and the source.
- `GET /admin/config` adds `worker.declared_queues` and
  `worker.queue_source`. `worker.queues` is now the polled list.
- `GET /admin/queue-coverage` adds `unpolled_declared_queues`: each
  declared queue with no live worker on an inspected shard. It does not
  change `uncovered`. `harvest queue coverage --strict` exits `2` when the
  list is not empty.
- `HarvestEmbedding` applies `[harvest.worker]` and the variable over the
  code value, as it does for `[harvest.startup]`.
- The SQLite backend refuses a list. Its one driver runs each queue.
- This is **source-breaking** for a `HarvestRuntimeConfig` struct literal
  with no `..Default::default()`. Add `worker: Default::default()`.
- The `autumn doctor --strict` hook waits for autumn#3545. Issue #2480
  tracks it.

**Compatibility: `autumn-web` 0.10.** `autumn-harvest-plugin` now depends
on `autumn-web` 0.10, which adds named roles (autumn#3543). An app with its
own `autumn-web` entry must bump it to `"0.10"`. A leftover `"0.9"` builds
two copies, and `HarvestPlugin` does not match the app's `AppBuilder`.
`cargo tree -i autumn-web` must show one version. The plugin follows two
API changes. `AppState::role()` now returns a `Role`, and Harvest reads
its `base()` for the switches of issue #2477. `Route` has a new `group`
field. `autumn-web` 0.10 also moves its own `p256` to 0.14. The
`RUSTSEC-2026-0387` entry in `deny.toml` now names the one path left, through
`jsonwebtoken`.

No `WorkflowEvent` variant, no migration, no replay impact.

`formal/verus/src/worker_queues.rs` proves `queue_source` and `pin_misses`
for every input. An explicit list wins. A role pin applies only with no
explicit list and only when it names a declared queue. The warning fires
only when the pin changes nothing. Proptests in `worker_queues.rs` check
that a resolved list is a non-empty subset of the declared list, in code
order. The DB test `a_worker_polls_only_the_queues_that_config_names` in
`api_scheduler_integration.rs` drains a `priority` run on a `priority` tier
and leaves a `default` run until a full tier starts.
`an_undeclared_worker_queue_fails_the_boot_with_its_name` checks the
refusal. The design is in `docs/design/DESIGN-2479.md`.
