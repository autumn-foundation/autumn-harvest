## Phase 6.2 — The boot-time orphaned-workflow gate now covers the standalone runner (issue #1128)

PR #1109 (issue #700 AC4) added a boot-time orphaned-workflow-type reachability
fail-fast to the **plugin** boot path and explicitly logged the gap it left:
"the standalone `HarvestRunner::start` embedder path has no orphan gate at all
… A separate issue should add an equivalent pre-worker check inside the runner."
This closes that gap — and closes it by making the two paths run *the same
code*, not an equivalent copy.

**What was wrong.** A standalone (non-plugin) deployment could boot with
orphaned non-terminal executions — a workflow type with in-flight runs but no
registered `#[workflow]` handler — entirely unflagged. Those runs cannot replay;
they wedge and surface days later as timeouts or DLQ entries. `[harvest.startup]
orphaned_workflows` already lived on `HarvestRuntimeConfig` — the very struct
`HarvestRunner::start` takes — and was already parsed and validated by
`HarvestRuntimeConfig::load()`, so an operator could set `fail` on a standalone
deployment and get nothing at all for it.

**What shipped.**

- `runner::run_startup_orphan_gate(action, &built, &resources)` — one
  implementation of the gate, called by **both** boot paths. The registered-set
  union (workflow names ∪ unified-DAG names), the pool precedence, the decision
  table and the operator-facing log/error text can no longer drift between the
  plugin and the runner, because there is only one of each. The plugin's inline
  gate block is replaced by a call to it.
- `HarvestRunner::start` runs it as its **first act**, before
  `PreparedHarvestRuntime::build`. That is stronger than "before workers spawn":
  an aborted standalone boot has installed no global shard router, no
  `GLOBAL_SHARDED_POOL`, no completion-callback config, and has synced no
  completion triggers — it leaves the process and the database exactly as it
  found them. The #700 P1 guarantee (no worker can claim and terminally fail one
  of the very runs the gate protects) holds by construction.
- **Cross-shard, because the standalone path is the multi-shard path.** The
  plugin's gate reads shard 0 only, which is sound *there* because the plugin
  rejects multi-shard configs outright. The runner is exactly the path that
  supports them (#522), so the new `build_reachability_report_for_shards` fans
  out across every shard of the resolved pool. A shard-0-only gate would have
  reported a `complete`, clean fleet while shards 1..N hosted orphans. It reuses
  the existing `observe_shard` + `build_report_from_observations` helpers, so it
  cannot drift from the `GET /admin/workflow-types/reachability` route;
  `build_reachability_report_single_shard` is now a thin delegation to it.
- **Shard enumeration mirrors the started-runtime fan-out.**
  `select_runtime_gate_shards` resolves the shard→pool map through the same
  `pick_runtime_pool_source` the runner installs from (issue #700 P2: the gate
  reads the databases the workers will actually poll), installs no process
  global (issue #700 P4), and includes shards the *router* names but this
  process has no pool for — mapped to `None`, so they are reported
  `unavailable` rather than silently dropped. Omitting them is the one way a
  gate can claim a `complete` inspection it never performed.
- `HarvestRunnerResources::with_startup_orphan_gate_already_run()` — the
  plugin marks its resources with this, because it runs the gate *earlier* than
  the runner can: before it publishes any process-global admission state, so an
  abort has nothing at all to unwind. The marker keeps that path from paying for
  a second identical scan and logging a second identical warning. It is public,
  so an embedder driving a custom boot sequence can do the same.

- **The shipped standalone example can now actually be configured.** The plugin
  loads `[harvest.startup] orphaned_workflows` from configuration;
  `HarvestRunner::start` uses whatever `HarvestRuntimeConfig` it is handed, and
  `examples/standalone-runner` built its config in code — so `fail` in a TOML
  file was inert and the action silently stayed `warn`. The example now threads
  the loaded `startup` section through (falling back to the default rather than
  failing, in the same spirit as the gate's own crash-loop rule), which is the
  wiring a standalone embedder needs to copy. The runbook says so explicitly.

**Behaviour notes.** `off` still skips the check entirely and is still the only
zero-cost setting; `warn` (the default) never blocks a boot, so this is not a
breaking change for any existing standalone deployment. Crash-loop safety is
unchanged and now covers the runner too: a DB read failure surfaces as an
`unavailable` shard, and `startup_orphan_decision` degrades an incomplete report
to `Warn`, never `Abort`. A deliberately handler-free control-plane process that
shares the Harvest database will see every in-flight type as orphaned — that is
the knob doing what it says, and such a process should set
`orphaned_workflows = "off"`; the runbook now says so. Because the gate is the
first act of `start`, a deployment that is *both* orphaned and misconfigured
(classic DAGs, a router/pool mismatch) now reports the orphan error rather than
the config error; boot is refused either way.

**No new `WorkflowEvent` variant, no migration, no new route, no
`harvest_events` footprint.** The gate is a read-only `GROUP BY workflow_name`
over non-terminal executions plus a pool *selection*.

- **Every shard's gate query is bounded** (`STARTUP_GATE_SHARD_TIMEOUT`, 10s).
  Harvest configures no deadpool `Timeouts`, so a bare `pool.get().await` is an
  *unbounded* wait — tolerable for the management route, which serves a request
  with its own deadline, but not for a check that is the first act of `start`
  with nothing yet in existence to time it out. A shard that is
  reachable-but-silent (a dropped security-group rule, a primary mid-failover, a
  wedged pooler) would otherwise park the boot forever: no error, no crash loop,
  just a process that never becomes ready — a worse outcome than the one the
  crash-loop rule exists to prevent, and made worse by the fan-out, where one
  parked shard strands the whole gate. An elapsed shard is reported
  `unavailable` like any other unreadable shard, so it flows into the existing
  incomplete-report rule and degrades to `Warn`.
- **The two `Warn` outcomes no longer share a log line.**
  `startup_orphan_decision` returns `Warn` both for "orphans found" and for "the
  report was incomplete", and the plugin's single message asserted the former in
  both cases. On the plugin's single shard the incomplete case was
  near-unreachable; on a multi-shard standalone fleet one flaky shard makes it
  routine, so every boot would have logged "orphaned workflow types detected"
  with an empty list — either paging an operator who alerts on that string, or
  training them to filter away the real detection. The incomplete case now has
  its own message naming the uninspected shard ids and stating plainly that boot
  continues.
- **Pure configuration validation moved ahead of the gate.** The classic-DAG
  rejection lived inside `PreparedHarvestRuntime::build`, which now runs after
  the gate. Two reasons to hoist it into `start` rather than leave it there:
  a classic DAG's in-flight runs read as orphans (the registered set counts only
  *unified* DAGs), so under `fail` the operator would have got an orphan refusal
  for what is really an unsupported-configuration error; and inside `build` the
  check ran *after* `install_completion_callback_config`, so rejecting a
  configuration that can never boot had already replaced a process-global. It
  now replaces nothing.

**Test evidence.**

- `runner.rs` unit tests (DB-free, `max_size`-tagged pools so nothing connects
  and no process global is read): `select_runtime_gate_shards` covers every
  shard of a multi-shard pool and maps each to *its own* pool; it honours the
  runner-override → `WorkerConfig` → `harvest_pool` precedence; a router-known
  shard with no pool is enumerated as `None` rather than dropped.
  `registered_workflow_type_names` unions workflow names with unified-DAG names
  (the DAG half is load-bearing — without it every running DAG reads as an
  orphan). The already-run marker defaults to *off*, which is the whole point of
  this issue.
- `tests/runner_orphan_gate_integration.rs` (new suite, wired into
  `.github/ci/integration-suites.txt`) drives the **real** `HarvestRunner::start`
  against a live Postgres rather than the decision helper: `fail` + a seeded
  orphan refuses boot and names the type — asserted with `worker_enabled = true`,
  and the orphan's claimable task row is still `PENDING`/unclaimed with no
  `WorkflowFailed` appended, the deterministic analogue of "the gate ran before
  any worker could claim it"; `warn` and `off` boot with the same orphan
  present; `fail` boots a clean fleet; a **multi-shard** runner refuses boot for
  an orphan seeded on shard **1** (the case the plugin's single-shard gate could
  not see); and a caller that already ran the gate is not gated twice.
- The same suite pins the two branches whose failure modes are outages rather
  than test failures: **crash-loop safety** end to end (a real orphan plus a
  shard the router names but this process cannot inspect, under `fail`, must
  warn and continue — previously covered only by the pure decision table and a
  unit test asserting the *map shape*), and **a registered unified DAG's
  in-flight runs are `in_use`, not orphaned** (the DAG half of the registered-set
  union is load-bearing: without it a correctly configured DAG deployment
  refuses to boot under `fail`).
- The refuse-to-boot test also carries a **deterministic** ordering pin
  alongside its row assertions. Those assertions are a race detector — if the
  gate ran after the worker spawn, `start` would still return `Err` and whether
  the spawned poll loop reached the row first would be timing. The pin is not
  timing: `PreparedHarvestRuntime::build` resolves the storage pool through
  `ShardedDbPool::single`, which *writes* `GLOBAL_SHARDED_POOL`, so a gate that
  ran after `build` would leave the global carrying that test's
  distinctively-tagged pool. It snapshots and compares instead of asserting
  `None`, because a sibling test installs a global of its own and libtest
  guarantees no ordering.
- Each test gets its **own database** — a fresh container in CI, and a freshly
  provisioned, uniquely-named, migrated database against a shared
  `HARVEST_TEST_DATABASE_URL` server (the `provision_ephemeral_db` pattern
  `canary_tests.rs` already uses). The gate deliberately has no
  `?workflow_type=` filter — a boot gate that inspected only some types would
  not be a gate — so without per-test isolation one test's seeded orphan would
  decide another's verdict, and the clean-fleet test could not be written at
  all.
- `gate_no_global_install.rs` additionally pins that the multi-shard selector
  installs no `GLOBAL_SHARDED_POOL`, in its own test binary so global state is
  deterministic.
