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
orphaned_workflows` was already parsed and validated on the standalone path (it
lives on `HarvestRuntimeConfig`, which `HarvestRunner::start` already takes), so
an operator could set `fail` on a standalone deployment and get nothing at all
for it.

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
- `gate_no_global_install.rs` additionally pins that the multi-shard selector
  installs no `GLOBAL_SHARDED_POOL`, in its own test binary so global state is
  deterministic.
