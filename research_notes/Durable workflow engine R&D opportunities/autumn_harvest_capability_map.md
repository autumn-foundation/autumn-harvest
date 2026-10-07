# Autumn Harvest capability map (HEAD 301ea95, branch claude/relaxed-hopper-9n4q5u, v0.7.0)

Method note: all evidence comes from reading files in `/home/user/autumn-harvest` at HEAD `301ea95` (2026-10-06). Nothing was built or run. Citations are repo-relative `path:line`. The clone is shallow: `git log` holds only 50 commits (2026-10-03 to 2026-10-06), so older history comes from `docs/changelog.d/` (219 fragments) and `docs/shipped-work.md`.

## 1. Architecture and execution model

### Takeaway
Harvest is an **embedded Rust library**, not a server. It is a Temporal-style event-sourced engine with deterministic replay, and Postgres is its only required dependency and only source of truth. It scales out through hash-routed shards, one Postgres database per shard. It has an optional Redis Streams dispatch channel, a separate single-writer SQLite backend for local/edge use, opt-in partitioned history, and operator-driven cross-region DR. Since 2026-10-03, warm decisions run from **resident in-memory workflow state** (sticky routing on by default) and do not replay history.

### Cited Findings
- Positioning: "Postgres-backed durable workflow engine for Rust … Temporal-style durability semantics with a single-Postgres operational footprint." — [README.md:8-12](README.md)
- Execution model: workflows are deterministic Rust async fns. The first `ctx.execute_activity` enqueues to a Postgres task queue and the workflow suspends. Workers claim with `SELECT … FOR UPDATE SKIP LOCKED`. Results are written as history events. The workflow resumes on the same worker if cached, or by full replay on any worker. "This is the same model as Temporal and Cadence." — [README.md:1125-1137](README.md)
- Dispatch is LISTEN/NOTIFY wakeups with a poll-loop fallback. No broker, no visibility store, no separate cluster. — [docs/comparison.md:67](docs/comparison.md); [docs/comparison.md:191-206](docs/comparison.md)
- Deployment shapes: (a) `HarvestPlugin` inside an Autumn web app; (b) the bare `autumn-harvest` crate for a worker or CLI process; (c) `HarvestEmbedding` on plain Axum, with the management API and Vantage UI. — [README.md:332-362](README.md)
- No leader. Every replica runs a worker, a scheduler and all background scanners. They coordinate through row claims, advisory locks and leases. — summarized in [reports/Autumn Harvest resilience gap analysis.md:19](reports/Autumn%20Harvest%20resilience%20gap%20analysis.md). Since #1795, a lease elects one timeout scanner per shard: [docs/changelog.d/pr-1866-scanner-lease.md:1-9](docs/changelog.d/pr-1866-scanner-lease.md)
- Phase-2 design: "Coroutine stays in memory; durability comes from the event history … The executor re-invokes the workflow from the top on each replay cycle." — [docs/architecture.md:1852-1853](docs/architecture.md)
- **Deterministic suspension readiness (#1797)** replaced the 100 ms wall-clock `SUSPENSION_TIMEOUT`. A cycle now suspends when the handler is `Pending`, no wake fired during the poll, and a Harvest future is parked. The handler runs inside `tokio::task::unconstrained`. — [docs/changelog.d/issue-1797-deterministic-suspension-readiness.md:3](docs/changelog.d/issue-1797-deterministic-suspension-readiness.md); [docs/architecture.md:223-235](docs/architecture.md) (Key Design Decision 10)
- **Sticky routing on by default (#1798 step 1)**: `DEFAULT_STICKY_TIMEOUT` is 5 s, Temporal's sticky-queue fallback window. The warm cache defaults to 1000 entries of full decoded history. — [docs/changelog.d/pr-1869-sticky-routing-default.md:1-14](docs/changelog.d/pr-1869-sticky-routing-default.md); [autumn-harvest/src/builder.rs:84](autumn-harvest/src/builder.rs)
- **Resident workflow state (#1798 step 2)**: a warm cache entry keeps the suspended handler future. The next decision sends the result into the parked future without replay, "so its replay work no longer grows with history length". This applies only to a narrow shape: exactly one awaited command, no race, no park token, no held mutex, not hot-swapped. — [docs/changelog.d/pr-1892-resident-workflow-state.md:1-24](docs/changelog.d/pr-1892-resident-workflow-state.md); module [autumn-harvest/src/resident.rs](autumn-harvest/src/resident.rs)
- History storage: one append-only `harvest_events` table with adjacently tagged JSON events and `UNIQUE (workflow_exec_id, event_id)`. A `BEFORE UPDATE` trigger enforces append-only in the database (#1817). — [docs/architecture.md:134-160](docs/architecture.md); [docs/changelog.d/pr-1910-append-only-guard.md:1-9](docs/changelog.d/pr-1910-append-only-guard.md)
- Default history caps (#1804): `event_hard_cap = 50_000` and `byte_hard_cap = 50 MiB`. A run at the cap fails to the DLQ with `HistoryCapExceeded`. — [autumn-harvest/src/context.rs:238-239](autumn-harvest/src/context.rs); [docs/changelog.d/issue-1804-default-history-caps.md:1-14](docs/changelog.d/issue-1804-default-history-caps.md)
- **Sharding**: each `ExecutionId` encodes its `ShardId` in the first two bytes of the UUID, so routing is O(1) with no directory. There are no cross-shard transactions. A shard health and readiness gate guards promotion. — [README.md:1139-1177](README.md). Cross-shard child placement is opt-in (#956). Quiescent workflows can be rebalanced (#964), running ones cannot. — [docs/comparison.md:292-302](docs/comparison.md). A stalled rebalance cutover now auto-resumes (#1839). — [docs/changelog.d/issue-1839-rebalance-auto-resume.md:1-9](docs/changelog.d/issue-1839-rebalance-auto-resume.md)
- **Partitioned events (#958)**: an opt-in physical layout for `harvest_events`, so retention drops partitions instead of deleting rows. Replay is unchanged. — [docs/partitioned-events.md:1-8](docs/partitioned-events.md). Cross-cohort duplicate appends are detected, not prevented (ADR): [docs/adr/0004-partitioned-duplicate-append-detection.md](docs/adr/0004-partitioned-duplicate-append-detection.md)
- **Redis (`autumn-harvest-redis`)**: an optional Redis Streams *dispatch channel*. It carries references to claimable rows. The worker still claims the row in Postgres with the full predicate, and Postgres keeps the history write path. If Redis is lost at runtime, workers fall back to Postgres claims. A configured URL that is unreachable fails startup. v1 supports a single instance and single shard; Redis Cluster is not supported. TLS via `rediss://` (#1834). — [README.md:1196-1231](README.md)
- **SQLite (`autumn-harvest-sqlite`)**: an embedded single-writer backend with "no server to run and no Docker", built as a separate crate rather than a core storage trait. — [docs/sqlite-backend.md:1-16](docs/sqlite-backend.md). v0.1 rejects children, external signals and cancels, local and external activities, updates, search attributes, continue-as-new, sessions and cancellable timers with a typed `UnsupportedFeature` failure. — [docs/sqlite-backend.md:415-445](docs/sqlite-backend.md). An OS file lock enforces the single writer (#1834). — [docs/changelog.d/issue-1834-backend-hardening.md:1-9](docs/changelog.d/issue-1834-backend-hardening.md)
- **Cross-region DR (#954)**: shipped. Each shard replicates asynchronously to a standby region, with write-authority fencing (`harvest_shard_generation`), a measured WAL-LSN RPO, and an ordered failover runbook. "Failover is **operator-initiated**. There is no automatic promotion, no active-active writing, and no zero-RPO mode." — [docs/cross-region-dr.md:1-9](docs/cross-region-dr.md)
- **WASM**: there is no WASM "plugin crate". `autumn-harvest-plugin` is the Autumn-framework integration (management API, Vantage, MCP, outbox, connectors; ~264k LOC). WASM support lives in core behind Cargo features. `wasm-activities` (#965) runs sandboxed activities on wasmtime with fuel, epoch and memory limits and a deny-all host surface: [autumn-harvest/src/wasm_activities.rs:1-12](autumn-harvest/src/wasm_activities.rs); [autumn-harvest/src/lib.rs:559-560](autumn-harvest/src/lib.rs). `hot-code-swap` (#967) runs WASM workflows as an R&D spike: [autumn-harvest/src/lib.rs:363](autumn-harvest/src/lib.rs); [docs/rnd/hot-code-swap.md:1-9](docs/rnd/hot-code-swap.md)
- ADR 0002: "Autumn Harvest is a **Rust-native durable workflow engine**. First-class workflow and activity authoring is Rust-only." Non-Rust code integrates through external activities with task tokens, signals, webhooks and the management API. — [docs/adr/0002-rust-native-execution-boundary.md:42-69](docs/adr/0002-rust-native-execution-boundary.md)

### Inferences
- The execution model is moving from Temporal's classic replay-from-top model toward Temporal's sticky-cache model, where live state is kept in memory. The resident path covers only single-await cycles, so fan-out, race and mutex workflows still pay O(n) replay on each decision.
- Postgres plus a hash-encoded shard id is a deliberate DBOS-like "library plus Postgres" architecture. There is no log-structured or embedded-KV store like Restate's RocksDB, and no pluggable persistence trait. SQLite is a forked runtime, not a backend behind a shared trait.

### Gaps
- I could not measure how often real workflows hit the resident fast path versus fall back to replay. No benchmark after #1798 was found.
- The shard-count ceiling is not stated beyond "two bytes" of the UUID. ADR 0001 lists `harvest.shard.id` cardinality as "≤ 256" ([docs/adr/0001-otel-trace-contract.md](docs/adr/0001-otel-trace-contract.md), span attribute table), so the practical limit is unverified.

## 2. Programming model

### Takeaway
The surface is broad and close to Temporal parity: activities, local activities, external task-token activities, timers (absolute, business-day, cancellable), signals, queries, updates with validators, child workflows, race/select, windowed fan-out, sagas, durable mutexes, sessions, continue-as-new (including cross-type), patching, deterministic side-effect primitives, cron/calendar schedules, DAGs, completion triggers and callbacks, webhooks, debounce, throttle, batch starts and transactional activities and starts. Three things are missing: a Nexus-like cross-service durable RPC, Restate-style virtual objects, and first-class durable promises. Non-Rust clients get a generated TypeScript client only.

### Cited Findings
- Proc macros: `workflow`, `activity`, `dag`, `query`, `update`, `webhook`, `signal`, plus the registration macros `workflows!`, `activities!`, `dags!`, `queries!`, `updates!`, `signals!`, `webhooks!`. — `#[proc_macro*]` items in [autumn-harvest-macros/src/lib.rs](autumn-harvest-macros/src/lib.rs). Activity attributes include `start_to_close`, `heartbeat_timeout`, `retry`, `max_concurrent`, `concurrency_key` and `local = true`. — [README.md:96](README.md); [README.md:304-314](README.md); [docs/architecture.md:1284-1289](docs/architecture.md)
- `WorkflowContext` API (all in [autumn-harvest/src/context.rs](autumn-harvest/src/context.rs)):
  - Activities: `execute_activity` :8236 and `execute_activity_raw` :5903. Local activities: `execute_local_activity` :8298. External task-token activities: `execute_activity_external` :12422.
  - Timers: `timer` :6411, `sleep_until` :6576, `timer_business_days` :6724, cancellable `start_timer` :6923.
  - Signals: `wait_for_signal` :9064, `receive_signal_timeout` :9372. External workflows: `signal_external_workflow` :9426, `request_cancel_external_workflow` :9769, `await_external_workflow` :9993.
  - Child workflows: `spawn_child_workflow` :8371, `spawn_child_workflow_detached` :8171.
  - Concurrency: `ctx.race()` (#600) :1504, windowed fan-out `execute_activity_fan_out_windowed` :10880, `await_condition` :9019, durable `mutex` (#691) :7125, sessions via `create_session` (#606) :12309.
  - Continue-as-new: `continue_as_new` :12562, cross-type `continue_as_new_as` (#803) :12735, `should_continue_as_new` :4593.
  - Versioning: `version` :5512, `patched` :5652, `deprecate_patch` :5712.
  - Deterministic primitives: `side_effect` :5074, `system_now` :5304, `new_uuid` :5341, `random_u64` :5355.
  - Handlers and status: `register_query_handler` :12927, `register_update_handler` :13241, `set_current_details` :13601, `publish_progress` :13675.
- Saga: the `Saga<'ctx>` builder with LIFO compensation. — [autumn-harvest/src/saga.rs:119](autumn-harvest/src/saga.rs); [docs/architecture.md:1870](docs/architecture.md)
- Schedules: cron and interval, with jitter (#240, default 10 s cron jitter since #1792), overlap policies (Skip, BufferOne, BufferAll, CancelOther, TerminateOther), calendars with backfill (#337), catchup window, finite runs, HA-safe multi-replica ticks, run history, in-place update and last-completion carryover. — [docs/comparison.md:133](docs/comparison.md); [docs/architecture.md:1871-1873](docs/architecture.md)
- DAGs: `DagBuilder` with trigger rules, cron schedules, retry from a failed node, and declarative signal/timer gate nodes (DESIGN-746). — [autumn-harvest/src/dag.rs:796](autumn-harvest/src/dag.rs); [DESIGN-746.md:1-8](DESIGN-746.md). DAG tooling includes `dag_simulator.rs`, `dag_linter.rs`, `dag_profiler.rs`, `critical_path.rs` and a Chrome-trace exporter (`trace_export.rs`). — module headers in [autumn-harvest/src](autumn-harvest/src)
- Completion triggers start a target workflow on a source's terminal state. Same-shard starts are in-transaction. Cross-shard starts are out-of-band. A `harvest_completion_trigger_fires` ledger dedupes them. — [docs/completion-triggers.md:1-12](docs/completion-triggers.md). Completion callbacks push the terminal result to a URL (#605). A permanent 4xx dead-letters at once and `Retry-After` is honored (#1832). — [autumn-harvest/src/completion_callback.rs:1-2](autumn-harvest/src/completion_callback.rs); [docs/changelog.d/issue-1832-failure-handling-fixes.md:1-12](docs/changelog.d/issue-1832-failure-handling-fixes.md)
- Inbound webhook triggers (#344): [autumn-harvest/src/webhook_trigger.rs:1](autumn-harvest/src/webhook_trigger.rs). SignalWithStart (#244), start conflict policy (#685), request-scoped start idempotency keys (#808): [docs/architecture.md:484](docs/architecture.md); [docs/architecture.md:582](docs/architecture.md); [autumn-harvest/src/start_idempotency.rs:1](autumn-harvest/src/start_idempotency.rs). Debounce (#499), throttle (#607), batch start (#357): [docs/comparison.md:133](docs/comparison.md); [autumn-harvest/src/batch_start.rs:1](autumn-harvest/src/batch_start.rs)
- Transactional activities (`ctx.run_transactional`): a domain write and `ActivityCompleted` commit atomically. Transactional workflow start (#763) does the same for starts. — [docs/transactional-activities.md:1-10](docs/transactional-activities.md); [docs/transactional-start.md:1-8](docs/transactional-start.md)
- Latest-wins concurrency: `#[workflow(concurrency(key=…, limit=1, on_conflict="cancel_running"))]` (#811). — [docs/architecture.md:1490-1497](docs/architecture.md)
- Typed input/output JSON Schema (#373), with server-side input validation. — [docs/architecture.md:1553-1642](docs/architecture.md)
- **Nexus: absent.** "Harvest has no cross-service RPC primitive." — [docs/migrating-from-temporal.md:185-192](docs/migrating-from-temporal.md)
- Durable promises and awakeables: no module or doc mentions either. A grep for "durable promise" and "awakeable" hits only competitor rows in `docs/comparison.md`. External task tokens (`external_task.rs`) are the nearest analogue. — [docs/adr/0002-rust-native-execution-boundary.md:170-176](docs/adr/0002-rust-native-execution-boundary.md)
- Client SDKs: `clients/typescript` is a thin generated client. `src/index.ts` has 62 lines and wraps `openapi-fetch` over `docs/openapi.json`. It is attached as an `npm pack` tarball to releases. npm publication is deferred. Only 7 core lifecycle routes are fully typed. — [DESIGN-1616.md:16-30](DESIGN-1616.md); [docs/changelog.d/pr-1887-published-typescript-client.md:1-9](docs/changelog.d/pr-1887-published-typescript-client.md). Python, Go and Java clients were "Rejected for now". — [DESIGN-1616.md:28](DESIGN-1616.md)

### Inferences
- Harvest has several primitives that Temporal lacks as first-class features: durable mutex, business-day timers, transactional activities and starts, debounce and throttle, DAG gates, and completion triggers. These map to "workflow plus database co-transaction" research (DBOS-style) more than to Temporal.
- The missing primitives (durable promises, virtual objects or keyed actors, cross-namespace RPC) are the main programming-model R&D openings.

### Gaps
- I did not verify whether an update-with-start (Temporal's `UpdateWithStart`) exists. A changelog fragment `issue-1440-with-start-shared-step.md` suggests "with-start variants" exist, and [docs/changelog.d/issue-1802-fail-closed-mutations.md:6-9](docs/changelog.d/issue-1802-fail-closed-mutations.md) mentions "update (with the with-start variants)". Semantics were not read.

## 3. Correctness tooling

### Takeaway
Correctness tooling is the most distinctive area, and it grew quickly in Oct 2026. Determinism checks come in four layers: compile-time HVG guardrails, `det_check` lints, the MIR taint verifier `harvest-verify`, and replayers with drift gate and canary. Formal and search-based testing are new: TLA+ models of three protocols, Kani proofs of pure kernels, a Resonate-style deterministic simulation with an oracle store and Postgres differential checking (4M seeds nightly), loom and Shuttle on every PR, nightly fuzzing with a structure-aware replay target, a stateful lifecycle model, and process, DB and network fault injection with toxiproxy. All of this is scoped narrowly so far: the activity claim protocol, workflow-task claims and codec rotation.

### Cited Findings
- Compile-fail guardrails: `autumn-harvest/tests/compile_fail/` holds 85 files covering HVG001 wall clock, HVG002 randomness and side-effect escape, HVG003 env, HVG004 sleep, HVG005 background task, HVG006 direct I/O, HVG007 process globals, HVG008 non-deterministic predicate, HVG010 select, and HVG011 HashMap iteration. — directory listing; rule catalog [autumn-harvest/src/guardrail.rs:1](autumn-harvest/src/guardrail.rs)
- `harvest det-check` (DET001–DET011) is the sub-second syntactic layer. `harvest-verify` (#962) is opt-in and second-line. It emits textual MIR via `cargo rustc --emit=mir`, resolves the call graph (impls, closures, async state machines, generics), and runs a 3-kind taint analysis (Value, Order, Control) from non-deterministic sources to command-emitting sinks. A TOML model drives it, and anything it cannot see becomes a named boundary with an `unknown` verdict. It is labelled a **prototype**. — [docs/harvest-verify.md:1-40](docs/harvest-verify.md). CI jobs `harvest-verify` and `harvest-verify-tests`: [.github/workflows/ci.yml:2150](.github/workflows/ci.yml); [.github/workflows/ci.yml:2213](.github/workflows/ci.yml). Corpus crates (seeded, clean, boundary, helpers): [Cargo.toml:11-15](Cargo.toml)
- Replay tooling: the `WorkflowReplayer` harness for CI ([autumn-harvest/src/testing.rs:1](autumn-harvest/src/testing.rs)); the in-flight replay-drift gate with stratified sampling (#798) ([autumn-harvest/src/replay_sample.rs:1](autumn-harvest/src/replay_sample.rs)); the time-travel replay debugger with run diff (#949) ([docs/replay-debugger.md:1-12](docs/replay-debugger.md)); a test generator that turns production histories into tests ([autumn-harvest/src/test_generator.rs:1](autumn-harvest/src/test_generator.rs)); and non-terminal ND-blocking that parks divergent runs (#603) ([docs/comparison.md:225-231](docs/comparison.md)). Since #1791 the production worker path also ND-blocks a cycle that leaves recorded commands unconsumed. — [docs/changelog.d/issue-1791-worker-path-drift-guard.md:1-30](docs/changelog.d/issue-1791-worker-path-drift-guard.md)
- **Formal models (#1819)**: `formal/tla/` holds three TLA+ specs (ActivityClaim.tla 222 lines, WorkflowTaskClaim.tla 195, CodecRotation.tla 107) with 10 TLC configs. "PreFix" configs must reproduce the historical bug as a named invariant violation, such as `TerminalByCurrentClaim` and `ErasureIsFinal`. — [formal/tla/models.txt:1-21](formal/tla/models.txt); [formal/tla/ActivityClaim.tla:1-20](formal/tla/ActivityClaim.tla). The CI job `formal-models` runs TLC in about 40 s. — [.github/workflows/ci.yml:2055-2068](.github/workflows/ci.yml)
- **Kani**: 5 proofs. `policy.rs` covers jitter range and retry-delay cap at :2839, :2851, :2860, :2878. `chaos.rs` covers seeded action caps at :1387. The CI `kani` job fails if Kani verifies fewer proofs than the source holds. — [autumn-harvest/src/policy.rs:2839-2879](autumn-harvest/src/policy.rs); [autumn-harvest/src/chaos.rs:1387](autumn-harvest/src/chaos.rs); [.github/workflows/ci.yml:2070-2090](.github/workflows/ci.yml)
- No Verus, P or Lean artifacts exist. `formal/` contains only `tla/`.
- **Deterministic simulation (ADR 0004-DST, #1830)**: the team chose option (b), "Resonate-style": an in-memory oracle store driven on one thread from a seed, plus a differential test that replays each operation log on Postgres through the production statements. Option (a), Antithesis-style, was rejected because it "needs a paid hypervisor service, so a failure does not replay locally". A run takes under 1 ms, so the nightly job runs **4,000,000 seeds**. The harness checks 6 invariants from `ActivityClaim.tla`. It does **not** run `worker.rs`, and it does not model the timeout sweeper, the FAILED state or quarantine. "Next scope: those three, workflow tasks, the scheduler fire claim and timers." — [docs/adr/0004-deterministic-simulation-testing.md:22-58](docs/adr/0004-deterministic-simulation-testing.md); module [autumn-harvest/src/dst/](autumn-harvest/src/dst) (`sim.rs`, `store.rs`, `invariant.rs`, `sweep.rs`); nightly [.github/workflows/dst-nightly.yml:47-135](.github/workflows/dst-nightly.yml) (jobs sweep, differential, alert)
- loom and Shuttle run on every PR (#1800). There are three Shuttle models under random and PCT schedulers, for example slot-tuner permit conservation. — [docs/changelog.d/issue-1800-concurrency-models-on-every-pr.md:1-14](docs/changelog.d/issue-1800-concurrency-models-on-every-pr.md); [.github/workflows/ci.yml:2008](.github/workflows/ci.yml); [.github/workflows/ci.yml:2030](.github/workflows/ci.yml)
- Fuzzing (#1835): 5 cargo-fuzz targets: `fuzz_replay` (structure-aware), `fuzz_det_check_source`, `fuzz_workflow_event_deser`, `fuzz_failure_signature` and `fuzz_validate_target_url`. Each runs 600 s nightly with a persisted corpus and about 40 seed histories under `fuzz/seeds/fuzz_replay/`. — [docs/changelog.d/issue-1835-nightly-fuzz-replay-target.md:1-12](docs/changelog.d/issue-1835-nightly-fuzz-replay-target.md); [fuzz/fuzz_targets/](fuzz/fuzz_targets)
- Stateful lifecycle model (#1829): random sequences of start, claim, heartbeat, park, complete, signal, cancel, worker death and revival, and orphan reclaim, run against real Postgres and a reference model. CI runs 128 cases, plus a nightly proptest run. — [docs/changelog.d/issue-1829-stateful-model-and-history-checks.md:1-14](docs/changelog.d/issue-1829-stateful-model-and-history-checks.md); [.github/workflows/proptest-nightly.yml:36-72](.github/workflows/proptest-nightly.yml)
- Chaos: the nightly suite now parses and runs, with a watchdog (#1790). — [docs/changelog.d/pr-1790-chaos-nightly-runs.md:1-9](docs/changelog.d/pr-1790-chaos-nightly-runs.md). Infra-level faults (#1801) cover `pg_terminate_backend` during COMMIT for an append, a terminal write and a claim, plus Postgres restart and toxiproxy network faults against a Postgres 16 container. — [docs/changelog.d/pr-1801-infra-crash-recovery-tests.md:1-9](docs/changelog.d/pr-1801-infra-crash-recovery-tests.md)
- Synthetic liveness canary (#796) and scanner liveness heartbeats (#797): [autumn-harvest/src/canary.rs:1](autumn-harvest/src/canary.rs); [autumn-harvest/src/scanner_health.rs:1](autumn-harvest/src/scanner_health.rs)
- The DB-suite run-coverage allowlist shrank from 64 entries to 6 (#1799). — [docs/changelog.d/issue-1799-wire-remaining-db-suites.md:1-8](docs/changelog.d/issue-1799-wire-remaining-db-suites.md); commit `d04f2d8`
- Online-migration lock-safety lint (#1810) and comment-hygiene audit: [docs/changelog.d/pr-1810-migration-lock-safety-lint.md:1-9](docs/changelog.d/pr-1810-migration-lock-safety-lint.md); [CLAUDE.md](CLAUDE.md) "Comment Hygiene"

### Inferences
- Harvest's determinism checking spans compile time, MIR analysis and runtime parking. That is more layers than any peer named in `comparison.md`. The MIR taint verifier in particular has no counterpart I know of in Temporal, Restate or DBOS, but that comparison is outside this note's evidence.
- The formal and DST work is young (all merged between 2026-10-04 and 2026-10-06) and covers only the claim protocols and codec rotation. Open R&D frontiers include: model checking replay, the scheduler, sharding and rebalance, and DR failover; running the real worker loop under simulation; refinement checks linking TLA+ to the code; and a model-based conformance check between the SQLite and Postgres backends.

### Gaps
- No code-coverage tooling was found (no llvm-cov or tarpaulin references in `.github` or `scripts`), so coverage is unmeasured.
- I did not count loom models or read their scope. The gap report said there were "four models cover two modules" before #1800.

## 4. Operations

### Takeaway
Operations surface is wide: an HTTP management API with 174 OpenAPI operations, the `harvest` CLI with a TUI debugger, the embedded Vantage UI, MCP tool exposure, a DLQ with bulk replay and discard, retention plus a pre-delete archival hook, SIEM audit export with an optional HMAC hash chain, per-tenant quotas and usage reports, cooperative tenant "cells", an AES-256-GCM codec with KMS and key rotation, PII erasure, OTel traces, 172 metric names, alert/SLO/dashboard packs, signed releases with SBOMs, and many overload controls. Published performance is modest. The v0.6.0 headline is 23.73 wf/s on one shard, and a pre-registered same-box assay found Temporal 7.91x faster. That assay predates the Oct 2026 hot-path fixes.

### Cited Findings
- Management API: 174 `operationId`s in `docs/openapi.json` (grep count). DESIGN-1616 notes "868 top-level fields on 116 operations have no type". — [DESIGN-1616.md:27](DESIGN-1616.md). The result-wait endpoint `GET /workflows/{id}/result?wait=5s` uses LISTEN/NOTIFY. — [README.md:139-149](README.md)
- CLI (`autumn-harvest-cli`, a thin HTTP client): command groups include workflow, dag, dlq, retention, concurrency, schedule, shard, queue, worker, token, usage, version-usage, reachability, replay, debug (TUI), det-check, preflight, backup verify and migrate. — [README.md:357-420](README.md); string literals in [autumn-harvest-cli/src/lib.rs](autumn-harvest-cli/src/lib.rs)
- Vantage UI is embedded and server-rendered with no CDN. — [docs/vantage-ui.md:1-5](docs/vantage-ui.md). `comparison.md` calls it partial, with a rendered DAG graph "still Phase 4". — [docs/comparison.md:277-286](docs/comparison.md)
- DLQ: list, replay, bulk-replay, bulk-discard and aggregation, with redrive spread (#1832). — [README.md:920-968](README.md); [docs/changelog.d/issue-1832-failure-handling-fixes.md:1-12](docs/changelog.d/issue-1832-failure-handling-fixes.md)
- Archival: a `HistoryArchiver` hook ships `HistoryExportDocument` to cold storage before the retention janitor deletes rows (#345). — [docs/archival.md:1-6](docs/archival.md). A terminal task-row janitor is on by default (#1811). — [docs/changelog.d/issue-1811-task-queue-hygiene.md:1-12](docs/changelog.d/issue-1811-task-queue-hygiene.md)
- Audit export to a SIEM is at-least-once with lag metrics and redrive (#953). — [docs/audit-export.md:1-10](docs/audit-export.md). An optional keyed HMAC-SHA256 hash chain over exported audit rows (#1838) adds tamper evidence. — [docs/adr/0004-security-extras.md:25-30](docs/adr/0004-security-extras.md); [docs/changelog.d/issue-1838-security-extras.md:1-12](docs/changelog.d/issue-1838-security-extras.md)
- Multi-tenancy: per-tenant quotas on executions, history and DLQ (#946) ([autumn-harvest/src/quota.rs:1](autumn-harvest/src/quota.rs)), and read-only usage aggregation by workflow or search-attribute key such as tenant (#596) ([autumn-harvest/src/usage.rs:1-8](autumn-harvest/src/usage.rs)). Cells: one reserved shard plus its worker pool. "Harvest does not support hostile multi-tenancy … Harvest adds no first-class namespaces." — [docs/adr/0004-tenant-isolation-cells.md:83-90](docs/adr/0004-tenant-isolation-cells.md)
- Codecs: the `PayloadCodec` envelope (ADR 0003) ([docs/adr/0003-payload-codec-event-boundary.md](docs/adr/0003-payload-codec-event-boundary.md)) and a production `AeadCodec` with AES-256-GCM and a KMS provider (#1825) ([docs/changelog.d/pr-1825-aead-payload-codec.md:1-9](docs/changelog.d/pr-1825-aead-payload-codec.md); plugin `aws_kms.rs`). Failure text stays in clear by decision (#1920). — [docs/changelog.d/issue-1920-aead-codec-failure-text-boundary.md:1-10](docs/changelog.d/issue-1920-aead-codec-failure-text-boundary.md). Key rotation uses a lazy CAS re-encryption sweep (#948), proven replay-identical and TLA+-modelled. — [CLAUDE.md](CLAUDE.md) "Codec key re-encryption"; [formal/tla/CodecRotation.tla](formal/tla/CodecRotation.tla). Large payloads use a claim-check offload to an embedder-supplied `PayloadStore` (#524), encrypt-then-offload, with no bundled S3 client. — [autumn-harvest/src/payload_store.rs:1-27](autumn-harvest/src/payload_store.rs)
- PII erasure (#495) tombstones payload fields on terminal executions only. — [autumn-harvest/src/erase.rs:1](autumn-harvest/src/erase.rs); [CLAUDE.md](CLAUDE.md) "PII erasure"
- Security: mutating routes fail closed outside `dev` (#1802, mapped to OWASP API2/API5) ([docs/changelog.d/issue-1802-fail-closed-mutations.md:1-12](docs/changelog.d/issue-1802-fail-closed-mutations.md)); an admin token scope and authorizer hook with tenant and shard inputs (#1803) ([docs/changelog.d/pr-1803-admin-scope-authorizer.md:1-9](docs/changelog.d/pr-1803-admin-scope-authorizer.md)); optional per-client rate limit with 429 (#1827) ([docs/changelog.d/issue-1827-api-rate-limit.md:1-12](docs/changelog.d/issue-1827-api-rate-limit.md)); Ed25519-signed WASM modules (#1838) ([docs/adr/0004-security-extras.md:104-110](docs/adr/0004-security-extras.md)); supply chain with daily `cargo deny`, SHA-pinned actions, Dependabot, `cargo auditable`, CycloneDX SBOMs, Sigstore signing and provenance attestations (#1826) ([docs/changelog.d/pr-1826-supply-chain.md:1-9](docs/changelog.d/pr-1826-supply-chain.md))
- Observability: ADR 0001 defines an OTel trace contract with 8 span kinds and `harvest.*` attributes ([docs/adr/0001-otel-trace-contract.md](docs/adr/0001-otel-trace-contract.md); [docs/comparison.md:144](docs/comparison.md)); 172 `METRIC_*` constants in `telemetry.rs` (grep count); alert starter pack, SLO burn-rate pack (#1816) and Grafana dashboard pack ([docs/alerts/](docs/alerts), [docs/dashboards/](docs/dashboards); [docs/changelog.d/pr-1907-slo-burn-rate-alerts.md:1-9](docs/changelog.d/pr-1907-slo-burn-rate-alerts.md)); timeline API (#739), stall root-cause classifier (#809), open-awaitables projection (#615) and lineage ([autumn-harvest/src/timeline.rs:1](autumn-harvest/src/timeline.rs); [autumn-harvest/src/stall_diagnosis.rs:1](autumn-harvest/src/stall_diagnosis.rs); [autumn-harvest/src/awaitables.rs:1-12](autumn-harvest/src/awaitables.rs)). **Native OTLP export is declined.** Metrics map to OTel semconv through a Collector recipe. — [docs/adr/0004-security-extras.md:156-178](docs/adr/0004-security-extras.md)
- Overload and resilience controls:
  - Admission gate, manual, 503 (#377).
  - Automatic load shedding by backlog age, 429, with hysteresis; opt-in (#1794) — [docs/changelog.d/issue-1794-load-shedding.md:1-12](docs/changelog.d/issue-1794-load-shedding.md)
  - Retry budget, on by default (#1793).
  - Adaptive concurrency limit per activity type, gradient-based (#1836) — [docs/architecture.md:292-300](docs/architecture.md)
  - Circuit breaker that defers work when open (#1809) — ADR 0005 [docs/adr/0005-activity-timeout-retry-and-open-circuit.md](docs/adr/0005-activity-timeout-retry-and-open-circuit.md)
  - Adaptive slot tuner (#548).
  - Continuation band, where a new start sorts as if due 30 s later (#1824) — [autumn-harvest/src/queue.rs:865](autumn-harvest/src/queue.rs)
  - Claim-epoch fence `(worker_id, attempt)` (#1789) — [docs/architecture.md:179-200](docs/architecture.md)
  - Postgres session timeouts and bounded pool acquire (#1788).
  - Post-commit NOTIFY (#1796) — [docs/changelog.d/issue-1796-post-commit-notify.md:1-12](docs/changelog.d/issue-1796-post-commit-notify.md)
  - Default activity start-to-close of 10 min (#1808) — [autumn-harvest/src/builder.rs:70](autumn-harvest/src/builder.rs)
  - `/health/live` and `/health/ready` with a draining state (#1812).
  - 25 s shutdown timeout with claim release (#1813) — [autumn-harvest/src/builder.rs:77](autumn-harvest/src/builder.rs)
- **Published performance numbers**:
  - v0.6.0 e2e on 4 vCPU, PG 16.13, durability off. Throughput **23.73 / 35.70 / 33.58 workflows/s** at 1, 2 and 4 shards (3-activity workflow). Dispatch p50/p99 **40.98/58.63 ms** (1 shard), rising to 58.02/111.75 ms at 4 shards. Signal round-trip p50/p99 **53.59/65.96 ms**. In-memory replay about **9.2M events/s**. — [docs/benchmarks.md:37-50](docs/benchmarks.md)
  - Claim latency against backlog (8 claimers, 4 cores). 1k backlog: p50 10.44 ms, 640 claims/s. 10k: p50 200.03 ms, 29 claims/s. 100k: p50 2,919.99 ms, **3 claims/s**, and the scenario could not finish. — [docs/performance.md:196-204](docs/performance.md)
  - Replay budget: "a 10 000-event history replays in under 200 ms" (#135). — [docs/performance.md:3-4](docs/performance.md)
  - **Harvest vs Temporal, same box, same PG** (assay 0011, pre-registered 2026-09-16): `harvest_pg` **5.47** wf/s vs `temporal_go` **43.29** wf/s, "KILL on L1, decisively and against harvest, by 7.91x". — [docs/assays/0011-harvest-vs-temporal-single-box.md:1](docs/assays/0011-harvest-vs-temporal-single-box.md); [docs/assays/0011-harvest-vs-temporal-single-box.md:116-121](docs/assays/0011-harvest-vs-temporal-single-box.md)
  - Redis assays: Redis claims 18,933/s vs Postgres 290/s at a 10k backlog (assay 0002, "pursue"). Integrated Redis dispatch reached 173.04 tasks/s against a 10,000 target (assay 0008, "kill"). — [docs/assays/0002-redis-matched-workload-vs-postgres.md:1](docs/assays/0002-redis-matched-workload-vs-postgres.md); [docs/assays/0008-redis-dispatch-integrated-throughput.md:1](docs/assays/0008-redis-dispatch-integrated-throughput.md)
  - There are 13 assays in total, each with a pre-registration in `docs/rnd/`, plus about 50 `docs/performance-*.md` component pages and 44 `docs/perf-artifacts/` directories.

### Inferences
- The 7.91x Temporal gap and the claim-latency cliff at 10k+ backlog are the clearest quantitative R&D target. Several later fixes target exactly the replay and suspension overhead and the notify-commit serialization: deterministic readiness (#1797, removing a ≥100 ms per-decision floor), resident state (#1798) and post-commit NOTIFY (#1796). No post-fix rerun of assay 0011 was found, so the current gap is unknown.
- Claim-path scaling (O(backlog) sort, "any residual predicate defeats sort-elision") is a structural limit of the single-table SKIP LOCKED queue. Postgres-queue research (partitioned queues, sort-elision-friendly indexes, Redis offload) applies directly.

### Gaps
- No DB pool-usage or wait gauge, query-latency metric or poller-count metric was found. Only `harvest.db.pool_acquire_timeout` and transaction-retry counters exist ([autumn-harvest/src/telemetry.rs:1518-1534](autumn-harvest/src/telemetry.rs)).
- I did not verify a metric-gated automatic build-ramp abort. The safe-deploy runbook describes manual "ramp up, or abort" ([docs/runbooks/safe-deploy.md:771](docs/runbooks/safe-deploy.md)).

## 5. AI and agent features

### Takeaway
AI support is a real but thin layer on top of general durability. It covers MCP tool exposure of workflows, an ephemeral progress and token-streaming side channel, claim-check offload for large payloads, and a reference Claude agent daemon example on SQLite. There is no LLM-specific primitive: no token or cost accounting, no model-call activity type, no prompt caching, no semantic memoization, no agent-specific replay semantics.

### Cited Findings
- MCP tools (#597): `#[workflow(mcp)]` and `#[update(..., mcp)]` generate a correlated set of start, watch and steer tools, served over autumn-web's Streamable-HTTP JSON-RPC MCP layer. Workflows with debounce or batch policies are excluded because a deferred start has no `execution_id`. — [docs/mcp-tools.md:1-60](docs/mcp-tools.md); plugin [autumn-harvest-plugin/src/mcp_tools.rs](autumn-harvest-plugin/src/mcp_tools.rs)
- Streaming: `ctx.publish_progress` (#791) is an "ephemeral, best-effort live-output side channel … an AI agent streaming tokens", fire-and-forget, and does not touch the event log. — [docs/streaming-progress.md:1-8](docs/streaming-progress.md); [autumn-harvest/src/context.rs:13675](autumn-harvest/src/context.rs)
- Large payloads: claim-check offload names "document processing, media pipelines, RAG ingestion, report generation" as motivating workloads. — [autumn-harvest/src/payload_store.rs:1-14](autumn-harvest/src/payload_store.rs)
- `examples/claude-agent-daemon`: each model call and tool call is an activity, and a workspace write parks on an approval signal with a deadline. A killed daemon resumes by replay "instead of paying for completed turns twice". Activities are at-least-once, so tool bodies are idempotent. It runs offline with a stub model. — [README.md:76-80](README.md); [examples/claude-agent-daemon/README.md:1-40](examples/claude-agent-daemon/README.md)
- Token accounting: the only `input_tokens` reference is a test fixture in the example ([examples/claude-agent-daemon/src/tests.rs:6682](examples/claude-agent-daemon/src/tests.rs)). The usage report counts workflow starts and activity executions, not LLM tokens. — [autumn-harvest/src/usage.rs:11-20](autumn-harvest/src/usage.rs)

### Inferences
- Agent workloads fit the existing primitives well: human-in-the-loop signals with deadlines, updates, sessions, mutexes, payload offload and streaming. The R&D opportunity is in agent-specific features: token and cost budgets as first-class quotas, durable streaming with resume, deterministic LLM-call memoization and caching, replay-safe tool-call idempotency keys, and MCP-native durable task semantics.

### Gaps
- No roadmap item or open-issue citation for further agent features was found in docs.

## 6. Roadmap signals and stated non-goals

### Takeaway
Activity from 2026-10-03 to 2026-10-06 is dominated by epic #1786, which closes the September resilience gap analysis. The work covers safe defaults, fencing, formal and DST testing, supply chain, and tenant cells. Each item is a docs/changelog.d fragment with a "No migration / no new WorkflowEvent variant" discipline. The non-goals are stable and explicit: Rust-only authoring, no managed cloud, no hostile multi-tenancy or namespaces, no native OTLP, no Temporal history import, no automatic regional failover, and no WASM workflow hosting beyond the spike.

### Cited Findings
- The last 50 commits (2026-10-03..06) are almost all #1786-epic items: #1787–#1839 plus fixes #1876, #1879 and #1917. Examples: "Deterministic simulation of the activity claim protocol (#1830)", "TLA+ models of core protocols and Kani proofs (#1819)", "keep suspended workflows resident (#1798, step 2)", "Tenant isolation with cells (#1837)", "Security extras … (#1838)". — `git log --oneline -50` (commits `301ea95` … `660d681`)
- The gap-closing epic is #1786. — [docs/changelog.d/issue-1832-failure-handling-fixes.md:3](docs/changelog.d/issue-1832-failure-handling-fixes.md); [docs/adr/0004-security-extras.md:5-6](docs/adr/0004-security-extras.md)
- Next DST scope: timeout sweeper, FAILED state, quarantine, workflow tasks, scheduler fire claim and timers. — [docs/adr/0004-deterministic-simulation-testing.md:55-58](docs/adr/0004-deterministic-simulation-testing.md)
- Non-Rust adoption (epic #1605, #1616): TypeScript client published. A `harvest-server` prebuilt binary that runs WASM workflows is "Rejected for now". The recommended path is a documented thin binary around `HarvestEmbedding`, with a `cargo generate` template "recommended as the next step". — [DESIGN-1616.md:115-141](DESIGN-1616.md)
- WASM roadmap tiers from the hot-code-swap spike: T1 WASM activities "Go"; T2 sequential WASM workflows "Conditional go"; T3 full context "No-go for now"; T4 dylib "No-go, permanently". — [docs/rnd/hot-code-swap.md:1006-1031](docs/rnd/hot-code-swap.md)
- Polyglot non-goal: no official Python, Node, Go, Java or C# worker SDKs, no gRPC worker protocol, no non-Rust workflow definitions. A deferred alternative is trusted remote activity workers over gRPC, only behind a new ADR. — [docs/adr/0002-rust-native-execution-boundary.md:101-133](docs/adr/0002-rust-native-execution-boundary.md)
- **Conflicting signal**: `comparison.md` still lists "**Planned:** a TypeScript activity-worker SDK (#959)". — [docs/comparison.md:89](docs/comparison.md); [docs/comparison.md:255-261](docs/comparison.md). This contradicts ADR 0002 and DESIGN-1616.
- Managed cloud: "**None.** … by design … none is currently planned". — [docs/comparison.md:166](docs/comparison.md)
- Tenancy: no hostile multi-tenancy, no namespaces. — [docs/adr/0004-tenant-isolation-cells.md:83-90](docs/adr/0004-tenant-isolation-cells.md)
- Temporal migration non-goals: no history import ("None is planned"), no codemods. — [docs/migrating-from-temporal.md:40-62](docs/migrating-from-temporal.md). "No equivalent yet": Nexus, multi-region global namespaces, non-Rust SDKs. — [docs/migrating-from-temporal.md:180-204](docs/migrating-from-temporal.md)
- Gaps the project names itself: Rust-only, operator-driven DR, no managed cloud, Postgres-only with no pluggable persistence, incomplete UI, a young ecosystem, limited cross-shard composition, no cross-engine benchmark. — [docs/comparison.md:250-314](docs/comparison.md)
- Plans directory: the latest dated plans are `2026-10-02-post-commit-notify`, `2026-10-04-aead-payload-codec`, `2026-10-06-residual-windows` (implemented) and `2026-10-06-supply-chain`. — [docs/plans/](docs/plans); [docs/plans/2026-10-06-residual-windows.md:1-12](docs/plans/2026-10-06-residual-windows.md)
- Process signal: performance work follows a pre-registered "assay" method (hypothesis, kill line, apparatus, verdict) with negative results published. — [docs/assays/0011-harvest-vs-temporal-single-box.md:3-8](docs/assays/0011-harvest-vs-temporal-single-box.md)

### Inferences
- The team's revealed priorities are, in order: correctness and assurance (formal, DST, fencing), safe defaults and operability, then performance. The ecosystem and polyglot axis is deliberately de-prioritized.
- Doc drift exists and the report writer should watch for it. [docs/migrating-from-temporal.md:191-197](docs/migrating-from-temporal.md) says Harvest "does not replicate workflow state across geographic regions", but cross-region DR shipped (#954). comparison.md:89 lists a TS worker SDK as planned against ADR 0002. ci.yml:2253 still has a manual `fuzz-smoke` job that the #1835 fragment says is gone.

### Gaps
- Open GitHub issues were not queried (no connector use, by constraint), so the backlog beyond doc-cited issue numbers is unknown.
- `docs/shipped-work.md` (9,097 lines) has a single `#` heading and no `##` structure, so I skimmed it rather than mapping it phase by phase.

## 7. Prior gap analysis: what it found and what has since been fixed

### Takeaway
The September 2026 report (at 0.6.0, HEAD `937b655`) found three gap classes: overload and metastability defaults, ownership fencing, and an assurance gap (chaos never ran, no DST or formal model). It credited Harvest as matching or beating Temporal on determinism tooling, ND parking, DR fencing, outbox and restore verification. Nearly every P0–P3 item it listed has a merged fix at HEAD. The report writer should treat that report as historical and not repeat its gaps as current.

### Cited Findings
- The report's thesis: "the protective toolbox is opt-in, and the amplifiers are on by default". — [reports/Autumn Harvest resilience gap analysis.md:48](reports/Autumn%20Harvest%20resilience%20gap%20analysis.md). Roadmap table: [reports/Autumn Harvest resilience gap analysis.md:86-112](reports/Autumn%20Harvest%20resilience%20gap%20analysis.md)
- Status of its items at HEAD (fix evidence in changelog fragments):

| Gap-report item | Status at HEAD | Evidence |
|---|---|---|
| P0 over-claim past local permits | Fixed #1787 | `docs/changelog.d/pr-1787-poll-capacity-gate.md:1-9` |
| P0 unbounded pool and no PG session timeouts | Fixed #1788 | `pr-1788-postgres-timeouts.md:1-9` |
| P0 unfenced activity complete, fail and heartbeat | Fixed #1789 (claim epoch); attempt checks #1806, #1917 | `pr-1789-activity-claim-epoch-fence.md`; commits `4db7e7a`, `aff18f3` |
| P0 chaos suite never ran | Fixed #1790, plus watchdog | `pr-1790-chaos-nightly-runs.md` |
| P0 worker path lacks drift guard | Fixed #1791 | `issue-1791-worker-path-drift-guard.md` |
| P1 retry jitter off | Fixed #1792 (Full jitter default) | `issue-1792-default-retry-and-schedule-jitter.md` |
| P1 no retry budget or auto shedding | Fixed #1793 (on by default), #1794 (opt-in) | `pr-1793-retry-budget.md`; `issue-1794-load-shedding.md` |
| P1 every replica scans every shard | Fixed #1795 (lease, jitter, LIMIT) | `pr-1866-scanner-lease.md` |
| P1 `pg_notify` inside commits | Fixed #1796 | `issue-1796-post-commit-notify.md` |
| P1 100 ms wall-clock suspension | Fixed #1797 | `issue-1797-deterministic-suspension-readiness.md` |
| P1 O(n²) replay, sticky off | Fixed #1798 steps 1–2 (sticky default plus resident state, narrow shape) | `pr-1869-…`, `pr-1892-…` |
| P1 ~60 unwired DB suites, loom never run | Fixed #1799 (allowlist 64 → 6), #1800 (loom and Shuttle per PR) | `issue-1799-*`, `issue-1800-*` |
| P1 no process, DB or network crash tests | Fixed #1801 | `pr-1801-infra-crash-recovery-tests.md` |
| P1 open mutating routes, coarse authZ | Fixed #1802, #1803 | `issue-1802-…`, `pr-1803-…` |
| P2 no history caps | Fixed #1804 (50k events, 50 MiB) | `issue-1804-default-history-caps.md` |
| P2 build pinning fails open | Fixed #1805 | commit `0f841e9` |
| P2 #1184 guard omits attempt; heartbeat host clock | Fixed #1806, #1807 | `issue-1807-db-clock-heartbeat-stamps.md` |
| P2 no default activity timeout; timeouts terminal; breaker | Fixed #1808 (10 min default), #1809 (retry per policy, breaker defers) | `issue-1808-…`; ADR 0005 |
| P2 migration lock safety; queue hygiene | Fixed #1810 (lint), #1811 (janitor) | `pr-1810-…`, `issue-1811-…` |
| P2 readiness, drain, rollback | Fixed #1812, #1813; metric-gated ramp abort not verified | `issue-1812-…`, `issue-1813-…` |
| P2 no DB-pool, query or poller metrics; no SLO pack | SLO pack fixed #1816; pool-usage, query-latency and poller metrics **still absent** (only acquire-timeout and tx-retry counters) | `pr-1907-…`; `telemetry.rs:1518-1534` |
| P2 append-only by grep; no coverage; no formal model | Trigger fixed #1817; TLA+ and Kani #1819; **coverage still absent** | `pr-1910-…`; `pr-1819-…` |
| P2 no AEAD codec; unscheduled advisory scan | Fixed #1825, #1826 | `pr-1825-…`, `pr-1826-…` |
| P3 engine DST | Fixed (scoped) #1830, claim protocol only | ADR 0004-DST |
| P3 hygiene and doc drift | Partly fixed #1831; drift remains (see §6) | `pr-1831-repo-hygiene-doc-drift.md` |
| Backlog: SQLite single-writer, Redis TLS, callback 4xx, DLQ redrive pacing, panics | Fixed #1834, #1832, #1821 | `issue-1834-…`, `issue-1832-…`, `issue-1821-…` |
| Deadlock and serialization retries (`40P01`/`40001`) | Fixed #1822 | `pr-1822-tx-conflict-retry.md` |
| Continuations before new starts; run-deadline skip | Fixed #1824 | `issue-1824-…` |
| Tenant isolation; security extras (P3 additions) | #1837 cells (cooperative only), #1838 audit chain, WASM signing, semconv | ADR 0004-tenant, ADR 0004-security-extras |

(All paths in the Evidence column are under `docs/changelog.d/` unless they name another file.)

### Inferences
- The remaining "classic resilience" gaps are narrow: DB saturation metrics, coverage measurement, automatic metric-gated rollback, DST scope beyond the claim protocol, and verification of the new resident path. The defaults-first stance the report asked for has largely been adopted (sticky, jitter, retry budget, timeouts, caps and fail-closed all on by default). Load shedding and the adaptive concurrency limit stay opt-in.

### Gaps
- None of the fixes have post-fix load tests confirming that the metastable loops are gone. The gap report asked for "a database-backed load test to confirm their thresholds" ([reports/Autumn Harvest resilience gap analysis.md:120](reports/Autumn%20Harvest%20resilience%20gap%20analysis.md)), and none was found.

## 8. Quick metrics

### Takeaway
This is a very large single-team Rust codebase: about 1.06M lines of `.rs` across 7 workspace crates. Tests make up roughly half of the core crate. There are 505 test files, about 15.4k test attributes, a 20-job CI pipeline and 7 scheduled or auxiliary workflows.

### Cited Findings
- Crates and LOC (`find … -name '*.rs' | xargs cat | wc -l`, including tests and examples inside each crate dir):

| Crate | .rs files | LOC |
|---|---|---|
| `autumn-harvest` (core) | 536 | 705,875 (`src/` alone: 357,452) |
| `autumn-harvest-plugin` (Autumn integration, API, UI, MCP) | 193 | 263,776 |
| `autumn-harvest-cli` | 19 | 30,069 |
| `autumn-harvest-verify` (MIR verifier plus corpora) | 106 | 25,180 |
| `autumn-harvest-sqlite` | 37 | 18,091 |
| `autumn-harvest-redis` | 13 | 8,050 |
| `autumn-harvest-macros` | 11 | 7,316 |
| `fuzz` (excluded from workspace) | 5 | 119 |

- The largest core modules are `worker.rs` (50,520 lines), `context.rs` (29,972), `replay.rs` (13,991), `queue.rs` (13,112), `execution.rs` (10,610) and `scheduler.rs` (9,586) (`wc -l autumn-harvest/src/*.rs`).
- Workspace: version 0.7.0, edition 2024, MSRV 1.88, MIT OR Apache-2.0. There are 6 example crates, including `claude-agent-daemon` and `saga-choreography`. — [Cargo.toml:1-30](Cargo.toml)
- Tests: 505 `.rs` files under `*/tests/`, 251 of them in `autumn-harvest/tests/integration/`. There are 10,001 `#[test]`/`#[tokio::test]` attributes in core and 15,364 across the workspace, 14 files with `proptest!`, and 85 compile-fail fixture files (grep and find counts). 125 migration directories are under `autumn-harvest/migrations`, plus plugin migrations.
- CI: `ci.yml` has 20 jobs: changes, lint, test, test-nodb, test-db-linux, quickstart, standalone-chapter, mixed-version-smoke, openapi-client-smoke, typescript-client-package, scaffold-smoke, msrv, loom, shuttle, formal-models, kani, dependency-audit, harvest-verify, harvest-verify-tests and fuzz-smoke (manual). Other workflows: advisory-scan (daily), chaos plus chaos-watchdog (nightly), dst-nightly (sweep, differential, alert), fuzz-nightly, proptest-nightly, release (binaries, sign, SBOM, client), claude, claude-code-review. — [.github/workflows/ci.yml:40-2253](.github/workflows/ci.yml); [.github/workflows/](.github/workflows)
- Docs: 8 ADR files (three share the number 0004), 5 root `DESIGN-*.md`, 13 assays, 24 runbooks, 219 changelog fragments, about 50 `performance-*.md` pages. — directory listings of [docs/](docs)

### Inferences
- The test-to-source ratio (core `src/` is 357k of 706k lines) and the amount of process documentation show a heavily agent-assisted, specification-first workflow. That is consistent with the `claude.yml` and `claude-code-review.yml` workflows and the CLAUDE.md rules. This is an inference, not a stated fact.

### Gaps
- LOC counts include comments, tests and examples inside each crate directory. No `tokei` or `cloc` split was run.

## 9. Synthesis: has / partial / absent matrix (for matching against R&D directions)

### Takeaway
Harvest is strongest on determinism and assurance and on Postgres-native operations. It is weakest on polyglot reach, raw throughput, automatic multi-region HA, and new programming abstractions (virtual objects, durable promises, cross-service RPC, AI-native primitives).

### Cited Findings
| Area | Has (shipped) | Partial / opt-in / spike | Absent (explicit or not found) |
|---|---|---|---|
| Execution model | Event-sourced replay; sticky by default; resident state (narrow shape); deterministic suspension (§1) | Resident path only for single-await cycles (`pr-1892`) | Keyed actors or virtual objects; journal-per-invocation model |
| Storage | Postgres plus hash shards; append-only DB trigger; history caps (§1) | Partitioned events opt-in; SQLite v0.1 subset; Redis dispatch, single-instance (§1) | Pluggable persistence; Redis Cluster; embedded KV ([docs/comparison.md:272-276](docs/comparison.md)) |
| HA/DR | Per-shard async DR with generation fencing and measured RPO ([docs/cross-region-dr.md:1-9](docs/cross-region-dr.md)) | Rebalance of quiescent workflows only | Automatic failover; active-active; zero RPO |
| Programming model | Timers, signals, queries, updates, children, race, fan-out, saga, mutex, sessions, CAN, patching, schedules and calendars, DAGs, triggers, callbacks, webhooks, transactional activities and starts (§2) | MCP tools; progress streaming (best-effort) | Nexus-style RPC ([docs/migrating-from-temporal.md:185-192](docs/migrating-from-temporal.md)); durable promises; virtual objects |
| Polyglot | HTTP API, OpenAPI, generated TS client (§2) | WASM activities (feature-gated spike); WASM workflows (spike, "not a go") | Non-Rust worker or authoring SDKs (ADR 0002 non-goal) |
| Determinism tooling | HVG001–011, DET001–011, replayer, drift gate, canary, ND-block, debugger (§3) | `harvest-verify` MIR taint "prototype" | — |
| Formal / testing | TLA+ (3 specs), Kani (5 proofs), DST 4M seeds/night, loom and Shuttle per PR, nightly fuzz, lifecycle model, infra faults (§3) | DST and TLA+ cover claim protocols and codec rotation only | Model checking of replay, scheduler and DR; coverage measurement; Verus, P or Lean |
| Ops / observability | 174-op API, CLI, Vantage, 172 metrics, OTel traces, SLO pack, DLQ, archival, audit chain (§4) | Vantage UI partial; Collector-based semconv | Native OTLP (declined); DB pool and query metrics |
| Security / tenancy | AES-GCM plus KMS, rotation, erasure, fail-closed authZ, rate limit, signed WASM, SBOM and Sigstore (§4) | Cooperative cells | Hostile multi-tenancy; namespaces ([docs/adr/0004-tenant-isolation-cells.md:83-90](docs/adr/0004-tenant-isolation-cells.md)) |
| Performance | Reproducible e2e suite and assays (§4) | Redis offload | Parity with Temporal on one box (7.91x behind, pre-#1796/#1797/#1798) |
| AI / agents | MCP tools, streaming, payload offload, agent daemon example (§5) | — | Token or cost accounting; LLM-call memoization; agent-specific primitives |
| Managed service | — | — | Managed cloud (by design, [docs/comparison.md:166](docs/comparison.md)) |

### Inferences
- The most defensible R&D directions that fit the project's own stance are: extending DST and TLA+ coverage to replay, scheduler and DR; making the resident and incremental execution path general; a scalable Postgres queue design that removes the backlog-dependent claim cliff; deterministic WASM workflow hosting (T2); and agent-native durability primitives. Polyglot SDKs and a managed cloud go against stated ADRs.

### Gaps
- Current performance after the Oct 2026 hot-path changes is unmeasured in the repo, so any throughput comparison must be labelled pre-fix.
