# Changelog

All notable changes to autumn-harvest will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-10-28

### Upgraded

- **Upgrade to autumn-web 0.9.** Harvest 0.8.0 depends on `autumn-web` 0.9, so an app that mounts `HarvestPlugin` bumps `autumn-web` in the same change: Cargo treats 0.8 and 0.9 as distinct crates, and a leftover `autumn-web = "0.8"` compiles a second copy whose `Plugin` trait `HarvestPlugin` does not implement. Harvest's generated webhook, MCP tool and MCP task routes set 0.9's new `Route::criticality` to `Criticality::Default`, which is what a route attribute without `criticality` produces. **`HarvestPlugin` now declares `[harvest]` as its own config root** through `AppBuilder::config_section` (public as `autumn_harvest_plugin::config::HARVEST_CONFIG_ROOT`): 0.9's `prod` profile turns on `server.strict_config`, which otherwise refuses the undeclared `[harvest]` table and stops the boot. The plugin contract declares `autumn-web = "0.9"`. autumn-web 0.9 adds framework migrations, none sharing a version with a Harvest migration. The `harvest new` scaffold now writes `autumn-harvest = "0.8"`, `autumn-harvest-plugin = "0.8"` and `autumn-web = "0.9"`. MSRV stays 1.88.0. See [`docs/upgrading/0.8.0.md`](docs/upgrading/0.8.0.md) §2.

### Changed

- **Breaking and behavior changes.** Decision boundaries are recorded opt-in (issue #1833); the default claim reads a bounded window and sets its own planner settings (issue #1971, migration `20261008042107_harvest_task_queue_claim_seek_index`); `queue::complete_task` takes a `&TaskClaim` (issue #1992); no reset forks a PII-erased run (issue #1999); a worker runs up to two claims at once by default (`max_concurrent_claims = 2`); the schema contract covers activities and side effects, `contract_version` 2 (issue #1994); the usage report carries the agent cost ledger (issue #1996); fairness keys are opt-in (issue #1976); column encoding covers activity rows and short-lived copies (issues #1979 and #2043); a keyed entity drops the input of a caller start (issue #2082); a dropped `Session` releases its slot (issue #2193); activity results and workflow-side values nested past 100 levels fail (issues #2225 and #2224); and a calendar tests the schedule's local date (issue #2332). Each has its own section and check in [`docs/upgrading/0.8.0.md`](docs/upgrading/0.8.0.md) §1.

### Added

- **Keyed entity: serialized handlers over durable state for each key (issue #1975).** `autumn_harvest::entity` adds a keyed entity, like a Restate virtual object or an Azure durable entity. [ADR 0006](docs/adr/0006-keyed-entity.md) records the design and the priority call: P1 for this library layer, rank 13 for a native primitive.

- **A durable agent loop: `autumn-harvest-agent` (issue #1973).** A new optional crate, `autumn-harvest-agent`. The agent loop is the workflow `agent_loop`. Each model call is the activity `agent_model_turn`, and each tool call is the activity `agent_tool_call`.

- **The AEAD codec covers execution, signal and DLQ columns (issue #1979).** The codec used to encrypt the `harvest_events` payload fields only. Execution input, output and memo, signal payloads and DLQ inputs stayed in clear.

- **Worker container image and Helm chart (issue #1989).** ADR 0006 (`docs/adr/0006-worker-container-image.md`) states what the image holds: the `harvest` CLI, `harvest-replay`, the reference `standalone-runner` and the plugin migrations. It holds no user workflow. A user worker builds on the image.

- **Mid-run signals, updates and queries in `WorkflowTestEnv` (issue #1991).** `WorkflowTestEnv` could queue signals only before the run started. A test could not send a signal after the workflow reached a given point. It could not run an update or a query against the running workflow.

- **Always-on agents: heartbeats, follow-ups, delivery, memory, loop guard (issue #1973).** `autumn-harvest-agent` gains the always-on primitives, modelled on `autumn-plugin-agent` with no dependency on it. `docs/agent-adapter.md` section 10 shows them end to end.

- **Assay #11 rerun on 0.7.0 at every depth, and 0.7.0 benchmarks (issue #1972).** Assay ledger #14 reruns assay #11 against 0.7.0. The pre-registration (`docs/rnd/2026-10-08-harvest-vs-temporal-0.7.0-depth-sweep-preregistration.md`) was committed and pushed before any run.

- **`queue::complete_task` requires a claim (issue #1992).** **Breaking API change.** `queue::complete_task` now takes a `&queue::TaskClaim` in place of a task id:

- **HashiCorp Vault Transit KMS binding (issue #1981).** AWS KMS was the only KMS binding for the AEAD codec. This adds HashiCorp Vault Transit, which runs on any cloud and on premises.

- **Fan-out failure tolerance and result writer (issue #1986).** Issue #1986 adds two options to the activity fan-out helpers. The new `fan_out` module holds them.

- **Every reset fork refuses a PII-erased source (issue #1999).** Batch reset could fork a PII-erased `FAILED`, `CANCELLED` or `TIMED_OUT` run. The fork resumed on `{"_harvest_erased": true}` tombstones. The refusal was an opt-in flag, and only DAG retry set it.

- **Replay-as-evaluation for agent runs (issue #2001).** `autumn_harvest_agent::eval::evaluate` re-drives a recorded `agent_loop` run with a candidate model or prompt. It reports where the decisions of the two runs diverge. It is behind the new `eval` feature of `autumn-harvest-agent`.

- **MCP Tasks for `#[workflow(mcp)]` workflows (issue #2005).** `HarvestPlugin::mcp_tasks()` serves each `#[workflow(mcp)]` workflow as a task of the `io.modelcontextprotocol/tasks` extension (MCP 2026-07-28). A client gets a task at once and polls `tasks/get` for the result.

- **Pre-deploy upgrade verdict for each in-flight run (issue #1995).** The new upgrade check gives each in-flight run one verdict against a candidate build: `migrate`, `review` or `pin`. It runs inside the candidate build, because only that build holds its workflow types and codec keys.

- **A worker runs more than one claim at once (PR #2106, assay #14 re-charter item 1).** A worker now runs up to `max_concurrent_claims` claims at once. The default is 2. Before, one serial loop ran one claim at a time. Set `WorkerConfig::with_max_concurrent_claims(1)` to keep the old loop.

- **Resident state for parallel activity awaits (issue #2008).** **Measurement first (issue #2007, counter part).** A new counter, `harvest.workflow.resident{workflow, queue, outcome}`, counts each decision attempt on a worker with resident state on. `outcome` is `hit` when the decision resumed a resident workflow.

- **OR filters and live Vantage views (issue #1982).**

- **Durable, resumable workflow output streams (issue #1974).** `ctx.publish_progress` is best-effort. A chunk is not stored. A slow client loses chunks. A client that disconnects cannot resume. LLM token streaming needs a stream that a client can resume at any offset.

- **SQLite: `continue_as_new` and the default history caps (issue #2023).** **`continue_as_new` works on SQLite.** Before, it failed the run with `UnsupportedFeature`. Now one transaction appends `WorkflowContinuedAsNew`, seals the run `CONTINUED_AS_NEW`, and starts a successor with the same `workflow_name` and `workflow_id`.

- **Schemas for activity payloads and side-effect values (issue #1994).** Activities publish input and output schemas. Side effects publish a value schema. `harvest schema check` gates them with the issue #794 ruleset.

- **Export aged partitions before the drop (issue #2009).** On the partitioned layout, the sweep dropped an aged `harvest_events` partition, and its history was gone. A `PartitionArchiver` now keeps that history in object storage.

- **Durable outbound MCP/A2A task calls (issue #2006).** `WorkflowContext::call_remote_task` lets a workflow call a long-running task on a remote MCP or A2A server. The workflow suspends until the task ends. The wait holds no worker slot and no open connection, and a worker restart loses nothing.

- **A cross-run, content-addressed LLM response cache (issue #1998).** `autumn-harvest-agent` gets an opt-in `ResponseCache`. Install it with `AgentHarness::response_cache`. A model call that matches an earlier call is served from the cache, in any later run.

- **Verify idle pool connections only (Ledger, pool recycle pings).** `pool::engine_manager(dsn)` builds the connection manager with `RecyclingMethod::Fast`. `pool::with_engine_timeouts` adds a `pre_recycle` hook that pings a connection only when its last checkout is `pool::VERIFY_IDLE_AFTER` (1 second) or longer ago.

- **SQLite ingests every staged signal (issue #2020).** The SQLite backend no longer drops signals read with `drain_signals`, `try_receive_signal` or a push handler. Each decision cycle now appends every staged signal to history before replay, in arrival order.

- **More formal verification: Verus proofs, more Kani proofs, a fourth Shuttle model.** **Verus.** A new `verus` CI job runs `scripts/check-verus-proofs.sh`. The script pins the weekly Verus release `0.2026.10.04.426d8b0` by SHA-256 and checks `formal/verus/src/lib.rs`. The proofs take about 2 seconds.

- **Agent cost ledger (issue #1996).** An activity can now record each LLM call:

- **SQLite backend: workflow updates (issue #2025).** `SqliteRuntime::update(exec, name, args)` runs a workflow update and returns its result. Before this change, a `RecordUpdateResult` command sealed the run `FAILED` as `UnsupportedFeature`.

- **SQLite: cancel and terminate a run, with cooperative cancellation (issue #2021).** `SqliteRuntime` had no way to stop a live run. The only path was a `TerminateIfRunning` restart of the same workflow id. Embedded apps, such as the agent daemon, could not stop a stuck run.

- **S3 and GCS adapters, archived-history read-back (issue #1983).**

- **SQLite runs the `#[activity]` body with an `ActivityContext` (issue #2024).** **Breaking for `autumn-harvest-sqlite`.** `SqliteRuntime::register_activity` now takes only `&ActivityInfo`. It awaits the `#[activity]` body on the drive's async runtime, with a real `ActivityContext`. One activity fn now runs unchanged on SQLite and on Postgres.

- **LLM token and cost budgets per run and per tenant (issue #1997).** A declared budget now stops the LLM spend of an agent loop. `QuotaPolicy` takes four new caps and one window: `max_run_llm_tokens`, `max_run_llm_cost_usd_micros`, `max_tenant_llm_tokens`, `max_tenant_llm_cost_usd_micros` and `tenant_llm_window_secs`.

- **SQLite: workflow queries (issue #2022).** A workflow on SQLite registered queries with `ctx.register_query(..)`, but nothing could call them. `SqliteRuntime::query(exec, name, args)` now does.

- **Small primitives: matching wait, durable promise, `AllowAll` (issue #1985).** Issue #1985 asked for a decision on four primitives that peer engines ship. Three shipped. This change declines the counting semaphore and gives the reason in `docs/comparison.md`.

- **Weighted fairness keys within a queue (issue #1976).** A task can carry a fairness key. A worker with `WorkerConfig::with_fairness_keys(true)` serves the keys of each queue in weighted round robin. One tenant's flood can no longer hold another tenant's tasks in a shared queue.

- **Agent mesh primitives: delegation, mandates, budgets and flow rules (PR #2148).** `autumn-harvest-agent` gains agent mesh primitives. A run can delegate a task to a peer agent. The peer runs as a sub-agent inside the same workflow, on Postgres and on SQLite.

- **SQLite: keyed signals, signal by workflow id, signal-with-start (issue #2028).** Before this change, `send_signal` on SQLite takes only an `ExecutionId`. A retried delivery stages the signal twice. No call starts a run or signals it in one step.

- **Codec column follow-ups (issue #2054).** Three follow-ups from the review of issue #1979.

- **Agent payload-budget follow-ups (issue #2040).** `autumn-harvest-agent` now keeps a run inside the engine payload caps in four more cases. Items 4 and 6 of the issue shipped earlier, in the always-on PR.

- **Sandboxed code activities on Firecracker microVMs.** The new `sandbox-activities` feature lets an activity run code that an agent writes at run time. The code can be Python, Node, shell or any program in the guest image.

- **SQLite: cancellable timers, workflow mutex, local activities, execution timeout and workflow retry (issue #2029).** Five primitives sealed a SQLite run `FAILED` with `UnsupportedFeature`, or panicked at registration. Code that used any of them could not move from Postgres. Each one now works on SQLite and records the Postgres events, so its history replays on the core `WorkflowReplayer`.

- **SQLite: list and describe executions, search attributes and memo (issue #2027).** The SQLite runtime read one execution at a time by id. It had no list, no filter and no describe. `upsert_search_attrs` sealed the run `FAILED`.

- **The AEAD codec covers the short-lived payload copies (issue #2043).** Issue #1979 left the short-lived payload copies in clear. A finished activity task row stays until task retention removes it, and a callback dead letter stays until a redrive removes it.

- **Audit chain verification reuses the keyed HMAC state.** `ChainVerifier` sets each key up once and streams the canonical row into the MAC. Verify-only cost over 5,000 rows drops from 176.8M to 139.2M instructions (-21.3%) and from 44,999 to 10,000 allocations (-77.8%).

- **Shared optional-duration conversion for typed-stub macros (🪞 Echo clone-class merge).** `workflow.rs`'s `start_with_options` (four fields: `execution_timeout`, `max_execution_timeout_ceiling`, `chain_execution_timeout`, `max_workflow_chain_timeout_ceiling`), its `signal_with_start` (the first two of those four), and `update.rs`'s `update_with_start` (the same first two) each hard-coded an identical match converting an `Option<std::time::Duration>` field into an `Option<chrono::Duration>`, rejecting overflow with a `HarvestError::Config` naming the field — eight hand-copied occurrences across two files.

- **Republish tasks shifted by the late resume pass (issue #2066).** `resume_queue` shifts held tasks in two passes. The late pass shifts tasks that the first pass cannot see, such as a task created during the pause.

- **SQLite: schedules, retention and the append-only guard (issue #2031).** The SQLite backend had no schedules and no retention. Its `harvest_events` table had no append-only guard.

- **SQLite: child workflows, and signal, cancel and await between runs (issue #2026).** Before this change, the SQLite backend rejected composition. A workflow that started a child, awaited another run, or signalled or cancelled another run ended `FAILED` as an unsupported feature.

- **Replay-stable `should_continue_as_new()` (issue #2080).** `should_continue_as_new()` compared the loaded history size with the threshold. The loaded size grows from task to task, so a cold replay of a loop could answer `true` where the live run answered `false`.

- **Autoresearch capstone example (PR #2190).** `examples/autoresearch` runs Karpathy's autoresearch loop as durable Harvest workflows on the SQLite backend. An agent edits `train.py`, a trainer runs it, and a pure keep rule keeps the commit or resets it.

- **Late stream chunks survive a terminated staged copy (issue #2128).** Activation now copies the source's durable stream chunks whenever the target is not erased. Before, an operator terminate of the staged copy skipped the copy, and the forwarded durable read ended without the late chunks.

- **Monotonic durable stream inserts (issue #2083).** `append_stream_chunks` admits only offsets above the highest stored real offset. Before, a re-run could store an offset below a reader's cursor, and no reader received it.

- **Continue-as-new provenance in `ctx.info()` (issue #2082).** `ctx.info()` now reports `start_source` and `continued_from`. They come from the execution row columns `start_source` and `continued_from_exec_id`. The row never changes after the insert, so the values are replay-stable.

- **Fairness state bound, preview locks and key spelling (issue #2097).** Four follow-ups to the fairness keys of issue #1976.

- **Orphaned payload blob sweep (issue #2123).** None by default. The sweep is opt-in.

- **Cross-backend conformance suite (issue #2030).** A new test-only crate, `autumn-harvest-conformance`, runs one corpus of workflows on Postgres 16 and on SQLite. Each scenario must give the same executions, the same histories after the named comparison rules, the same outcome and the same update results.

- **The AEAD codec covers the last payload copies (issue #2043).** The first change for issue #2043 left four groups in clear. This change encodes them. No payload column stays clear now.

- **Non-destructive fork of any run (issue #2000).** **New route.** `POST /workflows/{id}/fork` (admin-only). It copies a history prefix of a source run to a new execution with a new workflow id. The source is never written, so a run in any state can be forked, `COMPLETED` and `TERMINATED` included.

- **Timeouts on the orphan sweep's store calls (issue #2123).** None unless the orphan sweep is on and its store hangs.

- **Stage a NOTIFY note without a probe round trip (issue #2125).** A staged NOTIFY note needs the transaction id and the database of its write. The stage used to read them in its own `SELECT txid_current(), current_database(), pg_postmaster_start_time()`.

- **Retention keeps the source of a recorded fork (issue #2122).** A recorded fork reads its source history while it runs. Retention did not know about forks. It could delete a terminal source under a running recorded fork.

- **Timeout lanes share one refill page (issue #2126).** Before this change, the four task-timeout lanes read the same page of live rows with four statements. Lanes at one sweep clock and one cursor now share one statement, and each lane tests its predicate once.

- **Release a Session when its owner drops it (issue #2193).** A `Session` now releases its host slot when its owner drops it. This covers a cancelled `Future`, a race loser and a `select!` branch. Before, the slot stayed `ACTIVE` until the 24-hour lease ended.

- **Cancellation scopes (issue #1984).**

- **A drop release waits for live members of its session (issue #2193).** A dropped `Session` releases its host slot at the next suspension. Its owner can be dropped while a member activity of the session is still queued or running.

- **Harvest-verify proves the autoresearch capstone, and three soundness fixes.** `harvest-verify` now proves all four workflows of the autoresearch capstone: `experiment`, `research_session`, and the agent crate's `agent_loop` and `agent_heartbeat`. CI runs that gate with `--strict`, so an `unknown` fails the build as well as a finding.

- **Assay #15: the Postgres 12 floor at the schema layer.** Assay ledger #15 applies all 139 migrations to fresh PG 12.22, PG 14.24 and PG 16.15 databases, three runs each. All 834 PG 12 and PG 14 applications succeed (417 per version), and the schema signature matches PG 16.

- **Durable routes: an Autumn route that runs as a keyed workflow (PR #2223).** The new `durable_route` module of `autumn-harvest-plugin` adds the `DurableRoute` extractor. A route hands its work to a registered workflow, keyed by the request's `Idempotency-Key`. A retry with the same key attaches to the same run and gets its stored result, so side effects do not run again.

- **Share the DR-fence prelude of `harvest partition` (issue #2221).** `harvest partition enable`, `maintain` and `disable` repeated one prelude: connect, check write authority, take the DR fence (issue #1823). The new `prepare_partition_shard` holds it once.

- **Durable streaming: a durable route that answers with a resumable SSE stream (PR #2246).** `DurableCall::stream()` in `autumn-harvest-plugin` joins durable routes (PR #2223) and durable streams (issue #1974). A route starts or attaches to its keyed run, then streams each chunk that the run stores with `publish_durable_progress`.

- **ADR 0009: decision partitions and the fenced owner lease (issue #2249).** [ADR 0009](docs/adr/0009-decision-owner.md) records the stage 1 owner model of epic #2247. The ADR changes no engine code. Issues #2250 to #2254 build it.

- **Shared result-to-observation mapping for cross-shard fan-out reads (🪞 Echo clone-class merge).** `usage.rs`, `workflow_count.rs`, `version_usage.rs`, `version_gate_retirement.rs` and `workflow_reachability.rs` each hand-copied the identical tail of their `observe_shard` function: run the shard's own query, then `match result { Ok(rows) => ShardObservation { shard_id, rows, error: None }, Err(e) => ShardObservation { shard_id, rows: Vec::new(), error: Some(e.to_string()) } }`.

- **LargePayloadRule counts payload bytes without allocating (Bolt).** `LargePayloadRule` now measures a payload with a counting writer instead of `Value::to_string()`. The byte count and every warning stay identical. On a 6,000-event history, instructions in `HistoryAnalyzer::analyze` fall 77.5% and allocated bytes fall 97.6%.

- **The decision owner lease and the commit fence (issue #2251).** Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the protocol. Owner mode stays off until issue #2252 adds its switch and its claim, so no production path holds a tenure yet.

- **Foreign writers wake the partition owner and evict its cache (issue #2253).** Stage 1a of epic #2247. Decision 12 of [ADR 0009](docs/adr/0009-decision-owner.md) records the design and the inventory of foreign writers. No migration. No `WorkflowEvent` variant. No new append path, so the append-only invariant keeps its two exceptions.

- **Owned workflow-task claims and the owner-mode switch (issue #2252).** Stage 1a of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) fixes the protocol. Owner mode replaces the sticky lease for workflow tasks. Activity claims do not change. Owner mode is off until an operator turns it on.

- **PAUSED-run index for the claim gate (issue #2212).** Migration `20261018053859` adds `idx_harvest_we_paused ON harvest_workflow_executions (id) WHERE state = 'PAUSED'`. The claim's PAUSED-run gate read one primary-key page chain per candidate. With more than 32 paused-run tasks at a queue head it read every PAUSED row.

- **SLA-breach scan uses its partial index (Ledger).** `timeout::enforce_workflow_sla_breaches` repeats the tail of the predicate of `idx_harvest_executions_sla_deadline`, `(completed_at IS NULL OR completed_at > sla_deadline_at)`, in its `WHERE`. The new clause adds no condition.

- **`is_redacted_export` probes one key without a `Value` tree (perf).** `debugger::is_redacted_export` reads `payload_policy` with a streaming map visitor instead of parsing the whole history into a `serde_json::Value`. `harvest debug diff` and `ReplayDebugger::trace_json` call it before they parse the same text again.

- **Owner failover tests: frozen owner, kill -9 and scale-out (issue #2254).** Stage 1a of epic #2247. No migration. No `WorkflowEvent` variant. No new append path, so the append-only invariant keeps its two exceptions.

- **Baseline of the claim split, the persist phases and the fence cost (issue #2248).** Stage 1 of epic #2247. The case for a decision owner extrapolated from the formula `1 / (claims × claim latency)`. This change measures the three inputs that the formula did not have.

- **Owner-local disk cache, encrypted and disposable (issue #2258).** Stage 2 of epic #2247. A decision owner can keep the history of each live run in a local SQLite file. A memory miss then reads the file and the Postgres delta, not the full Postgres history.

- **Keyed-entity state as history events with a Postgres projection (issue #2259).** Stage 3a of epic #2247. [ADR 0006](docs/adr/0006-keyed-entity.md) records the amendment. The design record is [`docs/design/DESIGN-2259.md`](docs/design/DESIGN-2259.md).

- **Stage 1b: workflow tasks leave the contended claim path (issue #2255).** Stage 1b of epic #2247. [ADR 0009](docs/adr/0009-decision-owner.md) decision 13 records the design. Owner mode stays off until an operator turns it on. Legacy mode does not change.

- **Durable agents and agent meshes (epic #2262).** The agent loop (#1973), the keyed entity (#1975) and the agent mesh join into one durable `Agent`. `docs/design/DESIGN-2262.md` is the spec. Each sub-issue below has its own red tests.

- **Durable stream append skips the stored-row count (issue #2083).** `store::append_stream_chunks` counted every stored chunk of a run on each append. It now reads the lowest and highest real offset in one statement and skips the count while that span plus the new chunks fits the cap.

- **Failed agent children keep their reports (issue #2320).** A failed child of a fan-out no longer drops the reports of its later siblings. `fan_out` merges every successful report, then fails the batch with the first error in input order.

- **Owner-local SQLite projection for SQL over entity state (issue #2260).** Stage 3b of epic #2247. A decision owner can keep the last state snapshot of each live entity in its disk cache file. User code then runs read-only SQL over the state of one entity with `Worker::query_entity_sql`.

- **Owner decider takes workflow tasks in claim order (issue #2308).** The owner decider of issue #2255 took workflow tasks in wake order and by `scheduled_at`. Under saturation, an older low-priority backlog could take every workflow permit before a newer high-priority task.

- **A failed fan-out child reports its usage (issue #2328).** A failed `agent_loop` run returned only an error string. A parent lost the usage and the flow labels of that run, and of its whole subtree.

- **Group commit across executions in one decision partition (issue #2256).** `group_commit::GroupCommit` lets an owner commit the decisions of several executions of one decision partition in one Postgres transaction. [`DESIGN-2256.md`](docs/design/DESIGN-2256.md) section 1 is the design note.

- **Queue age gauge seeks the oldest rows (Ledger, issue #2339).** `queue::oldest_pending_ages` no longer reads every eligible pending row. For each queue it reads the oldest 32 rows through `idx_harvest_tq_coverage_sample`, runs the claim gates over them, and keeps the result when it is provably exact.

- **Assay #16: the Postgres 12 floor at the engine layer.** Assay ledger #16 tried to run the engine's database suites on PG 12.22, PG 14.24 and PG 16.15. The verdict is undetermined: the PG 16 control passed 79.0% of in-scope tests against a registered 90% validity line, so no arm was graded.

- **Deterministic profiles for the 0.8.0 fork, owner and agent paths.** No `*_profile.rs` harness measured the new 0.8.0 paths of the fenced decision owner, the non-destructive fork or the durable agent. Nine harnesses now do. Each runs under `valgrind --tool=callgrind`, needs no database, asserts its outcome and has a page with its baseline numbers and reproduce steps.

- **Queue depth gauge counts up to a cap (Ledger, issue #2356).** The `harvest.queue.depth` sampler no longer counts every eligible pending row. `queue::bounded_queue_depths` counts each queue up to `telemetry::QUEUE_DEPTH_CAP` (10,000). Statistics in `pg_class` pick one of two ways to count.

- **Runtime DAG schedule overrides (issue #2385, epic #2384).** A DAG schedule came only from code. The scheduler converged each `harvest_schedules` row to the code value on every tick, so an operator could change a schedule only by a restart or a deploy.

- **Runtime overrides for code-declared workflow schedules (issue #2386, epic #2384).** `PATCH /admin/schedules/{id}` edited a schedule row in place (issue #771). For a schedule declared with `WorkerBuilder::workflow_schedule`, the next scheduler tick converged the row back to its code value.

- **Runtime SLA and execution-timeout overrides per workflow type (issue #2387, epic #2384).** `WorkflowInfo::sla`, `WorkflowInfo::execution_timeout` and their `DagInfo` counterparts were static, so a change needed a restart.

- **See and change runtime overrides from the CLI and Vantage (issue #2388, epic #2384).** The overrides of #2385, #2386 and #2387 were visible only one route at a time. An operator could not tell which parameters were overridden.

- **Schedule overrides follow their DAG to a new shard (issue #2389, epic #2384).** A DAG schedule override lives on the shard that owns the DAG. A shard routing change moved the schedule row to its new owner, but the override stayed behind.

- **SQLite: runtime overrides of schedules (issue #2390, epic #2384).** `SqliteRuntime` gains `set_schedule_override`, `clear_schedule_override` and `schedule_override`. An override wins over the code value of `upsert_schedule` and survives a reopen with new code, as on Postgres (#2386). Fields SQLite cannot override return `SqliteError::Unsupported` and write nothing.

### Fixed

- **Durable agent workspace snapshots (PR #2431).** `DirWorkspaceBlobs::put` now syncs the scratch file before the rename, then the folder and its parent. Until one put has synced every ancestor of the folder, each put syncs them all, so a retry after a failed first put still makes a new root durable. Before, a crash after a committed tool call could lose the snapshot that the history names.

- **Schedule budget updates touched every schedule (issue #2423).** The `max_runs` guard of trigger-now and backfill was a raw SQL fragment with a bare `OR`, so the `id` guard covered one branch only. Reservation incremented `runs_started` on every capped schedule under its cap on the shard, and backfill release cleared `exhausted_at` on them. Check `runs_started` and `exhausted_at` on capped schedules in a shard that ran a trigger-now or a backfill.

- **A resolved race and each timer arm no longer scan whole SQLite tables (issue #2429).** On SQLite, the cancel of a losing `ctx.race()` activity matched the task by `activity_id` alone, and each timer arm read `MAX(arm_seq)` over every timer. The cancel now names the run, and `idx_harvest_timers_arm_seq` serves the arm. An existing file gets the index on its next open.

- **Run the MCP tools and durable webhook suites in CI (issue #1959).** `mcp_tools_integration` and `webhook_durable_integration` now run in CI from `linux` manifest rows. Before this change, every test in them was `#[ignore]`d, so CI never ran them.

- **A permit freed during a gated poll wakes the idle wait (issue #1787).** A poll that finds a pool full claims only the other kind. When it then goes idle, the wait also wakes on a released permit.

- **A redundant wake keeps resident activities parked (issue #2008).** A workflow task can run right after a decision that already read every result. Its delta then holds the events of the last suspension and no result.

- **The dispatch reader waits for a permit, and a task keeps its own-row hint (issue #1429).** **Behaviour change 1: the reader waits for a full pool.** A worker gives a reference straight back when the pool it needs has no free permit, so a peer with a permit can read it.

- **Wasmtime 48.0.6 and rustls 0.23.46 for new RustSec advisories.** New RustSec advisories failed the `Dependency ledger (cargo-deny)` job on every pull request against `trunk-dev`.

- **A refusal after the idle-wait snapshot still arms the capacity wake (issue #1787).** Before an idle wait, the leader claim loop reads `capacity_bound()`. When it is false, the wait does not wake on a released permit, because a release cannot unblock a claim.

- **Agent adapter: failed-edit keys, tick input cap, final-turn delivery, follow-up size (issue #2050).** This fix closes four open items of #2046.

- **The worker runs admitted declarative updates (issue #2035).** The worker now runs each pending update and records its result. Before this change the HTTP edge appended `UpdateAdmitted` and woke the workflow task, but no engine code ran the handler.

- **Cap the interval schedule period (issue #2048).** `validate_schedule` accepted any non-zero whole-second `Schedule::Interval`. The scheduler adds the period to a timestamp to find the next run. chrono panics when the sum passes year 262143.

- **Codec failures and carried columns (issue #2065).** Follow-up to issue #1979. Three defects, one fix each.

- **Tenant binding on a raw router mount (issue #2076).** `harvest_api_router` now installs the tenant binding layer. Before, a raw mount whose middleware set `VerifiedTenant` did not confine the caller. Tenant A could read, signal or cancel the run of tenant B by id.

- **Exact number literals in `filter` (issue #2141).** A number in `attrs.<key> =`, `!=` or `IN` kept only `i64`, `u64` or `f64` precision. So `12345678901234567890123` and `...124` bound the same value.

- **SQLite payload depth cap (issue #2157).** The SQLite backend refuses a start input, memo, signal payload or update input that nests deeper than `MAX_PAYLOAD_DEPTH` (100). Ingress returns the new `SqliteError::PayloadTooDeep` before it persists anything.

- **A signal staged during the final decision is no longer lost (issue #2079).** A task ingests the pending signals at its start. A signal that arrived later stayed unconsumed. If the run then completed or failed, the run sealed over the signal.

- **`upgrade_check` hides a mistyped URL (issue #2119).** The `upgrade_check` command printed a mistyped database URL, password included, to stderr. A URL without `--database-url` reached the unknown-flag error. A URL given to `--database-url-env`, `--limit` or `--format` reached the matching error too.

- **A cross-shard child gets the cancel reason of its parent (issue #2194).** A same-shard child that a parent cancels gets the reason of the parent. A cross-shard child always got `parent requested cancellation`.

- **Autoresearch compiles again, and `harvest-verify` gates it in CI.** PR #2187 added `start_provenance` to `testing::HistorySnapshot`. PR #2190 merged the autoresearch capstone in parallel, and its replay helper builds a `HistorySnapshot` without the new field.

- **Autoresearch compiles again after three parallel fixes.** PR #2187 added `start_provenance` to `testing::HistorySnapshot`, and the autoresearch replay helper did not set it. PRs #2201, #2204 and #2207 each added the missing field on their own.

- **SQLite deep activity result (issue #2216).** An activity result nested past `MAX_PAYLOAD_DEPTH` (100) is no longer stored. The SQLite worker turns it into a non-retryable `ActivityFailed` with `error_type = "PayloadTooDeep"`, the new `PAYLOAD_TOO_DEEP_ERROR_TYPE`.

- **`WorkflowCancelRequested` in the payload slot matches.** `trunk-dev` did not compile. Issue #1984 added `WorkflowCancelRequested`, and the exhaustive payload slot matches in `event.rs` did not name it. The event has no payload field, so both matches now return `None` for it.

- **Workflow-side payload depth (issue #2224).** A value that workflow code hands to the engine can no longer nest past `MAX_PAYLOAD_DEPTH` (100). Before this fix, such a value was stored. Then every later drive of the run failed with "recursion limit exceeded".

- **Postgres deep activity result (issue #2225).** Breaking: an activity result nested 101 to 125 levels used to complete on Postgres. It now fails with `PayloadTooDeep`, as on SQLite. A heartbeat checkpoint nested past 100 levels is now dropped.

- **DST nightly: rare sweep targets and an interrupted image pull (issue #2280).** Run 15 of the DST nightly failed in two jobs.

- **Broken-session scan seeks `harvest_task_queue` without an index (issue #2069).** `enforce_broken_sessions` runs on every timeout tick. For each candidate session it seeks member tasks by `session_id` twice: a probe for a RUNNING member, and a load of the PENDING and RUNNING members.

- **Ctutils 0.4.5 and tokio 1.53.3 for RUSTSEC-2026-0385 and -0386 (issue #2307).** The daily advisory scan failed on two new advisories.

- **Det-check rejects a blank suppression reason (issue #2314).** `harvest det-check` accepted `// harvest-suppress: DET003 " "`. The parser tested `reason.is_empty()`. It now tests `reason.trim().is_empty()`. A blank reason no longer suppresses a finding or appears in `--list-suppressions`.

- **Calendar tests the schedule's local date (issue #2332).** A calendar skip policy tested the UTC date of a fire time. A `CronInTimezone` schedule outside about UTC±5h then met the wrong day. A Mon–Fri 08:00 `Asia/Tokyo` schedule with `weekends-off` lost its Monday fire.

### Security

- **wasmtime 48.0.8 for RUSTSEC-2026-0422 to -0424 (issue #2434).** Three new advisories affect `wasmtime` 48.0.7, used by the optional `wasm-activities` feature: a type-check panic, a cross-engine type confusion, and unchecked memory use when components transfer borrowed resources. `Cargo.lock` moves to 48.0.8, and the manifest floor in `autumn-harvest` moves to 48.0.8, so a downstream consumer stays off the vulnerable release.

- **OIDC login and custom roles for Vantage and the API (issue #1978).** Issue #1978 asks where SSO lives. [ADR 0006](docs/adr/0006-oidc-sso-and-custom-roles.md) records the decision: SSO lives in Harvest as a thin opt-in layer, custom roles are built in, and mTLS stays in autumn-web.

- **Bind the tenant to the principal, per-tenant retention (issue #1977).** ADR 0004 (`docs/adr/0004-tenant-isolation-cells.md`) records the decision in a new amendment. Hostile multi-tenancy is now in scope at the management API. It stays out of scope inside the engine.

- **Authorizer hook sees the body `workflow_id` (issue #2081).** `AuthzRequest` has two new fields: `workflow_id` and `start_input_present`. Harvest reads them from the body of `start`, `signal-with-start` and `update-with-start` before the hook runs. They are `None` and `false` on every other route.

### Performance

- **Held claims no longer send the claim to the full scan (issue #2420).** With 32 or more claims in flight on one queue head, every row of the #1971 seek window could be locked, and the claim ran the full backlog scan. At 100K pending rows that read 106,467 buffers. A deep window of up to 256 rows per held head now runs first, and reads about 1,750 buffers. Claim order does not change. No migration.

- **Scaling signals, canary status and shard rebalance read an index (issue #2424).** `queue::queue_task_counts` now reads live rows only, and `GET /admin/canary`, the rebalance quiescence check and the timer copy use new indexes. Migration `20261030015439_harvest_census_scan_indexes` builds three indexes with a plain `CREATE INDEX`. On a large deployment, build them `CONCURRENTLY` first, as the header of its `up.sql` shows.

- **Standalone durable activity: measured, decided (issue #1987).** [ADR 0007](docs/adr/0007-standalone-activity.md) records it: document the one-step-workflow pattern, and do not build a standalone-activity start path now. The activities guide shows the pattern and its local-activity fast path.

- **`decode_event` moves payload trees instead of rebuilding them.** `decode_event` reads a stored event out of a `serde_json::Value`. Serde rebuilds a `Value` field node by node, even from a `Value`. So every payload tree was torn down and rebuilt on every history read and replay load.

- **`decode_event` moves payload trees instead of rebuilding them (issue #2136).** `PayloadCodecs::decode_event` runs on every history read and replay load. Serde decoded each payload `Value` by building a copy of it, one `BTreeMap` node and one `Vec` at a time, then freed the original.

- **Intern metrics-rs label values and seed circuit-breaker state (PR #2202).** **Metrics labels.** `MetricsRsRecorder` copied every per-task label value into a new `String` on every sample. Those values are the activity, workflow, queue, build id and error type.

- **Count activity occurrences without hashing (issue #2000).** `fork::scheduled_occurrence` found the occurrence number of a scheduled activity with a `HashMap` that hashed every `ActivityScheduled` name before the match. It now finds the activity by id first, then counts earlier events with the same name.

- **Batched owner acquisition (issue #2296).** `OwnerLoop` now takes the partitions of a scan in one transaction, `decision_owner::acquire_many`. Before, each partition was its own transaction of 6 statements. A join of 512 partitions sent 3,072 statements.

- **Fan-out replay of `match_activity` is no longer quadratic (issue #2330).** `HistoryMatcher::scan_activity_terminal` walked from each schedule to its terminal. In a fan-out of N activities, each walk stepped over the later schedules and the consumed completions, so a replay cost O(N²).

- **The external, timer and child matcher scans are no longer quadratic (issue #2338).** Three `HistoryMatcher` scans walked history one event at a time on each call, so a fan-out replay cost O(N²):

- **The external stash no longer costs O(N²) (issue #2346).** The external stash of `HistoryMatcher` had three quadratic paths:

- **Return Chrome trace events without a serde round trip (PR #2279).** `trace_export::export_chrome_trace` returned `json!(events)`. On a `Vec<Value>`, that macro serializes the finished tree into a second, deep copy. It now returns `Value::Array(events)`, which moves the vector.

- **Hash replay event positions without SipHash (PR #2303).** `HistoryMatcher::is_consumed` runs once per scanned event, and it probed three sets of event positions with the default `SipHash`. Those sets now use `EventIndexHasher`, a multiplicative hasher in `replay.rs`.

- **The signal wait is no longer quadratic (issue #2349).** `HistoryMatcher::match_signal` had two quadratic paths:

- **Perf(replay): the recorded-update lookup is no longer quadratic.** `HistoryMatcher::match_update` scanned the whole history for each update. It now builds an index of update results once the scans have walked as many events as the history holds.

- **The retention candidate scan uses its index again (issue #2395).** The retention janitor selects terminal executions in six states, `TERMINATED` included. Its partial index, `idx_harvest_we_retention_scan`, listed only five. Postgres uses a partial index only when the query predicate implies the index predicate, so each retention tick read the whole of `harvest_workflow_executions`, on every shard.

### Documentation

- **0.8.0 end-to-end benchmarks.** [`docs/benchmarks/results-v0.8.0.md`](docs/benchmarks/results-v0.8.0.md) publishes the 0.8.0 sweep, with a replay spread of 5.6%. It was measured on a new host. A same-box control runs the 0.7.0 suite there. On that host, 0.8.0 completes 57% more workflows at one shard, and its 1-shard dispatch p50 is 6.89 ms against 12.60. The control was noisier than the 10% bar, so that comparison is rough. `PUBLISHED_BASELINES` holds the new numbers.

- **License texts, security policy and contribution guide (issue #1990).** The repository root holds `LICENSE-MIT`, `LICENSE-APACHE`, `SECURITY.md` and `CONTRIBUTING.md`. `LICENSE-APACHE` is the canonical Apache-2.0 text. `SECURITY.md` sends reports to GitHub private vulnerability reporting. It names the fail-closed gate and the scoped token checks as in scope.

- **Why Harvest keeps deterministic replay (issue #1993).** Docs-only. New page `docs/why-deterministic-replay.md` answers engines that advertise "no deterministic replay" as a feature. It compares replay with checkpoint-only steps and process snapshots by what each engine checks on resume.

- **Jepsen-style safety report (issue #2004).** `docs/safety-report.md` states four guarantees: leases, fencing, exactly-once completion and signal ordering. For each one, it gives the claim, the tests, the results and the known limits.

- **Stop the safety and SQLite reports from conflicting every PR.** Two docs guards made feature PRs edit the same lines. `sqlite_feasibility_docs` pinned live counts in the prose of `docs/rnd/sqlite-feasibility.md`: the core module total, the coupled total, the (a)/(b)/(c) totals, the per-mechanism counts and the migration count.

- **Clean-room harness for the getting-started path.** `scripts/clean-room-getting-started.sh` starts from an empty directory and pastes the Chapter 1 `Cargo.toml` and `src/main.rs` plus the Chapter 2 workflow, activity and registration blocks. It runs `cargo check` and logs each step's exit code and seconds to a TSV.

- **OpenTelemetry workflow conventions draft (issue #2015).** `docs/otel-workflow-semconv/README.md` is a draft proposal of workflow semantic conventions for OpenTelemetry. It generalizes the ADR 0001 span contract into a `workflow.*` namespace. It has one span for each core operation and a replay rule.

- **Upstream Weaver check for the OTel workflow draft (issue #2015).** `scripts/check-otel-workflow-semconv.sh` runs the upstream OpenTelemetry policy checks on the draft workflow model. It mirrors the upstream `semantic-conventions` `make check-policies` recipe at a pinned upstream commit.

- **Ready the OTel workflow draft for the upstream process (issue #2015).** The draft now follows the upstream process for new conventions. That process has two gates. An issue finds co-owners. Then a community project proposal gets a TC review and a GC vote.

- **Chapter 3 snippets guarded in CI.** Docs tooling only. `scripts/check-durable-timers-examples.sh` extracts the three flagship timer workflows of chapter 3 and compiles them with `cargo check` in CI's lint job. Baseline: 3 snippets, 0 compile errors, 8 min cold.

- **Chapter 9 snippets guarded in CI.** Docs tooling only. `scripts/check-worker-routing-examples.sh` extracts the four Rust snippets of chapter 9 (activity `requires`, label map, `with_label`, `with_queue_weights`) and compiles them in CI's lint job.

- **Depth walk profile, negative result (issue #2312).** `payload_depth::nests_past_cap` runs on every successful activity result. A callgrind profile shows it at 10.6% of a harness that only clones and checks values. The one idea that the profile suggests saves at most 18.5% of the walk.

### Testing

- **Seeded deep-backlog fixture and stats snapshot before drop (issue #1956, PR #2045).** Issue #1956 found that no committed fixture was production-shaped. The e2e bench also dropped its databases at exit, so `pg_stat_user_tables` was lost. This change adds the fixture and the harness, and re-runs Ledger against them.

- **Check chaos traces against the TLA+ specs (issue #2003).** **Trace validation.** The chaos suite now records the history of each task row. Two test-only triggers copy each committed write to `harvest_task_queue`, and each activity or terminal event, into `harvest_tla_trace`.

- **The real worker loop under deterministic simulation (issue #2002).** Issue #1830 simulated the activity claim protocol on an in-memory oracle. It did not run `worker.rs`. The new world simulation runs the real worker loop on Postgres, and a seed picks every step.

- **Resident-state hit rate (issue #2007).** #2114 added the counter `harvest.workflow.resident{workflow, queue, outcome}`. This change measures it on two workloads and records the results in `docs/rnd/2026-10-11-resident-hit-rate.md`.

- **Nightly coverage and a ratchet on the critical modules (issue #1818).** CI now measures line and branch coverage every night. A ratchet stops the coverage of seven critical modules from dropping. There is no new `WorkflowEvent` variant, no migration and no change to engine behavior.

- **Seeded interleaving simulation of workflow code (issue #2033).** `WorkflowTestEnv::simulation()` tests user workflow code against the delivery orders that production can produce. The default env lands every result of a suspension in one task, in command order.

- **A fair drain phase for the world simulation (issue #2217).** The DST nightly failed world seed 52000000302 on `Converges`. The engine was not at fault. Worker `w1.1` abandoned the first decision task of `s2`, and only the reclaimer requeues the claim of a dead worker.

- **The fenced owner lease, modelled before code (issue #2250).** Epic #2247 adds a fenced decision owner per partition. This change writes the spec first. It adds no engine code, no migration and no new `WorkflowEvent` variant.

- **Stateright model checks of the DST oracles (PR #2283).** A new test target, `stateright` (`autumn-harvest/tests/stateright/`), with the [Stateright](https://www.stateright.rs) crate as a dev-dependency. Two models visit every reachable state of the DST oracles within a bound:

- **A fair drain phase for the owner-lease simulation (issue #2294).** The DST nightly failed the owner-lease sweep in all four shards. Each shard stopped at its first failing seed, with "the run did not consume every signal within 400 steps".

- **Stage 2 gate: cold decisions and history load (issue #2257).** Stage 2 of epic #2247 is an owner-local disk cache (#2258). It ships only if cold decisions pass 20% of decisions for a week, or the cold p99 passes 50 ms.

### Internal

- **Speculative durable execution, a DST spike (issue #2011).** Issue #2011 asks whether a worker can run the next decision while the previous commit flushes. The answer is a no-go. The engine does not change.

- **Workflows as analyzable artifacts: the flow graph and saga coverage (issue #2010).** `harvest-verify` now emits each workflow's step, signal, update and compensation graph as data. A model check runs over that data alone. Write-up and verdict: `docs/rnd/workflow-graph-spike.md`.

- **Journaled, capability-gated host calls for agent code (issue #2014).** `docs/rnd/agent-code-host-call-journal.md` answers issue #2014 with a working prototype and a verdict. The prototype is the new `autumn_harvest::wasm_journal` module, behind the `wasm-activities` feature. A guest imports one function, `harvest::host_call(name, request)`.

- **Physical backout, saga or hybrid per workflow (issue #2012).** An R&D spike asks whether Harvest can pick physical backout or a saga per workflow from contention. The write-up is `docs/rnd/contention-adaptive-atomicity.md`. **Verdict: go with changes.**

- **Typed state snapshot spike and resident hit-rate measurements (issue #2013).** **The gate.** Issue #2013 is gated on the resident hit rate of #2007. The counter `harvest.workflow.resident{workflow, queue, outcome}` came with #2114. This change measures it on two workloads:

- **Split the CI `lint` job so clippy runs in parallel.** **CI-only change** (implemented). The `lint` job ran about 35 cargo invocations one after another: clippy on each crate and plugin feature set, plus eight docs-snippet compile checks.

- **Land the Dependabot bumps that needed code: Rust 1.99.1, syn 3, aes 0.9 with aes-gcm 0.11.** Four Dependabot PRs could not pass CI alone. This change lands them together with the code each one needs.

- **Ledger profile for every SQL query (issue #2364).** Every function that sends SQL to Postgres or SQLite now has a Ledger profile. The catalog lives in `docs/ledger/`, with one TOML file for each source file, so two PRs conflict on it only when they change queries in the same source file.

- **Ledger census: the access path of every per-task query (issue #2366).** A new census plans each SQL statement that the DB suites send, and maps it to its Ledger profile. A Diesel instrumentation hook in the integration test binary records each statement and its call stack when `HARVEST_LEDGER_CENSUS` names a file.

- **Ledger census batch 2: the per-tick profiles (issue #2396).** The Ledger census now plans the `per_tick` Postgres profiles, the background loops, scanners and sweepers. 172 of them link a plan page under `docs/ledger/plans/`. The suites did not reach 23.

- **Ledger census batch 3: the per-request profiles (issue #2402).** The Ledger census now plans the `per_request` Postgres profiles, the queries behind API calls and operator reads. 153 of them link a plan page under `docs/ledger/plans/`.

- **Ledger census batch 4: the setup profiles (issue #2414).** The Ledger census now plans the `setup` Postgres profiles, the queries that run at startup, in migrations and in partition layout changes. 63 of them link a plan page under `docs/ledger/plans/`.

- **Ledger census batch 5: the plugin test suites (issue #2425).** The census hook moved into a `ledger-census` cargo feature, so the plugin's test binaries can link it. 729 Postgres profiles now link a plan page, up from 552. The census found the #2423 bug.

- **Ledger census batch 6: the SQLite profiles (issue #2429).** The `ledger-census` feature registers a SQLite auto-extension, and `ledger-census.py plan-sqlite` plans the recorded statements with `EXPLAIN QUERY PLAN`. 103 of the 109 SQLite profiles now link a plan page.

The full entry for each change is in [`docs/shipped-work.md`](docs/shipped-work.md).

## [0.7.0] - 2026-10-05

### Upgraded

- **Upgrade to autumn-web 0.8.** Harvest 0.7.0 depends on `autumn-web` 0.8, so an app that mounts `HarvestPlugin` bumps `autumn-web` in the same change: Cargo treats 0.7 and 0.8 as distinct crates, and a leftover `autumn-web = "0.7"` compiles a second copy whose `Plugin` trait `HarvestPlugin` does not implement. Harvest itself needed no source change — `Route` and `AppBuilder::plugin_migrations` are unchanged — and MSRV stays 1.88.0. `HarvestPlugin` now implements 0.8's new `Plugin::contract`, declaring `autumn-web = "0.8"` (exposed as `autumn_harvest_plugin::plugin::harvest_plugin_contract()` and `AUTUMN_WEB_REQUIREMENT`, pinned to the crate's `Cargo.toml` requirement by a unit test), so `autumn plugin-check` passes its new `plugin-contract` check and `AppBuilder::plugin` rejects a mismatched framework at registration. The workspace `tokio-postgres-rustls` pin moves 0.13 → 0.14 to match autumn-web 0.8, keeping one copy in the graph; `autumn_harvest::pg_tls::Connector` is now the 0.14 `MakeRustlsConnect` (constructor unchanged). autumn-web 0.8 adds framework migrations, none sharing a version with a Harvest migration; the six pre-existing shared versions with Autumn's job-queue set are still resolved by 0.6.0's name-keyed substitution, so the upgrade moves no tracking record. The `harvest new` scaffold now writes `autumn-harvest = "0.7"`, `autumn-harvest-plugin = "0.7"` and `autumn-web = "0.8"`. See [`docs/upgrading/0.7.0.md`](docs/upgrading/0.7.0.md) §2.

### Changed

- **Breaking defaults and behavior changes.** Mutating management routes fail closed with `401` outside the `dev` profile when no auth layer is declared (issue #1802); activities with no timeout of their own stop after a 10-minute `default_activity_start_to_close` (issue #1808); a worker with an empty `build_id` no longer claims a run pinned to a build (issue #1805); the retention janitor deletes finished `harvest_task_queue` rows after 7 days (issue #1811, migration `20261003201739_harvest_task_queue_hygiene`); continuations are claimed before new starts while a claim skips a task whose run is past its deadline (issue #1824, migration `20261004162927_harvest_task_queue_new_start`); each run's history is capped by default at 50,000 events and 50 MiB (issue #1804); a `start_to_close` or `heartbeat_timeout` timeout now retries per the activity's retry policy, and an open circuit breaker defers work instead of failing it (issue #1809); and `WorkerConfig::default()` drains for 25 s, down from 30 s (issue #1813). Each has its own section, opt-out and check in [`docs/upgrading/0.7.0.md`](docs/upgrading/0.7.0.md) §1.

- **Corrected the assay #10 and #11 numbers after three further review
  findings (follow-up to #1617).** #1617 merged with its apparatus already
  fixed and its reports still carrying numbers the pre-fix apparatus had
  produced. This re-measures and replaces them.

  **Assay #11 had never run its own registered workload.** Its Shape table
  registers a ~40-byte payload for both arms; both had been moved to the
  canonical empty object so assay #10's L1 could compare against a published
  figure taken that way, and #11 then reused #10's harvest arm unchanged. The
  deviation had been disclosed, which is not the same as grading registered
  lines on the registered shape. The apparatus now takes `ASSAY10_INPUT_JSON`,
  so #10 keeps the empty object its L1 needs and #11 runs both arms at its own
  payload. The same round found the Temporal arm persisting **no** activity
  input payload where harvest persists an explicit JSON null, which is 6,000
  smaller history records per repetition in Temporal's favour, and the embedded
  SQLite arm still returning JSON null where the other two return
  `{"ok": true}` — the embedded backend runs a caller-supplied callback rather
  than `ActivityInfo::handler`, so the earlier fix never reached it.

  Corrected figures: assay #11 is **43.29 against 5.47 workflows/sec**, a
  **7.91x** kill against harvest (computed from the unrounded means; a first
  revision printed 7.92x by dividing rounded display values, caught in review),
  replacing 44.31 against 5.58. Assay #10's
  embedded arm is **3.19**, replacing 3.20. The corrections moved neither
  headline measurably. Both facts are recorded: the withdrawn numbers came
  from an apparatus with known defects, *and* they happened to be right.

  **A harvest run was discarded for box contamination, and the report says so
  in detail.** Its repetitions read 5.39, 5.37 and **15.02** workflows/sec, an
  outlier roughly 2.8x its own siblings against an arm whose clean repetitions
  sit inside 5%. The cause was `git merge` and `git push` running on the box
  mid-measurement, against the idleness precondition `docs/benchmarks.md`
  insists on. What made it dangerous is that the contamination is invisible in
  the arm's mean, 8.60, and visible only per repetition, so a harness
  reporting means alone would have published it.

  An earlier revision additionally claimed that 8.60 *passed* assay #10's L1
  validity band the same arm otherwise kills, and called it a verdict flipped
  by interference. That is withdrawn: #10's band was registered for the
  canonical `{}` input and this run used #11's payload, so the band never
  applied and the flip could not have occurred. The claim rested on the same
  defect the apparatus fix above removes.

  Also newly reported rather than smoothed away: Temporal's spread across three
  repetitions (39.28-48.95, about 25%) is far wider than harvest's (about 5%),
  which three repetitions cannot characterise and this assay does not try to.

  **Every depth-diagnostic cell was re-measured** after the payload
  corrections. A first revision kept the pre-correction cells and argued from
  the single re-run cell that the curve was unaffected; review caught that as
  an overreach, since the corrections are asymmetric (one adds persisted
  payloads to Temporal only) and the shallowest cell had the narrowest margin.
  The re-measured 250-row cell reads 36.17 against 28.85 before, so the
  shallowest margin reads 1.53x where it read 1.22x. That difference is **not
  attributed to the correction**: both are single repetitions and the gap is
  about the size of this arm's own ~25% spread, so run-to-run variation alone
  explains it. The cells support a range (~1.5x-2x against harvest's best
  mode) rather than a trend, and Temporal's own cells span about 33%, which is
  too noisy to call flat. What holds is that Temporal wins at every depth
  tested and shows no counterpart to harvest's 4.2x collapse.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change. `registered-sweep.md` is kept unedited with
  the withdrawn embedded figure still in it, and the report points at the
  re-run instead, so the record shows what was measured rather than what was
  preferred.

- **Assay ledger #12: re-charter of assay #10's "position of the depth
  curve's knee" pit, killed on its own pre-registered stop rule.** #10's
  post-hoc, single-repetition depth diagnostic found the `postgres` arm
  ahead of `redis_pg` at backlog depths 250 and 500, and behind it at 1000
  and 2000, suggesting a throughput crossover somewhere in `(500, 1000]`.
  `docs/operations/redis-dispatch.md`'s "When to use it" section has no
  number backing "deep"/"shallow", so this re-runs the same two bracketing
  depths at n=3 (not n=1) before spending any time narrowing a band.

  Depth 500 replicates cleanly: `postgres` mean 23.76 (range
  [23.55, 23.86]) stays above `redis_pg`'s mean 21.48 (range
  [20.06, 22.29]), no overlap. Depth 1000 does not replicate: `postgres`'s
  three repetitions (14.94, 24.04, 23.80 — mean 20.93) span a 61% swing,
  and its range fully contains `redis_pg`'s tight [21.24, 22.03]. The
  pre-registration's own committed stop rule treats a rep-range overlap at
  either bracketing depth as unsettling the crossover claim; depth 750 was
  not run.

  This does not disturb #10's own separate, tightly-clustered finding
  (`redis_pg` flat within 1.4% from 250 to 2,000 rows in that sweep) — it
  shows that `postgres`'s own repetition-to-repetition variance at depth
  1000, under this apparatus's single-worker shape, is large enough that a
  lone repetition (exactly what #10's diagnostic was) cannot reliably say
  which arm leads at that depth. The operator guide gets no depth figure
  from this; the honest outcome is that "deep" stays undefined rather than
  acquiring a false-precision number that failed to replicate.

  One environment correction made before any reported run: the apparatus's
  own printed durability line showed the freshly started Postgres server
  defaulting to `fsync = on` / `synchronous_commit = on`, not #10's
  registered `off`/`off`. Caught before treating the first attempt as
  comparable; `postgresql.conf` was corrected and the server restarted, and
  the mismatched run was discarded rather than reported.

  Named, un-chartered pits: what causes the ~60% spread in `postgres`'s
  own repeated measurements at depth 1000 (cold cache after the
  apparatus's per-repetition database recreation is one untested
  candidate); a higher-repetition (n≥6) characterization of that spread;
  the knee question itself, which needs a variance-characterized apparatus
  before any depth number is decision-grade.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change. Apparatus reused unmodified from #10;
  raw per-rep output archived under
  `docs/assays/apparatus/0010-cross-mode-throughput/results/depth-knee-0012/`.

### Added

- **Cross-region DR via logical replication with fenced failover (issue #954).** Harvest's availability ceiling was one Postgres region per shard, and the obvious answer — a cloud cross-region replica — is dangerously incomplete for an event-sourced engine: after a failover, old-region workers that come back (or were partitioned, not dead) can still claim tasks and append events against their local, now-stale database, forking a workflow's history.

- **Mixed-kind concurrent waits in one suspension batch (issue #950).** `futures::join!`/`try_join!` and `ctx.race()` now accept **any combination** of ctx-managed awaitables — activities (fresh dispatch or re-park), durable timers, signal waits and child workflows — in a single call.

- **Payload codec key rotation with lazy re-encryption sweep (issue #948).** Adds a key-rotation story to the `PayloadCodec` boundary (ADR-0003). Before this, encryption at the codec boundary was permanent: `harvest_events` is append-only, so after a key compromise every byte of stored history stayed encrypted under the compromised key forever, and the only escape was a bespoke export/transform/reimport nobody runs under incident pressure.

- **Failing decision cycles are replay-clean and lossless (issue #952).** Two documented engine limitations made a workflow's *failing* decision cycle second-class. Both are closed here, composing only existing events — **zero new `WorkflowEvent` variants, no migration, no contract change.**

- **Stream audit records to an external SIEM sink (issue #953).** Every mutating management-API operation already writes an audit record, but those rows lived per-shard inside the same Postgres databases they describe — readable only through Harvest's own API, by whoever holds Harvest credentials.

- **Cross-shard child workflow placement (issue #956).** Child workflows were always pinned to the parent's shard, and `spawn_child_workflow_fan_out` had made a 10k-child fan-out three lines of code — so a single orchestrator concentrated its entire fan-out's event-append, task-claim and vacuum load on one Postgres instance no matter how many shards the operator provisioned.

- **Opt-in partitioned `harvest_events` for high-throughput retention (#958).**
  Native Postgres declarative partitioning, so a retention pass reclaims event
  storage by **dropping partitions** — an O(1) metadata operation — instead of
  row-level `DELETE`s that leave dead tuples, index bloat and vacuum debt.
  Measured at 200k rows: the row-`DELETE` path issued 99,740 deletes and left a
  15.41% dead-tuple ratio; the partitioned path issued **zero** and left
  **0.00%**, reclaiming the same 100,000 events in 99 ms, with concurrent append
  and task-claim p99 inside the ±5% budget.

  **Existing deployments are untouched.** The migration
  (`20260901115500_harvest_event_partitioning`) is inert: it ships the machinery
  but converts nothing. Opt in per shard with `harvest partition enable`, or
  `harvest partition plan` for a large live table (the plan keeps the index
  builds and constraint validation outside the `ACCESS EXCLUSIVE` window, so the
  swap itself is metadata-only). `harvest partition disable` reverts.

  Partition creation and reclamation are automated by the retention janitor — no
  operator cron pre-creates partitions. `harvest partition status` reports every
  cohort the sweeper left alone and why, which is the answer to "why has space
  not come back?".

  Zero new `WorkflowEvent` variants, no change to the event JSON at rest, and no
  change to any Diesel-generated statement: the partition key is a `cohort`
  column that `schema.rs` deliberately does not declare, so every read and write
  is byte-for-byte identical in both layouts. Per-execution event ids,
  `load_history`/`load_history_since` ordering and delta loads are unchanged —
  asserted by re-running the existing store, replay, retention, legal-hold and
  end-to-end suites against the partitioned layout in CI.

  Legal holds (#747) and per-type retention overrides (#737) block reclamation
  with no special-casing: a held or over-retained execution keeps its row, which
  keeps its events owned, which blocks the drop.

  Honest limits, all documented in `docs/partitioned-events.md`: history reads
  do not partition-prune (size the cohort width so the live partition count
  stays around 8–16); the partitioned layout drops the `harvest_events` foreign
  key — its `ON DELETE CASCADE` is the delete storm being eliminated — replacing
  the insert-time half with a trigger that also enforces `(workflow_exec_id,
  event_id)` uniqueness across partitions, with a documented residual window for
  two appends in flight at once across a cohort boundary; a legal hold now pins
  every cohort its execution wrote into, retaining other executions' expired
  rows with it unless `straggler_grace_secs` is set; the partitioned layout
  needs Postgres 14+; and the whole retention pass is not made faster by this
  change — the per-execution candidate loop the `HistoryArchiver` contract
  requires dominates it on both layouts.

- **Reproducible end-to-end benchmark suite with published results (#941).**
  Harvest had two performance artifacts — the replay CPU budget (#135) and the
  task-claim microbenchmark with its CI gate (#786) — and neither answered the
  first question an evaluating architect asks: *how many workflows per second,
  end to end, and at what latency?* This ships the answer, the harness that
  produces it, and a committed topology so a reader can check it themselves in
  one command.

  Four scenarios at 1, 2 and 4 shards: sustained **workflows completed/sec** for
  a canonical 3-activity workflow; **activity dispatch latency** p50/p99
  (`harvest_task_queue.created_at` → the handler's first line); **signal
  round-trip** p50/p99 (an HTTP signal request leaving the client → the workflow
  resuming past `wait_for_signal`); and **replay throughput** over the same
  10 001-event history #135 budgets. Results are published in
  `docs/benchmarks.md`, with each release's numbers kept in
  `docs/benchmarks/results-v<version>.md` rather than overwritten.

  `./benchmarks/run.sh` is the one documented command: it brings up
  `benchmarks/docker-compose.yml` (four **independent** Postgres servers, one per
  shard — a Harvest shard is a database with its own pool, so four databases on
  one server would make a shard sweep a measurement of one server contending
  with itself), runs all twelve cells, and tears it down.
  `HARVEST_BENCH_CHECK=1` additionally compares the run against the published
  baselines at ±15% and prints a per-number verdict; `HARVEST_BENCH_SCENARIOS`
  and `HARVEST_BENCH_SHARDS` narrow the matrix so reproducing one headline does
  not cost forty minutes.

  **Measurement discipline is the actual engineering here**, and every rule is
  enforced by the harness rather than promised in prose. Throughput is a
  **bounded closed loop**, not a pre-loaded drain: a first implementation that
  pre-loaded a backlog reported 473 workflows/s on the middle half of the drain
  and 55/s over the whole of it, because claim cost grows superlinearly with
  backlog depth — the drain accelerates as it empties, so the "rate" was an
  artifact of where you looked, and publishing it would have re-published #786's
  claim-depth curve under an end-to-end label. The closed loop keeps depth
  shallow and constant; `inflight_soundness` refuses to publish a run in which
  the harness, not the engine, was the limiter (it fires at 384 in flight on the
  reference box). The published default is **four times** the worker's workflow
  slots: a deeper population buys a higher number by measuring further into the
  claim-depth curve, which is #786's finding rather than this suite's. The two latency scenarios are paced at ~30% of the measured saturated
  rate, because a p99 taken under saturation measures the backlog rather than
  the dispatch path, and `pacing_verdict` marks a run that could not hold its
  pace. The signal round-trip is read from **one monotonic clock in one
  process**, so it carries no skew term; activity dispatch necessarily spans a
  database and a host clock, so the harness measures the offset and publishes it
  beside the number, and a negative sample — which only skew can produce — is
  counted and reported rather than clamped to zero. Any cell that is thin,
  truncated, left a shard idle, or failed a request reports `n/a` with a named
  reason.

  Replay is run at all three shard counts as the **noise control**: it is
  in-memory and cannot legitimately move with shard count, so drift across those
  three runs bounds how loaded the box was while the other nine cells were
  measured. Its history builder now lives in the shared harness and
  `benches/replay_bench.rs` calls it, so the throughput published here and the
  #135 budget can never drift into describing different workloads.

  **No duplication of #786 and no new CI gate.** The suite contains no claim or
  enqueue scenario, `docs/benchmarks.md` cross-references `docs/performance.md`
  as the component-level complement, and `benchmarks_docs.rs` fails the build if
  a CI manifest row ever runs an end-to-end scenario (issue #941 puts CI-gated
  end-to-end budgets out of scope until baselines stabilise). The same guard
  pins the doc to the harness: the published baselines are constants, the index
  page and the versioned results file must both carry them (the results file on
  a matching scenario/shard row, not merely somewhere in the text), every
  documented environment variable must be one the harness actually reads, the
  documented command must be the committed runner, the published worker and
  concurrency configuration must be the constants the run used, and the compose
  file must define a service per shard.

  **Zero engine impact.** No new `WorkflowEvent` variant, no migration, no
  behaviour or public-API change; nothing under `autumn-harvest/src/` is touched.
  Every number is taken from columns and events that already existed.

  Honest limits, all stated with the numbers: durability is off
  (`fsync=off`/`synchronous_commit=off`) so these are an upper bound for a
  durably configured Postgres; the signal endpoint is a minimal HTTP/1.1 handler
  calling the same `signal::send_signal` entry point the plugin route calls, not
  autumn-web's middleware stack; the workflow is deliberately trivial; the shard
  sweep holds hardware fixed, so it bounds *software* scale-out on one machine
  and says nothing about a four-machine deployment; and the published numbers
  come from native Postgres clusters rather than containers, which makes the
  compose path the more pessimistic of the two.

- **Vantage schedules management page (issue #951).** Every schedule capability an operator needs already existed as an API — list, fire-time preview (#348), backfill (#337), per-schedule run history (#534/#762), catchup policy state (#484), bounded runs (#478/#543), jitter (#240), overlap policy (#241), pause/resume — but at 3 a.m. the question "did the nightly billing schedule fire, and if not, is it paused, exhausted, or wedged?" still meant a curl session against five endpoints and a mental join.

- **`harvest migrate` for split/external Harvest databases (issue #1240).** Since 0.6.0 Autumn owns Harvest's migrations — but only the ones that live in the **application** database. Under `harvest.mode = "split"` / `"external"` Harvest storage is a separate database `plugin_migrations` cannot reach, and outside the `dev` profile the plugin only *warns* about pending migrations there.

- **Zero-setup local dev runtime (issue #525).** autumn-harvest is, by primitive surface, essentially Temporal-complete — and the first thing a new developer had to do was unrelated to workflows: install Docker, author a `compose.yaml`, `docker compose up -d`, poll for Postgres health, set `DATABASE_URL`.

- **Semantic (MIR-level) determinism analysis for workflows, R&D (issue #962).** The determinism defences shipped so far are **syntactic**: the `#[workflow]` compile-time guardrails (HVG001–HVG011) pattern-match tokens inside the annotated body, and `harvest det-check` (#778) plus the HashMap-iteration rule (#785) extend the same tables across exactly one hop, resolved in the caller's own module.

- **The boot-time orphaned-workflow gate now covers the standalone runner (issue #1128).** PR #1109 (issue #700 AC4) added a boot-time orphaned-workflow-type reachability fail-fast to the **plugin** boot path and explicitly logged the gap it left: "the standalone `HarvestRunner::start` embedder path has no orphan gate at all … A separate issue should add an equivalent pre-worker check inside the runner." This closes that gap — and closes it by making the two paths run *the same code*, not an equivalent copy.

- **Dependency ledger harness + wasmtime/h2/crossbeam-epoch security fixes (PR #1303).** No lockfile-verified dependency audit previously existed in this repo — no `cargo-audit`/`cargo-deny`, no advisory scan, no license gate in CI. This PR adds one (`deny.toml` + a new `dependency-audit` CI job running `cargo deny check` via `EmbarkStudios/cargo-deny-action@v2`) and, using it, fixes the reachable findings it turned up:

- **Idle rate-limit bucket GC (issue #1127).** `harvest_rate_limit_buckets` rows are auto-registered `ON CONFLICT (key) DO NOTHING` and, until now, were **never deleted** — no production `DELETE` existed anywhere in the tree. Two of the key conventions that share that table embed caller/tenant input: `dyn-rate:{expr}:{resolved}` (per-key activity rate limits, #699/PR #1101) and `start-throttle:{workflow}:{key}` (workflow-start throttles, #607).

- **Shared connection guard for cross-shard fan-out reads (🪞 Echo clone-class merge).** Eight management-API read models (`canary`, `status_summary`, `usage`, `workflow_count`, `workflow_reachability`, `queue_coverage`, `version_gate_retirement`, `version_usage`) each hand-copied the same per-shard connection guard every time a new cross-shard fan-out endpoint was added between issues #164 and #796: resolve `Option<DbPool>` or record `"shard {id} has no configured storage pool"`; acquire a connection or record `"database connection for shard {id} could not be acquired"`.

- **Shard-placement-aware resolution for `workflow_id`-addressed signal/cancel (issue #1146).** `ctx.signal_external_workflow_by_id` / `ctx.request_cancel_external_workflow_by_id` (issue #751) address a target by its stable `(workflow_name, workflow_id)` business key. To deliver, the engine has to know which shard owns the target, and it answered that by re-deriving `ShardRouter::pick_for_new_workflow` — the rendezvous hash a **fresh start** of that key would use.

- **Dev runtime: the `sslmode` scan reads syntax, not substrings (issue #1286).** `classify_database_url` runs a textual `sslmode` scan *before* handing the DSN to `tokio_postgres::Config::from_str`, because the client models only `disable`/`prefer`/`require` — so `verify-ca` and `verify-full`, the two strongest signals that a DSN points at a remote managed database, would otherwise arrive as an opaque parse error rather than as the specific thing they are.

- **Reject an unrepresentable workflow duration at `try_build()` (issue #1163).** Closes a silent-data-loss gap in `#[workflow(execution_timeout = "…")]` (and the sibling `chain_execution_timeout` (#617) and `sla` (#487) fields): every start path resolved a declared `std::time::Duration` via `chrono::Duration::from_std(d).ok()`.

- **Shard rebalancing: migrating quiescent workflows across shards (issue #964).** The sharding contract used to end at "cross-shard rebalancing of existing workflows is out of scope", and the consequences compounded: adding a shard only helped **new** starts, so a hot shard stayed hot for as long as its residents lived — forever, for a continue-as-new entity workflow — and a shard could never be decommissioned.

- **Strict percent-decoding swept to every raw-pairs route (issue #1151).** Issue #774 fixed one route, `GET /admin/queue-coverage`: axum's built-in `Query<Vec<(String, String)>>` extractor is backed by `serde_urlencoded`/`form_urlencoded`, which *always* succeed by silently substituting `U+FFFD` for a malformed percent-encoded byte sequence instead of rejecting the request — so `?queue_name=%FF` silently decoded to a *different*, legitimate-looking `queue_name` instead of the documented `400`, letting a corrupted request produce a false-clean result on a CI/CD deploy gate.

- **The dev profile serves the management API unauthenticated, as documented (issue #1284).** The quickstart's documented Step 3 —

- **Canary replay's completing path now agrees with its suspended path at the frontier (issue #1175).** `executor.rs::run_workflow_canary` had two terminal arms that reach the frontier — recorded history fully consumed — and disagreed about whether a new command emitted there is drift.

- **Hot code swap for workflow definitions via runtime modules, R&D (issue #967).** Harvest's *routing* side of deploys has been finished for a while — build-ID routing (#171), compatibility sets, percent ramps (#604), reachability (#520/#535) — but the *delivery* side is still "ship a new binary and bounce the fleet".

- **Ownership guard on ordinary workflow terminal writes (issue #1184).** Issue #1182 (and #804 before it) closed the worker-ownership race for one narrow branch: `handle_suspended_workflow`'s empty-command-set catch-all, via `commit_terminal_failure_if_still_claimed` / `fail_suspended_workflow_if_still_claimed`, reusing `queue::claim_still_held_for_update` (a `FOR UPDATE SKIP LOCKED` recheck of `worker_id` + `crash_strikes` under a fresh row lock).

- **Build-scope the diagnose rate-limit bypass per eligible worker (issue #1190).** Follow-up from issue #809 (PR #1188). `GET /workflows/{id}/diagnose` (`build_diagnosis_report` in `autumn-harvest-plugin/src/api.rs`) decides whether a pending activity's rate-limit bucket is even a claim-time impediment by checking whether the activity is circuit-breaker-tracked in **this API replica's own in-process** `CircuitBreakerRegistry` — a compile-time `#[activity(circuit_breaker = ...)]` fact, because a breaker-tracked activity is gated at dispatch, not at claim (#369).

- **Distinguish a timed circuit-open from a forced one in stall diagnosis (issue #1193).** Follow-up from a Codex P2 on PR #1188 (issue #809, `GET /workflows/{id}/diagnose`). `BlockedOn::health()` reported **every** non-half-open circuit-breaker verdict as `ExecutionHealth::Stalled` ("needs a human: nothing will move this run forward on its own").

- **Post-commit completion-trigger supersede metrics and an observable nested-admission residual (issue #1197).** Follow-up to #811 (PR #1196), closing the two residuals that were deliberately deferred out of that slice rather than rushed in.

- **`TerminateIfRunning` cleans up an orphaned undelivered signal (sqlite backend, issue #1374).** 🪝 Snag exploratory QA finding: `autumn-harvest-sqlite`'s `cancel_and_seal_prior` (the `TerminateIfRunning` reuse-policy cleanup path) already deleted a sealed prior's PENDING task rows and unfired timers — see `terminate_if_running_cleans_up_orphan_timer` — so nothing orphaned by the replace could be claimed or fire against a run that will never be driven again.

- **Quota-blocked retries get a bounded backoff (issue #1227).** A sixth automated Codex review round on PR #1221 (issue #946, per-tenant resource quotas) surfaced four `HarvestError::QuotaExceeded` catch sites that correctly avoid terminally failing an execution over an unrelated tenant's transient quota condition, but don't defer the retry with a backoff — so a target durably at quota cap (e.g. a `max_dead_letters` cap, which only clears via manual operator action) causes either a zero-delay retry loop or starvation of unrelated …

- **Source-aware shard coverage for `GET /workers` fan-out (issue #1213).** Filed from Codex round 7 on #1207 (itself part of #961): `GET /workers?shard_id=B` could return a worker registered with the empty (auto/legacy) `shard_assignments` shape in shard A's database, even though that shape means "covers whatever shard the row was read from" (#1150), not "covers every shard".

- **Attribute-macro signature checks route through attr_util (🪞 Echo clone-class merge).** `query.rs`, `update.rs`, and `signal.rs` each hard-coded a private copy of `first_param_is_ctx` (checks the handler's first argument is `&WorkflowContext`), and `query.rs`/`update.rs` each hard-coded a private copy of `returns_result` (checks the handler returns `Result<T, E>`).

- **SSRF guard rejects IPv4-compatible IPv6 loopback/private embeds (issue #1390).** 🪝 Snag exploratory QA finding: `completion_callback::is_ipv6_non_routable` unwrapped an embedded IPv4 address via `Ipv6Addr::to_ipv4_mapped()`, which recognizes only the modern IPv4-*mapped* form (`::ffff:a.b.c.d`, RFC 4291 `::ffff:0:0/96`). The deprecated IPv4-*compatible* form (`::a.b.c.d`, the bare `::/96` prefix) encodes the identical address but `to_ipv4_mapped()` returns `None` for it, so `validate_target_url` classified `https://[::127.0.0.1]/hook`, `https://[::10.0.0.5]/hook`, `https://[::192.168.1.1]/hook`, and `https://[::169.254.1.1]/hook` as **routable** — bypassing the exact guarantee the function's own doc comment states ("even then is rejected if it …

- **Replay-drift gate propagates nested fixture-walk errors (issue #1174).** Filed from a Codex review on #1171 (issue #798): `testing.rs::collect_json_files` propagated a failure to read the **top-level** bundle directory, but swallowed every error found while walking **nested** directories.

- **Fleet health `by_shard` counts empty-assignment workers (issue #1208).** Split out of PR #1207 (issue #961). `GET /workers/health`'s `by_shard` tally dropped a worker advertising the empty (auto/legacy) `shard_assignments` shape: an empty array names no bucket, so a healthy worker polling shard 7 could leave `by_shard[7] == 0`.

- **Cross-type continue-as-new: target-resolved input cap and an exhaustive carry/re-resolve partition (issue #1161).** Follow-up from #803 (cross-type continue-as-new). #803 left four `WorkflowInfo` fields out of the successor-default resolution: `throttle` (#607), `debounce` (#499), `batch` (#518), and `max_input_bytes` (#252), documented only as "not consulted".

- **Backup-verify post-budget correctness fixes (issue #1205).** Four correctness findings from the Codex review of #1204's `e3b089f`, filed after that PR's review-round budget was exhausted. All four were verified against source before filing; none is a regression from #1204 — each is a gap in the verifier #1204 shipped.

- **OpenAPI 3.1 spec for the management API (issue #694).** The management API now publishes an OpenAPI 3.1 document, so an integrator generates a typed client in any language instead of hand-writing HTTP calls against routes reverse-engineered from prose.

- **Bump `lru` 0.16 -> 0.18.4, closing our own RUSTSEC-2026-0253 exposure (PR #1417).** `deny.toml` (PR #1393) deferred RUSTSEC-2026-0253 — a use-after-free in `LruCache::pop()` when a stored key's `Drop` panics mid-pop — as unreachable for `autumn-harvest/src/cache.rs`'s `WorkflowCache` (its key and value types have no custom `Drop`), and flagged the fix (`lru` >= 0.18.2) as a candidate for "the next scheduled batch" once a changelog read confirmed no breaking API surface.

- **Shared stub-use path derivation for query/update/signal (🪞 Echo clone-class merge).** `query.rs`, `update.rs`, and `signal.rs` each hard-coded an identical 32-line derivation of `leading_colon`/`nested_path_tokens` from a parsed `WorkflowPath`, differing only in the `mod_name` prefix declared one line above the block.

- **Bound multi-shard worker shutdown against an exhausted shard pool (issue #1209).** Filed from the Codex review on PR #1207 (itself part of #961): that review's budget was exhausted, so this correctness finding shipped as its own issue.

- **Reject an empty `workflow_id` across the by-id route family (issue #1353).** `workflow_id: ""` was accepted by `POST /workflows/{name}/start` with no validation, as a value distinct from omitting the field (omitting it auto-generates a UUID; an explicit empty string was taken literally as the business id).

- **`redact_dsn` withholds a hostless DSN (issue #1416).** `redact_dsn`'s doc comment promises that a malformed-but-secret-bearing DSN never reaches a report or a log line — it falls back to `"<unparseable dsn>"` on anything `url::Url::parse` rejects outright.

- **Redis dispatch worker integration (issue #1312).** **Opt-in. Additive. No new `WorkflowEvent` variant and no migration.** The `autumn-harvest-redis` crate has existed and been tested since Phase 4, but it was never wired into the worker: its own module doc named the required transactional-boundary refactor as unbuilt, so an operator could not turn it on at any throughput.

- **`cancel` on a terminal execution now answers 409, not 400 (issue #1444).** `docs/openapi.json` documents `409 Conflict` — "Execution is already in a terminal state" — for both `POST /workflows/{id}/cancel` and `POST /workflows/by-id/{workflow_name}/{workflow_id}/cancel`. The sibling mutating routes, `pause_workflow` and `rerun_workflow`, honor that: both route their `HarvestError::Config("… already terminal …")` through the shared `conflict_from()` helper (issue #383's own state-conflict convention) and answer 409.

- **Vantage degraded-banner focus-on-load (issue #1446).** The Vantage operator dashboard's `div.degraded-banner` render sites (schedule fire-time preview, schedule run history's cross-shard degradation banners, and the backfill launcher's error/warning banners) — the dashboard is the product's stated no-separate-service alternative to querying Postgres by hand for debugging failed workflows (`docs/plans/vantage-spec-dashboard-ui.md`), so a message an operator cannot perceive defeats the page's own purpose.

- **Exact filter-argument equality in the chaos docs/CI parity guard (issue #1224).** `chaos_docs::doc_example_filter_matches_chaos_workflow_filter` tied `docs/testing/chaos.md`'s local-iteration example to `.github/workflows/chaos.yml`'s filter using two independent `contains("chaos_tests::")` checks. Both are prefix checks, so if either side narrowed its filter (e.g.

- **Backfill quota_key for pre-upgrade executions (issue #1226).** Follow-up from a Codex automated-review finding on PR #1221 (issue #946, per-tenant resource quotas): the migration that added `harvest_workflow_executions.quota_key` shipped with no backfill, by design — the key-resolution expression is Rust application code, not something a pure-SQL migration can evaluate.

- **Vantage jump-to-event label association (issue #1485).** The Vantage operator dashboard's workflow detail page (`autumn-harvest-plugin/src/ui.rs`), specifically the "Jump to event" control in the event timeline pagination. It only renders once a workflow's history exceeds `DETAIL_EVENT_PAGE_SIZE` (100 events) — a long-running or heavily-retried workflow is exactly the kind an operator is investigating via the runbook's five stuck-workflow scenarios (`docs/vantage-ui.md`), so this is exactly when the control appears.

- **Codec key rotation: structural fleet-wide preconditions (issue #1244).** Follow-up to #948 / #1242. Two correctness preconditions for codec key rotation were real but enforced only by operator discipline. This closes both structurally.

- **Cross-shard child placement: five P1 follow-ups from #1260 (issue #1263).** **Scope.** Issue #1263 tracks 17 follow-ups deliberately deferred from PR #1260 (opt-in cross-shard child workflow placement) so that PR could land without widening. This PR closes the six the issue's own author flagged P1, all introduced by #1260: items 7, 10, 11, 13, 15, and 17.

- **Terminate's idempotent no-op stops claiming a false cancellation (issue #1456).** `terminate_workflow_execution_collect` and `cancel_workflow_execution_collect` share `CancelledWorkflowExecution::idempotent()` for their already-terminal no-op path. Its fallback reason — used only when the row's `error` column is `NULL` — was a single fixed string, `"workflow already cancelled"`, written for cancel's narrow reuse (reachable there only on an already-`CANCELLED` row, where `error` is always populated by the original cancellation, so the fallback was dead code).

- **Cross-region DR follow-up correctness fixes (issue #1249).** Thirteen correctness findings from rounds 6–9 of the automated review of #1246's cross-region DR work (issue #954), filed after that PR's review-round budget was exhausted.

- **Quota-vs-supersede ordering and a detached-child lock-ordering gap (issue #1228).** A seventh automated Codex review round on PR #1221 (issue #946, per-tenant resource quotas) surfaced two more findings against commit `3bda6fc`, after all 5 permitted review rounds for that PR were used and round 6's findings were already tracked in #1227.

- **Close the sweep-batch-vs-retirement race in codec rotation (issue #1251).** Fixes a narrow but serious race in the codec-rotation re-encryption sweep (issue #948): a re-encryption batch could commit a row under a key that `retire_codec_key` had already retired, making that row undecodable — permanently, if the operator destroyed the key material on a green gate.

- **A coherent audit-export cursor lifecycle: close a revival race, audit retire/reactivate (issue #1273).** Two deferred findings from #1261's review (issue #953's audit-export cursor), filed together because they are two symptoms of the same under-designed mechanism: `decommission_cursor` retired a shard's export cursor by setting `retired_at`, but the states a cursor can be in, who may transition it, and what gets recorded were never written down.

- **Audit-export redrive reports what it can actually deliver (issue #1267).** `POST /admin/audit-export/redrive` rewinds a shard's export cursor under `SELECT … FOR UPDATE` on the cursor row. `audit::purge_old_audit_records` does not take that lock — it reads the cursor through a plain, uncorrelated subquery.

- **Claude agent-daemon example on the SQLite backend (PR #1493).** A new top-level example, `examples/claude-agent-daemon/`: a local daemon (`agentd`) that runs Claude agent sessions as **durable workflows** on the embedded `autumn-harvest-sqlite` backend. No Postgres, no Docker, one binary and one file.

- **Directory-privacy enforcement no longer skips Windows (issue #1287).** Both directory-privacy checks in the dev runtime lived inside a `#[cfg(unix)]` block: `harden_root` (`reaper.rs`, guards the session root a planted record could point `stop_orphan`'s `pg_ctl` at) and `directory_is_private` (`acquire.rs`, guards the managed-tier binary cache).

- **Dependency ledger: `unsound` advisory scope gap + anyhow RUSTSEC-2026-0190 fix (PR #1556).** Routine Ballast ledger sweep. `cargo deny check` came back fully green (no errors), but with one warning: the `RUSTSEC-2026-0253` (lru) ignore entry in `deny.toml`, previously verified reachable-and-deferred in PR #1417, now reported "advisory was not encountered" — cargo-deny no longer evaluates it at all.

- **Audit-export retention bootstrap-window guard (issue #1266).** `purge_old_audit_records` already refused to delete an unexported audit row when a live cursor existed for the shard, or when the sweeping process had a sink configured.

- **Give the codec-rotation revalidation deadline its own column (issue #1258).** Fixes a liveness bug in the codec-rotation re-encryption sweep (issue #948): on a busy, already-converged shard, the periodic re-census that finds a row committed below the cursor could be starved indefinitely.

- **Print the actual history shape on a flaky assertion mismatch (issue #1558).** `worker_fails_workflow_when_activity_start_to_close_timeout_elapses` (`autumn-harvest/tests/integration/integration_e2e.rs`) failed once after a prior hardening fix. The issue ruled out all four candidate root-cause mechanisms by reading source and found no supported hypothesis.

- **Codec envelope nests, closing the business-data collision (issue #1253).** Found by automated review on autumn-foundation/autumn-harvest#1242 and filed separately: the flat codec envelope (`_harvest_codec_envelope` as a sibling key next to `codec_id`/`data`/`kid`) was structurally indistinguishable from business data that happened to carry the same keys. #948 moved the version discriminator from 1 to 2 to avoid reinterpreting old four-key plaintext, but a *new* value shaped exactly like the version-2 envelope collided with it too — bumping the version relocates this hazard …

- **Redact completion-callback target URLs in startup and runtime logs (issue #1274).** `HarvestBuilderError::CallbackTargetRejected`'s `Display` interpolated the full completion-callback target URL. A builder error surfaces through `try_build()` straight into startup logs. A completion-callback target often carries a bearer token or signing key in its path or query.

- **Harvest-verify correctness follow-ups from Codex round 6-8 (issue #1296).** Nine correctness findings from three Codex review rounds on #1294 (rounds 6-8, after that PR's review budget was exhausted), all in `autumn-harvest-verify` — no engine footprint.

- **Close the fourth `expected_shards` copy issue #1146 missed (issue #1146).** `autumn-harvest-plugin/src/version_gate_retirement.rs`'s `expected_shards` was a byte-for-byte hand-rolled duplicate of "pool keys ∪ readable shards ∪ default shard, or `{0}` if empty" — the same fan-out rule issue #1146 consolidated onto [`shard_fanout::expected_shards`] (itself `external_target_location::fanout_shards`) for `version_usage.rs`, `workflow_reachability.rs`, and `workflow_count.rs`.

- **Rate-limit bucket GC failure reports keep `dry_run` (issue #1316).** `RateLimitBucketGcOutcome::failed` filled every field but `error` from `Default`. `dry_run` came back `false` on every failure, even a failed `dry_run: true` preview. `GET /admin/retention` then showed a failed preview as a failed REAL pass — the exact conflation `RateLimitBucketGcOutcome` (issue #1127) exists to prevent.

- **`harvest migrate`'s target label reads DSN separators as libpq does (issue #1321).** `autumn-harvest-cli::scan_keyword_dsn` split a libpq keyword/value DSN's options with `u8::is_ascii_whitespace`. `tokio_postgres` — the client that actually connects with these DSNs — splits on `char::is_whitespace`, the Unicode `White_Space` property (`tokio-postgres`'s `config::Parser::skip_ws`).

- **Two pre-registered assays: cross-mode throughput, and the first
  cross-engine comparison (ledger #10, #11).** Harvest had four measurements
  of its own modes, taken at four shapes on at least three hosts against at
  least three trees, with the embedded SQLite backend appearing in none of
  them; and `docs/benchmarks.md` stated plainly that no competitor had ever
  been run on the same hardware. Both gaps are now closed by measurement, and
  both assays report a **kill**.

  **#10 (cross-mode)** drains one backlog of the canonical 3-activity workflow
  on three arms — embedded SQLite, the Postgres claim path, and Postgres with
  `RedisDispatch` — on one box, from one tree, in one sitting. Its validity
  line **failed**: the Postgres arm read 5.58 workflows/sec against a
  registered `[7.91, 71.19]` band around the published 23.73, so L2 and L3 are
  ungraded and the apparatus refuses to print them. The cause was this repo's
  own documented warning: `docs/benchmarks.md` records why #941 chose a bounded
  closed loop over a pre-loaded drain, because a drain re-publishes #786's
  claim-depth curve under an end-to-end label — and #10 registered a 2,000-deep
  drain anyway. The durable finding is its post-hoc depth diagnostic:
  `redis_pg` is **flat within 1.4%** from 250 to 2,000 rows of backlog
  (22.21–22.53 workflows/sec) while `postgres` falls **4.2x** (23.90 → 5.63).
  The channel's margin is therefore 0.93x at 500 rows and 3.94x at 2,000: it is
  **insurance against backlog depth, not a general throughput upgrade**, and a
  deployment that never builds a deep queue pays for a second stateful
  dependency and gets nothing. The embedded arm's 3.20 workflows/sec is
  deliberately *not* presented as like-for-like — `autumn-harvest-sqlite`
  hard-codes `PRAGMA synchronous = FULL` and exposes no knob, while the
  Postgres arms ran `fsync=off` per the published bench conditions, so part of
  any gap is durability rather than engine, and L3 was registered without
  noticing that.

  **#11 (harvest vs Temporal)** puts both engines on one 4-core box against the
  same PostgreSQL server at the same shape. **Harvest lost, by 7.9x** (5.58
  against 44.31 workflows/sec), and lost in the venue that disadvantages its
  competitor, with all four Temporal services co-resident. Its depth diagnostic
  is what makes the result usable rather than merely bad news: Temporal is
  faster at *every* depth tested, so the kill is not an artifact of depth;
  against harvest's *best* mode the margin is a stable ~1.7x rather than 7.9x;
  and the widening part is the known, fixable `#786`/`#1177` claim-path defect.
  That bounds the highest-value performance fix available: repairing #1177
  would move the default mode from 5.63 toward the Redis arm's flat 22 at depth
  2,000, taking the margin from 6.9x to roughly 1.8x.

  **Measurement discipline is most of the work here, and it is adversarial by
  construction.** Both pre-registrations were committed before either apparatus
  existed, and #11's fixed *in advance* what its result may not be read to mean,
  so a harvest win could not later be spun as "faster than Temporal." Six
  rounds of review found 19 defects; **three sweeps were discarded rather than
  reported**, including one whose Redis arm published no dispatch hints at all
  and was therefore timing the reconcile sweep's recovery path. Five separate
  places where #10's workload was not the port-by-value it claimed were fixed
  and then the whole workload was diffed against `e2e_bench_support.rs` field by
  field. Four parity defects in the Temporal arm, **all of which disadvantaged
  Temporal**, were fixed before the reported run. Caveats that cut against
  harvest are recorded rather than omitted: the measured window charges worker
  startup, which is far costlier for Temporal, so the shallow-depth cells
  understate it.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change; nothing under `autumn-harvest/src/` is
  touched. Both apparatus are non-production throwaways, detached from the root
  workspace. `docs/comparison.md`'s stale "no first-party benchmarks yet" bullet
  is corrected, and `docs/benchmarks.md` now carries the Temporal result
  including the part where harvest loses.

- **Dev runtime: withhold a DSN a keyword scanner cannot vouch for (issue #1322).** `banner::redact_keyword_value` accepted any `key=value` token as a libpq keyword. A mistyped URL that lost its `://` — `postgres=//alice:hunter2@db` — scans as the "keyword" `postgres`. It has no `password=` option, so the scanner found nothing to blank out and printed the whole string, credential included.

- **Hot code swap review-round 6/7 fixes (issue #1345).** Seven correctness findings against the `hot-code-swap` R&D spike (#967), found past that PR's review budget and recorded on this issue instead. All behind the `hot-code-swap` Cargo feature, so none affects a default build.

- **Cross-shard business-key race: decision recorded, not re-fought (issue #1313).** Issue #1313 followed up on PR #1306 (issue #1146, Codex round 4): a by-id fan-out reads every expected shard sequentially, on separate connections, with no shared snapshot.

- **Ownership guard on the pause fast-path park (issue #1347).** Issue #1184 added a worker-ownership recheck (`queue::claim_still_held_for_update`) to every ordinary terminal-write path: completion, failure, and `check_paused_and_park`'s own operator-pause re-park. One site kept the old, unguarded shape: `process_workflow_task`'s early, non-locking pause fast path, which called `queue::park_workflow_task` directly.

- **Both Harvest routers return `Router<()>` (issues #1606, #1607).** First of epic #1605's two roots. `harvest_api_router` and `harvest_ui_router` returned `Router<autumn_web::AppState>`, and that type parameter is what forced every non-autumn-web embedder to construct an `AppState` it had no other use for.

- **A standalone mount can reach a working admin credential (issue #1608).** The second of epic #1605's two roots, and the one that made the standalone integration path unusable rather than merely awkward.

- **A by-id cancel reports from a later fan-out than the one that cancelled (issue #1313).** `external_target_location`'s by-id fan-out reads every expected shard sequentially, on separate connections to separate databases, with no shared snapshot — Postgres has no cross-shard transaction and Harvest adds no coordinator.

- **Batched seek-and-refine claim: `queue::claim_task_batched` (issue #1340).** Issue #1340 tracks the architectural fix `docs/performance.md`'s "Any residual predicate defeats sort-elision" section (issue #1177) names but declines to attempt: a seek-and-refine restructuring of the claim candidate scan.

- **Retry deadlines computed on Postgres's own clock (issue #1389).** A Codex review round on PR #1386 (issue #1227) found that `queue::requeue_for_retry` computes its retry deadline on the **host** clock (`Utc::now() + delay`), but `claim_task` checks eligibility (`scheduled_at <= NOW()`) on **Postgres's** clock.

- **Quota-retry backoff survives a stale sentinel (issue #1391).** A Codex review round on PR #1386 (issue #1227's quota-retry backoff), filed as a follow-up because that PR's own 5-round review budget was already spent, found a way to defeat the bounded backoff PR #1386 added.

- **Outbox relay backoff deadlines computed on Postgres's own clock (issue #1392).** A Codex review round on PR #1386 (issue #1227), past that PR's 5-round review budget, found that the completion-trigger outbox's backoff deadlines (`next_attempt_at`) are computed and checked on each scanner replica's own **host** clock, not Postgres's.

- **`HarvestMetricsRecorder` gains framework-neutral Prometheus rendering (issue #1611).** Part of epic #1605's Tier 2 (make the non-autumn-web integration path first-class). `HarvestMetricsRecorder` aggregates the ADR-0001 §7 catalogue metrics entirely in-process, but the only way to read them back out was `impl MetricsSource`, whose rendering happens inside autumn-web's shared `/actuator/prometheus` endpoint.

- **Select every per-shard monitor's acquisition against cancellation (issue #1426).** Split out of the review on PR #1424 (itself part of #1209): that PR's fix was scoped to three specific mechanisms (`transition_fleet_status`, the heartbeat loop, and the schedule-overdue sampler's eager first pass).

- **TLS for LISTEN/NOTIFY listeners (issue #1717).** `QueueListener`, `WorkflowEventListener` and `WorkflowProgressListener` always used `NoTls`. Every listener failed on Postgres with `sslmode=require`. Workers fell back to polling, so the failure stayed hidden. `result_raw` had no fallback, so each synchronous result wait failed.

- **Shared release-claim-via helper for queue/activity/workflow pause (🪞 Echo clone-class merge).** `queue_pause::release_claim_if_queue_paused`, `queue_pause::release_claim`, `activity_pause::release_claim_if_activity_paused`, and `execution::release_claim_if_workflow_paused` each bound `task_id`/`worker_id` into their own pre-built statement, executed it, and mapped the row count to a bool, in a byte-identical body.

- **Redis dispatch follow-ups: latency, coverage, and operability (issue #1429).** Issue #1312 shipped Redis Streams as an opt-in dispatch channel in front of the Postgres task queue. It shipped with ten known gaps, each named in its own doc comment or tracking note.

- **Stall diagnosis: timer provenance survives a scheduled_at drift (issue #1402).** Follow-up to issue #1191 (PR #1403): `stall_diagnosis::is_the_missed_timer_wake` trusted an exact or near-exact match between a workflow task's `scheduled_at` and a durable timer's `fires_at` as proof the row is that timer's own missed wake.

- **Redis dispatch: a worker keeps its channel, and a late install is revalidated (issue #1431).** Follow-up to issue #1312 and PR #1698. Both findings share one root cause: the process-global dispatch slot can change after a worker starts.

- **Lifecycle invariant gaps: guarded transitions, one transition table, CI that runs the DAG suites.** An audit set every workflow execution, task queue and DAG lifecycle transition against the checks that enforce it. This change fixes the defects it found and closes the coverage gaps it can close without a schema change.

- **Redis dispatch reads reach every queue under slow round trips (issue #1754).** `RedisDispatch::read_across_queues` divided its blocking budget between the queues and did not count the Redis round trip. On a slow link, the deadline passed before the rotation reached the queues at its tail.

- **Dependency ledger: restore unsound-advisory transitive scope (PR #1760).** `deny.toml`'s `[advisories]` section had documented `unsound` advisory scoping as a solved problem: cargo-deny 0.18 removed the `[advisories.unsound]` field and unconditionally denied `unsound` advisories for every reachable instance, workspace or transitive.

- **Vantage action forms preserve entered data on error (issue #1687).** The Vantage workflow detail page's three operator recovery actions — Send signal, Reset to event N, Trigger update (`autumn-harvest-plugin/src/ui.rs`). `docs/vantage-ui.md`'s "Diagnosing a stuck workflow" runbook routes an operator through these for 3 of its 5 documented scenarios (waiting-on-signal, blocked-on-child, replay non-determinism), so this is an incident-response path, not a peripheral one.

- **Inbound webhooks work off the plugin path (issue #1612).** Part of epic #1605 (making the non-`HarvestPlugin` integration path first-class). `build_webhook_routes` returns `Vec<autumn_web::Route>`, which only composes with an `autumn_web::AppBuilder` — so a `#[webhook]` binding was reachable only through `HarvestPlugin`, even though Chapter 12's own example is a plain 50-line Stripe integration with nothing plugin-specific about the mapping logic itself.

- **Offload look-alike escape (issue #1758).** Business data that carried `_harvest_offload_envelope: 1` was read back as a dangling reference. Replay of that event failed with `PayloadOffload`.

- **Audit export disable marks shards unobserved (issue #1506).** A live process could clear its audit-export config. Both audit-export gauges then kept their last value, and no alert fired. `set_global_audit_export_config` now records the `Some` to `None` change.

- **A `before` redrive reports a prefix retention already purged (issue #1508).** `POST /admin/audit-export/redrive` with `before` derives its window from the records that still exist. Retention could purge the earliest records at or after the instant. The redrive then resolved to the lowest survivor and reported `already_purged_records: 0`.

- **`HarvestEmbedding`: one entry point for a standalone embedding (issue #1613).** `HarvestEmbedding::new(built, config, resources).start()` runs the standalone startup sequence and returns the runner and a mounted `Router<()>`. The embedder used to do each step by hand, and the reference example skipped most of them.

- **Skip-policy shift target is a weekday (issue #1772).** `run_next_business_day` and `run_prev_business_day` could land on Saturday or Sunday for calendars such as `us-federal-holidays` and `nyse`. The scheduler passes `exclude_weekends = false` for every calendar except `weekends-off`.

- **Shared with-start admission step (issue #1440).** Signal-with-start and update-with-start no longer duplicate their start path.

- **Postgres poll path claims only against a free local permit (issue #1787).** The default Postgres poll path claimed a task before the worker had a free permit for it. The claim stamps `started_at`, and start-to-close and heartbeat deadlines run from `started_at`.

- **The chaos suite runs nightly, with a watchdog (issue #1790).** `chaos.yml` did not parse from its first commit. Two `run:` values held `chaos:: ` unquoted, and YAML reads `: ` as a mapping value. GitHub ran the file as a zero-job failure and never fired its cron.

- **Claim-epoch fence on activity ownership writes (issue #1789).** A worker whose liveness row went stale could still write the result of an activity. The orphan reclaimer requeued the row, and a second worker claimed it.

- **Completion-trigger fire retention (issue #1676).** `backup verify` proves a delivered completion-trigger fire from the target's `harvest_execution_summaries` row. That row expires at `--summary-age`. The fire row never expired, so an old fire fell back to a timestamp guess.

- **`#[workflow]` names the fix for a `_` input parameter.** `#[workflow]` now rejects a `_` or destructuring input parameter with a compile error that names the cause and the fix (`_input: ()`). Before, the macro dropped the pattern and rustc reported `E0061` "takes 5 arguments but 4 arguments were supplied" on the attribute line.

- **README cron-schedule and retry snippets compile.** The README's "Scheduling workflows on a cron" snippet called `.workflows(..)` and `.workflow_schedule(..)` on `autumn_web::app()`. That builder has neither method, so the snippet failed with `E0599`.

- **Process-, database- and network-level crash-recovery tests (issue #1801).** The chaos KILL is a panic in one tokio task. The new tests in `chaos_tests::infra_faults` inject faults below the engine. They run real workers against a Postgres 16 container and a toxiproxy container:

- **Deterministic suspension readiness (issue #1797).** The executor no longer uses the 100 ms `SUSPENSION_TIMEOUT` to decide that a workflow is suspended. A cycle now suspends when the handler is `Pending`, no wake fired during the poll, and a Harvest future is parked.

- **Batch the mixed-suspension per-timer lookup, clock read and insert.** `persist_mixed_suspension_batch` and the history-cap preflight resolved each `StartTimer` with its own lookup, `SELECT NOW()` and `INSERT`. A park that arms `n` timers now issues one lookup per site, one clock read and one multi-row insert.

- **Sticky routing on by default, with shutdown release (issue #1798, step 1).** `WorkerConfig::default()` now turns sticky routing on. `sticky_timeout` is `DEFAULT_STICKY_TIMEOUT` (5 s), the fallback window that Temporal uses for its sticky queue. Until the pin expires, only the worker that holds the warm cache can claim the next decision of a suspended execution.

- **Lazy audit claim-scan index (issue #1667).** An unconfigured deployment no longer pays for the audit export index.

- **Retry, re-dispatch and cron fire jitter on by default (issue #1792).** Activities that failed together used to retry together, at 1 s, 2 s, 4 s. Cron schedules all fired on the same instant. Jitter now spreads both.

- **Retry budget per activity type (issue #1793).** Without a budget, nothing limits retries in aggregate. Each task retries up to its own policy. During a dependency brownout, the retries multiply the load on the dependency when it can least absorb it.

- **Post-commit NOTIFY for appends and enqueues (issue #1796).** History appends and enqueues called `pg_notify` inside the write transaction. Postgres takes a database-wide lock at commit for such a transaction, so these commits ran one at a time.

- **`standalone-runner` has no `autumn-web` in its manifest (issue #1615).** Part of epic #1605, Tier 3. This is the acceptance test for the epic's definition of done. `examples/standalone-runner` replaces its old form, so the tree keeps one standalone reference, not two.

- **Admin token scope and authorizer hook (issue #1803).** API tokens had two scopes, `read` and `mutate`. A `mutate` token could mint and revoke tokens, so one leaked token could create others. Authorization had no tenant or shard input, and `shard_id` was not a capability.

- **Automatic load shedding by backlog age (issue #1794).** A queue can now shed new workflow starts while its backlog is old. A shed caller gets `429 Too Many Requests` with a `Retry-After` header.

- **Default activity start-to-close timeout (issue #1808).** `WorkerConfig::default()` now sets `default_activity_start_to_close` to `DEFAULT_ACTIVITY_START_TO_CLOSE` (10 minutes). Before, the field was `None`. With that value, a regular activity with no timeout of its own could hang on a live worker forever and hold a slot.

- **Workflow-task ownership guard checks `attempt` (issue #1806).** `queue::claim_still_held_for_update` now matches `attempt` as well as `crash_strikes`.

- **Resident workflow state on warm decisions (issue #1798, step 2).** A warm cache entry now keeps the suspended workflow itself, not only its events. The next decision on the same worker sends the new result to the parked handler future and polls it again.

- **Build pinning fails closed (issue #1805).** A worker with an empty `build_id` can no longer claim a run pinned to a build. The `OR $n = ''` bypass is gone from all claim queries and from `BuildCompatibilitySet::is_eligible`.

- **Liveness and readiness probes with a draining state (issue #1812).** Harvest now has `GET /health/live` and `GET /health/ready`. Both are `PublicSafe`.

- **Workflow-claim releases check `attempt` (issue #1806).** `queue::release_suspended_workflow_claim` and `queue::release_terminal_workflow_claim` now match `attempt` as well as `crash_strikes`. The first #1806 fix covered only the persist guard.

- **A published TypeScript client, and a harvest-server evaluation (issue #1616).** Part of epic #1605. Part 1 ships code. Part 2 is a design note.

- **Task-queue table hygiene (issue #1811).** The retention janitor now deletes finished `harvest_task_queue` rows. Before, they went only when their execution was deleted, and history retention is off by default. So `COMPLETED`, `FAILED` and `CANCELLED` rows stayed forever, and the table and its indexes grew without limit.

- **Bounded pool acquire and Postgres session timeouts (issue #1788).** A full pool or a stuck connection no longer stops claims and heartbeats.

- **SLO definitions and burn-rate alerts (issue #1816).** **New optional pack.** `docs/alerts/slo-pack-v0.1.0.rules.yml` defines three reference SLOs and pages on error-budget burn. It uses the multi-window, multi-burn-rate method from the Google SRE Workbook.

- **Production AES-256-GCM payload codec with a KMS key provider (issue #1825).** Harvest had a payload codec hook and key rotation, but only `IdentityCodec`. Payloads were plaintext by default, and rotation had no production codec to rotate.

- **Continuations before new starts; no work past a run deadline (issue #1824).** Two claim rules now apply by default. See [`operations/claim-order.md`](docs/operations/claim-order.md).

- **Database guard on append-only `harvest_events` (issue #1817).** A `BEFORE UPDATE` trigger on `harvest_events` now rejects history rewrites with `restrict_violation` (SQLSTATE `23001`). Before this change, only a source grep test enforced the invariant.

- **Worker drain releases its claims (issue #1813).** `WorkerConfig::default()` now sets `shutdown_timeout` to `DEFAULT_SHUTDOWN_TIMEOUT` (25 s). Before, it was 30 s, the same as the Kubernetes default `terminationGracePeriodSeconds`. The platform could then kill the pod at the moment the drain gave up.

- **Document that `AeadCodec` leaves failure text in clear (issue #1920).** `AeadCodec` encrypts the six payload fields only. Failure strings, such as `WorkflowFailed.error` and `WorkflowStarted.last_error`, stay in clear. The docs did not name this boundary.

- **One timeout scanner per shard, jittered and bounded (issue #1795).** Every replica ran the timeout checker on every shard every 500 ms, with no jitter, and its task-timeout scans had no `LIMIT`. Scan load grew with fleet size, so adding workers to clear a backlog added database load in proportion.

- **Shared deferred-admission decode for debounce/throttle/batch fire (🪞 Echo clone-class merge).** `debounce::fire_claimed_debounce_row`, `throttle::fire_claimed_throttle_row`, and `event_batch.rs`'s two flush/fire sites each decoded the same seven `DebounceStartOptions` fields — `reuse_policy`, `execution_timeout`, `sla`, `max_execution_timeout_ceiling`, `chain_execution_timeout`, `max_workflow_chain_timeout_ceiling`, `priority` — into locals, in a byte-identical block.

- **Optional per-client API rate limiting (issue #1827).** The management API can now rate-limit each client. A client over its limit gets `429 Too Many Requests` with a `Retry-After` header in whole seconds.

- **The N/N-1 rolling-deploy contract and its smoke job (issue #1828).** `docs/upgrading/README.md` states the contract. N-1 and N run together on the schema of N. A release adds expand migrations only. A contract step ships at least one minor release after the last user of the object.

- **SQLite single-writer lock, typed unsupported failures, Redis TLS (issue #1834).** **SQLite: the single-writer contract is enforced.** `SqliteRuntime::open` takes an exclusive OS lock on `<database>.lock` (`flock` / `LockFileEx`, via `fs4`). A second runtime on the same file fails fast with the new `SqliteError::DatabaseLocked`.

- **Default history event cap and byte cap (issue #1804).** Before this change, a runaway loop grew `harvest_events` and replay CPU without bound. The only default was the advisory `should_continue_as_new` at 10,000 events. The worker now caps each run's history by default.

- **Adaptive concurrency limit per activity type (issue #1836).** The slot tuner grows the worker slots when local permit waits rise. When a dependency is the bottleneck, that is the wrong direction. More calls only add latency and errors.

- **Orphan reclaim skips a locked row (issue #1876).** `poison_pill::reclaim_orphaned_tasks` no longer stalls behind a task-row lock that another session holds. A partitioned worker can keep its transaction, and its row locks, open on the server.

- **Tenant isolation with cells (issue #1837).** [ADR 0004](docs/adr/0004-tenant-isolation-cells.md) records it. Harvest supports cooperative multi-tenant deployment. The isolation unit is a cell: one reserved shard plus the worker pool assigned to it.

- **Automatic resume after a stalled cutover; duplicate-append detection (issue #1839).** A worker now settles a shard migration that stalled after its cutover. Before, only a manual `harvest shard rebalance-resume` did this. Set `ScannerConfig::rebalance_resume_enabled = false` to keep the old behavior.

- **Adaptive concurrency limit: operator guide and example (issue #1836).** The adaptive concurrency limit shipped with design notes and metrics, but without an operator guide or an example. This follow-up adds both. It changes no code in the library.

- **Retry deadlock and serialization aborts; lock-order table (issue #1822).** At the wired sites, a deadlock victim no longer fails its workflow.

- **Activity timeout retries and a deferring circuit breaker (issue #1809).** Activity timeouts were terminal, but the design doc said that start-to-close and heartbeat timeouts retry per policy. Every timeout of a `RUNNING` task also fed the circuit breaker, even when the handler never started.

- **Decision-boundary event records build and worker per decision (issue #1833).** A history now shows which build and which worker made each decision.

- **Metric-gated build-ramp abort and a `build_id` metric label (issue #1814).** A build ramp can now abort itself. The opt-in ramp guard compares the target build of a ramp with its base build at each ramp step.

- **DB-pool, query-latency and poller metrics, and a gray-failure signal (issue #1815).** Harvest reported no saturation of its database, and worker health was a heartbeat check only. A worker that was alive but sick passed that check.

- **DR fencing on by default, admin writes fenced, batched claim fenced (issue #1823).** The cross-region DR generation fence (issue #954) was opt-in per process. A process started without it could write to a demoted primary after a failover.

- **Claim cost no longer grows with backlog depth (issue #1971).** **Claim order does not change.** The claim picks the same row as before. Its cost changes. One fix changes behavior: a capped concurrency key can no longer run tasks over its cap.

### Fixed

- The PII-erasure tombstone `UPDATE` on `harvest_events` now keys on
  `workflow_exec_id` as well as the row id, so the planner prunes to a single
  partition instead of probing every partition's index. No behaviour change on
  the unpartitioned layout.

- **`enforce_external_cancels_outbox` self-deadlocked on aliased shards, hanging `Test DB (linux, shard 6)` for 6h on every CI run (issues #1350, #1357).** `Test DB (linux, shard 6)` never completed — it burned a full 6-hour GitHub Actions job on every run, on `trunk-dev` and on every PR, deterministically hanging at the same test every time: `shard_placement_by_id_tests::outbox_by_id_resolves_when_the_caller_holds_the_higher_aliased_shard`.

- **Pin `tinyvec` in the `harvest new` scaffold to avoid a build-breaking upstream regression (issue #1359).** `harvest new` scaffolds a project with a fresh `Cargo.toml` and no committed `Cargo.lock`, so the first `cargo build` a new user runs resolves whatever's newest on crates.io.

- **Fix sqs feature's legacy TLS/HTTP stack pull (PR #1393).** `deny.toml` (from the dependency-ledger harness, PR #1303) deferred four RUSTSEC advisories — RUSTSEC-2026-0098/0099/0104 (rustls-webpki 0.101.7 certificate-parsing bugs) and the h2@0.3.27 half of RUSTSEC-2026-0258 (unbounded empty DATA frames) — as reachable via the optional `sqs` feature but stuck pending an `aws-config`/`aws-sdk-sqs` manifest bump.

- **`timer_overdue` false positive under saturated workflow dispatch slots (issue #1191).** `stall_diagnosis::workflow_wake_was_missed` gated the `timer_overdue` verdict on the run's own workflow-task row being `PENDING` with `scheduled_at` aged past a 60-second grace window (`TIMER_OVERDUE_GRACE_SECONDS`), on the assumption that only `persist_started_timer` (via `queue::reschedule_task`) re-pends a row to that exact shape.

- **Shard-rebalance seal integrity, legal-hold cutover race, and successor minting (issue #1317, PR #1406).** Follow-up to #964 and PR #1305, addressing the highest-severity findings from that PR's round 6-12 review: several ways the shard-rebalance feature could strand or destroy the one forwarding seal an execution's id resolves through, plus several call sites that derived a shard from an `ExecutionId`'s origin bits instead of the row's current residence.

- **Local-activity batch silently dropped co-batched bookkeeping commands (issue #1247).** `extract_run_local_activity` (`autumn-harvest/src/worker.rs`) walked a drained `RunLocalActivity` command batch with a `match` that handled only four kinds (`RecordMarker`, `RecordSideEffect`, `SpawnDetachedChildWorkflow`, `RunLocalActivity`) and ended in a `_ => {}` catch-all.

- **A redriven run re-dispatches past its failing cycle's trailing records (issue #1262).** , composing only existing events — **zero new `WorkflowEvent` variants, no migration, no contract change.**

- **Audit-export unobservable-shard gauge (issue #1268).** Follow-up from the review of #1261 (issue #953). `harvest.audit.export_lag` is written only on a successful observation, so a shard the exporter cannot observe — a connection it cannot acquire, a failing cursor read, a failing lag query — kept serving its last value, commonly `0`.

- **`harvest.audit.export_lag` no longer under-reports on commit-order skew (issue #1271).** **Bug fix. No new `WorkflowEvent` variant, no replay impact.** One migration: widens an existing partial index; no new table, no new column.

- **Audit export runs on its own task (issue #1269).** Follow-up from the review of #1261 (issue #953). `fire_due_audit_exports` ran inline inside `timeout::enforce_timeouts_once`, sharing that loop's connection and cadence with timeout enforcement, SLA checks, session cleanup, and codec rotation.

- **One unsupported-command execution head-of-line-blocked unrelated executions in `poll_once`/`run_until_idle` (issue #1530).** `SqliteRuntime::poll_once`/`poll_once_as_of` drove `store::running_executions` — every `RUNNING` execution, in ascending `ExecutionId` order — with `self.drive_one_cycle(exec, now, &failure_now).await?`. The bare `?` propagated the first per-execution error and aborted the rest of that pass, so any execution ordered after a permanently-erroring one (an unsupported command such as `SignalExternalWorkflow`, but any `drive_one_cycle` error applies) never got a decision cycle — not once, forever, even after a restart, since the broken execution never reaches a …

- **`run_until_idle` capped fleet convergence at one decision cycle per call in the presence of a persistently-broken execution (issue #1555).** `run_until_idle`'s outer loop called `self.poll_once().await?`. The bare `?` propagated ANY per-pass error immediately, so a single call returned after exactly one internal `poll_once` pass whenever the fleet contained an execution that errors every cycle (e.g. an unsupported command through ordinary dynamic control flow, same shape as #1530).

- **A tokenless dev-session record no longer reaps a live pid (issue #1295).** `postmaster_is_the_recorded_one` fell back to plain liveness when a session record carried no postmaster start token: any live process at the recorded pid counted as a match.

- **Three narrow dev-runtime robustness gaps closed (issue #1299).** Follow-up to #525 / #1281, filed after that PR's review budget was spent. Three small, independent hardening gaps in the same module.

- **A keyed by-id signal could double-deliver across a shard-crossing continue-as-new or restart (issue #1318).** `signal_external_workflow_by_id_with_idempotency`'s exactly-once promise held per execution, not per business key. `resolve_and_signal_by_workflow_id`'s idempotency pre-check (`execution::lookup_idempotent_signal_dedupe`) already recognised a key reused against a same-shard predecessor — issue #751's PR #1145 closed that route.

- **The by-id fan-out could mislabel a rebalanced caller's own shard (issue #1324).** `timeout::resolve_delivery_route` already read the caller's real shard off the held connection (`caller_shard`, the issue #964 fix): a query against `harvest_workflow_executions.shard_id` on `conn` itself, not a decode of the caller's `ExecutionId` bits.

- **Workflow-pause post-claim re-check (issue #1640).** `apply_post_claim_rechecks` (`queue.rs`) is the shared, post-claim authoritative re-check both `claim_task_on_shard` and `claim_task_batched` call after a claim's own `UPDATE` commits its snapshot. It already re-checks queue pause (`queue_pause`, issue #619) and activity pause (`activity_pause`, issue #807), but not workflow-execution pause.

- **Durable workflow terminal metrics survive post-commit cancellation (issue #1348).** `process_workflow_task`'s `Persisted` arm computes this cycle's terminal metrics (`harvest.workflow.duration`, `.history_size`, `.continue_as_new`, `harvest.workflow.terminal`, canary metrics) before the persist transaction, then emits them only after that transaction commits (issue #1184) — so a rolled-back attempt cannot double-count.

- **`audit::list_audit`'s `before` cursor keeps tied rows (issue #1408).** `list_audit` (`autumn-harvest/src/audit.rs`) ordered strictly by `occurred_at DESC` and filtered `before` with `occurred_at.lt(before)` — a single-column, exclusive cursor with no tiebreaker. A caller paging `GET /admin/audit` by setting each request's `before` to the last row's `occurred_at` permanently dropped any other row sharing that exact timestamp.

- **`backup verify` text output names both causes when unreadable and skipped-no-handler samples mix (issue #1410).** **Text-output wording fix only** (implemented). `format_backup_verify_text` in `autumn-harvest-cli/src/lib.rs` had one branch for "every sampled history was unreadable" that fired whenever `unreadable > 0` and nothing replayed, regardless of `skipped_no_handler`.

- **Backup verify adjudicates cross-shard completion-trigger fires (issue #1401).** `backup_verify.rs` had zero references to `harvest_completion_trigger_fires` or `harvest_completion_trigger_outbox`. `completion_trigger.rs`'s cross-shard relay (`relay_gate_checked_start`) commits the source shard's outbox delete and the target shard's start in two independent transactions; a restore that skews the two shards can leave the source confirming a delivery the target never received, with no scanner and, until now, no drill check to catch it.

- **Child placement and SSE resume cursor use residence, not origin (issue #1405).** Follow-up to #964/#1317/#1596. That prior work fixed the residence-routing bug class (deriving a shard from `ExecutionId`'s origin-encoded bits instead of a row's live residence after a shard-rebalance migration) at `handle.rs` (`WorkflowHandle`'s cancel/terminate/result paths), `completion_trigger.rs`, and ten sites across `api.rs`/`mcp_tools.rs` (SSE listeners and audit attribution).

- **SQLite: `ExecutionNotFound` for an unknown id on every accessor (issue #1735).** `SqliteError::ExecutionNotFound` means an unknown id. Before this change, only `outcome` and `send_signal` returned it. For an unknown `ExecutionId`:

- **Shard health names unhealthy assigned pollers (issue #1463).** The `no_live_worker` blocking reason said "no live worker assigned to this shard polls queue(s)" when an assigned worker did poll the queue but was stale, unhealthy, or draining.

- **Worker heartbeat aborts on external cancellation (issue #1552).** `Worker::run_with_listener` and the multi-shard runner held each heartbeat `JoinHandle` as a bare local. Cancelling the `Worker::run()` future from outside, for example with `JoinHandle::abort()`, dropped the handle.

- **Reaper checks the process table before its startup grace period (issue #1585).** The dev reaper skipped a session with no known postmaster pid for 15 seconds after `created_at`. That deadline is wall-clock time. A system suspend freezes a starting postmaster but not the clock.

- **Shared test database uses the full migration bundle (issue #1693).** `quota_enforcement_tests::completion_trigger_defers_to_outbox_when_target_quota_exceeded` timed out on CI. The source workflow stayed `RUNNING` and its task row stayed queued. The task looked unclaimed, but the claim path was correct.

- **Directory-privacy checks and filesystem ACLs (issue #1548).** The dev runtime checks POSIX mode bits to decide that no other user can write to the cache root, its ancestors, and the session root.

- **Schedule health end_at check skips jitter when slot is unknown (issue #1568).** The Vantage schedule health check judged `end_at` against the jitter-adjusted `next_run_at`. The scheduler hashes jitter against a different slot in two cases. A calendar can rebase the slot first.

- **The worker path ND-blocks a cycle that skips recorded commands (issue #1791).** Only the replay paths (strict, canary, query and debugger) checked for unconsumed recorded history at the end of a cycle. `drive_workflow`, the production worker path, did not.

- **Raw SQL derives terminal-state lists from the constants (issue #1546).** Sixteen raw-SQL state lists were copied by hand. Twelve of them listed six terminal states. `erase::TERMINAL_STATES` now holds a seventh state, `MIGRATED` (issue #964), so those copies are not the same rule as the constant.

- **Re-drive a workflow task on a local activity event-id conflict (issue #1787).** **Local activity append conflict.** A local activity append that loses its `event_id` to a concurrent append now re-drives the workflow task. Before, it failed the run.

- **Wire five resilience DB suites into CI and lock the allowlist (issue #1799).** Five DB suites from issue #1799 now have `linux` manifest rows: `scheduler_ha_tests`, `poison_pill_tests`, `signal_tests`, `replayer_integration_tests` (with `testing`), and the plugin suite `erase_payloads_integration`. Before this change, CI compiled them but never ran them.

- **Quota reconcile scan reads rows per registered name (issue #1631).** The `quota_key` reconcile sweep fetches up to `batch_size` candidate rows per tick. The old query filtered `workflow_name = ANY($1)` after an `(id)` index scan. Postgres read every unrelated `quota_key IS NULL` row to find the matches.

- **One shared quota-lock ordering for debounce and throttle (issue #1752).** The `debounce` and `throttle` scanners each held a copy of the same five functions. They order a claimed batch by advisory-lock id, so two scanner transactions cannot form an ABBA deadlock (issue #1230 Finding 2).

- **Heartbeat stamps use the database clock (issue #1807).** `record_heartbeat` wrote `last_heartbeat_at` from the worker host clock. The heartbeat-timeout scan compares that column with the database `NOW()`. A host 60 seconds behind the database stamped a heartbeat that was already too old.

- **Buffered-schedule drain takes the fire claim (issue #1820).** `drain_buffered_schedule_runs` serves the `BufferOne` and `BufferAll` overlap policies. It took no claim. Every replica runs the scheduler, so two replicas could drain one buffered slot.

- **No panics on scanner, `Drop` and request paths (issue #1821).** A panic on a path that every replica runs can disable that path across the fleet. One bad row stops the same scanner loop on every replica.

- **The capability-miss release checks attempt (issue #1917).** `release_task_for_capability_miss` guarded on `worker_id` and `crash_strikes`. A workflow cycle reaches this release after its handler starts. The stuck-running requeue keeps `crash_strikes`, and the same worker can win the row again.

- **Tracked artifacts and doc drift (issue #1831).** **Artifacts removed.** `test_debug`, `test_keys`, their sources and `autumn-harvest/src/analyzer.rs.orig` are gone. `.gitignore` now ignores `*.orig` and `*.rej`.

- **A late heartbeat no longer quarantines live work (issue #1879).** **Bug.** The worker heartbeat slept one interval, then ran its tick. The period was therefore the interval plus the tick latency. The stale window is two intervals, rounded up to whole seconds.

- **Wire the remaining DB suites into CI and track the rest (issue #1799).** 49 more DB suites now have `linux` manifest rows: 27 core suites and 22 plugin suites. Before this change, CI compiled them but never ran them.

- **Callback 4xx and Retry-After, spread DLQ redrive, CLI timeouts (issue #1832).** Three fixes from the September 2026 resilience gap analysis (epic #1786).

- **No timeout retry after the run ends (issue #1870).** #1809 made a `StartToClose` or `Heartbeat` timeout retry per the retry policy. That fixes the bug of issue #1870. This change closes one gap in it.

- **Business-day scan bound and skip-scan date limits (issue #1968).** Three defects in `calendar.rs`.

- **Reject a sub-second interval schedule (issue #1967).** `validate_schedule` accepted any non-zero `Schedule::Interval`. The stored form is `interval:<whole seconds>`, so the scheduler dropped the fraction. Each tick reads the stored form back. A 500 ms schedule became `interval:0`, fired once, and then stopped.

- **Rate-limit claimed API tokens before the token lookup (issue #1827).** The API rate limiter ran inside the token layer. The token layer looks up every `hvst_` bearer in the database first. A made-up token got `401` from that lookup and never reached the limiter.

### Security

- **Mutating routes fail closed outside `dev` (issue #1802).** Outside the `dev` profile, a management API with no declared auth boundary now refuses every mutating route with `401`. Before, only admin-gated routes did. Workflow start, signal and update (with the with-start variants), reset, DAG trigger, patch and retry, schedule changes, external-activity callbacks, worker drain, Vantage form posts and MCP tool mutations were open to any caller.

- **Audit hash chain, signed WASM modules, OTel Collector recipe (issue #1838).** ADR 0004 (`docs/adr/0004-security-extras.md`) records each decision. All three features are opt-in. A deployment that sets nothing sees no change in runtime behavior or wire bytes.

### Performance

- **Index both sides of the external signal/cancel/await outbox scans, and pin their plan (issue #1486).** `timeout::enforce_timeouts_once` runs three sibling outbox scanners on every worker's periodic tick: `enforce_external_signals_outbox`, `enforce_external_cancels_outbox` and `enforce_external_awaits_outbox`. Each drains its own outbox of pending cross-workflow `ctx.signal()` / `ctx.cancel()` / `ctx.await_external()` requests with the same query — claim one candidate row under `LIMIT 1 ...

- **`decode_event` skips serde `Content` buffering (issue #1721).** `WorkflowEvent` is adjacently tagged. `serde_json::Map` sorts keys, so `data` comes before `type`. Serde then buffers each event in `Content` before it chooses a variant. That runs on every history read and replay load.

- **Workflow input shared across the start path (issue #1733).** The start path holds the workflow `input` in four owned structs. These are `StartWorkflowParams`, `NewWorkflowExecution`, `EnqueueParams` and `NewTaskQueueItem`. Each struct deep-copies the JSON. The start transaction also clones the whole request and the enqueue params on entry.

- **Timeline AccKey hashing (PR #1894).** `derive_timeline` hashes its `AccKey` lookup keys with a manual `Hash` impl that writes one 128-bit value, instead of three `SipHash` writes. Equality and the keyed `RandomState` hasher are unchanged.

- **Perf:** `info::validate_node` looks up `required`, `properties`, `items` and `additionalProperties` only when the value has the matching kind. Instructions on `schema_validate_profile` drop 18.8% (312.5M to 253.8M, callgrind). Behavior is unchanged; the 50 `info::` unit tests pass. See `docs/performance-schema-validate-kind-gated-lookups.md`.**

### Documentation

- **Restore the engine reference as `docs/architecture.md` (PR #1285).** **Docs-only** (implemented). `562c781` ("fix: claude.md") reduced `CLAUDE.md` to workflow instructions — correctly, since `CLAUDE.md` is agent instructions and not a place for project documentation to accumulate.

- **Correct `docs/performance.md`'s claim-plan attribution: any residual predicate defeats sort-elision, not just the sticky CASE (issue #1177).** `docs/performance.md` (issue #786) measured `claim_task`'s superlinear backlog scaling and attributed it to one specific mechanism: the claim query's `ORDER BY` leading with a non-indexable sticky-routing `CASE` expression, which `idx_harvest_tq_poll` cannot serve.

- **Pause-array-size claim cost (issue #1215).** `docs/performance.md`'s "Known limitations" section listed five claim-path predicates the attribution table does not measure (capability labels, queue pauses, `schedule_to_close`, worker sessions, sticky routing). A sixth — the per-activity-type pause (#807) — was not on that list at all, and the queue-pause fix's own "resolved" entry (#619) had been measured against exactly one active pause, never a realistically wide pause table.

- **Chapter 5 and 6 flagship snippets compile.** Docs-only. Chapter 5's `spawn_child_workflow_raw` call passed a workflow id the method does not take (E0061). Chapter 6's `charge_card` used `.map_err(|e| e.to_string())?` in a `HarvestResult` function, and `HarvestError` has no `From<String>` (E0277).

- **Calendars.md snippets compile under CI (issue #1783).** The "Programmatic Helpers" example in `docs/calendars.md` called `apply_skip_policy` with three arguments. The function takes four. Issue #1772 fixed the call. Nothing guarded it, so the example could drift again.

- **`docs/embedding.md` and a getting-started fork for plain Axum (issue #1614).** `docs/embedding.md` is the reference for the standalone path. It covers what the runtime needs, what the router gives you, and what you own instead of `HarvestPlugin`.

### Testing

- **Loom and Shuttle model checking on every PR (issue #1800).** **CI.** The loom models used to run only on manual dispatch (`.github/workflows/loom.yml`), and had never run. `ci.yml` now has two jobs, `loom` ("Loom models") and `shuttle` ("Shuttle models").

- **TLA+ models of the core protocols and Kani proofs (issue #1819).** **TLA+ models.** `formal/tla/` holds three models. The new `formal-models` CI job checks each one through `scripts/check-formal-models.sh`. The runner reads `formal/tla/models.txt` and pins `tla2tools.jar` 1.7.4 by SHA-256.

- **Stateful lifecycle model and crash-history checks (issue #1829).** Two new test layers check client-visible guarantees. Neither changes engine code. There is no new `WorkflowEvent` variant and no migration.

- **Pin the nightly lifecycle counterexamples (issue #1923).** The first deep nightly run failed `lifecycle_matches_the_reference_model` in all 8 shards. That run tested `e22a53bb` (PR #1909, issue #1824). #1824 made a new start sort 30 seconds later at claim.

- **The chaos append test expects the #1871 fix (issue #1925).** No scheduled `chaos.yml` run passed for 48 h, so the watchdog opened #1925. Two runs failed:

- **Deterministic simulation of the activity claim protocol (issue #1830).** [ADR 0004](docs/adr/0004-deterministic-simulation-testing.md) chooses option (b): an in-memory oracle store, a seeded single-thread simulation, and a differential test against Postgres. A failing seed replays on a laptop with one command.

- **Nightly fuzzing and a structure-aware replay target (issue #1835).** Fuzzing now runs every night with a persisted corpus. A new target fuzzes the replayer with structured histories. There is no new `WorkflowEvent` variant, no migration and no change to engine behavior.

### Internal

- **Comment-hygiene CI harness and a corpus audit.** **Tooling + comment-only source changes** (implemented). Adds `docs/audits/comment-hygiene.py`, wired into CI's ungated `lint` job alongside the other Folio corpus harnesses, and fixes every Tier A defect it found across the 785 `*.rs` files (822k lines, 175k comment lines).

- **Contract audit reads raw-byte bodies and typed queries (issue #1411).** This change closes the two audit blind spots left open in #1411. The contract fixes shipped in #1690 and #1699. Now those defect classes fail CI.

- **`StartWorkflowParams::new` replaces 40-field literals (issue #1448).** `StartWorkflowParams` has 40 fields. Each production start site wrote all of them, so every new field touched up to 12 files.

- **Pin the workspace toolchain to Rust 1.99.0.** `rust-toolchain.toml` pins every build, format and lint run to Rust 1.99.0 with the `rustfmt` and `clippy` components. CI's `dtolnay/rust-toolchain@stable` steps run `rustup default`, and rustup gives the file precedence over that default, so all stable jobs now compile with the pinned release.

- **One shared workflow-task backoff requeue (issue #1751).** Three functions in `autumn-harvest/src/queue.rs` re-pend a claimed workflow task with a Postgres-clock backoff: `requeue_workflow_task_nd_blocked`, `requeue_workflow_task_after_panic`, and `requeue_workflow_task_for_quota_retry`. Each held a hand-copied `UPDATE`. Four earlier fixes patched one copy and missed another (issues #1389, #1391, #1589, #1402).

- **Repair chaos.yml and guard workflow YAML (Semaphore).** `chaos.yml` was invalid YAML: two `run:` scalars held `chaos::` followed by a space, which YAML reads as a mapping value. GitHub ran the file as a zero-job failed run on every push and never fired the nightly cron.

- **One shared handler-dispatch builder (issue #1632).** The `#[query]`, `#[update]`, `#[workflow]`, and `#[activity]` macros each copied the same block. The block decodes the handler arguments by arity (0, 1, or many), calls the handler, and encodes the result as JSON.

- **Online-migration lock-safety lint (issue #1810).** Safe online migrations were a convention, not a check. Only three migrations set `lock_timeout`. `20260915231809` drops and rebuilds a unique index on `harvest_workflow_executions` inside its transaction, and no test noticed.

- **Supply chain: advisory scan, SHA pins, Dependabot, signed releases (issue #1826).** **Daily advisory scan.** `.github/workflows/advisory-scan.yml` runs `cargo deny check advisories` every day. `.github/ci/advisory-scan.sh` opens an alert issue for a finding, comments on the open one, and closes it after a clean scan.

The full entry for each change is in [`docs/shipped-work.md`](docs/shipped-work.md).

## [0.6.0] - 2026-08-26

### Upgraded

- **Upgrade to autumn-web 0.7 and hand migration ownership to Autumn** — the defining change of this release. `HarvestPlugin` no longer applies its own embedded migration sets from its `on_startup` hook; it **registers** them with the framework via autumn-web 0.7's `AppBuilder::plugin_migrations`, and Autumn applies them during `setup_database` — which runs before any startup hook, so the Harvest runtime still always boots against an already-migrated schema. The motivation is cross-plugin correctness: Diesel's `__diesel_schema_migrations` is keyed by version *alone*, with no notion of which source recorded it, so two independently authored migrations that reuse a version (two plugins each picking an all-zero placeholder for their first migration, say) would silently skip one of them forever — the losing `up.sql` never runs, yet the version reads as applied. Autumn now sees every registered set at once, tracks the loser under a substitute version derived from that migration's own full name so **both** still apply, and logs the substitution at `INFO`. Registration is mode-conditional: the workflow-start outbox set always targets the application database and is always registered, while the two Harvest sets are registered only under the default `harvest.mode = "embedded"` (where the application database *is* the Harvest database). Under `split`/`external` the Harvest sets still go through the plugin's own apply path against `harvest.database.url`, because `plugin_migrations` can only reach the application's primary database — same sets, same databases as 0.5.0, only a different component applying them. If Harvest configuration cannot be loaded, `HarvestPlugin::build` now fails fast while resolving the migration topology instead of registering an incomplete plan or deferring the error to startup; this also makes migration-only commands fail rather than report false success. **No embedder action is required in the common case** — mounting the plugin is enough — and the dev/prod policy is unchanged (`dev` applies automatically; every other profile warns and defers to `autumn migrate`). Two source-visible consequences: `autumn_web::Route` gained a `seo` field (generated MCP-tool and webhook routes now pass `SeoRouteDefaults::EMPTY`, matching what a route attribute without `seo(...)` produces), and the workspace `rusqlite` pin moves 0.39 → **0.40**, which is mandatory rather than cosmetic — autumn-web 0.7 depends on `libsqlite3-sys` 0.38 while rusqlite 0.39 pins 0.37, and since `libsqlite3-sys` declares `links = "sqlite3"` Cargo refuses to resolve a graph containing both, even with the `sqlite` feature disabled. MSRV is unchanged at 1.88.0. See [`docs/upgrading/0.6.0.md`](docs/upgrading/0.6.0.md) for the full guide. No new `WorkflowEvent` variant, no new migration.

### Added

- Add broker event-source connectors — Kafka and SQS as workflow triggers (issue #944). A Kafka topic or SQS queue can now trigger a durable workflow with idempotent redelivery, correct ack ordering, poison isolation and backpressure, none of which the embedder writes. **The core engine gains zero broker dependencies** — the connector lives in `autumn-harvest-plugin` behind the `connectors`/`kafka`/`sqs` Cargo features, and `connector_dependency_graph.rs` shells out to `cargo tree` to fail the build if `rdkafka`/`aws-sdk-sqs` ever reaches the engine's graph (with a falsifiability test proving the guard genuinely sees them when present). Declarative `SourceBinding::starts`/`::signals_with_start` mirror `WebhookTriggerInfo`'s shape (synchronous mapping fn, `MappingError::Deserialize` vs `::Rejected`); `HarvestPlugin::connector(binding, source)` wires it and `build` fails fast on an unregistered/DAG target, a duplicate binding name, or an adapter/binding stream mismatch. Idempotency derives from stable broker coordinates (Kafka `topic:partition:offset`, SQS `MessageId`) namespaced per binding and fed to the #808 `idempotency_key` machinery through #699's injective `L{len}:{value}`/`H{64hex}` encoding; a deferred-admission target falls back to `WorkflowId` dedupe (forcing the incompatible combination is a build-time panic). Ack happens only after the dispatch durably committed, was an idempotent replay, or was throttle-deferred; Kafka commits only the contiguous completed prefix via `OffsetTracker`, distinguishing in-flight offsets from never-delivered holes (transaction control records, aborted-transaction filtering, compaction). `max_in_flight` (default 16) bounds concurrency; deterministic failures dead-letter immediately while a mapping rejection retries to `poison_threshold` (default 3) and a transient harvest failure never dead-letters, routed either to the plugin-owned `harvest_connector_dead_letters` table (migration `20260719900000`) or the broker's own redrive via the distinct `abandon` vs `nack_for_dead_letter` verbs. Four bounded-label metrics `harvest.connector.received`/`dispatched`/`poisoned`/`lag`, plus the additive `StartSource::Broker` provenance variant. Partition ordering is deliberately not preserved (the entity pattern is the per-key answer). Both halves of the success metric are falsifiable in CI: a 17-line measured wiring budget and a 10,500-delivery soak settling in 21.6 s with exactly 9,990 executions, zero duplicates and 10 dead letters. No new `WorkflowEvent` variant, no core migration.

- Add durable per-execution workflow logs (issue #790). Opt-in `WorkflowLogPolicy` gives a workflow body a durable, per-execution log stream surfaced by the new `GET /workflows/{id}/logs` route and a Vantage Logs panel — the answer to "what did this run actually print?" that `tracing` output scattered across worker pods cannot give. Log lines ride a bookkeeping `WorkflowCommand::RecordLog { seq, level, message }`; the call-ordinal `seq` plus `UNIQUE(workflow_exec_id, seq)` and `ON CONFLICT DO NOTHING` make emission at-most-once across replays, a SAVEPOINT warn-and-swallow means a logging failure can never fail the workflow, and a drop-newest byte cap ends with a terminal truncation marker at `seq = i64::MAX` so a capped stream is never silently short. `ON DELETE CASCADE` ties retention to the owning execution. No new `WorkflowEvent` variant, one additive migration `20260719000000_harvest_workflow_logs`.

- Add transactional workflow start — enqueue a run in the caller's own transaction (issue #763). `WorkflowHandleClient::start_workflow_transactional(conn, …) -> TransactionalStartOutcome` (with `TransactionalStartOptions`) lets an embedder atomically commit its own domain write and the workflow start together, closing the classic dual-write window where a row lands but its workflow never starts (or vice versa). Atomicity comes from diesel-async 0.9.2's nested SAVEPOINT semantics, so the start participates in whatever transaction the caller already holds. New `StartProducer::Transactional` (admission-gated with `GateMode::CheckCached`) and `StartSource::Transactional` provenance; `WorkflowInfo` now derives `Clone`. Proven by a 500-crash-point fault-injection test yielding zero orphaned rows, plus eight rounds of Codex P2 hardening. No new `WorkflowEvent` variant, no migration.

- Add capability-miss release for a capable peer (issue #804). A worker that claims a task whose handler it does not register no longer fails the execution terminally — a self-inflicted outage mid-rolling-deploy, since `SKIP LOCKED` hands old pods the new build's work — but **releases the claim back to `PENDING`** for a capable peer, with capped-exponential backoff, bounded by a durable redelivery budget, and observable. Claim-time filtering is structurally impossible (a worker can enumerate the handlers it has, never the ones it lacks), so release-then-escalate is the only in-scope design. All four handler-lookup sites now raise the typed `HarvestError::HandlerNotRegistered { kind, name }` and fail nothing; `fail_execution_on_error` passes that one variant through un-failed so it lands in a single interception point in `process_task`. The ownership-guarded release undoes `claim_task`'s attempt increment, never increments `crash_strikes`, clears sticky routing, leaves `error` untouched (so `/stack` never renders a blameless deploy skew as a `last_failure`), and clears the `mixed_signal_suspension` sentinel on workflow rows only. The budget is consumed per **distinct worker** and tracked in a new additive `capability_misses` column (migration `20260720000000_harvest_task_capability_misses`), reset only on paths that *prove* the claiming worker was capable. At the bound, escalation reuses the ordinary terminal path with a stable greppable `no_capable_worker:` reason in four cause-specific variants resolved through an exhaustive `EscalationCause` enum, and the new `harvest.task.capability_miss{queue, task_type, outcome}` counter splits `released`/`escalated`/`escalated_never_offered` so three separate starter-pack rules can page, ticket and hold correctly. `WorkerConfig::capability_miss_max_redeliveries` (default 5; `0` restores the pre-#804 fail-fast). AC4 independence from poison pill (#367) and the workflow-task timeout (#494) holds across four layers, in both directions. No new `WorkflowEvent` variant, no event-JSON change, and a release appends nothing to `harvest_events`.

- Add scanner liveness heartbeats to detect a wedged control loop (issue #797). Harvest's background control loops (timeout enforcement, SLA, poison-pill reclaim, external outboxes, retention, schedule ticker, pause auto-resume) run as bare spawned Tokio tasks and previously failed **silently** — every pre-existing loop metric emits only when there is work, so a healthy idle loop and a wedged one both emit zero. New core module `scanner_health.rs` exposes one call, `record_scanner_tick(metrics, owner)`, driving both an unconditional `harvest.scanner.tick` counter (emitted on every iteration, including no-work passes, enforcement errors and failed connection checkouts, so a flat-lined counter *is* the wedge signal) and an in-process last-tick registry whose staleness verdict feeds a new `scanner_liveness` check in `GET /admin/preflight`. The `scanner` label is a closed `Scanner` enum so cardinality is bounded by construction; the SLA and external-outbox responsibilities are ticked by the timeout loop they live in, sharing one liveness fate rather than being skipped behind a `?` on a transient DB error. `staleness_threshold = max(2 × poll_interval, 60s)` with `Healthy`/`Stale`/`Wedged` classification; the 60 s floor is deliberate because a loop's iteration period is not its poll interval and a false `warn` breaks a deploy pipeline. Register-on-spawn supplies the expected scanner set, the boot grace window and the per-scanner threshold in one mechanism. Two additive no-op-default `MetricsRecorder` methods. No new `WorkflowEvent` variant, no `harvest_events` footprint, no migration, no replay impact — the registry is in-process only.

- Add task-queue pause/resume to hold dispatch during a downstream outage (issue #619). New `queue_pause.rs` plus three admin routes let an operator freeze and thaw an entire task queue while a downstream provider is down, without cancelling work or editing config. Enforcement is a claim-layer anti-join (not a cache), so a pause takes effect immediately and cannot go stale; a qualified-outer-column bug is pinned by test. A hold never fails a task: `schedule_to_start` enforcement is suspended for the pause duration and resume shifts `scheduled_at` forward accordingly, while `schedule_to_close` is deliberately *not* suspended (a cross-retry wall-clock deadline must still mean what it says). New `harvest.queue.paused` gauge, a Vantage Workers banner, and a `queue_paused` reason code in the eligibility explainer. All four queue-lock transactions pin `READ COMMITTED` (a `repeatable read` session default would otherwise let a resume report a cheerful no-op while the queue stays frozen). No new `WorkflowEvent` variant; migration `20260715000000_harvest_queue_pause`.

- Add per-activity-type pause/resume for surgical incident containment (issue #807). `POST /activities/{name}/pause|resume` and `GET /activities` hold a single activity type's dispatch while the rest of the queue keeps draining — the narrower sibling of the #619 queue pause. Enforcement is the same claim-layer anti-join, scoped by `task_type = 'activity'` so the `mixed_signal_suspension` sentinel on workflow rows can never be mistaken for an activity name. Resume uses a two-pass credit so a paused-then-resumed activity is not charged for the hold, `effective_scope` reports which queues a pause actually covers, and a partial cross-shard write returns `207`. No new `WorkflowEvent` variant, no replay impact, one additive migration `20260722000000_harvest_activity_pause`.

- Add latest-wins concurrency (`on_conflict = "cancel_running"`, issue #811). Per-key concurrency (#247) had one overflow behaviour — defer — which is exactly backwards for an idempotent recompute (reindex a document, recompute a report, re-run a per-tenant sync) where the in-flight run is stale the moment a newer one is requested. The new `ConcurrencyOnConflict` axis makes a key latest-wins: admitting a run cancels the oldest in-flight run(s) for the same key, inside the start transaction after the fresh insert and under the shard-local advisory lock, so the later-admitted run deterministically wins. Superseded runs reach `CANCELLED` through ordinary cooperative cancellation (Saga compensation and `ParentClosePolicy` fire normally) — it is not a force-terminate. One admission sheds at most `SUPERSEDE_SCAN_LIMIT` (32) runs, so latest-wins stays a per-key fair-share control rather than a bulk-cancel tool; workflow-level retry (#523) and the backfill/outbox/manual paths explicitly stay `Defer`. New `harvest.concurrency.superseded{workflow}` counter and an `on_conflict` column on `GET /admin/concurrency`. A documented ABBA lock-ordering hazard exists when combined with `ctx.mutex` (#691) — Postgres detects it, the start aborts atomically and is safe to retry. Shard-local, exactly like the cap it extends. No new `WorkflowEvent` variant, no migration.

- Add cross-type continue-as-new for multi-phase entity workflows (issue #803). `ctx.continue_as_new_as::<I>(&paid_subscription_info(), input)` (typed) and `continue_as_new_as_type(name, value)` (untyped) continue the *same logical entity* as a *different registered workflow type*, so a `trial → paid → churned` lifecycle no longer forces either one ever-branching monolith or a broken `workflow_id` that `signal_with_start`/`update_with_start` depend on. The successor keeps `workflow_id`, shard and queue and starts with clean history; `execution_timeout`/`sla`/concurrency/owner/runbook/severity and the workflow-level retry policy are **re-resolved from the target type** (clamped by the fleet-wide ceilings), while the #617 chain lifetime cap is carried **verbatim** so a type change is not an escape hatch from a runaway-loop budget. Presence decides: a plain `ctx.continue_as_new` takes the byte-identical legacy path. Four fleet-invariant rejections fail the predecessor terminally with an operator message and create no successor (blank target, a registered unified DAG, a live run already holding the target key, and a shard-routing change), while an *unregistered* target is treated as a #804 capability miss and released for a capable peer. The addressing consequence — harvest's active-run identity is the `(workflow_name, workflow_id)` pair, so external callers must name the current phase type — is documented, along with the schedule-overlap and handler-removal-reachability caveats. Rides the existing `WorkflowContinuedAsNew` event via one additive optional `new_workflow_type` field, compared *before* the input by `HistoryMatcher::match_continue_as_new` so a code change that redirects an already-recorded transition surfaces as `NonDeterminismKind::ContinueAsNewMismatch`. No new `WorkflowEvent` variant, no migration.

- Add business-day durable timers (issue #806). `ctx.timer_business_days(timer_id, n, calendar)` resolves after `n` business days per a named calendar, stepping over weekends and holidays — the one-call answer to "escalate after 2 business days", which `ctx.timer(id, 2 * 86_400)` gets wrong the moment the window straddles a weekend. A non-suspending sibling `ctx.business_days_from_now` freezes the same deadline without arming a timer. The resolved deadline is frozen into the **existing** `SideEffectRecorded { kind: Custom }` variant (#384) and the wait rides the **existing** `TimerStarted`, with freeze and arm sharing one suspension batch, so arming costs one decision cycle. The calendar is read once, ever: replay returns the recorded value verbatim, so an operator adding a holiday can never move an already-armed timer's deadline. Calendars register via `HarvestBuilder::state(BusinessCalendars::…)`; weekends are always non-business days whatever the calendar is named, business days are counted on UTC dates preserving the anchor's time-of-day, and `n = 0` rolls forward rather than meaning "one business day later". A declared coverage horizon rejects a resolution beyond it rather than silently answering weekends-only, and the error taxonomy is split into *prologue* errors (recorded zero commands, fixable by redeploy) and *frozen* errors (recorded and replayed forever, recoverable only by reset) so calendar availability — worker-local deployment state — cannot diverge a run. No new `WorkflowEvent` variant, no migration.

- Add activity run metadata and deadline accessors via `ActivityContext::info()` (issue #783). The activity-side mirror of the workflow-side `ctx.info()` (#698): a read-only, zero-footprint `ActivityExecutionInfo` snapshot of who owns this attempt (`execution_id`, `workflow_id`, `workflow_type`, `activity_type`, `activity_id`, `attempt`, `max_attempts`, `is_local`, `queue_name`, `task_id`) plus three deadline accessors — `deadline()`, `time_remaining()` (saturating at zero) and `is_expiring_within(reserve)` (always `false` when unbounded, so the `Option`-ordering trap of comparing `time_remaining()` yourself is avoided). The deadline is the **minimum** of `started_at + start_to_close` and the cross-retry `schedule_to_close` deadline, because #378 enforcement kills a `RUNNING` row and reporting only the per-attempt budget would over-report; a local attempt is additionally clamped to the enclosing workflow-task budget. `info.identity()` returns the retry-stable `ActivityIdentity` subset for deriving an idempotency key that is reused across attempts. Together these enable the deadline-aware checkpointing pattern — yield cleanly with a retryable error before the budget is spent so a retry re-executes nothing. No new `WorkflowEvent` variant, no migration, no replay impact.

- Add operator re-run of a terminal workflow (issue #777). `POST /workflows/{id}/rerun` starts a fresh execution from a terminal run's verbatim input — the "just run that again" operator action that previously required reconstructing the input by hand. `RERUNNABLE_SOURCE_STATES` deliberately excludes `CONTINUED_AS_NEW` (a sealed link in a chain is not a re-runnable outcome), the tri-state `input` lets a caller override or explicitly reuse, `completed_at` is repaired after the seal, and new `StartSource::Rerun` provenance marks the result. Gated on shard writability, retry-chain membership and schedule attribution so a re-run can neither land on a draining shard nor silently fork a scheduled lineage. 64 DB/HTTP tests. No new `WorkflowEvent` variant, no migration.

- Add declarative DAG node compensation (issue #780). `DagTaskRef::compensate(fn)` / `compensate_named` attach a compensating activity to a DAG node, unwound LIFO in reverse-topological order on the existing `Saga` when a downstream node fails — so a multi-node DAG gets Saga semantics without hand-writing a `#[workflow]`. The compensator receives a `{"dag_compensate", "input", "output"}` envelope and rides the #801 saga markers and counters unchanged. Four build-time rejections (`CompensateOnGate`, `EmptyCompensator`, `CompensatorNameCollidesWithNode`, `DagCompensationRequiresUnifiedExecution`) catch misuse at DAG-build time rather than mid-run. No new `WorkflowEvent` variant, no migration.

- Add DAG-level `execution_timeout` / `sla` attributes (issue #743). A `#[dag]` can now declare the same two deadline knobs a `#[workflow]` can; they propagate purely onto the unified-DAG shadow `WorkflowInfo` (#256) and are enforced by the **existing** #243 and #487 scanners — no new scanner, no new enforcement path. Durations are validated at compile time, and `GET /admin/schedules` surfaces the effective values via `resolve_schedule_deadline_secs`. No new `WorkflowEvent` variant, no migration.

- Add opt-in declared workflow dependencies validated at preflight (issue #802). `#[workflow(activities = [...], children = [...])]` lets a workflow declare the activity and child-workflow types it dispatches, resolved against the registered catalogs during deploy-time preflight so a missing handler is caught before the first run rather than at the first dispatch. Three-state semantics (`None` = undeclared, `Some(&[])` = declares none, `Some(&[..])` = declares these) keep it strictly opt-in, `workflow_unregistered_dependency_failures` names the gaps, the declarations surface on `GET /workflows/registered`, and the preflight table renders failures with remediation. No new `WorkflowEvent` variant, no migration, no replay impact, no macro-path change.

- Add logical-handle routing across the workflow-level retry chain (issue #843). An operator or client holding the *original* execution id of a workflow that has since been retried (#523) now reaches the **live attempt**: `resolve_live_attempt` routes cancel, terminate, pause, resume, signal, query, update and update-result to the current run, and a signal delivers the whole mailbox forward rather than into a sealed predecessor. Describe is deliberately **not** routed (an operator asking about a specific execution must get that execution), audit fidelity is preserved, and every routed response carries `routed_execution_id` so the redirection is never invisible. Documented in `docs/logical-handle.md`. No new `WorkflowEvent` variant, no event-JSON change, no replay impact; one index-only migration `20260723000000_harvest_retry_chain_index`.

- Add a recursive cross-shard execution lineage tree (issue #621). `GET /workflows/{exec_id}/tree` returns an execution's entire descendant lineage as one nested JSON tree assembled across every readable shard — previously the only relationship primitive was direct-children-only, and because a spawned child gets a fresh rendezvous-hashed `ExecutionId` it can land on a different shard, so a naive single-shard `parent_id` walk silently missed cross-shard children. Any node is a valid root, each node carries the spawn relationship (`await_mode: "awaited"` vs `"detached"` + `parent_close_policy`, #347) so a `FAILED` detached child under a still-`RUNNING` parent is visible at a glance, and `?summary=true` returns a per-state descendant roll-up with every `KNOWN_WORKFLOW_STATES` key always present. The walk is level-synchronous and batched (one `parent_id = ANY($1)` query per shard per level, all shards concurrent) giving O(depth × shards) rather than O(nodes × shards) — a recursive CTE is structurally impossible when children live in a different physical database. Bounded by `max_depth`/`max_nodes` with out-of-range values rejected `400` naming the parameter rather than clamped, and a cut sets `truncated: true` plus `truncation_reason` and names the dropped subtrees' parents so the operator can re-root. Cross-shard rows are re-sorted `(started_at, exec_id)` before any budget cut so truncation never depends on shard iteration order; a `visited` set makes cycles and duplicates safe by construction; an unreachable shard degrades to `200` + `status: "partial"` (#756) rather than failing the triage read. CLI `harvest workflow tree`. No new `WorkflowEvent` variant, no migration, zero replay impact.

- Add the open-awaitables diagnostic — "why is this execution parked?" (issue #615). `GET /workflows/{id}/awaitables` reports every durable thing an execution is currently waiting on, derived by a read-only replay through the #612 machinery plus a pure `awaitables.rs` projection. It covers all six awaitable categories plus mutex and external-op kinds, folds a race timer's deadline into the branch it bounds, and degrades to a history-only answer with an explicit contract rather than failing when replay is unavailable. Vantage panel and CLI included. No new `WorkflowEvent` variant, no migration, zero writes.

- Add operator early-warning for workflow history bloat near the cap (issue #704). A `GET /workflows?history_bloat_min_events=N` discovery filter (cross-shard, sorted descending, each row carrying `history_event_count`) finds the runs approaching the history hard cap *before* they hit it, and a soft-threshold `harvest.workflow.history_bloat` counter plus a starter-pack alert make it a standing signal rather than a manual sweep. The filter deliberately rejects composition with cursor/page-size/order parameters with a `400` rather than silently returning a differently-ordered page. No new `WorkflowEvent` variant, one additive nullable-column migration `20260716000000_harvest_workflow_history_bloat_warn` (`history_bloat_warned_at`), no shard-routing change, no replay impact.

- Add an in-flight replay-drift gate — sample what is running now, replay it against the candidate build, block promotion on divergence (issue #798). New `replay_sample.rs` plus `GET /admin/history/export-sample` (stratified sampling via window functions) and `harvest history export-sample` produce a `replay_bundle`, and `ReplayDriftReport` replays it in **canary** mode (not strict, so a healthy parked run is not reported as divergent) with a four-level exit ladder (0/1/2/3/4) suitable for a promotion gate. Zero new `WorkflowEvent` variants, no migration.

- Add a CI gate for payload-schema changes to protect in-flight replay (issue #794). `docs/workflow-schema-contract.json` is a committed baseline of every published payload schema, and `harvest schema check` / `harvest schema update` compare a build against it under a **replay-read** ruleset — with `$ref` resolution, a numeric-format width lattice, identity-keyed `oneOf`/`anyOf` matching, and an append-only `acknowledged_breaking_changes` escape hatch for deliberate breaks. Exit 0/1 makes it a drop-in CI step. No new `WorkflowEvent` variant, no event-schema change, no migration, no macro-path change.

- Add backup/restore runbook and PITR restore-verification tooling (issue #943). Harvest's durability story ended at "it's in Postgres, back that up"; this answers the question a backup does not — *after the restore, is the fleet safe to resume?* New core module `backup_verify.rs` provides a four-tier severity model (`Reclaimable`/`Advisory`/`Incoherent`/`Undetermined`, only the last two failing a drill, because a correct restore always contains dead-worker `RUNNING` rows and expired leases) with 24 `FindingClass` variants each naming its healing mechanism and issue number. **`Undetermined` outranks `Incoherent`**: an unmigrated or empty "restore" makes every probe error and every condition read as absent, so a naive tool would report a clean bill of health on an empty database — never allowed to render as a pass. Read-only three independent ways: every connection sets `SESSION CHARACTERISTICS AS TRANSACTION READ ONLY` (a Postgres-level `25006` guarantee), no mutating scanner is ever called (verify reuses the live scanners' selection predicates verbatim from the same `const` strings, so the report is drift-proof by construction), and a live-DSN guard that fails closed. Sampled histories replay in canary mode and an unregistered type records `replay_skipped_no_handler` rather than a divergence. `harvest backup verify --shard <[N=]DSN>…` accepts every shard in one invocation, cross-checks parent/child terminal ordering and external signal/cancel/await targets across them, degrades a reference into an unsupplied shard to an advisory, and reports fleet `restore_point_skew_secs`; exit codes 0/0/1/2. New runbook `docs/runbooks/backup-restore.md` covers backup approach per table class, exact post-restore semantics per in-flight artifact, the plainly-stated at-least-once consequence, a 30-minute drill, and the multi-shard fencing procedure. No new `WorkflowEvent` variant, no migration, no runtime behavior change.

- Add a time-travel replay debugger for recorded workflow histories (issue #949, R&D). `autumn_harvest::debugger` (behind a new `debugger` Cargo feature) plus a `harvest debug` CLI replay a history *prefix* at a time, producing a `ReplayTrace` of `DebugStep`s an engineer can step through in a TUI — the answer the existing replay surfaces stop short of, which is not "did it diverge" but "at which decision, and what did the code see". `diff_traces` reports the **first difference only**, so a drift between two builds points at one decision rather than a wall of downstream noise; the O(N²) prefix replay is bounded by `--max-steps`. Read-only: no new `WorkflowEvent` variant, no migration, no engine-runtime change, no DB access.

### Changed

- Finish per-shard worker poll loops and scheduler ticks, and make every acceptance criterion falsifiable (issue #961). A reconciliation slice in the #543/#593/#609/#753 precedent: most of the multi-shard runtime had shipped piecemeal under #522 (per-shard poll loops and control loops, the stranded-work sampler), #796/#1157 (per-shard scheduler ticks, DAG→shard pinning, stale-row collection), #797 (per-shard scanner liveness) and #774 (queue coverage) — this closes the residuals and gives each AC a test that can fail. **Behavior change:** an empty `WorkerConfig::shard_assignments` now means "cover every shard this process has a pool for" (resolved by `resolve_shard_assignments` at the two choke points where the pool is final) instead of covering nothing; an explicit list is never widened, and with no sharded pool the list stays empty rather than fabricating a `ShardId(0)` the process never established. A third drifted shard-membership predicate was closed in `preflight::worker_can_cover`, `ShardedInfo` lost `Copy`, multi-shard LISTEN/NOTIFY is per-shard-URL only, and the buffered-drain shard encoding was de-drifted. New `harvest.shard.dispatched{shard}` counter (the shard-dimension twin of `harvest.queue.dispatched{queue}`) and a `harvest_shard_undrained` starter alert make coverage observable rather than assumed.

- **Source-visible change (issue #811):** five `pub` functions in `execution.rs` — `start_or_load_workflow_execution_collect`, `start_or_load_workflow_execution_idempotent`, `signal_with_start_workflow_execution_with_metrics`, `rerun_workflow_execution`, and `update_with_start_workflow_execution_with_metrics` — changed their 4th tuple element from `Vec<(String, String)>` to `Vec<StartCancelledRun>` (which carries `workflow_name`/`queue_name` plus a `superseded: bool` discriminator). A downstream caller that binds or iterates that element must adapt; the wrapper functions most callers use are unchanged.

### Fixed

- Fix the schedule reconciler's "DAG Storm" — a permanent per-tick registration write storm (issue #1157). An upstream deployment reported a single unconvergeable `harvest_schedules` row causing the same failing `UPDATE` to be re-issued once per second forever, starving every other DAG's registration and multiplying by fleet size. Four independent defects compose into that outcome and all four are fixed: (1) `find_reusable_dag_workflow_schedule` looked for a `workflow_name` holder only among `dag_name IS NULL` rows, so a non-NULL non-matching holder produced a guaranteed permanent unique violation — now routed through a total `classify_workflow_name_holder` classifier whose repair path is gated on the registrant owning the name *by right* (`ws.dag_name == ws.workflow_name`) and issued as a compare-and-swap, with a genuine collision returning a named `Config` error rather than starting a new flap-storm; (1b) the INSERT form had the same blind spot under an `ON CONFLICT (dag_name)` arbiter that offers no protection against the `workflow_name` index; (2) `register_workflow_schedules_for_shard` propagated the first error with `?`, so one bad DAG silently disabled every DAG after it — both `_for_shard` passes now collect per-schedule errors and record a capped-exponential `ScheduleRegistrationBackoff` (2 s → 300 s, so an unconvergeable schedule costs ≤ 12 attempts/hour instead of 3600) while every other schedule keeps full cadence; (3) both upserts now short-circuit when the resolved row already matches the desired state (`workflow_schedule_row_is_converged` / `dag_schedule_row_is_converged`, with a NULL `next_run_at` counted as drift only when the fallback would actually fill it), and the stale-row `DELETE` gets its own existence probe — removing roughly 2 million no-op `UPDATE`s/day on a 12-row table; (4) the two tick-path registration writers wrap the actual write in a `pg_try_advisory_xact_lock`, so a peer already reconciling causes this process to skip rather than queue. The delete is deliberately **not** behind that lock and a failed collection stands the shard's own firing down for the tick, because deferring a delete is a correctness bug (duplicate firing) in a way deferring an upsert is not. Every defect is RED→GREEN-verified by neutering each fix in isolation. No new `WorkflowEvent` variant, no migration, no schema change.

- Fix `ctx.race()` so a follow-on durable command no longer nd-blocks when a loser is in-flight (issue #1126). Calling `ctx.mutex(k).acquire()` — or any follow-on durable command — in the same decision cycle immediately after `ctx.race()` resolved previously nd-blocked: a losing activity branch's `ActivityStarted` was left unconsumed at the `HistoryMatcher` cursor (its synthetic `ActivityFailed` is appended only at persist time, invisible to the resolving cycle), so the next positional `match_*` diverged. `settle_race` now consumes the loser branches' in-flight progress-frontier events in the resolving cycle via a new `HistoryMatcher::consume_race_loser_frontier`, so no interposing state-consulting call is needed. This flips the documented `ctx.mutex` (#691) known limitation to resolved. Matcher-side only: no event mutation, no new `WorkflowEvent` variant, no persist-ordering change, no migration.

- Guard the suspended-dispatch catch-all against a stolen mid-flight claim (issue #1182). `handle_suspended_workflow`'s catch-all branch — reached when a live invocation suspends with an empty or unrecognised command set — called `persist_workflow_failure` with no recheck that the dispatcher still owned the claim, so a claim stolen mid-flight (a poison-pill reclaim of a zombie worker, an operator requeue, or a claim race) let the *original* dispatcher terminally fail a task that had already changed hands, corrupting the new owner's in-flight work with a spurious `WorkflowFailed`. Reported as a reproducible CI failure across three consecutive runs. New `fail_suspended_workflow_if_still_claimed` reuses #804's `commit_terminal_failure_if_still_claimed` primitive (re-deriving `next_event_id` under a row lock and rechecking `worker_id`/`crash_strikes` against current state), writing nothing and logging at `info` when the claim has moved. A broader pre-existing finding — the happy-path terminal-write route has no ownership check at all — was filed separately as issue #1184 rather than folded in. Post-review hardening additionally distinguishes a genuine ownership transfer from a transient lock hold, and clears the stale poison-pill `crash_strikes` count on release. No new `WorkflowEvent` variant, no migration.

- Fix `ensure_runtime_migrations` panicking on a TLS-enabled Postgres database URL ("Cannot start a runtime from within a runtime"). `autumn_web::migrate` routed a `sslmode=require`/`verify-ca`/`verify-full` URL through a rustls arm that called `Handle::block_on` on the ambient runtime, which Tokio forbids from a thread already driving that runtime's tasks — so the process died on boot, deterministically and before any network I/O. Fixed by making it an `async fn` wrapping the renamed `ensure_runtime_migrations_blocking` in `spawn_blocking`. **The underlying hazard is now fixed upstream** in the autumn-web 0.7 bump below: `run_pending`/`pending_migrations` run both the connect and the migration body on a freshly spawned `std::thread::scope` thread, safe from any caller context. Harvest keeps the `spawn_blocking` wrapper, now purely so the blocking join never stalls an async worker thread rather than for correctness, and the `#[should_panic]` reproduction test was replaced by `migration_body_is_safe_to_call_directly_from_an_async_context`, which pins the new upstream guarantee. No new `WorkflowEvent` variant, no migration, no route/contract change, no replay impact.

### Internal

- Add a task-claim / enqueue throughput benchmark and a CI-gated performance contract (issue #786). Harvest published a CPU-path budget for replay (10k events < 200 ms, #135) but nothing for `queue::claim_task` — the most scalability-critical query in the engine, which has accreted roughly one `WHERE` predicate per phase since 3.7 (build-id routing, per-key concurrency, rate-limit gate, circuit-breaker tracked set, `schedule_to_close`, PAUSED skip, worker sessions, queue pauses, capability labels, sticky routing), each added for correctness and none measured. Three artifacts share one harness so the published numbers and the gated number can never come from different code: a `harness = false` bench (criterion's repeat-the-closure model would drain the seeded backlog and end up timing an empty queue, since `claim_task` is destructive), an exploratory report in the new `docs/performance.md`, and a CI budget gate. Headline findings: claim latency is superlinear in backlog depth, the sticky `CASE` in `ORDER BY` is non-indexable, per-gate attribution isolates `concurrency_key` at +644%, and enqueue is flat at ~4,800/s. Measurement only — the claim query is byte-for-byte unchanged, with no new `WorkflowEvent` variant, no migration, no schema change and no public API change.

- Add the SQLite edge/local-first storage-trait feasibility report (issue #966, R&D). Closes the one deliverable of the #966 spike that was never written up — a retrospective decision record in `docs/rnd/sqlite-feasibility.md` covering what a storage trait would have to abstract and why the companion-crate route was taken — backed by a 14-test anti-rot guard suite so the report's claims about the current code cannot silently go stale. Docs and tests only, zero production code.

- Heal `trunk-dev` CI where two hand-rolled test INIT_SQL bundles were missing the issue #704 migration (issue #1074), plus a follow-up de-flake of `history_bloat_counter_fires_even_when_the_same_decision_reaches_the_hard_cap` via a new `wait_for_history_bloat_warned` poll helper. Test-code-only.

- Fix four hand-rolled test INIT_SQL fixtures missing the `20260716000000_harvest_workflow_history_bloat_warn` migration (issue #1142), plus a follow-up for a missing `20260514010000_unified_dag_schedule_kind` include. Test-fixture SQL only.

## [0.5.0] - 2026-07-21

### Upgraded

- **Upgrade to autumn-web 0.6.0 / diesel-async 0.9** — the defining change of this release (landed via PR #1124). The plugin, CLI, and example crates move from autumn-web 0.5 to 0.6 and from diesel-async 0.8 to 0.9, keeping Harvest current with the companion web framework's latest lifecycle, routing, MCP, and Postgres-integration surface. All features folded into this 0.5.0 section below ship on top of that upgraded baseline.

### Added

- Add typed workflow failures (issue #767). A `#[workflow]` can now return `Result<T, WorkflowFailure>` to carry a structured, replay-safe failure cause — a stable low-cardinality `error_type` class, optional structured `details`, and an advisory `non_retryable` hint — mirroring the typed `ActivityFailure` surface onto whole workflows. Opt-in and non-breaking: a workflow returning `Result<T, String>` (or any `E: ToString`) is byte-for-byte unchanged and decodes to `error_type = None`. New public `WorkflowFailure` builder (`::new`/`.with_details`/`.non_retryable`), the `harvest_workflow_failure_v1` wire envelope + tolerant `decode_workflow_failure`, macro opt-in detection (`workflow_returns_workflow_failure`), `HarvestError` accessors (`workflow_error_type`/`workflow_details`/`is_workflow_non_retryable`), and the typed handle surface (`TypedWorkflowHandle::result()`/`result_snapshot()`, `WorkflowHandle::terminal_typed_failure`). The `non_retryable` flag is an **advisory** classification hint for the caller / completion-trigger only — it is deliberately *not* a control input to the engine's workflow-level retry (#523) loop. No new `WorkflowEvent` variant, no migration (the typed fields are additive optional columns on the existing `WorkflowFailed`/`ChildWorkflowFailed` events).

- Add activity execution interceptors (issue #680). Register an ordered middleware chain on `HarvestBuilder` with `.activity_interceptor(...)` (repeatable) that wraps **every** activity execution on the worker — regular and local — outermost-first, so cross-cutting behavior (structured logging, custom metrics, header propagation, input/output shaping, fault injection) is added once at registration time instead of per `#[activity]`. Object-safe `ActivityInterceptor` trait (no `async_trait`, mirroring `PayloadStore`/`HistoryArchiver`); an interceptor may observe, short-circuit (skip `next`), or transform input/result/error. Errors ride the same `Err(String)` typed-failure channel as a handler error (identical retry / circuit-breaker / DLQ handling), and a construction- or poll-time panic is contained as a retryable `HandlerPanic` (issue #782) so the worker never crashes and poison-pill accounting stays intact. Replay-safe by construction: interceptors run only on the live dispatch path, never during history replay, and the value the chain returns is exactly what is recorded. A zero-interceptor default is a byte-for-byte-unchanged direct handler call. A transactional activity (`ctx.run_transactional`) seals its own outcome atomically, so an interceptor observes but cannot transform its recorded result — the worker reads a self-commit flag so a post-interceptor transform can no longer trip the circuit breaker or record `Failed` on a committed `ActivityCompleted`. No new `WorkflowEvent` variant, no migration, no event-schema change.

- Add non-blocking signal drain for the aggregator pattern (issue #775). Five synchronous, non-suspending `WorkflowContext` methods — `try_receive_signal`/`try_wait_for_signal` (oldest buffered signal or `None`), `drain_signals`/`drain_signals_raw` (all currently-buffered signals in recorded-history order, fail-fast on a bad payload), and `drain_signals_collect` (poison-tolerant per-slot `Result`) — make the collect-N-signals-then-batch-process-then-`continue_as_new` pattern O(1) tasks + O(N) replay instead of O(N) tasks + O(N²) replay. They only consume already-recorded `SignalReceived` events, honor the cursor-bound history-order and race-reservation (#476) contracts, and never double-deliver against `receive_signal`/push handlers (#546). `WorkflowTestEnv`/`WorkflowSimulator` now mirror production's task-prep signal ingest so a drain-first workflow is faithfully testable. No new `WorkflowEvent` variant, no migration.

- Add declarative DAG signal/approval gate nodes (issue #746). A unified `#[dag]` can pause on a named signal between nodes with an optional durable timeout via `DagBuilder::signal_gate(name)` / `signal_gate_with_timeout(name, timeout, on_timeout)` (`GateTimeoutAction::{FailRun, Continue}`) — the declarative way to insert a human approval or external-event wait without hand-writing a `#[workflow]`. Gates lower onto the existing #476 `receive_signal_timeout`/`wait_for_signal` machinery (reusing `TimerStarted`/`TimerFired`/`SignalReceived`), so a gated run is durable and replays deterministically; a `build()` post-pass isolates each gate onto its own singleton execution level so a `WaitForSignal` never shares a suspension batch with a level's activity dispatches (no-gate DAGs are byte-for-byte unchanged). Under `Continue`, the gate's null-vs-payload output is the branch selector via a downstream `.condition(...)`. The graph-view and MCP `signal_{dag}` tool surfaces are updated for gate nodes; classic (non-unified) DAGs with a gate are rejected at build time. No new `WorkflowEvent` variant, no migration.

- Add tiered / summary retention (issue #752). When the history-retention janitor (issue #737) hard-deletes a terminal execution, it can optionally demote it into a compact, queryable `harvest_execution_summaries` row (identity, terminal state, timing, search-attrs, and an opt-in byte-capped result/error) instead of losing all trace — full histories expire on one horizon while lightweight ~<1 KB summaries live on a second, decoupled (longer or unbounded) horizon. Opt-in via `RetentionConfig::with_summary_retention*`; `None` (default) is byte-for-byte identical to before. Demotion is atomic inside the existing delete transaction (no window where both are absent; legally-held #747 rows are never summarized). New admin-guarded read route `GET /workflows/summaries` (keyset-paginated, cross-shard, filters mirror `GET /workflows`), a second GC pass with a distinct `harvest.retention.summary_deleted{workflow}` counter, and a PII-erasure (#495) extension that scrubs a matching summary — including a `parent_id`-based child-summary erase cascade so a demoted child/grandchild summary is reached even after its execution row is gone. CLI `harvest workflow summaries`. Migration `20260710000001_harvest_execution_summaries`. No new `WorkflowEvent` variant, no replay impact.

- Add per-execution legal hold (issue #747). `POST /workflows/{id}/legal-hold` places a durable, per-execution hold that exempts that execution's `harvest_events` history from both retention deletion (issue #737) and targeted PII erasure (issue #495) until it is released (`/legal-hold/release`) or auto-expires (optional `hold_until`) — so a subpoena/litigation hold on one run no longer requires disabling retention globally or racing the janitor. A held execution is skipped as the first per-candidate gate in the retention tick; an erase attempt on a held execution returns `409`. Idempotent (re-hold preserves provenance, release-unheld is a no-op), admin-only, audited (`legal_hold.set`/`legal_hold.release`), surfaced as a derived `legal_hold` boolean on `GET /workflows/{id}` and filterable via `GET /workflows?legal_hold=true`. CLI `harvest legal-hold set/release`. Additive nullable columns only (migration `20260709000001_harvest_legal_hold`); no new `WorkflowEvent` variant, no replay impact, shard-local.

- Add cancellable / renewable durable timers (issue #768). `ctx.start_timer(id, secs) -> TimerHandle` arms a durable timer via a non-suspending bookkeeping command; the handle's `cancel()`, `reset(secs)`, and `await_fire() -> TimerOutcome::{Fired, Cancelled}` (plus untyped `ctx.cancel_timer`/`reset_timer`) let idle-session timeouts, sliding-window debounces, and lease renewals slide a deadline without leaking orphaned `harvest_timers` rows that still fire — the missing complement to the fire-once `ctx.timer`/`ctx.sleep_until`. One new `WorkflowEvent::TimerCancelled { timer_id }` variant (appended at the enum end; pre-#768 histories deserialize unchanged), no migration; fire suppression is the pending-row delete, and the cancel-races-fire winner is decided by recorded-history order. `ctx.timer()`/`ctx.sleep_until()` are unchanged. Extensively hardened across sixteen post-review rounds (reset-interleaving replay soundness, terminal-cycle event emission, duration/clock accounting, and bidirectional classic-vs-cancellable timer-id namespace disjointness), gated by a 1000-distinct-fixture replay-determinism bar.

- Contain `#[workflow]` and `#[activity]` handler panics as clean failures (issue #782). A `panic!()` in a handler body (a stray `unwrap()`, an out-of-bounds index, an overflow) is now caught at the dispatch boundary via `catch_unwind`/`catch_construct` and routed into the engine's existing failure paths instead of unwinding the spawned worker task and leaving the `harvest_task_queue` row stuck `RUNNING` (where the poison-pill reclaimer's missing-heartbeat path never reached it). An activity panic becomes a retryable typed `ActivityFailed` (`error_type = "HandlerPanic"`) that follows the activity's retry policy; a workflow-body panic is re-dispatched under a bounded, configurable budget (`WorkerConfig::workflow_panic_max_attempts`, default 3) and then fails terminally via `WorkflowFailed`. The worker process stays up and the task leaves `RUNNING` within one poll interval; two panic-distinct counters (`harvest.activity.panic`, `harvest.workflow.panic`) alert on handler-panic rate separately from process-crash quarantine. Requires `panic = "unwind"` (documented). No new `WorkflowEvent` variant, no migration.

- Add continue-as-new run-chain timeline API (issue #701). New read-only `GET /workflows/{id}/run-chain` and `harvest workflow run-chain <exec_id> [--json]` return the complete, ordered continue-as-new succession for a workflow, resolvable from any member run (head, middle, or latest) in one call — previously an operator holding the latest run had no way to see its predecessors, since continue-as-new successors carried no backward link. Two additive nullable columns (`continued_from_exec_id`, `first_exec_id`, migration `20260710000002_harvest_workflow_continue_chain`) give O(1) head resolution and a single indexed cross-any-member query; pre-feature chains degrade gracefully via the existing `WorkflowContinuedAsNew` events with a `head_unknown` flag rather than ever returning a 500. Shard-local, read-only. No new `WorkflowEvent` variant, zero replay-determinism impact.

- Surface open circuit breakers in the stuck-task eligibility explainer (issue #611). `GET /admin/queues/{queue_name}/eligibility` and `/admin/tasks/{id}/eligibility` now emit `circuit_open`/`circuit_half_open` reason codes for a pending task blocked by its next activity's open/half-open circuit breaker — previously such a task had an empty reason set and its worker was wrongly reported eligible during a downstream outage. The `rate_limit_saturated` reason code is renamed to `rate_limit_exhausted` (a live-label response-contract change), and for a circuit-breaker activity the rate-limit reason is suppressed in favor of the breaker phase (matching #369's dispatch-time enforcement). Read-only, plugin-only; no new `WorkflowEvent` variant, no migration, no core-crate change.

- Bootstrap property-based testing (`proptest`) + a `cargo-fuzz`/libFuzzer harness (PR #1004). A new `tests/property/` target covers pure functions and parsers (retry-delay/jitter bands, queue-fairness weighting, task-duration round-trips, completion-trigger evaluation totality, callback-URL SSRF, `WorkflowEvent` serde back-compat, build-ramp bucketing, DLQ failure-signature determinism), and a workspace-excluded `fuzz/` crate adds four coverage-guided targets over `WorkflowEvent` deserialization, `det_check::check_source`, callback-URL validation, and the DLQ failure signature. CI gains a db-gated property-suite execution step and an opt-in `workflow_dispatch`-only fuzz-smoke job. Test/tooling/docs/CI only — no product-code behavior change, no new `WorkflowEvent` variant, no migration.

- Bootstrap loom concurrency model checking for the in-process state machines (PR #1007, testing infra). A contained `cfg(loom)` sync-primitive shim (`src/loom_sync.rs`) and four `#![cfg(loom)]`-gated models (`tests/loom_models.rs`) exhaustively explore the thread interleavings of the per-activity circuit breaker and the worker session-slot registry, proving the generation-fence / half-open-probe / capacity-bound invariants under concurrency. Runs on `workflow_dispatch` only (new `.github/workflows/loom.yml`), never in the required-status matrix. No production behavior change, no new `WorkflowEvent` variant, no migration.

- Add in-place schedule update: `PATCH /api/harvest/admin/schedules/{id}` edits a schedule's cron expression, workflow input, queue, overlap/skip/catchup policies, jitter, calendar, `max_active_runs`, and #478 bounds (`end_at`/`max_runs`) without recreating it (issue #771). Previously the only way to change a schedule's cadence or input was DELETE + re-create, which minted a new `schedule_id` — breaking #488 last-completion-result carryover lineage, severing #534 run history, and opening a missed-fire window between the delete and the re-create. The PATCH is a partial update (only provided fields change; explicit JSON `null` clears a nullable field, absence preserves it), validates the fully merged spec with the create-path validators before any write, and applies inside a single `FOR UPDATE` transaction so an edit can never race a scheduler tick into firing the old spec (a live #350 fire claim returns `409`; an expired claim is fenced; the tick re-reads the fresh row after winning its claim). `workflow_name` is not editable (`400`), unknown body fields are rejected (`400`, `deny_unknown_fields`). Success recomputes `next_run_at` from the new expression anchored at now (never retroactively firing elapsed slots) and reconciles #478 exhaustion in both directions; paused schedules stay paused. New audit op `schedule.update` (route classified Mutating), additive `workflow_input` field on schedule list/get responses, CLI `harvest schedule update <id> [--cron ... --input-json ... --clear-end-at ...]`. No new `WorkflowEvent` variant, zero `harvest_events` writes, no migration.

- Add operator force-fail for a single hung in-flight activity (issue #765). New admin-guarded `POST /workflows/{id}/activities/{activity_exec_id}/fail-now` route and `harvest workflow fail-activity <workflow_id> <activity_exec_id> [--reason <text>]` CLI subcommand complete the per-activity operator toolkit started by `retry-now` (issue #516): where `retry-now` skips a *backing-off* (PENDING) activity's backoff, `fail-now` force-fails an *in-flight* (RUNNING) one — an activity wedged against a dead downstream that will never heartbeat-timeout, or whose remaining retry curve the operator wants to stop dead. Appends `ActivityFailed` with the new stable `ERROR_TYPE_OPERATOR_FORCE_FAILED = "OperatorForceFailed"` error type through the existing recording path, skips every remaining retry attempt (non-retryable by construction), and wakes the parked workflow task so the owning workflow advances to its own failure/compensation path — it is NOT terminated (workflow code can branch on the new `HarvestError::is_operator_force_failed()`). Idempotent: a repeat call on an already-forced task is a zero-write no-op success (`already_forced: true`). Guarded: `409` when the owning execution is already terminal, when the task is not RUNNING (or failed with a genuine non-forced error), when the id names a workflow task, or when history already carries a terminal event for the activity (including legacy `activity_id`-less rows); `404` for unknown ids / wrong workflow / local activities (no task-queue row). The operator-supplied `reason` is truncated server-side to 500 characters (mirroring erase-payloads) before being durably recorded. Audited under `activity.fail_now`. No new `WorkflowEvent` variant, no migration.

- Close the pause/resume operator gaps on top of the issue #383 primitive (issue #609). New CLI subcommands `harvest workflow pause <execution_id> [--reason <text>]` and `harvest workflow resume <execution_id>`, mapping 1:1 onto the existing admin-guarded `POST /workflows/{id}/pause`/`/resume` routes (previously only schedule/DAG pause existed in the CLI). Additive response fields: `paused_at` on the pause response and `newly_resumed` on the resume response (both registered in `docs/api-contract.json`). **Behavior change**: resuming a non-paused (running or terminal) execution is now a `200` success no-op reporting `newly_resumed: false` with a zero pause duration, instead of a `409` — idempotent operator retry, mirroring terminate's `newly_terminated: false` precedent; `404` is kept for unknown ids. Pause now also suspends the per-activity cross-retry `schedule_to_close` clock (issue #378 interaction): the `ScheduleToClose` timeout scanner excludes tasks whose owning execution is `PAUSED` — both ordinary task-queue rows and external (`harvest_external_tasks`) tasks — the worker's retry path requeues instead of deadline-failing while paused and re-validates the row-current deadline under the execution row lock before failing a task terminally (so a concurrent resume's shift is never lost to a stale claim-time snapshot), and resume shifts `schedule_to_close_at` forward by the pause span for still-open task-queue and external-task rows. A `PENDING` activity row whose deadline elapses mid-pause is *frozen* (unclaimable, spared by the scanners) rather than timed out: the `ScheduleToStart` scanner carves out exactly those frozen rows — unfrozen pending activities of a paused execution stay claimable and enforced, and `start_to_close`/`heartbeat_timeout` stay pause-blind — and resume restores a frozen row's `scheduled_at` alongside its deadline so the first post-resume scan doesn't instantly kill it. New operator runbook `docs/runbooks/contain-runaway-execution.md` (pause/cancel/terminate decision matrix, what pause does and does not stop, deadline-shift semantics, the auto-resume ceiling) plus a `runaway-execution-containment` drill in `docs/runbooks/synthetic-incident-drills.md`. No new `WorkflowEvent` variant, no migration.

- Add durable completion callbacks — push terminal results to a URL (issue #605). An embedder starting a workflow from their Autumn app previously had no push notification when it finishes, only `GET /workflows/{id}/result` polling (issue #527). An embedder can now register a completion callback target (URL + `EventFilter`: `CompletedOnly` / `AnyTerminal` / `States([...])`) either as a per-execution `completion_callbacks` start option or a `HarvestBuilder`-wide default (`completion_callback_default`/`_allowlist`/`_allow_http`/`_allow_ip_literals`/`_secret`/`_retry_policy`/`_deliverer`), and Harvest durably POSTs a fixed, HMAC-signed JSON envelope (`{delivery_id, execution_id, workflow_name, workflow_id, state, result?, error?, completed_at}` with `X-Harvest-Signature: sha256=<hex>` + `X-Harvest-Timestamp` headers) the moment the workflow reaches a terminal state. SSRF-guarded by default: HTTPS-only, allowlist-required (exact-host and `*.suffix` wildcard matching, IP-literal loopback/private/link-local/CGNAT/ULA blocking), validated both at registration (a non-allowlisted per-execution target is rejected `422` at the start route; a bad builder default fails `try_build()`) and again at enqueue time against the live policy. Delivery is itself durable execution: a new `harvest_completion_deliveries` table (one row per matched target, frozen envelope + retry-policy snapshot immune to later row mutation/retention) is enqueued inside the same terminal transaction every other terminal side effect already runs in, and a two-transaction shard-local scanner (folded into the existing timeout-checker poll loop — no new background task) retries with backoff (10 attempts, 30s initial, ×2, 600s cap by default) and dead-letters on exhaustion under a new typed `DeadLetterReason::CallbackDeliveryExhausted`. New management routes `GET /workflows/{id}/completion-deliveries` (list) and `POST /workflows/{id}/completion-deliveries/{delivery_id}/redrive` (admin-guarded, audited, idempotent-shaped — resets the same `delivery_id` so a receiver's idempotency key still matches). New CLI: `harvest completion-delivery list <execution_id> [--state ...]` / `redrive <execution_id> <delivery_id>`. Core (`autumn-harvest`) defines a boxed-future `CompletionCallbackDeliverer` trait and ships no HTTP client (mirroring `PayloadStore`/`HistoryArchiver`); the plugin ships a `reqwest`-based default (`redirect::Policy::none()`, since an allowlisted host could otherwise 302 to an internal address and bypass the SSRF guard), auto-wired by `HarvestPlugin`/`HarvestRunner` and overridable. See `docs/completion-callbacks.md`. No new `WorkflowEvent` variant, no replay-determinism impact — `harvest_events` is untouched and a callback-less workflow behaves byte-for-byte identically to before this feature existed.

- Add a first-class inbound HTTP webhook receiver primitive (issue #344). Harvest does not roll its own signature verification, timestamp tolerance, or replay protection — autumn-web 0.5's `[security.webhooks.endpoints]` / `SignedWebhook` layer already does that (Stripe/GitHub/Slack/generic HMAC presets, secret rotation, boot-time fail-fast on a missing/weak secret) — so `#[webhook]` maps an **already-verified** delivery onto a deterministic workflow trigger and dispatches it idempotently. New core module `autumn-harvest/src/webhook_trigger.rs` (`WebhookCtx`, `WebhookTarget::{Starts, SignalsWithStart}`, `WebhookTriggerInfo`, pure `validate_webhook_triggers`), new `#[webhook(path = "…", starts = "…" | signals = "…", signal_name = "…", queue = "…")]` macro + `webhooks![...]` collector (mirroring the `workflows!`/`activities!` family), and a new `autumn-harvest-plugin/src/webhook_receiver.rs` slice behind the existing `webhooks` cargo feature (`HarvestPlugin::webhooks(...)`, app-level route generation mirroring the `mcp_tools.rs` pattern from issue #597). Verification failures propagate autumn-web's own structured error (`401`/`400`/`409`/`503`); payload parse failures and mapping-function rejections return `400` with a distinguishable `error_code` (`parse_failed`/`mapping_rejected`/`missing_idempotency`); dispatch delegates to the existing `start_workflow`/`signal_with_start_workflow` handlers (so schema validation, the admission gate, debounce/SLA/execution-timeout resolution, and audit apply identically to a webhook-triggered start) and reshapes the response into `{"status": "accepted"|"idempotent_replay", "workflow_exec_id", "workflow_id"}` (`202`/`200`). `HarvestPlugin::build` fails fast (panics) on a duplicate binding path or a trigger targeting an unregistered workflow. New metrics `harvest.webhook.received`/`harvest.webhook.rejected` (bounded `path`/`outcome` labels) and audit operation `webhook.trigger` (dispatch attempts only — an unauthenticated sender can't generate audit rows by sending unsigned requests). Doc chapter `docs/getting-started/12-webhooks.md`, example `autumn-harvest-plugin/examples/webhook_receiver_quickstart.rs`. **No new `WorkflowEvent` variant, no migration, no `docs/api-contract.json` change** (webhook binding paths are user-defined app-level routes, invisible to the management-router manifests — same treatment as the MCP tool routes).

- Expose `#[workflow]`s as durable MCP tools (issue #597). A single `#[workflow(mcp)]` opt-in (plus `HarvestPlugin::mcp_tools()` / `mcp_tools_at(prefix)` on the app side, under the new plugin `mcp` cargo feature) projects a workflow onto autumn-web 0.5's MCP layer as a correlated tool set: `start_{wf}` (returns the durable `execution_id` handle immediately — the work survives daemon restarts and does not require the agent to stay connected), `{wf}_status` (state / `current_details` progress / output / error by handle), `signal_{wf}` (async signal delivery, `Idempotency-Key` supported), `{wf}_watch` (streaming progress over MCP `notifications/progress`, driven by the shard's LISTEN/NOTIFY `harvest_events` channel — no polling), and one synchronous `{wf}_update_{name}` tool per `#[update(workflow = "…", mcp)]` handler. The `start_{wf}` tool's `inputSchema` is derived from the workflow's published `input_schema` (issue #373) via a process-global schema map drained by a static `ApiDoc::register_schemas` hook — no second, hand-maintained schema — and start input is validated against it at the tool edge before any storage access. Handlers delegate to the existing management-API primitives; every handle-taking tool verifies the execution belongs to its workflow (uniform 404, no cross-workflow existence oracle). Effectful-by-default safety: exposure is strictly opt-in, the mutating tools are never part of autumn-web's read-only `expose_all_as_mcp` hatch, tool calls replay the caller's credentials through the real handler pipeline (gate the endpoint with `secure_mcp`), and routes fail closed before the runtime starts. Known inherited gap: autumn-web 0.5 derives tool `annotations` from the HTTP verb, so mutating tools carry `readOnlyHint: false` but a literal `destructiveHint: true` is only emitted for DELETE routes — needs an autumn-web change to annotate POST tools. Gap-fills shipped along the way: `HarvestPlugin::updates()`/`queries()` now forward to the builder (previously `#[update]`/`#[query]` handlers could not be registered through the plugin at all), `HarvestBuilder` gains pre-build `workflow_infos()`/`update_handlers()`/`query_handlers()` accessors, and `RegisteredWorkflowRecord` (`GET /workflows/registered`) surfaces the new `mcp` flag (additive response field). New module `autumn-harvest-plugin/src/mcp_tools.rs` (pure descriptor layer + global schema map + route/handler layer), example `autumn-harvest-plugin/examples/mcp_tools_quickstart.rs`, doc page `docs/mcp-tools.md`. Determinism preserved by construction: exposure is an HTTP-edge concern — the `mcp` flag is never consulted by core execution. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Harden the MCP tool exposure (issue #597) against a code review pass. The most severe finding: the generated tool routes are registered via `AppBuilder::routes(...)`, not `nest()`, so a mutating tool's own HTTP path bypassed both `HarvestPlugin::api_with_auth`'s middleware (which only wrapped the `nest()`-mounted management router) and autumn-web's `secure_mcp` (which only gates the `/mcp` JSON-RPC envelope) — fixed by threading a new `McpToolMiddlewareFn` through the route builder so `api_with_auth`'s configured layer now wraps every generated tool route's `MethodRouter` directly, not just the management API. Also fixed: `{wf}_status`/`{wf}_watch` now transparently follow a `ContinuedAsNew` successor chain (reusing the `/result` endpoint's chain-walk, issue #527, extracted into `resolve_terminal_workflow_execution`) instead of reporting the sealed predecessor's dead-end sentinel with null output/error; `{wf}_watch`'s SSE loop now sends an `event: error` frame on a lost LISTEN/NOTIFY connection instead of silently ending the stream, and detects a client that disconnected during a quiet stretch instead of leaking the background polling task until the watched execution eventually terminates; the fallback `inputSchema` for a workflow/update with no published schema no longer asserts `"type": "object"` (would reject valid array/scalar input from a multi-param or non-object-param workflow); `start_tool`'s redundant, closure-captured-schema pre-validation (which also skipped the audit record the delegated `start_workflow` writes on rejection) was removed in favor of the single, already-existing check; the process-global MCP schema map now warns on a genuinely divergent same-name re-registration (an inherent limitation of autumn-web's bare-fn-pointer schema hook, not fully fixable without an upstream change, but no longer silent); and the triplicated `mcp`/`allow_nondeterministic_apis` bare-flag-or-`=bool` attribute parsing across `workflow.rs`/`update.rs` was consolidated into one `autumn-harvest-macros::attr_util::parse_bool_flag` helper. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Fix a broken CI lint job and two more MCP tool gaps found by automated PR review (issue #597). `StartWorkflowRequest::from_input`/`AdmitUpdateRequest::new` are only reachable from the `mcp`-gated `mcp_tools.rs` module, so the plain (no-`mcp`-feature) clippy lint job saw zero callers and failed on `-D warnings`; fixed with a feature-conditional `allow(dead_code)`. An `#[workflow(mcp)]` workflow that also carries a `debounce`/`batch` policy could return `202 Accepted` with no `execution_id` from `start_{wf}`, breaking every other generated tool's "durable handle immediately" contract — such workflows are now excluded from MCP exposure entirely (warned, not silently broken). `signal_{wf}`/`{wf}_update_{name}` now resolve a `ContinuedAsNew` chain the same way `{wf}_status`/`{wf}_watch` already did, so an agent holding the original durable handle can still signal or update a workflow after it continues-as-new instead of the delivery failing against the sealed predecessor. No new `WorkflowEvent` variant, no migration.

- Warn loudly when MCP tool routes have no route-level auth configured (issue #597). The prior auth-bypass fix only closes the gap for embedders who call `HarvestPlugin::api_with_auth`; an embedder who instead relies solely on autumn-web's `secure_mcp(...)` (which only gates the `/mcp` JSON-RPC envelope, not a route's own direct HTTP path) still has the generated `start_{wf}`/`signal_{wf}`/update routes reachable unauthenticated — and `HarvestPlugin` has no way to detect `secure_mcp`, since it's configured on the outer `AppBuilder` after `Plugin::build()` returns. `Plugin::build()` now logs a `tracing::warn!` naming the exact risk whenever `mcp_tools()` is enabled with no `api_with_auth` middleware configured. No new `WorkflowEvent` variant, no migration.

- Fix two more MCP tool gaps found by automated PR review (issue #597). `admit_update` (the standalone update-admit handler shared by the plain HTTP route and the new `{wf}_update_{name}` MCP tool) never consulted a registered update's validator before durably admitting it — an invalid payload became durable history instead of being rejected at the edge, unlike the sibling `update_with_start` path which already validates first. Fixed by running the same validator check (scoped by workflow name + update name, matching `update_with_start`'s `422` response shape) before admission. The MCP quickstart example previously told production users to add only `secure_mcp(...)`, but the generated tool routes are also reachable at their own direct HTTP paths under `/api/harvest/mcp`, which `secure_mcp` doesn't cover — the example now configures both `api_with_auth` and `secure_mcp`. No new `WorkflowEvent` variant, no migration.

- Exclude MCP workflows whose generated tool names collide with a sibling's (issue #597). Operation ids are derived purely from a workflow's (and its updates') name via fixed templates, so two differently-named workflows can still collide — e.g. workflow `invoice_status`'s start tool (`start_invoice_status`) is the same string as workflow `start_invoice`'s status tool. autumn-web keeps only the first registration on a collision and silently drops the rest, so an unchecked collision would make one exposed workflow quietly lose a tool from `tools/list` while its direct route still worked. `collect_descriptors` now tracks generated operation ids across the whole workflow list and excludes (with a warning) any workflow whose tool set would collide with an already-accepted one — first-seen wins, the whole colliding workflow is dropped rather than partially exposed. No new `WorkflowEvent` variant, no migration.

- Fix a no-DB test that asserted the wrong thing for the wrong reason (issue #597). `tests/mcp_tools_http_tests.rs::tools_call_dispatches_through_the_real_pipeline_and_fails_closed` sent a schema-violating `start_order_flow` body expecting to prove issue #373 schema rejection at the tool edge, but the earlier removal of `start_tool`'s redundant pre-delegation schema check (see the "Harden the MCP tool exposure" entry above) means this no-runtime harness now fails closed on `harvest runtime is not started` before ever reaching `start_workflow`'s own schema check — and that response's `"status":400` field kept the old assertion passing for an unrelated reason. Fixed by asserting the actual, reachable guarantee (fail-closed on the runtime-not-started error) and adding `mcp_start_tool_rejects_input_that_violates_the_published_schema` to `tests/mcp_tools_integration.rs` (testcontainers, with a real runtime) as the schema-rejection guarantee's actual coverage. No production code changed; no new `WorkflowEvent` variant, no migration.

- Fix MCP tool exposure for duplicate workflow-name registrations to match the runtime's own dedupe (issue #597). `HandlerRegistry` collapses the builder's workflow list into a `HashMap` via `.collect()`, so the runtime executes the *last* registration when the same workflow name appears twice; `collect_descriptors` instead filtered to `mcp`-flagged entries and kept the *first* same-named descriptor, so `.workflows([foo.with_mcp(), foo])` could still expose MCP tools for `foo` derived from a `WorkflowInfo` the runtime does not actually execute. Fixed by resolving the same last-wins collapse before filtering by `mcp` (with a warning naming the duplicate), so tool exposure and schema derivation always reflect the effective, runtime-executed `WorkflowInfo`. No new `WorkflowEvent` variant, no migration.

- Fix the MCP docs primary setup snippet to use `api_with_auth` instead of plain `api` (issue #597). The snippet paired unauthenticated `.api("/api/harvest")` with `secure_mcp(...)` commented as the production gate, but `secure_mcp` only protects the `/mcp` JSON-RPC envelope, not a generated tool route's own direct HTTP path — matching the same gap already fixed in the quickstart example. (A separate reported P2 finding, that `HarvestPlugin::api_with_auth`'s `M: Clone` bound was newly required by this feature, was investigated and found not to apply: that bound predates this PR, coming from `axum::Router::layer`'s own signature — no code change was needed.)

- Fix a post-merge compile break in `autumn-harvest/tests/nd_block_tests.rs` (issue #597). Merging `trunk-dev` (issue #603's `nd_block_tests.rs`, landed concurrently with this branch's `mcp: bool` field addition to `WorkflowInfo`) left a raw `WorkflowInfo { .. }` struct literal missing the new field, since a merge cannot auto-add a field to an existing literal. Added `mcp: false,`. Caught by CI's Lint job; verified with a full re-run of the CI lint/test command matrix.

- Fix duplicate update-handler resolution to match the runtime's own dedupe (issue #597). `admit_update`/`update_with_start` resolve a declarative update handler via `registry.update_handlers.iter().find(...)` (first-wins), but `collect_descriptors` filtered to `mcp`-flagged updates before dedup and kept the first *mcp-flagged* one — so a duplicate `(workflow, update_name)` registration where only the later one was `mcp`-flagged could expose a tool derived from a handler the runtime doesn't actually validate/admit against. Fixed by resolving first-wins across the whole unfiltered list before filtering by `mcp`, mirroring the workflow-name fix. No new `WorkflowEvent` variant, no migration.

- Fix a self-inflicted regression from the workflow-name-dedup fix above (issue #597): sorting workflow names alphabetically before the operation-id collision-detection loop silently flipped the collision tie-break from "first-registered wins" to "alphabetically-first wins" — registering `start_invoice` before `invoice_status` would incorrectly keep `invoice_status` instead. Fixed by processing collision candidates in first-registration order; the final returned list is still sorted by name.

- Fix a second, subtler issue in the same collision-order fix (issue #597): ordering collision candidates by a workflow name's *first mention* is still wrong when that name is registered more than once, since the first mention can be superseded by a later, effective registration. Fixed by ordering by the index of the registration `effective` actually resolved to (tracked in lockstep with the existing last-wins map), so the collision tie-break always reflects the registration the runtime actually uses, not a superseded earlier one.

- Clarify docs on MCP tool-name collision scope (issue #597). Harvest's own operation-id collision check only compares Harvest-generated tool names against each other, since `AppBuilder` has no accessor for introspecting the host app's other registered routes — a collision with an app-registered (non-Harvest) tool name falls through to autumn-web's own `derive_tools` fallback instead, which already warns (not silently) and keeps the first registration. `docs/mcp-tools.md` also previously mischaracterized autumn-web's own collision handling as "silent"; corrected. No code change — the underlying risk was already covered by an existing autumn-web safety net.

- Fix `admit_update`'s validator check to fail closed instead of silently skipping during plugin startup (issue #597). `Plugin::build` installs the storage pool before installing the runtime, with a genuine `.await` in between (the boot-time admission-gate load), so a request could reach `admit_update` with a working storage pool while `api_state.runtime()` still errored — the round-4 validator check silently fell through to admission in that case instead of failing closed, letting a validator-bearing update become durable history unvalidated. Fixed by requiring the runtime unconditionally, mirroring `update_with_start_workflow`'s existing hard fail-closed pattern. Affects both the classic HTTP update route and the MCP update tool, since they share this handler.

- Harden the per-tenant usage report (issue #596) against a code review pass. `GET /admin/usage` now: (1) hard-caps the number of returned groups (`usage_max_groups`, default 10,000) and fails loudly with `413` naming the cap and actual group count rather than either a silent rollup or an unbounded response, mirroring the endpoint's existing "never silently drop a tenant" principle; (2) is properly configurable via `HarvestBuilder::usage_window_ceiling`/`usage_max_groups` (previously the underlying `HarvestApiState` knobs existed but were never wired through the builder); (3) derives the `"(unattributed)"` SQL literal from the `UNATTRIBUTED_GROUP` Rust constant instead of duplicating it, closing a drift risk, and documents the residual (accepted, AC-mandated) limitation that a real `search_attrs` value literally equal to `"(unattributed)"` merges with the missing-key bucket; (4) merges two previously-separate `harvest_events` scans for `ActivityFailed`/`ActivityTimedOut` events into one CTE, avoiding a duplicate table read; (5) adds three new indexes (`idx_harvest_we_shard_started`, `idx_harvest_we_shard_completed`, `idx_harvest_events_activity_type_ts`, migration `20260702000000_harvest_usage_report_indexes`) to keep the endpoint within its stated `<2s`/250k-execution SLA; (6) fixes an error message that mis-rounded non-whole-day window ceilings (e.g. a 30-hour ceiling previously reported "1-day"); (7) documents that `workflow_starts` deliberately counts every execution row including continue-as-new/auto-retry successors. Also extracted a shared `shard_fanout::summarize_shard_errors` helper (used by both `usage.rs` and `workflow_count.rs`, replacing near-identical duplicated accumulator logic) and removed a duplicate `cell_f64`/`format_f64` CLI helper. No new `WorkflowEvent` variant, no replay-determinism impact.

- Add per-tenant usage report for chargeback and capacity planning (issue #596). New read-only `GET /admin/usage` endpoint is the *historical* companion to the *point-in-time* `GET /admin/concurrency` endpoint (issue #247): it aggregates already-durable data (`harvest_workflow_executions` + `harvest_events`) over a required `from`/`to` window (RFC 3339 or relative duration), grouped by `workflow_name` (default) or a `search_attr:<key>` tenant key, fanned out and summed across every shard. Each record reports `workflow_starts`, terminal outcomes (`completed`/`failed`/`cancelled`/`timed_out`), `activity_executions`, `activity_executions_failed`, and `activity_compute_seconds` (summed final-attempt activity durations). Executions lacking the requested `search_attr` key are grouped under an explicit `"(unattributed)"` bucket rather than dropped. A window wider than a configurable ceiling (default 90 days, `HarvestApiState::set_usage_window_ceiling`) returns `400` naming the ceiling. An unreachable shard is named in `unavailable_shards` rather than failing the call wholesale. New `harvest usage --from … --to … [--group-by …]` CLI subcommand renders a tabular report by default (`--json` for piping). Documented alongside `/admin/concurrency` in `docs/sharding.md`. No new `WorkflowEvent` variant, no migration, no replay-determinism impact.

- Add grouped workflow-count visibility endpoint for fleet snapshots (issue #544). New read-only `GET /workflows/count` endpoint returns execution counts grouped by `state` and/or `workflow_name` (via `group_by`, default `state`) across all shards in one request, answering "how many RUNNING/FAILED per workflow type, right now?" without paginating `GET /workflows` or hand-querying every shard database. Filters (`workflow_name`, `state`, `started_after`, `started_before`) mirror `GET /workflows`. Counts are computed with a real per-shard SQL `GROUP BY … COUNT(*)` (`autumn_harvest::execution::count_workflow_executions_grouped`) and summed across shards in-process, so the response stays cheap at any execution volume. Bounded cardinality: the response caps returned groups at `limit_groups` (default 50, max 500), rolling the long tail into a single `other: true` group. An unreachable shard is named in `unavailable_shards` with a reason rather than failing the call wholesale; `status` reports `complete`/`partial`/`unavailable`. The response is documented as an eventually-consistent point-in-time snapshot with no replay or ordering guarantee under concurrent writes. No new `WorkflowEvent` variant, no JSON-tagging change, no migration.

- Make `WorkflowSimulator` honor `RetryPolicy` for failure-path tests (issue #541). Mocked activities that return `Err` are now retried by the simulator according to the activity's resolved `RetryPolicy` (from the registered `ActivityInfo` via `WorkflowSimulator::with_activity_info`, an explicit `WorkflowSimulator::with_retry_policy` override, or a call-site override), including typed non-retryable fast-fail classification (`ActivityFailure`, issue #227) and correct, incrementing `attempt` numbers on `ActivityFailed` events — never a hardcoded `1`. New `WorkflowSimulator::mock_activity_with_attempt` lets a mock observe the current attempt number. Backoff is advanced logically with no real-time sleeps. `Saga`-based compensation is reachable from a Postgres-free unit test. No new `WorkflowEvent` variant, no schema change, no migration.

- Add paginated, filterable single-execution workflow history API (issue #529). New read-only `GET /workflows/{id}/history` endpoint supports keyset pagination via opaque `next_cursor` (backed by `harvest_events.id`), `limit` (1–1000, default 100), `after` cursor, and repeatable `event_type` filter for server-side event-type narrowing. Response includes `total_events` (unfiltered) and `last_event_id` for UI progress tracking. `GET /workflows/{id}` now bounds `history` to the first 100 events and adds `history_truncated: bool` and `history_endpoint: string` fields so callers can discover the pagination endpoint. No new `WorkflowEvent` variant, no migration.

- Add workflow-type reachability check to gate safe handler removal (issue #520). New read-only `GET /admin/workflow-types/reachability` endpoint and `harvest workflow-types reachability [--type] [--json]` CLI subcommand report, per workflow type, how many non-terminal executions still depend on its handler and a `safe_to_remove` / `in_use` / `orphaned` verdict. The CLI exits `2` when any type is `orphaned` or the cross-shard answer is incomplete, so it is usable as a CI/deploy gate. No new `WorkflowEvent` variant, no migration.

- Add deadline-aware continue-as-new for long-lived, low-event workflows (issue #772). `ctx.should_continue_as_new()` gains a second, independent trigger: it also returns `true` once a run has consumed a configurable fraction (default 0.8) of its `execution_timeout` budget, so an author can continue-as-new before the hard deadline truncates a healthy run. Additive `WorkflowContext`/replay logic — no new `WorkflowEvent` variant, no migration.

- Add child-or-deadline waits for bounded sub-orchestrations (issue #779). `WorkflowContext::execute_child_workflow_timeout::<O>(info, input, timeout)` (and untyped `spawn_child_workflow_timeout`) bound a child-workflow await with a durable deadline, mirroring `receive_signal_timeout` one level up: `Ok(Some(output))` on child success, `Ok(None)` when the deadline fires first (the still-running child is request-cancelled), and `Err(_)` with the typed failure (#767) preserved on child failure. No new `WorkflowEvent` variant, no migration.

- Add signal & update lifecycle metrics (issue #684). Six new additive no-op-default `MetricsRecorder` counters give first-class, alertable visibility into signal delivery/consumption and the update admit→complete/fail path, bridged in `metrics_rs_adapter`. No new `WorkflowEvent` variant, no migration, no macro-path change.

- Add business-`workflow_id` HTTP addressing for existing workflows (issue #805). Every act-on-existing management route (`GET /workflows/{id}`, `/result`, signal/query/cancel/pause/resume/stack/children) gains a business-id form addressed by `(workflow_name, workflow_id)` alongside the existing `exec_id` routes (which stay byte-for-byte backward compatible), so an embedder can act on a workflow by the id it assigned and always reach the live run rather than a stale continue-as-new/reset predecessor.

- Add the `harvest det-check` CLI, the runnable front door for the `det_check` determinism engine (issue #778). Exposes DET001–DET011 with `--format json`, 1-based columns, and one first-party hop of transitive reachability — flagging a non-deterministic call factored out of a `#[workflow]` body into a first-party helper the body calls directly, via a new `DetFinding.via_helper` attribution. Read-only source analysis: no `WorkflowEvent` variant, no migration, no runtime code touched.

- Add workflow run metadata via `ctx.info()` (issue #698). One read-only, replay-safe accessor returns a `WorkflowExecutionInfo` bundling the run's system metadata (`execution_id`, `workflow_id`, `workflow_type`, `start_time`, `history_event_count`, `is_replaying`, `parent_execution_id`) — the identifiers operator surfaces key on, which author code previously could not read. Leaves zero `harvest_events` footprint. No new `WorkflowEvent` variant, no migration.

- Add the `autumn-harvest-sqlite` companion crate (issue #1068). A new workspace member providing an embedded, single-writer SQLite backend for the workflow engine, targeting edge / local-first deployments; productizes the R&D feasibility spike as a separate companion crate, leaving the Postgres path completely untouched.

- Add WASM-sandboxed polyglot activities inside the Rust worker (issue #965). An R&D spike (not a committed GA feature) proving an activity can run as a WASM-sandboxed, non-Rust module inside the Rust worker.

- Add cause-targeted bulk DLQ replay/discard plus `dlq_reason`/`error_class` aggregate facets (issue #613). `GET /dead-letters/aggregate?group_by=dlq_reason` (or `error_class`) shows a flood's cause shape, and a facet's cause tuple feeds back into `POST /dead-letters/replay|discard` (and `harvest dlq replay/discard --dlq-reason poison_pill`) to act on exactly the counted cohort — a two-call incident runbook.

- Add Vantage UI DAG run-graph page (issue #957) and execution timeline/Gantt page (issue #960), both server-rendered inline SVG in the plugin `ui.rs` (the Workers/DLQ precedent — no JS), read-only except one already-audited mutation. No new backend endpoint, no new `WorkflowEvent` variant, no migration, no core change.

- Add a read-only operator role for least-privilege triage access (issue #776). One builder call promotes `autumn_harvest::audit`'s route classification to authz: a principal marked read-only reaches 100% of `RouteClass::ReadOnly` routes but gets 403 on every `Mutating` management route and every mutating MCP tool — replacing a custom reverse proxy for exposing status dashboards to support/on-call users.

- Add published workflow interaction schema, `/interface` discovery, and boundary validation (issue #610). Extends the #373 input/output schema story to a workflow's signals, queries, and updates: each handler can publish an argument/response JSON Schema and description (new `SignalHandlerInfo`, schema-feature `with_schemas`, `signals![…]` collector, `#[query]`/`#[update]`/`#[signal]` `description =`), surfaced by the new read-only `GET /workflows/registered/{name}/interface`.

- Add workflow start provenance (`StartSource`) across every start path (issue #740). Every execution now durably records what created it and from where, via a new bounded, non-`db`-gated `StartSource` enum (snake_case serialization; `from_str` falls back to `unknown` and never errors) threaded through `StartWorkflowParams`/`NewWorkflowExecution` and stamped at every start path — replacing reverse-engineering of `workflow_id` prefixes and hand-walking `parent_id`.

- Add live workflow output streaming via `ctx.publish_progress` (issue #791). One replay-safe method — `WorkflowContext::publish_progress(chunk: impl Serialize)` — emits an ordered, author-defined business-content chunk from live workflow code, delivered to a subscriber over a new SSE route, so an AI agent / long-running import can push incremental output to a waiting client without an external message bus.

- Add scoped API tokens plus rotation for the management API (issue #942). A first-class least-privilege credential layer: create/list/revoke scoped, optionally-expiring, individually-revocable tokens with every mutating operation attributable to a named actor. Entirely a plugin auth layer plus one additive config-table migration — no new `WorkflowEvent` variant, zero replay-determinism impact, `harvest_events` untouched.

- Add a workflow-type reachability follow-up: representative samples, a top-level orphan summary, and a startup-read boot gate (issue #700). Closes the four re-scoped ACs on the shipped reachability check (#520). No new `WorkflowEvent` variant, no migration, no new route — read-path plus startup-read, entirely additive.

- Add the `harvest new <name> [--path --force --template minimal]` project-scaffolding CLI subcommand (issue #692), generating a runnable starter Harvest project locally.

- Add an auto-heartbeat guard for long-running activities (issue #682), so an activity that runs longer than its `heartbeat_timeout` but makes no manual heartbeat call is not spuriously timed out.

- Add an active-workflow population gauge `harvest.workflow.active` (issue #770), a continuously-sampled gauge of currently-running workflow executions.

- Add durable await of external workflows via `ctx.await_external_workflow` (issue #757), letting a running workflow durably block until an independently-started sibling reaches a terminal state and then read its typed result or terminal cause.

- Add bounded / windowed activity fan-out (issue #750). Four new `WorkflowContext` methods add a `max_in_flight` window `W` to each existing fan-out shape (`execute_activity_fan_out_windowed`/`_collect_windowed`/`_raw_windowed`/`_collect_raw_windowed`), applying natural backpressure to workers and downstreams instead of scheduling every input at once — a pure `context.rs` scheduling change with no new `WorkflowEvent` variant, no migration, no worker/replay change.

- Add a chain-scoped workflow lifetime cap (`chain_execution_timeout`, issue #617), distinct from the per-run `execution_timeout`: anchored at the first run's start and carried verbatim across every continue-as-new so a runaway loop cannot escape it by continuing-as-new, enforced by the existing timeout scanner and `WorkflowExecutionTimedOut` event. No new `WorkflowEvent` variant.

- Add a single-execution replay diagnosis endpoint (issue #614): a new read-only, admin-gated `POST /api/harvest/workflows/{id}/replay-diagnosis` route.

- Add `WorkflowIdConflictPolicy` for idempotent plain workflow starts (issue #685). Splits the overloaded `WorkflowIdReusePolicy` into two orthogonal axes (reuse for terminal priors, conflict for active priors) so an idempotent plain start can express "attach to a currently-running run" without abusing reuse semantics designed for terminal priors.

- Add server-side overdue-schedule detection (issue #696). Since Harvest stores every schedule's `next_run_at` + cadence in Postgres, it now computes "overdue" authoritatively server-side and names which schedule is wedged and how late — replacing the brittle absence-of-runs inference. Read/observability slice over existing columns: no new `WorkflowEvent` variant, no migration.

- Add per-key activity rate limits (issue #699). Extends the static per-activity rate limiter (#332) so an activity's bucket key is resolved per-execution from the workflow input via a dot-path — giving each tenant its own RPS bucket — via a new nested `#[activity(rate_limit(key = "input.tenant_id", rps = 50, burst = 10))]` attribute (rejected at compile time on `local = true` activities); the flat `rate_limit_*` attributes are unchanged.

- Add DAG node input binding to pass data between graph nodes (issue #702). A `#[dag]` node's activity input can now be bound directly to one or more upstream node outputs — one builder call per data edge — so an extract→transform→load pipeline consumes each prior stage's output without flattening into a mega-activity or hand-threading through shared state.

- Add a dry-run preview for batch operations (issue #769). `POST /api/harvest/batch-operations` (the fleet-wide Cancel/Terminate/Signal surface) gains an additive `dry_run: bool` (default false) so an operator sees the exact blast radius (count + a bounded sample) across all shards before committing.

- Add an update admit→terminal latency histogram `harvest.update.duration` (issue #781), recording wall-clock seconds from an update's durable `UpdateAdmitted` to its terminal `UpdateCompleted`/`UpdateFailed` — the p99 latency companion to the #684 counters. No new `WorkflowEvent` variant, no migration, no macro-path change.

- Add an opt-in built-in synthetic liveness canary (issue #796) that continuously probes the live start→dispatch→activity→durable-timer→complete path so a wedged pipeline is detected within one probe interval, with zero operator-authored workflow code — distinct from the #512 replay canary, which validates code changes rather than the running pipeline.

- Add a durable named cross-workflow mutex (issue #691). `WorkflowContext::mutex(key).acquire().await` durably suspends a `#[workflow]` until it holds exclusive access to `key`, returning a `#[must_use]` RAII `MutexGuard` that releases on drop / scope exit, explicit `release()`, the holder reaching any terminal state, or lease-TTL expiry — the replay-safe answer to "two executions must not read-modify-write the same shared resource at once" (a workflow cannot reach for a `std::sync::Mutex`: it would not survive a worker restart, and holding a std lock across a durable `.await` breaks replay). Unlike per-key concurrency (#247) — which gates admission at workflow *start* and caps the running *count* of whole workflows sharing a key — this gates a critical *region* mid-workflow, so only the section serializes while the rest of each workflow runs concurrently. FIFO-fair via a BIGSERIAL head-of-line waiter queue; lease-based crash safety with a `lock_seq` fencing token and an advisory-first reclaim scanner; terminal auto-release folded into the single terminal chokepoint; non-reentrant (`HarvestError::MutexSelfDeadlock`, checked before any history match); shard-local scope (contending workflows must resolve to the same shard, matching #247). Records exactly one append-only `WorkflowEvent` tail variant `MutexGranted` (release is event-less bookkeeping) and one migration (`20260714000001_harvest_mutex_locks`); three metrics `harvest.mutex.wait_duration`/`held_duration`/`contention_depth` (`workflow`-labeled only — the lock key is high-cardinality tenant input). `MutexHandle`/`MutexGuard` re-exported from `lib.rs` and the prelude; example `autumn-harvest/examples/mutex_ledger.rs`.

### Changed

- **Behavior change (issue #767):** a failed **child** workflow now surfaces to its parent as `HarvestError::WorkflowFailed` (typed) instead of `HarvestError::ActivityFailed { name: "child-workflow:{name}" }`. The `child-workflow:{name}` name prefix is preserved. Downstream parent code that matched `ActivityFailed` for a child result — or called `.activity_error_type()`/`.activity_details()` on it — must switch to the `WorkflowFailed` variant and the `workflow_error_type()`/`workflow_details()`/`is_workflow_non_retryable()` accessors.

- **Source-breaking additive public fields (issue #767):** `HarvestError::WorkflowFailed`, `WorkflowEvent::WorkflowFailed`, `WorkflowEvent::ChildWorkflowFailed`, `replay::HistoryMatch::Failed`, and `TypedWorkflowResult` each gained new fields (`error_type`/`details`/`non_retryable`, as applicable). Downstream code that exhaustively destructures any of these variants/structs must add `..`.

- Make the admission gate authoritative across all in-process workflow-start producers (issue #618). The #377 gate is now either gated or explicitly, observably exempt on every producer: completion triggers reach the shared gate cache via a new process-global static and a block records a clean, exactly-once resolved-skip (`outcome = 'admission_blocked'`) that never rolls back the source terminal; the outbox relay is exempt-by-design and counts a new `harvest.admission.bypassed{producer}` metric; and `GET /admin/gates` now returns a discoverable per-producer contract. No new `WorkflowEvent` variant, no migration.

- Harden the admission gate on the throttle scanner: fire-time gate check plus reuse-policy-aware batch bypass (issue #1053). The throttle scanner now re-checks the admission gate immediately before firing a deferred start and, on a match, blocks-and-re-defers the row (refunds the reserved token, bumps `deferred_at` for cross-key fairness, counts `harvest.admission.blocked`) — a deliberate contract change so a throttle `202` now means "will run when tokens allow AND the gate is open at fire time"; `throttle` moves from `gated_at_admission` to `gated_at_relay` in the producer contract. No new `WorkflowEvent` variant, no migration.

- Harden `autumn-harvest-sqlite` v0.1 (issue #1068): freeze activity defaults, complete the `WorkflowIdReusePolicy` matrix, and add race fairness. Confined to the `autumn-harvest-sqlite` crate — zero core changes.

- Return partial availability for cross-shard fan-out read endpoints (issue #756). A single unreachable shard no longer turns every cross-shard read into a whole-request `500`; these endpoints now collect-and-continue, returning `200 OK` with the reachable shards' union, a machine-readable `unavailable_shards`, and a `status` verdict. Writes keep strict failure semantics, and the single-execution business-id resolver keeps its `503`-to-avoid-false-`404` behavior.

### Fixed

- Reject `0.0.0.0/8` and `198.18.0.0/15` callback targets in the completion-callback SSRF guard (issue #605 hardening, PR #1006). The validator previously blocked the bare `0.0.0.0` but not the rest of `0.0.0.0/8` (which routes to localhost on Linux) nor the `198.18.0.0/15` benchmarking range; both are now rejected as `SsrfRejection::IpNotRoutable` (IPv6 v4-mapped forms inherit the fix through the existing canonicalization), closing two IP-literal bypasses surfaced by the new PR #1004 property/fuzz coverage. Validator + tests only — no new rejection code, no `WorkflowEvent` variant, no migration, no API/wire change.

- Fix the workflow `/stack` endpoint's `pending_local_activities` list (PR #1022, issues #773/#608). Three defects, all in the same fold in `autumn-harvest-plugin/src/api.rs`: (1) it read the event's `activity_id`/`name` at the top level of the adjacently-tagged `event_data` envelope instead of under `.data`, so the list was always empty; (2) it never consumed the terminal `LocalActivityExhausted` event, so a caught-and-continued exhausted local activity stayed listed forever; (3) with the read-path decoder (#608) active it decoded a removed entry's `last_failure` before the terminal-remove arm dropped it, writing a spurious `payload.decode_read` audit row for un-surfaced data. Fixed: read under `.data`, treat `LocalActivityExhausted` as a terminal close, and decode `last_failure` only for surviving entries in a single post-fold pass. No public API change, no new `WorkflowEvent` variant, no migration.

- Fix a test-fixture schema omission that reddened `trunk-dev` (PR #1022, issue #767). Two `db_handle_surface` typed-workflow-failure tests built their testcontainers schema from a hand-rolled `INIT_SQL` list that omitted the `20260709000001_harvest_legal_hold` migration (issue #747), so once `WorkflowExecution::as_select()` selected the new `legal_hold_*` columns, loading any execution row errored (`column ... legal_hold_set_at does not exist`) and the handle surfaced `Database(...)` instead of the typed `WorkflowFailed`. Added the missing migration to the include list — test-fixture only, no production code change.

- Fix an inline-resolved mixed timer + external-op wake latency bug (issue #678). A `select!{ timer(3600s), signal_external_workflow(target) }` (or `request_cancel_external_workflow`) whose external target lives on the same shard could park its timer for the full timer duration instead of resolving on the external branch that had already completed.

- Fix two #476 signal-or-deadline race follow-ups ported from the #779 child-timeout twin. A timer-retention leak on signal-win (the still-armed `__signal_timeout` durable timer was left `fired=false` forever, pinning the run against retention) now pushes a gated `CancelRaceLosers` teardown; and a canary-frontier false non-determinism (`match_signal_or_timer`'s `InProgress` arm erroring under strict replay for a parked race) is now suppressed at the canary frontier. Supersedes the stale Phase 3.24 note about the armed timer leaking. No new `WorkflowEvent` variant, no migration.

- Fix an inline-resolved external-op wake bug across every standard mixed-suspension shape (issue #1034, follow-up to #678). A workflow racing `signal_external_workflow`/`request_cancel_external_workflow` against a non-timer branch in one decision cycle, with the target on the same shard, failed to wake on the inline-resolved external terminal (a latency bug, and for one shape an indefinite hang).

- Fix a policy-blind admission-gate bypass on the single-start throttle route (issue #607). The committed-prior idempotent-retry bypass treated any non-sealed prior as an attach, letting a reuse policy that actually creates a fresh replacement (`allow_duplicate_failed_only`/`terminate_if_running`) slip an armed gate via the token-empty deferred path; the bypass is now reuse-policy-aware and skips the gate only when the start will not create a new execution.

- Fix query-replay suspension misclassification so a spinning terminal query returns `408`, not `410` (issue #612). `executor::drive_query_replay` inferred workflow suspension from a custom waker flag assuming `tokio::task::yield_now()` wakes synchronously, but inside a tokio runtime `yield_now` defers its wake to the scheduler — so a genuinely-spinning terminal query was misread as a command-suspension.

- Fix a race-primitive canary replay false-positive on interleaved bookkeeping siblings (issue #1048). A canary replay of an in-flight `wait_for_signal_timeout`/`spawn_child_workflow_timeout` race started concurrently with a bookkeeping side effect incorrectly reported `NonDeterminismDetected`, because the inline canary-frontier guard misread the momentarily-rewound cursor as "not at frontier".

- Fix a pre-existing replay limitation where interleaved sibling terminals broke forward-scan matchers (issue #1071). An ideal, correct history that interleaves a sibling terminal (e.g. a timer-first `tokio::join!(timer, activity)` mixed suspension) next to the event a matcher scans for falsely failed with `NonDeterminismDetected(EarlyCompletion)`; the forward scans now tolerate the interleaved sibling.

- Fix `match_signal` to cross only its own deadline timer during replay (issue #1071 follow-up). The interleaved-sibling arm added in #1084 crossed any reserved `__signal_timeout:*` timer keyed only on the prefix, so a plain `wait_for_signal("a")` racing a `receive_signal_timeout("b", …)` in one batch advanced the cursor past a foreign signal's deadline events without a rewind, causing a spurious strict-replay divergence.

- Enforce the start-route capability-precise admin gate before the fail-closed runtime check (issue #685 follow-up). After #1100 reordered the gate to run after both policy parses (which sit after the runtime check), the no-DB security test regressed; the gate is moved back ahead of the runtime check, restoring the intended ordering (not a live-deployment security hole in a running deployment).

- Fix with-start keyed committed-replay validation ordering (issue #1092 follow-up). `signal_with_start_workflow`/`update_with_start_workflow` ran fresh-start-only validation (payload/schema #610, `start_input` schema #373, the #684 validator) before idempotency dedup, so a retry of an already-committed keyed with-start was rejected `400`/`422` whenever validation had tightened between the original delivery and the retry; an early read-only committed-replay probe now short-circuits to the documented no-op first.

### Internal

- Add a test INIT_SQL bundle migration-drift guard (PR #1031) so a hand-maintained `include_str!` schema bundle that misses a new migration column becomes a hard CI error instead of a runtime-only failure, plus a drift-proof shared schema bundle and fixes for the concrete known gaps. No product-code behavior change.

- Wire three previously-never-run db-gated core integration suites (`workflow_retry_tests`, `completion_callback_tests`, `event_batch_tests`) into Docker-backed CI (PR #1037), fixing 5 test-harness bugs each already contradicted by a passing sibling. No product-code behavior change.

- Replace ~35 copy-pasted per-suite CI `cargo test` steps with one manifest-driven integration-suite runner (`.github/ci/run-suites.sh` + a merge-safe `integration-suites.txt`), so adding a suite is one sorted manifest line with no `ci.yml` edit.

- Document a pre-existing child-timeout hard-cap deadline-cleanup known limitation (PR #1041, follow-up to #1029): if the parent hits its event-history hard cap on the same cycle the deadline branch resolves, the seal bypasses `persist_terminal_outcome_commands` and drops the `CancelRaceLosers` cleanup, leaving the over-deadline child un-cancelled. Docs-only.

- Sweep 87 db-gated integration-test fixtures off hand-rolled `concat!(include_str!(...))` migration bundles onto the paved-path `autumn_harvest::full_migrations_sql()` helper (PR #1045, net −9918 lines), deleting stale bundles that had drifted 8–71 migrations behind. No product-code behavior change.

- Guard against reintroducing hand-rolled migration bundles (PR #1049): a no-DB guard in `migration_hygiene.rs` fails if any test file contains a hand-rolled migration `include_str!` outside a small commented allowlist, making the `full_migrations_sql()` paved path self-enforcing. No product-code behavior change.

- Fix three pre-existing test-harness failures and move the now-green `metrics_integration` suite onto a real CI run row (PR #1050): a stale `FAILED`-vs-nd-`RUNNING` assertion, a microsecond/nanosecond timestamp self-comparison flake, and a nonexistent-column snapshot. No production code changed.

- Heal the batch-route gate-bypass test after the #1085/#1053/#1073 merge burst (PR #1093). Test-code-only.

- Heal two RED `trunk-dev` failures after a merge burst (PR #1094). Tooling only.

- Add `StartSource` fields to the `ui_integration` test literals after the #1095 × #1085 semantic merge conflict (PR #1096). Test-code-only, no production code changed.

- Gate the `webhook_receiver_integration` plugin suite into CI and fix its `TestApp::plugin` deadlock (PR #1097), un-`#[ignore]`ing 5 of 6 tests and wiring the suite into the manifest to run against Docker Postgres. Test-harness change only — no production webhook code touched.

- Add an honest competitor comparison page `docs/comparison.md` (issue #963) positioning autumn-harvest against Temporal, DBOS, Inngest, Hatchet, and Restate, naming harvest's own gaps plainly. Docs-only.

- Publish the admission gate to the process-global cache in the keyed-replay gate test to fix trunk-red (issue #618/#808). Test-only fix — no production code changed.

- De-flake the `nd_blocked_cycle_does_not_emit_signal_unhandled` engine test (Refs #1074). Test-code-only.

- 0.5.0 documentation sweep + migration guide (PR #1130): verified every published doc and runnable example against the shipped source, added the `docs/upgrading/0.5.0.md` upgrade guide covering the 0.4.0 → 0.5.0 transition, filled documentation-coverage gaps for features that landed without a corresponding docs page, and cleaned up stale org-rename links across the docs tree. Docs-only — no code, no `WorkflowEvent` variant, no migration, no test change.

## [0.4.0] - 2026-06-16

### Added

- Add last-completion-result carryover for scheduled workflows (issue #488) (#677)([16e7334](https://github.com/madmax983/autumn-harvest/commit/16e7334933d115b7e767c8a9a2d256ee479a0092))

- Add external workflow cancellation primitive (issue #492) (#676)([33595ff](https://github.com/madmax983/autumn-harvest/commit/33595ff5890efeb75c95a71f0f6281ddb0fdc9a8))

- Add soft SLA breach detection for workflow executions (issue #487) (#671)([f85ba1b](https://github.com/madmax983/autumn-harvest/commit/f85ba1b28526f02d060fd5533cbf5aaf236ec00c))

- Add bounded catchup window policy for scheduled workflows (issue #484) (#669)([b11e5a9](https://github.com/madmax983/autumn-harvest/commit/b11e5a9197d17cbe616b990ecf01a448cccbbf99))

- Issue #482: Data-dependent DAG branching with condition predicates (#667)([774079b](https://github.com/madmax983/autumn-harvest/commit/774079bdc522375d29537926dd562945d2a6e2e1))

- Add per-execution context headers (issue #481) (#664)([44f2af3](https://github.com/madmax983/autumn-harvest/commit/44f2af383216cab8c6b52eb165cb6ac7c1d231ad))

- Add stalled-workflow discovery filter to GET /workflows (issue #486) (#663)([3a63370](https://github.com/madmax983/autumn-harvest/commit/3a63370d37635412a0c79f9fc9be6cf0792fb125))

- Implement non-determinism deploy detection (#480) (#652)([53fd0c9](https://github.com/madmax983/autumn-harvest/commit/53fd0c968455f7c10a9524e7494e435017d9c048))

- Add bounded schedule runs: end_at and max_runs (issue #478) (#651)([e4a3479](https://github.com/madmax983/autumn-harvest/commit/e4a3479268fa530b0500ceaaa44de2fdc3d4826d))

- Add update-with-start atomic primitive (issue #479) (#649)([5ac5716](https://github.com/madmax983/autumn-harvest/commit/5ac5716af41b35f34475bdad3c18bd7840622da7))

- **macro:** Implement compile-time determinism check guardrails for #[workflow] (#640)([9a7aafb](https://github.com/madmax983/autumn-harvest/commit/9a7aafb9d8f0489203ce487803bc5608fcc6399e))

- Add signal-or-deadline race primitive (issue #476) (#645)([0d58067](https://github.com/madmax983/autumn-harvest/commit/0d5806799b3cc28bfe7a4211cab2d3a981a7cd5c))

- Add set_current_details() for durable workflow status breadcrumbs (#643)([7254bb1](https://github.com/madmax983/autumn-harvest/commit/7254bb1b182fe30b187604e40a439e2c8f5f304f))

- Add operator pause/resume primitive for workflow executions (#383) (#630)([b56a916](https://github.com/madmax983/autumn-harvest/commit/b56a916d0258ab680b88ae2d9050ac3978485ae1))

- Implement worker capability labels and activity routing (issue #382) (#622)([1ba16d6](https://github.com/madmax983/autumn-harvest/commit/1ba16d68dd993d3470b2e1573fcddaed53efddbb))

- Add deterministic side-effect primitives to WorkflowContext (issue #384) (#628)([c702e68](https://github.com/madmax983/autumn-harvest/commit/c702e6877f98d843d7f850c27c4be587499e8bd2))

- Add DLQ root-cause aggregation API for fast incident triage (#385) (#629)([3783037](https://github.com/madmax983/autumn-harvest/commit/37830378e841ff091fdd19a4975306c51f8eb26a))

- Implement queue & task eligibility explainer for stuck-task triage (#595)([a41087e](https://github.com/madmax983/autumn-harvest/commit/a41087e6d4568d1769ae1d599ee01f7b82893a08))

- Add worker-level retry context: attempt(), previous_failure(), max_attempts() (#616)([6a0834c](https://github.com/madmax983/autumn-harvest/commit/6a0834c8844665e7c11e1314c0b8214f8d19a4a5))

- Add harvest.workflow.terminal counter for per-outcome success-rate SLO (#594)([c9ff68a](https://github.com/madmax983/autumn-harvest/commit/c9ff68a473796d12aa006286cb0de38f7e182ce2))

- Add replay-safe WorkflowLogger and HVG009 guardrail (issue #379) (#587)([0e24d26](https://github.com/madmax983/autumn-harvest/commit/0e24d266fb929eeb9c1320881d65faa58d012944))

- Display timezone next to cron expression in Vantage UI schedules page (#588)([e59293c](https://github.com/madmax983/autumn-harvest/commit/e59293c1a443262eb4c0f39de3f4827ea851579a))

- Add schedule-to-close timeout for cross-retry wall-clock deadlines (#586)([2b1e75a](https://github.com/madmax983/autumn-harvest/commit/2b1e75af87186a7543291ae72b04f99608d531af))

- **dag:** Implement dynamic task mapping (fan-out) for DAGs (#585)([3fbf957](https://github.com/madmax983/autumn-harvest/commit/3fbf957b708f7e42391a10cb624d3200d58a570c))

- Add admission gate primitive for incident-response workflow halts (#579)([7974582](https://github.com/madmax983/autumn-harvest/commit/797458248ed0108761ee1b5020b1256e0aa6cde2))

- Implement declarative completion triggers (issue #517) (#561)([96e82bf](https://github.com/madmax983/autumn-harvest/commit/96e82bfd10fe633c0d3887d0264bf9398053cf7c))

- **metadata:** Implement workflow metadata ownership (#372) (#550)([6b3fb18](https://github.com/madmax983/autumn-harvest/commit/6b3fb18c0ab288bf9a1a8bc774b0e54b20fb2d86))

- Add JSON Schema opt-in for workflow input validation (issue #373) (#556)([7f5323d](https://github.com/madmax983/autumn-harvest/commit/7f5323d9d7fba5004791462e1c1d7d8671416318))

- Add per-activity circuit breaker for downstream outage fast-fail (issue #369) (#549)([c03e1d8](https://github.com/madmax983/autumn-harvest/commit/c03e1d8d2b2908b80d9c834f0a8ecfe5d8fadb7a))

- Implement poison-pill task quarantine (issue #367) (#539)([31c8305](https://github.com/madmax983/autumn-harvest/commit/31c8305c1838875f131896ef9e33587827fec851))

- Add DAG retry-from-failed-node operator surface (issue #366) (#530)([f80154e](https://github.com/madmax983/autumn-harvest/commit/f80154e0e981b5e1147b52b27424239e4936e4f0))

- Auto-pause schedules after consecutive execution failures (issue #360) (#500)([24af11f](https://github.com/madmax983/autumn-harvest/commit/24af11f7819d3a26f49885932ef6dfa33b4a5221))

- Add fan-out / parallel activity execution (issue #359) (#497)([b3535fe](https://github.com/madmax983/autumn-harvest/commit/b3535fe140783f39d78a7ce548d3f0b5ef8ddb2c))

- Add POST /workflows/batch_start endpoint for bulk workflow execution (#496)([2ca0a6d](https://github.com/madmax983/autumn-harvest/commit/2ca0a6dcbea59bb1bb5c79876d865c3d89994959))

- Add HA scheduler claim protocol for multi-replica deployments (issue #350) (#490)([0a3c4f8](https://github.com/madmax983/autumn-harvest/commit/0a3c4f8b3e39de10154c382a509dc70134cfdb72))

- Add transactional activities (issue #352) (#491)([51fe1ab](https://github.com/madmax983/autumn-harvest/commit/51fe1ab36a46517f7b689c1a8094aa001958e630))

- Propagate workflow cancellation to in-flight activities via heartbeat (#483)([246378a](https://github.com/madmax983/autumn-harvest/commit/246378aa82884beaa3b90ccfb4c355fa7ce0429a))

- Add DAGs listing and detail pages to Harvest UI (#424)([8444dc9](https://github.com/madmax983/autumn-harvest/commit/8444dc9c2c168d8ba029248b5c54289a2866d712))

- Add schedule next-fires preview API (issue #348) (#474)([db0387a](https://github.com/madmax983/autumn-harvest/commit/db0387ae729bb59da82cb80d0a285e6193fae1d6))

- Implement detached child workflow spawns with parent-close policies (#453)([17a7036](https://github.com/madmax983/autumn-harvest/commit/17a703651f24f784c96300e7717ae658b894e11d))

- Implement webhooks feature gate with durable signed webhook delivery workflow and activity (#452)([e3f8676](https://github.com/madmax983/autumn-harvest/commit/e3f86762f675b181c1997a7a0ede67895e635311))

- Add Build Routing UI page and API endpoints (issue #362) (#448)([5f4975c](https://github.com/madmax983/autumn-harvest/commit/5f4975c3966be6ccada02269909bd0feebd25ec6))

- Add schedule trigger-now endpoint (issue #343) (#450)([472cc80](https://github.com/madmax983/autumn-harvest/commit/472cc80b41ff28e3184bd3ec8d471b4fa6a28ef5))

- Implement compile-time type-safe workflow client stubs and handles (#341) (#443)([29ec69f](https://github.com/madmax983/autumn-harvest/commit/29ec69f4892f19b22fa109da1077e993b6d52726))

- **await-condition:** Implement await_condition and await_condition_timeout (issue #340) (#439)([9a34b15](https://github.com/madmax983/autumn-harvest/commit/9a34b15b4c4ba08a5377221cde7f550365f52f14))

- **retention:** Implement pre-retention history archival hook (issue #345) (#432)([a4927f7](https://github.com/madmax983/autumn-harvest/commit/a4927f73073e1c5079e037f2e949d3e6b6228d4f))

- Deterministic retry jitter: add `JitterPolicy`, seeded delays, worker seeding, tests, docs and benchmark (#431)([3d24a53](https://github.com/madmax983/autumn-harvest/commit/3d24a53438de369b62135094d12bb36cc87edce4))

- **rate-limit:** Implement time-windowed per-activity downstream API protection (RPS rate limiting) (#332) (#428)([611de9b](https://github.com/madmax983/autumn-harvest/commit/611de9bed73a5ee4d6e083a16cf49da0d94d457c))

- Add DAGs pages to Vantage UI (list + detail) (#426)([a90ffe8](https://github.com/madmax983/autumn-harvest/commit/a90ffe8fc663eb50d183d84461aa24bfe0052a32))

- **signal:** Deterministic Cross-Workflow Signaling (#421)([1c6b996](https://github.com/madmax983/autumn-harvest/commit/1c6b996c80c97889a5184716f6e05d3ea33d2e47))

- Expose worker-pool scaling endpoints for KEDA (#325) (#420)([0aa07df](https://github.com/madmax983/autumn-harvest/commit/0aa07df0fe56e0245e02eac9a18d61c5a2adaa36))

- Implement schedule decision persistence (issue #325) (#419)([66c355d](https://github.com/madmax983/autumn-harvest/commit/66c355dd4f56be4b71d25cde16215d0d2cd36580))

- Persist Schedule Decisions (Issue #325) (#417)([0e5b441](https://github.com/madmax983/autumn-harvest/commit/0e5b44180c42e7a4d9b4916c2a6295bc4ea5aa68))

- Support delayed/future workflow execution start (Issue #322) (#403)([090226b](https://github.com/madmax983/autumn-harvest/commit/090226b99e300bd1f38acd9417938467ad7dadc8))

- Add SSE execution event stream endpoint (issue #324) (#402)([c08b9b0](https://github.com/madmax983/autumn-harvest/commit/c08b9b01551420b4b06a559447a4225a8760f217))

- Add calendar-aware schedule filtering (issue #337) (#399)([f5ca74a](https://github.com/madmax983/autumn-harvest/commit/f5ca74ac2fe0f701460b8de10f6185dd5d5d2a3b))

- Display history event count and continue-as-new threshold on workflow detail page (#398)([805c2f4](https://github.com/madmax983/autumn-harvest/commit/805c2f46a8fab3c5a48aaa03d0dfbcd4130efef0))

- Implement payload size caps for workflow payloads (issue #252) (#395)([2d903b0](https://github.com/madmax983/autumn-harvest/commit/2d903b05d4bbbab02f8d1fc3b8a2ff61f89057b4))

- Add ReplayVerifier batch CI gate for workflow determinism (issue #251) (#393)([a4c1299](https://github.com/madmax983/autumn-harvest/commit/a4c129976be1cb685eb75a7421026782a36e0abc))

- Add task priority support for within-queue ordering (issue #249) (#391)([e37b4f2](https://github.com/madmax983/autumn-harvest/commit/e37b4f2dc0104e5c82d661fa38710585e2469e4d))

- Add timezone-aware cron schedules (Schedule::CronInTimezone) (#389)([ee81805](https://github.com/madmax983/autumn-harvest/commit/ee818052126e7a6b50cb2f35848721bf0017a101))

- Add typed dispatch helpers for activities, workflows, and signals (#387)([68b4ceb](https://github.com/madmax983/autumn-harvest/commit/68b4ceb601f4aee5e43844d82ae031b9bc874afd))

- Add SignalWithStart primitive for atomic webhook handlers (issue #244) (#364)([d3729c4](https://github.com/madmax983/autumn-harvest/commit/d3729c485fe645f5884901b0edab51a5e702e7ec))

- Add per-key concurrency limits for tenant fair-share scheduling (#370)([14240b5](https://github.com/madmax983/autumn-harvest/commit/14240b52a93074755d90d06f267de6b29ec56aae))

- Add ctx.signal_external_workflow for saga choreography (issue #330) (#376)([cde9046](https://github.com/madmax983/autumn-harvest/commit/cde9046102c402be265187b70cbada57a815a4f4))

- Implement workflow execution timeout for SLA enforcement (issue #243) (#374)([5b74436](https://github.com/madmax983/autumn-harvest/commit/5b744368d67b4f3bf26dafd0e04e382f6b7dc03d))

- Add WorkflowTestEnv in-process unit-test harness (issue #250) (#375)([0e56b6e](https://github.com/madmax983/autumn-harvest/commit/0e56b6e1daf1c966bc66e0399a0ea7fe378260c0))

- Add schedule overlap policy for concurrent run collisions (issue #241) (#361)([995c1c4](https://github.com/madmax983/autumn-harvest/commit/995c1c40a37ba69815a79f0f324162b1e137d7de))

- Add TDD DB integration tests for sticky cross-worker routing (spec #35) (#363)([04f723a](https://github.com/madmax983/autumn-harvest/commit/04f723ab46f556192c6378788ab2867d0c2347a0))

- Add workflow detail page with operator actions and event timeline (#354)([1d73748](https://github.com/madmax983/autumn-harvest/commit/1d73748b440d2a728f620e57e15f50929ab13a12))

- Declarative query and update handlers (issue #346) (#353)([dfee5fc](https://github.com/madmax983/autumn-harvest/commit/dfee5fce5abb281ef5fc5ed60c57613745c9bfc4))

- Add deterministic jitter to schedule fire times (#351)([4bdb434](https://github.com/madmax983/autumn-harvest/commit/4bdb434ae49e2ff1f3bcc1c1e96967211cea7b96))

- Add saga cancellation semantics and idempotency contract (#339)([d415ab2](https://github.com/madmax983/autumn-harvest/commit/d415ab29292864baef8f24852025919303cfa9a7))

- Implement sticky cross-worker routing with in-process cache delta-load (issue #235) (#336)([59a10d9](https://github.com/madmax983/autumn-harvest/commit/59a10d92140dfbac3221720a249ff2ef918d6bfc))

- Issue #234: Read-only Query handlers for live workflow state inspection (#328)([ca0e166](https://github.com/madmax983/autumn-harvest/commit/ca0e16618952def10e18fee1f42da78add3ddc36))

- Add schedules management UI page with pause/resume/delete actions (#333)([bc8df91](https://github.com/madmax983/autumn-harvest/commit/bc8df91b604d09f00c6b37740a12fd4fe4df5bd6))

- Unify DAG execution onto workflow path (issue #256 Step 5) (#302)([4e914ca](https://github.com/madmax983/autumn-harvest/commit/4e914ca8484eee2a38f6442c26ef3f8992bc9202))

- Add Claude Code GitHub Workflow (#310)([cb0cb48](https://github.com/madmax983/autumn-harvest/commit/cb0cb48dd294fb75f26653190c5251544427167c))

- Add pause/resume metadata tracking for schedules (issue #229) (#301)([2da1c38](https://github.com/madmax983/autumn-harvest/commit/2da1c382ef5d6eee5b706694a7a9fabf76d72a22))

- Issue #227: Typed activity failure surface with error classification (#299)([511ea8e](https://github.com/madmax983/autumn-harvest/commit/511ea8e44c0e2261cdcd53a87a65cf08c0c7c1d2))

### Fixed

- Propagate WorkflowInfo defaults to scheduled, child, and trigger-now starts; fix false SLA breach on cancel-while-paused([ea6c173](https://github.com/madmax983/autumn-harvest/commit/ea6c173c6f543193a05548c56ca146df56cd844c))

- Signal/update-with-start inherit info execution_timeout, update-with-start runs validator, typed stub resolves concurrency([2a54431](https://github.com/madmax983/autumn-harvest/commit/2a5443119bf5bb2fecd0500da03e61710423dc96))

- DAG trigger-now uses pick_for_dag, workflow backfill gate uses actual default shard([f8fe156](https://github.com/madmax983/autumn-harvest/commit/f8fe156261a881d0833e9364d3d9a66f436df6c3))

- Auth guard on trigger route, atomic batch pre-validation, DAG backfill gates, rate-limit reporting([5710a1d](https://github.com/madmax983/autumn-harvest/commit/5710a1da9a1e913b6af1a67263063b433f8b56f5))

- Gate bypass covers all non-terminal states, rate-limit upsert propagates to all shards([6d9ffa5](https://github.com/madmax983/autumn-harvest/commit/6d9ffa5bb55954c4a553e86835f2a4a77cb9b969))

- Three Codex round-5 issues (P1 backfill gate, P2 input caps, P2 contract)([0d204ad](https://github.com/madmax983/autumn-harvest/commit/0d204ad4b6b7e0cfe020f29a8464a87e7fe04d0e))

- Clippy option_if_let_else in batch-start validation([df1ffc7](https://github.com/madmax983/autumn-harvest/commit/df1ffc7f29c28e629a861b213bd80c58b214fdc9))

- Five Codex round-4 P2 issues([2860cab](https://github.com/madmax983/autumn-harvest/commit/2860cab91a023f32d9de04d43e433f73b01cb7ab))

- Fix claim_task SQL column bug and address Codex review comments([874c6b7](https://github.com/madmax983/autumn-harvest/commit/874c6b7f46ba2d744bbb3b6cb1ea7e4491caa8ee))

- Fix four more correctness bugs flagged in second Codex review([ce6a8be](https://github.com/madmax983/autumn-harvest/commit/ce6a8be2b8c8b05c9a05f8cd4e380084cbf5ce2a))

- Fix three correctness bugs flagged in Codex review([ab2d896](https://github.com/madmax983/autumn-harvest/commit/ab2d896786415b58eb0ccf691f0d2abc849c5c17))

- SSE parser correctness — colon-split and multi-line data concatenation([eb30fc9](https://github.com/madmax983/autumn-harvest/commit/eb30fc9a8f5eb813c23be6b9ef2c9ec110803fa2))

- Remove duplicate sla field in cross_workflow_cancel_tests struct literals([7d67847](https://github.com/madmax983/autumn-harvest/commit/7d67847a25a6cb7699f3307dd49fd8c4355687f2))

- **plugin:** Require auth for cancel and dead-letter endpoints (#316)([50e7c3e](https://github.com/madmax983/autumn-harvest/commit/50e7c3ef092a1f6b49c3c32ea9720fb5bb5389c3))

### Performance

- Pre-allocate markers vector to reduce heap allocations (#290)([23f0796](https://github.com/madmax983/autumn-harvest/commit/23f0796e6d1d441973cd43645b58257d8229fc06))

### Miscellaneous

- Cargo clean before plugin lib tests to avoid disk exhaustion([d41207d](https://github.com/madmax983/autumn-harvest/commit/d41207d2a7b49af2b3bffd8e3def5e9dc2838292))

- Fix changelog (#300)([512796c](https://github.com/madmax983/autumn-harvest/commit/512796c4ae1c1a365e24b2958d9869f8a787e174))

## [0.3.0] - 2026-05-13
## [0.1.1] - 2026-04-19

### Added

- **harvest:** Add history guardrails for long-running workflows (#280)([212abd4](https://github.com/madmax983/autumn-harvest/commit/212abd474b6aa450d281d37e18616ed418afc10c))

- Add workflow handle result waiting (#276)([af2b7ea](https://github.com/madmax983/autumn-harvest/commit/af2b7ea3fe639edf808f023ffa98cce64bb94f79))

- Add schedule backfill API for recovering missed scheduled runs (#277)([fc58a3f](https://github.com/madmax983/autumn-harvest/commit/fc58a3f7da4fedf0937b1b81158296ee85466546))

- Add versioned management API contract and regression tests (#274)([761ff09](https://github.com/madmax983/autumn-harvest/commit/761ff09841ea203571442e23e83fa65a61d3dd7d))

- **det_check:** Add deterministic workflow guardrails (issue #172) (#265)([8794a63](https://github.com/madmax983/autumn-harvest/commit/8794a63d2f646ff3001e23aff968d523820e2fd0))

- Add test coverage for update.rs and version_usage.rs (#259)([8dc00d9](https://github.com/madmax983/autumn-harvest/commit/8dc00d9aa0c4c39678109e45c7c00a3c8405f752))

- Management API auth posture — route classification + security coverage (issue #174) (#267)([165e188](https://github.com/madmax983/autumn-harvest/commit/165e1880d3b9c7e20f614d4da94f0b7a5301ef2e))

- Add deterministic guardrail rule catalog foundation (issue #173) (#266)([7301229](https://github.com/madmax983/autumn-harvest/commit/7301229532059b4c55597f8c924d9a84c876f3b5))

- Add build-id routing for safe rolling deploys (issue #171) (#261)([91cad67](https://github.com/madmax983/autumn-harvest/commit/91cad67c944c8acb1253ee6104a0b6656fc58948))

- Add comprehensive getting started guide for Autumn Harvest (#254)([2877dca](https://github.com/madmax983/autumn-harvest/commit/2877dcad11460214e0e14c5791341623c1230a7a))

- Add remote worker drain controls (issue #170) (#246)([f011825](https://github.com/madmax983/autumn-harvest/commit/f0118258e6779fe4f9dd271daddd5a5382d93a81))

- Export workflow histories as replay fixtures (#242)([bdb37b7](https://github.com/madmax983/autumn-harvest/commit/bdb37b76edff49d32d2382f1a7820cb4e4187464))

- Expose operator external activity handoffs (#239)([38ce949](https://github.com/madmax983/autumn-harvest/commit/38ce9498c794693939725ca48a0f50c43747683a))

- Version-gate retirement check (issue #164) (#228)([8f7ecb2](https://github.com/madmax983/autumn-harvest/commit/8f7ecb25a5c71cb5c95c56f7eddecd598feb4dff))

- Report workflow version-gate usage (#223)([4246798](https://github.com/madmax983/autumn-harvest/commit/4246798b0a9b1f3dc586d2ab5411ee9108e16ad1))

- Add compensation when payment capture signal missing capture_id (#221)([dd5ca1e](https://github.com/madmax983/autumn-harvest/commit/dd5ca1eb025f305d9734e5edaffac5aab6c36d5d))

- Implement todo 87([4bf6cb7](https://github.com/madmax983/autumn-harvest/commit/4bf6cb753d3fd6f68c0bf0a562d8a2dee9c69a6e))

- Add documentation for WorkflowResetError, RetentionConfig, and Worker types (#218)([7707294](https://github.com/madmax983/autumn-harvest/commit/77072944451699e22554043e17d113817357330f))

- Add shard readiness health gate (#219)([10655a3](https://github.com/madmax983/autumn-harvest/commit/10655a3403049bbd45c6ac617977f868500c9912))

-  feat(harvest): add deployment preflight checks (#212)([9cf96ee](https://github.com/madmax983/autumn-harvest/commit/9cf96eeca0e99aa67deaacb5b234c81a1fa53075))

- Add mutable search attributes API for workflow executions (#210)([ddf6aa3](https://github.com/madmax983/autumn-harvest/commit/ddf6aa3626a0e827b4d58213ed1196e82a948ffa))

- Add audit trail for management API mutations (issue #158) (#208)([fa9f183](https://github.com/madmax983/autumn-harvest/commit/fa9f183a6a946f2d6fdb063d04dd461c5977f4a3))

- Add activity idempotency key support for at-least-once semantics (#203)([feb23b5](https://github.com/madmax983/autumn-harvest/commit/feb23b599839b4e4dc825a209f19af66e60d3c55))

- Heartbeats (#202)([68b6e59](https://github.com/madmax983/autumn-harvest/commit/68b6e599abcd6d29d54b46dbcbf043920d357988))

- Add test to verify api filters ignore unknown query keys (#190)([4b8ba87](https://github.com/madmax983/autumn-harvest/commit/4b8ba87f82e1e72490b6d5b7fe5703bb442bbf5d))

- Add workflow `stack` inspection endpoint and CLI command (#188)([bdb5216](https://github.com/madmax983/autumn-harvest/commit/bdb5216306d655362a68e204b1bef24b01da1e26))

- Add pluggable PayloadCodec system for event payload encoding/decoding (#187)([ca859d6](https://github.com/madmax983/autumn-harvest/commit/ca859d6b7758c69f1c040f71cfb1bbae6961cebf))

- Add Workers tab to Vantage dashboard UI (#186)([d5679b6](https://github.com/madmax983/autumn-harvest/commit/d5679b6a546fe869ef91190715e19fc08d00af42))

- Add Codecov badge to README([2143608](https://github.com/madmax983/autumn-harvest/commit/21436080ccd7ef78ae64610db2c86a2458031b64))

- Add badge for Ask DeepWiki to README([a1dbdde](https://github.com/madmax983/autumn-harvest/commit/a1dbdde4eb3254794fbde22398a47f6ea526c5be))

- Implement Update primitive for synchronous workflow requests (issue #140) (#181)([0af6d35](https://github.com/madmax983/autumn-harvest/commit/0af6d35bff9332cb34dae51124971eec468a13ef))

- Add metrics recording infrastructure and telemetry adapters (#178)([50dc23d](https://github.com/madmax983/autumn-harvest/commit/50dc23dbe96a31b999d032852c51991bbefbcf3a))

- Implement ADR-0001 OpenTelemetry span emission for workflow execution (#167)([5dbe09e](https://github.com/madmax983/autumn-harvest/commit/5dbe09e21bc80effb46603eb61eb9d1718a782e5))

- Add advanced workflow examples (#156)([f0ccf8d](https://github.com/madmax983/autumn-harvest/commit/f0ccf8d016f1d26b4ddfce51776cf6035013c04f))

- Add unit tests for ID string parsing and formatting in types.rs (#147)([bf04595](https://github.com/madmax983/autumn-harvest/commit/bf045950a357c7b7e8acebaf911bab8e1aeb5e46))

- Add version gate support with mismatch detection (#146)([8942960](https://github.com/madmax983/autumn-harvest/commit/89429604b2416afcc2777492ce6edd0f933e5193))

- Add WorkflowReplayer harness for pre-deploy replay-safety testing (#141)([8a331f9](https://github.com/madmax983/autumn-harvest/commit/8a331f92a91035dc31d3180c1c1d4d8c338d43a2))

- Add quickstart example with workflow, activity, and durable timer (#137)([5e3f4c4](https://github.com/madmax983/autumn-harvest/commit/5e3f4c4c65df912cc6f04628fb2ff39081ea9b51))

- Add batch operations for fleet-wide workflow management (issue #102) (#134)([3418c2f](https://github.com/madmax983/autumn-harvest/commit/3418c2fd5e091a98d626e75339c84d337cabfb82))

- Add Vantage spec for Child Workflows (#128)([265ae74](https://github.com/madmax983/autumn-harvest/commit/265ae74f38c19f1569f302719219581a2e7c19a8))

- Add worker fleet observability with liveness tracking and heartbeats (#133)([2f455ce](https://github.com/madmax983/autumn-harvest/commit/2f455ceab88c5fd2332b6b299e7f9984be9c7044))

- Implement local activities (issue #98) (#132)([69a44a6](https://github.com/madmax983/autumn-harvest/commit/69a44a6106ad902ca529edb92ba9ccfb05e908f1))

- Add bulk dead-letter queue replay and discard operations (#131)([c7f181a](https://github.com/madmax983/autumn-harvest/commit/c7f181a41f4c97be2bf8fbd85d4bb73f0ed8bbf7))

- Add external activity completion via task tokens (issue #92) (#123)([d5a7828](https://github.com/madmax983/autumn-harvest/commit/d5a7828055c4220d4576ee48d641eeb039e8c4bc))

- Add per-workflow cron schedules (issue #91) (#122)([a56bb42](https://github.com/madmax983/autumn-harvest/commit/a56bb421524433313e56d9e08460c3194e9e830f))

- Add tests covering unreachable code paths in history export (#118)([24a7af0](https://github.com/madmax983/autumn-harvest/commit/24a7af091f755ac559eeccf61172cf3a668351df))

- Add cluster-wide concurrency caps for rate-limited activities (#117)([59c3c62](https://github.com/madmax983/autumn-harvest/commit/59c3c6249494e741b7cbd01c388a97f225af0cc4))

- Add WorkflowIdReusePolicy to control duplicate workflow starts (#115)([fc5ae22](https://github.com/madmax983/autumn-harvest/commit/fc5ae22a7264ec3e5159170887cce143d9bfd5b6))

- Add opt-in retention janitor with management API and CLI controls (#111)([cbf39b3](https://github.com/madmax983/autumn-harvest/commit/cbf39b36abd0c5d24f1e9fd30e071ba3e89d88e9))

- Filter GET /workflows by state, workflow name, and search_attrs (#85)([53bef7f](https://github.com/madmax983/autumn-harvest/commit/53bef7f9264f41f5145775ef12851f7cb481aff6))

- Add Redis Streams task queue adapter (autumn-harvest-redis) (#79)([69172b2](https://github.com/madmax983/autumn-harvest/commit/69172b272ad8cfbdaf18f6b64307c979cc5c4042))

- Implement continue-as-new for long-running workflows (#77)([ca89e9c](https://github.com/madmax983/autumn-harvest/commit/ca89e9c49a49f01d52b12393e60e757746483f35))

- Add tests for wait_for_signal and child workflow diverged history paths (#71)([411e5e0](https://github.com/madmax983/autumn-harvest/commit/411e5e0ff4cffe7a1825efd1f40fa0a7eec13ae8))

- Add Vantage Spec for Redis-backed Task Queue Adapter (#68)([4f1d18a](https://github.com/madmax983/autumn-harvest/commit/4f1d18a68c7edfb7d781a21bdd38ed9c678c0d6a))

- Add DagSimulator for in-memory DAG testing (#59)([18ea876](https://github.com/madmax983/autumn-harvest/commit/18ea87604329c8f165c756af3fba6e891e8eb53f))

- Add Vantage embedded dashboard UI for workflow observability (#55)([5d28376](https://github.com/madmax983/autumn-harvest/commit/5d283765b55e35dc5d69fd18f42d98e6879b19e4))

- Add OpenTelemetry integration for trace context propagation and metrics (#53)([ad4a35b](https://github.com/madmax983/autumn-harvest/commit/ad4a35b901dcdee234e8dacf042d74362b41a11d))

- Implement cancellation grace period for uncooperative activities (#52)([6577e36](https://github.com/madmax983/autumn-harvest/commit/6577e3624347c861d93dcc9b00ed19c0c5624d4e))

- Add workflow sharding support across multiple Postgres databases (#54)([94dfc99](https://github.com/madmax983/autumn-harvest/commit/94dfc99d2780716ecf39c6a50748ee4d275dbe68))

- Add sticky cross-worker routing for workflow task affinity (#51)([329053d](https://github.com/madmax983/autumn-harvest/commit/329053d0296e3aad37a639c0c51d64c71cfd8ce8))

- Add Vantage spec for Database Sharding (#48)([3d58b5c](https://github.com/madmax983/autumn-harvest/commit/3d58b5ccd04679af198ca2ea50c70e688270b14f))

- Add Vantage specs for OpenTelemetry integration and Cancellation Semantics. (#43)([d093c75](https://github.com/madmax983/autumn-harvest/commit/d093c75f7d14f00926bd5088fbdbad8159167bea))

- Add `side_effect` and `random_uuid` to workflow context (#42)([100fcf0](https://github.com/madmax983/autumn-harvest/commit/100fcf0a625ea2fc399b3d67dd6331e9287ebb8a))

- Add Vantage spec for Sticky Cross-Worker Routing (#35)([a98b3e7](https://github.com/madmax983/autumn-harvest/commit/a98b3e789e08725baea2e73cf895daf972e04da7))

- Add harvest management cli (#29)([597dcfc](https://github.com/madmax983/autumn-harvest/commit/597dcfca7406dd5554223a843589a31d6cd2647c))

- Add Mermaid.js workflow history exporter (#25)([7226560](https://github.com/madmax983/autumn-harvest/commit/72265609750ef9b02b18f6b9cb49b2d31ddcee91))

- Add DAG visualization exporters for Mermaid and DOT (#17)([3cfdec9](https://github.com/madmax983/autumn-harvest/commit/3cfdec96e21f52059ccd09af11f8ed7041aba398))

### Fixed

- Route DLQ UI actions through audited bulk API (#286)([30e01dd](https://github.com/madmax983/autumn-harvest/commit/30e01dd44335e858417f0312f375f626635423a7))

- Compensate saga when approve_high_value_subscription fails (#222)([3784578](https://github.com/madmax983/autumn-harvest/commit/3784578bfd90754f2e1cdb1f0e80267e5b5e74db))

- Gate shared rollouts on readiness attempt 2 (#220)([190e5be](https://github.com/madmax983/autumn-harvest/commit/190e5be5a647cdf0769fda6ee405b1297b7ad6e9))

- Gate shard rollouts on readiness([b960a15](https://github.com/madmax983/autumn-harvest/commit/b960a15efd39e84ef924283ca24ee44fb67f3d7b))

- **worker:** Prevent panic in chrono_duration_from_secs (#126)([8511545](https://github.com/madmax983/autumn-harvest/commit/851154528d8d14afd3f264fc74706c3038498f70))

- Redis depth handling([76335e6](https://github.com/madmax983/autumn-harvest/commit/76335e65f96643faf3e08c635dc3a7ad5fe2277f))

- Replay determinism([d0a14d7](https://github.com/madmax983/autumn-harvest/commit/d0a14d7eede408bcf4449f1ad68ce04013c47b91))

- Redit and continue-as-new([fd1c024](https://github.com/madmax983/autumn-harvest/commit/fd1c0242a42985e6a8cfc4a1d19b40daf091e88e))

- Fix deadlock in WorkflowContext::execute_query (#66)([de47dd5](https://github.com/madmax983/autumn-harvest/commit/de47dd52a892e9a44c4bbd7fd473cb2077ac8f5b))

- Prevent silent integer truncation in concurrency limits and event indexing (#45)([68d5225](https://github.com/madmax983/autumn-harvest/commit/68d5225d6d5e97850ba975188d971e86a4bd5ce3))

- Fix integer overflow in task_duration string parsing (#32)([36add46](https://github.com/madmax983/autumn-harvest/commit/36add4631e3c16741acd7dfa4f5301017c3fbdea))

- Resolve broken intra-doc links for HarvestError (#19)([cd77f63](https://github.com/madmax983/autumn-harvest/commit/cd77f6365848bcd7c9861564ffc53b2c0bf69f08))

- Fix retry backoff calculation panic on negative floats and NaN (#16)([6eb93b6](https://github.com/madmax983/autumn-harvest/commit/6eb93b68cd1fa8776595b23b73af479b29c7d733))

### Changed

- **worker:** Extract helper methods from `run_with_listener` (#258)([ba5da61](https://github.com/madmax983/autumn-harvest/commit/ba5da61e6622148a7f4ccec323669cb54f9fc7b7))

- Simplify context history matching and worker execution error handling (#116)([f163e67](https://github.com/madmax983/autumn-harvest/commit/f163e6757515e4b78582abf85e649826fb56c131))

- **plugin:** Render Vantage dashboard with maud + autumn-web extractors (#101)([875830e](https://github.com/madmax983/autumn-harvest/commit/875830e51a60407a16dd68d3af488183c1786be4))

### Documentation

- **alerts:** Add starter Harvest alert pack and runbooks (#275)([49c41f2](https://github.com/madmax983/autumn-harvest/commit/49c41f209a44bde2dd7eccb8e2054e9af4719f7d))

- Fix broken intra-doc links across workspace (#236)([0726486](https://github.com/madmax983/autumn-harvest/commit/0726486382140a672a4e289876677a5c9eded8cd))

- Polyglot ADR([c43e8e4](https://github.com/madmax983/autumn-harvest/commit/c43e8e4b6770e140fc387670a671c8f295e4dc9f))

- Skills([be33d2e](https://github.com/madmax983/autumn-harvest/commit/be33d2ebcde3c6eeb29540baf2e728ae175bd565))

- Add module-level documentation and executable doc tests for query, dag_export, schema, and signal. (#69)([859bb21](https://github.com/madmax983/autumn-harvest/commit/859bb2101fcbc7f6efbc0b486adb05b6b171e1c4))

- Add Vantage spec for Continue-As-New (#60)([d884143](https://github.com/madmax983/autumn-harvest/commit/d884143fe78da00cf627b11a72ea07445d8b077e))

- Add Vantage specification for Saga Primitives (#18)([0cc01fe](https://github.com/madmax983/autumn-harvest/commit/0cc01fed2dea12ed2ce076ae230a1fd5d0ee3cf4))

### Testing

- **error:** Add unit tests for HarvestError Display formatting (#215)([d326df5](https://github.com/madmax983/autumn-harvest/commit/d326df50a4e7d6be7142c4bf492567fa3d0d5f6d))

- Add test coverage for WorkflowContext::check_cancellation (#90)([b4ed42e](https://github.com/madmax983/autumn-harvest/commit/b4ed42ed9ba7c5a0fb0d8da6ef1003b54261c7bc))

- **saga:** Add comprehensive unit tests for Saga compensation logic (#44)([72d5760](https://github.com/madmax983/autumn-harvest/commit/72d5760b1217909d79342ac9a5242bc3a672b57d))

- Improve test coverage for Error, Info, and Saga modules (#40)([48f5a37](https://github.com/madmax983/autumn-harvest/commit/48f5a377dd024b65c2782447fca4407ad3577e2a))

### Miscellaneous

- Delete claude files([3c73e9e](https://github.com/madmax983/autumn-harvest/commit/3c73e9e6271497864a414987799f6cbed4caa85d))

- Relase work([7e61ec0](https://github.com/madmax983/autumn-harvest/commit/7e61ec038ee985e93186a72c0cad28d53d8b7ddc))

- Clippy([51b9b6e](https://github.com/madmax983/autumn-harvest/commit/51b9b6ec3626e5804eca98f138ac907f58554fdb))

- PR feedback([6f40114](https://github.com/madmax983/autumn-harvest/commit/6f40114d7ac6109a0a277d98cce469ca8dfefc8f))

- Cleanup([2c59de4](https://github.com/madmax983/autumn-harvest/commit/2c59de42684a19c036e1360584bfc53522eaa4e7))

## [0.1.0] - 2026-04-19

### Added

- **worker:** Add worker runtime with semaphore-bounded poll loop([47f8291](https://github.com/madmax983/autumn-harvest/commit/47f8291b8f6e7716f6950ff8505105825f67ca3c))

- **pool:** Add separate worker pool with shared ceiling enforcement([c602cbd](https://github.com/madmax983/autumn-harvest/commit/c602cbdb5e086aaecf10a44137b9fc50389696b9))

- **dlq:** Add dead letter queue for permanently failed tasks([07b0ba0](https://github.com/madmax983/autumn-harvest/commit/07b0ba03a5bae5379b8b5d497feb77f1e395a5e7))

- **cache:** Add LRU workflow state cache for replay optimization([6f5eb96](https://github.com/madmax983/autumn-harvest/commit/6f5eb96c1c2db92493863f1648b06d1d5bca212a))

- **timeout:** Add timeout enforcement for heartbeat, start-to-close, and schedule-to-start([f860113](https://github.com/madmax983/autumn-harvest/commit/f860113d6647e8fca33f5286c6dc38dc9b32e960))

- **heartbeat:** Add batched heartbeat flusher with 1s debounce([b563e83](https://github.com/madmax983/autumn-harvest/commit/b563e834ef99e491385af6db591e9afec729f784))

- **executor:** Add workflow executor with replay and suspension detection([b00d056](https://github.com/madmax983/autumn-harvest/commit/b00d056b1cefa6023839d42f8a2fa04ca5d22ca0))

- **notify:** Add LISTEN/NOTIFY wrapper with channel naming and payload types([387dd3a](https://github.com/madmax983/autumn-harvest/commit/387dd3a102d823dccf0ae6f7d8d8103c1d2d4e22))

- **queue:** Add Postgres-backed task queue with SKIP LOCKED claiming([574503f](https://github.com/madmax983/autumn-harvest/commit/574503f2a97de05ef5f8bcb83c67eb45bffc67b9))

- **context:** Implement ActivityContext with heartbeat channel and cancellation([233189a](https://github.com/madmax983/autumn-harvest/commit/233189a1bf368412c6757ec050e0199697f1d4d2))

- **context:** Implement WorkflowContext with replay-aware execute_activity and versioning([3e908bb](https://github.com/madmax983/autumn-harvest/commit/3e908bb22c173de66d784b0d7eae93d9f473b182))

- **replay:** Add HistoryMatcher with activity, timer, and version matching([f79b6ef](https://github.com/madmax983/autumn-harvest/commit/f79b6ef340d2d18ad457203af123b880c517fc4d))

- **store:** Add load_history reader for event replay([921a57f](https://github.com/madmax983/autumn-harvest/commit/921a57f7c81ef856ef03929adff88b1e40e41658))

- **store:** Add event store writer with sequential event IDs([3f7337f](https://github.com/madmax983/autumn-harvest/commit/3f7337f45b250d3978c27cb762a3399238802b0f))

- **macros:** Add workflows![] and activities![] collection macros([d09fd56](https://github.com/madmax983/autumn-harvest/commit/d09fd56e518a949a95af5d379535efc46d856bc1))

- **macros:** Implement #[activity] with retry/timeout/queue attributes([a6ea41a](https://github.com/madmax983/autumn-harvest/commit/a6ea41ae142b7164b61ae34a779b195339387c18))

- **macros:** Implement #[workflow] companion function generator([bd9de34](https://github.com/madmax983/autumn-harvest/commit/bd9de34c630260902e0bf2d4a1132d7b60be100c))

- **builder:** Add HarvestBuilder, WorkerConfig, and prelude([923edbe](https://github.com/madmax983/autumn-harvest/commit/923edbe84551000dbefedfaca4cd24384dce5e67))

- **db:** Add Diesel schema and Queryable/Insertable models for harvest_* tables([cbac2e7](https://github.com/madmax983/autumn-harvest/commit/cbac2e7d4d4486bbc72881177f18e15cdb85161d))

- **migrations:** Add harvest_* Postgres schema (initial migration)([6881762](https://github.com/madmax983/autumn-harvest/commit/6881762d8c64ec409f0a43712514f47fd9df9da2))

- **context:** Add WorkflowContext and ActivityContext skeletons with state access([747a1c4](https://github.com/madmax983/autumn-harvest/commit/747a1c43fdcb7729b6df50ff5db491f206b18fb9))

- **info:** Add WorkflowInfo, ActivityInfo registration types([713f20a](https://github.com/madmax983/autumn-harvest/commit/713f20a62f5ab275b1cebb82c6856ab95eaa4ed9))

- **event:** Add WorkflowEvent enum with serde round-trip([bb2d637](https://github.com/madmax983/autumn-harvest/commit/bb2d6371756cb823334a059e41e6edb261bd9943))

- **policy:** Add RetryPolicy, TriggerRule, Schedule([e22c9ea](https://github.com/madmax983/autumn-harvest/commit/e22c9ea35bb3865d64fb7b1cb346c8795176aecf))

- **error:** Add HarvestError, HarvestResult, TimeoutType, compute_retry_delay([7f6a27e](https://github.com/madmax983/autumn-harvest/commit/7f6a27e335577aabff50b753d5257ed552adc282))

- **types:** Add WorkflowId, ExecutionId, ActivityExecId, TimerId, WorkerId newtypes([e1b26a7](https://github.com/madmax983/autumn-harvest/commit/e1b26a7ba371fcf07c1d1d5143f0bb2b593f277c))

### Fixed

- **plugin:** Explicitly enable autumn-harvest db feature([2603761](https://github.com/madmax983/autumn-harvest/commit/2603761b1a7008077f51ce5023deff0f023c4ef9))

- **migrations:** Remove duplicate harvest initial migration([a3deb05](https://github.com/madmax983/autumn-harvest/commit/a3deb0518745ae99376346ecfcb22aa2115bc944))

- **plugin:** Explicitly enable autumn-harvest db feature([bbf93f3](https://github.com/madmax983/autumn-harvest/commit/bbf93f3bf0c82610071c5b32684be3417954f725))

- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([cd98e36](https://github.com/madmax983/autumn-harvest/commit/cd98e3656868cb67ee7e1dc26cf8eb0697fefefe))

- **manifests:** Add version to internal workspace path deps([8ed6456](https://github.com/madmax983/autumn-harvest/commit/8ed6456fd812ee06fa4dfecfb6883bf877324fc5))

- **plugin:** Consume MIGRATIONS const instead of cross-crate macro path([43ed6f7](https://github.com/madmax983/autumn-harvest/commit/43ed6f70e94163052e2d3debb2e1ea46aa6c5fd1))

- **manifests:** Add version to internal workspace path deps([3d2254f](https://github.com/madmax983/autumn-harvest/commit/3d2254f1c33889ab3f4466be8397015269ea55ab))

- Resolve clippy warnings in pool/worker and run cargo fmt([28d83ae](https://github.com/madmax983/autumn-harvest/commit/28d83aea5d11a201e09ca5ff43234a4f92d872e7))

- **macros:** Propagate serialization errors and add Debug impls to builder types([c460035](https://github.com/madmax983/autumn-harvest/commit/c460035d8d6d6c2c763f728a337962819932d9a8))

- **models:** Add serde derives and doc comments to all model structs([309f156](https://github.com/madmax983/autumn-harvest/commit/309f1563ace0803b15fb71414a565267deedb6e4))

- **context:** Add production constructors and fix unused_async lint([83645ad](https://github.com/madmax983/autumn-harvest/commit/83645ad69707997e0c5d9b1dca71afe5f58a047b))

- Move compute_retry_delay to policy, fix underflow, AllDone vacuous truth, const type_name, expanded tests([e9d7c46](https://github.com/madmax983/autumn-harvest/commit/e9d7c46715e68a4ffb867783077010e586935820))

- **types:** Const fn on as_uuid, add as_str to TimerId and WorkerId([5fe02cb](https://github.com/madmax983/autumn-harvest/commit/5fe02cb46f3b8114aa35eabf311731513ddbccd6))

### Documentation

- Add README and crates.io metadata for first publish([74dbbb9](https://github.com/madmax983/autumn-harvest/commit/74dbbb912eb4f3d430c209c1ef98fb36e830785e))

- Add README and crates.io metadata for first publish([df0d628](https://github.com/madmax983/autumn-harvest/commit/df0d628153bb66641d4affbef34cb61ae45ba453))

- Update CLAUDE.md with Phase 2 completion status([38f506e](https://github.com/madmax983/autumn-harvest/commit/38f506e52a995e85342d5f5aa3ddfebc854b10a8))

- Add CLAUDE.md with architecture overview and development guide([48675e7](https://github.com/madmax983/autumn-harvest/commit/48675e777c46cbfa0701477aaccf18a1044dad7b))

### Testing

- Fix clippy doc_markdown warnings in replay tests([2ee4667](https://github.com/madmax983/autumn-harvest/commit/2ee466788daae3300818a5fda85ee3856ceda493))

- Add E2E integration tests with testcontainers Postgres([c670f8b](https://github.com/madmax983/autumn-harvest/commit/c670f8bc577ba3bd09277b877835bc3095ac0937))

- Add replay engine correctness tests including non-determinism detection([c624524](https://github.com/madmax983/autumn-harvest/commit/c6245246344cc3beea5e95da8910ffa065d9b0f9))

- Add end-to-end integration tests for workflow lifecycle([c81bcc7](https://github.com/madmax983/autumn-harvest/commit/c81bcc75effb9db41f6aee875d3786c1ff2109ba))

### Miscellaneous

- Trigger on trunk-dev push and pull_request([0601dd0](https://github.com/madmax983/autumn-harvest/commit/0601dd092f66b074ccced99c18862e075d93f150))

- Trigger on trunk-dev push and pull_request([7bf8cc0](https://github.com/madmax983/autumn-harvest/commit/7bf8cc0908ed0de336389c506fdc4cca530622be))

- **plugin:** Use published autumn-web 0.2 from crates.io([20aa1cd](https://github.com/madmax983/autumn-harvest/commit/20aa1cd9cfc63e087fb6b732a04e5d1cfec74bf1))

- **plugin:** Use published autumn-web 0.2 from crates.io([15d9eba](https://github.com/madmax983/autumn-harvest/commit/15d9ebaaf32dcd125d3e34a205b07c7f095c7450))

- Sync Phase 3 from autumn monorepo([0ee395e](https://github.com/madmax983/autumn-harvest/commit/0ee395e142d34271b5eaa4c391254f0517235a20))

- Clippy + fmt clean across autumn-harvest Phase 2([9d44e2d](https://github.com/madmax983/autumn-harvest/commit/9d44e2d4f11826338399beffe235de290bd5e80c))

- **lib:** Remove stale task comment from context module declaration([11ce797](https://github.com/madmax983/autumn-harvest/commit/11ce79793270986a5bbbf65c0449a58e2a1bacb4))

- Fix workspace review issues (deadpool version, clippy msrv, gitignore, testcontainers deps)([1d99fc3](https://github.com/madmax983/autumn-harvest/commit/1d99fc3da51bbafb231d6743a51caa501176dae0))

- Initialize autumn-harvest workspace([55f4ceb](https://github.com/madmax983/autumn-harvest/commit/55f4ceb915fd88dc59249db9a98adf1f044c09a7))
