# Core execution model: state of the art vs Autumn Harvest

Scope: the core durable-execution model only: durable state, determinism, code versioning, history growth, payloads, sticky execution, and shard ownership and fencing. Retries and backpressure, the Postgres data layer, testing, and ops/security are covered by other researchers.

Research date: 2026-09-30. Repo HEAD: `937b655`. All codebase paths are relative to `/home/user/autumn-harvest`. No code was built or run. Every codebase claim comes from reading source, tests and docs.

Status vocabulary used throughout: **absent**, **present but untested**, **present but not wired into the runtime**, **present and tested**.

---

## Q1 (External) How do leading engines model durable state, enforce determinism, and version workflow code?

### Takeaway
There are three state models. Temporal and Azure Durable Functions (DF) use an event-sourced history that is replayed and compared against commands. Restate, DBOS, Inngest and Hatchet use a per-invocation journal or checkpoint, and completed steps are memoized. AWS Step Functions uses an interpreted state machine. The strict-replay engines detect non-determinism by comparing commands to events. They treat a divergence as a retryable *task* failure, not a workflow failure. Versioning has converged on two tools used together: pinning in-flight runs to the build that started them, and in-code patch markers for runs that must move to new code.

### Cited Findings
**State model**
- Temporal: "When the Workflow's code replays, the Commands that are emitted are compared with the existing Event History." A mismatched command yields a non-deterministic error. — [Temporal: Workflow Definition](https://docs.temporal.io/workflow-definition)
- Azure DF uses event sourcing. "Instead of directly storing the current state of an orchestration, the Durable Task Framework uses an append-only store to record the full series of actions." When an orchestrator gets more work, it "re-executes the entire function from the start to rebuild the local state." The dispatcher commits new actions and messages together. After that, "the orchestrator function can be unloaded from memory." The page was updated 2026-08-14. — [Microsoft Learn: Durable orchestrations](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations)
- DF on Azure Storage "doesn't provide any transactional guarantees about data consistency between table storage and queues", so it uses eventual-consistency patterns. The MSSQL provider and the Durable Task Scheduler give stronger consistency. — [same source](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-orchestrations)
- DBOS writes "one database write per step (to checkpoint the step's outcome) plus two additional database writes per workflow". Recovery re-runs the workflow from its saved inputs and returns stored step outputs until "a step with **no checkpoint**." — [DBOS architecture](https://docs.dbos.dev/architecture)
- Restate treats "the log as the primary durability layer". A step "happened" when its journal-entry append is replicated to a quorum. On retry, the processor "attaches the full journal so far" and returns already-committed results. — [Restate architecture](https://docs.restate.dev/references/architecture)
- Inngest checkpoints each step result. On replay "the SDK skips the callback and returns the saved result". "Code outside a step can run again each time the handler replays." — [Inngest: how functions are executed](https://www.inngest.com/docs/learn/how-functions-are-executed)
- In Hatchet, each completed piece of a durable task "creates a new checkpoint (an entry in a durable event log)". Durable tasks may only *wait* or *spawn child tasks*. — [Hatchet durable execution](https://docs.hatchet.run/v1/durable-execution)

**Determinism enforcement**
- Temporal names "intrinsic non-deterministic logic", for example branching on local time or randomness. Such operations must go through SDK APIs. — [Temporal: Workflow Definition](https://docs.temporal.io/workflow-definition)
- Temporal retries workflow-task failures, including non-determinism errors (TMPRL1100), automatically and preserves workflow state. This lets teams "fix bugs and redeploy without losing Workflow state", or reset the workflow. — [temporalio/rules TMPRL1100](https://github.com/temporalio/rules/blob/main/rules/TMPRL1100.md); [Temporal errors reference](https://docs.temporal.io/references/errors)
- The Temporal Go SDK runs workflow coroutines through a deterministic dispatcher (`ExecuteUntilAllBlocked`). It has a *deadlock detector* that fails the workflow task when code does not yield for over about 1 second. That is a failure signal, not the way a suspension is detected. — [Temporal community forum](https://community.temporal.io/t/potential-deadlock-detected-workflow-goroutine-root-didnt-yield-for-over-a-second/2414); [sdk-go #329](https://github.com/temporalio/sdk-go/issues/329)

**Versioning**
- Temporal Worker Versioning: "A **Pinned** Workflow is guaranteed to complete on a single Worker Deployment Version." Auto-Upgrade workflows move to new versions and need patching. Deployments have Current, Ramping and Target versions. — [Temporal Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
- DBOS computes the application version "from a hash of workflow source code" by default. It "only recovers workflows whose version matches the current application version". It also offers `DBOS.patch()` and deprecation. — [DBOS upgrading workflows](https://docs.dbos.dev/python/tutorials/upgrading-workflows)
- Restate uses immutable deployments. "Existing requests continue on the original deployment." Its guidance is to "Avoid long-running handlers (days or months)" because old deployments must be kept. — [Restate versioning](https://docs.restate.dev/services/versioning)
- Step Functions has published state-machine versions and aliases, with a quota of 1000 versions per state machine. — [AWS Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)

### Inferences
- Harvest follows the Temporal model: event-sourced history plus positional command matching. The right comparison for Harvest is therefore Temporal and DF, not the journal engines.
- The common best practice is three layers. Pin by default. Patch when a run must move. Treat a non-determinism error as a *parked, retryable* condition.

### Gaps
- Hatchet and Inngest publish no in-flight version-pinning primitive in their official docs. This matches the "(unverified)" cells in `docs/comparison.md`.
- Temporal's default sticky-queue timeout and per-SDK cache sizes were not in the pages I fetched. The sticky page says only "approximately five seconds".

---

## Q2 (External) How do they handle history growth, large payloads, sticky execution and shard ownership?

### Takeaway
Every engine caps history, and continue-as-new (or a new execution) is the standard answer. Temporal warns at 10,240 events and terminates at 51,200 events. Step Functions fails an execution at 25,000 events. Payloads are capped at 2 MB (Temporal) or 256 KiB (Step Functions), and the claim-check pattern is the recommended fix. Temporal's sticky execution caches the *live workflow state* so a warm worker does not replay. Shard ownership is fenced by a monotonic generation number checked on every write: range_id in Temporal, the leader epoch in Restate.

### Cited Findings
- Temporal "logs a warning after 10,240 Events" and terminates when history "exceeds 51,200 Events". It also terminates at more than 2000 Updates or more than 10000 Signals. The documented fix is Continue-As-New. — [Temporal: Events](https://docs.temporal.io/workflow-execution/event)
- Temporal payload limit: "2 MB on Temporal Cloud... default of 2 MB" self-hosted. There is a "4 MB limit on each request", so a workflow task that schedules many activities can exceed it. The primary fix is the "claim check pattern". — [Temporal blob-size-limit troubleshooting](https://docs.temporal.io/troubleshooting/blob-size-limit-error)
- Step Functions Standard caps history at "25,000 events... If the execution history reaches this quota, the execution will fail". Task, state and execution I/O is capped at 256 KiB. Maximum execution time is 1 year. — [AWS Step Functions quotas](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)
- Temporal sticky execution: the worker "cache[s] the Workflow state in memory", and later tasks go to a worker-specific sticky queue. If that worker does not start the task within about 5 s, stickiness is disabled for the execution. It is "the default behavior of the Temporal Platform". — [Temporal: Sticky Execution](https://docs.temporal.io/sticky-execution)
- Temporal cache eviction: "An evicted Workflow Execution will need to be replayed when it gets any action that may advance it." — [Temporal: workflow cache tuning](https://docs.temporal.io/develop/worker-performance/workflow-cache)
- Temporal shard fencing: RangeID is "a monotonically increasing generation number used for fencing". "All updates to the history service database are protected by a conditional update on the range-id value." Shards are acquired by bumping rangeID. — [temporalio/temporal history-service architecture doc](https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md) (also summarized in the [deepwiki History Service page](https://deepwiki.com/temporalio/temporal/3-history-service))
- Restate: "Attempts carry monotonically increasing epochs; the processor rejects any events from superseded epochs". It snapshots RocksDB partition stores to S3 and then trims the log "to cap storage and replay cost". Keys hash to one partition, so "no cross-partition coordination is needed". — [Restate architecture](https://docs.restate.dev/references/architecture)
- DF extended sessions and eternal orchestrations (ContinueAsNew) exist, but the page I fetched did not quote their limits. See Gaps.

### Inferences
- Temporal's sticky cache saves *CPU replay work*, not just a database read. A cache that stores only the event list still pays the full re-execution cost of the workflow function on every decision.
- A default hard history cap exists in every surveyed engine that publishes one. An engine with *no* default cap can run into unbounded replay cost.

### Gaps
- Azure DF has no published maximum history size in the page I fetched. I found no reliable source for DF extended-session defaults within budget.
- Hatchet and Inngest step and state-size limits: not in the fetched pages.

---

## Q3 (External) What are the documented failure modes of these engines?

### Takeaway
The recurring failure classes are these. Non-determinism after a deploy parks or fails runs. Shard or ownership transitions lose or strand tasks. History or payload limits terminate runs. Codec or serialization failures turn into non-determinism.

### Cited Findings
- Temporal issue #12300 reports transfer tasks lost for good after a shard reload. The reader watermark "advances past undelivered tasks". Workflows stay "permanently stuck in RUNNING" or hit ScheduleToStart timeouts, with no self-heal after 16+ hours. The setup was Temporal v1.27.2 on PostgreSQL with pgpool. The page shows the issue as open. The fetched summary dates it to 2026-09-30, which I could not verify independently. — [temporalio/temporal #12300](https://github.com/temporalio/temporal/issues/12300)
- Temporal #3135 proposes asserting shard ownership "against source of truth when acquireShards is invoked". It is evidence that ownership races are a real, known class. — [temporalio/temporal #3135](https://github.com/temporalio/temporal/issues/3135)
- In the Temporal Go SDK, a session activity cancellation combined with a DataConverter or codec failure causes a *permanent* TMPRL1100 on replay. — [sdk-go #2206](https://github.com/temporalio/sdk-go/issues/2206)
- Temporal TS SDK: histories from 1.11.2 failed to replay on 1.11.5, so an SDK upgrade itself became a source of non-determinism. — [sdk-typescript #1582](https://github.com/temporalio/sdk-typescript/issues/1582)
- A forum thread asks how to stop a non-deterministic error from retrying forever. Indefinite workflow-task retry is the flip side of "park, don't fail". — [Temporal forum](https://community.temporal.io/t/how-to-stop-non-deterministic-error-retry-forever/5694)
- Go SDK "Potential deadlock detected" errors come from CPU-heavy workflow code or an overloaded worker. Worker load becomes a workflow-task failure. — [Temporal forum](https://community.temporal.io/t/potential-deadlock-detected-in-workflow-go-routine/15323)
- Oversized activity results "cause retry loops or `ScheduleToCloseTimeout` failures with no server notification". — [Temporal blob-size-limit troubleshooting](https://docs.temporal.io/troubleshooting/blob-size-limit-error)

### Inferences
- The same failure classes apply to Harvest. Each gap in Q5 is tied to one of them: timing-dependent suspension, silent drift, unbounded history, ownership after a reclaim, and fail-open pinning.

### Gaps
- I found no formal public postmortems for Restate, DBOS, Hatchet or Inngest execution-model incidents within budget. The evidence is GitHub issues and forum threads, not vendor RCAs.

---

## Q4 (Codebase) Does Autumn Harvest have each capability, where, is it tested, and is it wired into the runtime?

### Takeaway
Harvest is a Temporal-style event-sourced engine with an unusually deep set of determinism and versioning tools. Most capabilities are present, tested, and wired. Three core-mechanism differences stand out. The runtime replays the workflow *from the top on every decision cycle*, and its warm cache holds only events. Suspension is detected by a **100 ms wall-clock timeout**. The production worker path **lacks the unconsumed-history drift check** that the offline and strict paths have.

### Cited Findings (capability matrix)

**Durable state: event-sourced history**
- **Present and tested.** `harvest_events` is an append-only log with `UNIQUE (workflow_exec_id, event_id)` (`autumn-harvest/migrations/20260409000000_harvest_initial/up.sql:37-45`). `WorkflowEvent` has 50 adjacently-tagged variants (`autumn-harvest/src/event.rs:86`).
- The append doc comment says a unique violation on `start_id` "indicates a concurrency conflict where two workers tried to advance the same workflow" (`autumn-harvest/src/store.rs:156-167`). The event-id unique constraint is the optimistic-concurrency detector (`autumn-harvest/src/partition.rs` module doc).
- Doc drift: `docs/architecture.md:198-199` lists `event.rs` twice, with 41 and 35 variants.
- **Transactional outbox, present and wired.** The decision's events and the activity task rows commit in one transaction (`autumn-harvest/src/worker.rs:9986-10080`, `conn.transaction` at about line 10028; `queue::enqueue_batch` at about line 10075). This is the analogue of Temporal writing history and transfer tasks in one shard transaction.
- No workflow-task events: there is no WorkflowTaskStarted or Completed equivalent in the variant list (`autumn-harvest/src/event.rs:86`). History does not record which build ran each decision. Temporal records this in its workflow-task events.

**Execution driver and suspension detection**
- **Present and tested, but the mechanism departs from the state of the art.** Each cycle calls `run_workflow_with_state_history_policy_and_caps` with the *entire* `history_events.clone()` (`autumn-harvest/src/worker.rs:20680-20684`). The handler is re-run from the top.
- Suspension is inferred from `tokio::time::timeout(SUSPENSION_TIMEOUT, ...)` with `SUSPENSION_TIMEOUT = 100 ms` (`autumn-harvest/src/executor.rs:89-91`, `:151-159`). The doc comment reads: "if the workflow hasn't completed within this window, it's blocked on a oneshot channel (suspended)".
- The codebase names the hazard itself. A live dispatch "still mid-flight on some other I/O -- a slow downstream call, a database round trip -- when SUSPENSION_TIMEOUT elapses" suspends with zero commands and is failed (`autumn-harvest/src/worker.rs:17043-17053`).
- A hot-swap trampoline's `yield_now()` produced exactly that zero-command suspension, "a workflow parked on nothing, which the worker fails terminally" (`docs/changelog.d/pr-967-hot-code-swap.md:127-137`).
- The simulator treats 100 ms as a "fixed, pre-existing per-suspension cost" (`autumn-harvest/src/simulator.rs:1273-1283`).

**Sticky execution and workflow cache**
- **Present and tested, opt-in (off by default), and a history cache rather than a state cache.**
- `WorkflowCache` is an LRU of `CachedWorkflowState { events, next_event_id }` used for delta loads (`autumn-harvest/src/cache.rs:1-17`, `:31-45`).
- It is used only when `sticky_timeout > 0` (`autumn-harvest/src/worker.rs:17254-17263`). The default is `sticky_timeout: Duration::ZERO` and `workflow_cache_size: 1000` (`autumn-harvest/src/builder.rs:3932-3933`). It is enabled through `with_sticky_routing` (`autumn-harvest/src/builder.rs:4388-4390`).
- Tests: `autumn-harvest/tests/integration/sticky_routing_tests.rs` (9 tokio tests) and `cache_delta_load_tests.rs` (3 tests).
- The repo's own profile states the consequence: "the workflow function itself still replays every prior activity call from the top on every cycle". Total run cost stays `O(n²)` even with a delta cache (`docs/performance-sqlite-runtime-drive.md:440-457`). The profile was done on the SQLite backend, but it attributes the from-the-top replay to "the determinism engine itself".

**Determinism enforcement, pre-deploy**
- **Present and tested.** Compile-time guardrails HVG001–HVG011 are pinned by compile-fail fixtures (`autumn-harvest/tests/compile_fail/hvg001_wallclock.rs` … `hvg011_hashmap_iteration.rs`).
- The `det_check` static analyzer covers DET001–DET011 (`autumn-harvest/src/det_check.rs:1-30`, tests in `autumn-harvest/tests/integration/det_check_tests.rs`).
- Deterministic side-effect primitives record into `SideEffectRecorded` (`autumn-harvest/src/event.rs:86`; docs `docs/architecture.md:863`).

**Determinism enforcement, runtime**
- **Present and tested (ND-block).** A divergence with `non_deterministic_details` is gated *before* any terminal side effect. It appends zero events, stamps `nd_blocked_at`/`nd_block_count`, and re-pends with backoff of 5 s × 2^n, capped at 300 s (`autumn-harvest/src/worker.rs:21638-21681`, `:7846-7955`, `:5600-5622`). Recovery clears the markers atomically (`:7958-7982`).
- Tests: `autumn-harvest/tests/integration/nd_block_tests.rs`, 10 tests, including `divergent_replay_blocks_instead_of_failing` at :539 and `blocked_execution_resumes_after_rollback` at :646.
- This matches Temporal's "workflow-task failure is retried" semantics.
- **Partially wired: the unconsumed-history drift guard.** `history_has_unconsumed_events()` (`autumn-harvest/src/context.rs:3461`) is called only in the strict and canary executors (`autumn-harvest/src/executor.rs:1182`, `:1312`, `:1482`, `:1578`).
- The production `drive_workflow` turns `Ok(Ok(output))` into `Completed` and a timeout into `Suspended` with no such check (`autumn-harvest/src/executor.rs:1956-2067`). The worker never calls `run_workflow_strict` or `run_workflow_canary` (grep of `autumn-harvest/src/worker.rs` returns no hits).
- Yet the replay matcher's own comments rely on that guard. `replay.rs:4150-4158` says: "`executor.rs` fails the workflow if `history_has_unconsumed_events()`". `replay.rs:4268-4280` says a stray event "is still unconsumed at suspend, and `executor.rs`'s ... guard nd-blocks the workflow instead of letting it park forever".
- The pinning test `interleaved_sibling_signal_stray_timer_started_still_diverges` (`autumn-harvest/tests/integration/replayer_tests.rs:9838`) runs through `WorkflowReplayer::replay_from_events`, the strict path, not the worker.
- Early-completion drift is tested only in offline gates: `replay_drift_tests.rs:862` (ReplayVerifier) and `replayer_tests.rs:4717`.
- **Also a partial gap: non-strict matching skips input comparison.** Production uses `match_activity`, which checks only the activity *name*. `match_activity_strict` also compares input (`autumn-harvest/src/replay.rs:2297-2331` vs `:2338`). `docs/performance-replay.md` confirms that ordinary workers run the non-strict path. Temporal documents that changing activity *inputs* is a safe change, so this is parity, not a defect.

**Pre-deploy drift gates**
- **Present and tested, wired to the API and CLI.** The replay canary (`autumn-harvest/src/executor.rs:1369`) is exposed at `POST /admin/workflows/replay-canary` (`autumn-harvest-plugin/src/api.rs:33436`).
- The in-flight sample export and bundle gate come from issue #798 (`autumn-harvest/src/replay_sample.rs:1-20`). They are tested by `autumn-harvest/tests/integration/replay_drift_tests.rs` (80 tests) and `replay_canary_tests.rs` (3 tests).

**Code versioning**
- **Present and tested.** `ctx.version` (`autumn-harvest/src/context.rs:5310`), `ctx.patched` (`:5450`) and `ctx.deprecate_patch` (`:5510`). The retirement and usage queries are in `version_gate_retirement.rs` and `version_usage.rs`.
- Build-ID pinning plus a compatibility graph are enforced in the claim SQL (`autumn-harvest/src/queue.rs:1029-1037`, `:6546-6554`; `autumn-harvest/src/build_routing.rs:24-31`). There is a percentage ramp for issue #604. Tests: `build_routing_tests.rs` (27 fns).
- **Fail-open default.** The claim predicate includes `OR $3 = ''`, which is documented as "Legacy worker (empty build_id) can claim anything" (`autumn-harvest/src/build_routing.rs:28`, `:97-99`). `WorkerConfig` defaults `build_id: String::new()` (`autumn-harvest/src/builder.rs:3941`).
- Hot code swap (WASM modules per build) is **present but not in the default build**. It is an R&D spike behind the `hot-code-swap` feature (`autumn-harvest/src/hot_swap.rs:1-6`; `autumn-harvest/Cargo.toml:15,68`). Tests: `hot_code_swap_tests.rs` (41).

**History growth**
- Continue-as-new: **present and tested.** See `ctx.continue_as_new` (`autumn-harvest/src/context.rs:12243`), cross-type continue-as-new (`tests/integration/cross_type_continue_as_new_tests.rs`, 20 tests) and run-chain links (`autumn-harvest/src/run_chain.rs:1-20`).
- The `should_continue_as_new` advisory (`autumn-harvest/src/context.rs:4391`) uses a soft threshold of 10,000 events and a deadline fraction of 0.8 (`autumn-harvest/src/context.rs:49`, `:58`).
- Hard caps: **present and tested, opt-in, with no default.** The worker-side `history_event_hard_cap` moves the run to the DLQ (`autumn-harvest/src/worker.rs:19879`). The scanner-side `max_workflow_history_events` defaults to `None` (`autumn-harvest/src/builder.rs:198`, `:2119-2132`; `autumn-harvest/src/timeout.rs:5298`). A bloat warning fires at 75% of the cap (`autumn-harvest/src/context.rs:67`; `autumn-harvest/src/worker.rs:20104-20160`).
- Tests: `history_ceiling_claim_tests.rs` (2) plus unit tests at `worker.rs:32043+`.
- There is no history *byte* cap in the engine. `max_history_bytes` exists only as a per-tenant quota (`autumn-harvest/src/quota.rs:213`).
- Snapshots and state checkpoints: **absent.** Temporal is the same. Restate snapshots partition state.

**Large payloads**
- **Present and tested.** Default caps: 2 MiB for workflow input and activity input or result, 256 KiB for signals (`autumn-harvest/src/builder.rs:43-49`). The claim-check offload uses an embedder-supplied `PayloadStore` (`autumn-harvest/src/payload_store.rs:1-25`). Offload composes after the codec in the append path (`autumn-harvest/src/store.rs:379-420`).
- Tests: `payload_cap_tests.rs`, `payload_offload_db_tests.rs`, `payload_offload_replay_tests.rs` (1 test).
- Bind-parameter and byte chunking of event inserts: `autumn-harvest/src/store.rs:430-465`.

**Ownership and fencing, per execution**
- **Present and tested.** Tasks are claimed with `FOR UPDATE SKIP LOCKED`. The execution row is locked `FOR UPDATE` before history load and append (`autumn-harvest/src/store.rs:921-960`, `:1145-1170`).
- A `(task_id, worker_id, crash_strikes)` claim-held check guards terminal writes and several park paths (`autumn-harvest/src/queue.rs:4266-4288`; 12 call sites in `worker.rs`, e.g. `:7833`, `:8036`, `:8283`, `:18680`). Tests: `terminal_write_ownership_tests.rs` (11).
- The workflow-task timeout defaults to 10 s (`autumn-harvest/src/builder.rs:3953`). The stuck-RUNNING reclaim threshold is 4 × timeout + 30 s (`autumn-harvest/src/worker.rs:4296-4304`). Tests: `workflow_task_timeout_tests.rs` (8).

**Ownership and fencing, per shard and per region**
- **Present and tested, wired into the claim and persist paths.** `harvest_shard_generation` is a monotonic epoch. The claim CTE cross-joins it, and `assert_fence` takes it `FOR SHARE` at the top of every persist (`autumn-harvest/src/replication.rs:1-30`, `:1171`; call in `autumn-harvest/src/store.rs:220, :333, :406`). Tests: `cross_region_dr_tests.rs` (31).
- This is the closest analogue to Temporal's range_id. It is per shard database, pinned at worker startup.
- Shard placement is in the ExecutionId: shard in the UUID's first two bytes plus rendezvous hashing (`docs/architecture.md:166-171`).
- Rebalancing is **present, tested and wired**, but only for *quiescent* executions (copy, replay-verify, one atomic cutover) (`autumn-harvest/src/shard_rebalance.rs:1-25`, `:3348`). It is exposed through `autumn-harvest-plugin/src/api.rs` and `autumn-harvest-cli/src/lib.rs`. Tests: `shard_rebalance_db_tests.rs` (112).
- Cross-shard children: **present and tested** (`autumn-harvest/src/cross_shard_child.rs:1-25`; `cross_shard_children_tests.rs`, 19).
- Doc drift: `docs/architecture.md:162` still says "Cross-shard rebalancing of existing workflows is out of scope". `docs/comparison.md:155`, `:262` and `:294` still say cross-shard workflows are out of scope and multi-region DR is "planned". Both contradict the shipped `shard_rebalance.rs`, `cross_shard_child.rs` and `replication.rs`.

### Inferences
- On breadth, Harvest meets or exceeds Temporal for determinism tooling: compile-time plus static plus replay-canary plus ND-block. The weak points are in the *mechanism* of the live executor, not in the tooling around it.
- Harvest's per-execution safety is sound for a Postgres design. It comes from the row lock, the event-id UNIQUE constraint and the claim-held check, and it needs no shard-leader election, because Postgres is the single serialization point per shard.

### Gaps
- I did not run the test suite, so "tested" means a test exists that targets the code path. It does not mean I saw it pass.
- I did not trace every non-strict `match_*` variant, such as signal waits and child waits, to confirm how each behaves when the cursor sits on a mismatched event versus past the end of history.

---

## Q5 (Codebase) Concrete gaps, rated, with reasoning and remedies

### Takeaway
One **High** correctness gap: production replay does not detect drift from unconsumed history. Two **High** design gaps: suspension inferred from a 100 ms wall-clock timeout, and O(n²) from-the-top replay with sticky off by default. Two **Medium** gaps: no default history cap, and fail-open build pinning. Several **Low** items: no workflow-task events, and stale docs.

### Cited Findings (gap register)

**G1: High. The production worker does not ND-block on unconsumed history (silent early completion or parking forever).**
- Evidence: the guard runs only in the strict and canary executors (`autumn-harvest/src/executor.rs:1182`, `:1312`, `:1482`, `:1578`). The production `drive_workflow` completes or suspends without it (`autumn-harvest/src/executor.rs:1956-2067`). The matcher comments depend on it (`autumn-harvest/src/replay.rs:4150-4158`, `:4268-4280`). The pin test uses the strict replayer (`autumn-harvest/tests/integration/replayer_tests.rs:9838`).
- Failure scenario: a deploy deletes the last two activity calls of a workflow that is in flight. Offline gates catch this only if an operator ran them (`replay_drift_tests.rs:862`).
- On the worker, the new code returns `Ok` while `ActivityScheduled` rows are still unconsumed. The worker then persists `WorkflowCompleted` and abandons the in-flight pipeline, with no ND-block. The same applies to a stray `TimerStarted` in front of a signal wait: the run parks indefinitely instead of being ND-blocked, the exact outcome the `replay.rs` comment says is prevented.
- Temporal flags this class as a non-deterministic error ([Temporal: Workflow Definition](https://docs.temporal.io/workflow-definition)).
- Remedy:
  - In `drive_workflow`'s `Ok(..)` and `Err(_elapsed)` arms, run `ctx.history_has_unconsumed_events()`. When it is true, return `Failed { non_deterministic_details: Some(..) }` so the existing #603 ND-block path handles it.
  - Keep the canary frontier and failing-frontier exceptions (`autumn-harvest/src/context.rs:3079-3092`).
  - Add a worker-path integration test to `nd_block_tests.rs`.
- I found no GitHub tracking issue: semantic searches for "early completion unconsumed history production worker" returned 0 results.

**G2: High. Suspension is detected by a 100 ms wall-clock timeout.**
- Evidence: `autumn-harvest/src/executor.rs:89-91`, `:151-159`. The codebase documents the hazard at `autumn-harvest/src/worker.rs:17043-17053` and `docs/changelog.d/pr-967-hot-code-swap.md:127-137`.
- Consequences:
  - Every suspending decision costs at least 100 ms of wall time while holding a workflow slot and a DB connection. That is a throughput and latency floor.
  - Whether a cycle "suspends" depends on scheduling and timing, not on workflow semantics. If an await resolves outside Harvest's replay-controlled futures in more than 100 ms (a slow runtime, a starved tokio worker, an unguarded await), the cycle produces a partial or empty command set. A zero-command suspension is failed terminally.
- Temporal's dispatcher runs until all coroutines are blocked, which is a deterministic readiness check. It uses a roughly 1 s *deadlock detector* only as a failure signal ([Temporal forum](https://community.temporal.io/t/potential-deadlock-detected-workflow-goroutine-root-didnt-yield-for-over-a-second/2414)).
- Remedy: detect suspension deterministically. Poll the handler future with a custom waker. Treat `Poll::Pending` as "suspended" when no Harvest-owned future is ready and the command buffer is stable (run until all futures are blocked). Keep a separate, longer deadlock timeout that fails the *task*, not the workflow.
- I found no tracking issue.

**G3: High (performance and scale). From-the-top replay on every decision, sticky cache off by default and holding events only.**
- Evidence: `autumn-harvest/src/worker.rs:20680-20684`; `autumn-harvest/src/cache.rs:31-45`; default `sticky_timeout: ZERO` (`autumn-harvest/src/builder.rs:3933`); the repo's own `O(n²)` analysis (`docs/performance-sqlite-runtime-drive.md:440-457`).
- By default each cycle reloads and re-deserializes the full history, then re-executes the whole function.
- Temporal's default sticky cache keeps the live workflow and avoids that replay ([Temporal: Sticky Execution](https://docs.temporal.io/sticky-execution)).
- Combined with G4, a long-lived signal loop grows quadratically in CPU and I/O. The ceiling is 200 ms per 10k-event replay (budget from issue #135, `docs/performance-replay.md`).
- Remedy:
  - Short term: enable sticky routing by default, using Temporal's roughly 5 s fallback as a model.
  - Longer term: keep suspended workflow futures resident per execution in the LRU, and feed only new events on a warm hit. That needs a cache-validity check against `next_event_id` and eviction on any conflict.

**G4: Medium. No default history hard cap.**
- Evidence: `max_workflow_history_events: None` (`autumn-harvest/src/builder.rs:198`). `history_event_hard_cap` is opt-in (`:2094-2098`). Only the advisory `should_continue_as_new` at 10k exists by default (`autumn-harvest/src/context.rs:49`).
- Temporal terminates at 51,200 events and Step Functions at 25,000 ([Temporal](https://docs.temporal.io/workflow-execution/event); [AWS](https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html)).
- A runaway loop grows `harvest_events` without bound and makes G3 worse.
- There is also no history byte-size cap outside tenant quotas (`autumn-harvest/src/quota.rs:213`).
- Remedy: ship defaults, for example a warning at about 10k and a hard cap at about 50k, plus a byte cap. Keep the builder override.

**G5: Medium. Build pinning fails open.**
- Evidence: `OR $3 = ''` in the claim SQL (`autumn-harvest/src/queue.rs:1031`, `:6548`). The rule is documented at `autumn-harvest/src/build_routing.rs:28`, and the default is `build_id: String::new()` (`autumn-harvest/src/builder.rs:3941`).
- One worker deployed without a build id, for example because of a config regression, can claim executions pinned to *any* build. That defeats the pinning guarantee and invites non-determinism, which ND-block then parks.
- In Temporal, pinned workflows are "guaranteed to complete on a single Worker Deployment Version" ([Temporal](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)).
- Remedy:
  - Once any build policy exists on a queue, reject empty-build workers from claiming pinned rows (fail closed).
  - Emit a startup warning or metric when `build_id` is empty on a queue that has a policy.

**G6: Low. No workflow-task events in history.**
- Evidence: the variant list at `autumn-harvest/src/event.rs:86` has no decision-boundary event.
- A history cannot show which build or worker produced each decision. That weakens post-incident drift attribution and per-decision build auditing, which Temporal records.
- Remedy: add an additive, append-only variant such as `DecisionCommitted { build_id, worker_id }`, written in the same transaction as each decision. Keep it outside matching.

**G7: Low. Doc drift on core execution-model claims.**
- Evidence: `docs/architecture.md:162` (rebalancing "out of scope"), `docs/architecture.md:198-199` (duplicate `event.rs` rows with 41 and 35 variants; actual count is 50), and `docs/comparison.md:155`, `:262`, `:294` (no cross-shard workflows or DR).
- These contradict `shard_rebalance.rs`, `cross_shard_child.rs` and `replication.rs`.
- Evaluators will under-count Harvest's capabilities.
- Remedy: update the docs.

**Strengths to preserve (parity or better, not gaps)**
- ND-block that parks instead of failing (#603).
- Pre-deploy in-flight drift gate (#798) and replay canary.
- Compile-time guardrails.
- Transactional outbox of events and tasks.
- The event-id UNIQUE constraint as an optimistic-concurrency detector.
- Generation fence on claim and on every persist (#954).
- Claim-check offload (#524).
- Quiescent-only shard migration verified by replay (#964).

### Inferences
- G1 and G2 compound. A timing-dependent partial suspension (G2) produces commands that differ from the recorded history. The divergences that end in a *completion* or a *park*, rather than a positional mismatch, are exactly the ones G1 lets through silently.
- Fixing G2 (a deterministic readiness check) is also the precondition for G3's resident-state cache, because a cached future must be resumed deterministically.
- Severity is calibrated for an embedded engine. G1 would be Critical if pre-deploy drift gates were optional in practice. Whether the documented safe-deploy runbook makes the #798 gate mandatory was not verified.

### Gaps
- There is no runtime evidence, from a test run, that G1 happens end to end on the worker. The finding comes from static reading of every call site. A worker-path reproduction test should confirm it before a fix is prioritized.
- The frequency of G2 in production could not be quantified. No production telemetry for zero-command suspensions was found in the repo.
- No GitHub issues track G1–G5, based on two semantic searches of `autumn-foundation/autumn-harvest`. The only nearby items were #1459 (a parent task wedged by a workflow-task-timeout reset racing DB-pool contention, closed) and #1348 (closed).
