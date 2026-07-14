# R&D Spike — SQLite Edge / Local-First Harvest Feasibility

**Status:** Complete (throwaway spike). **Issue:** #966. **Branch:**
`feat/966-sqlite-feasibility-spike` (never merged to `trunk-dev` as-is).
**Audience:** engineering leadership — this report is written to be decided from
on its own.

---

## 1. Executive summary & recommendation

**Verdict: FEASIBLE, but do not port in-crate. Recommended path — a separate
companion crate that depends on the backend-neutral determinism core; decline
the in-crate `StorageBackend` trait.**

The spike proved the load-bearing hypothesis: the valuable, hard part of
Harvest — the deterministic replay core (`run_workflow`, `WorkflowEvent`, the
`HistoryMatcher`, `WorkflowContext`) — is **already backend-neutral**. It was
reused *wholesale* and driven to a terminal state by a single-writer, embedded
SQLite persistence layer (`rusqlite`) in ~1,150 lines, with **zero core-logic
changes** — the only edit to the crate outside the new module is additive,
feature-gated registration (a single `#[cfg(feature = "sqlite-spike")] pub mod
sqlite_spike;` in `lib.rs` plus the `sqlite-spike` Cargo feature) — and **zero
impact on the default Postgres build**. All four required
durability scenarios plus cross-backend replay — **executed in both directions**
(a SQLite-written history, including a retry history, replays on the engine path;
and a genuinely PG-shaped `ActivityStarted`-bearing history drives the SQLite
runtime) — pass (§5).

What the spike also made concrete is the *cost of the in-crate option*. Roughly
**35 modules** touch `AsyncPgConnection`, **34 `SELECT … FOR UPDATE SKIP
LOCKED`** call sites across **12 modules** encode multi-writer claim contention,
and the coordination surface (event store, task queue, timers, signals,
concurrency, throttle, debounce, completion callbacks, poison-pill reclaim,
retention) is Postgres-shaped by design. Hiding all of that behind a
`StorageBackend` trait would tax the Postgres hot path (dyn dispatch on claim),
double the integration test matrix, and refactor every `db`-gated module — for a
use case the core already serves without any of that cost.

**Costed options (full detail in §9):**

| Option | One-line | Cost to the shipped PG path | Recommendation |
|---|---|---|---|
| (i) In-crate `StorageBackend` trait | Both backends live in `autumn-harvest` behind a trait | **High** — hot-path indirection, test-matrix ×2, every `db`-module refactored | **Decline** |
| (ii) Separate companion crate | New crate depends on the core (`event`/`replay`/`context`/`executor`) as a library; reimplements only persistence | **Zero** — PG path untouched, spike proved the core is already reusable | **Recommended** |
| (iii) Decline entirely | Ship nothing; edge demand unserved | Zero | Acceptable fallback |

The spike is **evidence-gathering, not a committed port**. "Conclude it's not
worth it" was an acceptable outcome; the honest finding is that it *is* worth
it — **as option (ii)**, precisely because the spike shows the core needs no
changes to be reused.

---

## 2. Motivation & scope

### The demand

There is recurring, real demand for a durable-workflow engine that runs
**without a Postgres server**: desktop and CLI apps that want durable
background jobs, edge/IoT nodes that run disconnected and later sync, local-first
software, single-binary self-hosted deployments, and test/dev ergonomics
(no container to stand up). Embedded SQLite is the natural substrate — a file,
no server, no ops.

The prior-art scan (§11) confirms this category is real and **actively being
explored** — an embedded/local-first durable engine is no longer speculative.
DBOS — an in-process durable-execution *library*, the closest architectural
precedent to Harvest's plugin model — ships **SQLite as its default** system
database for local/single-server use; Oban (Elixir) ships a built-in SQLite
engine (`Oban.Engines.Lite`); Restate ships a single self-contained binary with
no external database; even Temporal's dev server runs on embedded SQLite. Harvest
would **join** this group, not pioneer it — the narrower gap it fills is a Rust,
deterministic-replay durable engine on embedded SQLite with a clean Postgres-hub
handoff story (§8). Crucially, DBOS's own guidance that its SQLite path "can't be
used in a distributed setting where an application runs on multiple servers"
independently corroborates this spike's central finding: a SQLite Harvest backend
is inherently single-writer / single-server.

### Scope & framing (read this before §9)

Core Harvest is **Postgres-only by design**. `CLAUDE.md` states it up front, and
the entire Phase 2+ coordination model (SKIP LOCKED claim, LISTEN/NOTIFY wakeups,
advisory-lock concurrency, cross-shard fan-out) is built on Postgres primitives
deliberately. This spike is a **timeboxed feasibility study**, not a committed
port and not a productization effort. Its job is to answer one question with
evidence:

> Can the backend-neutral determinism core be driven by a non-Postgres,
> embedded, single-writer persistence layer — and if so, at what cost, and
> where should that code live?

Explicit non-goals of the spike: multi-writer SQLite, sharding, cross-workflow
features (child workflows, external signals/cancels), HA scheduling, the
management API, the plugin, and any production-grade polish. Once the four
scenarios passed, the prototype was **abandoned rather than hardened** (§10).

---

## 3. Postgres-coupling inventory

This is the exhaustive, grep-verified map of where core Harvest is coupled to
Postgres, classified per surface as:

- **(a) trivially trait-able** — a thin CRUD call that any SQL backend answers;
- **(b) needs a semantic substitute on SQLite** — the *behavior* is portable but
  the *mechanism* is Postgres-specific; the substitute is named;
- **(c) fundamentally Postgres-shaped** — the feature only makes sense with
  multi-writer/multi-node Postgres semantics; it has no single-writer analog and
  is simply out of scope for an edge runtime.

### 3.1 Grep-verified counts

**Every count below excludes the throwaway `sqlite_spike/` prototype** — it is
the spike under evaluation, not PG-coupled core, and its module docs *mention*
"SKIP LOCKED"/"FOR UPDATE" in prose (explaining the single-writer substitutes),
which are concept references, not call sites. Reproduce each from the repo root;
the three load-bearing counts are:

```
grep -rn  "SKIP LOCKED" autumn-harvest/src --include=*.rs | grep -v sqlite_spike | wc -l   # 34
grep -rln "FOR UPDATE"  autumn-harvest/src --include=*.rs | grep -v sqlite_spike | wc -l   # 16
grep -rn  "pg_advisory" autumn-harvest/src --include=*.rs | grep -v sqlite_spike           # admission_gate.rs:920
```

| Pattern | Grep (piped through `grep -v sqlite_spike`) | Occurrences | Modules |
|---|---|---|---|
| `FOR UPDATE SKIP LOCKED` | `grep -rn "SKIP LOCKED"` | **34** call sites | 12 |
| `FOR UPDATE` (incl. SKIP LOCKED) | `grep -rln "FOR UPDATE"` | — | 16 |
| LISTEN / NOTIFY / `pg_notify` | `grep -rlnE "pg_notify\|LISTEN \|NOTIFY "` | — | 5 (+`handle.rs` via the `notify` wrapper) |
| Advisory locks (`pg_advisory_xact_lock`) | `grep -rn "pg_advisory"` | **1** SQL call site | `admission_gate.rs:920` (the per-key concurrency gate) |
| `gen_random_uuid()` | `grep -rln "gen_random_uuid"` | 1 runtime SQL (`throttle.rs`) + 13 migration DDL defaults | 1 src + 13 migrations |
| `to_regclass(...)` (table-exists probe) | `grep -rln "to_regclass"` | — | `completion_callback`, `debounce`, `event_batch`, `start_idempotency`, `throttle` |
| `INTERVAL` / `make_interval` arithmetic | `grep -rlnE "INTERVAL\|make_interval"` | — | 11 |
| `AsyncPgConnection` (the pool/conn type) | `grep -rln "AsyncPgConnection"` | — | **35** |
| Diesel `table!` definitions | `grep -c "table! {"` (`schema.rs`) | **30** | `schema.rs` |
| Migration directories | `ls -d migrations/*/` | **73** timestamped PG DDL dirs | `migrations/` |

The **34** SKIP-LOCKED call sites reconcile exactly with the per-module rows in
§3.2 (they sum to 34), and the sole `pg_advisory_xact_lock(377)` call is in
`admission_gate.rs`, not `queue.rs`. The claim in the success metric — "every
Postgres-coupled call site, verifiable against a grep-level audit" — is satisfied
by the greps above; every count below is reproducible from them.

### 3.2 Module-by-module classification

| Module | Coupling | # `SKIP LOCKED` | Class | Substitute on SQLite |
|---|---|---:|:--:|---|
| `store.rs` | Event append/load; `FOR UPDATE` lock on execution row | 1 | **(a)** | Plain `INSERT`/`SELECT … ORDER BY seq`; single-writer needs no row lock. **Proven** — `sqlite_spike/store.rs`. |
| `queue.rs` | Task claim, retry requeue, notify | 5 | **(b)** | `BEGIN IMMEDIATE` single-writer claim replaces SKIP LOCKED; polling replaces NOTIFY. (The advisory-lock concurrency gate lives in `admission_gate.rs`, not here.) **Proven** — `sqlite_spike/queue.rs`. |
| `notify.rs` | The LISTEN/NOTIFY wrapper itself | — | **(b)** | Poll loop (drain-ready-then-repoll). **Proven** — `sqlite_spike` driver loop. |
| `worker.rs` | Poll/dispatch loop, `append_activity_started_if_pending`, notify, `FOR UPDATE` | — | **(b)** | Synchronous `drain_ready` pass; no semaphore, no multi-worker dispatch. **Proven** — `sqlite_spike/worker.rs`. |
| `timeout.rs` | Timer/timeout scan, `INTERVAL` deadline arithmetic | 3 | **(b)** | Explicit absolute `fire_at INTEGER` (epoch) column + `WHERE fire_at <= now`. **Proven** — `spike_timers`. |
| `execution.rs` | Start/attach/reuse-policy, `FOR UPDATE`, `INTERVAL` | 4 | **(b)** | Single-writer `INSERT`; reuse-policy is app logic, not a PG feature. (Spike does start only.) |
| `completion_trigger.rs` | Terminal-transaction trigger eval + fan-out | 6 | **(b)/(c)** | Trigger eval is app logic **(b)**; cross-workflow fan-out is **(c)** — out of scope. |
| `completion_callback.rs` | Durable callback delivery scanner, `to_regclass`, SKIP LOCKED | 3 | **(b)** | Two-transaction claim→POST→record is single-writer-friendly; SKIP LOCKED → single-writer claim. Not in spike scope. |
| `debounce.rs` | Debounce upsert + scanner, `to_regclass`, `INTERVAL` | 3 | **(b)** | `ON CONFLICT DO UPDATE` upsert works in SQLite; scanner is single-writer. |
| `throttle.rs` | Token-bucket start throttle, `gen_random_uuid`, `to_regclass` | 3 | **(b)** | App-minted UUID replaces `gen_random_uuid()`; token bucket is portable SQL. |
| `event_batch.rs` | Batched event append claim | 2 | **(a)/(b)** | Single-writer batch append; no claim contention. |
| `signal.rs` | Signal insert + idempotent deliver (Diesel DSL, no raw `SKIP LOCKED`/`FOR UPDATE`) | 0 | **(b)** | A staged-row `INSERT` + oldest-undelivered `SELECT`/mark-delivered; a single writer needs no claim coordination. **Proven** — the spike's `spike_signals` / scenario 3. |
| `start_idempotency.rs` | Start-dedup reservation (`ON CONFLICT`, table-exists probe; ~18 SQL constructs) | 0 | **(b)** | A real coordination surface, but portable: the reservation is an upsert keyed on the idempotency key; a single writer serializes it trivially (`ON CONFLICT DO NOTHING` works in SQLite). Not in spike scope. |
| `retention.rs` | Retention janitor scan + delete | 2 | **(b)** | Single-writer sweep; `INTERVAL` → explicit deadline column. |
| `poison_pill.rs` | Orphaned-`RUNNING` reclaim by dead-worker heartbeat | 1 | **(c)** | **No analog** — a single-writer process has no *other* worker whose crash strands a `RUNNING` row; process crash is recovered by replay on restart (the spike's scenario 4). |
| `concurrency.rs` | Per-key concurrency claim gate | 1 | **(c)** | Advisory-lock fair-share across *concurrent* claimers is meaningless with one writer. |
| `admission_gate.rs` | Advisory-lock admission gate — holds the **sole** `pg_advisory_xact_lock(377)` call (`admission_gate.rs:920`) | — | **(c)** | A coordination primitive for *many* claimers; a serialized single writer needs no advisory lock. |
| `external_task.rs` | External-activity `FOR UPDATE` claim | — | **(c)** | Multi-process external-task delivery; out of scope. |
| `scheduler.rs` | HA schedule tick with claim-token exclusivity (#350) | — | **(c)** | Multi-replica claim exclusivity is definitionally multi-node. Single edge node = single scheduler; trivial. |
| `build_routing.rs` | Build-id routing, `INTERVAL` reachability | — | **(c)** | Fleet-deploy concept; no meaning on one edge binary. |
| `sessions.rs` | Worker sessions, `gen_random_uuid` (DDL), `INTERVAL` | — | **(c)** | Co-locate activities across a *fleet*; a single writer trivially co-locates everything. |
| `pool.rs` | Two `AsyncPgConnection` pools with shared ceiling | — | **(a)** | One `rusqlite::Connection`; no pool. |
| `schema.rs` / `models.rs` | 30 Diesel `table!` + Queryable/Insertable | — | **(a)** | Hand-written SQLite DDL (dialect differs: TEXT UUIDs/JSON, INTEGER epochs). **Proven** — `sqlite_spike/schema.rs` (6 tables). |
| `migrations/` | 73 timestamped PG DDL dirs | — | **(a)** | A single idempotent `CREATE TABLE IF NOT EXISTS` batch. **Proven**. |
| *(sweep — the remaining ~15 `AsyncPgConnection` modules)* | `audit.rs`, `batch.rs`, `calendar.rs`, `context.rs`, `dlq.rs`, `erase.rs`, `handle.rs`, `heartbeat.rs`, `reset.rs`, `schedule_decision.rs`, `testing.rs`, `usage.rs`, `version_gate_retirement.rs`, `version_usage.rs`, `workers.rs` | 0 | **(c)/peripheral** | None is a blocker or in spike scope. Split two ways: **fleet/multi-node concerns** with no single-writer analog (`workers.rs`/`audit.rs`/`batch.rs`/`schedule_decision.rs`/`usage.rs`/`version_usage.rs`/`version_gate_retirement.rs` — worker-fleet registry, cross-node audit, batch admin, HA schedule decisions, fleet build-version reachability) are class **(c)**; and **support surfaces the four scenarios simply didn't exercise** (`dlq.rs`/`heartbeat.rs`/`reset.rs`/`calendar.rs`/`erase.rs`/`handle.rs`/`context.rs`/`testing.rs`) are portable class (a)/(b) plumbing left unbuilt. This row exists so the "every `AsyncPgConnection` module" bar is visibly met: all **35** are now accounted for. |

**Reading of the table.** The *core coordination surface a durable engine
minimally needs* — event store **(a)**, task queue + timers + signals **(b)** —
is entirely class (a)/(b) and was **proven portable by the prototype**. The bulk
of the class **(c)** surface (poison-pill reclaim, per-key concurrency, HA
scheduler, build routing, sessions, external tasks, cross-shard/cross-workflow
fan-out) is Postgres-shaped **because it coordinates many writers/nodes** — and
therefore has no single-writer analog and is legitimately out of scope for an
edge runtime, not a blocker to it.

---

## 4. `StorageBackend` seam sizing (the in-crate option's cost)

If Harvest were to host both backends behind a trait, the seam would sit at four
boundaries the spike already isolated:

1. **Event store** — `append(exec, events)` / `load(exec) -> Vec<WorkflowEvent>`
   / `load_since(exec, seq)`.
2. **Task queue** — `enqueue(task)` / `claim_next_ready(...) -> Option<Task>` /
   `complete`/`requeue_for_retry`.
3. **Timer + signal delivery** — `arm_timer`/`due_timers`/`fire`, `stage_signal`/
   `take_pending_signal`.
4. **Notifier** — `notify(queue, task)` / `wait_for_wakeup()` (a no-op poll on
   SQLite).

Roughly **12–18 trait methods**, of which the spike implements the ~10
load-bearing ones. That part is small. The cost is **not** the trait — it is the
**cost to the shipped Postgres path** of adopting it:

- **Performance — a weaker leg than it first appears.** The task-claim path is
  the engine's hottest loop, and a `dyn StorageBackend` object would add a virtual
  call on every claim. But this is *avoidable*: a `StorageBackend` taken as a
  monomorphized generic (`fn worker<S: StorageBackend>(…)`) is statically
  dispatched with **zero vtable cost**, so "hot-path indirection" is not an
  inherent tax of the trait — only of the `dyn`-object form. The genuine
  performance-adjacent cost is subtler: Postgres-specific query shapes (`SKIP
  LOCKED`, `RETURNING`, advisory locks) would either hide behind a
  lowest-common-denominator method or leak back through backend-specific escape
  hatches, eroding the abstraction. So the decline rests not on dispatch cost but
  on the **two real legs below** — the wide-but-half-empty trait shape and the
  ×2 test matrix.
- **Code complexity.** All **35** `AsyncPgConnection`-touching modules would be
  refactored to speak the trait. Many of them (`poison_pill`, `concurrency`,
  `admission_gate`, `scheduler`, `external_task`, `build_routing`, `sessions`)
  are class **(c)** — they'd need `unimplemented!()`/error stubs on the SQLite
  side, so the trait would be *wide but half-empty*, which is worse than no
  abstraction.
- **Test-matrix blow-up.** Every one of the ~30 `db`-gated integration suites
  (`autumn-harvest/tests/integration/*` + the plugin suites) would need to run
  against **both** backends to be trustworthy — and most of them (SKIP LOCKED
  races, HA scheduler exclusivity, poison-pill reclaim, sharding) are
  *meaningless* on single-writer SQLite (§6), so the matrix would be ×2 in CI
  cost while covering the SQLite backend only shallowly.

This is the central argument against option (i): **the abstraction taxes the
90%-case (Postgres, multi-writer, production) to serve the 10%-case (SQLite,
single-writer, edge) that the core already serves as a plain library dependency
with zero tax** (option ii).

---

## 5. Prototype evidence

### 5.1 What the spike built

Feature-gated behind `sqlite-spike = ["dep:rusqlite"]`; the module lives at
`autumn-harvest/src/sqlite_spike/` and is **absent from the default build
graph** (§7). It reuses the core wholesale and reimplements only persistence:

| File | Role | Reuses from core |
|---|---|---|
| `mod.rs` | `SqliteRuntime` + the decision loop; calls the **reused** no-DB `run_workflow` once per cycle, then persists resulting commands and runs one worker pass | `run_workflow`, `WorkflowOutcome`, `WorkflowCommand`, `WorkflowEvent`, `WorkflowInfo`, `ExecutionId` |
| `schema.rs` | Hand-written SQLite DDL (6 tables): executions, events, tasks, timers, signals, activity-attempts audit | — |
| `store.rs` | Event append/load (the canonical replayable log) + signals + the per-attempt activity audit | `WorkflowEvent` (`serde_json` round-trip) |
| `queue.rs` | Activity task queue + durable timers; claim via `BEGIN IMMEDIATE` (single-writer substitute for SKIP LOCKED); polling substitute for LISTEN/NOTIFY | `ActivityExecId`, `ExecutionId` |
| `worker.rs` | `drain_ready`: runs activity bodies, applies retry, fires due timers, appends terminal events | `WorkflowEvent`, `TimerId` |

The decision loop is the whole idea in one paragraph: load history from SQLite →
call `run_workflow` once → on `Suspended`, persist each new command as event(s)
and enqueue work → run the worker pass to execute ready activities / fire due
timers → repeat until terminal or blocked on an external input. A "crash" is
modelled by dropping the runtime + its SQLite connection and reopening on the
same file — deterministic replay reproduces the identical command stream, so no
activity is re-executed.

**Persistence model — every event append is atomic with its companion state
mutation, with one honest residual window.** The persistence is **transactional
throughout**. The single-writer `BEGIN…COMMIT` is doing double duty: besides
substituting for `SKIP LOCKED` (one writer, no claim race), it gives **atomic
multi-row persistence**, mirroring the Postgres engine, which persists an event
and its task-queue row in one transaction. A full audit of every site that
appends a `WorkflowEvent` alongside a companion durable state mutation confirms
each pair now commits in **one** transaction — none is left as a pair of separate
autocommits:

- *Append-before-enqueue* (the apply side, `apply_commands`). A decision cycle's
  derived event and its paired task/timer row commit in one transaction, so a
  crash can never leave history saying an activity/timer is scheduled while its
  `spike_tasks`/`spike_timers` row is missing. (Before the fix this stranded the
  reload at `SpikeError::Stuck`: replay re-derived a `WaitForActivity` whose wait
  branch enqueued nothing.) The staged-signal path — `take_pending_signal`'s
  `delivered = 1` flag + the `SignalReceived` append — shares this same batch
  transaction.
- *Result-before-finish* (the drain side, `drain_ready`). A drained activity's
  terminal `ActivityCompleted`/`ActivityFailed` event and its task-state flip
  (plus the attempt audit row), or the retry requeue, commit in one transaction,
  so the task is never left "terminal event appended but row still `RUNNING`,"
  and a retry is never half-recorded.
- *Fire-before-flag* (timer firing, `drain_ready`). Each due timer's `TimerFired`
  append + `fired = 1` flag commit in one transaction. Before the fix these were
  separate autocommits: a crash between them left the `TimerFired` event durable
  with `fired = 0`, so the reload's `due_timers` scan re-fired the timer and
  appended a stray **second** `TimerFired` — a non-lifecycle event the workflow
  consumed only once, which would later diverge replay.
- *Terminal event before state flip* (`drive_one_cycle`). The `WorkflowCompleted`
  /`WorkflowFailed` event + the execution-state flip commit in one transaction.
  Before the fix a crash between them left the terminal event in history while the
  row stayed `RUNNING`; the reload's top-of-cycle "already terminal?" check would
  miss it, re-run the workflow, and append a **duplicate** terminal event.
- *Start row before start event* (`start_workflow`, `import_execution`). The
  execution row + its `WorkflowStarted` event (or the whole imported history)
  commit in one transaction, so a crash mid-start can never leave a `RUNNING`
  execution with a missing or torn history.
- *Import materializes derived state for open work* (`import_execution`). When an
  imported cross-backend history carries an **open** (un-terminated)
  `ActivityScheduled` or `TimerStarted`, the importer rebuilds the companion
  `spike_tasks`/`spike_timers` row a live cycle would have created — inside the
  same import transaction — so a cross-backend handoff of a *partially-complete*
  execution drains correctly instead of wedging at `SpikeError::Stuck` (replay
  re-derives the wait, `apply_commands` skips enqueueing because the schedule is
  already in history, and the worker would otherwise have no row to drain). Open
  timers are re-armed relative to import time (`fire_at = clock + duration_secs`),
  since a source backend's absolute deadline is not portable across backends.
- *Deterministic side-effect commands are persisted, never dropped*
  (`apply_commands`). The issue #384 primitives (`system_now`/`new_uuid`/
  `random_*`/`side_effect`) and version-gate markers emit `RecordSideEffect`/
  `RecordMarker` — often *before* a suspending activity in the same batch. These
  are persisted as `SideEffectRecorded`/`MarkerRecorded` in command order, so
  replay recovers the recorded value at the same cursor position rather than
  diverging on the following scheduled-work event (or re-minting). The
  `apply_commands` match is exhaustive (no `_` wildcard): every command is
  persisted, a single documented no-op (`WaitForActivity`), or an explicit
  `SpikeError::Unsupported` that rolls the batch back — no command is ever
  silently dropped, and a newly-added engine `WorkflowCommand` fails the spike
  compile instead of vanishing.

**The virtual clock is itself durable.** It is persisted on every advance
(`spike_meta`) and restored on `open` (belt-and-braces: never below any
already-fired timer's deadline). Timers store an **absolute** `fire_at`, so a
prior implementation that reset the clock to 0 on reopen left any timer armed at a
non-zero logical time permanently not-yet-due — a durability bug for the very
timer-across-restart property the spike exists to prove. It now holds for timers
armed at **any** logical time, not just the first.

The **one residual window** is a crash *while the activity body itself is
mid-execution* — after the task is claimed `RUNNING` but before the post-body
transaction commits. The body is run *outside* any transaction deliberately: a
`SQLite` write lock cannot be held across arbitrary user work, and the PG engine
does not hold one across the activity body either. Such a crash leaves the task
`RUNNING` and un-reclaimed (the reload path's `claim_next_ready_task` only claims
`PENDING` rows; `classify_block` surfaces this honestly as [`SpikeError::Stuck`]
rather than mislabelling it a timer wait). This is inherent to a *single-process*
spike, which has no other worker and so no heartbeat-timeout or poison-pill
reclaimer. A productized edge runtime would recover it exactly as the Postgres
engine already does — the heartbeat-timeout scanner (`timeout.rs`) + poison-pill
reclaim (`poison_pill.rs`) — which is out of a single-process spike's scope. The
prototype's atomicity is asserted directly by four tests
(`scenario_atomic_persist_event_and_row_never_out_of_sync` — the event/row
invariant survives a reload;
`scenario_atomic_persist_rolls_back_the_whole_batch_on_unsupported_command` — a
batch that fails partway rolls back *both* the event and its row;
`scenario_timer_fire_is_atomic_no_double_fire_across_reload` — a fired timer's
event and `fired = 1` row agree across a reload, so it is never re-fired; and
`scenario_two_timers_second_fires_across_restart_via_durable_clock` — a timer
armed at a non-zero logical time fires after a restart via the durable clock).

### 5.2 What it proved (live test run)

`cargo test -p autumn-harvest --no-default-features --features
sqlite-spike,testing --test integration sqlite_spike` → **13 passed; 0 failed**
(no Docker — SQLite is embedded via `rusqlite`'s `bundled` feature):

| Test | Proves |
|---|---|
| `scenario_1_activity_retry_then_success` | Activity fails on attempt 1, succeeds on attempt 2; workflow completes with the correct value; exactly 2 invocations. |
| `scenario_2_timer_fires_across_restart` | Arm a durable timer, drop the runtime (== process exit) **before** it fires, reopen on the same file, advance the clock — the timer fires and the workflow completes. Durable timers survive restart. |
| `scenario_3_signal_delivery` | Workflow blocks on `wait_for_signal`; an out-of-band `deliver_signal` unblocks it; the payload is echoed. |
| `scenario_4_deterministic_replay_after_crash` | Drive one cycle so the activity **completes and is persisted** but the workflow is not resumed past it; drop the runtime; reopen; the workflow resumes **purely by replay** — the activity is **not** re-executed (invocation counter stays at 1). |
| `scenario_cross_backend_replay` | A history *written by the SQLite prototype* replays cleanly on the engine's own (`testing`) `WorkflowReplayer` path, because both backends serialize the **same** `WorkflowEvent` via `serde_json`; and a terminal-stripped history drives the prototype's own reload path to the identical outcome with no duplicate activity execution. |
| `scenario_cross_backend_replay_retry_history` | **(executed, both-directions closure)** A *retry-bearing* history — produced by driving the fail-once-then-succeed scenario — round-trips through the engine's `WorkflowReplayer` with `ReplaySucceeded`. Closes the §5.3 gap that the original cross-backend test only exercised a success-path, single-attempt history. |
| `scenario_pg_shaped_history_replays_on_sqlite` | **(executed, "vice versa")** A hand-built, genuinely **PG-shaped** history — `WorkflowStarted → ActivityScheduled → ActivityStarted → ActivityCompleted`, *including* the `ActivityStarted` event the spike itself never writes — is imported into a fresh SQLite runtime and drives the prototype's own reload path: it resumes deterministically to the identical output with the already-completed activity **not** re-executed. Closes the §5.3 gap that no `ActivityStarted`-bearing PG history had actually been pushed through the prototype. |
| `scenario_atomic_persist_event_and_row_never_out_of_sync` | **(Codex P1 regression)** Across a reload, every `TimerStarted`/`ActivityScheduled` event is observed together with its `spike_timers`/`spike_tasks` row — never one without the other. Guards the cross-table invariant the atomic per-cycle transaction (§5.1) upholds. |
| `scenario_atomic_persist_rolls_back_the_whole_batch_on_unsupported_command` | **(Codex P1 regression, both-or-neither)** A `[ScheduleActivity, StartChildWorkflow]` batch appends+enqueues the activity, then hits the unsupported child command and returns `Err`, dropping the uncommitted transaction — **neither** the `ActivityScheduled` event **nor** its task row persists. Fails on the pre-fix per-command-autocommit code (where the activity event + row would have committed before the unsupported command was reached). |
| `scenario_timer_fire_is_atomic_no_double_fire_across_reload` | **(Codex P1 regression)** A fired timer's `TimerFired` event and its `fired = 1` row agree — including across a reopen, where `due_timers` then finds nothing to re-fire (exactly one `TimerFired`). Guards against the pre-fix separate-autocommit window that re-fired the timer into a stray duplicate `TimerFired`. |
| `scenario_two_timers_second_fires_across_restart_via_durable_clock` | **(Codex P1 regression)** A workflow fires a first timer (advancing the virtual clock), arms a second timer at that advanced clock (absolute `fire_at = 10`), advances *part-way* (to 8, captured only by the durable clock — not the fired-timer floor of 5), then **restarts**; advancing 2 more reaches 10 and the second timer fires. Fails on the pre-fix code that reset the clock to 0 on reopen (the second timer would never become due). |
| `scenario_import_in_flight_activity_materializes_task_and_drains` | **(Codex P1 regression, round 4)** An imported PG-shaped history with an **open** (un-terminated) `ActivityScheduled` (`Scheduled → Started`, no `Completed`) materializes the companion PENDING `spike_tasks` row on import, so the worker claims + runs it and the imported execution drains to `Completed` (activity runs exactly once). Fails on the pre-fix code where the import path created an "event without companion row" and the run wedged at `SpikeError::Stuck`. |
| `scenario_side_effect_command_persists_and_replays_across_restart` | **(Codex P2 regression, round 4)** A workflow calls `ctx.new_uuid()` (an issue #384 deterministic side effect) before a suspending activity; the minted value is frozen into history as `SideEffectRecorded` (before the activity schedule, in command order) and recovered **verbatim** on a fresh runtime after a restart. Fails on the pre-fix code that silently dropped the `RecordSideEffect` command (no `SideEffectRecorded` reaches history → replay diverges / re-mints). |

### 5.3 Honest fidelity caveat — retry recording (verified against the PG engine)

This is the finding a feasibility report exists to surface, stated plainly.

**The prototype records a *retryable* activity failure in an
`spike_activity_attempts` AUDIT table and the task-queue row's `attempt`
counter — it does NOT append an `ActivityFailed` event to the replayable log.**
The replayable event log for a fail-once-then-succeed retry is therefore
`ActivityScheduled → ActivityCompleted`, with **no** intermediate
`ActivityFailed` (asserted directly in `scenario_1`, lines 128–135).

**Verified truth about the Postgres engine: it does the same thing.** Reading
the live code:

- `queue::requeue_for_retry` (`queue.rs:1230`) is what the PG engine calls for a
  *retryable* failure (`worker.rs:6799`). It **only updates the task-queue row**
  — it writes `previous_error` to the row's `error` column via
  `PendingRequeueChangeset` and re-pends the row. **It appends nothing to
  `harvest_events`.**
- `ActivityFailed` reaches `harvest_events` **only** on a *terminal*
  (retry-exhausted) failure, via `finalize_activity_failure` (`worker.rs:5955`),
  taken when `next_retry_delay` returns `None` (`worker.rs:6806`).

So on the retry recording question the prototype and the PG engine **agree**:
neither records `ActivityScheduled → ActivityFailed(attempt=n) → ActivityScheduled
→ ActivityCompleted`. Both record `ActivityScheduled → ActivityCompleted` for a
recovered retry; the intermediate failure lives off the replayable log (on the
task row in PG, in the audit table in the prototype). The prototype's module
docs claim this parity, and it holds.

**The original caveat was about test coverage — now closed by an executed
round-trip.** The first cut's `scenario_cross_backend_replay` only exercised a
**success-path, single-attempt** history. The review pass added
`scenario_cross_backend_replay_retry_history`: it drives the *fail-once-then-
succeed* scenario through the prototype and round-trips the resulting
(terminal-clean, `ActivityScheduled → ActivityCompleted`) history through the
engine's `WorkflowReplayer` with `ReplaySucceeded`. So a **retry-produced**
history's cross-backend replay is now **proven by an executed test**, not argued.
One honest boundary remains: the parity claim *about the Postgres engine's own
internals* — that PG stores a retryable error on the task-queue row, never in
`harvest_events` — is verified by **code reading** (the citations above), because
this spike has no Postgres to run against; it cannot be an executed PG-side test
here.

**The second nuance — `ActivityStarted` — is also now executed in both
directions.** The PG engine appends an `ActivityStarted` on claim
(`append_activity_started_if_pending`, `worker.rs:2678`) that the prototype
**never** writes: a genuine PG history is `ActivityScheduled → ActivityStarted →
ActivityCompleted`, the prototype's is `ActivityScheduled → ActivityCompleted`.
The two **replay identically** because `HistoryMatcher::scan_activity_terminal`
*skips* `ActivityStarted` (`replay.rs:1041`). The precise statement is therefore
**"per-event `serde_json` encoding is byte-identical + the event sets are
replay-equivalent,"** *not* "identical event streams." The review pass added
`scenario_pg_shaped_history_replays_on_sqlite`, which hand-builds a genuinely
`ActivityStarted`-bearing PG-shaped history and drives it through the prototype's
own reload path — resuming to the identical output with the activity not
re-executed — so the "vice versa" direction (a PG-shaped history on the SQLite
runtime) is now **executed**, not merely argued from the matcher-skip.

Neither caveat changes the go/no-go: the **invariant that matters** — every
persisted event is the same `WorkflowEvent`, serialized the same way, replaying
under the same matcher — holds, and is now demonstrated by executed tests in
**both** cross-backend directions (SQLite→engine, incl. a retry history; and
PG-shaped→SQLite, incl. an `ActivityStarted`-bearing history). The one residual
code-read (not executed) item is the PG engine's *internal* retry-recording
mechanism, unavoidable without a Postgres to exercise.

---

## 6. Test-portability analysis (AC3)

The default no-DB unit/integration suite (`cargo test -p autumn-harvest
--no-default-features` → **1565 passed / 0 failed**; also green with `--features
testing`) is the pool of tests that exercise backend-neutral logic. The
*integration* suites that are `db`-gated split cleanly:

**Semantically portable to single-writer SQLite** (the behavior under test is
backend-neutral or single-writer-compatible):

- `replayer_tests.rs`, `replay_tests.rs` — pure determinism; no DB semantics.
- `workflow_test_env_tests.rs` — the no-DB harness; already backend-free.
- The core happy-path of `integration_e2e.rs` — start → activity → complete,
  timer fire, signal delivery (exactly what the spike's 5 scenarios cover).
- `schedule_*` *content* logic (cron parsing, catchup windows) minus the HA
  claim exclusivity.

**Meaningless on single-writer SQLite** (the test's entire point is multi-writer
/ multi-node Postgres semantics):

- `scheduler_ha_tests.rs` — **HA scheduler claim exclusivity (#350)**: asserts
  that concurrent replicas each claim a due schedule slot at most once via
  `fire_claim_token`. A single edge node has one scheduler; there is nothing to
  make exclusive.
- `poison_pill_tests.rs` — **orphaned-`RUNNING` reclaim (#367)**: the whole
  mechanism recovers a row stranded `RUNNING` by a *crashed peer worker* with no
  live heartbeat. A single writer has no peer; a process crash is recovered by
  replay-on-restart (the spike's scenario 4), not by reclaim.
- `queue_fairness_tests.rs` / any SKIP-LOCKED race test — assert disjoint claims
  under contention; single-writer claims are never contended.
- Sharding suites (cross-shard fan-out, shard health, `all_build_reachability_
  sharded`) — sharding is multi-database by definition.
- `build_ramp_integration` / build-routing suites — fleet-deploy concepts.
- `sessions`/`worker_session_tests.rs` — fleet co-location.

**Takeaway:** the *portable* suites are exactly the ones covering the core the
spike reused; the *meaningless* ones are exactly the class **(c)** surface from
§3.2. This is strong corroboration that the backend seam falls on a clean line.

---

## 7. Invariants preserved (AC4 / AC5)

- **Append-only history.** The prototype's `spike_events` table is strictly
  append-only (`INSERT … (exec_id, seq, event_json)` ordered by `seq`); no event
  is ever mutated or reordered. Same invariant as `harvest_events`.
- **Adjacently-tagged JSON, per-event byte-identical across backends.** Every
  event row is `serde_json::to_string(&WorkflowEvent)` — the exact `#[serde(tag =
  "type", content = "data")]` encoding (`event.rs:68`) the Postgres backend uses.
  The event *streams* are replay-equivalent, not byte-identical (the PG engine
  also writes `ActivityStarted` per claim; §5.3). This is **proven, in both
  directions, by executed tests**: `scenario_cross_backend_replay` and
  `scenario_cross_backend_replay_retry_history` replay SQLite-written histories
  (success-path *and* retry) on the engine's `WorkflowReplayer` with
  `ReplayStatus::ReplaySucceeded`; and `scenario_pg_shaped_history_replays_on_sqlite`
  drives a genuinely `ActivityStarted`-bearing PG-shaped history through the
  prototype's reload path to the identical outcome (the one residual code-read
  item — the PG engine's internal retry recording — is disclosed in §5.3).
- **Zero core-logic changes; additive registration only.** Precisely stated: the
  only edit to the crate outside the self-contained `sqlite_spike/` module is one
  additive, feature-gated line in `lib.rs` (`#[cfg(feature = "sqlite-spike")] pub
  mod sqlite_spike;`) plus the `sqlite-spike = ["dep:rusqlite"]` Cargo feature. No
  existing function, type, or event was modified.
- **Macro paths untouched.** The spike adds no macro and touches no
  `::autumn_harvest::` path plumbing; the test workflows are ordinary
  `#[workflow]` fns whose macro-generated `WorkflowInfo` is registered as-is.
- **Zero new `WorkflowEvent` variant.** The prototype writes only existing
  variants (`WorkflowStarted`, `ActivityScheduled`/`Completed`/`Failed`,
  `TimerStarted`/`Fired`, `SignalReceived`, `WorkflowCompleted`/`Failed`). No
  variant added, no migration.
- **Zero regression to the default Postgres build.** Verified two ways:
  - *Dependency graph:* `cargo tree -p autumn-harvest -i rusqlite` on default
    features → *"did not match any packages"* (rusqlite is absent from the
    default graph). Under `--features sqlite-spike`, the graph carries a **single**
    `libsqlite3-sys v0.36.0` — it **unifies** with the copy already pulled by
    `autumn-web`/`diesel`, so **no duplicate `libsqlite3-sys`** is introduced
    (verified: exactly one entry in `Cargo.lock`).
  - *Build/test:* `cargo build -p autumn-harvest` (default) and `cargo check
    --workspace` are green; `--all-features` clippy `-D warnings` is clean; the
    no-DB suite is 1565/0. The spike compiles out entirely.

---

## 8. Sync / handoff-to-hub scoping (AC6 — scoping only, nothing built)

An edge node running the SQLite engine will eventually want to **hand off** its
durable histories to a central Postgres hub (roaming device reconnects, edge→
cloud aggregation). This section scopes what that would require; **none of it was
built.**

- **`ExecutionId` shard-byte semantics on an edge node.** `ExecutionId` encodes
  a `ShardId` in its first two bytes (`types.rs:206`). The prototype mints ids
  with `ExecutionId::new()`, which emits the reserved sentinel
  `ShardId::UNENCODED` (`0xFFFF`, `types.rs:98/184`). A router resolves
  `UNENCODED` to the configured *default* shard. **The problem:** if every edge
  node mints `UNENCODED` ids and hands them to a sharded hub, they all collapse
  onto the hub's default shard (a hotspot), and two edge nodes can mint
  *colliding* ids only up to UUID-v4 randomness — fine for uniqueness, but the
  shard byte carries no *origin* information. A real edge→hub design needs either
  (a) a per-edge-node `ShardId` allocation (the hub assigns each node a shard, and
  the node mints `ExecutionId::new_for_shard`), or (b) a hub-side re-homing step
  that rewrites the shard bytes on ingestion (`ExecutionId::new_for_shard` builds
  a fresh id; the *history* would need its `execution_id` references rewritten
  consistently). Option (a) is cleaner and keeps ids stable across the handoff.
- **Conflict rules on handoff.** The hub must decide what a re-ingested id means:
  first-writer-wins, or reject-if-exists. Because histories are append-only and
  the edge node is the *sole* writer of its executions, idempotent re-ingestion
  is natural — replaying the same event stream twice is a no-op if keyed on
  `(exec_id, seq)`.
- **Replay-fidelity guarantee.** A history produced on the edge **must replay
  byte-identically on the hub.** The byte-identical-encoding invariant (§7) —
  proven by the cross-backend replay test — is exactly this guarantee: the hub's
  `HistoryMatcher` will drive the same commands from the edge-written history.
  (The §5.3 `ActivityStarted`/retry caveats apply: the hub would *add*
  `ActivityStarted` on any *new* work it runs, but a fully-terminal
  edge history needs no further work and replays as-is.)
- **Idempotent re-ingestion.** Keyed on `(exec_id, seq)` with `INSERT … ON
  CONFLICT DO NOTHING`, a retried sync is safe.

**Alternatives to plain SQLite (one paragraph each, per Out-of-Scope):**

- **libSQL / Turso.** A SQLite fork with built-in **embedded replication** and a
  server protocol. It would let the edge node *stream* its WAL to a Turso/libSQL
  hub instead of hand-rolling the §8 sync — arguably the strongest fit for the
  edge→hub story, since replication + conflict handling is the product. The
  trade-off is a heavier dependency and a hosted/self-hosted server component,
  which partially reintroduces the "run a server" cost the edge case is avoiding.
- **DuckDB.** Columnar, analytics-oriented, embedded. A poor fit for the
  transactional single-row claim/update pattern a task queue needs (its strength
  is bulk analytical scans, not OLTP row churn), and no replication story. Not
  recommended for the engine substrate, though plausibly interesting for an edge
  *analytics/reporting* sidecar over the event log.

---

## 9. Go / no-go recommendation (costed)

**Recommendation: pursue option (ii) — a separate companion crate.**

**(i) In-crate `StorageBackend` trait — DECLINE.** Cost to the shipped Postgres
path is the blocker — and it is **not** hot-path dispatch (a monomorphized
generic `StorageBackend` is statically dispatched with zero vtable cost; §4). The
two real legs are: a **wide-but-half-empty trait** (the class **(c)** modules
would stub out with `unimplemented!()`/error on the SQLite side, refactoring all
**35** `AsyncPgConnection` modules to speak a trait most of them can't honour),
and a **×2 integration test matrix** that covers the SQLite backend only
shallowly (§6). The spike specifically demonstrates this cost is
**unnecessary** — the core needed zero changes to be reused.

**(ii) Separate companion crate — RECOMMEND.** Publish the backend-neutral core
(`event`, `replay`, `context`, `executor`, `types`, `info`) as a library, and
build the SQLite/edge engine as a **separate crate that depends on it** — exactly
the shape the prototype already has (it reaches into `crate::run_workflow`,
`crate::event`, `crate::executor` and reimplements only `store`/`queue`/`worker`).
The Postgres path is **completely untouched**: no trait, no hot-path indirection,
no test-matrix blow-up. The core is already reusable — the spike is the proof.
The main engineering work becomes *stabilizing the core's public surface* as a
dependency contract (today `run_workflow`/`WorkflowCommand`/`WorkflowOutcome` are
`pub` but evolve freely), plus productizing the edge runtime (a real poll loop,
signal ingress, an embedding API). One concrete surface item to resolve:
`ActivityContext::run_transactional` (`context.rs:10528`) is a **public** API that
hands activity code a raw `&mut diesel_async::AsyncPgConnection` — a
Postgres-typed leak in an otherwise backend-neutral surface. The "core is
backend-neutral" claim holds for every primitive the spike exercises (the spike's
activities never touch `run_transactional`), but this signature is exactly the
kind of thing the separate-crate path must abstract or gate when the core's public
API is stabilized as a dependency contract. The §5.3 fidelity caveats are the
other known follow-ups to close before productization.

**(iii) Decline entirely — acceptable fallback.** If edge/local-first is not a
near-term product priority, shipping nothing is a legitimate outcome; the spike
cost was one timeboxed branch and this report. The demand (§2, §11) is real and
growing, so revisiting later is likely, but there is no obligation created here.

**My recommendation, grounded in the evidence:** **(ii).** The single most
important thing the spike learned is that the *hard, valuable* part of Harvest —
deterministic replay — is already backend-neutral and reusable **with zero cost
to the production Postgres engine**. That fact makes the separate-crate path
strictly dominate the in-crate path: same capability, none of the tax.

---

## 10. Timebox statement (AC7)

This was an explicitly **timeboxed feasibility spike**, and it was run as one:

- The prototype was built to answer the feasibility question, **not** to be
  productized. It has no real poll loop (tests drive the poll and a virtual
  clock explicitly), no signal ingress API beyond a test helper, no management
  surface, and only the primitive subset the four scenarios need
  (`ContinueAsNew`, child workflows, external signals, local activities, updates,
  search attributes, etc. are all `Unsupported`/no-ops).
- Once the four scenarios + the cross-backend check passed, the prototype was
  **abandoned rather than polished**. No effort was spent hardening it.
- This is **evidence-gathering, not productization**. The deliverable is *this
  report + the disposable prototype as its exhibit*, not a shippable feature.
- The spike branch (`feat/966-sqlite-feasibility-spike`) is **never merged to
  `trunk-dev` as-is.** The prototype is feature-gated (`sqlite-spike`, off by
  default) precisely so its presence on a branch cannot affect anything; the
  intended end state is that its *lessons* (this report, option ii) inform a
  future decision, and the throwaway code is discarded or reborn in a companion
  crate.

---

## 11. Gap analysis vs. prior art

The competitive landscape confirms the category is real and **actively being
served** — an embedded/local-first durable engine is no longer speculative:

| Engine | Substrate | Embedded / local-first story |
|---|---|---|
| **DBOS** | **SQLite (default)** or Postgres | **Closest precedent — an in-process durable-execution *library* (like Harvest's plugin) that ships embedded SQLite *by default*.** Recommends Postgres "for production" because a SQLite file "can't be used in a distributed setting where an application runs on multiple servers" — the same single-writer / single-server bound this spike found. (Citation in References; accessed 2026-07-14.) |
| **Restate** | Single self-contained binary; no external DB | The single binary *is* the durable runtime; your app's SDK connects to it — a **separate server process**, not an in-process library. Strong evidence the "no external database" demand is real; a different form factor from Harvest's (and DBOS's) in-process model. |
| **Oban** (Elixir) | Postgres, MySQL, **or SQLite** | **Not** Postgres-only — ships a built-in SQLite engine (`Oban.Engines.Lite`) alongside Postgres/MySQL. A job queue, not a replay-based workflow engine, but direct evidence the embedded-SQLite-for-durable-jobs demand is both real and *serviceable*. Its own docs note SQLite "may not be suitable for high-concurrency systems" — again the single-writer bound. |
| **Temporal** | Cassandra/MySQL/Postgres server + separate service | No *production* embedded mode; the heavyweight server + workers is the product. Its dev server (`temporal server start-dev`, formerly *temporalite*) does run on embedded SQLite, but "for development or testing and not production use." |
| **Hatchet** | Postgres server | Server-backed; no embedded story. |
| **Inngest** | Managed / dev-server | Dev-server for local iteration only; not an embeddable durable engine. |

**Reading:** the corrected landscape *strengthens* the feasibility case rather
than resting it on novelty. DBOS already ships an in-process durable-execution
library on **embedded SQLite by default** — so the category Harvest would enter is
real and shipped, not speculative — and DBOS's own "can't be used in a distributed
setting" caveat independently confirms this spike's single-writer finding. Oban
shows the embedded-SQLite-for-durable-jobs demand is real *and serviceable* (it
ships a SQLite engine); Restate shows the "no external database" demand is real (a
single self-contained binary). **Harvest would join these, not pioneer a new
category.** Its differentiator is *not* being first on SQLite — it is a **Rust,
deterministic-replay durable engine whose replay core is already backend-neutral**
(none of the above offered to reuse a production replay engine unchanged; the
spike shows Harvest can), plus a clean Postgres-hub handoff (§8). The go/no-go
rests on **feasibility + real demand + zero cost to the production Postgres path** —
not on being first — and every one of those holds independent of this prior-art
correction. **Verdict unchanged: feasible; pursue option (ii), the separate
companion crate.**

---

## References

- Issue #966 — SQLite edge/local-first harvest feasibility spike.
- Prototype: `autumn-harvest/src/sqlite_spike/` (feature `sqlite-spike`).
- Smoke suite: `autumn-harvest/tests/integration/sqlite_spike_tests.rs`.
- Retry-recording verification: `queue::requeue_for_retry` (`queue.rs:1230`),
  `finalize_activity_failure` (`worker.rs:5955`), retry split (`worker.rs:6799`).
- `ActivityStarted` skip on replay: `HistoryMatcher::scan_activity_terminal`
  (`replay.rs:1041`); append site `append_activity_started_if_pending`
  (`worker.rs:2678`).
- Shard-byte semantics: `ExecutionId` (`types.rs:98/183/202/220`).
- Adjacently-tagged event contract: `WorkflowEvent` (`event.rs:68`).
- `docs/adr/0001-otel-trace-contract.md` — analytical-depth template for this
  report.

### Prior-art citations (§11; all accessed 2026-07-14)

- DBOS system database — SQLite is the default; "because a SQLite database is
  just a file on disk, it can't be used in a distributed setting where an
  application runs on multiple servers … for production, we recommend using
  Postgres": <https://docs.dbos.dev/python/tutorials/database-connection>.
- Oban ships a built-in SQLite engine (`Oban.Engines.Lite`) alongside Postgres
  and MySQL: <https://github.com/oban-bg/oban>.
- Restate is a single self-contained binary / server (SDK-in-app + separate
  server), no external database: <https://docs.restate.dev/server/overview>.
- Temporal dev server runs on embedded SQLite, "for development or testing and
  not production use": <https://docs.temporal.io/cli/server>.
