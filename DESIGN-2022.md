# Design — Issue #2022: SQLite workflow queries

A workflow on SQLite registers queries with `ctx.register_query(..)`. Today
nothing can call them. This change adds `SqliteRuntime::query`.

**No migration. No new `WorkflowEvent` variant. No write of any kind.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- The core has a read-only query driver: `executor::drive_query_replay_async`.
  It replays a handler to its frontier and appends nothing.
- The core classifies a query on a terminal run:
  `executor::classify_terminal_query`. The Postgres HTTP path uses it
  (issue #612).
- `WorkflowContext::execute_query_with_args` runs a handler that
  `ctx.register_query` registered. It does not catch a handler panic.
- The SQLite drive passes no declarative query handlers. No
  `register_declarative_query_handler` exists on this backend.
- `store::execution_state` returns `ExecutionNotFound` for an unknown id
  (issue #1735).
- `rusqlite::Connection` is `Send` but not `Sync`. An `async fn` on
  `&SqliteRuntime` gives a future that is not `Send`. Clippy `nursery`
  (`future_not_send`) then fails CI.
- The Postgres default query timeout is 5 s (`HarvestBuilder`).

### 0.2 Brainstorm — how can SQLite serve a query?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Run one real decision cycle and read the registry. | Rejected. A cycle persists commands and runs activities. |
| B2 | Keep the last drive's context in memory and query it. | Rejected. A restart loses it. A query before the first drive has none. |
| B3 | Load history, replay with `drive_query_replay_async`, then call the handler. | **Adopted.** The core proves the driver read-only. |
| B4 | Serve terminal runs post-mortem with `classify_terminal_query`. | **Adopted.** It matches the Postgres HTTP path. SQLite never prunes history. |
| B5 | `async fn query(&mut self, ..)`. | Rejected. A read that needs `&mut` hides the read-only contract. |
| B6 | `fn query(&self, ..) -> impl Future + Send + 'static`. Read the rows now; replay in the future. | **Adopted.** The future is `Send` and borrows nothing. |
| B7 | Wrap the core `HarvestError` in one `SqliteError::Query` variant. | Rejected. A caller must then match two levels. |
| B8 | Add flat typed variants: `QueryNotFound`, `QueryFailed`, `QueryPanicked`, `QueryTimedOut`, `QueryUnavailable`. | **Adopted.** This matches the style of the crate. |
| B9 | A configurable timeout, default 5 s. | **Adopted.** `set_query_timeout` sets it. |

### 0.3 Reverse brainstorm — how can a query go wrong?

| # | Failure | Guard |
|---|---------|-------|
| R1 | The query appends an event or changes a row. | Only `store` read functions run. A test compares every table and `PRAGMA data_version`. |
| R2 | The query consumes a staged signal. | The query never reads `harvest_signals`. A test stages a signal first. |
| R3 | The query runs an activity body. | Replay resolves results from history. A test counts body calls. |
| R4 | The answer is stale or lost after a restart. | The query rebuilds from the database. A test reopens the file. |
| R5 | The answer shows state from past the frontier. | Replay stops at the first unrecorded command. A test checks it. |
| R6 | A handler panic unwinds into the caller. | `catch_unwind` maps it to `QueryPanicked`. A test covers it. |
| R7 | A spinning workflow blocks the query forever. | The replay has a deadline. A test covers `QueryTimedOut`. The deadline holds only while the replay yields. |
| R8 | A caller cannot tell an unknown name from an unknown run. | Two typed variants. A test checks both. |
| R9 | The future cannot go to `tokio::spawn`. | A compile-time test checks `Send + 'static`. |
| R10 | Drift on a terminal run serves a wrong answer. | `classify_terminal_query` maps it to `QueryUnavailable`. `drifted_terminal_run_is_query_unavailable` covers it. |
| R11 | A seal for an unsupported feature or a history cap (issue #2023) rolls back its last cycle, side effects included. A replay mints new values, so each query answers differently. | A terminal replay that emits `RecordSideEffect` or `RecordMarker` is `QueryUnavailable`. A test covers it. |
| R12 | A query fires a due timer. | The query never reads the timer queue. A test sets the clock past the deadline and compares the tables. |
| R13 | Code past the last recorded event, a `side_effect` closure included, runs in each query. | Postgres does the same. The docs state it. A core replay-only mode is a follow-up. |

### 0.4 Six thinking hats

- **White (facts).** See 0.1. The core already owns replay, the driver and
  the classifier.
- **Red (feeling).** A user wants one call:
  `rt.query(exec, "status", json!(null)).await?`.
- **Black (risks).** Five new error variants grow the enum. The returned
  future is a point-in-time read: it reads the rows at call time. The core
  does not catch an imperative handler panic, so this backend must. Review
  found R11 and R13 after the first green phase.
- **Yellow (benefits).** Shared driver and classifier give parity with
  Postgres by construction. One process needs no routing to a worker.
- **Green (alternatives).** A sync API on `drive_query_replay`. It
  misclassifies a deferred `yield_now` wake inside a runtime, so this
  design rejects it.
- **Blue (process).** Red: tests against a stub. Green: the real method.
  Refactor: share the context setup with nothing new. Then docs, the
  changelog fragment, the comment-hygiene audit and clippy.

---

## 1. Behaviour

`query(exec, name, args)`:

1. Read the run state. An unknown id is `ExecutionNotFound`.
2. Read the workflow name. An unregistered name is `UnknownWorkflow`.
3. Read the history, input and business `workflow_id`.
4. Build a replay context. It carries the workflow name and id, as the
   drive does.
5. Replay with `drive_query_replay_async` under the query timeout.
6. Running run: a panic is `QueryPanicked`. A timeout is `QueryTimedOut`.
   Any other outcome serves. This matches the in-process Postgres query.
7. Terminal run: `classify_terminal_query` decides, as on the Postgres HTTP
   path. `HistoryUnavailable`, a replay panic included, is
   `QueryUnavailable`. A served replay that minted a side-effect value is
   also `QueryUnavailable` (R11).
8. Call the handler. An unknown name is `QueryNotFound`. A handler `Err` is
   `QueryFailed`. A handler panic is `QueryPanicked`.

Steps 1 to 3 run when `query` is called. Steps 4 to 8 run when the future
is polled.

## 2. Test plan

`autumn-harvest-sqlite/tests/integration/workflow_queries.rs`:

| Test | AC or risk |
|------|----|
| State set before the last suspension is returned; state after it is not. | AC1, R5 |
| The answer survives a restart. | AC1, R4 |
| A completed run answers post-mortem with its final state. | AC1 |
| Every table and `data_version` are unchanged by a query, a staged signal included. | AC2, R1, R2 |
| No activity body runs during a query. | AC2, R3 |
| A terminal and a sealed run are unchanged by a query. | AC2 |
| A due timer does not fire during a query. | AC2, R12 |
| An unknown query name is `QueryNotFound`. | AC3, R8 |
| An unknown execution is `ExecutionNotFound`. | AC3, R8 |
| An unregistered workflow is `UnknownWorkflow`. | — |
| Args reach the handler. Bad typed args and a handler `Err` are `QueryFailed`. | — |
| A handler panic, or a replay panic on a running run, is `QueryPanicked`. | R6 |
| A replay panic on a terminal run is `QueryUnavailable`. | R6 |
| A spinning replay is `QueryTimedOut` within the configured limit. | R7 |
| Drift on a terminal run is `QueryUnavailable`. | R10 |
| An unrecorded side effect on a sealed run is `QueryUnavailable`. | R11 |
| An unrecorded side effect on a history-capped run is `QueryUnavailable`. | R11 |
| A continued-as-new run serves a stable answer. | R11 |
| `ctx.info()` gives the stored workflow id and type. | — |
| The future is `Send + 'static`. | R9 |
