## Phase — SQLite: workflow queries (issue #2022)

A workflow on SQLite registered queries with `ctx.register_query(..)`, but
nothing could call them. `SqliteRuntime::query(exec, name, args)` now does.

**What shipped**

- `query` reads the history, input and workflow ids of the run. It replays
  them with the core `drive_query_replay_async`, then calls the handler.
- A running run answers with the state at its frontier, as the in-process
  Postgres query does.
- A terminal run answers with the state at its recorded end. The core
  `classify_terminal_query` decides, as on the Postgres HTTP path (issue
  #612).
- A seal for an unsupported feature or a history cap rolls back the last
  cycle. If the replay then mints a side-effect value that the history does
  not record, the query returns `QueryUnavailable`. A changing answer is
  worse than none.
- The call reads the rows at once. The returned future is `Send + 'static`
  and borrows nothing from the runtime.
- New typed errors: `QueryNotFound`, `QueryFailed`, `QueryPanicked`,
  `QueryTimedOut` and `QueryUnavailable`. An unknown id stays
  `ExecutionNotFound`.
- `set_query_timeout` sets the replay limit. `DEFAULT_QUERY_TIMEOUT` is 5 s,
  the Postgres builder default.
- A panic in a query handler does not reach the caller. It is
  `QueryPanicked`.

**Behavior change.** `SqliteError` has five new variants. An exhaustive
`match` on it must add arms.

**Invariants.** No new `WorkflowEvent` variant. No migration. A query writes
no row: it runs only `store` reads.

**Limit.** The replay runs workflow code past the last recorded event, as
on Postgres. A `ctx.side_effect` closure there runs again in each query.

**Tests.** `tests/integration/workflow_queries.rs` covers it. One test
compares every row of every table and `PRAGMA data_version` before and after
five queries, with a staged signal in place. Others cover state at the
frontier, a restart, terminal and sealed runs, and a due timer. More cover
the run identity, each typed error and a `Send + 'static` future.
