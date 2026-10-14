## Phase — SQLite: list and describe executions, search attributes and memo (issue #2027)

The SQLite runtime read one execution at a time by id. It had no list, no
filter and no describe. `upsert_search_attrs` sealed the run `FAILED`. The
agent daemon read `harvest_executions` with its own SQL.

**What shipped**

- `SqliteRuntime::list_executions(&ExecutionFilter)` returns one page of runs,
  newest first, with no payloads. `next` is the cursor of the next page.
- `visit_executions` passes the same rows with their payloads, one row at a
  time. `describe(exec)` reads one run.
- `SqliteReader::open(path)` has the same three reads. It opens the file
  `READ_ONLY` and takes no writer lock. It reads a file that an older version
  wrote: a missing column reads as `Null`.
- `ExecutionFilter` filters by workflow name, execution id, exact workflow
  id, workflow id prefix, states, a start-time range and search-attribute
  equality. The search-attribute filter uses JSON1. Numbers compare as
  numbers. A filter integer above `i64::MAX` is refused, because JSON1 reads
  it as a float. A filter holds at most 64 states and 64 search attributes.
- One state filter seeks on the new `idx_harvest_executions_state` index.
- Each field is a `Stored<T>`: `Value`, `Null` or `Unreadable`. One damaged
  row does not fail a page.
- `ctx.upsert_search_attrs` merges its patch in the cycle transaction, on the
  suspend path and on the terminal path, as on Postgres. It is not progress.
- `start_workflow_with_options` takes a workflow id, a reuse policy, a memo
  and search attributes. A continue-as-new copies the memo and the search
  attributes.
- New errors: `SqliteError::InvalidSearchAttribute` and
  `SqliteError::InvalidFilter`.
- `StartOptions` has builder methods, as `ExecutionFilter` has.
- The agent daemon reads executions through `SqliteReader`. Its listing
  decodes each document with the decode `status` runs, so the two views
  agree.

**Behavior change.** A workflow that upserts search attributes no longer
ends `FAILED` as `UnsupportedFeature`. `SqliteError` has two new variants. An
exhaustive `match` on it must add the arms. A `WorkflowStarted` timestamp is
now cut to the microsecond, so the row and the event hold the same time.

**Invariants.** No new `WorkflowEvent` variant. The additive `migrate` step
adds four nullable columns to `harvest_executions`: `started_at`,
`closed_at`, `search_attrs` and `memo`. Both times are epoch microseconds.
`started_at` is the `WorkflowStarted` time. The first terminal write sets
`closed_at`, and a later reuse-policy seal keeps it.

**Limits.** A row written before this change has no start time, so a time
filter does not match it. Search-attribute filters test equality only.

**Tests.** `tests/integration/visibility.rs` covers describe, paging and
every filter. It also covers upserts on both paths, a rolled-back cycle,
continue-as-new, the reader beside a live runtime, damaged rows and an old
file. Unit tests assert that no page query sorts in a temporary B-tree, and
that one state seeks on its index. A daemon test refuses any source that
names `harvest_executions` outside a comment. The daemon tests pass.
