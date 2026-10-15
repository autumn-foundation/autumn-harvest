# Design — Issue #2027: list and describe executions on SQLite

The SQLite runtime reads one execution at a time by id. It has no list, no
filter and no describe. `upsert_search_attrs` seals the run `FAILED`. The
agent daemon reads `harvest_executions` with its own SQL for this reason.

This change adds a list, a describe and search attributes. It also adds a
memo. The daemon then reads executions through the backend.

**No new `WorkflowEvent` variant.** An additive migration adds four columns.

---

## 0. Planning record

### 0.1 Facts found before the plan

- `harvest_executions` holds no time, no search attributes and no memo. Its
  `rowid` is the only insert order.
- Postgres sets the memo at start only. No command changes it. A
  continue-as-new copies the memo to the successor.
- Postgres stores search attributes as a JSON object. `UpsertSearchAttributes`
  merges a patch: `Some` sets a key, `None` removes it. The worker writes it in
  the cycle transaction, on the suspend path and on the terminal path. It
  appends no event.
- The core suppresses `UpsertSearchAttributes` during replay. A cycle that
  rolls back sends the command again on the next drive.
- The core validates the patch keys and values. A value is a JSON primitive.
- The Postgres list pages newest first on a keyset cursor. Its filters
  include state, workflow name, start time and search attributes.
- The daemon opens a second, read-only connection, so a read does not wait
  for the writer. It has four readers of `harvest_executions`: `is_session`,
  `running`, `execution` and `executions`.
- The daemon has 24 tests that write damaged rows on purpose. A value can be
  in the wrong storage class, hold bytes that are not UTF-8, or hold invalid
  JSON. One damaged row must not hide the listing.
- Most daemon tests build a fixture table with six columns. A read that
  needs all the engine columns fails on that table.
- A read-only connection cannot migrate a file. It can open a file that an
  older version wrote.

### 0.2 Brainstorm — how can SQLite list and describe executions?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Add `list_executions(filter)` and `describe(exec)` on `SqliteRuntime`. | **Adopted.** It is the API the issue asks for. |
| B2 | Add a read-only `SqliteReader` with the same reads. | **Adopted.** The daemon reads without the writer. It takes no writer lock. |
| B3 | Return payloads in every listed row. | Rejected. A page can hold hundreds of megabytes. |
| B4 | Return no payloads in a list. Add `visit_executions`, which streams full rows one at a time. | **Adopted.** Memory holds one row. The daemon decodes each row as `status` does, so the two agree by construction. |
| B5 | Store `started_at`, `closed_at`, `search_attrs` and `memo` as columns. | **Adopted.** JSON1 filters the JSON columns. |
| B6 | Back-fill `started_at` from the `WorkflowStarted` event. | Rejected. A damaged event would fail the open. An old row has no start time. |
| B7 | Match the name and the state by their bytes. | **Adopted.** A value in the wrong storage class still matches, as in the daemon. |
| B8 | Read each column without a type. Report a value that is not text as unreadable. | **Adopted.** One damaged row does not fail the page. |
| B9 | Select NULL for a column that the file does not have. | **Adopted.** The reader works on an old file and on the daemon fixtures. |
| B10 | Add `start_workflow_with_options` with a memo and search attributes. | **Adopted.** Postgres sets both at start. |
| B11 | Add comparison, `IN` and `EXISTS` search-attribute predicates. | Rejected for now. Equality covers the issue. Section 2 records it. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | One damaged row fails the whole list. | Each column is read untyped. A bad value is `Stored::Unreadable`. A test inserts BLOB, integer and non-UTF-8 values. |
| R2 | A list holds every payload of a page. | `list_executions` reads no payload. `visit_executions` passes one row at a time. |
| R3 | An upsert counts as progress, so the drive loop spins. | The write is not progress, as for an update result. A test drives a body that upserts and then waits. |
| R4 | A rolled-back cycle keeps its upsert. | The write is in the cycle transaction. The rollback drops it. |
| R5 | The terminal path drops the upsert, or seals the run `FAILED`. | The terminal drain applies the patch before the terminal event. |
| R6 | A continue-as-new or a retry loses the memo or the search attributes. | The successor copies both. A test checks each path. |
| R7 | The reader writes to the file, or takes the writer lock. | It opens the file `READ_ONLY`. A test opens a reader beside a live runtime. |
| R8 | A filter value with SQL in it changes the query. | Every value is a bound parameter. |
| R9 | A search attribute that is an object breaks the filter. | Start rejects a value that is not a primitive. The core rejects it in a patch. A filter value that is not a primitive is an error. |
| R10 | A list walks the whole table. | The cursor is a `rowid` bound and the limit is capped. A plan test asserts no temporary B-tree. One state seeks on an index on `cast(state as blob)`. Other filters that match few rows still walk the table. Section 2 records it. |
| R11 | The migration fails on an old file. | It only adds nullable columns, and only when a column is absent. |
| R12 | The daemon reads `harvest_executions` again later. | A test reads the daemon source and refuses the table name. |
| R13 | The daemon shows a row that `status` refuses. | Both decode the whole document with the same function. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. The table has no time or JSON columns. The
  daemon has strict tests for damaged rows.
- **Red (feelings).** A field type with an `Unreadable` case is less easy to
  use. A list that hides a damaged row is worse.
- **Black (risks).** Section 0.3. Old rows have no start time, so a time
  filter leaves them out. The daemon test suite changes where it built its
  own table.
- **Yellow (benefits).** Apps can list, filter and describe runs without raw
  SQL. Workflows that upsert search attributes run on SQLite.
- **Green (ideas).** Typed predicates (B11). A close-time filter. An index
  on `started_at` for large files.
- **Blue (process).** Red tests, then green code, then refactor. Then a
  review from several angles.

---

## 1. Change

### 1.1 Schema

`migrate` adds four nullable columns to `harvest_executions`:

| Column | Type | Meaning |
|---|---|---|
| `started_at` | INTEGER | The `WorkflowStarted` time, in epoch microseconds. |
| `closed_at` | INTEGER | The time of the first terminal write, in epoch microseconds. |
| `search_attrs` | TEXT | A JSON object, or NULL. |
| `memo` | TEXT | Any JSON value, or NULL. |

### 1.2 Reads

`SqliteRuntime` and `SqliteReader` both have these three reads:

- `list_executions(&ExecutionFilter) -> ExecutionPage`. Newest first. No
  payloads. `next` is the cursor of the next page.
- `visit_executions(&ExecutionFilter, f)`. The same rows, with payloads. It
  calls `f` once for each row and holds one row at a time.
- `describe(exec) -> ExecutionDescription`. One row with its payloads.

`ExecutionFilter` filters by workflow name, exact execution id, exact
workflow id, workflow id prefix, states, a start-time range and
search-attribute equality. All filters combine with AND. `before` and
`limit` page the result. A filter holds at most 64 states and 64 search
attributes.

Each stored field is a `Stored<T>`: `Value`, `Null` or `Unreadable`.

### 1.3 Writes

- `UpsertSearchAttributes` merges its patch into `search_attrs` in the cycle
  transaction. It is not progress. It no longer seals the run.
- An index on `cast(state as blob)` serves a single-state filter. The daemon
  startup read then does not walk the finished sessions.
- `start_workflow_with_options` takes a workflow id, a reuse policy, a memo
  and search attributes.
- A continue-as-new and a workflow retry (issue #2029) copy the memo and
  the search attributes, as on Postgres.
- The first terminal write sets `closed_at`. A later reuse-policy seal keeps
  it.

### 1.4 The daemon

`inspect.rs` reads executions through `SqliteReader`. It keeps its own
connection for events, timers and signals. Those tables are outside this
issue.

---

## 2. Known limits

- A row written before this change has no `started_at`. A time filter does
  not match it.
- Search-attribute filters test equality only. Numbers compare as numbers,
  so `1` matches `1.0`. A filter number of magnitude 2^63 or more is refused,
  because JSON1 reads a stored integer of that size as a float.
- One state filter seeks on an index. Any other filter that matches few rows
  walks the table to fill a page, and reads past each large input.
- Search attributes have no size cap of their own, as on Postgres.
- Start-time search-attribute keys are not checked against the core key
  rules. Postgres does not check them either. A value must be a primitive.
- The memo is stored as plain JSON. This backend has no payload codec.

---

## 3. Review record

Four review agents read the change from four angles: backend correctness,
daemon parity, SQL security and performance, and API and documentation. A
Codex review ran on the pull request. The fixes:

- A reuse-policy seal moved the close time of a run that had already ended.
  The seal now keeps the first close time.
- A millisecond start time broke a start-time filter below one millisecond.
  Both times are now microseconds, and the event time is cut to match.
- A filter integer above `i64::MAX` matched its float neighbours. It is now
  refused.
- A state filter read past every large input in the file. One state now
  seeks on an index. Several states walk the `rowid`, so no page sorts.
- An unbounded filter list could reach the `SQLite` variable and depth
  limits. A filter now holds at most 64 states and 64 search attributes.
- An exact workflow id filter, as on Postgres, and builders on `StartOptions`.
- The daemon source guard now reads every directory and joins split
  strings. A `concat!` or a line continuation no longer escapes it.
- Documentation gaps and the load-bearing rationale that the daemon move
  had cut.

---

## 4. Tests and acceptance criteria

| AC | Test |
|----|------|
| List and describe cover the fields the daemon reads today. | `describe_returns_every_field_the_daemon_reads`, `list_pages_newest_first_with_a_cursor` |
| Search attributes upsert and filter. | `upserted_search_attributes_filter_the_list` |
| The daemon no longer reads `harvest_executions` directly. | `the_daemon_source_does_not_read_the_executions_table` |
