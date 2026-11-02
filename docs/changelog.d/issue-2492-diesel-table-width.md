## Phase X.Y — Every diesel table fits the default 32-column tier (issue #2492)

`harvest_workflow_executions` had 65 columns in its `table!`, so the workspace
enabled diesel's `128-column-tables` feature. Cargo unifies features, so every
embedder built diesel at that tier too. Compiling diesel alone at that tier
passes 9.9 GB RSS. The workspace now builds diesel at its default tier, 32
columns. That tier compiles diesel in about a third of the time and memory of
`64-column-tables`.

- **Split tables.** `harvest_workflow_executions` (61 columns),
  `harvest_task_queue` (48) and `harvest_schedules` (38) each have a core
  `table!` of 32 columns and an extension `table!`, `<table>_ext`, with
  `#[sql_name]` set to the same table. The physical tables do not change, so
  there is no migration.
- **`table_split` bridges the two declarations.** An extension column selects,
  filters, orders and joins in a query on the core table. `Ext(..)` adds
  extension values to an insert into the core table, `ON CONFLICT` and batch
  inserts included. `SetExt(..)` assigns an extension column in an update of
  the core table. Each statement renders the same SQL as one wide declaration.
  The insert bridge needs diesel's
  `i-implement-a-third-party-backend-and-opt-into-breaking-changes` feature,
  which the workspace now names.
- **Flat models.** `WorkflowExecution`, `TaskQueueItem`, `HarvestSchedule`
  and their `New*` structs keep their fields and their JSON. `split_queryable!`
  and `split_insertable!` implement `Selectable`, `Queryable` and `Insertable`
  over both declarations. An insert borrows its row, so pass `&row`, or a
  slice for a batch.
- **Rebalance markers.** Four shard-rebalance columns of the executions table
  read through typed SQL fragments in
  `schema::harvest_workflow_executions_unmapped`. `WorkflowExecution` embeds
  them as `rebalance: RebalanceMarkers` with `#[serde(flatten)]`.
- **Guard.** `docs/audits/diesel-table-width.py` runs in the `lint` job. It
  fails on a `table!` over 32 columns, and on a manifest outside `docs/` that
  enables `64-column-tables` or `128-column-tables`.

Code that names a moved column changes its path from `<table>::<column>` to
`<table>_ext::<column>`. An embedder that queries these tables through
`autumn_harvest::schema` changes the same way.

No new `WorkflowEvent` variant. No migration. `harvest_events` is untouched.
`table_split_db_tests` and `rebalance_markers_db_tests` round-trip every moved
field against Postgres. The unit tests in `table_split` pin the SQL of each
bridged statement.
