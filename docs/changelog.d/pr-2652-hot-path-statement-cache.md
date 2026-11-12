## Hot statements stay prepared (issue #2651, PR #2652)

An audit of the whole hot path found that Postgres planned more than it
executed. In one run of the `throughput` cell of `e2e_bench`, 172,247 of
368,753 statements ran the planner. Planning took 22.2 s and execution took
18.6 s.

Diesel sends a statement unnamed when it cannot prove that its text is
constant. That covers every `diesel::sql_query`, every DSL `UPDATE`, every
batch insert, every insert with an `Option` value and every statement with a
`sql::<T>()` fragment. An unnamed statement costs two round trips, and
Postgres parses and plans it on every call. `queue::CachedClaimQuery` (issue
#1971) fixed this for the claim statement alone.

- `cached_stmt::CachePreparedDsl::cache_prepared` marks one statement as
  safe to cache. The cache key is the SQL text and the bind types. The
  statement sites of the claim, decide and complete paths carry the mark.
  No statement text changes.
- `cached_stmt::ConstSql` is a `sql::<T>()` fragment with a type of its own,
  so a statement that holds one stays cacheable with no mark. It carries the
  `RETURNING` stamp of each notifying write.
- The four unmapped columns of `harvest_workflow_executions` made every load
  of `WorkflowExecution` run unnamed since the table split (issue #2492).
  They stay `sql::<T>()` fragments. Each load by primary key carries the
  mark. The lookup by workflow name and id does not: a generic plan cannot
  use the partial active-key index, and reads every sealed run of a
  continue-as-new chain.
- The workflow start insert is not marked. Its row has 28 options that the
  caller of a start chooses, and each option set is one text.
- A marked statement must come from a small fixed set of texts and must name
  its result columns. A cached statement that returns `*` fails after a
  migration adds a column. A debug build refuses such a statement. A batch
  insert is cached up to 8 rows, and a task insert for one row only.
- A claim sends its `BEGIN` and its planner settings in one batch
  (`queue::CLAIM_BEGIN_WITH_SETTINGS_SQL`), not two. A statement of that
  batch can fail after its `BEGIN` ran, so `begin_claim_transaction` sends a
  `ROLLBACK` on a database error.
- A decision renews the mutex leases of its run only when its history
  records a `MutexGranted` event (`mutex::history_records_a_grant`). The
  gate is exact: the one caller of the grant statement appends that event in
  the same transaction. A unit test guards the premise.
- Measured: planner runs fall from 172,247 to 11,795 per run, and planning
  time from 22.2 s to 2.9 s. The `throughput` cell moves from 47.8 to 82.0
  workflows/s on one 4-core box: the median of six alternating pairs, 1.67
  to 1.79 times in each. A pooled connection holds about 1.1 MB more. See
  `docs/performance-hot-path-statement-cache.md`, which also records five
  costs that this change leaves in place. Issues #2653 and #2654 track them.
- Tests: `cached_stmt::tests`, `cached_stmt_db_tests`,
  `mutex::pure_tests::the_grant_has_one_caller_and_it_appends_the_event` and
  `mutex_tests::a_decision_of_a_holder_renews_its_lease`.
- No new `WorkflowEvent` variant. No migration. No schema change.
