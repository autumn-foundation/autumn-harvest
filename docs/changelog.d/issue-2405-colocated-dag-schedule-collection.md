## Fix — colocated shards keep the DAG schedule row of the owner (issue #2405)

Two shards on one pool share one `harvest_schedules` table. Before the fix,
the shard that did not own a DAG deleted the row of the owner as stale. If
that shard came after the owner in shard order, the row was gone at the end
of each tick. If it came before, the owner inserted the row again on each
tick with a new `next_run_at`, so the schedule did not fire.

Now a shard does not collect a DAG schedule row when another shard on its
pool owns the DAG. The scheduler finds those shards with
`ShardedDbPool::same_physical_pool`. A shard with its own pool collects a
stale row as before. Two shards of one pool also collect a row when no shard
of that pool owns the DAG. The #2389 override move skips shards of one pool
for the same reason.

The firing pass of a shard also skips a DAG row that a pool peer owns. The
owner fires it, so the run carries the shard id of the owner. If the owner
is held, no peer fires the DAG for it.

`register_workflow_schedules_for_shard` and
`register_workflow_schedules_for_shard_with_codecs` take no pool, so they
collect as before.

No migration. No `WorkflowEvent` changes. No public API changes.

**Limit.** The scheduler finds shards of one pool through
`ShardedDbPool::same_physical_pool`: a cloned pool, or DSNs that
`from_dsns` reads as one database. Two pools built apart for one database
are not found.

**Tests.** DB tests are in `schedule_override_reshard_db_tests.rs`. Two
shards on one pool keep one row over two ticks, with the same id and
`next_run_at`. This holds with the owner first and with the owner last in
shard order, and for a DAG on every writable shard. A due DAG on a colocated
pool fires once, with the shard id of the owner. A shard with its own pool
deletes a stale row. Two shards of one pool delete a row that moved to a
shard on another pool. Unit tests cover `pool_peer_owns`, `pool_peers`,
`peer_owned_dags` and `fired_by_a_peer`.
