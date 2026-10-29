## Scheduler — Schedule overrides follow their DAG to a new shard (issue #2389, epic #2384)

A DAG schedule override lives on the shard that owns the DAG. A shard routing
change moved the schedule row to its new owner, but the override stayed
behind. The new owner then reconciled the row to the code value.

Now the old shard's scheduler pass finds each override that it holds for
another shard. It copies the row to the new owner, then deletes its own copy.
The copy commits first, so a failure at either step leaves the override on at
least one shard, and the next tick retries. An override that the new owner
already holds wins. Two shards on one pool share one table, so nothing moves
between them. No migration and no configuration change.
