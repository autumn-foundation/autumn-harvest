-- A record of each override row that a reshard carry delivered.
--
-- A routing change moves a DAG to a new shard. The old shard copies the
-- override row to the new owner, then deletes its own row. These are two
-- transactions on two databases. When the delete fails, both shards hold
-- the row, and the next tick retries.
--
-- An operator can clear the override on the new owner before that retry.
-- Without a record, the retry finds no row there and copies the stale row
-- back. So the copy transaction also writes this row. It holds the
-- `updated_at` of the copied row, which is the version of that row. A
-- retry that finds the same version here writes nothing.
--
-- The source delete also removes the record on the source shard. A record
-- that outlives its row would make a later move of the same version skip
-- the copy.
--
-- `row_updated_at` is a version tag. No code compares it with a clock. The
-- table holds at most one row per DAG, so it stays small.
--
-- The table is new, so no other session holds a lock on it.
CREATE TABLE harvest_schedule_override_receipts (
    kind           TEXT        NOT NULL CHECK (kind IN ('dag')),
    name           TEXT        NOT NULL,
    row_updated_at TIMESTAMPTZ NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kind, name)
);

COMMENT ON TABLE harvest_schedule_override_receipts IS
    'The version of each override row that a reshard carry delivered to this shard.';
