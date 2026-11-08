-- Issue #2602: each receipt row becomes the floor of its DAG on its shard.
--
-- The floor is the newest override version that the shard retired. A
-- replace raises it to the old row. A clear raises it to a new version. A
-- carry skips a version at or below the floor of its destination. A carry
-- also moves the floor to the owner. So a cleared version never comes back,
-- also after a routing change.
--
-- Issue #2413 wrote a receipt for each delivered copy. A receipt at or
-- above the live row of its shard is such a receipt. Its row is still
-- there, or the copy yielded to an older row. As a floor, it would make a
-- later carry skip a live version, so this statement deletes it. Each
-- other receipt is a version that its shard cleared or replaced.
--
-- No hot table is touched, and each table here has one row per DAG.
SET LOCAL lock_timeout = '5s';

DELETE FROM harvest_schedule_override_receipts AS r
USING harvest_schedule_overrides AS o
WHERE o.kind = r.kind
  AND o.name = r.name
  AND o.updated_at <= r.row_updated_at;

COMMENT ON TABLE harvest_schedule_override_receipts IS
    'The newest override version that this shard retired, per DAG (issue #2602).';
COMMENT ON COLUMN harvest_schedule_override_receipts.row_updated_at IS
    'The floor: a carry skips a version at or below it.';
COMMENT ON COLUMN harvest_schedule_override_receipts.received_at IS
    'When the floor last rose.';
