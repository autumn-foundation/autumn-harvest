-- The deleted delivery receipts do not come back. Restore the comments.
COMMENT ON TABLE harvest_schedule_override_receipts IS
    'The version of each override row that a reshard carry delivered to this shard.';
COMMENT ON COLUMN harvest_schedule_override_receipts.row_updated_at IS NULL;
COMMENT ON COLUMN harvest_schedule_override_receipts.received_at IS NULL;
