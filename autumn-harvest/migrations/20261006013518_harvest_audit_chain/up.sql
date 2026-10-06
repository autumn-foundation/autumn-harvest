-- Keyed hash chain over exported audit rows (issue #1838).
--
-- The audit exporter stamps the row columns when it assigns `export_seq`,
-- but only when a chain key is set. A NULL hash means the chain does not
-- cover the row. `chain_prev` is the link of the previous `export_seq`, or 32
-- zero bytes for the first chained row. `chain_newest_before` is the newest
-- `occurred_at` of every row chained before this one. `chain_hash` is
-- HMAC-SHA256(key, domain || chain_prev || chain_newest_before || canonical
-- row).
--
-- The cursor holds a keyed checkpoint: the first chained seq, the newest
-- chained seq, its link and the newest `occurred_at`, and an HMAC over all four
-- under the chain key. The next stamp continues from `chain_head`. The
-- verifier uses the checkpoint to find stripped rows and a deleted tail. A
-- database writer without the key cannot move the checkpoint.
--
-- Audit metadata only: no `WorkflowEvent` variant, no change to
-- `harvest_events`, no replay impact.
--
-- Nullable columns with no default need no table rewrite. ADD COLUMN takes an
-- ACCESS EXCLUSIVE lock for a moment. A held lock fails the migration after
-- 5 s, so audited requests do not queue behind it.
SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_audit_log
    ADD COLUMN IF NOT EXISTS chain_prev BYTEA,
    ADD COLUMN IF NOT EXISTS chain_newest_before TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS chain_hash BYTEA;

ALTER TABLE harvest_audit_export_cursor
    ADD COLUMN IF NOT EXISTS chain_head BYTEA,
    ADD COLUMN IF NOT EXISTS chain_start_seq BIGINT,
    ADD COLUMN IF NOT EXISTS chain_head_seq BIGINT,
    ADD COLUMN IF NOT EXISTS chain_newest_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS chain_mac BYTEA;

COMMENT ON COLUMN harvest_audit_log.chain_hash IS
    'Audit-chain link of this row (issue #1838). NULL when the chain does not '
    'cover the row.';
COMMENT ON COLUMN harvest_audit_log.chain_prev IS
    'Audit-chain link of the previous export_seq (issue #1838). 32 zero bytes '
    'for the first chained row.';
COMMENT ON COLUMN harvest_audit_log.chain_newest_before IS
    'Newest occurred_at of every row chained before this one (issue #1838). '
    'The link covers it. NULL for the first chained row.';
COMMENT ON COLUMN harvest_audit_export_cursor.chain_head IS
    'Newest audit-chain link on this shard (issue #1838).';
COMMENT ON COLUMN harvest_audit_export_cursor.chain_mac IS
    'HMAC under the chain key over the shard, chain_start_seq, '
    'chain_head_seq, chain_head and chain_newest_at (issue #1838).';
