SET LOCAL lock_timeout = '5s';

ALTER TABLE harvest_audit_export_cursor
    DROP COLUMN IF EXISTS chain_mac,
    DROP COLUMN IF EXISTS chain_head_occurred_at,
    DROP COLUMN IF EXISTS chain_head_seq,
    DROP COLUMN IF EXISTS chain_start_seq,
    DROP COLUMN IF EXISTS chain_head;

ALTER TABLE harvest_audit_log
    DROP COLUMN IF EXISTS chain_hash,
    DROP COLUMN IF EXISTS chain_prev;
