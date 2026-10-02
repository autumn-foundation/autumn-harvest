-- Retention watermark for audit-export redrive (issue #1508).
--
-- A `before` redrive derives its window from the rows that still exist. A
-- purged row leaves no trace, so the redrive cannot see a prefix that
-- retention already removed. This table keeps that trace.
--
-- One row per database, shared by every shard in it. The `before` resolver
-- is also database-wide, so the two agree. `purge_old_audit_records` upserts
-- the row in the same statement that deletes, so a purge cannot commit
-- without its watermark. A purge before this migration left no trace.
-- Only sequenced rows count: a `before` redrive never selects an unsequenced
-- row, so losing one cannot shorten its window.
CREATE TABLE IF NOT EXISTS harvest_audit_purge_watermark (
    singleton              BOOLEAN     PRIMARY KEY DEFAULT TRUE,
    max_purged_occurred_at TIMESTAMPTZ NOT NULL,
    purged_records         BIGINT      NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT harvest_audit_purge_watermark_singleton CHECK (singleton),
    CONSTRAINT harvest_audit_purge_watermark_non_negative CHECK (purged_records >= 0)
);

COMMENT ON TABLE harvest_audit_purge_watermark IS
    'Latest occurred_at of any sequenced audit record retention has purged '
    '(issue #1508), shared by every shard in the database. Lets a before-redrive detect a purged record. Never deleted.';
