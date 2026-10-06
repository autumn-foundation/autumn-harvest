-- Scanner leases (issue #1795).
--
-- One row per shard and scanner kind. The row names the replica that runs
-- that scanner now. Other replicas stand by. A holder renews its row on each
-- tick. When `lease_until` passes, any replica can take the row.
--
-- The lease cuts duplicate scan load. It is not a safety fence. Every scanner
-- that uses it must stay correct with more than one holder at a time.
--
-- `epoch` counts changes of holder. A renewal by the same holder keeps it.
CREATE TABLE IF NOT EXISTS harvest_scanner_leases (
    shard_id    INTEGER     NOT NULL,
    scanner     TEXT        NOT NULL,
    holder      TEXT        NOT NULL,
    epoch       BIGINT      NOT NULL DEFAULT 1,
    lease_until TIMESTAMPTZ NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, scanner),
    CONSTRAINT harvest_scanner_leases_epoch_positive CHECK (epoch > 0)
);

COMMENT ON TABLE harvest_scanner_leases IS
    'Which replica runs each per-shard background scanner (issue #1795). '
    'A load control, not a fence: scanners stay correct with two holders.';
