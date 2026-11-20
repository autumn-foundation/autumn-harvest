-- Shared gauge samples.
--
-- One row per shard and sampler key. The elected gauge sampler of a shard
-- writes its read of that shard here. The other workers use the row and do
-- not read the shard. Every worker still emits each gauge.
--
-- `sampler` is the lease key of `harvest_scanner_leases`. It names the
-- sampler and a digest of its inputs. `sample` is the read as JSON.
-- `sampled_at` is the database clock at the write. A standby uses a row only
-- while it is young. Each write deletes the rows of its shard that no leader
-- wrote for one day.
--
-- A cache, not a record. A lost row only makes a worker read the shard.
CREATE TABLE IF NOT EXISTS harvest_gauge_samples (
    shard_id   INTEGER     NOT NULL,
    sampler    TEXT        NOT NULL,
    sample     JSONB       NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shard_id, sampler)
);

COMMENT ON TABLE harvest_gauge_samples IS
    'The elected gauge sampler read of each shard. A cache: a lost row only '
    'makes a worker read the shard itself.';
