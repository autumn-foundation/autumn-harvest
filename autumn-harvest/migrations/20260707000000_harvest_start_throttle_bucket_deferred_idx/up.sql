-- Companion index for the per-key-fair scanner claim query (issue #607 code
-- review fix): fire_due_on_conn's claim SQL now computes
-- ROW_NUMBER() OVER (PARTITION BY bucket_key ORDER BY deferred_at ASC) to cap
-- how many rows a single throttle key can contribute per scanner tick, which
-- must visit every pending row to rank them (unlike the prior flat
-- `ORDER BY deferred_at ASC LIMIT N`, which could stop early via an index
-- scan). This composite index lets that visitation happen via an ordered
-- index walk instead of a full-table Sort once the backlog is large.
CREATE INDEX IF NOT EXISTS idx_harvest_start_throttle_bucket_deferred
    ON harvest_start_throttle (bucket_key, deferred_at ASC);
