-- Issue #1676. Retention prunes old rows of this table by `fired_at`.
-- No existing index leads with that column, so each tick would scan the
-- whole table. The table grew without bound before this prune existed.
CREATE INDEX idx_harvest_completion_trigger_fires_fired_at
    ON harvest_completion_trigger_fires (fired_at);
