-- Issue #1227, Finding 4 (follow-up to #946/#1221's Codex round-6 review).
--
-- The completion-trigger outbox claim query (`enforce_completion_triggers_outbox`)
-- has no `ORDER BY` and no per-row backoff tracking. A row whose target is
-- blocked by a durably exhausted per-tenant quota (e.g. `max_dead_letters`,
-- which only clears via manual operator action) was left completely untouched
-- on a quota-blocked relay attempt -- neither deleted nor timestamped -- so it
-- can dominate every unordered `LIMIT 50` claim batch on every scanner tick,
-- starving any OTHER, unrelated cross-shard completion-trigger relay that
-- happens to sort after it in the same batch.
--
-- `next_attempt_at` gives a quota-blocked row a backoff: the relay stamps it
-- with `now() + backoff` instead of leaving the row untouched, and the claim
-- query excludes a row whose backoff has not yet elapsed. NULL (the default,
-- and every pre-existing row) means "never blocked; eligible immediately" --
-- so this is purely additive and does not change behavior for a row that has
-- never hit a quota block.
ALTER TABLE harvest_completion_trigger_outbox
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

-- Supports the claim query's `WHERE target_shard = ANY(...) AND
-- (next_attempt_at IS NULL OR next_attempt_at <= now()) ORDER BY created_at`.
CREATE INDEX idx_harvest_completion_trigger_outbox_shard_next_attempt
    ON harvest_completion_trigger_outbox (target_shard, next_attempt_at, created_at);
