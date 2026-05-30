-- Make scheduler ticks safe under multi-replica HA deployments (issue #350).
--
-- Adds two columns to harvest_schedules for atomic per-slot claiming.
-- Only one plugin replica can hold the claim for a given schedule slot at a
-- time; if the replica that claimed the slot crashes before advancing
-- next_run_at, the claim expires after 30 s and a peer retries it.
--
-- Both columns default to NULL (no claim held). Single-replica deployments
-- see no behaviour change: the claim UPDATE is always a no-op when
-- fire_claim_token IS NULL.

ALTER TABLE harvest_schedules
    ADD COLUMN IF NOT EXISTS fire_claim_token UUID NULL,
    ADD COLUMN IF NOT EXISTS fire_claimed_until TIMESTAMPTZ NULL;

-- Partial index on fire_claimed_until for fast expiry scanning.
-- The claim predicate checks this column on every tick for all due rows.
CREATE INDEX IF NOT EXISTS harvest_schedules_fire_claim_expiry_idx
    ON harvest_schedules (fire_claimed_until)
    WHERE fire_claimed_until IS NOT NULL;
