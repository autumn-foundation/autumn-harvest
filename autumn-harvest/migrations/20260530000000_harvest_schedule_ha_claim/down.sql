DROP INDEX IF EXISTS harvest_schedules_fire_claim_expiry_idx;

ALTER TABLE harvest_schedules
    DROP COLUMN IF EXISTS fire_claim_token,
    DROP COLUMN IF EXISTS fire_claimed_until;
