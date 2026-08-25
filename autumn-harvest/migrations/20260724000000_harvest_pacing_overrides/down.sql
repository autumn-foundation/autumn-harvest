-- Revert: drop the TTL'd rate-limit/throttle override columns (issue #945).
ALTER TABLE harvest_rate_limit_buckets
    DROP COLUMN IF EXISTS override_refill_rate,
    DROP COLUMN IF EXISTS override_burst,
    DROP COLUMN IF EXISTS override_expires_at;
