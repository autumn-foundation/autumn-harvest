-- Rollback: percent-based build ramp (issue #604)

ALTER TABLE harvest_build_policies
    DROP CONSTRAINT IF EXISTS harvest_build_policies_ramp_percent_range,
    DROP COLUMN IF EXISTS ramp_percent,
    DROP COLUMN IF EXISTS target_build_id;
