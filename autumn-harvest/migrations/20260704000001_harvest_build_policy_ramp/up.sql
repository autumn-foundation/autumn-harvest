-- Percent-based build ramp for safe deploys (issue #604)
--
-- Extends the per-queue build policy with an optional ramp target build and
-- percentage. Additive only: existing single-build policies (target/percent
-- both NULL) keep routing 100% of new starts to `build_id`, unchanged.

ALTER TABLE harvest_build_policies
    ADD COLUMN target_build_id TEXT,
    ADD COLUMN ramp_percent INTEGER,
    ADD CONSTRAINT harvest_build_policies_ramp_percent_range
        CHECK (ramp_percent IS NULL OR (ramp_percent >= 0 AND ramp_percent <= 100));
