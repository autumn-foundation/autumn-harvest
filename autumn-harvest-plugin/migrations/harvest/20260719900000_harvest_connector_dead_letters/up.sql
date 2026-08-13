-- Broker connector poison-message dead letters (issue #944).
--
-- VERSION `20260719900000` -- the `900000` time component is deliberate.
-- Plugin-harvest migrations run against the SAME database as core migrations
-- and share `__diesel_schema_migrations`, which Diesel keys on the version
-- ALONE; core runs first, so a version core also claims makes this migration
-- silently "already applied" and this table is never created. Core allocates
-- `<date>000000`/`000001`/`000002`, so a `9xxxxx` slot is unreachable by its
-- convention. `plugin_and_core_migrations_never_share_a_version` in
-- `autumn-harvest/tests/integration/migration_hygiene.rs` fails CI if a future
-- version ever collides anyway.
--
-- Deliberately a PLUGIN-owned table, not the engine's `harvest_dead_letters`:
-- that table is keyed to a task (`original_task_id` is NOT NULL and
-- `task_type` carries a CHECK constraint on 'workflow'/'activity'). A poison
-- broker message has neither -- it never became a task, and by definition
-- never produced an execution. Reusing it would require relaxing a CORE schema
-- constraint, which issue #944 forbids for the trigger path.
CREATE TABLE harvest_connector_dead_letters (
    id UUID PRIMARY KEY,
    -- The binding the message arrived on (the metrics `source` label).
    binding TEXT NOT NULL,
    -- The logical stream: Kafka topic or SQS queue name.
    stream TEXT NOT NULL,
    -- Rendered broker coordinates, e.g. 'orders:3:91' or 'orders-queue:dedup-1'.
    coordinates TEXT NOT NULL,
    -- The connector's derived idempotency key. UNIQUE so dead-lettering is
    -- itself idempotent: a redelivery that dead-letters again (e.g. after a
    -- connector restart reset the in-process strike counter) must not create a
    -- second record for the same logical message.
    idempotency_key TEXT NOT NULL UNIQUE,
    workflow_name TEXT NOT NULL,
    -- Bounded classification: 'malformed' | 'mapping_rejected' | 'target_rejected'.
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    -- The raw message body, so an operator can replay it by hand after a fix.
    payload BYTEA NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Triage: "what has this binding dead-lettered recently?"
CREATE INDEX idx_harvest_connector_dead_letters_binding
    ON harvest_connector_dead_letters (binding, failed_at DESC);
