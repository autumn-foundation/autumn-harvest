-- Worker capability labels for hardware-aware activity routing (issue #382).
--
-- Adds a JSONB column to the harvest_workers table to store capability
-- labels (e.g. {"gpu": "true", "region": "eu-west-1"}) advertised by the worker.
ALTER TABLE harvest_workers
    ADD COLUMN labels JSONB NOT NULL DEFAULT '{}';
