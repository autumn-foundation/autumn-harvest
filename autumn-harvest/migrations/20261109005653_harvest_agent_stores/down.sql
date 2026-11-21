-- Reverts 20261109005653_harvest_agent_stores.
SET LOCAL lock_timeout = '5s';

DROP TABLE IF EXISTS harvest_agent_transcripts;
DROP TABLE IF EXISTS harvest_agent_store_docs;
