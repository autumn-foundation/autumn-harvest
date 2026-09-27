-- Drop the durable codec key state table (issue #1244).
--
-- Safe to drop: it is a coordination cache the scanner tick rebuilds from
-- operator action (activate_codec_key / retire_codec_key calls), not the
-- payload data itself. Reverting loses in-flight retirement staleness-window
-- progress; a pending retirement must restart its wait after re-applying.
DROP TABLE IF EXISTS harvest_codec_key_state;
