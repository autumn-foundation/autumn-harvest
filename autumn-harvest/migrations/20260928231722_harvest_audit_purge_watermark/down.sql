-- Revert: drop the audit purge watermark (issue #1508).
DROP TABLE IF EXISTS harvest_audit_purge_watermark;
