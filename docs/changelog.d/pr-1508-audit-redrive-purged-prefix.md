## Phase — a `before` redrive reports a prefix retention already purged (issue #1508)

`POST /admin/audit-export/redrive` with `before` derives its window from the
records that still exist. Retention could purge the earliest records at or
after the instant. The redrive then resolved to the lowest survivor and
reported `already_purged_records: 0`. When every matching record was gone,
it returned a plain `400` with no mention of the loss. No race is needed: a
purge, then a later `before` redrive, is enough.

Fix: a persisted purge watermark.

- Migration `20260928231722_harvest_audit_purge_watermark` adds
  `harvest_audit_purge_watermark`, one row per database, holding the latest
  `occurred_at` of any sequenced record retention purged.
- `purge_old_audit_records` now deletes and upserts the watermark in one
  statement (a data-modifying CTE). A purge cannot commit without its
  trace. The upsert uses `GREATEST`, so the value never moves backwards.
  Unsequenced rows are skipped: a `before` redrive never selects one.
- `audit_export::redrive_window_truncated(conn, request, outcome)` is `true`
  for a `before` request when the watermark is at or after the instant. It is
  `false` for `to_seq` (already exact) and for `NotConfigured`.
- The redrive response gains `window_truncated`. The `audit_export.redrive`
  audit detail and the `400` refusal name the loss when the flag is set.

The flag shows that a record at or after the instant is gone. It does not
count them: no surviving row holds that number. The table is database-wide,
like the `before` resolver, so shards that share a database share it and the
flag can be `true` for a shard that lost nothing. A purge that commits after
the check is not seen. Purges before the upgrade left no trace. The redrive still runs when the flag is set, since the
surviving records are real and `to_seq` remains available.

The watermark is a new table, not a cursor column. Retention takes no
cursor-row lock, and adding one would contend with the exporter and the
redrive.

No `WorkflowEvent` variant. No `harvest_events` writer. The watermark
table is not an audit-log row and does not weaken any append-only rule.

Tests (`audit_export_tests.rs`, real Postgres):
- `before_redrive_flags_a_prefix_retention_already_purged`
- `before_redrive_flags_a_refusal_when_every_matching_record_was_purged`
- `before_redrive_is_not_flagged_when_nothing_was_purged`
- `before_redrive_is_not_flagged_when_the_instant_is_after_every_purged_record`
- `seq_redrive_is_never_flagged`
- `the_purge_watermark_never_moves_backwards`
- `purging_unsequenced_records_leaves_no_watermark`

Also: `docs/api-contract.json`, `management_api_response_fields()`, both
`openapi.json` copies, `docs/audit-export.md`, and the upgrade guide table.
