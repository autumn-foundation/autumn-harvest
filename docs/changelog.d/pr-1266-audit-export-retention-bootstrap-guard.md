## Phase — audit-export retention bootstrap-window guard (issue #1266)

`purge_old_audit_records` already refused to delete an unexported audit row
when a live cursor existed for the shard, or when the sweeping process had a
sink configured. Issue #1266 (a Codex review round 6 follow-up on PR #1261)
found both signals could be absent at once in a split web/worker deployment:
before the worker's first successful tick on a shard, the process running
retention has no sink and no cursor row to read. A retention sweep landing in
that window deleted every retention-aged audit row, including rows the
exporter had not shipped yet.

Adds a third, explicit signal: `RetentionConfig::protect_unexported_audit`
(default `false`). Set the same way on every process in a split deployment,
it closes the window from the moment export is configured rather than from
the moment the exporter's first tick succeeds — the two cheaper fixes the
issue considered and rejected (seeding the cursor row in a migration; letting
the retention process create it) both fail for the same reason: a shard's
own database cannot know whether an exporter is coming, and the retention
process does not know either.

`purge_old_audit_records` gained a third parameter,
`protect_unexported_audit: bool`, bound as its own SQL parameter — no schema
change. It is gated on `NOT EXISTS(any cursor row for the shard)` rather than
OR'd flatly into the existing `is_configured()` signal: an adversarial review
round caught that the flatter version would make the flag override a retired
cursor too, silently defeating `decommission_cursor` for as long as the flag
stayed set — exactly the deployment shape this fix targets, since the flag is
meant to be left on permanently. Scoped to "no cursor row yet", the flag does
one job (the bootstrap window) and a retired cursor stays the unconditional
override it has always been. Retiring export is still the explicit two-step
it always was: the flag protects rows before a shard has any cursor row, it
never blocks a decommission from taking effect once one exists.

New tests:
- `retention_protects_unexported_audit_when_configured_with_no_cursor_and_no_local_sink`
  reproduces the exact bootstrap window (no cursor row anywhere, no sink in
  the sweeping process) and asserts the flag alone keeps every row.
- `retention_decommission_overrides_protect_unexported_audit`
  pins the scoping fix: with the flag left `true`, decommissioning a shard
  still resumes purging its unacknowledged rows.
- `retention_still_purges_acknowledged_rows_when_protect_unexported_audit_is_true`
  confirms the flag never blocks purging of rows the exporter already
  shipped, even while a live cursor exists.

**Zero migration, zero engine impact beyond the new parameter.** No new
`WorkflowEvent` variant, no schema change, no change to any existing call
site's behavior when the new flag is left at its default `false`.
