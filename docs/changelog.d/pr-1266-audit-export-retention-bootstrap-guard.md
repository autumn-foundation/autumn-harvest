## Fix — audit-export retention bootstrap-window guard (issue #1266)

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
`protect_unexported_audit: bool`, OR'd into its existing guard in Rust before
the delete statement runs — no SQL or schema change. Retiring export is still
the explicit two-step it always was: the flag protects rows, it does not
retire a cursor.

New test: `retention_protects_unexported_audit_when_configured_with_no_cursor_and_no_local_sink`
reproduces the exact bootstrap window (no cursor row anywhere, no sink in the
sweeping process) and asserts the flag alone keeps every row.

**Zero migration, zero engine impact beyond the new parameter.** No new
`WorkflowEvent` variant, no schema change, no change to any existing call
site's behavior when the new flag is left at its default `false`.
