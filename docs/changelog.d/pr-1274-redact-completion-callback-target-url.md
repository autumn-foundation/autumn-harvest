## Phase — redact completion-callback target URLs in startup and runtime logs (issue #1274)

`HarvestBuilderError::CallbackTargetRejected`'s `Display` interpolated the
full completion-callback target URL. A builder error surfaces through
`try_build()` straight into startup logs. A completion-callback target
often carries a bearer token or signing key in its path or query.
`AuditSinkRejected` already closed this exact leak for the audit-export
sink URL (issue #953). The two variants sat next to each other with
opposite behavior and no stated reason.

Fix: `CompletionCallbackBuilderConfig::validate_default_targets` now routes
the rejected URL through the existing `audit_export::redact_webhook_url`
helper, mirroring `AuditExportBuilderConfig::validate_webhook_url` exactly.
The redacted form (`scheme://host/<redacted>`) is what reaches
`CallbackTargetRejected.url`, and therefore `Display`. Every
`SsrfRejection` variant discriminates on an origin property, so the fix
loses no diagnostic information.

Two more call sites had the same leak, both in the completion-callback
runtime re-validation path the issue named as worth checking:
`enqueue_completion_deliveries`'s skip warning, and the scanner's
dead-letter warning. Both log the raw target URL when a tightened
operator SSRF policy rejects a previously-registered target. Both now log
the redacted form too.

No new `WorkflowEvent` variant, no migration. A pure redaction fix across
`builder.rs` and `completion_callback.rs`.

Regression tests:

- Unit (`cargo test -p autumn-harvest --lib`):
  `validate_default_targets_redacts_a_credential_bearing_target`,
  `builder_redacts_a_credential_bearing_completion_callback_target_in_startup_error`.
- Integration, against a real Postgres container
  (`autumn-harvest/tests/integration/completion_callback_tests.rs`):
  `enqueue_redacts_a_credential_bearing_target_that_fails_live_ssrf_revalidation`,
  `scanner_redacts_a_credential_bearing_target_url_when_dead_lettering`.
  Each asserts the rejected host still appears in the log, and a
  credential-bearing query string does not.
