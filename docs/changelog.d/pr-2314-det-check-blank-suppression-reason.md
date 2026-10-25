## Fix — det-check rejects a blank suppression reason (issue #2314)

`harvest det-check` accepted `// harvest-suppress: DET003 "   "`. The parser tested `reason.is_empty()`. It now tests `reason.trim().is_empty()`. A blank reason no longer suppresses a finding or appears in `--list-suppressions`. This matches `GuardrailSuppression::new`. No migration. No `WorkflowEvent` change.

Tests: `parse_suppression_comment_rejects_a_whitespace_only_reason` and `parse_suppression_comment_keeps_a_padded_reason` in `det_check.rs`. `suppression_rejects_a_whitespace_only_reason` in `tests/integration/det_check_tests.rs` covers the same-line and previous-line forms. `blank_reason_suppression_is_not_listed_and_still_gates` in `det_check_cli.rs` covers the CLI gate and the suppression listing.
