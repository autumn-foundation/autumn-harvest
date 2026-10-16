## Phase 3.x — `upgrade_check` hides a mistyped URL (issue #2119)

The `upgrade_check` command printed a mistyped database URL, password included, to stderr. A URL without `--database-url` reached the unknown-flag error. A URL given to `--database-url-env`, `--limit` or `--format` reached those errors too.

Now `plain` in `upgrade_check.rs` shows a token only when it is a plain word. Any other token shows as `<hidden>`. A bare argument is always hidden. The error text changed from "unknown flag" to "unknown argument".

No `WorkflowEvent` variant. No migration.

Tests, red then green: four unit tests in `upgrade_check.rs` and `a_bare_url_is_not_echoed_to_stderr` in `upgrade_check_db_tests.rs`.
