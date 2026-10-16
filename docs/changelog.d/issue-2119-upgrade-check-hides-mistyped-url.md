## Fix — `upgrade_check` hides a mistyped URL (issue #2119)

The `upgrade_check` command printed a mistyped database URL, password included, to stderr. A URL without `--database-url` reached the unknown-flag error. A URL given to `--database-url-env`, `--limit` or `--format` reached the matching error too.

Now `plain` in `upgrade_check.rs` shows a token only when it is a plain word. Any other token shows as `<hidden>`. A bare argument is always hidden. A plain flag name or variable name stays visible. `--baseline-structure`, `--candidate-structure`, `--baseline-build` and `--workflow-name` refuse a value that holds `://`. Later errors and reports echo those values. The example binary reads its arguments lossily, so a non-UTF-8 argument no longer panics.

The error text changed from "unknown flag" to "unknown argument".

This change adds no `WorkflowEvent` variant and no migration.

Tests, red then green: four unit tests in `upgrade_check.rs` and `a_bare_url_is_not_echoed_to_stderr` in `upgrade_check_db_tests.rs`.
