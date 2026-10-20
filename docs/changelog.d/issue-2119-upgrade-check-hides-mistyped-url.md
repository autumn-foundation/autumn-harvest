## Fix — `upgrade_check` hides a mistyped URL (issue #2119)

The `upgrade_check` command printed a mistyped database URL, password included, to stderr. A URL without `--database-url` reached the unknown-flag error. A URL given to `--database-url-env`, `--limit` or `--format` reached the matching error too. A URL given to `--baseline-structure` or `--candidate-structure` reached the "cannot read" error. A URL given to `--baseline-build` reached the build finding in the report.

Now `plain` in `upgrade_check.rs` shows a token only when it is a plain word. Any other token shows as `<hidden>`. A bare argument is always hidden. A plain flag name or variable name stays visible. The error text changed from "unknown flag" to "unknown argument".

The "cannot read" error shows a path only when it holds letters, digits and `_-./+~ `. Any other path shows as `<hidden>`. The build finding no longer repeats the baseline build. The command still accepts every file path and build ID, such as `C:\manifests\old.json` and `sha256:abc`. The check redacts the echo, not the input. A guard on the input rejected valid values. A list of bad spellings missed new ones.

The example binary reads its arguments lossily, so a non-UTF-8 argument no longer panics.

This change adds no `WorkflowEvent` variant and no migration.

Tests, red then green: unit tests in `upgrade_check.rs` and `a_bare_url_is_not_echoed_to_stderr` in `upgrade_check_db_tests.rs`.
