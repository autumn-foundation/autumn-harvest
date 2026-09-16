## Phase 5.x — dev runtime: withhold a DSN a keyword scanner cannot vouch for (issue #1322)

`banner::redact_keyword_value` accepted any `key=value` token as a libpq
keyword. A mistyped URL that lost its `://` — `postgres=//alice:hunter2@db`
— scans as the "keyword" `postgres`. It has no `password=` option, so the
scanner found nothing to blank out and printed the whole string, credential
included.

`autumn-harvest-cli` had already solved this for its own migration-target
labels: `is_connection_keyword` (`autumn-harvest-cli/src/lib.rs`) rejects a
token that is keyword-*shaped* but not a real libpq keyword. The dev
runtime's `dev::dsn` module — lifted from the same code in #1320 — inherited
only the lenient half of that scanner.

**The fix.** `dev::dsn::is_connection_keyword` adds the same allow-list, kept
in step with the CLI's copy. `banner::redact_keyword_value` now withholds
the whole DSN — a `<redacted dsn>` placeholder, not a splice — the moment
its scanner meets a token this list does not recognize, rather than echoing
a string it never actually examined for a password.

This is the second item of issue #1322; the first (a `?` before the URI
userinfo's `@` moving the real query string) was already fixed in #1320.

**Test evidence.** Two new tests in
`autumn-harvest-plugin/tests/dev_runtime_tests.rs`:
`redaction_withholds_a_keyword_shaped_token_that_is_not_a_keyword` (the
three DSNs from the issue and its CLI counterpart) and
`redaction_still_accepts_every_recognized_keyword`, which guards against the
withholding swallowing a legitimate keyword/value DSN. `cargo test -p
autumn-harvest-plugin --features dev-runtime --lib` and `--test
dev_runtime_tests` are both green; so is `cargo clippy -p
autumn-harvest-plugin --lib --features dev-runtime -- -D warnings`.

**Zero engine impact:** no new `WorkflowEvent` variant, no migration, no
schema change. `is_connection_keyword` is a private `dev::dsn` function; no
public signature moved.
