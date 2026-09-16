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

**A second gap, found by both an internal review pass and Codex's review of
the first commit.** `dsn::keyword_options` silently skips a bare token — one
with no `=` at all. That is safe for `safety`, which has `Config::from_str`
refusing malformed input behind it either way. It is not safe for `banner`:
`alice:hunter2@db` is one bare token and was never turned into an `Option`
for `is_connection_keyword` to see, so it reached the output unexamined —
whether it was the *only* token, or sat right next to a real option
(`alice:hunter2@db host=localhost` still leaked, because the scanner just
skipped past the bare token and kept going).

**The fix for the second gap.** `dev::dsn::keyword_options`'s internal
cursor is refactored into a shared `next_token` step, now used by two public
readers instead of one: `keyword_options` keeps its exact existing lenient
behaviour (bare tokens skipped, for `safety`), and a new `keyword_tokens`
reports every token, bare ones included, as a `KeywordToken::Bare` variant.
`banner::redact_keyword_value` reads through `keyword_tokens` and withholds
the instant it sees a `Bare` token, wherever it sits in the string.

**Test evidence.** New tests in
`autumn-harvest-plugin/tests/dev_runtime_tests.rs`:
`redaction_withholds_a_keyword_shaped_token_that_is_not_a_keyword` (the
three DSNs from the issue and its CLI counterpart),
`redaction_withholds_a_dsn_with_no_option_at_all` and
`redaction_withholds_a_bare_token_next_to_a_real_option` (the second gap, in
both of its shapes),
`redaction_still_accepts_every_recognized_keyword` (now exercising every
keyword `is_connection_keyword` allows, not a handful), and
`redaction_of_a_keyword_dsn_with_no_password_round_trips_byte_for_byte`.
`dev::dsn`'s own test module gained `every_libpq_keyword_is_recognized` and
`a_keyword_shaped_token_that_is_not_a_keyword_is_refused`, testing
`is_connection_keyword` directly rather than only through the banner.
`cargo test -p autumn-harvest-plugin --features dev-runtime --lib` and
`--test dev_runtime_tests` are both green; so is `cargo clippy -p
autumn-harvest-plugin --lib --features dev-runtime -- -D warnings`.

**Zero engine impact:** no new `WorkflowEvent` variant, no migration, no
schema change. `is_connection_keyword` is a private `dev::dsn` function; no
public signature moved.
