## Phase — `harvest migrate`'s target label reads DSN separators as libpq does (issue #1321)

`autumn-harvest-cli::scan_keyword_dsn` split a libpq keyword/value DSN's
options with `u8::is_ascii_whitespace`. `tokio_postgres` — the client that
actually connects with these DSNs — splits on `char::is_whitespace`, the
Unicode `White_Space` property (`tokio-postgres`'s `config::Parser::skip_ws`).
The two disagree on U+0085, U+00A0, U+1680, U+2000–200A, U+2028, U+2029,
U+202F, U+3000, and on U+000B — ASCII, but excluded from
`is_ascii_whitespace`.

So a DSN separated by one of those read as a single option to the scanner
and as several to the client. The scanner's unquoted-value reader kept
consuming past the separator, so `password=hunter2` from the *next* option
became part of the *previous* option's value; `redact_keyword_dsn` found no
`password` key and returned the DSN whole. `migrate_target_label` exists so
`harvest migrate`'s per-target report can name a database without its
credential — this was the one path where that redaction is load-bearing, and
where the credential leaked.

Same defect, same fix shape, as the one issue #1286 (PR #1320) closed in
`autumn-harvest-plugin/src/dev/dsn.rs`'s `skip_whitespace`.

**Fix.** All five whitespace tests inside `scan_keyword_dsn` (the
between-option skip, the key-scan boundary, the spacing around `=`, and the
unquoted-value terminator) now read one character at a time via a small
`peek_char` helper and compare with `char::is_whitespace`, advancing by
`len_utf8()` rather than one byte — matching the escape handling the
function already used for multi-byte characters.

**Test evidence.** `keyword_separators_use_unicode_whitespace_like_the_client_does`
sweeps U+0009, U+000B, U+0085, U+00A0, U+1680, U+2003, U+202F, and U+3000 as
the option separator and asserts the credential never appears in the label;
`a_no_break_space_separated_dsn_redacts_to_the_exact_expected_label` pins the
exact output for one separator, so a span slip that leaks the edge of a
credential is caught too. The full pre-existing `migrate_target_label` suite
stays green.

**Zero engine impact:** no `WorkflowEvent` change, no migration, confined to
`autumn-harvest-cli`'s redaction helper.
