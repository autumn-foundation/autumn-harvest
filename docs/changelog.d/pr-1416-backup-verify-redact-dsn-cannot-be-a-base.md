## Phase — `redact_dsn` withholds a cannot-be-a-base DSN (issue #1416)

🪝 Snag exploratory QA finding: `backup_verify::redact_dsn`'s own doc comment
promises that a malformed-but-secret-bearing DSN can never reach a report or
a log line, falling back to `"<unparseable dsn>"` on anything
`url::Url::parse` rejects outright.

A DSN missing `//` after the scheme — one character short of the documented
`postgres://` form, e.g. `postgres:user:hunter2@host/db` — still parses
`Ok`, as a `url`-crate "cannot-be-a-base" URL with no authority component at
all. `password()`/`set_password()` are no-ops on such a URL, so the
credential sitting in the opaque tail was echoed back verbatim by
`url.to_string()`, contradicting the doc comment's own guarantee.

`dr_connect`, `dr_connect_read_only`, and `scratch_guard` in
`autumn-harvest-cli` each interpolate `redact_dsn(dsn)` straight into a
`CliError::InvalidInput` message on a connection failure or a scratch-guard
refusal. An operator who drops `//` from a `--dsn`/`--live-dsn` (a
one-character typo on the documented form) got the real password echoed
into that error message.

Fix: treat a `cannot_be_a_base` parse the same as an outright-unparseable
one. No valid Postgres DSN is `cannot_be_a_base`, so this never withholds an
identity a caller could otherwise have used — it only closes the
credential-leak path. No new `WorkflowEvent` variant, no migration — a pure
string-handling fix in `autumn-harvest/src/backup_verify.rs`. Regression
test: `backup_verify::tests::redact_dsn_withholds_a_cannot_be_a_base_dsn` —
RED pre-fix (`hunter2` echoed verbatim for several `//`-dropped DSN shapes),
GREEN post-fix.
