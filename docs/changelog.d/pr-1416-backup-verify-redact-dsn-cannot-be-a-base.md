## Phase — `redact_dsn` withholds a hostless DSN (issue #1416)

🪝 Snag exploratory QA finding: `backup_verify::redact_dsn`'s own doc comment
promises that a malformed-but-secret-bearing DSN can never reach a report or
a log line, falling back to `"<unparseable dsn>"` on anything
`url::Url::parse` rejects outright.

A DSN missing one or both `/` after the scheme — one or two characters
short of the documented `postgres://` form — still parses `Ok`. Dropping
both (`postgres:user:hunter2@host/db`) yields a `url`-crate
"cannot-be-a-base" URL with no authority at all. Dropping only the second
slash (`postgres:/user:hunter2@host/db`, the more likely one-character
typo, flagged by Codex's review of this PR) yields an ordinary base URL — an
absolute path, still with no host. Neither URL has a `host()`, so
`password()`/`set_password()` are no-ops on both, and the credential
sitting in the tail was echoed back verbatim by `url.to_string()`,
contradicting the doc comment's own guarantee.

`dr_connect`, `dr_connect_read_only`, and `scratch_guard` in
`autumn-harvest-cli` each interpolate `redact_dsn(dsn)` straight into a
`CliError::InvalidInput` message on a connection failure or a scratch-guard
refusal. An operator who drops a `/` from a `--dsn`/`--live-dsn` (a
one- or two-character typo on the documented form) got the real password
echoed into that error message.

Fix: check `host().is_none()` rather than `cannot_be_a_base()`. No valid
Postgres DSN omits its host, so this never withholds an identity a caller
could otherwise have used — it only closes both credential-leak paths. No
new `WorkflowEvent` variant, no migration — a pure string-handling fix in
`autumn-harvest/src/backup_verify.rs`. Regression test:
`backup_verify::tests::redact_dsn_withholds_a_hostless_dsn` — RED pre-fix
(`hunter2` echoed verbatim for both the one- and two-slash-dropped DSN
shapes), GREEN post-fix.
