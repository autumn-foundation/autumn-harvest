## Phase — `redact_dsn` withholds a hostless DSN (issue #1416)

`redact_dsn`'s doc comment promises that a malformed-but-secret-bearing
DSN never reaches a report or a log line — it falls back to
`"<unparseable dsn>"` on anything `url::Url::parse` rejects outright. A
DSN missing one or both `/` after the scheme still parses `Ok`, with no
host, so `password()`/`set_password()` do not touch it and a credential
in the tail reached `url.to_string()` unchanged.

`dr_connect`, `dr_connect_read_only`, and `scratch_guard` in
`autumn-harvest-cli` each put `redact_dsn(dsn)` into a
`CliError::InvalidInput` message on a connection failure or a
scratch-guard refusal. A `/` typo on `--dsn`/`--live-dsn` put the real
password into that message.

Fix: check `host().is_none()` rather than `cannot_be_a_base()`. No valid
Postgres DSN omits its host, so this closes the leak without withholding
an identity a caller could otherwise use. A hostless DSN with no
credential (`postgresql:///harvest`, the libpq Unix-socket form) is also
withheld now, on purpose — a scan for `@` in the path cannot tell the two
cases apart, since percent-encoding hides the `@` while the credential
stays intact. No new `WorkflowEvent` variant, no migration — a pure
string-handling fix in `autumn-harvest/src/backup_verify.rs`. Regression
tests: `redact_dsn_withholds_a_hostless_dsn`,
`redact_dsn_withholds_a_hostless_unix_socket_dsn_too`,
`redact_dsn_withholds_a_credential_hidden_by_percent_encoding`.
