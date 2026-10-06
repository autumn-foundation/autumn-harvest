## Backend hardening — SQLite single-writer lock, typed unsupported failures, Redis TLS (issue #1834)

**SQLite: the single-writer contract is enforced.** `SqliteRuntime::open`
takes an exclusive OS lock on `<database>.lock` (`flock` / `LockFileEx`, via
`fs4`). A second runtime on the same file fails fast with the new
`SqliteError::DatabaseLocked`. This applies to another process and to a second
runtime in the same process. The open fails before any pragma, schema step or
orphan reclaim, so it cannot steal a `RUNNING` task. The kernel releases the
lock when its process exits, so a crash leaves no stale lock. The lock path
comes from the canonical database path, so a symlink maps to the same lock. A
read-only inspector connection still works. An in-memory database takes no
lock. A new `SqliteError::Io` reports a lock file that cannot be opened.

**SQLite: an unsupported feature ends the run `FAILED`.** Before, the run stayed
`RUNNING` and failed again on every drive. Now the drive rolls back the cycle
and seals the run in a new transaction. The `WorkflowFailed` event carries
`error_type = "UnsupportedFeature"` (`UNSUPPORTED_FEATURE_ERROR_TYPE`),
`details = {"feature": …}` and `non_retryable = true`. The sealing drive still
returns `SqliteError::Unsupported`. A later drive returns `RunState::Failed` and
does not run the handler again. Other errors (unregistered workflow or
activity, replay divergence, contained panic under budget, failed task) still
leave the run `RUNNING`. Two existing tests changed their expectation from
`RUNNING` to `FAILED`.

**Redis: `rediss://` works.** The workspace `redis` dependency moves from 0.27
to 0.32 and enables `tokio-rustls-comp`. 0.29 and later dropped the
unmaintained `rustls-pemfile`, the crate that `cargo deny` refused. The bump
adds no new crate to the normal build. A `rediss://` URL verifies the server
against the platform store, which honours `SSL_CERT_FILE` and `SSL_CERT_DIR`.
`RedisDispatch::connect_with_tls` and `RedisTaskQueue::connect_with_tls` take a
`RedisTlsOptions` with a private CA and a client certificate for mutual TLS.
They check the PEM before any network I/O, and they reject a plain `redis://`
URL. A one-shot probe reports a handshake failure by name instead of a
timeout. There is no option to skip verification. The
`RedisAdapterError::TlsUnavailable` variant is removed. Redis Cluster stays a follow-up.

No new `WorkflowEvent` variant and no migration.

**Tests.** `autumn-harvest-sqlite/tests/integration/single_writer_lock.rs` runs
the test binary again as a child process: the second process fails, a killed
holder leaves no stale lock, a symlink shares the lock, and an inspector still
reads. `unsupported_terminal.rs` covers a child workflow, `continue_as_new`, a
worker session, a terminal-cycle upsert, the fleet drivers and a reopen.
`autumn-harvest-redis/tests/tls_redis.rs` starts a TLS-only `redis:7.4-alpine`
with certificates minted by `rcgen`: a private CA, an untrusted CA, mutual TLS,
the standalone queue, and `SSL_CERT_FILE` in a child process. The suite is in
`.github/ci/integration-suites.txt`, and CI pre-pulls the image.
