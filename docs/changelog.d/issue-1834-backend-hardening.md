## Backend hardening — SQLite single-writer lock, typed unsupported failures, Redis TLS (issue #1834)

**SQLite: the single-writer contract is enforced.** `SqliteRuntime::open`
takes an exclusive OS lock on `<database>.lock` (`flock` / `LockFileEx`, via
`fs4`). A second runtime on the same file fails fast with the new
`SqliteError::DatabaseLocked`. This applies to another process and to a second
runtime in the same process. The open fails before any pragma, schema step or
orphan reclaim, so it cannot steal a `RUNNING` task. The kernel releases the
lock when its process exits, so a crash leaves no stale lock. The lock path
comes from the canonical database path, so a symlink maps to the same lock. A
hard link or a bind mount gives a second lock. On Unix the lock file takes the
database file's read and write bits. A read-only inspector connection still
works. A non-UTF-8 path still takes the lock. An in-memory database takes no
lock. The open retries the lock for about 100 ms, so a lock that a forking
thread or a lagging release holds for a moment does not fail it. A new
`SqliteError::Io` reports a lock file that cannot be opened.

**SQLite: an unsupported feature ends the run `FAILED`.** Before, the run stayed
`RUNNING` and failed again on every drive. Now the drive rolls back the cycle
and seals the run in a new transaction. The `WorkflowFailed` event carries
`error_type = "UnsupportedFeature"` (`UNSUPPORTED_FEATURE_ERROR_TYPE`),
`details = {"feature": <stable token>, "message": <full text>}` and
`non_retryable = true`. If the seal itself fails, the drive logs it and still
returns the original error. The sealing drive still
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
timeout. Every connect refuses the `#insecure` URL fragment, and `deny.toml`
bans the `redis` features that turn verification off. The first TLS connect
installs `ring` as the process `rustls` provider when the application set
none. Redis Cluster stays a follow-up.

**Breaking change.** `RedisAdapterError::TlsUnavailable` is removed.
`SqliteError` gains `DatabaseLocked` and `Io`, so an exhaustive `match` no
longer compiles. The public `redis` types in `from_connection` and
`RedisAdapterError::Redis` move to 0.32. An unsupported SQLite run now ends
`FAILED`, not `RUNNING`. A second `open` of one file in one process now fails.

No new `WorkflowEvent` variant and no migration.

**Tests.** `single_writer_lock.rs` runs the test binary again as a child
process. It proves four things: a second process fails, a killed holder leaves
no stale lock, a symlink shares the lock, and an inspector still reads. `unsupported_terminal.rs` covers a child workflow, `continue_as_new`, a
worker session, a terminal-cycle upsert, the fleet drivers and a reopen.
`tls_redis.rs` starts a TLS-only `redis:7.4-alpine` with certificates minted
by `rcgen`. It covers a private CA, an untrusted CA, mutual TLS, the standalone
queue, and `SSL_CERT_FILE` in a child process. The suite is in
`.github/ci/integration-suites.txt`, and CI pre-pulls the image.
