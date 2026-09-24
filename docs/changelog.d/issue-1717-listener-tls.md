## Phase — TLS for LISTEN/NOTIFY listeners (issue #1717)

`QueueListener`, `WorkflowEventListener` and `WorkflowProgressListener` always
used `NoTls`. Every listener failed on Postgres with `sslmode=require`.
Workers fell back to polling, so the failure stayed hidden. `result_raw` had no
fallback, so each synchronous result wait failed.

**What changed**

- The three listeners share one connection helper, `open_listen_connection`.
  It reads the `sslmode` of the DSN:
  - `require` uses rustls (`ring` provider). The chain and the hostname are
    verified against the platform trust store, as in `harvest migrate`
    (issue #1240). The configuration is built once per process.
  - `disable`, `prefer` and no `sslmode` stay plaintext. This keeps the old
    behavior for a server with a self-signed certificate.
- New `tls` cargo feature, on by default. It adds `rustls`,
  `rustls-native-certs` and `tokio-postgres-rustls`. The lockfile already
  had them through the CLI, so it gets no new crate. Without the feature, a
  `require` DSN gets a `HarvestError::Config` that names the feature.
- Listener errors show the full `source()` chain. A TLS failure now names its
  cause, for example `invalid peer certificate: UnknownIssuer`.
- `result_raw`, `result_raw_with_timeout` and `result_snapshot_with_wait` poll
  every 500 ms when the listener cannot connect. Each loop reads the execution
  state again, so polling changes only the wake-up latency. A missing
  notification URL is still a configuration error.

No new `WorkflowEvent` variant. No migration. The public API is unchanged.

**Tests**

- Unit (`notify.rs`): only `sslmode=require` selects TLS, in URL and keyword
  form. `error_chain` names every cause.
- Integration (`workflow_handle_tests.rs`): each of the three result waits
  returns the result when the listener URL refuses connections. A
  `sslmode=require` listener reaches a real TLS negotiation and names the cause.
- Manual: a listener with `sslmode=require` connects to a TLS-only Postgres 16
  and receives a notification. The connection shows `ssl = t` in
  `pg_stat_ssl`.
