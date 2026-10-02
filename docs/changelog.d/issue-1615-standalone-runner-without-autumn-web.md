## Feature — `standalone-runner` has no `autumn-web` in its manifest (issue #1615)

Part of epic #1605, Tier 3. This is the acceptance test for the epic's
definition of done. `examples/standalone-runner` replaces its old form, so
the tree keeps one standalone reference, not two.

**What changed in the example.**

- The manifest has no `autumn-web` entry. `autumn-harvest-plugin` still
  depends on it, so it stays in the build graph. The embedder never names it.
- The pool is a `diesel-async` `deadpool` pool, not `autumn_web::db::create_pool`.
  A checkout waits at most 5 s, as the `autumn-web` pool did.
- `diesel-async` connects without TLS, so the pool and the migration
  connection go through rustls. `require` and `verify-full` check the chain
  against the platform trust store and the host name. `verify-ca` checks the
  chain only. `prefer` and an unset `sslmode` are below. `sslmode` is read
  with the libpq grammar, in both DSN forms.
- In `dev`, migrations go through `autumn_harvest::migrate`, the code behind
  `harvest migrate run`, not `autumn_web::migrate::run_pending`.
- `HarvestEmbedding` mounts the API and Vantage on a plain `axum::Router`.
- `StandaloneAdminAuth::with_api_tokens()` gates the admin routes. The first
  token comes from `harvest token bootstrap`.
- `GET /metrics` serves `HarvestMetricsRecorder::render_prometheus()`.
- `build_webhook_router` serves a `#[webhook]` binding. The route exists only
  when `STANDALONE_RUNNER_WEBHOOK_SECRET` is set. Outside `dev`, the example
  calls `WebhookConfig::validate(true)`, so a short or demo secret refuses
  boot. `build_webhook_router` itself checks only that the secret is not empty.
- `STANDALONE_RUNNER_ADDR` sets the listen address. The default is still
  `127.0.0.1:8082`. The log names the bound address, so port `0` works.
- Ctrl-C and SIGTERM both drain the worker and stop the runtime. Open
  responses, such as an SSE stream, get 10 s. The server then closes them, so
  shutdown always reaches the worker drain.
- Each fallible step runs before `start`. After `start`, the runtime always
  stops, also when the server fails.

**Plugin additions.**

- `webhook_receiver` re-exports `WebhookConfig`, `WebhookEndpointConfig` and
  `WebhookConfigError`. Before this, `build_webhook_router` took a type that
  only an `autumn-web` dependency could name.
- `embedding::ambient_deployment_profile()` returns the profile that
  `with_ambient_profile` reads. The example uses it to decide on migrations.
  A local reader would miss the normalization, so `development` would not
  migrate. The acceptance suite pins this.

**Enforcement.**

- `manifest_names_no_autumn_web` reads every dependency table, target tables
  and `package =` renames included.
- `source_names_no_autumn_web_path` rejects a code line that names the
  crate, so a re-export path fails too.
- `tests/acceptance.rs` runs the built binary against Postgres. It covers
  `dev` migration, Vantage, a completed order, `/metrics`, a clean exit on
  Ctrl-C and on SIGTERM, also with an SSE stream open, a bootstrap token
  under `prod`, signed and forged webhooks, and a weak secret that refuses
  boot.
- CI runs the unit tests with `cargo test -p standalone-runner --bins`. The
  manifest row `linux standalone-runner acceptance` runs the live suite on
  `test-db-linux`. `ci_run_coverage.rs` accepts the new crate name and fails
  when an example test target has no row.

**Known limits.**

- Under a named non-`dev` profile, `harvest preflight` reports
  `admin_auth_boundary: fail`. The token gates the admin routes only. A
  workflow start, signal-with-start or update-with-start has no auth until
  the embedder adds a layer and declares `with_admin_auth_boundary()`. The
  plugin path behaves the same.
- `HarvestEmbedding::start` still returns an `autumn-web` error type, and the
  webhook config types are `autumn-web` types behind a plugin re-export.

**`sslmode=prefer` and an unset `sslmode` use TLS when the server offers it.**

A managed Postgres, Fly for example, hands out a URL with no `sslmode` and
refuses plaintext. Harvest sent plaintext for such a URL from every
connection it opens itself, so those connections failed.

- New module `autumn_harvest::pg_tls` holds the rule once. `prefer`, which is
  also the default, follows libpq: the client sends an `SSLRequest`, and goes
  on in plaintext only when the server declines. The certificate is not
  checked, as in libpq, so a self-signed server keeps working. `require` and
  `verify-full` verify the chain and the hostname, `verify-ca` the chain.
  `allow` starts in plaintext and, as in libpq, retries once with TLS when
  the server rejects it. `disable` is plaintext. `sslmode` is read with the
  libpq grammar, in both DSN forms.
- Users: the LISTEN/NOTIFY listeners (`notify.rs`), `harvest backup verify`,
  `harvest dr`, and the `standalone-runner` pool and `dev` migrations. The
  listeners now also accept `verify-ca` and `verify-full`.
- Without the `tls` feature, `prefer` and `allow` stay plaintext, and a
  verified mode is a configuration error.
- `harvest migrate` keeps its own connector (issue #1240). It verifies the
  certificate for `prefer` too.
- `docs/getting-started/10-operations.md` shows the new table.

Tests. Fake-server tests in `notify.rs` and `pg_tls.rs` check the wire.
`prefer` and an unset `sslmode` send `SSLRequest` and then a ClientHello. When
the server answers `N`, they send a plaintext startup message. The example
acceptance test runs a runner with no `sslmode`. It checks that every
connection is encrypted when the server offers TLS, and plaintext when it
does not.

No migration. No new `WorkflowEvent` variant. No `harvest_events` change.
