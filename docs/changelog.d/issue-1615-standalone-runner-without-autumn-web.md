## Feature — `standalone-runner` has no `autumn-web` in its manifest (issue #1615)

Part of epic #1605, Tier 3. This is the acceptance test for the epic's
definition of done. `examples/standalone-runner` replaces its old form, so
the tree keeps one standalone reference, not two.

**What changed in the example.**

- The manifest has no `autumn-web` entry. `autumn-harvest-plugin` still
  depends on it, so it stays in the build graph. The embedder never names it.
- The pool is a `diesel-async` `deadpool` pool, not `autumn_web::db::create_pool`.
  A checkout waits at most 5 s, as the `autumn-web` pool did.
- `diesel-async` connects without TLS. For `sslmode=require`, `verify-ca` and
  `verify-full`, the pool and the migration connection use rustls with the
  platform trust store. Other modes stay plaintext, as with `autumn-web`.
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
- Ctrl-C and SIGTERM both drain the worker and stop the runtime.
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
  Ctrl-C and on SIGTERM, a bootstrap token under `prod`, signed and forged
  webhooks, and a weak secret that refuses boot.
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

No migration. No new `WorkflowEvent` variant. No `harvest_events` change.
