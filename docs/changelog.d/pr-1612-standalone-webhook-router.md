## Phase — Inbound webhooks work off the plugin path (issue #1612)

Part of epic #1605 (making the non-`HarvestPlugin` integration path
first-class). `build_webhook_routes` returns `Vec<autumn_web::Route>`, which
only composes with an `autumn_web::AppBuilder` — so a `#[webhook]` binding
was reachable only through `HarvestPlugin`, even though Chapter 12's own
example is a plain 50-line Stripe integration with nothing plugin-specific
about the mapping logic itself.

**Why this was not just a return-type change.** The issue's own suggested
fix — return a bare `axum::Router` and have the plugin adapt it into
`Vec<autumn_web::Route>` — undersells the actual coupling. `autumn_web::Route::handler`
is `MethodRouter<AppState>`, not generic, because the `SignedWebhook`
extractor it wraps only implements `FromRequest<AppState>`: it reads the
installed `WebhookRegistry` out of that state's typed-extension store. A bare
`axum::Router<()>` cannot use `SignedWebhook` at all — the blocker is the
extractor's state requirement, not the `Route` wrapper.

**The fix: `AppState::detached()`.** autumn-web 0.7 already ships exactly the
right primitive for this — a minimal `AppState` with no HTTP server or
database pool, documented for "background runtimes or helper processes that
still need framework-managed resources such as typed extensions". The new
`build_webhook_router` builds one, installs a `WebhookRegistry` resolved
directly from a caller-supplied `WebhookConfig` (a standalone embedder has no
`autumn.toml` to load it from), mounts every trigger through the *same*
handler `build_webhook_routes` uses (extracted into `webhook_method_router`
so the two entry points cannot dispatch differently), and erases the state
with `Router::with_state` before returning. The result merges onto an
embedder's own bare router like any other handler, with no
`autumn_web::AppBuilder` anywhere in the caller's code.

**One real, documented gap: `replay_protection = true`.** autumn-web's
boot-window/5xx replay-key cleanup (`WebhookReplayCleanupLayer`) is installed
only by its own `AppBuilder` and has no public constructor. Mounting a
replay-protected endpoint standalone would reserve a key on every signed
request and never release one on failure, so `build_webhook_router` rejects
such a config at build time (`WebhookConfigError::InvalidEndpoint`) instead
of shipping that latent bug. In practice this is narrow:
`docs/getting-started/12-webhooks.md` already recommends
`replay_protection = false` for every Harvest-bound endpoint, since Harvest's
own dedup is durable and autumn-web's in-memory replay store is not.

No new `WorkflowEvent` variant, no migration, no replay impact. The plugin
path (`build_webhook_routes`, `HarvestPlugin::webhooks(...)`) is byte-for-byte
unchanged — it now calls a shared validation helper and a shared handler
builder, but produces the identical `Vec<autumn_web::Route>` it always did.

**Tests.** New no-database suite
`autumn-harvest-plugin/tests/standalone_webhook_mount.rs`, registered in
`.github/ci/integration-suites.txt` as an `allos` row (`webhooks` feature).
Drives a genuinely HMAC-signed request through the real `SignedWebhook`
extractor with no `autumn_web::AppBuilder` anywhere in the test: a good
signature reaches this crate's handler and fails closed with `503` (no
`HarvestApiState::install(...)` ran, same boot-window behavior as the plugin
path), a bad signature is rejected with `401` before any dispatch code runs,
and a `replay_protection = true` config is rejected at build time rather than
mounted. Full dispatch correctness (idempotent redelivery, exactly-one-
execution) needs no new coverage — it was already proven for the shared
handler by `webhook_receiver_http_tests.rs` and the testcontainers suite
`webhook_receiver_integration.rs`, and both entry points now call that same
handler.

**Not in scope.** Wiring a live example (`examples/standalone-runner` or the
epic's planned no-`autumn-web` reference app, issue #1615) is separate --
this issue's own verification bar is the router and its test, not an example
update.
