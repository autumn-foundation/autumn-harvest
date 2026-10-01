# Embedding Harvest on plain Axum

This page is the reference for a Rust service that runs Harvest without
autumn-web's `AppBuilder` and without `HarvestPlugin`. The service starts the
engine with `HarvestEmbedding` and serves the management API from its own Axum
server.

For a short, runnable start, read the getting-started fork,
[The first workflow on plain Axum](getting-started/standalone-axum.md). This
page explains each part of that chapter and the parts that it leaves out.

> `HarvestEmbedding`, `StandaloneAdminAuth`, `render_prometheus` and
> `build_webhook_router` are not in the 0.6.0 release. Use a checkout of this
> repository until the next release ships.

## Contents

- [Choose a path](#choose-a-path)
- [What the runtime needs](#what-the-runtime-needs)
- [Start the runtime](#start-the-runtime)
- [What the router gives you](#what-the-router-gives-you)
- [What you own](#what-you-own)
- [Authenticate](#authenticate)
- [Scrape metrics](#scrape-metrics)
- [Receive webhooks](#receive-webhooks)
- [Shut down](#shut-down)
- [Run more than one shard](#run-more-than-one-shard)
- [What is not available](#what-is-not-available)
- [Mount the routers without HarvestEmbedding](#mount-the-routers-without-harvestembedding)

## Choose a path

| Path | Use it when | Start here |
|---|---|---|
| `HarvestPlugin` on an autumn-web app | Your service uses autumn-web. Every feature is available. | [Getting started, Chapter 1](getting-started/01-project-skeleton.md) |
| `HarvestEmbedding` on plain Axum | Your service uses Axum, not autumn-web. | This page |
| HTTP from another language | Your service is not written in Rust. | [`openapi.md`](openapi.md), [`examples/typescript-client`](../examples/typescript-client/) |
| The `autumn-harvest` core crate alone | You need the executor and the storage layer, with no HTTP surface. | [`README.md`](../README.md#workspace) |

## What the runtime needs

**A Postgres database with the Harvest migrations.** `HarvestEmbedding` does
not apply migrations. Apply them before the process starts:

```bash
cargo run -p autumn-harvest-cli -- migrate run \
  --database-url postgres://harvest:harvest@localhost:5435/harvest
```

`harvest migrate status --check` exits `1` while a migration is pending. Use it
as a deploy gate. For a multi-shard pool, give `--database-url` once per shard.

**These crates.** The Axum types come from the `autumn_web::reexports::axum`
re-export, so the version always matches the router.

```toml
[dependencies]
autumn-harvest = "0.6"
# `metrics` adds the Prometheus recorder. `webhooks` adds webhook receivers.
autumn-harvest-plugin = { version = "0.6", features = ["metrics", "webhooks"] }
autumn-web = "0.7"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

`autumn-web` is still a dependency, because `autumn-harvest-plugin` uses its
types. Your code does not call `AppBuilder`. Issue #1615 tracks an embedding
with no `autumn-web` in its `Cargo.toml`.

**A database pool.** `HarvestRunnerResources::new` takes an
`autumn_harvest::worker::DbPool`. The core crate re-exports `diesel_async`, so
you can build the pool without autumn-web. See the
[`main.rs` of the fork chapter](getting-started/standalone-axum.md#3-the-server).

## Start the runtime

`HarvestEmbedding::start` runs the startup sequence that `HarvestPlugin` runs,
in the same order. Both paths call the same steps in `boot.rs`.

```rust
use autumn_harvest_plugin::api::StandaloneAdminAuth;
use autumn_harvest_plugin::prelude::*;
use autumn_web::reexports::axum;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

async fn serve(
    builder: HarvestBuilder,
    pool: autumn_harvest::worker::DbPool,
    database_url: String,
    admin_auth: StandaloneAdminAuth,
) -> Result<(), BoxError> {
    let config = HarvestRuntimeConfig {
        database: HarvestDatabaseConfig {
            url: Some(database_url),
        },
        outbox: HarvestOutboxConfig {
            enabled: false,
            ..HarvestOutboxConfig::default()
        },
        ..HarvestRuntimeConfig::default()
    };
    let harvest = HarvestEmbedding::new(
        builder.try_build()?,
        config,
        HarvestRunnerResources::new(pool),
    )
    .with_admin_auth(admin_auth)
    .start()
    .await
    .map_err(|error| format!("Harvest did not start: {error}"))?;

    let app = axum::Router::new().nest("/api/harvest", harvest.router());
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    harvest.stop().await;
    Ok(())
}
```

`start` does these steps:

1. It applies the operator's `[harvest.startup]` settings over `config.startup`.
   It reads `autumn.toml`, `autumn-{profile}.toml` and
   `AUTUMN_HARVEST_STARTUP__ORPHANED_WORKFLOWS`. An invalid value stops the
   boot.
2. It applies the declared admin posture to the API state and wraps the
   router in the declared auth layers.
3. It copies the builder limits into the API state.
4. It loads the persisted admission gates before a worker starts.
5. It runs the orphaned-workflow gate. See
   [`safe-deploy.md`](runbooks/safe-deploy.md).
6. It starts the runner. It then installs the storage pool and the API
   runtime, in that order, and starts the gate refresh loop.

These builder methods set the inputs:

| Method | Effect |
|---|---|
| `with_admin_auth(StandaloneAdminAuth)` | Declares the credential and the deployment profile. See [Authenticate](#authenticate). |
| `with_ambient_profile()` | Reads the profile from `AUTUMN_ENV`, then `AUTUMN_PROFILE`. A declared profile wins. |
| `without_ui()` | Leaves the Vantage dashboard out of the router. |
| `with_notification_database_urls(..)` | Sets one result-notification URL per shard. See [Run more than one shard](#run-more-than-one-shard). |
| `with_api_state(HarvestApiState)` | Uses your API state, for example to call `set_actor_extractor`. Do not install a runtime or a pool on it. |

`start` returns an error when the startup config is invalid, a shard has no
notification URL, the orphan gate refuses the boot, or the runner does not
start.

## What the router gives you

`HarvestEmbeddingRuntime::router()` returns an `axum::Router<()>`. Nest it
under any path. It holds these routes:

- The management API. [`management-api.md`](management-api.md) lists every
  route.
- The Vantage dashboard under `/ui`, unless you call `without_ui()`.
- `GET /openapi.json`. It needs no credential, by design.

The admin guard covers only the `/admin` routes. Other routes, for example a
workflow start, a signal or a cancel, have no built-in guard. Put your own
auth layer around the router, as on the plugin path.

The layer order is load-bearing. A request passes through these layers, from
the outside in:

1. Your auth layer.
2. The scoped-token layer, if you declare `with_api_tokens()`.
3. The read-only-role layer, if you declare `with_read_only_role()`.
4. The admin guard on each `/admin` route.

`router()` applies layers 2 to 4. Apply layer 1 to the router that it returns.

## What you own

`HarvestPlugin` does some work that `HarvestEmbedding` does not do. This table
shows who does each item on each path.

| Responsibility | `HarvestPlugin` | `HarvestEmbedding` | You |
|---|---|---|---|
| Apply `[harvest.startup]` operator settings | Yes | Yes | — |
| Load the admission gates at boot, then refresh them | Yes | Yes | — |
| Install the storage pool, then the API runtime | Yes | Yes | — |
| Copy the builder limits into the API state | Yes | Yes | — |
| Set the deployment profile | From autumn-web | From `StandaloneAdminAuth` or `with_ambient_profile()` | Declare it |
| Install the token and read-only-role layers | `enable_api_tokens()`, `api_with_role_auth(..)` | From `StandaloneAdminAuth` | Declare them |
| Authenticate non-admin routes | `api_with_auth(..)` | No | Your auth layer |
| Apply migrations | Under the `dev` profile | No | `harvest migrate run` |
| Create the database pool | From `[database]` | No | Build a `DbPool` |
| Serve HTTP | autumn-web | No | Your Axum server |
| Serve metrics | `/actuator/prometheus` | No | A route that calls `render_prometheus()` |
| Mount webhook receivers | `HarvestPlugin::webhooks(..)` | No | `build_webhook_router(..)` |
| Stop on shutdown | Shutdown hook | `stop()` | Call `stop()` after the server stops |

## Authenticate

A new `StandaloneAdminAuth` declares nothing. The profile is then `unknown`,
and every `/admin` route answers `401`. Declare a profile and a credential.

### The deployment profile

Declare it with `with_deployment_profile("prod")`, or call
`HarvestEmbedding::with_ambient_profile()` to read it from the environment.
The `dev` profile with no boundary admits any caller with no credential. The
server logs a warning when this is the case. Use `dev` on a workstation only.

### Scoped API tokens

`with_api_tokens()` installs the token layer (issue #942). A request with an
`Authorization: Bearer hvst_…` header reaches the admin routes that its scope
allows. A `read` token gets `403` on a mutating route.

`POST /admin/tokens` mints a token, but only an admin can call it. Seed the
first token offline instead:

```bash
cargo run -p autumn-harvest-cli -- token bootstrap --name ops-seed --scope mutate
```

The command opens no connection. It prints the secret once and an `INSERT`
statement. Run the statement against the Harvest database. Then pass the
secret to the CLI with `--token` or `HARVEST_TOKEN`.
[`security-posture.md`](security-posture.md#first-token-bootstrap-standalone-mode)
explains the design.

### Your own auth layer

`with_admin_auth_boundary()` declares that your layer authenticates every
request. The admin guard then admits each request that reaches it. Declare it
only when a layer really wraps the router.

```rust
use autumn_harvest::api_token::looks_like_harvest_token;
use autumn_harvest_plugin::HarvestEmbeddingRuntime;
use autumn_harvest_plugin::api::StandaloneAdminAuth;
use autumn_web::reexports::axum::{
    self,
    extract::Request,
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};

/// The posture for a production deployment.
fn admin_auth() -> StandaloneAdminAuth {
    StandaloneAdminAuth::new()
        .with_api_tokens()
        .with_admin_auth_boundary()
        .with_deployment_profile("prod")
}

/// Your own credential check. This placeholder admits no caller.
fn is_operator(_bearer: &str) -> bool {
    false
}

/// Your auth layer. The Harvest token layer inside it verifies `hvst_` tokens.
async fn require_operator(request: Request, next: Next) -> Response {
    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match bearer {
        Some(token) if looks_like_harvest_token(token) || is_operator(token) => {
            next.run(request).await
        }
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}

fn app(harvest: &HarvestEmbeddingRuntime) -> axum::Router {
    let api = harvest.router().layer(middleware::from_fn(require_operator));
    axum::Router::new().nest("/api/harvest", api)
}
```

A browser does not send a bearer header. To use Vantage, accept a browser
credential in your layer too, for example a session cookie.

### The read-only operator role

`with_read_only_role()` installs the class-aware read-only layer (issue #776).
It reads an autumn-web `Session` that your layer sets. Without that session,
the layer has no effect. [`operator-role.md`](operator-role.md) describes the
role.

### What preflight reports

`harvest preflight` calls `GET /admin/preflight`. Its `admin_auth_boundary`
check reads only the profile and the boundary declaration.

| Profile | `with_admin_auth_boundary()` | `admin_auth_boundary` check |
|---|---|---|
| `dev` | No | Pass, with `unauthenticated_access: true` |
| `dev` | Yes | Pass |
| `unknown` | No | Warn |
| Any other | Yes | Pass |
| Any other | No | **Fail**, also when tokens are on |

Tokens alone do not pass this check. The token layer guards the `/admin`
routes only, so a workflow start is still open. Wrap the router in your own
layer and declare the boundary.

## Scrape metrics

`HarvestMetricsRecorder` collects the catalogue metrics in process. It needs
the `metrics` feature. Give the same recorder to the builder and to a route.

```rust
use std::sync::Arc;

use autumn_harvest_plugin::metrics_scrape::HarvestMetricsRecorder;
use autumn_harvest_plugin::prelude::*;
use autumn_web::reexports::axum::{self, http::header::CONTENT_TYPE, routing::get};

/// Send the engine's samples to `metrics`.
fn with_metrics(builder: HarvestBuilder, metrics: &HarvestMetricsRecorder) -> HarvestBuilder {
    builder.telemetry(
        TelemetryConfig::builder()
            .metrics(Arc::new(metrics.clone()))
            .build(),
    )
}

/// Serve the samples in the Prometheus text format.
fn metrics_route(metrics: HarvestMetricsRecorder) -> axum::Router {
    axum::Router::new().route(
        "/metrics",
        get(move || {
            let metrics = metrics.clone();
            async move {
                (
                    [(CONTENT_TYPE, "text/plain; version=0.0.4")],
                    metrics.render_prometheus(),
                )
            }
        }),
    )
}
```

The output is empty until the engine records a sample. The recorder covers
the catalogue metrics, not every metric that the alert pack reads.
[`telemetry.md`](telemetry.md) describes the full set and the `metrics-rs`
adapter.

## Receive webhooks

`build_webhook_router` returns an `axum::Router<()>` for `#[webhook]`
triggers (issue #1612). It needs the `webhooks` feature. Give it the
endpoint config that `autumn.toml` holds on the plugin path.

```rust
use autumn_harvest_plugin::HarvestEmbeddingRuntime;
use autumn_harvest_plugin::prelude::*;
use autumn_harvest_plugin::webhook_receiver::build_webhook_router;
use autumn_web::reexports::axum;
use autumn_web::webhook::{WebhookConfig, WebhookConfigError, WebhookEndpointConfig};

fn webhooks(
    harvest: &HarvestEmbeddingRuntime,
    triggers: &[WebhookTriggerInfo],
    workflows: &[WorkflowInfo],
    secret: &str,
) -> Result<axum::Router, WebhookConfigError> {
    let endpoint = WebhookEndpointConfig::generic("orders", "/hooks/orders", secret)
        .without_replay_protection();
    let config = WebhookConfig {
        endpoints: vec![endpoint],
        ..WebhookConfig::default()
    };
    build_webhook_router(triggers, workflows, &[], harvest.api_state(), &config)
}
```

Pass the workflows that you register on the builder. Merge the result into
your app at the root, not under the management API path.

These limits apply off the plugin path:

- An endpoint with replay protection is an error. The cleanup layer that
  releases a replay key after a `5xx` exists only in autumn-web. Harvest
  deduplicates by `WorkflowId`, so you do not need it.
- The function panics at build time when a trigger has no endpoint, targets
  a DAG, or targets an unregistered workflow.
- The routes do not get the plugin's request timeout, idempotency metadata or
  OpenAPI entry.

[Chapter 12](getting-started/12-webhooks.md) explains triggers and signature
verification.

## Shut down

Stop the HTTP server first. Then call `HarvestEmbeddingRuntime::stop`:

1. The gate refresh loop stops.
2. The runner drains its worker, up to `WorkerConfig::shutdown_timeout`.
3. The admission globals are removed.
4. The API state is cleared, so the router answers `503`.

A dropped runtime keeps its background tasks and its process globals. Always
call `stop()`.

## Run more than one shard

Only the standalone path runs a multi-shard pool. `HarvestPlugin` rejects one
by design. Give the sharded pool to `HarvestRunnerResources`, and one
notification URL per shard to `with_notification_database_urls`. `start`
refuses a shard with no URL.
[`sharding.md`](sharding.md#step-4--flip-writable-and-verify) has the code.

A cross-type continue-as-new whose new key routes to a different shard fails
terminally. See
[`architecture.md`](architecture.md#cross-type-continue-as-new--multi-phase-entities-issue-803).

## What is not available

These features need autumn-web. `HarvestEmbedding` does not provide them:

- **MCP tools.** The tool routes use autumn-web's `mount_mcp` envelope.
- **The workflow-start outbox relay.** `start` logs a warning when
  `config.outbox.enabled` is set.
- **Broker connectors** (Kafka, SQS). See
  [Chapter 13](getting-started/13-broker-connectors.md).
- **Outbound webhook delivery.** The plugin registers the delivery workflow.
- **Webhook replay protection.** See [Receive webhooks](#receive-webhooks).
- **Session login for the admin API.** Your layer can set an autumn-web
  `Session`, but no login flow comes with this path.

## Mount the routers without HarvestEmbedding

`harvest_api_router` and `harvest_ui_router` are public, and both return
`Router<()>`. A hand-built mount must do each step of
[Start the runtime](#start-the-runtime) itself. For example, it must load the
admission gates at boot. The module docs of `autumn_harvest::admission_gate`
explain why. Use `HarvestEmbedding` unless you need a different order.
