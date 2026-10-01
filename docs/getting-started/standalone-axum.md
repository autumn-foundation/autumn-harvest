# Fork — The first workflow on plain Axum

[← Your first workflow](02-first-workflow.md) · [Index](README.md) · [Next: Durable timers →](03-durable-timers.md)

---

This chapter is a fork in the road, not a fourteenth step. Take it when your
service runs on plain Axum, not on autumn-web. It runs the Chapter 2 workflow
with no `HarvestPlugin` and no `AppBuilder`.

Workflows, activities, timers, signals and child workflows are the same on
both paths. Three things change: how the process starts the engine, how it
serves the management API, and how the migrations run.

[`embedding.md`](../embedding.md) is the reference for this path. This chapter
is the short version.

> **Prerequisites**
> - Read [Chapter 2](02-first-workflow.md) first. It explains the workflow and
>   the activity. Skip its `HarvestPlugin` registration.
> - A clone of this repository. The code in this chapter is the crate
>   [`examples/standalone-quickstart`](../../examples/standalone-quickstart/),
>   and CI runs each command below as written.
> - Docker, for Postgres.
> - `jq`.
>
> Run each command from the repository root. `HarvestEmbedding` is not in the
> 0.6.0 release, so run this chapter from the checkout until the next release
> ships.

## 1. The crate

<!-- sync: examples/standalone-quickstart/Cargo.toml -->
```toml
# This crate inherits no workspace settings, so you can copy it out of the
# repository as it is.
[package]
name = "standalone-quickstart"
version = "0.1.0"
edition = "2024"
publish = false

[dependencies]
autumn-harvest = { version = "0.6.0", path = "../../autumn-harvest" }
autumn-harvest-plugin = { version = "0.6.0", path = "../../autumn-harvest-plugin" }
autumn-web = "0.7"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
tracing = "0.1"
tracing-subscriber = "0.3"

[dev-dependencies]
autumn-harvest = { version = "0.6.0", path = "../../autumn-harvest", features = ["testing"] }
```

The crate does not call autumn-web's `AppBuilder`. It still depends on
`autumn-web`. `autumn-harvest-plugin` needs it, and `main.rs` uses its Axum
re-export. Issue #1615 tracks the removal of that dependency.

Outside this repository, remove each `path` key. Set each `version` to the
first release that contains `HarvestEmbedding`.

## 2. The workflow

`src/workflows.rs` holds the Chapter 2 code with no change. Below it,
`harvest_builder` registers the workflow and the activity.

<!-- sync: examples/standalone-quickstart/src/workflows.rs -->
```rust
use std::time::Duration;

use autumn_harvest::prelude::*;

#[workflow]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> HarvestResult<String> {
    let result = ctx
        .execute_activity_raw(
            "send_welcome_email",
            serde_json::json!({ "user_id": user_id }),
            "default",
        )
        .await?;

    Ok(result["status"].as_str().unwrap_or("sent").to_owned())
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
async fn send_welcome_email(
    _ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let user_id = input["user_id"].as_i64().unwrap_or_default();
    tracing::info!(user_id, "sending welcome email");
    Ok(serde_json::json!({ "status": "sent" }))
}

/// Register the workflow and the activity. On the plugin path,
/// `HarvestPlugin::workflows` and `HarvestPlugin::activities` do this.
pub fn harvest_builder() -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows![onboarding])
        .activities(activities![send_welcome_email])
        .worker(WorkerConfig::default())
}

#[cfg(test)]
mod tests;
```

The `tests` module runs `onboarding` under `WorkflowSimulator`, with the
activity mocked. [Chapter 11](11-testing.md) covers workflow tests.

## 3. The server

<!-- sync: examples/standalone-quickstart/src/main.rs -->
```rust
//! The Chapter 2 workflow on a plain Axum server, with no `HarvestPlugin`.

mod workflows;

use autumn_harvest::diesel_async::AsyncPgConnection;
use autumn_harvest::diesel_async::pooled_connection::AsyncDieselConnectionManager;
use autumn_harvest::diesel_async::pooled_connection::deadpool::Pool;
use autumn_harvest_plugin::prelude::*;
use autumn_web::reexports::axum;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Print the engine's log lines, for example the dev-profile warning.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let database_url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
    let pool = Pool::builder(manager).max_size(10).build()?;

    // The default config runs the worker and the scheduler in this process.
    // The outbox relay needs autumn-web, so `HarvestEmbedding` does not run it.
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

    // Bind first. A busy port then stops the process before a worker starts.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    // Run the startup sequence of `HarvestPlugin` without autumn-web's `AppBuilder`.
    let harvest = HarvestEmbedding::new(
        workflows::harvest_builder().try_build()?,
        config,
        HarvestRunnerResources::new(pool),
    )
    .with_ambient_profile()
    .start()
    .await
    .map_err(|error| format!("Harvest did not start: {error}"))?;

    let app = axum::Router::new().nest("/api/harvest", harvest.router());
    tracing::info!("listening on http://127.0.0.1:3000");
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;

    // Drain the worker and remove the process globals, also after a serve error.
    harvest.stop().await;
    tracing::info!("Harvest stopped");
    served?;
    Ok(())
}

/// Wait for Ctrl-C.
async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "cannot listen for Ctrl-C");
    }
}
```

`HarvestEmbedding::start` runs the same startup steps as `HarvestPlugin`.
[Start the runtime](../embedding.md#start-the-runtime) lists them.

`harvest.router()` is a plain `axum::Router`. Nest it under any path.
`harvest.stop()` drains the worker after the server stops.

## 4. Run it

Start Postgres:

<!-- sync: examples/standalone-quickstart/compose.yaml -->
```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: harvest
      POSTGRES_PASSWORD: harvest
      POSTGRES_DB: harvest
    ports:
      - "127.0.0.1:5435:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U harvest"]
      interval: 2s
      timeout: 5s
      retries: 15
```

<!-- chapter-run: skip CI starts the same Postgres as a service container. -->
```bash
docker compose -f examples/standalone-quickstart/compose.yaml up -d --wait
```

Apply the Harvest migrations. On the plugin path, the `dev` profile applies
them at boot. Here, the `harvest` CLI applies them:

<!-- chapter-run: expect migration(s) applied -->
```bash
cargo run -p autumn-harvest-cli -- migrate run \
  --database-url postgres://harvest:harvest@localhost:5435/harvest
```

Start the server:

<!-- chapter-run: serve -->
```bash
DATABASE_URL=postgres://harvest:harvest@localhost:5435/harvest \
AUTUMN_PROFILE=dev \
cargo run -p standalone-quickstart
```

`with_ambient_profile()` reads `AUTUMN_ENV`, then `AUTUMN_PROFILE`. The `dev`
profile opens the admin routes to any caller with no credential. The server
logs a warning about this.

Other profiles do not close everything. With no auth layer of your own, a
workflow start or a signal is open in every profile. So keep this server on
`127.0.0.1`.

## 5. Start the workflow

In a second terminal, send the Chapter 2 request:

<!-- chapter-run: expect "execution_id": " -->
```bash
curl -s -X POST http://localhost:3000/api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"user-42","input":42}' | jq .
```

Wait for the result. The request returns when the run completes, or after 30
seconds:

<!-- chapter-run: expect "output": "sent" -->
```bash
curl -s 'http://localhost:3000/api/harvest/workflows/by-id/onboarding/user-42/result?wait=30s' | jq .
```

Open `http://localhost:3000/api/harvest/ui/workflows` to see the run in the
Vantage dashboard.

## 6. Run preflight

<!-- chapter-run: preflight -->
```bash
cargo run -p autumn-harvest-cli -- preflight
```

The CLI's default base URL is `http://localhost:3000/api/harvest`. Preflight
exits `0` on a pass and `2` on a warning. Expect `0` or `2` here. The
`admin_auth_boundary` row passes, and it says that any caller can reach the
admin API.

Press Ctrl-C in the server terminal. The server stops, drains the worker, and
logs `Harvest stopped`.

## Before production

The `dev` profile is for this chapter only. Read these sections of the
reference before you deploy:

- [Authenticate](../embedding.md#authenticate): declare a profile, a
  credential and your own auth layer. Tokens alone do not pass preflight.
- [What you own](../embedding.md#what-you-own): the work that
  `HarvestPlugin` does and this path does not.
- [Scrape metrics](../embedding.md#scrape-metrics) and
  [Shut down](../embedding.md#shut-down).
- [What is not available](../embedding.md#what-is-not-available): MCP tools,
  the outbox relay and broker connectors need autumn-web.

## Back to the main path

Continue at [Chapter 3](03-durable-timers.md). Add each workflow and activity
to `src/workflows.rs`, and register it in `harvest_builder`. Where a chapter
registers code with a `HarvestPlugin` method, such as `.signals(..)` or
`.dags(..)`, call the `HarvestBuilder` method with the same name.

These chapters differ on this path:

- [Chapter 10](10-operations.md): use `harvest migrate run`, not
  `autumn migrate`. Where it says that a call needs admin auth, see
  [Authenticate](../embedding.md#authenticate).
- [Chapter 12](12-webhooks.md): mount receivers with `build_webhook_router`.
  See [Receive webhooks](../embedding.md#receive-webhooks).
- [Chapter 13](13-broker-connectors.md): broker connectors are available on
  the plugin path only.

---

[← Your first workflow](02-first-workflow.md) · [Index](README.md) · [Next: Durable timers →](03-durable-timers.md)
