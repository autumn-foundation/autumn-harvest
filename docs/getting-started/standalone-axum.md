# Fork — The first workflow on plain Axum

[← Index](README.md) · [Chapter 2](02-first-workflow.md) · [Reference: `embedding.md`](../embedding.md)

---

This chapter is a fork in the road, not a fourteenth step. Take it when your
service runs on plain Axum, not on autumn-web. It runs the Chapter 2 workflow
with no `HarvestPlugin` and no `AppBuilder`.

Workflows, activities, timers, signals and child workflows are the same on
both paths. Only two things change: how the process starts the engine, and
how it serves the management API.

[`embedding.md`](../embedding.md) is the reference for this path. This chapter
is the short version.

> **Prerequisites**
> - A clone of this repository. The code in this chapter is the crate
>   [`examples/standalone-quickstart`](../../examples/standalone-quickstart/),
>   and CI runs each command below as written.
> - Docker, for Postgres.
> - `jq`.
>
> `HarvestEmbedding` is not in the 0.6.0 release. Run this chapter from the
> checkout until the next release ships.

## 1. The crate

<!-- sync: examples/standalone-quickstart/Cargo.toml -->
```toml
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

[dev-dependencies]
autumn-harvest = { version = "0.6.0", path = "../../autumn-harvest", features = ["testing"] }
```

The crate does not call autumn-web's `AppBuilder`. It still depends on
`autumn-web`, because `autumn-harvest-plugin` does and `main.rs` uses its Axum
re-export. Issue #1615 removes that dependency.

Outside this repository, remove each `path` key. Use the first release that
contains `HarvestEmbedding`.

## 2. The workflow

`src/workflows.rs` holds the Chapter 2 code with no change. Below it,
`harvest_builder` registers the workflow and the activity. On the plugin
path, `HarvestPlugin::workflows` and `HarvestPlugin::activities` do this.

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

The `tests` module runs `onboarding` under `WorkflowSimulator`.
[Chapter 11](11-testing.md) explains that tool.

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
    let database_url = std::env::var("DATABASE_URL").map_err(|_| "set DATABASE_URL")?;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(&database_url);
    let pool = Pool::builder(manager).build()?;

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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    tracing::info!("listening on http://127.0.0.1:3000");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Drain the worker and remove the process globals.
    harvest.stop().await;
    Ok(())
}
```

`HarvestEmbedding::start` runs the startup sequence of `HarvestPlugin`:

- It applies the operator's `[harvest.startup]` settings.
- It loads the persisted admission gates before the worker starts.
- It installs the storage pool and then the API runtime.

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
      - "5435:5432"
```

```bash
docker compose -f examples/standalone-quickstart/compose.yaml up -d
```

Apply the Harvest migrations. On the plugin path, the `dev` profile applies
them at boot. Here, the `harvest` CLI applies them:

<!-- chapter-run: expect MIGRATE_EXPECT -->
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

`with_ambient_profile()` reads `AUTUMN_PROFILE`. The `dev` profile opens the
management API to any caller with no credential. The server logs a warning
about this. Do not use `dev` outside your workstation.

## 5. Start the workflow

In a second terminal, send the Chapter 2 request:

<!-- chapter-run: expect execution_id -->
```bash
curl -s -X POST http://localhost:3000/api/harvest/workflows/onboarding/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"user-42","input":42}' | jq .
```

Wait for the result. The request returns when the run completes, or after 30
seconds:

<!-- chapter-run: expect RESULT_EXPECT -->
```bash
curl -s 'http://localhost:3000/api/harvest/workflows/by-id/onboarding/user-42/result?wait=30s' | jq .
```

Open `http://localhost:3000/api/harvest/ui` to see the run in the Vantage
dashboard.

## 6. Run preflight

<!-- chapter-run: preflight -->
```bash
cargo run -p autumn-harvest-cli -- preflight
```

The CLI's default base URL is `http://localhost:3000/api/harvest`. Preflight
exits `0` on a pass and `2` on a warning. Under `dev`, its
`admin_auth_boundary` check reports `unauthenticated_access: true`.

## Before production

The `dev` profile is for this chapter only. Read these sections of the
reference before you deploy:

- [Authenticate](../embedding.md#authenticate): declare a profile and a
  credential. Tokens alone do not pass preflight.
- [What you own](../embedding.md#what-you-own): the work that
  `HarvestPlugin` does and this path does not.
- [Scrape metrics](../embedding.md#scrape-metrics) and
  [Shut down](../embedding.md#shut-down).
- [What is not available](../embedding.md#what-is-not-available): MCP tools,
  the outbox relay and broker connectors need autumn-web.

## Back to the main path

Chapters 3 to 11 apply to this path. Add each workflow and activity to
`src/workflows.rs`, and register it in `harvest_builder`. Where a chapter
registers code with a `HarvestPlugin` method, such as `.signals(..)` or
`.dags(..)`, call the `HarvestBuilder` method with the same name. For the auth
methods in [Chapter 10](10-operations.md), see
[Authenticate](../embedding.md#authenticate).

Two chapters differ:

- [Chapter 12](12-webhooks.md): mount receivers with `build_webhook_router`.
  See [Receive webhooks](../embedding.md#receive-webhooks).
- [Chapter 13](13-broker-connectors.md): broker connectors are available on
  the plugin path only.

---

[← Index](README.md) · [Chapter 2](02-first-workflow.md) · [Reference: `embedding.md`](../embedding.md)
