# Chapter 1 — Project skeleton

[← Index](README.md) · [Next: Your first workflow and activity →](02-first-workflow.md)

---

> **Shortcut:** `harvest new <name>` scaffolds this entire skeleton — a
> `Cargo.toml`, a runnable `#[workflow]`/`#[activity]` pair with `HarvestPlugin`
> wiring, a `compose.yaml` Postgres, an `autumn.toml`, and a README whose
> three-command path reaches one durable execution. It is pure local file
> generation (no database, no network) and names everything after `<name>`. The
> rest of this chapter builds the skeleton by hand so you can see each moving
> part; reach for `harvest new` once you know what it emits.

Create a new Cargo project that depends on the engine, the Autumn plugin, and
the web framework:

```toml
# Cargo.toml
[package]
name = "harvest-tutorial"
version = "0.1.0"
edition = "2021"

[dependencies]
autumn-harvest = "0.4"
autumn-harvest-plugin = "0.4"
autumn-web = "0.5"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
```

Add the boilerplate `main.rs` — at this point we register zero workflows and
zero activities, just to confirm the plugin mounts cleanly:

```rust
// src/main.rs
use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
```

Drop in a Postgres `compose.yaml` next to your `Cargo.toml` (the
[quickstart's compose file](../../examples/quickstart/compose.yaml) is a good
starting point) and an `autumn.toml` that points the framework at it:

```toml
# autumn.toml
[database]
url = "postgres://postgres:postgres@localhost:5432/autumn_harvest"
```

Bring it up:

```bash
docker compose up -d
AUTUMN_PROFILE=dev cargo run
```

`AUTUMN_PROFILE=dev` runs `diesel migration run` automatically on startup so
you don't need `diesel-cli` for the dev loop. The app will start on
`http://localhost:3000`. Hit the health endpoint to confirm the plugin
mounted:

```bash
curl -s http://localhost:3000/api/harvest/health | jq .
```

---

[← Index](README.md) · [Next: Your first workflow and activity →](02-first-workflow.md)
