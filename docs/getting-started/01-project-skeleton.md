# Chapter 1 — Project skeleton

[← Index](README.md) · [Next: Your first workflow and activity →](02-first-workflow.md)

---

## The fastest path: `cargo dev`

If you just want to *see* a durable workflow run, you do not need a database, a
`compose.yaml`, or Docker. From a clone of this repository, with only the Rust
toolchain installed:

```bash
cargo dev
```

That starts an ephemeral PostgreSQL, applies the engine's migrations, runs a
worker, and serves the management API and the Vantage dashboard. It prints the
dashboard URL and one `curl` that starts a sample workflow — an activity, a
durable timer, another activity — so you can watch a real execution progress in
the UI, step by step through its append-only history. `Ctrl-C` stops it and
removes everything it created: no leftover processes, no leftover data
directories.

Note the flip side of that last sentence: because the storage is thrown away on
exit, **a `cargo dev` run cannot show you a workflow surviving a restart**. To
watch that — the property the durable timer exists to demonstrate — point the
runtime at a database of your own, which it will never delete:

```bash
HARVEST_DEV_DATABASE_URL=postgres://me@localhost:5432/harvest_dev cargo dev
```

Start the sample, kill the process while the timer is counting down, and start
it again: the engine replays the first activity from history rather than
re-running it.

The cluster it starts is **real PostgreSQL running the engine's real schema and
real migrations**, so what you see is exactly what you would get in production.
The dev runtime automates the database *lifecycle*, not the database.

A few things worth knowing:

- **It is development-only, and it enforces that.** It refuses to start against
  anything it cannot show to be a local database: a non-loopback host, a DSN
  demanding TLS, or a known hosted-Postgres endpoint are all rejected outright.
  Its banner says it is not for production on every start.
- **It does not need Postgres installed.** `cargo dev` enables the
  `dev-runtime-managed` feature, which downloads a platform-matched PostgreSQL
  build into a per-user cache the first time you run it (about 30 MB, once). If
  you already have Postgres installed, it uses that and never touches the
  network — and you can build the lighter tier explicitly:

  ```bash
  cargo run -p autumn-harvest-plugin --features dev-runtime --bin harvest-dev
  ```

- **Bring your own database** if you would rather:

  ```bash
  HARVEST_DEV_DATABASE_URL=postgres://me@localhost:5432/harvest_dev cargo dev
  ```

  It still goes through the same safety gate, and it is left exactly as it is on
  exit — the dev runtime only ever deletes storage it created itself.
- **On Windows** the whole path is pure Rust (no libpq, no OpenSSL). Provisioning
  finds a standard EnterpriseDB install under
  `C:\Program Files\PostgreSQL\<version>\bin`, and the managed tier downloads
  a Windows build.

`harvest-dev --help` lists the rest (`--port`, `--session-root`,
`--allow-suspicious-database-name`).

Everything from here on builds the same skeleton **by hand**, against a Postgres
you provide, because that is what your own project will look like.

---

## Bring your own Postgres

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
autumn-harvest = "0.6"
autumn-harvest-plugin = "0.6"
autumn-web = "0.7"
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

`HarvestPlugin` registers its migrations with Autumn, which applies them
before any startup hook runs. Under `AUTUMN_PROFILE=dev` pending migrations are
applied automatically, so you don't need `diesel-cli` for the dev loop. (Outside
`dev`, pending migrations are only *reported* — run `autumn migrate` in your
deploy pipeline first. See [Chapter 10](10-operations.md).) The app will start
on `http://localhost:3000`. Hit the health endpoint to confirm the plugin
mounted:

```bash
curl -s http://localhost:3000/api/harvest/health | jq .
```

---

[← Index](README.md) · [Next: Your first workflow and activity →](02-first-workflow.md)
