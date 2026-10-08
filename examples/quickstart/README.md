# quickstart

A minimal runnable example for [autumn-harvest](../../README.md): one `#[workflow]`, one `#[activity]`, one durable timer.
From `git clone` on a machine with Docker to a running durable workflow in under 5 minutes.

> Only want to *see* a durable workflow run? `cargo dev` from the workspace root
> needs neither Docker nor a database — it provisions an ephemeral Postgres,
> serves the dashboard, and cleans up after itself. See
> [Chapter 1](../../docs/getting-started/01-project-skeleton.md). This example
> is the bring-your-own-Postgres path, and is unchanged.

## Prerequisites

- Stable Rust toolchain (`rustup default stable`)
- Docker (for Postgres via `compose.yaml`)

## Step 1 — Start Postgres

```bash
docker compose -f examples/quickstart/compose.yaml up -d
```

Wait a few seconds for Postgres to become healthy (`docker compose -f examples/quickstart/compose.yaml ps`).

## Step 2 — Start the app

Run from the workspace root. `AUTUMN_PROFILE=dev` enables automatic migration application on startup.

```bash
AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p quickstart
```

The app starts on **http://localhost:3000**.  
The management API is at **http://localhost:3000/api/harvest**.  
The workflow dashboard is at **http://localhost:3000/api/harvest/ui**.

## Step 3 — Run preflight

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:3000/api/harvest preflight
```

Preflight reads the management API's deployment-readiness report — migrations,
shard read/write availability, catalog and schedule resolvability, worker queue
coverage, DLQ access. Exit code `0` is `pass`, `2` is `warn`, `1` is `fail`.

No credential is needed here because `AUTUMN_PROFILE=dev` (Step 2) serves the
management API unauthenticated to any local caller — which is also why the app
above logs a warning saying so at startup. Every other profile is fail-closed:
see [Deployment preflight](../../README.md#deployment-preflight) in the
top-level README for how to authenticate the same command against a real
deployment.

## Step 4 — Trigger a workflow execution

```bash
curl -s -X POST http://localhost:3000/api/harvest/workflows/greeting/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"demo-1","input":"World"}' | jq .
```

The `greeting` workflow will:

1. Run the `send_greeting` activity — logs `welcome to World!`
2. **Pause for 30 seconds** on a durable timer
3. Run `send_greeting` again — logs `farewell to World!`
4. Complete with the final greeting string

## Step 5 — Observe in the dashboard

Open **http://localhost:3000/api/harvest/ui** in your browser to watch the execution progress through each step in real time.

## The durability promise: Kill it and restart

The 30-second timer exists so you can observe the engine's durability guarantee directly:

```bash
# While the workflow is paused on the timer (between steps 1 and 3 above),
# press Ctrl+C to stop the process, then immediately restart it:
AUTUMN_MANIFEST_DIR=examples/quickstart AUTUMN_PROFILE=dev cargo run -p quickstart

# The engine replays the welcome step from Postgres event history and resumes
# waiting on the remaining timer — without re-executing the activity.
# After the timer elapses, the farewell step runs and the workflow completes.
```

Check the dashboard to confirm the workflow reaches the `COMPLETED` state after the restart, with the full event history intact.

## Step 6 — Tear down

```bash
docker compose -f examples/quickstart/compose.yaml down -v
```
