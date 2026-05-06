# quickstart

A minimal runnable example for [autumn-harvest](../../README.md): one `#[workflow]`, one `#[activity]`, one durable timer.
From `git clone` on a machine with Docker to a running durable workflow in under 5 minutes.

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

The app starts on **http://localhost:8080**.  
The management API is at **http://localhost:8080/api/harvest**.  
The workflow dashboard is at **http://localhost:8080/api/harvest/ui**.

## Step 3 — Run preflight

```bash
cargo run -p autumn-harvest-cli -- --base-url http://localhost:8080/api/harvest preflight
```

## Step 4 — Trigger a workflow execution

```bash
curl -s -X POST http://localhost:8080/api/harvest/workflows/greeting/start \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"demo-1","input":"World"}' | jq .
```

The `greeting` workflow will:

1. Run the `send_greeting` activity — logs `welcome to World!`
2. **Pause for 30 seconds** on a durable timer
3. Run `send_greeting` again — logs `farewell to World!`
4. Complete with the final greeting string

## Step 5 — Observe in the dashboard

Open **http://localhost:8080/api/harvest/ui** in your browser to watch the execution progress through each step in real time.

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
