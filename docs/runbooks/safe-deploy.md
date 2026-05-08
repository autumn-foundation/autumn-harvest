# Runbook: Safe Worker Drain Before a Deploy

Use the Harvest drain controls to quiesce one or more workers gracefully before
deploying a new version or taking a host out of rotation. This avoids dropped
in-flight tasks and mid-execution interruptions without needing SSH or raw
process signals.

**When to use:** rolling deploys, host maintenance, canary rollbacks, scheduled
scale-in events.

**When _not_ to use:** emergency kills — use SIGKILL directly and let Harvest's
retry/timeout logic recover the in-flight tasks.

---

## Step 1 — Identify the target worker(s)

List all active workers to find the candidates:

```bash
harvest worker list --status Active
```

Filter by queue or shard when operating a large fleet:

```bash
harvest worker list --queue email-workers --shard-id 0
```

Key fields in the response:
- `worker_id` — the string passed to `--drain` below.
- `in_flight_count` — tasks currently executing on this worker.
- `status` — `Active`, `Draining`, or `Stopped`.
- `health` — `healthy` (heartbeat recent) or `stale` (heartbeat expired).

---

## Step 2 — Dry-run with drain-preview

Before draining, confirm which workers would be affected:

```bash
harvest worker drain-preview --queue email-workers
```

`drain-preview` is read-only and never changes any state. The response lists
every matching active worker with its current `in_flight_count`.

---

## Step 3 — Request the drain

Drain a specific worker:

```bash
harvest worker drain <worker-id>
```

The server sets the worker's status to `Draining`. The worker will finish its
current tasks and then transition to `Stopped` within one heartbeat interval
(default: 5 s) after quiescing.

To specify an explicit deadline (RFC 3339):

```bash
harvest worker drain <worker-id> --deadline 2026-05-09T14:30:00Z
```

When `--deadline` is omitted the server uses the configured
`WorkerConfig::shutdown_timeout` (default 30 s from the current time).

### Drain outcome codes

| `outcome`         | Meaning                                                     |
|-------------------|-------------------------------------------------------------|
| `accepted`        | Drain requested; worker will quiesce and stop.              |
| `already_draining`| Worker is already draining; deadline was refreshed.         |
| `already_stopped` | Worker has already stopped; no action taken.                |
| `stale_worker`    | Worker heartbeat is stale; drain was written but the process may already be gone. |
| `not_found`       | Worker ID not found on any shard.                           |

---

## Step 4 — Wait for the worker to stop

### Option A — CLI wait mode (recommended)

The `--wait` flag blocks until the worker reaches `Stopped` or the timeout
expires, polling every 2 s:

```bash
harvest worker drain <worker-id> --wait --wait-timeout-secs 120
```

Exits 0 when the worker stops, exits 1 on timeout.

### Option B — Manual polling

```bash
watch -n 2 'harvest worker get <worker-id> | jq .status'
```

Wait until `"status": "Stopped"` appears.

### Option C — Management API

```http
GET /workers/<worker-id>
```

Poll until `status == "Stopped"`.

---

## Step 5 — Terminate the process

Once the worker is `Stopped` you can safely send SIGTERM (or SIGKILL) to the
process, redeploy the binary, or decommission the host. No in-flight tasks will
be lost.

---

## Degraded mode: unavailable shards

When a shard is temporarily unreachable the drain response includes
`unavailable_shards: [<id>, ...]` and an `outcome` of `not_found`. The worker
_may_ live on an unavailable shard. Retry the drain once the shard recovers, or
use `GET /admin/shards/health` to investigate.

---

## Drain audit trail

Every `POST /workers/{id}/drain` call is recorded in the audit log:

```bash
harvest audit list --operation worker.drain --target-id <worker-id>
```

Audit fields include `actor`, `occurred_at`, `status` (`succeeded` / `failed`),
and `request_id`.

---

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Terminating the process before `Stopped` | Poll with `--wait` or `worker get` until status is `Stopped` |
| Forgetting `--deadline` on a slow worker | The default deadline is `shutdown_timeout` (30 s); set a longer deadline for workers with large in-flight batches |
| Draining the wrong shard | Use `--shard-id` with `drain-preview` to scope the preview first |
| Ignoring `unavailable_shards` in the response | The worker may be on an unreachable shard; retry after shard recovers |
