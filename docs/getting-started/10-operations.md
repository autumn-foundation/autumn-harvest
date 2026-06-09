# Chapter 10 — Operating the service

[← Worker routing and capabilities](09-worker-routing.md) · [Index](README.md) · [Next: Testing your workflow code →](11-testing.md)

---

## Preflight

Before promoting a Harvest service, run the deploy gate:

```bash
cargo run -p autumn-harvest-cli -- \
  --base-url http://localhost:3000/api/harvest preflight
```

Exit codes are CI-friendly: `0 = pass`, `2 = warn`, `1 = fail`. The same
endpoint is available as `GET /api/harvest/admin/preflight` for release
scripts.

## Dashboard

`http://localhost:3000/api/harvest/ui` shows live executions, event histories,
the DLQ, schedules, and the worker fleet. It's served by the plugin — no
separate process.

## CLI

The `harvest` binary is a thin client for the management API:

```bash
harvest workflow list --state RUNNING
harvest workflow get <execution-id>
harvest workflow signal <execution-id> approved --payload-json '{"approved":true}'
harvest workflow cancel <execution-id> --reason "operator request"

harvest dlq list --limit 25
harvest dlq replay <dead-letter-id>

harvest concurrency status
```

It never talks to Postgres directly — every call goes through the API your
service already exposes, so auth and policy stay in one place.

## Dead letters

When a task exhausts its retry policy, it lands in `harvest_dead_letters` and
shows up on the DLQ tab. Inspect the failure context, then either replay
(`harvest dlq replay`) once you've fixed the root cause, or discard
(`harvest dlq bulk-discard --activity-name ...`) when the work is no longer
relevant.

## Worker fleet and graceful drain

Every worker process registers itself in `harvest_workers` and heartbeats on
a schedule. Inspect the fleet from the CLI:

```bash
harvest worker list                       # all workers
harvest worker list --status active --health stale
harvest worker get <worker-id>
harvest worker health                     # rollup: active / draining / stale
```

When you need to roll a node — deploy, autoscale-down, drain a host before
maintenance — request a remote drain instead of sending `SIGTERM`. The
worker stops claiming new tasks within two heartbeat intervals and finishes
its in-flight work before exiting:

```bash
# Dry run first: who would be affected, what's in-flight, on which shards.
harvest worker drain-preview --queue email-workers

# Then drain a specific worker, optionally with a deadline.
harvest worker drain <worker-id>
harvest worker drain <worker-id> --deadline 2026-05-08T15:00:00Z
```

The response echoes `outcome` (`accepted`, `already_draining`,
`already_stopped`, `stale_worker`, `not_found`), the in-flight task count,
the drain deadline, and which shards the worker owns. The same surface is
available over HTTP for orchestration systems:

```bash
curl -s -X POST http://localhost:3000/api/harvest/workers/<worker-id>/drain \
  -H 'Content-Type: application/json' \
  -d '{"deadline_at":"2026-05-08T15:00:00Z"}' | jq .

curl -s 'http://localhost:3000/api/harvest/workers/drain-preview?queue=email-workers' | jq .
```

Drain requests are recorded in the audit log under the `worker.drain`
operation, so you have a "who quiesced this node, when" record without
correlating shell history across machines.

## Reuse policies

By default, starting a workflow with an existing `(name, workflow_id)` pair
returns the existing execution — correct for retries of a lost-response
start. When you need stricter semantics, pass `reuse_policy`:

| Value | Use when… |
|---|---|
| `allow_duplicate` *(default)* | Upstream may retry a start whose response was lost. |
| `reject_duplicate` | At-most-one is a hard requirement; second start returns 409. |
| `allow_duplicate_failed_only` | Retry only if the prior run is FAILED/CANCELLED. |
| `terminate_if_running` | Cancel the prior run and start fresh. |

---

[← Worker routing and capabilities](09-worker-routing.md) · [Index](README.md) · [Next: Testing your workflow code →](11-testing.md)
