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

`reuse_policy` governs a **terminal** prior run. For a collision with a
currently **active** (RUNNING/PAUSED) prior, use the orthogonal `conflict_policy`
axis (issue #685) — see [Conflict policies](#conflict-policies) below.

## Conflict policies

`reuse_policy` decides what to do about a *terminal* prior; `conflict_policy`
decides what to do about a currently **active** (RUNNING/PAUSED) prior. The two
are independent — a start can, for example, replace a failed prior *and* attach
to a running one. Omitting `conflict_policy` (the default `unspecified`) keeps
each reuse policy's existing active behavior, so nothing changes for callers who
don't set it.

| Value | Effect on an active (RUNNING/PAUSED) prior |
|---|---|
| `unspecified` *(default)* | Each reuse policy's native active behavior (byte-for-byte identical to today). |
| `fail` | Return `409` — never touch or attach to the active prior. |
| `use_existing` | Attach: return the running execution (`200`), no new run, no cancel. |
| `terminate_existing` | Cancel the active prior and start fresh (`201`). Requires admin auth (can cancel a live run). |

```bash
# Start-or-attach a singleton entity workflow in one call: replace a terminal
# prior, but attach to (return) a still-running one — the idempotent-starter
# shape. This does NOT require admin: over an active prior it attaches
# (use_existing), and over a terminal prior it replaces an already-dead run.
curl -s -X POST http://localhost:3000/api/harvest/workflows/cart/start \
  -H 'content-type: application/json' \
  -d '{"workflow_id":"cart-42","input":{},
       "reuse_policy":"terminate_if_running","conflict_policy":"use_existing"}'
```

When the request sends a `conflict_policy` field (even `unspecified`), the
response includes `started_fresh` (`true` = a new run was created, `false` =
attached to the existing run).

**Admin auth is required iff the request can cancel a live run** — that is, when
the resolved active-prior behavior is *Terminate*: namely `conflict_policy =
terminate_existing`, or `reuse_policy = terminate_if_running` with the
default/omitted `conflict_policy`. The flagship idempotent-starter shape above
(`terminate_if_running` + `use_existing`) resolves to *attach* and provably
cannot cancel a live run, so it does **not** require admin — non-admin
webhook/cron callers can use it directly. `terminate_if_running` +
`fail` (resolves to `409`) is likewise non-admin.

`conflict_policy` is **not** supported combined with a throttle / debounce /
batch policy (the start is deferred and has no active prior to resolve at
request time) — that combination returns `400`.

> **Concurrency note.** Concurrent `terminate_existing` starts of the same
> `(workflow_name, workflow_id)` against one live prior are last-writer-wins and
> CONVERGE to a single surviving run via a bounded internal retry — no transient
> `NotFound` is surfaced (the loser waits for the winner to commit, then resolves
> against the current replacement row). It does not corrupt data, deadlock, or
> double-run.

For the full `reuse_policy` × `conflict_policy` matrix see the
"Standalone Start — Conflict Policy" section in `CLAUDE.md`.

---

[← Worker routing and capabilities](09-worker-routing.md) · [Index](README.md) · [Next: Testing your workflow code →](11-testing.md)
