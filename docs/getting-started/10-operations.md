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

### Catching a forgotten registration before rollout

A `#[dag]` declares its activities structurally, so preflight already fails a
DAG that names an unregistered one. An **imperative** `#[workflow]` has no such
structure: `ctx.execute_activity(&send_email_info(), …)` compiles fine even when
`send_email` was never added to `activities![…]`, and the miss only surfaces at
runtime — one dispatch, one retry cycle, one dead letter later.

Opt in to the same deploy-time check by declaring what the workflow dispatches:

```rust
#[workflow(activities = [send_email, charge_card], children = [generate_report])]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> {
    ctx.execute_activity(&send_email_info(), user_id).await?;
    ctx.execute_activity(&charge_card_info(), user_id).await?;
    let _: Report = ctx.spawn_child_workflow(&generate_report_info(), user_id).await?;
    Ok(())
}
```

Read it as a cross-check against your registration lists: every name in
`activities = [..]` must appear in `activities![..]`, and every name in
`children = [..]` in `workflows![..]`. **The attribute never registers anything
— it only asserts.**

Entries are bare identifiers (a path like `billing::charge_card` also works —
only the last segment is used) or string literals for a name you dispatch
dynamically via `ctx.execute_activity_raw("…", …)`. The identifier is taken
literally and never name-resolved: an aliased import (`use send_email as
email_act;` + `activities = [email_act]`) records `"email_act"` and fails
preflight, and a typo stays a preflight failure rather than becoming a compile
error — which is the point. `activities` resolves against the registered
**activity** catalog, `children` against the registered **workflow** catalog;
the two namespaces are separate.

`children` is checked by name against the workflow catalog, so it also covers a
cross-type `continue_as_new_as` target — the other imperative workflow-type
reference preflight cannot otherwise see. Listing one turns "deleted a handler a
live run was about to continue into" from a runtime failure into a deploy-time
one. (The failure message still reads `child workflow`, matching the attribute
you wrote.)

Preflight then names every unresolved reference:

```console
$ harvest preflight
overall_status: fail
observed_at: 2026-08-14T09:12:00Z

STATUS  CHECK                SCOPE  SUMMARY
pass    database             -      database reachable
fail    catalog_consistency  -      registered catalog contains unresolved workflow runtime references

catalog_consistency (fail)
  - workflow 'onboarding' references unregistered activity 'send_emial'
  - workflow 'onboarding' references unregistered child workflow 'generate_reprot'
  remediation: Register the named handler in activities![…] / workflows![…], or — if the workflow no longer references it — delete the stale entry from its declared dependencies.
```

The same payload is available structurally for scripting:

```bash
harvest --output json preflight \
  | jq '.checks[] | select(.name == "catalog_consistency") | .details.failures'
```

The same builder is available without the macro, for a workflow registered by
hand:

```rust
.workflows(vec![
    onboarding_info()
        .with_declared_activities(&["send_email", "charge_card"])
        .with_declared_children(&["generate_report"]),
])
```

**This is opt-in, and silence means "not declared", not "verified".** A workflow
that never writes either attribute is never validated and can never fail
preflight because of it — adopt it workflow by workflow, at whatever pace suits
you. Declaring an empty list (`activities = []`) is different from declaring
nothing: it is an explicit "this workflow dispatches no activities", and is
checked (trivially) rather than skipped.

Two things it deliberately does *not* do. It does not read the workflow body, so
the declaration is a claim you maintain, not a fact the compiler derives — a
dependency you removed from the code but left in the list will fail preflight
until you delete it. And it checks only that the name is *registered somewhere
in this process* — so if you split registration across processes (the API
process registers workflows, a separate fleet registers the activity handlers),
declare only what *this* process registers. Whether a worker polling the right
queue is actually running is the separate `worker_health` / `queue_coverage`
question.

Declared dependencies are also surfaced per workflow type on
`GET /api/harvest/workflows/registered`, so a service can publish its dependency
graph without anyone reading Rust source.

## Migrations

In the default `embedded` mode, `HarvestPlugin` **registers** its migration sets
with Autumn rather than applying them itself, so they follow the same rules as
your app's own and every other plugin's. Autumn applies them during database
setup, which runs before any startup hook — the Harvest runtime therefore boots
against an already-migrated schema.

- **`dev` profile** — pending migrations are applied automatically at startup.
- **Every other profile** — pending migrations are *reported as warnings and not
  applied*. Run a one-shot `autumn migrate` in your deploy pipeline **before**
  rolling web replicas.

Registering rather than applying is also what lets Autumn resolve version
collisions between plugins: Diesel's `__diesel_schema_migrations` is keyed by
version alone, so two independently authored migrations that reuse a version
would otherwise silently skip one of them. Autumn sees every registered set at
once, tracks the loser under a substitute version so both still apply, and logs
it at `INFO`.

Two cases need a separate migration procedure because Autumn has no handle on
the target database:

- **`harvest.mode = "split"` / `"external"`** — `autumn migrate` applies only
  the application-database set (the workflow-start outbox). In non-`dev`
  profiles, the plugin only checks the dedicated Harvest database and warns
  about pending migrations; it does **not** apply them. Before rolling replicas,
  run `autumn migrate` for the application database and `harvest migrate run`
  for the one configured as `harvest.database.url`:

  ```bash
  autumn migrate

  export HARVEST_DATABASE_URL=postgres://…   # == harvest.database.url
  harvest migrate run
  ```

  `harvest migrate` connects to Postgres directly — no running app, no
  management API — and writes the same `__diesel_schema_migrations` ledger
  Autumn and Diesel use, so a migration is applied exactly once no matter which
  of them applies it. Harvest's own migrations are embedded in the `harvest`
  binary, so no source tree is needed. TLS DSNs work: `sslmode=require`,
  `verify-ca` and `verify-full` all connect, and the certificate chain and
  hostname are verified against the platform trust store in every case.

  Sets the binary does not embed are added with `--include-dir`. The one that
  matters in practice is the plugin's connector dead-letter table, needed
  before you enable a [broker connector](13-broker-connectors.md):

  ```bash
  harvest migrate run --include-dir autumn-harvest-plugin/migrations/harvest
  ```

  To gate a deploy without applying anything, `harvest migrate status --check`
  exits non-zero while any migration is still pending (and `harvest migrate run
  --dry-run` prints the same plan). Do not roll replicas if any command fails.
  See the [0.6.0 upgrade guide](../upgrading/0.6.0.md#split--external-mode-still-applies-its-own-harvest-migrations)
  for the ownership table and complete procedure.
- **Multi-shard deployments** — each Harvest shard database needs the full set
  applied; repeat `--database-url` once per shard. See
  [`sharding.md`](../sharding.md).

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

Almost every call goes through the API your service already exposes, so auth
and policy stay in one place. The exceptions are the commands that exist
precisely because no app is up to ask: `harvest migrate` (the schema step that
runs *before* replicas roll) and `harvest backup verify` (a restore drill
against scratch databases). Both take a DSN and talk to Postgres directly.

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
