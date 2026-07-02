# Harvest Sharding Guide

## Overview

Harvest can spread workflow state across N independent Postgres databases (shards). A single workflow's event log, task queue rows, timers, signals, and DLQ entries all live on the same shard, so per-workflow ACID guarantees are preserved without cross-shard transactions.

For the full sharding architecture, see `CLAUDE.md` §Sharding and `autumn-harvest/src/shard.rs`.

---

## Per-Key Concurrency Limits and Sharding (issue #247)

### Limits are shard-local

Concurrency limits declared via `#[workflow(concurrency(key = "input.tenant_id", limit = 10))]` are enforced **within a single shard**. The claim query in `harvest_task_queue` counts `RUNNING` rows on the local shard only; it has no visibility into rows on other shards.

This means:

| Deployment | Effective global cap |
|---|---|
| Single-shard (default) | `limit` — enforced globally |
| Multi-shard (N shards) | Up to `limit × N` — each shard enforces `limit` independently |

### Achieving a true global cap in a multi-shard deployment

If your use-case requires a hard global cap across all shards (e.g., "at most 10 concurrent workflows for any tenant, regardless of which shard they land on"), route all executions for the same concurrency key to a single shard.

`ShardRouter::new` builds a rendezvous router keyed on `(workflow_name, workflow_id)`. To achieve tenant-pinned routing you would need a workflow ID naming convention that incorporates the tenant identifier — for example, always prefixing workflow IDs with the tenant: `"acme::order-42"`. Because rendezvous hashing is deterministic, all workflow IDs with the same prefix will not necessarily land on the same shard (the hash also mixes in `workflow_name`), so this approach only works reliably if you pin both `workflow_name` and the tenant-identifying part of `workflow_id`.

A fully custom shard-selection strategy is not yet exposed via `ShardRouter`; it is planned as a future API extension.

With tenant-consistent placement, every execution for `tenant_id = "acme"` lands on the same shard, so the local `limit` is also the global limit.

**Trade-off**: routing all of one tenant's workflows to the same shard concentrates load. Size shards to handle the worst-case tenant burst, or use rate limiting above Harvest to bound how fast new workflows can be started per tenant.

### Cross-shard global limits — explicit out of scope

Distributed counting across shards requires either a coordination service (Redis, a dedicated Postgres coordinator) or accepting bounded inaccuracy (approximate counts via gossip). Both add operational complexity that conflicts with Harvest's goal of being a Postgres-native engine. Cross-shard global limits are therefore **out of scope** for this feature; the per-shard guarantee is the contract.

If you need approximate cross-shard fair-share rather than a hard global cap, the metrics observable via `GET /admin/concurrency` can feed an external rate limiter in the layer above Harvest.

### Worker crash and slot release

When a worker crashes and its heartbeat times out, the `timeout.rs` scanner transitions the claimed task back to `PENDING` state. The concurrency cap check counts only `RUNNING` rows, so the slot is immediately available for another worker to claim. No operator intervention is required.

This is handled entirely within the shard where the task lives — no cross-shard coordination is needed for crash recovery.

---

## Operational Checklist

### Enabling per-key concurrency on an existing deployment

1. Deploy the binary with `#[workflow(concurrency(...))]` attributes.
2. Existing in-flight executions continue without any per-key cap — their task queue rows have `concurrency_key IS NULL` and the claim query skips the cap check for them.
3. New executions started after the deploy get the cap applied immediately.
4. No migration is required — the `concurrency_key` and `concurrency_cap` columns already exist in `harvest_task_queue` (added in migration `20260429000000_harvest_concurrency_key`).

### Observing per-key in-flight counts

The management API exposes live stats:

```
GET /admin/concurrency
```

Response:
```json
[
  {
    "key": "tenant:acme",
    "max_concurrent": 10,
    "in_flight": 7,
    "pending": 23
  }
]
```

The `pending` field shows how many tasks are currently deferred because the key is at or above its cap.

The `harvest.concurrency.in_flight` metric (tagged by concurrency key) is emitted by the concurrency sampler at regular intervals. **Note**: the raw key value is tagged on the metric — ensure your key cardinality is bounded (e.g., use `tenant_id`, not `execution_id`) to avoid metric cardinality explosion. See ADR-0001 §7.

### Historical per-tenant usage report (issue #596)

`GET /admin/concurrency` above is strictly **point-in-time**: it answers "how many tasks are in flight for this key right now?" It cannot answer the question every multi-tenant embedder eventually asks finance: "how much did tenant `acme` actually consume last month?" ADR-0001's cardinality rule deliberately keeps `MetricsRecorder` low-cardinality, so that answer cannot come from Prometheus either.

`GET /admin/usage` is the **historical companion** to `/admin/concurrency` — and the supported alternative to querying `harvest_workflow_executions` / `harvest_events` directly, which is an internal schema this project does not guarantee stability for. It aggregates already-durable data over a caller-supplied time window, computed read-only, with no new `WorkflowEvent` variant and no migration.

```
GET /admin/usage?from=2026-06-01T00:00:00Z&to=2026-07-01T00:00:00Z&group_by=search_attr:tenant_id
```

`from` and `to` are required (RFC 3339 timestamp or a relative duration like `24h`, measured back from now). `group_by` defaults to `workflow_name`; pass `search_attr:<key>` to bucket by a tenant key already carried in `search_attrs` (e.g. `search_attr:tenant_id`).

Response:
```json
{
  "status": "complete",
  "from": "2026-06-01T00:00:00Z",
  "to": "2026-07-01T00:00:00Z",
  "group_by": "search_attr:tenant_id",
  "groups": [
    {
      "group": "acme",
      "workflow_starts": 4210,
      "completed": 4102,
      "failed": 58,
      "cancelled": 12,
      "timed_out": 3,
      "activity_executions": 51820,
      "activity_executions_failed": 340,
      "activity_compute_seconds": 918422.7
    }
  ],
  "unavailable_shards": []
}
```

Executions that lack the requested `search_attrs` key are grouped under the literal group `"(unattributed)"` rather than silently dropped.

**Metric semantics** (each unit is counted exactly once, in the window it actually occurred — the chargeback-consistent choice):

- `workflow_starts`: executions whose `started_at` falls in `[from, to]`.
- `completed` / `failed` / `cancelled` / `timed_out`: executions whose `completed_at` falls in `[from, to]` and whose terminal state matches. `TERMINATED` and `CONTINUED_AS_NEW` are not broken out separately.
- `activity_executions`: count of dispatch attempts (`ActivityStarted` events) in the window — retries reuse the same activity id but each attempt appends a fresh event, so a 3-attempt activity contributes 3.
- `activity_executions_failed`: count of terminal `ActivityFailed`/`ActivityTimedOut` events in the window (non-final retry attempts emit no event, so this counts exhausted-retry-or-timeout only).
- `activity_compute_seconds`: for each activity whose terminal event falls in the window, the wall-clock span from that activity's most recent (final-attempt) start to its terminal event, summed. Retry backoff wall time is excluded by construction.
- Local activities and externally-completed activities are excluded from the activity counters — they never emit `ActivityStarted`, so they're not worker compute.

**Window ceiling**: a `from`/`to` window wider than a configurable ceiling (default 90 days, `HarvestApiState::set_usage_window_ceiling`) is rejected with `400`, naming the ceiling, so an operator cannot accidentally trigger a full-table scan across every shard.

**Shard-aware, no rollup**: like `/admin/concurrency`, `/admin/usage` fans out across every shard and merges. Unlike `GET /workflows/count`, it does **not** roll a long tail into an `other` bucket — a chargeback report that silently drops low-volume tenants would be actively wrong. Choose a bounded-cardinality `group_by` key (a tenant id, not an execution id).

CLI:
```
harvest usage --from 2026-06-01T00:00:00Z --to 2026-07-01T00:00:00Z --group-by search_attr:tenant_id
```
Renders a table by default; pass `--json` for piping.

### Adding a shard to a deployment that uses per-key concurrency

Follow the standard add-a-shard procedure in `CLAUDE.md`. The new shard starts with no task queue rows, so the cap is independent from day one. If you need to migrate in-flight workflows to the new shard, that is out of scope (cross-shard rebalancing is not supported).

---

## Debounce coordination is shard-local (issue #499)

Debounce pending-start records (`harvest_debounce`) are routed to the same shard as the debounce key (via `ShardRouter`), matching the per-key concurrency scope above. All burst admissions for the same `(workflow_name, debounce_key)` pair must land on the same shard for the `UNIQUE (workflow_name, debounce_key)` upsert collapse to work. Cross-shard global debounce coordination is **out of scope**: embedders requiring a global cap should ensure all executions for a given debounce key route to a single shard (see the concurrency routing guidance above).

---

## Cross-shard keyset pagination for `GET /workflows`

When `page_size` (or `cursor` / `order`) is present on a `GET /workflows` request, the engine performs a **k-way merge** across all shards so the caller sees a single globally-ordered result set without knowing which shard each execution lives on.

### Per-shard query

Each shard receives the same keyset predicate and fetches **`page_size + 1`** rows:

```sql
SELECT *
FROM   harvest_workflow_executions
WHERE  <state/workflow_name/time-range filters>
  AND  (created_at, id) < ($cursor_created_at, $cursor_id)  -- DESC direction
       -- or (created_at, id) > ($cursor_created_at, $cursor_id)  -- ASC direction
ORDER  BY created_at DESC, id DESC                           -- or ASC
LIMIT  $page_size + 1;
```

The `idx_harvest_we_created_id` index on `(created_at DESC, id DESC)` makes this an O(log n) index scan regardless of how deep into history the operator has paged.

### K-way merge

Results from all N shards are merged in memory by sorting on the same `(created_at DESC, id)` key. The merged list is then evaluated against the overflow probe:

- If `merged_len > page_size`: a next page exists. Truncate to `page_size`, encode the last kept row as `next_cursor`.
- If `merged_len <= page_size`: this is the last page. `next_cursor = null`.

### Row budget under N shards

Each shard contributes at most `page_size + 1` rows to the merge, so the total rows read across all shards is at most `N * (page_size + 1)`. With `page_size = 50` and `N = 4` shards that is at most 204 rows read to return 50. The full history is reachable across pages — no execution is ever skipped.

### Cursor correctness across shards

The cursor encodes a concrete `(created_at, id)` pair from the **global** top-`page_size` list, not a per-shard position. When that pair falls on shard 2, shards 0, 1, and 3 use the same `(created_at, id)` anchor to exclude rows they already returned — this is safe because `(created_at, id)` is globally unique across all shards (UUID `id` guarantees this). Inserting new rows mid-pagination on any shard does not cause duplicates or gaps: the keyset anchor is immutable once the cursor is issued.

### Filter interaction

`started_after` / `started_before` apply as `WHERE started_at` predicates on every shard before the keyset filter and ORDER BY, so both filters and pagination compose cleanly in a single per-shard query.

### Index

The additive migration `20260618000000_harvest_workflow_list_keyset_index` creates:

```sql
CREATE INDEX IF NOT EXISTS idx_harvest_we_created_id
    ON harvest_workflow_executions (created_at DESC, id DESC);
```

This index must be present on every shard for deep-page performance to remain flat. The migration is idempotent (`IF NOT EXISTS`) and runs automatically with `diesel migration run`.

## Cross-shard typed search-attribute predicates (`search_attr_filter`, issue #506)

The `search_attr_filter=key:op:value` predicates (comparison/set filtering over
`search_attrs`) are pushed down to **each shard's `WHERE` clause** and the
matching rows are k-way merged exactly as the keyset pagination path above — the
predicate is part of the shared per-shard query, so it composes with `state`,
time-range, `cursor`, `page_size`, and `order` with no extra round-trips and no
row duplicated or skipped under concurrent inserts.

### Per-shard SQL and index strategy

Every predicate stays on an index path rather than a full per-row scan by
reusing the existing `idx_harvest_we_search` GIN index
(`USING GIN (search_attrs)`, default `jsonb_ops`), which supports both `@>`
(containment) and `?` (key existence):

| `op` | Per-shard predicate | Index leg |
|------|--------------------|-----------|
| `eq` | `search_attrs @> '{"key": <typed>}'` | GIN `@>` |
| `ne` | `search_attrs ? 'key' AND NOT (search_attrs @> '{"key": <typed>}')` | GIN `?` narrows |
| `gt`/`gte`/`lt`/`lte` | `search_attrs ? 'key' AND jsonb_typeof(search_attrs -> 'key') = 'number' AND (search_attrs ->> 'key')::numeric <op> $val` | GIN `?` narrows, numeric recheck |
| `in` | `search_attrs ? 'key' AND search_attrs -> 'key' = ANY($vals::jsonb[])` | GIN `?` narrows |
| `exists` | `search_attrs ? 'key'` | GIN `?` |

The key is always a **bound** parameter (never string-interpolated), so dynamic
keys are injection-safe; the comparison operator literal comes from a fixed
internal enum. Because the existing GIN index already covers `@>` and `?`,
**no new index and no migration are required** — there is no column change to
`harvest_workflow_executions`, no `harvest_events` change, and no new
`WorkflowEvent` variant. This index must be present on every shard (it ships in
the initial `20260409000000_harvest_initial` migration).

### Typed coercion is shard-stable

The value→type coercion (number / boolean / string) is a pure function of the
predicate text, so every shard derives the identical typed comparison and the
merged result is deterministic. Numeric comparison matches only number-typed
stored values; a value stored as a string on one shard is excluded uniformly on
all shards, never a partial false match.

---

## Adding a Shard — Operational Runbook (issue #522)

Follow this procedure to add a new shard to a live deployment. Each step is safe to stop and retry.

### Step 1 — Provision and migrate

Provision a new Postgres database and run migrations against it:

```bash
DATABASE_URL=postgres://user:pass@new-shard-host/harvest diesel migration run
```

### Step 2 — Add to readable_shards

Add the new shard to `readable_shards` (but **not** `writable_shards`) and deploy. The router can now resolve IDs that encode the new shard; nothing writes there yet.

### Step 3 — Wait for readiness gate

Run the shard health check against the new shard:

```bash
harvest shard health --candidate-shard <shard_id>
# or
GET /admin/shards/health?candidate_shard=<shard_id>
```

Wait for `readiness: "ready"`. A `degraded` row includes machine-readable `reason_codes` explaining what is blocking readiness. Three codes are relevant here:

| `reason_code` | Meaning | Resolution |
|---|---|---|
| `no_live_worker` | The shard is `Writable` and has claimable tasks, but **no live worker** lists this shard in its `shard_assignments`. | Add the shard to each worker's `shard_assignments` config and redeploy. |
| `worker_queue_uncovered` | No healthy worker covers a required queue on this shard. | Same as above — check queue bindings. |
| `schema_migration_missing` | The shard is missing required migrations. | Re-run `diesel migration run` against the shard. |

The `no_live_worker` gate is the primary pre-flip readiness gate for issue #522: until at least one `Healthy + Active` worker lists the new shard in its `shard_assignments`, the shard will not report `ready`. This prevents silently stranding work on the new shard.

### Step 4 — Flip writable and verify

Add the new shard to `writable_shards` and deploy. The fleet **automatically drains the newly-writable shard** — no operator intervention is needed. Workers that list the shard in `shard_assignments` will claim and dispatch tasks from it within one `poll_interval`.

```toml
# Example worker config
[harvest.worker]
shard_assignments = [0, 1]   # worker now covers both shards
```

Once flipped:
- New workflows begin landing on the shard via rendezvous hash.
- In-flight workflows on existing shards continue draining through their own worker tasks.
- Workers assigned to both shards poll each shard's pool independently on every tick, preserving per-shard ACID locality.

### Observing stranded work

The `harvest.shard.stranded_pending` gauge (emitted by the stranded-work sampler on each worker) shows per-shard claimable task counts for shards that have **no live covering worker**:

```promql
harvest_shard_stranded_pending{shard="1"} > 0
```

A non-zero value means tasks are queued on that shard but no worker is draining them. Healthy steady state is `0` on all shards. Use this as an alerting signal: if it stays non-zero for more than `2 × poll_interval`, check `shard_assignments` on the running worker fleet.

### Pre-flip checklist

Before adding a shard to `writable_shards`:

- [ ] `GET /admin/shards/health?candidate_shard=<id>` returns `readiness: "ready"` for the new shard.
- [ ] No `no_live_worker` reason code present (at least one live worker covers the shard).
- [ ] `harvest.shard.stranded_pending{shard="<id>"}` is `0` (no backlog from prior test writes).
- [ ] Schema migrations are applied (`schema_migration_missing` absent).

A `readiness: "degraded"` result with `no_live_worker` in `reason_codes` is the engine's way of saying: "flip cancelled — no worker will claim work on this shard."
