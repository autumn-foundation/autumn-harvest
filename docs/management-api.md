# Management API

This document covers the HTTP management API mounted by `autumn-harvest-plugin`.

## Full route registry

This page is a **selective deep-dive** into a few high-traffic surfaces (SSE
streaming, by-id addressing, listing, stack/describe, history pagination,
updates, signal delivery), not an exhaustive endpoint list. The **authoritative,
machine-readable registry of every route** — method, path, auth class, request
and response fields — is [`docs/api-contract.json`](api-contract.json) (see
[`api-contract-guide.md`](api-contract-guide.md) for how to consume it).

New route families added in **0.5.0** (all in the contract; each has a CLI verb):

- `GET /workflows/summaries` — tiered-retention summaries of expired runs (#752).
- `GET /workflows/count` — grouped RUNNING/FAILED-per-type fleet snapshot (#544).
- `GET /workflows/{id}/run-chain` — the ordered continue-as-new succession (#701).
- `GET /workflows/{id}/timeline` — per-run wall-clock breakdown from history (#739).
- `POST /workflows/{id}/legal-hold` / `.../legal-hold/release` — per-execution retention/erase hold (#747).
- `POST /workflows/{id}/activities/{activity_exec_id}/fail-now` — force-fail one hung in-flight activity (#765).
- `GET /workflows/{id}/completion-deliveries` + `.../redrive` — durable completion-callback deliveries (#605).
- `PATCH /admin/schedules/{id}` — in-place cron/input edit without recreate (#771).
- `GET /admin/status` — one-call rolled-up health verdict (#679).
- `GET /admin/config` — redacted effective runtime config (#695).
- `GET /admin/usage` — per-tenant/per-workflow historical usage report (#596).
- `GET /admin/workflow-types/reachability` — safe-handler-removal pre-flight (#520).
- `GET /dags/{dag_name}/runs/{run_exec_id}` — DAG run graph view (#690).

## SSE Execution Event Stream

### Endpoint

```
GET /executions/{exec_id}/events/stream
```

**Auth**: same bearer-token gate as all other management API routes (issue #174). Returns `401` if the token is missing or invalid.

**Content-Type**: `text/event-stream`

**Category**: read-only; classified as `ReadOnly` in the audit system.

---

### Wire format

Each event arrives as a standard SSE block:

```
id: <harvest_events.id BIGSERIAL>
event: <WorkflowEvent type name>
data: <JSON event payload>

```

- `id` is the row-level `BIGSERIAL` primary key of `harvest_events`, **not** the per-execution sequential event ID. It is monotonically increasing across all executions on the shard and is safe to use as a resume cursor.
- `event` is the adjacently-tagged type string (`ActivityScheduled`, `SignalReceived`, etc.). New `WorkflowEvent` variants land in the stream automatically without client changes.
- `data` is the `data` inner object of the adjacently-tagged JSON envelope `{"type":"…","data":{…}}` stored in `harvest_events.event_data`.

#### Keepalive

Between events the server emits an SSE comment line at the configured interval (default 15 s):

```
: ping

```

Keepalive comments prevent reverse proxies and load balancers from killing idle connections. They carry no semantic content and should be discarded by clients.

#### Terminal marker

When the execution reaches a terminal state (`Completed`, `Failed`, `Cancelled`, `TimedOut`, `ResetTerminated`), the server sends a final event block then closes the stream:

```
id: <last_row_id>
event: stream-end
data: {"reason":"completed","execution_id":"<exec_id>","state":"COMPLETED"}

```

The `reason` field is the execution's terminal state, lowercased and hyphenated — one of `completed`, `failed`, `cancelled`, `timed-out`, `terminated`, `continued-as-new`. It is derived from the authoritative `harvest_workflow_executions.state` column (not the event type), so a force-terminated run (issue #504) reports `terminated` even though it reuses the `WorkflowCancelled` event. Receiving `event: stream-end` is the client's signal to **stop reconnecting** — the execution will not produce more events. Contrast this with a transport drop, where the client should reconnect with `Last-Event-ID`.

#### Slow consumer

If the client cannot drain the server-side buffer (default depth 1024 events) before it fills, the server sends a final `event: stream-error` block and closes the stream with HTTP 409:

```
event: stream-error
data: {"error":"slow_consumer","drop_after_event_id":<n>}

```

The client may reconnect immediately with `Last-Event-ID: <n>` to resume from where the stream was cut.

---

### Resume protocol (`Last-Event-ID`)

The browser `EventSource` API sends `Last-Event-ID` automatically on reconnect. Curl and custom clients must set it explicitly.

When the server receives `Last-Event-ID: <n>`:

1. It queries `harvest_events` for all rows with `id > n` for this execution (the backfill).
2. It sends the backfill rows over the stream in ascending `id` order.
3. It then enters live-tail mode, forwarding new events via LISTEN/NOTIFY.

A client that drops mid-stream and reconnects with the last `id` it saw will receive every event exactly once with no gaps.

**First connection** (no `Last-Event-ID`): the server starts from the beginning — all existing events are backfilled, then live-tail begins.

---

### Sharding

The endpoint resolves `exec_id.shard()` and subscribes to that shard's Postgres LISTEN/NOTIFY channel. Cross-shard fan-out is not required in v1; one stream always maps to one shard.

---

### Concurrency model

The server holds **no database connection while idle**. Each LISTEN/NOTIFY notification triggers a short-lived pooled connection to load new events, which is released immediately after the batch is sent. A single harvest process can sustain ≥ 1,000 concurrent SSE streams within the configured worker DB pool ceiling (DD-2).

---

### Audit trail

Opening a stream records one `execution.stream.open` audit entry with:
- `actor`: extracted from the auth context
- `exec_id`: the target execution
- `last_event_id_seen`: the cursor value from `Last-Event-ID` (or `-1` for a fresh stream)

Per-event audit entries are **not** written — that would amplify writes by 1:N. Stream close is audited as `execution.stream.close` when the producer task exits.

---

### Working `EventSource` example (browser)

```javascript
const execId = '00000000-0000-0000-0000-000000000001';
const token  = 'your-mgmt-token';

// EventSource does not support custom headers natively in all browsers.
// Use a query-param token or an initial cookie exchange instead.
// For internal tools, a ServiceWorker or fetch-based polyfill handles auth headers.
const url = `/api/harvest/executions/${execId}/events/stream`;

const es = new EventSource(url);

es.addEventListener('message', (e) => {
  const payload = JSON.parse(e.data);
  console.log('event id', e.lastEventId, payload);
});

es.addEventListener('stream-end', (e) => {
  const { reason, state } = JSON.parse(e.data);
  console.log(`execution finished: ${state} (${reason})`);
  es.close();          // stop reconnecting — execution is terminal
});

es.addEventListener('stream-error', (e) => {
  const { drop_after_event_id } = JSON.parse(e.data);
  console.warn(`slow consumer; resume from ${drop_after_event_id}`);
  es.close();
  // reconnect logic: open a new EventSource and the browser will send
  // Last-Event-ID: <drop_after_event_id> automatically.
});

es.onerror = (e) => {
  // transport error or server 4xx/5xx — browser will retry with Last-Event-ID
  console.error('SSE error', e);
};
```

---

### CLI usage

```bash
# Tail a live execution (Ctrl-C to stop)
harvest events tail <exec_id>

# Resume from a known cursor (skips events already seen)
harvest events tail <exec_id> --last-event-id 4200
```

Each event prints as `<event-type>: <json-data>` to stdout, one per line.
Keepalive comments are discarded silently.
The command exits cleanly when the server sends `event: stream-end`.

---

### Reverse-proxy and CDN notes

| Environment | Required config |
|-------------|----------------|
| **nginx** | `proxy_set_header X-Accel-Buffering no;` on the upstream location. Without this, nginx buffers the response and the client sees no events until the buffer fills. |
| **Cloudflare** | No special config required since 2024; Cloudflare proxies SSE transparently. Enterprise plan: disable `rocket_loader` on the management API path if enabled. |
| **AWS ALB** | Set `idle_timeout` ≥ 65 s. The SSE keepalive fires every 15 s (default), which keeps ALB's 60 s default from killing idle connections. |
| **HAProxy** | Set `timeout tunnel` on the backend section (e.g. `timeout tunnel 10m`). Without it, HAProxy applies the shorter `timeout server` to streaming connections. |
| **Traefik** | No special config required. Traefik detects `text/event-stream` and disables response buffering automatically. |

**Important**: SSE requires HTTP/1.1 or HTTP/2. Ensure the proxy does not downgrade the connection to HTTP/1.0, which does not support chunked transfer encoding.

---

## Addressing workflows by business id (`/workflows/by-id/...`)

Embedders assign their own business `workflow_id` at start (e.g. `order-12345`,
`subscription:user-42`). The act-on-existing management routes accept a
**business-id form** addressed by `(workflow_name, workflow_id)` in addition to
the internal `exec_id` form. This removes the list-then-act round-trip and,
critically, always reaches the **current** run — a cached `exec_id` becomes a
stale handle after a continue-as-new or reset fork, which mint a **new**
`exec_id` under the **same** `workflow_id` (issue #805).

The existing `exec_id` routes are unchanged and remain fully supported.

### Routes

All routes are prefixed `P = /workflows/by-id/{workflow_name}/{workflow_id}`:

| Method | Path | Delegates to | Auth |
|--------|------|--------------|------|
| `GET`  | `P` | `GET /workflows/{id}` (describe) | read-only |
| `GET`  | `P/result` | `GET /workflows/{id}/result` | read-only |
| `GET`  | `P/stack` | `GET /workflows/{id}/stack` | read-only |
| `GET`  | `P/children` | `GET /workflows/{id}/children` | read-only |
| `POST` | `P/signal/{signal_name}` | `POST /workflows/{id}/signal/{signal_name}` | audited |
| `GET`  | `P/query/{query_name}` | `GET /workflows/{id}/query/{query_name}` | read-only |
| `POST` | `P/query/{query_name}` | `POST /workflows/{id}/query/{query_name}` | read-only |
| `POST` | `P/cancel` | `POST /workflows/{id}/cancel` | **admin** |
| `POST` | `P/pause` | `POST /workflows/{id}/pause` | **admin** |
| `POST` | `P/resume` | `POST /workflows/{id}/resume` | **admin** |

`workflow_name` is **required** — the uniqueness scope is
`(workflow_name, workflow_id)`, not `workflow_id` alone. Both path segments are
mandatory, so a bare `workflow_id` is not addressable (a request omitting either
segment does not match a by-id route). The business-id routes reuse the exact
admin middleware and audit classification of their `exec_id` counterparts.

### Resolution rule

Given `(workflow_name, workflow_id)`, the resolver returns:

1. the single **active** (non-terminal, i.e. `RUNNING`/`PAUSED`) run if one
   exists; otherwise
2. the **most recent terminal** run (by `started_at`); otherwise
3. `404` (never `500`).

This is well-defined because at most one active run can exist per
`(workflow_name, workflow_id)` (the per-shard partial unique index). Resolution
is **read-only** and **shard-aware**: it fans out across every shard and merges
the per-shard best candidates, so a run that lives on a shard the rendezvous
hash no longer points at (writable-subset drift) is still found. On the
single-shard default this is exactly one query.

#### Multi-shard degraded mode — correctness over availability

If any expected shard cannot be queried — no configured storage pool in this
process (mid a shard-add rollout) or an unreachable database — by-id resolution
returns **`503`**, never a false `404`. Because the run could live on the
un-queried shard, silently skipping it and answering `404` would be a false
negative; the resolver instead fails closed. This is a deliberate
correctness-over-availability trade-off scoped to the by-id routes: if one shard
is down, by-id resolution errors `503` fleet-wide rather than risk targeting the
wrong run (or falsely reporting "no such run"). The `exec_id` routes are
**unaffected** — an `exec_id` encodes its owning shard, so an exec-id request
targets exactly one shard and is not gated on the health of the others. A
fast-path-shard-first optimization (try the rendezvous-hash shard, fan out only
on a miss) is a possible future refinement and is out of scope here. On the
single-shard default this `503` path never triggers.

### The resolved `execution_id` is always returned

Every business-id response includes the resolved `execution_id`, so a caller may
pin to a specific run if it wants:

- **In the `X-Harvest-Execution-Id` response header** on *every* response
  (including empty-body cases like `/result`'s `204`).
- **In the JSON body** wherever one exists: `signal` and `query` responses are
  re-wrapped to include a top-level `execution_id`; `cancel`/`pause`/`resume`
  already carry it; `describe`/`stack` carry the id nested in their existing
  body. `/result` and `/children` surface it via the header.

### GET query response shape differs from the exec-id form

The by-id `GET P/query/{query_name}` returns the handler value **wrapped** as
`{ "execution_id": …, "result": <value> }` so the resolved run is always
identifiable, whereas the exec-id `GET /workflows/{id}/query/{query_name}`
returns the raw handler value directly. This is a deliberate shape difference.
The `POST` query form already wraps as `{ "result": … }` on the exec-id surface;
the by-id `POST` form adds `execution_id` to that (`{ "execution_id": …,
"result": … }`).

### Continue-as-new / reset note

After a continue-as-new (or a reset fork), the workflow keeps the **same**
business `workflow_id` but the engine mints a **new** `exec_id`. A caller that
cached the old `exec_id` and calls `POST /workflows/{old_exec_id}/signal/...`
would signal the sealed predecessor. The business-id form always resolves to the
**live successor** (the active run), so `POST P/signal/...` reaches the current
run 100% of the time across a fork.

**Resolve-then-act is a best-effort snapshot.** A by-id mutate resolves
`(workflow_name, workflow_id)` to an `exec_id` and then delegates to the exec-id
handler as two steps. Between resolution and the delegated action the run can
transition (a continue-as-new or reset fork mints a new run), so a by-id mutate
targets the run that was live **at resolution time**. The "always the live run"
guarantee therefore holds *at resolution*, not across the resolve→act window —
which is inherently narrow (a single in-process delegation), but a caller that
needs a hard guarantee against a concurrent fork should pin to the returned
`execution_id`.

### Security note — business ids are guessable

Unlike opaque `exec_id` UUIDs, the by-id read and signal routes are addressable
by human-meaningful `(workflow_name, workflow_id)` pairs (e.g.
`order_flow/order-12345`), which are frequently sequential or otherwise
predictable. These routes reuse their exec-id counterparts' auth posture
exactly: **read** (describe/result/stack/children/query) and **signal** are
*not* admin-gated, while **cancel/pause/resume** are admin-only. Because the
guessability of business ids removes the unguessable-`exec_id` defense-in-depth,
mount the harvest management API **behind your own auth boundary** (e.g.
`api_with_auth` / your app's authenticated admin surface) rather than relying on
id opacity to gate read/signal access.

### Examples

```bash
# Signal the current run of order-12345 by business id (no lookup first):
curl -X POST "$BASE/workflows/by-id/order_flow/order-12345/signal/approve" \
  -H 'content-type: application/json' -d '{"approved_by":"alice"}'
# → 202 { "execution_id": "…", "ok": true, "signal_delivered": true }
#   + header  X-Harvest-Execution-Id: <resolved exec_id>

# Describe the current run:
curl "$BASE/workflows/by-id/order_flow/order-12345"

# Cancel the current run (admin):
curl -X POST "$BASE/workflows/by-id/order_flow/order-12345/cancel" \
  -H 'x-harvest-admin: true' -H 'content-type: application/json' -d '{"reason":"duplicate"}'

# Unknown (name, id) → 404 (never 500):
curl -i "$BASE/workflows/by-id/order_flow/does-not-exist"   # HTTP 404
```

## Listing workflows (`GET /workflows`)

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `workflow_name` | string | Filter to a single workflow type by name |
| `workflow_id` | string | Filter by logical workflow ID |
| `state` | string | Filter by execution state (e.g. `RUNNING`, `COMPLETED`) |
| `limit` | integer | Maximum rows to return (default 200, max 200) |
| `search_attr` | repeated | `key:value` pairs against `search_attrs` JSONB (exact-match equality; value is always a string) |
| `search_attr_filter` | repeated | Typed comparison/set predicate `key:op:value` against `search_attrs` JSONB. See [Typed search-attribute predicates](#typed-search-attribute-predicates-getworkflows). |
| `no_progress_minutes` | integer | Return stalled workflows with no task activity for N minutes |
| `sla_breached` | bool | Filter to executions that have breached their SLA |
| `page_size` | integer | Per-page limit for keyset pagination (1–200; overrides `limit`). Presence activates the opt-in paginated envelope. |
| `cursor` | string | Opaque continuation token returned by a previous page's `next_cursor`. Presence activates the opt-in paginated envelope. |
| `order` | `asc` \| `desc` | Walk order on `(created_at, id)`. Default `desc` (newest first). Presence activates the opt-in paginated envelope. |
| `started_after` | RFC 3339 | Return only executions whose `started_at` is after this timestamp |
| `started_before` | RFC 3339 | Return only executions whose `started_at` is before this timestamp |
| `exec_id_prefix` | string | Return only executions whose string-formatted `exec_id` starts with this prefix |

### Response shape — opt-in envelope

When **none** of `page_size`, `cursor`, or `order` are present the endpoint returns a bare JSON array, identical to the pre-pagination behaviour:

```json
[ { "workflow_id": "...", "state": "RUNNING", ... }, ... ]
```

When **any** of `page_size`, `cursor`, or `order` are present the endpoint returns a paginated envelope:

```json
{
  "workflows": [ { "workflow_id": "...", "state": "RUNNING", ... }, ... ],
  "next_cursor": "<opaque string or null>"
}
```

`next_cursor` is `null` on the last page. Pass it verbatim as the `cursor` query parameter to fetch the next page.

### Keyset cursor semantics

Pagination is keyset-based over `(created_at, id)`:

- The cursor encodes the `(created_at, id)` values of the **last row returned** on the previous page.
- Each subsequent page request adds `WHERE (created_at, id) < cursor` (for `order=desc`) or `WHERE (created_at, id) > cursor` (for `order=asc`).
- The cursor is opaque — do not parse or construct it manually.
- Inserting new rows while paginating does not cause duplicates or skips: the keyset predicate anchors each page to a stable position in the index.

### `order` parameter contract

| Value | Walk direction | Use case |
|-------|---------------|----------|
| `desc` (default) | newest first | Browsing the live queue; monitoring dashboards |
| `asc` | oldest first | Draining a backlog in FIFO order; catch-up processing |

`order=asc` combined with `page_size` and `next_cursor` walks from the oldest execution forward.

### Combining with time-range filters

`started_after` / `started_before` filter on the `started_at` column (when the execution began), while the cursor/sort key is `created_at` (when the row was inserted). The two are closely related but not identical (a workflow can be inserted and queued before it starts). You can combine them freely.

### Example — paginate through all running workflows in pages of 50

```bash
# First page
curl "/workflows?state=RUNNING&page_size=50"
# Response: { "workflows": [...50 rows...], "next_cursor": "abc123" }

# Second page
curl "/workflows?state=RUNNING&page_size=50&cursor=abc123"

# Keep following next_cursor until it is null
```

### Notes

- The `no_progress_minutes` (stalled-workflow) filter always returns a bare array regardless of pagination parameters (cursor pagination is not supported on this path). The `state`, time-range, `search_attr`, and `search_attr_filter` filters **are** applied on the stalled path, so they compose with `no_progress_minutes`.
- `page_size` and `limit` are aliases; if both are present `page_size` wins.
- On a sharded deployment each shard is queried independently with the same cursor and `page_size+1` limit; the results are k-way merged and truncated to `page_size` before the response is returned. See `docs/sharding.md` for the cross-shard keyset contract.

### Typed search-attribute predicates (`GET /workflows`)

> Issue #506. The `search_attr_filter` param adds typed comparison/set filtering
> over search attributes. The legacy `search_attr=key:value` param is unchanged
> (exact-match string equality) and stays fully backward compatible.

Each `search_attr_filter` value has the form `key:op:value`. The param is
repeatable; multiple predicates are combined with **AND** (matching the repeated
`search_attr` semantics). Disjunction (OR) is out of scope in this slice.

| `op` | Value | Meaning |
|------|-------|---------|
| `eq` | scalar | typed equality — `amount:eq:100` matches numeric `100`, not the string `"100"` |
| `ne` | scalar | key present **and** typed value differs |
| `gt` `gte` `lt` `lte` | number | numeric comparison; the value must parse as a number |
| `in` | comma list | membership in a typed set, e.g. `phase:in:blocked,awaiting_approval` |
| `exists` | *(none)* | key is present with any value |

**Typed coercion (documented, explicit).** The value's text drives the type:

- A value that parses as a JSON number is compared **numerically**, so
  `amount:gt:20` returns `amount=100` (numeric ordering), not a lexical
  `"100" < "20"` false negative, and never a string-vs-number false positive.
- The exact literals `true` / `false` compare as **booleans**.
- Everything else compares as a **string**.

For `eq` / `ne` / `in` the coerced value is matched against the stored JSONB
value **by type** (so `phase:eq:blocked` only matches string-typed `"blocked"`,
and `retry_count:eq:3` only matches number-typed `3`).

**Comparison ops are numeric-only.** `gt` / `gte` / `lt` / `lte` require the
value to parse as a number — a non-numeric value (e.g. `amount:gt:lots`) returns
`400`. Numeric comparison matches only rows whose stored attribute is itself
number-typed; a row whose attribute is stored as a string is **excluded** from a
numeric comparison (it is a different type), never a false match.

**Top-level keys only.** Filtering operates on top-level search-attribute keys
(the shape #159 writes). A nested `.`-path (e.g. `a.b:eq:1`) is rejected `400`.

**Error handling.** Malformed predicates — unknown op, missing value where
required, non-numeric value for a comparison op, nested `.`-path, empty key,
value supplied to `exists` — return `400` with a message naming the offending
`search_attr_filter` input. Unknown query params remain ignored (non-breaking).

**Index path.** Every predicate stays on an index path rather than a full
per-row scan: `eq`/`in`/`exists` and the existence-narrowing leg of `ne` and the
comparison ops all hit the existing `idx_harvest_we_search` GIN index
(`USING GIN (search_attrs)`, `jsonb_ops`) via the `@>` (containment) and `?`
(key-existence) operators; comparison ops then recheck the numeric cast on the
narrowed candidate set. No new index or migration is required. See
`docs/sharding.md` for the cross-shard pushdown contract.

**Composition.** `search_attr_filter` composes with `state`, time-range,
`cursor`, `page_size`, and `order` — every predicate is pushed down to each
shard's `WHERE` clause and the matching rows are k-way merged exactly as the
keyset pagination path, with no row duplicated or skipped under concurrent
inserts.

```bash
# Numeric range + set, paginated, across shards:
curl "/workflows?search_attr_filter=amount:gt:10000\
&search_attr_filter=phase:in:blocked,awaiting_approval&page_size=50"

# Retry cohort intersected with a business phase:
curl "/workflows?search_attr_filter=retry_count:gte:3&search_attr_filter=phase:eq:blocked"

# Presence check:
curl "/workflows?search_attr_filter=phase:exists"

# Rejected (400) — non-numeric value for a numeric op:
curl "/workflows?search_attr_filter=amount:gt:lots"
```

The `harvest workflow list` CLI accepts the same syntax via the repeatable
`--search-attr-filter key:op:value` flag (forwarded verbatim to this param). The
embedded Vantage UI workflows list defers to the equality-only `search_attr`
filter in this slice; a predicate-aware UI form is a follow-up.

## Workflow Stack (describe)

### Endpoint

```
GET /workflows/{exec_id}/stack
```

Returns a point-in-time "describe" view of an in-flight workflow: its pending
activities, local activities, external handoffs, timers, signals, buffered
signals, and child workflows. This is the primary view for triaging a running
execution. For a terminal execution the pending arrays are empty.

### Pending activity heartbeat checkpoint (issue #503)

Each entry in `pending_activities[]` surfaces the latest **heartbeat checkpoint
payload** the activity reported via `ctx.heartbeat(...)` — the current value of
`harvest_task_queue.heartbeat_details` for that task row, alongside the existing
`last_heartbeat_at` timestamp. This lets an operator answer "how far has this
in-flight activity progressed?" (e.g. `{"processed": 4500, "total": 10000}`)
from a single API call, with zero direct database access.

| Field | Type | Meaning |
|-------|------|---------|
| `last_heartbeat_at` | timestamp \| null | When the most recent heartbeat was flushed. |
| `heartbeat_details` | JSON \| null | The latest checkpoint payload, verbatim as the activity reported it. `null` when no heartbeat has been flushed, when the payload exceeded its own cap (`heartbeat_details_truncated`), or when it was withheld by the per-response budget (`heartbeat_details_omitted_for_budget`). |
| `heartbeat_details_truncated` | bool | `true` when **this payload's own** size exceeded the activity's effective result-payload cap (issue #252) and was withheld. |
| `heartbeat_details_omitted_for_budget` | bool | `true` when the payload was within its own cap but withheld because the cumulative per-response checkpoint budget was already exhausted by earlier activities (see the response-level `checkpoints_truncated_for_budget`). |
| `heartbeat_details_bytes` | integer \| null | Observed serialized byte size of the stored payload, when one exists. Reported even when the payload is withheld (by either guard). |

The top-level response also carries `checkpoints_truncated_for_budget` (bool): `true` when one or more checkpoints were withheld by the per-response budget.

Notes:

- **Regular activities only.** Local activities cannot heartbeat
  (`ctx.heartbeat(...)` returns a `Config` error), so `pending_local_activities[]`
  has no heartbeat checkpoint field.
- **Read-only.** Surfacing the checkpoint never writes, clears, or alters the
  column, and never affects retry-resume semantics
  (`ActivityContext::heartbeat_details::<T>()` reads the same value unchanged).
- **Shard-correct.** The handler routes by the execution's encoded shard; the
  heartbeat column lives on the same shard as the task row, so no cross-shard
  fan-out is introduced.
- **Size-bounded (per checkpoint).** Each checkpoint is judged against the
  activity's effective result-payload cap rather than a new limit: the
  per-activity `max_result_bytes` override raised against the global ceiling
  (`override.max(global)`, default global **2 MiB**), matching the worker. An
  activity configured to allow large results keeps full checkpoint visibility,
  while a pathological payload still cannot bloat the describe response.
- **Size-bounded (per response).** A cumulative budget — the global
  activity-result cap (**2 MiB**) — bounds the *total* checkpoint bytes across
  all pending activities, so a large fan-out cannot return roughly
  `count × cap` bytes. Checkpoints are kept in task order until the budget is
  reached; the first payload-bearing checkpoint is always kept (so a single
  legitimately large checkpoint stays visible), and later ones are withheld with
  `heartbeat_details_omitted_for_budget: true`.

### Example

```bash
curl "/api/harvest/workflows/$EXEC_ID/stack"
```

```json
{
  "exec_id": "…",
  "workflow_name": "etl_pipeline",
  "state": "RUNNING",
  "checkpoints_truncated_for_budget": false,
  "pending_activities": [
    {
      "activity_name": "process_batch",
      "task_status": "RUNNING",
      "last_heartbeat_at": "2026-05-30T19:00:04Z",
      "heartbeat_details": { "processed": 4500, "total": 10000 },
      "heartbeat_details_truncated": false,
      "heartbeat_details_omitted_for_budget": false,
      "heartbeat_details_bytes": 31
    }
  ]
}
```

## Single-execution history (paginated)

### Endpoint

```
GET /workflows/{id}/history
```

Returns a bounded, paginated, filterable view of a single execution's event log.
Use this endpoint to page through history incrementally for long-running executions,
or to filter events by type during incident triage.

### Parameters

| Parameter | In | Required | Description |
|-----------|----|----------|-------------|
| `id` | path | yes | Execution UUID. |
| `limit` | query | no | Maximum events per page (1–1000). Default: **100**. Values above 1000 are silently clamped to 1000. |
| `after` | query | no | Exclusive cursor anchor. Opaque — treat as a string; absent means start from the first event. |
| `event_type` | query | no | Repeatable. Filter to matching event type discriminators (e.g. `TimerStarted`, `ActivityCompleted`). Unknown type names yield an empty events array, not a 400. |

### Cursor semantics

- The cursor is the decimal string of `harvest_events.id` (a BIGSERIAL integer).
- Pagination is exclusive: `after=N` returns rows with `id > N`, in ascending order.
- Absent `next_cursor` in the response means you are on the last page.
- The cursor is gap-tolerant and append-safe: concurrent event appends with higher `id`
  values are always reachable via future pages; already-returned rows are never re-emitted.

### Response shape

```json
{
  "events": [
    {
      "id": 42,
      "event_id": 0,
      "timestamp": "2026-06-27T10:00:00Z",
      "type": "WorkflowStarted",
      "data": { "workflow_id": "my-wf", "input": { "n": 1 } }
    }
  ],
  "next_cursor": "42",
  "total_events": 350,
  "last_event_id": 9999
}
```

| Field | Type | Description |
|-------|------|-------------|
| `events` | array | Events on this page (≤ `limit`). |
| `next_cursor` | string \| null | Cursor for the next page. `null` = last page. |
| `total_events` | integer | Total event count for this execution, **unaffected** by `event_type` filter. |
| `last_event_id` | integer | Highest `harvest_events.id` for this execution (unfiltered). |

Each entry in `events`:

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer | `harvest_events.id` — the cursor anchor for keyset pagination. |
| `event_id` | integer | Sequential event index within this execution (0-based). |
| `timestamp` | string | RFC 3339 timestamp recorded when the event was appended. |
| `type` | string | Event discriminator (e.g. `TimerStarted`, `ActivityCompleted`). |
| `data` | object | Event payload (the `data` object from the adjacently-tagged stored JSON). |

### `get_workflow` truncation contract

`GET /workflows/{id}` now bounds `history` to the first **100** events and adds two
new fields:

| Field | Type | Description |
|-------|------|-------------|
| `history_truncated` | boolean | `true` when the execution has more than 100 events and the embedded `history` is a partial view. |
| `history_endpoint` | string | URL path of this paginated endpoint (e.g. `/workflows/{id}/history`). |

`GET /workflows/{id}/history/export` (full history export) is unchanged.

### Paging example

```bash
# Page 1 (first 50 events)
curl -H "x-harvest-admin: true" \
  "/api/harvest/workflows/$EXEC_ID/history?limit=50"

# Page 2 (use next_cursor from page 1)
curl -H "x-harvest-admin: true" \
  "/api/harvest/workflows/$EXEC_ID/history?limit=50&after=$CURSOR"

# Filter to only TimerStarted + TimerFired events
curl -H "x-harvest-admin: true" \
  "/api/harvest/workflows/$EXEC_ID/history?event_type=TimerStarted&event_type=TimerFired"
```

---

## Workflow Updates Result API

### Endpoints

#### Poll/Admit Update
```
POST /workflows/{id}/update/{update_name}
```

By default (or when `wait=completed`), this endpoint admits the update and then polls for completion. If the workflow reaches a terminal state before the update is completed or failed, the poll immediately returns a `409 Conflict` containing the orphaned update error payload:

```json
{
  "update_id": "<uuid>",
  "error_type": "update_orphaned",
  "workflow_state": "COMPLETED"
}
```

#### Get Update Result
```
GET /workflows/{id}/update/{update_id}/result
```

Looks up the result of an admitted update. Returns:
- `200 OK` with the JSON output if completed successfully.
- `409 Conflict` with the failure error string if failed.
- `202 Accepted` if the update is still in-flight.
- `409 Conflict` with `update_orphaned` payload if the workflow ended while the update was unresolved:

```json
{
  "update_id": "<uuid>",
  "error_type": "update_orphaned",
  "workflow_state": "FAILED"
}
```

## Signal delivery (`POST /workflows/{id}/signal/{signal_name}`)

Delivers a named signal to a running workflow execution. The request body is
the signal payload itself (free-form JSON) — nothing else is ever smuggled
into it.

### Idempotent delivery (issues #521 / #753)

Webhook and event sources deliver at-least-once. To make duplicate deliveries
land **exactly one** `SignalReceived` event, supply an exactly-once key
out-of-band:

- `Idempotency-Key:` request header (wins when both are present), or
- `?idempotency_key=` query parameter.

A present `Idempotency-Key` header that is empty or not valid UTF-8 is
rejected with `400 Bad Request` rather than silently degraded to
at-least-once. Dedupe scope is shard-local, keyed on
`(execution_id, idempotency_key)` — the same upstream event id may safely
target different executions. Note that `execution_id` there is the execution
the signal actually **landed on**, which for a workflow-level retry chain is
the live attempt rather than the addressed id — see *Retry-chain routing*
below. Omitting the key preserves the legacy at-least-once contract exactly:
every call delivers a distinct signal event.

### Retry-chain routing (issue #843)

The addressed id names the **logical run**, so the signal is routed to the
**live attempt** of the workflow-level retry chain (#523). A caller still
holding the id `start` returned therefore reaches the attempt that is actually
running, not a sealed `FAILED` predecessor. When the signal landed on a
different execution than the one addressed, the ack carries an additional
`routed_execution_id`; it is omitted otherwise, so a non-retried run's response
is unchanged. Idempotency-key dedupe scope is unaffected — it stays keyed on
`(execution_id, idempotency_key)` against the execution the signal actually
landed on. When a retry is scheduled, the whole signal mailbox moves to the
successor (re-armed) in the retry's own transaction, so a keyed row moves with
its key and an at-least-once re-send still dedupes against the live attempt
rather than being swallowed by a sealed predecessor. See
[the logical-handle contract](logical-handle.md).

### Response

```
202 Accepted
{ "ok": true, "signal_delivered": true }   // freshly queued
{ "ok": true, "signal_delivered": false }  // deduplicated retry — idempotent replay, not an error

// when a workflow-level retry chain routed the signal to a later attempt (#843):
{ "ok": true, "signal_delivered": true, "routed_execution_id": "<live attempt exec_id>" }
```

Terminal executions: an **unkeyed** signal — or a keyed signal whose key has
never landed — keeps the existing terminal-error semantics (the keyed insert
is rolled back, so no orphan row is left behind). One deliberate carve-out:
a keyed **retry** whose key already landed while the execution was still
running dedupes to a no-op success (`202 { "signal_delivered": false }`) even
after the execution has since gone terminal — the retry acknowledges a
delivery that already happened rather than requesting a new one
(`send_signal_idempotent` attempts the insert *before* validating state for
exactly this reason). `404` for an unknown execution id, keyed or not.

### CLI usage

```bash
harvest workflow signal <exec-id> approval \
  --payload-json '{"approved": true}' \
  --idempotency-key evt_abc123
```

`--idempotency-key` maps onto the `?idempotency_key=` query parameter of this
route; an empty key is rejected at the CLI (the server would treat it as
omitted, silently degrading to at-least-once). See the
[signals chapter](getting-started/04-signals.md#idempotent-standalone-signals-over-http-issue-521)
for the full walkthrough and the
[idempotency chapter](getting-started/06-idempotency.md#idempotent-signal-delivery)
for the surrounding idempotency story (including `signal-with-start` for the
first-delivery case).
