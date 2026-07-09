# Chapter 6 — Idempotency for safe retries

[← Child workflows](05-child-workflows.md) · [Index](README.md) · [Next: Reliability knobs →](07-reliability-knobs.md)

---

Activities are at-least-once. If the worker crashes after Stripe accepts a
charge but before the engine writes `ActivityCompleted` to Postgres, the
retry will charge the customer again unless the downstream system
deduplicates. Every activity gets a stable, retry-safe key from
`ctx.idempotency_key()`:

```rust
#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(2)))]
async fn charge_card(
    ctx: &ActivityContext,
    input: serde_json::Value,
) -> HarvestResult<serde_json::Value> {
    let amount_cents = input["amount_cents"].as_u64().unwrap_or(0);
    let customer_id = input["customer_id"].as_str().unwrap_or("").to_owned();

    let idem_key = ctx.idempotency_key()?.as_str().to_owned();

    // Pass idem_key as Stripe's Idempotency-Key header. Subsequent retries
    // for this attempt carry the same key, so Stripe returns the original
    // charge instead of creating a new one.
    let charge_id = stripe_charge(amount_cents, &customer_id, &idem_key)
        .await
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "charge_id": charge_id }))
}
```

The key is stable across worker restarts, duplicate dispatch, and replay,
**but it's distinct for every logical invocation** — calling `charge_card`
twice for two different orders gets two different keys.

For activities that make several outbound calls, derive named subkeys:

```rust
let key = ctx.idempotency_key()?;
create_db_user(&user_id, key.subkey("db").as_str()).await?;
send_welcome_email(&user_id, key.subkey("email").as_str()).await?;
```

## Idempotent signal delivery

Idempotency also matters in the other direction: webhook and event sources
(Stripe, GitHub, SQS) deliver **at-least-once**, so the same logical event can
reach a running workflow twice. Supplying an idempotency key with the signal
collapses duplicate deliveries into exactly one `SignalReceived` event — no
hand-rolled "seen event ids" dedup set in the workflow body (issues #521/#753):

- **HTTP** — `POST /workflows/{id}/signal/{signal_name}` with an
  `Idempotency-Key:` header (or `?idempotency_key=` query param; the header
  wins when both are present).
- **CLI** — `harvest workflow signal <exec-id> <name> --idempotency-key <key>`.
- **Typed client stub** — the `#[signal]` macro generates a
  `signal_{name}_idempotent(...)` sibling method.
- **Untyped client** — `signal::send_signal_idempotent(conn, exec_id, name,
  payload, Some(key))`.
- **First delivery for a workflow** — `signal-with-start` carries its own
  `idempotency_key` field (issue #244); the surfaces above cover the
  steady-state "signal an already-running workflow" case.

Dedupe scope is per execution — `(execution_id, idempotency_key)` — so the
same upstream event id may safely target different executions. A deduplicated
delivery returns 2xx with `signal_delivered: false` — deliberately even when
the execution has since gone terminal, as long as the key originally landed
while the run was still active (a retry acknowledges a delivery that already
happened; a fresh key or an unkeyed signal to a terminal run still gets the
terminal error). Omitting the key preserves the legacy at-least-once behavior
exactly. Full contract and curl examples:
[signals chapter](04-signals.md#idempotent-standalone-signals-over-http-issue-521)
and the [signal-delivery section of the management-API reference](../management-api.md#signal-delivery-post-workflowsidsignalsignal_name).

## Idempotent workflow start — `idempotency_key` vs `workflow_id` (issue #808)

The same at-least-once problem hits the **start** path: a webhook or event
source that retries `POST /workflows/{name}/start` would launch a redundant
execution on every redelivery. Two knobs deduplicate a start, and they answer
**different questions**:

| | `workflow_id` (business identity) | `idempotency_key` (delivery identity) |
|---|---|---|
| Answers | "Is this the *same logical run*?" | "Is this the *same request*?" |
| Dedup scope | `(workflow_name, workflow_id)` via the `reuse_policy` matrix | `(workflow_name, idempotency_key)`, **independent of `workflow_id`** |
| Works when `workflow_id` is auto-generated per request | No (each request is a new id) | **Yes** — that's the point |
| On a duplicate | Depends on `reuse_policy` (may `409`, attach, or start fresh) | Always a `200` no-op returning the **same** `execution_id` |

`idempotency_key` is checked **first** and short-circuits the `reuse_policy`
matrix entirely. Supply it out-of-band via the `Idempotency-Key` request header
(preferred — the raw body stays your workflow input) or the body
`idempotency_key` field; the header wins when both are present.

```bash
# First delivery — creates the run (201, started_fresh: true).
curl -X POST /api/harvest/workflows/onboarding/start \
  -H 'Idempotency-Key: webhook-evt-abc123' \
  -H 'Content-Type: application/json' \
  -d '{"input": {"user_id": 42}}'

# Retry (same key) — no-op returning the SAME execution_id
# (200, deduplicated: true, no second WorkflowStarted, no second task).
curl -X POST /api/harvest/workflows/onboarding/start \
  -H 'Idempotency-Key: webhook-evt-abc123' \
  -H 'Content-Type: application/json' \
  -d '{"input": {"user_id": 42}}'
```

Semantics:

- **Exactly one execution** even under N simultaneous same-key starts
  (concurrency-safe by a composite-PK upsert).
- **Retention window** (default 24h, `HarvestBuilder::start_idempotency_window`):
  after it elapses the key is reusable and starts a fresh run.
- **Byte-identical when unused** — a start with no key omits the
  `started_fresh`/`deduplicated` response fields entirely.
- **A committed keyed replay short-circuits to the `200` no-op _before_
  fresh-start-only validations** — input-schema validation (#373),
  completion-callback SSRF validation (#605), and the delay/`start_at` checks —
  **and before the admission gate** (#377). Tightening any of those rules (or
  raising a gate during an incident) between the original delivery and a retry
  never rejects a retry of already-done work; the retry returns the original
  execution regardless of its own body. A genuinely fresh keyed start (no live
  claim) still runs every validation and the gate normally.
- **Empty key → `400`**; **combining a key with a throttle/debounce/batch
  policy → `400`** (those defer the start and expose no synchronous
  `execution_id` to converge on).
- Shard-local, and it introduces **no new event** — the dedup happens before
  `WorkflowStarted` is ever appended, so replay is unaffected.

### Consistency caveat in multi-shard deployments

The claim row and its execution are **shard-local**, so a keyed start's shard is
chosen deterministically from either the key or the `workflow_id`:

- **`workflow_id` omitted** (auto-generated): the start routes by the **key**
  `(workflow_name, idempotency_key)`. Same-key retries co-locate on one shard and
  dedup — routing by the per-request `workflow_id` would scatter them.
- **`workflow_id` supplied** (explicit business identity): the start routes by
  `(workflow_name, workflow_id)`, exactly as a non-keyed start does. This keeps
  the run on the shard that owns `(name, workflow_id)`, so the `reuse_policy`
  matrix and the `(name, workflow_id)` uniqueness invariant still apply — a keyed
  `reject_duplicate` start still `409`s against an existing run, and never
  silently creates a second run on a different shard. Same-`workflow_id` retries
  still co-locate (the `workflow_id` is constant) and dedup on the key.

For keyed dedup to converge in a multi-shard deployment, a client must supply a
**consistent `workflow_id`** — the *same* explicit value, or consistently
*omit* it and let the server auto-generate — **across all retries of the same
delivery**. A retry that carries a *different* explicit `workflow_id` is not a
retry of the same logical delivery: it changes the business identity, can route
to a different shard-local claim table, and may create a second run. This is
inherent to shard-local dedup coexisting with the `(workflow_name, workflow_id)`
reuse matrix — keyed starts with an explicit `workflow_id` route by
`workflow_id` (so the `reuse_policy` matrix still sees an existing run on that
identity's shard), and keyed starts with an omitted/auto-generated `workflow_id`
route by the key. Routing *all* keyed starts by the key instead would break the
reuse matrix for explicit-`workflow_id` starts whose existing run lives on the
`workflow_id` shard (the matrix could no longer see it, silently creating a
duplicate). The two placements cannot both be satisfied when the key and the
`workflow_id` hash to different shards, and #808 scopes dedup as shard-local with
no cross-shard probe — so the requirement is on the client: pick one
`workflow_id` shape *and value* per delivery. In a single-shard deployment this
is all moot (everything is on one shard).

### Known limitation — shard drain within the retention window

Keyed dedup routes via the **same rendezvous hash + writable-subset redirect**
as `(workflow_name, workflow_id)` uniqueness (`pick_for_idempotency_key` reuses
the identical logic as `pick_for_new_workflow`). Because of that, it is **not
guaranteed across a writable → read-only shard transition** within a key's
retention window: a key that first claimed a run on a shard which is *later*
removed from the writable set (drained to read-only, but still readable) will,
on a same-key retry, rehash to a *different* writable shard, probe/reserve a
different shard-local `harvest_start_idempotency` row, and can create a **second**
execution. This is the same drain limitation `(name, workflow_id)` uniqueness
already has (a drained-shard uniqueness key can likewise be re-created on a
different writable shard), and issue #808 deliberately scopes keyed dedup to be
shard-local, exactly as that uniqueness is — so the two share identical behavior.
The guarantee holds while the **writable set is stable**; do not drain a writable
shard to read-only while keyed starts targeting it are still within their
retention window.

See the [start route in the management-API contract](../api-contract.json) for
the full request/response schema.

---

[← Child workflows](05-child-workflows.md) · [Index](README.md) · [Next: Reliability knobs →](07-reliability-knobs.md)
