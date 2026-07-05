# Completion Callbacks — push terminal results to a URL (issue #605)

An embedder who starts a workflow (`process_refund`, `run_kyc`) from their
Autumn app has no push notification when it finishes: the only ways to learn
the terminal result are polling `GET /workflows/{id}/result` (issue #527) or
a long-poll (issue #527's `?wait=`), both anti-patterns for fire-and-forget
async flows. This feature closes the loop back **out** to an external system:
an embedder registers a **completion callback target** (URL + terminal-state
filter) and Harvest durably POSTs a signed, fixed JSON envelope to it the
moment the workflow reaches a terminal state.

**Invariant: no new `WorkflowEvent` variant, zero replay-determinism impact.**
Callback registration is execution metadata (a `completion_callbacks` column
on `harvest_workflow_executions`); delivery is a post-terminal operational
side-channel driven by its own table (`harvest_completion_deliveries`) and
its own shard-local scanner. `harvest_events` is untouched. A workflow that
registers no callback target behaves byte-for-byte identically to before
this feature existed.

## Registering a target

Per-execution, at start time:

```bash
curl -X POST /api/harvest/workflows/process_refund/start \
  -H 'Content-Type: application/json' \
  -d '{
        "input": { "refund_id": "r-123" },
        "completion_callbacks": [
          { "url": "https://api.example.com/hooks/harvest", "filter": { "type": "AnyTerminal" } }
        ]
      }'
```

Or as a `HarvestBuilder`-wide default applied to every workflow that doesn't
supply its own targets:

```rust
HarvestBuilder::new()
    .completion_callback_default(
        "https://api.example.com/hooks/harvest",
        EventFilter::CompletedOnly,
    )
    .completion_callback_allowlist(
        HostAllowlist::new().with_pattern("*.example.com"),
    )
    .completion_callback_secret(b"a-shared-hmac-secret".to_vec())
```

Per-execution targets and builder defaults are unioned (per-execution
first), deduplicated by `(url, filter)`, and each distinct target is
assigned a stable `callback_index` — so a target's delivery row identity
survives re-evaluation (parent-close cascades, retries of the terminal
transaction) without double-enqueueing.

`EventFilter` has three shapes: `CompletedOnly` (fires only on `Completed`),
`AnyTerminal` (fires on any of the five terminal states — Completed, Failed,
Cancelled, TimedOut, Terminated; **never** on `ContinuedAsNew`, which is not
a terminal outcome for the logical run), and `States([...])` for an explicit
subset.

## SSRF policy — HTTPS-only, allowlist required by default

`validate_target_url(url, &SsrfPolicy)` rejects, by default:

- Any scheme other than `https://` (opt into `http://` via
  `completion_callback_allow_http(true)` — not recommended).
- Any host not on the configured allowlist. `HostAllowlist` supports exact
  hostnames (`api.example.com`) and `*.suffix` wildcards (`*.example.com`
  matches `hooks.example.com` but not `example.com` itself and not
  `evilexample.com`).
- IP-literal hosts (`https://10.0.0.5/hook`) unless
  `completion_callback_allow_ip_literals(true)` is set, and even then,
  loopback/private/link-local/CGNAT/ULA ranges remain rejected.
- Userinfo in the URL (`https://user:pass@host/...`) and a port that
  conflicts with an allowlist entry's explicit `:port` suffix.

DNS is **not** resolved as part of validation (this is a TOCTOU trap for any
SSRF guard) — the check is host/scheme-syntactic plus IP-literal blocking,
by design. Rejections are a machine-readable, `serde`-tagged `SsrfRejection`
enum, not a bare string.

Validation happens twice: once at registration time (the HTTP start route
rejects a non-allowlisted per-execution target with `422 Unprocessable
Entity`; a builder-default target that fails validation fails `try_build()`
with `HarvestBuilderError::CallbackTargetRejected`), and again — defense in
depth — at enqueue time inside the terminal transaction, using whatever
policy is live *then*. A target that was allowlisted at registration but is
no longer allowlisted under a tightened operator policy is silently skipped
(logged at `warn`) rather than aborting the terminal transaction; a workflow
completing is never blocked on callback delivery.

## Envelope and signing

Every delivery attempt POSTs the identical, frozen JSON body (built once,
at enqueue time, from the terminal `WorkflowExecution` row — so it survives
retention hard-deleting the execution row later):

```json
{
  "delivery_id": "b6b6...uuid",
  "execution_id": "9c1e...uuid",
  "workflow_name": "process_refund",
  "workflow_id": "refund-r-123",
  "state": "Completed",
  "result": { "refunded": true, "amount_cents": 4200 },
  "completed_at": "2026-07-05T12:34:56Z"
}
```

- `result` is present only for `state: "Completed"` (the workflow's output).
- `error` (mutually exclusive with `result`) is present for the other four
  terminal states, sourced from the execution row's `error` column.
- Field order and presence (`skip_serializing_if = Option::is_none`) are
  part of the contract: the exact serialized bytes are what gets signed and
  POSTed, so a receiver can verify the signature against the raw body it
  received without needing to know Harvest's field ordering.

Each POST carries two headers:

```
X-Harvest-Signature: sha256=<lowercase-hex-hmac-sha256-of-raw-body>
X-Harvest-Timestamp: 2026-07-05T12:34:56.789Z
```

The HMAC key is supplied via `completion_callback_secret(bytes)` and is
**never persisted** — not in `harvest_completion_deliveries`, not in logs.
It lives only in the in-process `GLOBAL_CALLBACK_CONFIG` (mirroring the
`GLOBAL_WORKFLOW_METADATA` static pattern already used elsewhere in the
engine) and is wrapped in a `Debug`-redacting newtype (`CallbackSecret`) so
an accidental `{:?}` print can't leak it. A receiver verifies delivery by
recomputing `HMAC-SHA256(secret, raw_body)` over the exact bytes received
and comparing against `X-Harvest-Signature` in constant time.

## Delivery: at-least-once, retried, dead-lettered

Delivery is itself durable execution, reusing the engine's existing
retry/backoff/DLQ machinery rather than inventing new machinery:

- **Enqueue** happens inside the *same* terminal transaction as every other
  terminal side effect (`evaluate_triggers_for_execution`, the one function
  already called at all ~15 terminal call sites). `INSERT ... ON CONFLICT
  (workflow_exec_id, callback_index) DO NOTHING` makes re-entry (e.g. the
  parent-close cascade re-evaluating triggers) a no-op.
- **Scanner** (`fire_due_completion_deliveries`) is folded into
  `enforce_timeouts_once` — no new background task, no new poll interval.
  It is two-transaction by construction (never holds a row lock across
  network I/O): tx#1 claims a batch (`FOR UPDATE SKIP LOCKED`, transition to
  `INFLIGHT`, bump `attempt`, short in-flight lease), the POST happens with
  no lock held, tx#2 records the outcome.
- **Retry policy** defaults to 10 attempts, 30s initial backoff, ×2
  exponential, capped at 600s (`default_delivery_retry_policy()`),
  overridable via `completion_callback_retry_policy(...)`. The policy is
  frozen into the delivery row at enqueue time, so a later config change
  doesn't retroactively alter an in-flight delivery's schedule.
- **Dead-letter on exhaustion**: `DeadLetterReason::CallbackDeliveryExhausted
  { delivery_id, attempts, last_status, target }` — a typed reason distinct
  from `PoisonPill`/`WorkflowTaskTimeout`/plain task-retry exhaustion. The
  delivery row itself is kept in `FAILED` state (not deleted) so the
  management/CLI redrive surface has something to act on.
- **At-least-once + idempotent receivers**: `delivery_id` is stable across
  every attempt and every redrive of the same logical delivery, so a
  receiver keying idempotency off `delivery_id` (or the HMAC signature,
  which is deterministic per `delivery_id` + body) never double-processes a
  redelivered notification.

## Transport: trait seam in core, `reqwest` in the plugin

Core (`autumn-harvest`) defines a boxed-future `CompletionCallbackDeliverer`
trait and ships **no** HTTP client — consistent with the Postgres-only core
boundary (`PayloadStore`, `HistoryArchiver` follow the same shape):

```rust
pub trait CompletionCallbackDeliverer: Send + Sync {
    fn deliver<'a>(
        &'a self,
        target_url: &'a str,
        body: &'a [u8],
        headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a>;
}
```

`autumn-harvest-plugin` ships `ReqwestCallbackDeliverer`, auto-wired by
`HarvestPlugin`/`HarvestRunner` unless overridden via
`completion_callback_deliverer(...)`. It disables redirect-following
(`redirect::Policy::none()`) — an allowlisted host could otherwise 302 to an
internal address, silently bypassing the SSRF guard — and applies a hard
request timeout. HMAC signing, SSRF validation, envelope construction,
retry/backoff, and the DLQ all stay in core; the trait is a thin "send these
bytes+headers to this URL, tell me the status or transport error" seam.

## Management API

```
GET  /api/harvest/workflows/{id}/completion-deliveries
```
Lists every delivery row for an execution (PENDING/INFLIGHT/DELIVERED/FAILED),
ordered by `callback_index`. Read-only, no audit record (parity with
`GET /dead-letters`).

```
POST /api/harvest/workflows/{id}/completion-deliveries/{delivery_id}/redrive
```
Admin-guarded. Resets a `FAILED` delivery to `PENDING` with a fresh retry
budget (attempt 0) and clears its `harvest_dead_letters` entry — the *same*
`delivery_id`, so the envelope and signature are byte-identical to the
original attempts and a receiver's idempotency key still matches. Idempotent-
shaped: redriving a delivery that isn't currently `FAILED` returns `200`
with `outcome: "not_failed"` rather than erroring; redriving an unknown
`delivery_id` returns `404`. Self-contained — never touches
`harvest_workflow_executions` or the unrelated task-queue DLQ redrive path.

## CLI

```bash
harvest completion-delivery list <execution_id> [--state pending|inflight|delivered|failed]
harvest completion-delivery redrive <execution_id> <delivery_id>
```

Aliases: `completion-deliveries`, `callbacks`. `--state` is applied
client-side (the list endpoint has no server-side state filter, since the
per-execution row count is always small).

## Builder configuration reference

| Method | Purpose |
|---|---|
| `completion_callback_default(url, filter)` | Add a builder-wide default target (validated against the SSRF policy at `try_build()`). |
| `completion_callback_allowlist(HostAllowlist)` | Set the allowed hosts/patterns. Empty by default — no targets validate until this is set. |
| `completion_callback_allow_http(bool)` | Opt into `http://` targets (default `false`). |
| `completion_callback_allow_ip_literals(bool)` | Opt into IP-literal hosts, still subject to non-routable-range blocking (default `false`). |
| `completion_callback_secret(bytes)` | HMAC signing key. |
| `completion_callback_retry_policy(RetryPolicy)` | Override the default 10-attempt/30s/×2/600s-cap schedule. |
| `completion_callback_deliverer(impl CompletionCallbackDeliverer)` | Override the default `reqwest`-based transport (e.g. for testing, or a non-`reqwest` HTTP stack). |

## What this feature deliberately does not do

- No cross-shard delivery fan-out — a delivery row lives on the same shard
  as its owning execution, consistent with the sharding contract.
- No delivery ordering guarantee across multiple targets on the same
  execution — each `(url, filter)` pair is an independent row with its own
  retry schedule.
- No change to `ContinuedAsNew` semantics — it is not a terminal state for
  callback purposes; only the eventual real terminal state of the run (after
  following the continue-as-new chain, if the caller wants that — see
  `GET /workflows/{id}/result`, issue #527) fires a callback.
