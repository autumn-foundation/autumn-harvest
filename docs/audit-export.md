# Audit export to an external SIEM sink

Issue #953. Ships every management-API audit record off-box, to a sink you
run, with at-least-once delivery, visible lag, and a redrive path.

Harvest writes an audit record for every mutating management-API operation
(`docs/runbooks/audit-trail.md`). Those rows live **per shard, inside the same
Postgres databases they describe** — which is backwards for a compliance team:
an attacker (or a fat-fingered operator) with database access is the same
principal who can rewrite the record of what they did, and a SOC 2 / ISO 27001
audit asks where privileged-action logs ship and how you know none were lost.

This feature answers both. The design is a deliberate replay of the durable
completion-callback architecture (#605): a boxed async trait in core with no
HTTP client, a `reqwest` signed-webhook implementation in the plugin, a
two-transaction scanner that never holds a row lock across network I/O, and a
per-shard cursor that advances only on acknowledgement.

- **It is opt-in.** With no sink configured, nothing changes: no sequence is
  assigned, no cursor row is created, the scanner returns before issuing a
  single query.
- **It never touches workflow history.** No new `WorkflowEvent` variant, no
  replay-determinism impact. Audit rows are operational metadata; the exporter
  only reads them.

---

## Configuring it

The batteries-included path — a signed webhook your SIEM (or a collector in
front of it) receives:

```rust
use autumn_harvest::completion_callback::HostAllowlist;

let harvest = HarvestBuilder::new()
    .audit_export_allowlist(HostAllowlist::new().with_pattern("siem.example.com"))
    .audit_export_webhook("https://siem.example.com/harvest/audit")
    .audit_export_secret(std::env::var("HARVEST_AUDIT_HMAC")?)
    .build();
```

The allowlist is required, and HTTPS is required, for the same reason as
completion callbacks: the URL is validated at `try_build()` time and a
rejection **fails the build** rather than warning. An audit export that
silently never delivers is a compliance gap you would discover at audit time.
`audit_export_allow_http(true)` and `audit_export_allow_ip_literals(true)`
exist for local development; audit records name who acted on which tenant, so
shipping them in cleartext is itself a finding.

`audit_export_secret(...)` is **required** alongside a webhook, and its absence
fails the build too. HMAC-SHA256 accepts a zero-length key and produces a
well-formed signature, so an unconfigured secret does not yield a *missing*
`X-Harvest-Signature` — it yields one any third party can reproduce, which is
worse than none for a receiver that verifies it. A custom `AuditSink` may
authenticate however it likes (IAM, mTLS, a local file), so there the secret is
optional and its absence only warns.

Other knobs:

| Builder method | Default | Notes |
|---|---|---|
| `audit_export_batch_size(n)` | 500 | Records per batch, clamped to `[1, 5000]`. |
| `audit_export_backoff(b)` | 1s → 2s → 4s … capped at 60s | Capped exponential. **No attempt ceiling** — see below. |
| `audit_export_lease(d)` | 60s | How long one exporter holds a shard's cursor, **and** the timeout on the sink call. Set it above your sink's own request timeout. |
| `audit_export_secret(k)` | *(required for a webhook)* | HMAC key for `X-Harvest-Signature`. |
| `audit_export_sink(s)` | *(none)* | Your own `AuditSink`; takes precedence over the webhook. |

### Bringing your own sink

`AuditSink` lives in core and has no HTTP dependency, so a Kinesis writer, a
file appender, or an OTLP-logs bridge is a first-class implementation rather
than a fork:

```rust
use autumn_harvest::audit_export::{AuditBatch, AuditSink, SinkAttempt, SinkFuture};

struct KinesisSink { /* ... */ }

impl AuditSink for KinesisSink {
    fn deliver<'a>(&'a self, batch: &'a AuditBatch<'a>) -> SinkFuture<'a> {
        Box::pin(async move {
            // `batch.records` is the structured form; `batch.body` is the
            // canonical JSON-lines bytes core signed.
            match self.put_records(batch.records).await {
                Ok(()) => SinkAttempt::success(200),
                Err(e) => SinkAttempt::transport_error(e.to_string()),
            }
        })
    }
}
```

**A `success` return advances the cursor past those records.** Only return one
once the batch is durably accepted downstream.

> A direct core embedder (`HarvestBuilder::build()` → `into_worker_parts()`,
> without `autumn-harvest-plugin`) must use `audit_export_sink(...)`: core
> ships no HTTP client, so `audit_export_webhook(...)` alone has no transport
> on that path and logs a warning rather than silently doing nothing.

---

## The wire format

One POST per batch, `Content-Type: application/x-ndjson`, body is JSON lines —
one audit record per line, newline-terminated.

```
POST /harvest/audit
Content-Type: application/x-ndjson
X-Harvest-Signature: sha256=<hex HMAC-SHA256 of the exact body>
X-Harvest-Timestamp: 2026-08-31T04:11:07.881Z
X-Harvest-Audit-Shard: 0
X-Harvest-Audit-First-Seq: 4181
X-Harvest-Audit-Last-Seq: 4680

{"shard":0,"seq":4181,"id":"...","occurred_at":"2026-08-31T04:11:02.117Z","actor":"alice@example.com","operation":"workflow.cancel","target_type":"workflow","target_id":"exec-9f2…","route_or_command":"POST /workflows/{id}/cancel","request_id":null,"idempotency_key":null,"status":"SUCCEEDED","error_summary":null,"source":"api"}
{"shard":0,"seq":4182, …}
```

The signature is HMAC-SHA256 over the exact bytes of the body, in the same
`X-Harvest-Signature: sha256=<hex>` scheme as completion callbacks (#605) — a
receiver already verifying those can reuse the verification code unchanged.
Compare with a constant-time comparison.

> **The signature covers the body only.** `X-Harvest-Audit-Shard`,
> `-First-Seq`, `-Last-Seq`, and `X-Harvest-Timestamp` are unauthenticated
> routing and triage metadata. Read the authoritative `(shard, seq)` pair from
> each record **in the body**: deduplicating on the headers would mean
> deduplicating on attacker-controlled input, and a replay of a captured,
> validly-signed batch with a shifted range could mark a real range as
> already-seen — creating exactly the silent gap this feature exists to
> prevent.

Optional fields serialize as an explicit `null` rather than being omitted, so
a SIEM's schema inference sees a stable object shape across every batch.

### Verifying completeness: `(shard, seq)`

`seq` is a **dense, strictly monotonic, per-shard** sequence. Per shard it
starts at 1 and increases by exactly 1 per record, so a receiver can do better
than gap *detection* — it can check contiguity:

- Deduplicate on `(shard, seq)`. Delivery is at-least-once, so the same pair
  can arrive more than once (a retry, a process death between the POST and the
  cursor write, or an operator redrive). Re-deliveries are **byte-identical**.
- Alert on a hole. A missing `seq` between two received ones means records did
  not reach you — either still in flight, or lost on your side.
- Do not compare sequences across shards. They are independent counters.

#### Why the sequence is assigned by the exporter

The obvious implementation — a `BIGSERIAL` on the audit table — is wrong twice
over, and both reasons are worth knowing if you are auditing this design:

1. **It would lose records.** A serial value is handed out *before* the
   transaction commits, so two concurrent audited operations can take 5 and 6
   and commit in the order 6, 5. An exporter with a `WHERE seq > cursor` cursor
   that shipped 6 first would skip 5 forever. (`occurred_at` has the same
   defect — it is transaction *start* time and can move backwards between
   concurrent inserts.)
2. **It would break under DR failover.** Logical replication does not replicate
   sequence values, so a promoted standby (`docs/cross-region-dr.md`) would
   re-issue sequence numbers it had already exported, corrupting your
   `(shard, seq)` accounting.

Instead the exporter stamps `harvest_audit_log.export_seq` on rows it can
actually *see* (`export_seq IS NULL`), under the per-shard cursor row lock. A
row that commits late is still `NULL` on the next tick and simply receives a
later sequence — skipping is not representable. The counter lives in
`harvest_audit_export_cursor`, ordinary replicated table data.

### Mapping to OTLP logs

The record is deliberately flat, for embedders bridging to an OpenTelemetry
collector:

| Field | OTLP log record |
|---|---|
| `occurred_at` | `timeUnixNano` / `timeObservedUnixNano` |
| `operation` | `body` (or `event.name`) |
| `status` | `severityText` — `SUCCEEDED` → `INFO`, `FAILED` → `ERROR` |
| `error_summary` | `attributes["exception.message"]` |
| `shard` | `attributes["harvest.shard.id"]` |
| `seq` | `attributes["harvest.audit.seq"]` |
| `id` | `attributes["harvest.audit.id"]` |
| `actor`, `target_type`, `target_id`, `route_or_command`, `request_id`, `idempotency_key`, `source` | `attributes["harvest.audit.<field>"]` |

Vendor-specific integrations (Splunk HEC, Datadog intake) are embedder glue on
top of this surface, not engine features.

---

## Delivery semantics

**At-least-once, and it never gives up.** Unlike a completion callback, an
audit record has no dead-letter path: the export *is* the compliance artifact,
so a failing sink backs off (capped exponential) and retries forever, and the
cursor is held exactly where it was. A non-2xx response, a transport error, or
a 3xx (redirects are never followed — an allowlisted host answering with a
pointer at an internal address must not be chased) all mean "not delivered".

The delivery loop is three phases, and the separation is the point:

1. **Claim** (one transaction): take the shard's cursor row lock, stamp
   sequences on newly-visible audit rows, load the batch above the cursor, bump
   the claim epoch, take a lease. Commits before any network call.
2. **Deliver** (no transaction, no locks): hand the batch to the sink.
3. **Acknowledge** (one transaction): on a 2xx, advance the cursor to the
   batch's highest sequence. On anything else, write the backoff and **leave
   the cursor alone**.

Every acknowledgement is guarded on the claim epoch, so an attempt whose sink
call outlived its lease — and whose batch a later claim already re-delivered —
cannot apply a stale outcome over a fresher one, and a redrive that lands
mid-flight cannot be silently undone.

The exporter rides the existing background-scanner cadence
(`enforce_timeouts_once`); it spawns no task of its own.

### Retention interaction

`purge_old_audit_records` will **never** delete a record the exporter has not
shipped, even one past the retention window. A sweep that removed an unexported
row would be a silent compliance gap — gone from the database *and* absent from
the SIEM, with nothing anywhere to show it was lost.

The guard is keyed on **whether a sink is configured in the process running
the sweep**, not on whether a cursor row happens to exist. That distinction
matters in both directions:

- *Sink configured, cursor row not yet created* — a shard the exporter has not
  reached yet. A cursor-only guard would be vacuous here and purge unexported
  records outright.
- *Sink removed, cursor row left behind* — nothing deletes a cursor row, so a
  cursor-only guard would match every later record forever and silently turn
  audit retention off permanently.

With no sink configured the guard is skipped entirely and the purge is
byte-identical to its pre-#953 behaviour.

The trade is that a sink that is down indefinitely lets the audit table grow
past its retention window. That is deliberate: dropping a privileged-action log
to reclaim disk is not a decision the engine gets to make for you. Alert on
`harvest.audit.export_lag` (below) and it will not surprise you — see
`docs/runbooks/harvest-alerts.md#harvest_audit_export_lag_high`.

---

## Observability

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `harvest.audit.export_lag` | Gauge (seconds) | `shard` | Age of the **oldest** audit record the sink has not acknowledged. `0` means fully caught up. |
| `harvest.audit.exported` | Counter | `shard` | Records acknowledged, counted only after the cursor advanced. |

Both are labelled `{shard}` only. The audit `actor`, `operation`, and
`target_id` are deliberately never labels — they are unbounded, user-supplied,
and tenant-identifying (ADR-0001 §7).

> **Why oldest, not newest.** Under sustained mutating load a stuck exporter
> always has a brand-new unexported record, so a lag defined against the
> *newest* unexported record would read ≈0 during exactly the outage you need
> to see. The oldest-record age is the one an SLO like "export lag < 30s p99"
> can actually be measured against.

The gauge is emitted on **every** exporter tick, including ticks that deliver
nothing — the signal must not go stale precisely when delivery has stopped.

A suggested alert: `harvest_audit_export_lag > 300` for 10 minutes. Sustained
lag means privileged-action logs are not reaching the SIEM. Nothing is lost —
the cursor is held rather than advanced — but the window during which a
compromise would be invisible is growing.

### `GET /admin/audit-export`

Read-only, admin-gated, cross-shard.

```json
{
  "sink_configured": true,
  "shards": [
    {
      "shard": 0,
      "cursor_seq": 4680,
      "last_assigned_seq": 4712,
      "pending_records": 32,
      "lag_seconds": 1.8,
      "delivery_state": "IDLE",
      "consecutive_failures": 0,
      "last_status": 200,
      "last_error": null,
      "last_delivered_at": "2026-08-31T04:11:07.902Z",
      "next_attempt_at": "2026-08-31T04:11:07.902Z"
    }
  ],
  "status": "complete",
  "unavailable_shards": []
}
```

`delivery_state` is `IDLE`, `DELIVERING`, `BACKOFF`, `RETRYING`, or
`NOT_STARTED`. `sink_configured` reports whether *this* process has a sink
installed — `false` next to a growing `pending_records` is the signature of
"export configured on the web app but not on the worker fleet", which is
otherwise invisible. An unreachable shard degrades the response to
`"status": "partial"` rather than failing the read.

---

## Redrive: recovering from sink-side data loss

Your SIEM lost a day. Rewind the cursor and Harvest re-exports:

```bash
curl -X POST https://app.example.com/api/harvest/admin/audit-export/redrive \
  -H 'Content-Type: application/json' \
  -d '{"shard": 0, "before": "2026-08-30T00:00:00Z"}'
```

`{"shard": 0, "to_seq": 4100}` rewinds to an exact sequence instead; supply
exactly one of `to_seq` or `before`. Records with `seq > to` re-export on the
next scanner tick, **byte-identical** to their first delivery — the export
sequence is never re-stamped — so your receiver's `(shard, seq)` dedup sees
exactly what it stored.

Three properties worth knowing:

- **A cursor can only ever move backwards.** A request that would advance it —
  or leave it exactly where it is, which includes replaying the same redrive
  twice — is refused with `400`, not applied. Advancing it would mark records
  delivered that never were, the exact gap this feature exists to make
  impossible.
- **`before` resolves conservatively.** It rewinds to one below the *lowest*
  sequence assigned to a record at or after that instant, never to the highest
  sequence before it. `occurred_at` is transaction start time, so commit order
  and timestamp order can disagree; anchoring this way means any skew makes the
  rewind reach *further back* (costing duplicate deliveries your receiver
  dedupes) rather than skipping records the operator asked for.
- **The redrive is itself audited** (`audit_export.redrive`), so re-exporting is
  as auditable as the operations being exported. The rewind and its audit
  record **commit together** — the audit row is written inside the transaction
  holding the rewind — so an applied-but-unaudited redrive is not
  representable. The audit row's `target_id` records the shard *and* the
  requested position (`shard=0;to_seq=42`), since a redrive is the one
  operation here that can trigger a mass re-export. A refused redrive (`no-op`,
  unknown shard) is audited as `FAILED`: nothing moved, and the trail must not
  say otherwise.
- **It invalidates in-flight deliveries.** The rewind bumps the shard's claim
  epoch, so a batch already in flight cannot acknowledge over it.

Only records still present in the audit table can be re-exported; a redrive
past the retention window returns whatever survives.

---

## Out of scope

- **Exactly-once delivery.** At-least-once is the contract; receivers dedupe on
  `(shard, seq)`, matching #605.
- **Hash-chained / Merkle tamper-proofing of the at-rest rows.** A worthy but
  separate cryptographic-audit-log effort. What ships here is gap-detectable
  off-box export, which removes most of the incentive to tamper at rest:
  rewriting a row in the database does not rewrite the copy the SIEM already
  holds, and deleting an **exported** row leaves a sequence hole the receiver
  can see.

  Be precise about the limit: a row deleted **before the exporter has sequenced
  it** — inside the window between the audited action and the next scanner
  tick, or anywhere in the backlog during a sink outage — never receives a
  sequence, so the surviving rows are stamped densely and there is no hole to
  detect. Tamper evidence begins at the moment a record is sequenced, not at
  the moment it is written. Shorten that window by keeping the export healthy;
  close it properly only with at-rest hash chaining, which is out of scope
  here.
- **Exporting workflow event history.** That is `HistoryArchiver` (#345). This
  is the audit trail only.

---

## See also

- `docs/runbooks/audit-trail.md` — what is audited and why.
- `docs/completion-callbacks.md` — the #605 pattern this replays.
- `docs/security-posture.md` — mounting the management API safely.
- `docs/cross-region-dr.md` — why the sequence is not a Postgres sequence.
