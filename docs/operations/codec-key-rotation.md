# Payload codec key rotation (issue #948)

Rotating the key that protects stored workflow payloads, and retiring the old
one with proof that nothing still depends on it.

## The problem this solves

The [`PayloadCodec`](../adr/0003-payload-codec-event-boundary.md) boundary lets
you encrypt every payload-bearing field before it touches `harvest_events`.
Because event rows are append-only and nothing ever rewrites them, that
encryption used to be permanent: after a key compromise — or a routine
compliance-mandated rotation — every byte of stored history remained encrypted
under the old key forever. Encryption you cannot rotate is compliance theater.

Harvest owns its storage, so it can do what an external event store cannot: run
an automated, progress-reporting, retirement-gated sweep that converts stored
history onto the new key.

## The shape

- Each codec envelope carries an optional **key id** (`kid`) alongside the
  existing `_harvest_codec_envelope` discriminator. It rides inside the
  envelope, which is already opaque payload content — no new `WorkflowEvent`
  variant, no change to the event JSON contract.
- The registry holds **many keyed codecs**, exactly **one active**. New writes
  use the active key; decode resolves *any* registered key, so a mixed-key
  history replays transparently for the whole migration window.
- An envelope with **no `kid`** — every row written before this feature — is
  defined to be under the key id `legacy`. Pre-upgrade rows decode unchanged,
  and while `legacy` is the active key nothing new writes a `kid` at all, so an
  un-rotated deployment's stored bytes are byte-identical to before.
- A background **lazy re-encryption sweep** walks event rows carrying a
  non-active key id, decodes with the old key, and re-encodes with the active
  one — batched, rate-limitable, idempotent, resumable.
- Retiring a key is **gated**: it is refused while any reachable shard still
  holds a row referencing it, and an *unreachable* shard blocks retirement too.

## Wiring it up

> **Read this first.** On the current engine, *no production write path applies
> a configured `PayloadCodec` at all* — `store::append_events` (and therefore
> every worker append) encodes with the identity registry, and the worker
> replays with it too. The configured registry reaches only the read paths
> (`WorkflowHandleClient`, the management API), which is what ADR-0003's issue
> #608 addendum plumbed. Until that is fixed, registering a keyed codec
> encrypts nothing new, the sweep finds nothing to convert, and this runbook
> describes a rotation that rotates an empty set. It is an ADR-0003 write-path
> defect rather than a rotation defect, and it is tracked separately;
> `store::append_events_with_codecs` is the codec-aware seam it will be fixed
> through.

```rust
use autumn_harvest::payload_codec::CODEC_LEGACY_KEY_ID;

let harvest = HarvestBuilder::new()
    // Your existing codec, under the legacy key id, so already-stored
    // (kid-less) history keeps decoding.
    .payload_codec_key(CODEC_LEGACY_KEY_ID, AesGcmCodec::new(old_key))
    // The incoming key.
    .payload_codec_key("2026-q3", AesGcmCodec::new(new_key))
    // Flip: from here, every new write is encrypted under 2026-q3.
    .active_payload_codec_key("2026-q3")
    .build()?;
```

The registry's rotation state is **shared across clones**, so
`PayloadCodecs::set_active_key` at runtime (a config reload) takes effect for
every writer immediately. There is no restart-ordering window in which a worker
that booted before the flip keeps writing under the retired key.

## Running the sweep

The sweep is a resident of the existing timeout-scanner cadence, one bounded
batch per shard per tick. It costs nothing — not one statement — on a deployment
with no keyed codec registered.

| Knob | Meaning |
| --- | --- |
| `WorkerConfig::with_codec_rotation_batch_size(n)` | Rows examined per shard per tick. Default 200. |
| `…(0)` | Sweep off, no redeploy needed. |

**Sizing the batch.** The unit is *rows examined*, not rows converted — the
sweep walks `harvest_events` in `id` order and skips rows that need no work, so
a pass costs one visit per row on the shard. The scanner interval is
`WorkerRuntimeConfig::poll_interval`, which the builder fixes at
`DEFAULT_WORKER_POLL_INTERVAL` = **500 ms**, so the default 200 rows per tick is
roughly 400 rows/second and a 1M-row shard converges in about 40 minutes. Raise
the batch to convert faster (a first pass over a large corpus is the case that
wants a big number) or lower it to reduce the load a rotation imposes; it is
safe to change at any time, and once the first pass reaches the end of the shard
the cursor keeps it cheap forever after — later ticks only look at rows appended
since.

Watch progress:

```
GET /admin/codec/rotation      # admin-gated, read-only
```

```json
{
  "active_key_id": "2026-q3",
  "registered_key_ids": ["2026-q3", "legacy"],
  "shards": [
    {
      "shard_id": 0,
      "rows_by_key_id": { "legacy": 412, "2026-q3": 999588 },
      "rows_remaining": 412,
      "cursor": {
        "last_event_id": 998112,
        "rows_reencrypted": 999588,
        "completed_at": null,
        "updated_at": "2026-08-29T11:03:22Z"
      }
    }
  ],
  "rows_remaining_total": 412,
  "status": "complete",
  "unavailable_shards": []
}
```

`rows_remaining_total` is only a count when `status` is `"complete"`. Under
`"partial"` it is a **lower bound** — an unread shard's rows are unknown, never
zero.

Metric: `harvest.codec.reencrypted{shard}` counts swept rows. A rotation that
has stalled shows as
`rate(harvest_codec_reencrypted_total[5m]) == 0` while
`GET /admin/codec/rotation` still reports rows remaining.

## Retiring the old key

```rust
use autumn_harvest::codec_rotation::FleetWriteFence;

autumn_harvest::codec_rotation::retire_codec_key(
    &sharded_pool,
    &expected_shards,
    &codecs,
    CODEC_LEGACY_KEY_ID,
    // Only after the three steps in "Retirement needs a fleet write fence" below.
    FleetWriteFence::ConfirmedByOperator,
).await?;
```

It refuses with `HarvestError::CodecKeyRetirementBlocked` naming the remaining
count **per shard** while any row is left, and succeeds only at exactly zero
everywhere. It is **fail-closed** in five separate ways, all deliberate:

- a shard with no connection pool in this process blocks retirement;
- a shard whose census errors blocks retirement;
- an empty shard list is refused outright — proving nothing is not proving zero;
- a shard list that omits a shard this process can see is refused;
- an unattested `FleetWriteFence` is refused however clean the census is.

### ⚠️ Retirement needs a fleet write fence, and the census cannot supply it

`retire_codec_key` takes a `FleetWriteFence`, and passing
`FleetWriteFence::NotConfirmed` refuses the retirement no matter how clean the
census is. This is not ceremony. `PayloadCodecs` is a **per-process** registry:
`set_active_key` on one worker is invisible to every other worker, and the
census only sees rows that are committed and visible, at one instant, on the
shards this process can reach. Two things it therefore cannot see:

1. **Another live writer.** A worker that has not yet been rolled onto the new
   active key is still encoding under the old one, and will write another
   old-key row a millisecond after your census read zero.
2. **An in-flight append.** A transaction that already encoded its payload
   under the old key but has not committed is invisible to the census, and
   becomes visible immediately after it.

In both cases the gate would have returned `Ok` and dropped the decoder, and a
row exists that this process can no longer read. If you took that `Ok` as
licence to destroy the key material, it is unreadable permanently.

**Establish the fence before you attest it:**

1. Roll the new active key to **every** worker in the fleet (not just the one
   you are running the gate from) and confirm the rollout completed.
2. Let in-flight appends drain — wait past your longest activity/append
   timeout, or stop writers outright.
3. Confirm the census reads zero, via `GET /admin/codec/rotation`, and that it
   *stays* zero across that drain window.

Only then pass `FleetWriteFence::ConfirmedByOperator`. Harvest does not
coordinate the fleet and does not pretend to — the attestation is you saying
you did the three steps above.

### ⚠️ Upgrade every reader before activating a keyed codec

Activating a non-legacy key switches new writes to **envelope version 2** (four
keys, carrying `kid`). A reader built before issue #948 recognises an envelope
only as exactly three keys with version 1, and its decoder returns anything
else *unchanged* rather than rejecting it. A pre-#948 worker therefore hands the
raw envelope object to workflow code as if it were the payload — silent wrong
data, not a loud failure.

So the deployment order is not optional:

1. Deploy the #948-capable binary to **every** reader in the fleet.
2. Confirm the rollout completed.
3. *Then* activate a keyed codec.

Registering keys is safe at any point: while the legacy key is active no `kid`
is written and envelopes stay version 1, byte-identical to what a pre-#948
deployment stores. It is the **activation** that must come last. Harvest cannot
enforce this — it has no fleet-wide view of which binaries are running.

### ⚠️ What "zero" does and does not authorise

The gate proves one specific thing: **no `harvest_events` row on any expected
shard still references the key.** That is what the sweep converts, so that is
what the census counts. It is *not* a licence to destroy the key material yet,
because a codec envelope can also be sitting in places this feature does not
sweep:

- **Offloaded blobs** (issue #524). Offload composes *after* codec encode, so
  the ciphertext — and its key id — lives in your `PayloadStore`, while the DB
  row holds only a reference envelope. Those are the *large* payloads, and they
  are explicitly out of scope here (embedder-owned storage). Re-encrypt or
  re-key them yourself before retiring.
- **Codec-encoded columns outside the event log**:
  `harvest_workflow_executions.{input,output,memo,search_attrs,error}`,
  `harvest_execution_summaries.{result,search_attrs}`,
  `harvest_dead_letters.{input,error}`, `harvest_signals.payload`, and
  `harvest_completion_deliveries.payload` are all decoded on the read path
  (`decode_workflow_execution_fields` and friends) and are **not** swept or
  censused.
- **Nested envelopes.** The census and the sweep classify a payload field by its
  *top-level* envelope. A field whose decoded plaintext itself contains an
  envelope — e.g. an `ExternalAwaitResolved.output` frozen from another
  execution's raw column — is counted only by its outer key id.

So: treat a green gate as "the event log no longer needs this key", keep the key
registered (not destroyed) until you have independently accounted for the three
cases above, and prefer retiring the key from the registry well before
destroying the material. Closing these gaps is tracked as follow-up work.

Only after that, and after a successful retirement, should you dispose of the
key material itself. Harvest never holds it.

**Supplying `expected_shards`.** Pass every shard the deployment has, not just
the ones this process serves. The gate refuses outright when the list omits a
shard this process has a pool for, but it cannot see shards no process here
knows about — an omitted shard is not censused, and the gate's `Ok` would be
vacuous for it.

## What the sweep will not touch

- **Offloaded payloads** (issue #524). Offload composes *after* codec encode, so
  the stored field is a reference envelope, not ciphertext; re-encoding it would
  encrypt the reference and orphan the blob. Re-encrypting the blob in your own
  `PayloadStore` is embedder-owned and out of scope.
- **Erasure tombstones** (issue #495) — no ciphertext to rotate.
- **Plaintext fields.** The sweep migrates keys; it never newly encrypts history
  that was written in the clear.
- **Rows already on the active key**, which is what makes a re-run a no-op.

Because none of these carry a rotatable key id, none of them counts toward
`rows_remaining` — so none of them can block retirement forever.

## The append-only exception

Re-encryption mutates stored `harvest_events.event_data` bytes in place. That is
**sanctioned in-place mutation exception #3**, named alongside the other two in
the "Engine Invariants" section of the repository `CLAUDE.md`.

The scope guarantee that makes it safe: only the ciphertext bytes inside payload
fields change. The decoded plaintext is byte-identical before and after, and the
event `type`, variant structure, event ids, ordering and timestamps are never
touched — so replay determinism is unaffected **by construction**. It is proven
by `replay_fidelity_is_byte_identical_across_a_sweep`, which replays a fixture
history, runs the sweep, and replays again, asserting identical decoded
histories and `ReplaySucceeded` both times.

The sweep writes with a **compare-and-swap** on the row's previous bytes, so it
always loses a race against PII erasure or a heartbeat checkpoint. That is the
only safe direction: writing re-encrypted ciphertext over an erasure tombstone
would resurrect payload data the erasure had just destroyed.

## Troubleshooting

**`rows_remaining` is stuck above zero.** A row the sweep cannot decode is
logged (by row id and the key ids it references, never content), skipped, and
counted as *unresolved*. A pass that reaches the end of the shard with a
non-zero unresolved count resets its cursor to 0 and runs again instead of
being marked complete, so re-registering a key that was removed too early is
enough — the next pass picks those rows up with no manual intervention. A
cursor whose `completed_at` is set is therefore a real signal: that pass
converted everything it saw.

**`status` is `"partial"`.** A shard is unreachable. Retirement will refuse until
it is readable again; that is intended.

**Nothing is being swept.** Check that a keyed codec is registered
(`registered_key_ids` is non-empty), that `codec_rotation_batch_size` is not `0`,
and that the active key's codec is not the identity codec — the sweep refuses to
replace ciphertext with plaintext.
