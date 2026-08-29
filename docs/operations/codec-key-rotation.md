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
a pass costs one visit per row on the shard. At the default 200 rows per tick
and a 5-second scanner interval that is roughly 40 rows/second, so a 1M-row
shard converges in about seven hours. Raise the batch to convert faster (a
first pass over a large corpus is the case that wants a big number); it is safe
to change at any time, and once the first pass reaches the end of the shard the
cursor keeps it cheap forever after — later ticks only look at rows appended
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
autumn_harvest::codec_rotation::retire_codec_key(
    &sharded_pool,
    &expected_shards,
    &codecs,
    CODEC_LEGACY_KEY_ID,
).await?;
```

It refuses with `HarvestError::CodecKeyRetirementBlocked` naming the remaining
count **per shard** while any row is left, and succeeds only at exactly zero
everywhere. It is **fail-closed** in three separate ways, all deliberate:

- a shard with no connection pool in this process blocks retirement;
- a shard whose census errors blocks retirement;
- an empty shard list is refused outright — proving nothing is not proving zero.

Only after a successful retirement should you dispose of the key material
itself. Harvest never holds it.

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
logged (by row id only, never content) and skipped, and it keeps the retirement
gate correctly blocked. The usual cause is a key that was removed from the
registry before its rows were converted — re-register it and let the sweep
finish.

**`status` is `"partial"`.** A shard is unreachable. Retirement will refuse until
it is readable again; that is intended.

**Nothing is being swept.** Check that a keyed codec is registered
(`registered_key_ids` is non-empty), that `codec_rotation_batch_size` is not `0`,
and that the active key's codec is not the identity codec — the sweep refuses to
replace ciphertext with plaintext.
