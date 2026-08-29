## Phase — Payload codec key rotation with lazy re-encryption sweep (issue #948)

Adds a key-rotation story to the `PayloadCodec` boundary (ADR-0003). Before
this, encryption at the codec boundary was permanent: `harvest_events` is
append-only, so after a key compromise every byte of stored history stayed
encrypted under the compromised key forever, and the only escape was a bespoke
export/transform/reimport nobody runs under incident pressure.

**Envelope and registry.** The codec envelope gains an optional `kid` key id
alongside `codec_id`. `PayloadCodecs` now holds many keyed codecs with exactly
one marked active: new writes encode under the active key, decode resolves any
registered key (so a mixed-key history replays transparently through the
migration window), and an envelope with **no** `kid` is *defined* to be under
the designated legacy key id — pre-upgrade rows decode unchanged, and while the
legacy key is active no `kid` is written at all, so an un-rotated deployment's
stored bytes are byte-identical to pre-#948. Rotation state lives behind a
shared `Arc<RwLock<_>>`, so `set_active_key` is observed by every clone of the
registry the instant it returns — there is no restart-ordering window in which a
worker that booted before the flip keeps writing under the retired key.

**Sweep.** New `codec_rotation.rs`: a shard-local, batched, rate-limitable,
idempotent, resumable re-encryption sweep, folded into `enforce_timeouts_once`
alongside the other scanner residents. Its durable cursor is keyed on
`(shard_id, active_key_id)` rather than `shard_id`, so flipping the key starts a
fresh pass and a rollback resumes its own — no reset step an operator can
forget. One migration (`20260726000000_harvest_codec_rotation_cursor`). The
sweep returns without issuing a statement unless a keyed codec is registered, so
every deployment that has not adopted rotation pays nothing.

**⚠️ Sanctioned in-place mutation exception #3.** Re-encryption mutates stored
`harvest_events.event_data` bytes in place. `CLAUDE.md` gains an "Engine
Invariants" section naming all three exceptions (heartbeat checkpoints, PII
erasure #495, and this) with each one's scope guarantee. The guarantee here:
only the ciphertext bytes inside payload fields change — decoded plaintext is
byte-identical before and after, and event `type`, variant structure, event ids,
ordering and timestamps are never touched — so replay determinism is unaffected
*by construction*. Proven by `replay_fidelity_is_byte_identical_across_a_sweep`,
which replays a fixture history, sweeps, and replays again, asserting identical
decoded histories and `ReplaySucceeded` both times.

The sweep's write is a **compare-and-swap** on the row's previous `event_data`,
so it always loses a race against exception #1 or #2. That closes the P1 this
design's own reverse-brainstorm surfaced: a sweep that read a row before a PII
erasure tombstoned it would otherwise write its re-encrypted copy back over the
tombstone and resurrect payload data the erasure had just destroyed.

**Composition (#524, #495).** Offload reference envelopes are passed through
untouched (offload composes *after* encode, so the field holds a reference, not
ciphertext; re-encoding it would orphan the blob), erasure tombstones are
skipped as having no ciphertext, and plaintext is never newly encrypted. None of
the three carries a rotatable key id, so none of them counts toward
rows-remaining and none can block retirement forever.

**Retirement gate.** `retire_codec_key` refuses with a typed
`HarvestError::CodecKeyRetirementBlocked` naming the remaining count per shard
while any row references the key, and succeeds only at exactly zero everywhere.
Fail-closed three ways: a shard with no pool blocks, a shard whose census errors
blocks, and an empty shard list is refused outright — proving nothing is not
proving zero.

**Observability.** `GET /admin/codec/rotation` (admin-gated, read-only,
registered in `management_api_routes()` and `docs/api-contract.json`) reports
per-shard rows-remaining per key id plus the sweep cursor, degrading to
`status: "partial"` on an unreachable shard where the total is a lower bound,
never a count. Metric `harvest.codec.reencrypted{shard}` counts swept rows; the
key id is deliberately not a label (it would grow a series per rotation and
never retire one) — per-key detail lives on the admin read, mirroring
`harvest.quota.rejected` (#946).

**No new `WorkflowEvent` variant, no change to the adjacently-tagged event JSON
contract, one migration.**

Tests: 13 new `payload_codec.rs` unit tests (envelope `kid`, kid-less legacy
resolution, mixed-key decode, atomic flip across clones, malformed key ids,
typed vs. lossy unknown-key handling, near-envelope strictness, `codec_id`
back-compat fallback, active-key retirement refusal); 11 new `codec_rotation.rs`
unit tests (per-field skip rules, idempotence, all-or-nothing row semantics,
plaintext byte-identity, structural untouchability); and
`codec_rotation_db_tests.rs` covering the sweep end to end against a real
Postgres — batching, cursor resume, re-rotation, the erasure race, offload/
tombstone composition, the fail-closed retirement gate, progress reporting, the
metric, and the replay-fidelity proof.
