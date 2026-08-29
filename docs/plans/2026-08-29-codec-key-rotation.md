# Plan — Payload codec key rotation with lazy re-encryption sweep (issue #948)

Status: implementation plan (TDD: red → green → refactor)

## 1. Brainstorming — candidate approaches

Options generated before narrowing:

1. **Key id inside the codec envelope (`kid`), multi-key registry, background sweep.**
   The envelope is already opaque payload content, so a fourth key rides inside it with
   zero change to the adjacently-tagged `WorkflowEvent` JSON contract.
2. **New `codec_key_id` column on `harvest_events`.** Rejected: a payload field is
   per-field, not per-row (one event can carry `input` *and* `last_completion_result`),
   and it costs a wide migration on the hottest table.
3. **Encode the key id in `codec_id` itself** (`"aes-gcm:k2"`). Rejected: `codec_id` is
   `&'static str` from the trait; the embedder cannot mint one per key at runtime, and it
   silently overloads a field other subsystems already pattern-match on
   (`version_gate_retirement`'s SQL, the replay-drift fixture guard).
4. **Re-encrypt eagerly on read.** Rejected: read paths must not write; it never converges
   (cold history is never read), so retirement never becomes safe.
5. **Export/transform/reimport tool.** The status quo the issue exists to kill.
6. **Sweep as a standalone daemon.** Rejected: the repo's every other background duty is a
   resident of the shard-local scanner tick (`enforce_timeouts_once`). A second cadence is
   a second thing to operate, monitor and get wrong.
7. **Cursor carrying the target key id.** Chosen, but *not* by keying the row on
   `(shard_id, active_key_id)` — review showed that resumes a rolled-back-to key's
   already-completed cursor and silently skips every row written under the key being
   rolled back *from*. The row is keyed on `shard_id` alone with the target key as a
   column, so ANY change of active key — rotate, re-rotate, or roll back — restarts the
   scan.
8. **Compare-and-swap update rather than lock-and-update.** Chosen — see reverse
   brainstorming R1.
9. **Full-scan census for the retirement gate** vs. a maintained counter. Chosen: full
   scan. A counter is a second source of truth that can drift; the gate is an
   operator-invoked, admin-gated read that runs a handful of times per rotation.

Selected: **1 + 6 + 7 + 8 + 9.**

## 2. Reverse brainstorming — how would we make this *fail*?

Each failure mode below is a real hazard the design must foreclose, and each maps to a test.

- **R1. Resurrect erased PII.** The sweep does read-modify-write on `event_data`. PII
  erasure (#495) does the same. Sweep reads ciphertext → erase tombstones and commits →
  sweep writes its re-encrypted copy back, *resurrecting the payload erasure just
  destroyed.* Foreclosed by making the sweep's write a **compare-and-swap**
  (`UPDATE ... WHERE id = $1 AND event_data = $2::jsonb`): the sweep always loses a race,
  which is the only safe direction. Same guard covers heartbeat checkpoints and a second
  concurrent sweeper.
- **R2. Silent plaintext.** A codec that fails to encode mid-sweep, or a `kid` we cannot
  resolve, must never cause a row to be written as plaintext. Foreclosed by never writing
  a row whose re-encode did not fully succeed (per-row error → skip and keep going, never
  a partial field write), and by never encrypting a row we could not first decode.
- **R3. Break replay determinism.** Foreclosed by construction (only ciphertext bytes
  inside payload fields change) *and* proven: replay-fidelity test asserting byte-identical
  decoded history and `ReplaySucceeded` before and after.
- **R4. Retire a key that is still load-bearing.** A shard we cannot reach reports no rows;
  a naive gate reads that as zero and lets the operator delete the key. Foreclosed by
  fail-closed accounting: an unreachable shard is a *blocker*, never a zero.
- **R5. A write slips through under the old key after the flip.** If the active key lives
  in a cloned-by-value registry, the clone the worker captured at boot keeps encrypting
  under the old key forever. Foreclosed by putting rotation state behind a shared
  `Arc<RwLock<..>>` so every clone observes the flip atomically.
- **R6. Double encryption.** An offloaded payload field holds a *reference envelope*
  (offload composes after encode). Re-encoding it would encrypt the reference and orphan
  the blob. Foreclosed by skipping offload envelopes; likewise erasure tombstones, which
  hold no ciphertext.
- **R7. Sweep never terminates / thrashes.** Foreclosed by an id-ordered durable cursor,
  a bounded batch, and idempotency (a row already under the active key is skipped, so a
  re-run is a no-op).
- **R8. Break every existing deployment's stored bytes.** Foreclosed by emitting `kid`
  only when the active key id is not the legacy sentinel — an un-rotated deployment's
  envelopes stay byte-identical, and a `kid`-less envelope is *defined* as the legacy key
  id, so pre-upgrade rows decode unchanged.
- **R9. Unbounded metric cardinality.** `harvest.codec.reencrypted` is labelled by `shard`
  only. Key ids are operator-chosen and bounded, but the census belongs on the admin read,
  not on a metric label — mirroring `harvest.quota.rejected` (#946).
- **R10. Cost the un-rotated 99% anything.** Foreclosed by an early return: with no keyed
  registry configured, the sweep touches no connection at all.

## 3. Six hats

- **White (facts).** `PayloadCodecs` is a `default` codec plus a `codec_id`-keyed map;
  the envelope is exactly three keys and the shape check is single-sourced in
  `codec_envelope_parts`. `harvest_events` is append-only with two sanctioned in-place
  exceptions today (`queue::record_heartbeat`, `erase.rs` #495). Every background duty is a
  resident of `timeout::enforce_timeouts_once`. Offload composes after encode. Cross-shard
  admin reads already have a fail-closed scaffold (`shard_fanout`).
- **Red (instinct).** In-place mutation of the event log is the scariest thing in this
  codebase, and this is the third exception. The discomfort is warranted and should be
  answered with a *proof*, not a paragraph — hence the replay-fidelity test as a
  non-negotiable deliverable, and the loud documentation of the exception.
- **Black (risks).** The erase/sweep race (R1) is the P1 nobody would notice in review; the
  fail-open retirement gate (R4) is the one that turns a compliance feature into a
  compliance incident. The census is a sequential scan of the largest table — acceptable
  only because it is admin-gated and rotation-scoped, and it must be documented as such.
  Threading `PayloadCodecs` into `enforce_timeouts_once` widens an already-wide signature.
- **Yellow (upside).** Harvest owns its storage, so unlike Temporal it can actually
  *finish* a rotation and prove it. The cursor-per-active-key design makes "rotate again"
  and "roll back" free. Zero new event variants, zero event-contract change, one migration.
- **Green (creativity).** Key the cursor on `(shard, active_key_id)` so the flip resets
  progress implicitly. Make the sweep's write a CAS so it is safe against every other
  writer without a new lock. Define "absent `kid`" as the legacy key id so the migration
  of stored bytes is *lazy and optional* rather than required.
- **Blue (process).** TDD per AC: red test → minimal green → refactor. Order: envelope +
  registry (AC1-3), sweep + cursor + composition (AC4, AC8), fidelity proof (AC5),
  retirement gate (AC6), admin route + metric (AC7), docs (AC5 loud flag), then
  multi-angle review.

## 4. Design

### 4.1 Envelope (AC1)

```
{"_harvest_codec_envelope":1,"codec_id":"aes-gcm","kid":"2026-q3","data":"<base64>"}
```

`kid` is optional. `codec_envelope_parts` accepts an object of exactly 3 keys (no `kid`)
or exactly 4 keys where the fourth is a string `kid`; anything else is not an envelope, so
the pre-existing strictness against near-envelopes is preserved. An absent `kid` **is**
`CODEC_LEGACY_KEY_ID` (`"legacy"`), and the encoder omits `kid` when the active key is the
legacy sentinel — so an un-rotated deployment's stored bytes are unchanged.

### 4.2 Registry (AC2, AC3)

`PayloadCodecs` gains `keyed: Arc<RwLock<KeyRegistry>>` (`keys: BTreeMap<String, Arc<dyn
PayloadCodec>>`, `active: String`). Because the cell is shared, **every clone of the
registry observes an active-key flip immediately** — there is no restart-ordering window
in which a worker that cloned the registry at boot keeps writing under the retired key.

- `register_key(key_id, codec)` — first key registered becomes active.
- `set_active_key(key_id)` — errors unless registered. Exactly one key is active.
- `codec_for_key`, `active_key_id`, `registered_key_ids`, `retire_key_local`.
- Decode resolution for `(codec_id, kid)`: `keys[kid_or_legacy]`, else the pre-existing
  `codecs[codec_id]` map (so today's `register()` + `set_default()` deployments keep
  working verbatim), else typed `UnknownCodecKey` (strict) / `unknown_key` undecodable
  marker (lossy read path).

### 4.3 Sweep (AC4, AC8)

New module `codec_rotation.rs`.

Pure core (no DB): `reencrypt_event_payload_fields(codecs, &mut event_value)` walks the same
`PAYLOAD_FIELD_KEYS` the codec/erasure/export paths share. Per field:
skip erasure tombstones; skip offload envelopes; skip non-envelopes (plaintext is not
"carrying a non-active key id"); skip envelopes already on the active key (idempotence);
otherwise decode with the resolved old key and re-encode with the active key.

DB core: `sweep_codec_reencryption_once(conn, shard_id, codecs, batch_limit, metrics)`.
Returns 0 without touching the connection when no keyed registry is configured. Reads
`id > cursor ORDER BY id LIMIT batch`, re-encrypts in memory, writes with a
**compare-and-swap** on the previous `event_data`, advances the durable cursor, records
`harvest.codec.reencrypted{shard}`. Folded into `enforce_timeouts_once`.

Migration `20260726000000_harvest_codec_rotation_cursor`:
`(shard_id, active_key_id)` primary key, `last_event_id`, `rows_reencrypted`,
`completed_at`, `updated_at`.

### 4.4 Retirement gate (AC6)

`count_rows_by_key_id(conn)` — per-shard census over `harvest_events` payload fields,
grouping `kid` (absent → legacy). `retire_codec_key(sharded_pool, expected_shards, codecs,
key_id)` refuses with `HarvestError::CodecKeyRetirementBlocked { key_id, remaining }`
naming the per-shard remaining count, and treats an unreachable shard as a blocker with an
explicit `unreachable` marker rather than a zero.

### 4.5 Observability (AC7)

`GET /admin/codec/rotation` — admin-gated, read-only, fanned out via `shard_fanout` with
`complete`/`partial`/`unavailable` status; registered in `management_api_routes()` and
`docs/api-contract.json`. Metric `harvest.codec.reencrypted{shard}`.

### 4.6 Documentation obligation (AC5)

`CLAUDE.md` gains an **Engine Invariants** section naming all three sanctioned in-place
mutation exceptions; `erase.rs`'s "only sanctioned" wording is corrected; ADR-0003 gains a
rotation addendum; `docs/operations/codec-key-rotation.md` is the operator runbook.

## 5. Pre-existing gap found during implementation (NOT introduced here, NOT fixed here)

While wiring the sweep it became clear that **no production write path applies a
configured `PayloadCodec` at all**. `store::events_to_insert_rows_from_with_codecs`
is the only encode entry point, and before this change its only two callers
(`events_to_insert_rows` / `events_to_insert_rows_from`) both passed
`PayloadCodecs::default()` — the identity registry. The worker's replay loader
does the same (`worker.rs`'s `load_history_inflated(..., &PayloadCodecs::default(), ...)`),
so the two are at least self-consistent. The configured registry reaches only the
**read** paths (`WorkflowHandleClient`, the management API), which is what ADR-0003's
issue #608 addendum plumbed.

Consequence: on today's engine, an embedder who calls `.payload_codec(AesGcmCodec)`
gets plaintext in `harvest_events` regardless. That is an ADR-0003 write-path
defect, not a rotation defect, and fixing it means threading the registry through
every `append_events` / `load_history` call site in `worker.rs` and `executor.rs`
plus a compatibility story for deployments that configured a codec and have
plaintext history — a materially larger change with its own migration and review
bar than issue #948 describes.

This PR does the part that belongs to it: `store::append_events_with_codecs` is
added as the public, codec-aware write seam (`append_events` now delegates to it
with the identity registry, so behaviour is byte-for-byte unchanged), and every
rotation primitive here operates correctly on whatever the write path stores. The
write-path defect is tracked separately.

## 6. Review outcomes

Four review agents (correctness/concurrency, security/crypto, API-contract/docs, Rust
idiom/clippy/perf) ran against the first implementation. What they changed:

- **Cursor keyed on `(shard, key)` was wrong for rollback** — re-keyed on `shard` with the
  target key as a column, so any active-key change restarts the pass (plan item 7 above,
  corrected).
- **A row the sweep could not convert was abandoned behind the cursor forever** — a
  decode failure or a lost compare-and-swap is now counted as `unresolved_rows`, and a
  pass that ends with a non-zero count resets to 0 and runs again rather than stamping
  `completed_at`. That is what makes "re-register the key you removed too early" actually
  work, and what makes `completed_at` mean something.
- **A concurrent `set_active_key` could make the sweep write plaintext** — the check was
  a snapshot taken before the encode. `encode_payload_under` now refuses an identity
  codec *at the point of use* and pins one key id for a whole row.
- **A `kid` read back out of storage was untrusted** — on an identity deployment a
  caller's workflow input is stored verbatim, so envelope-shaped input could inject
  unbounded attacker-chosen key ids into the census, the admin response, and the logs,
  and keep `rows_remaining` permanently non-zero. The stored `kid` is now held to the
  same charset/length rule as a registered one, in Rust and in SQL.
- **The offload skip used the strict parser** (`extract_offload_ref`) rather than the
  discriminator, so a malformed offload envelope was mis-classified. Caught by my own
  unit test; `is_offload_envelope` is now the predicate.
- **The census ran unconditionally** — `GET /admin/codec/rotation` did a full-table scan
  per shard even with no keyed codec registered. It now short-circuits.
- **The retirement gate trusted the caller's shard list** — it now refuses a list that
  omits a shard this process has a pool for, and refuses an unregistered key id.
- **A per-shard sweep error aborted the whole scanner tick** — now logged and skipped, as
  its own doc always claimed.
- **`record_heartbeat` does not write `harvest_events`** — the "three exceptions" framing
  in both `erase.rs` and issue #948 itself is inaccurate. `CLAUDE.md` now says so.

Two limitations the reviews surfaced that are **real but out of this issue's scope**, and
are documented rather than fixed (see the runbook's "What zero does and does not
authorise"): the gate censuses `harvest_events` only, so offloaded blobs (#524),
codec-encoded columns outside the event log, and nested envelopes are not covered by its
zero.
