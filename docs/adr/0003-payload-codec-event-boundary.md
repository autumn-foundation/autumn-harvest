# ADR 0003: Event payload codec boundary

## Status
Accepted

## Decision
Workflow event payload fields (`input`, `output`, `payload`, `details`) are serialized through a pluggable `PayloadCodec` and persisted with a `{ codec_id, data }` envelope.

## Consequences
- Non-default codecs can be introduced without schema changes.
- Replay fails fast with `UnknownPayloadCodec` if required codec is not registered.
- Default compatibility remains with `IdentityCodec`.

## Addendum (issue #608): operator read-path tolerant decode

The management API / Vantage UI read path consumes the same envelope contract
via `PayloadCodecs::decode_value_lossy` / `decode_error_string_lossy` — an
envelope-driven recursive walk sharing the exact envelope shape check with the
strict `decode_event` path, so the two can never disagree about what an
envelope is. Unlike replay, the read path degrades per field instead of
failing fast: an undecodable envelope is replaced with
`{"_harvest_undecodable": {"codec_id", "reason"}}` (bounded reasons, never
ciphertext or codec error text) so one bad key never blanks an operator's
triage screen. Read-path decoding is opt-in (default off), admin-gated,
audited (`payload.decode_read`), and never mutates stored bytes. See
`docs/operations/read-path-decode.md`.

## Addendum (issue #948): key rotation and lazy re-encryption

The envelope gains an **optional `kid`** (key id) alongside `codec_id`. The
registry holds many keyed codecs with exactly one marked active: new writes use
the active key, decode resolves any registered key, and an envelope carrying no
`kid` is *defined* to be under the designated legacy key id — so pre-#948 rows
decode unchanged and an un-rotated deployment's stored bytes are byte-identical.

Rotation state lives behind a shared cell, so flipping the active key takes
effect for every clone of the registry the instant it returns; there is no
restart-ordering window in which a pre-flip writer keeps using the retired key.

A shard-local, batched, resumable sweep (`codec_rotation.rs`, a resident of the
existing timeout-scanner cadence) converts stored rows onto the active key, and
retirement of an old key is refused until a fail-closed per-shard census proves
zero remaining rows. The sweep mutates `harvest_events.event_data` in place —
**sanctioned exception #3**, see `CLAUDE.md` — changing only the ciphertext bytes
inside payload fields, with a compare-and-swap that makes it lose any race
against PII erasure. See [`docs/operations/codec-key-rotation.md`](../operations/codec-key-rotation.md).
