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

## Addendum (issue #1253): the envelope/business-data collision remains open

### The collision

`codec_envelope_parts` recognizes an envelope by JSON shape alone: three or
four specific keys, a version number, an optional `kid`. Under the identity
codec — the common, un-rotated case — `encode_payload` stores caller input
verbatim. Business data that happens to match a recognized shape is then
misread as ciphertext on the next read.

The sharpest case: a pre-#948 reader rejected the four-key keyed shape
outright. So that exact shape was legitimate stored plaintext before key
rotation existed. After #948 shipped, the same stored bytes read as an
envelope. Replay then fails with `UnknownCodecKey`. Worse, if an operator
later registers a key id matching the crafted `kid`, it decodes to garbage
that the rotation sweep re-encrypts permanently.

Bumping the version number only moves this hazard. The envelope stays
structurally indistinguishable from data that happens to match it, for any
version number chosen. Each future shape addition would reopen the same class
of bug, against data written before that addition existed.

### A write-time escape guard was attempted and reverted

A first attempt wrapped a value in a reserved escape marker at
`PayloadCodecs::encode_payload`'s identity-codec branch whenever it already
matched a reserved envelope shape, reversing the wrap on every read path.
Unit tests for `payload_codec.rs` in isolation passed. Full-suite CI did not:
several integration tests across `read_path_decode_integration.rs`,
`ui_integration.rs`, and `codec_rotation_db_tests.rs` failed, all tracing to
the same root cause.

**The root cause.** `encode_payload`'s identity branch is not exclusive to
genuinely fresh, never-before-touched caller input. `store::append_events`
(the plain, non-keyed writer) calls it on every event unconditionally, and
that single function is *also* the standard way this codebase's own test
suite seeds history that simulates a codec-encrypting deployment: a test
manually builds a real envelope via a real codec (`PayloadCodecs::encode_event`
with e.g. a `ReverseCodec`), then inserts that already-encoded value as a
literal event field through the plain identity-only `append_events`, relying
on identity's documented contract — "payloads are written to the database
exactly as they are passed... a safe fallback" — being an **unconditional,
exact** no-op. The escape guard broke that contract: it could not distinguish
"this is fresh plaintext that coincidentally looks like an envelope" (the
real hazard) from "this is a deliberately pre-built envelope being passed
through for storage" (a load-bearing, widely-used test-fixture pattern, and
plausibly a real pattern elsewhere history is copied or replayed). CI caught
it directly: a test asserting `stored event payload must remain ciphertext
byte-for-byte` found the stored row wrapped in the escape marker instead.

This is not a narrow bug fixable by tightening the guard's shape check —
it is a genuine ambiguity in what `encode_payload`'s single, shared identity
branch means at the point it runs: the codebase has no signal there for
"is this value fresh caller input, or a deliberate pass-through of bytes
that must not change." Resolving that needs either threading real
provenance through every write entry point (a much larger change than a
single function, and exactly what issue #1253's own option 3 was flagged as
impractical for), or the full re-nested-envelope migration (option 2). Both
are out of scope for this PR. The escape-guard code (and the tests and
`testing.rs` fixture-guard change built on top of it) has been reverted;
`payload_codec.rs`, `testing.rs`, and `codec_rotation_db_tests.rs` are
byte-identical to their pre-#1253 state again.

### Where this leaves issue #1253

Open, unresolved. The collision described above is real and still present,
exactly as it was before this investigation. What this pass adds is a
negative result worth recording: the cheapest-looking fix (escape at the one
identity passthrough site) does not work, because that site is not — despite
appearances — exclusively a fresh-write boundary. A future fix should start
from one of:

- **Option 2 from the original issue** — nest the whole envelope under one
  reserved key, with a real read-side migration for the four-key keyed shape
  already-rotated deployments depend on. Structural, not probabilistic;
  the only option the issue itself called a genuine fix.
- **Thread real provenance through the write boundary** — distinguish
  "fresh caller input" from "deliberate pass-through of already-final bytes"
  at every entry point that can supply either (workflow/activity/signal/
  update start, `append_events` callers building test or replay fixtures,
  `reset.rs`'s history-copy path). Large surface, correctness-critical to
  get every call site right.

Neither should be attempted as a small, incremental patch the way this pass
tried to.
