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

## Addendum (issue #1253): the envelope nests, closing a collision

The flat envelope shape — `_harvest_codec_envelope` as a sibling key next to
`codec_id` and `data` — depends on an exact combination of keys, types, and
key count to mean "this is ciphertext." Business data can coincidentally
match that combination. #948's `kid` addition moved the version discriminator
from `1` to `2` specifically to avoid reinterpreting old four-key data, but a
*new* value shaped exactly like the version-2 envelope collided with it too: a
pre-#948 reader had rejected version 2 outright, so an identity deployment
could have legitimately stored such a value as plaintext. Every version bump
recreates this hazard one shape later — no integer choice removes it, because
the ambiguity is structural, not a matter of which number is reserved.

**The fix:** the envelope's only top-level key is `_harvest_codec_envelope`,
and its value is an object carrying `codec_id`, `data`, and an optional `kid`.
"Envelope-ness" now depends on one fact — is the discriminator the field's
*only* key — rather than an exact sibling-key combination. That is the same
convention this module's own `_harvest_undecodable` marker already used
safely; the codec envelope was the one reserved marker in this codebase that
did not nest, and that is what made it collide.

**Escaping, not just reshaping.** Nesting alone only protects *future* writes
if nothing can still slip a colliding value into storage unwrapped. The
identity codec's encode path used to return a non-envelope-shaped payload
completely unchanged — the fast path that made the original collision
possible. It now checks whether the payload is *already* shaped like a
recognized envelope (nested or a legacy flat one) before taking that
shortcut, and if so, wraps it in a nested envelope naming `codec_id:
"identity"` instead of storing it verbatim. Decoding reverses it the same way
as any other envelope, recovering the original value byte-identical. This
closes the collision **by construction** for everything written from here on,
not merely making it less probable.

**Compatibility (read-side migration, no data rewrite).** `harvest_events` is
append-only, so historical rows cannot be reshaped retroactively — decode
keeps recognizing both legacy flat shapes (version 1, three keys; version 2,
four keys with a `kid`) exactly as before, alongside the new nested shape.

**Nesting is scoped to the escape case only — a keyed write stays flat.**
An earlier version of this fix nested every write under a genuinely active
key too, reasoning that key activation was already fleet-gated by
`codec_rotation::activate_codec_key`. That reasoning missed a real rollout
hazard (caught in review): that gate only runs when an operator calls it —
`HarvestBuilder::payload_codec_key`/`active_payload_codec_key` register and
activate a key entirely locally, at process startup, with no fleet check at
all. A deployment already using that path when this fix ships would have
started emitting envelopes older binaries cannot parse the moment a single
worker upgraded, with no way to enforce reader-first ordering at all. Keyed
writes staying flat — byte-identical to every pre-#1253 envelope — avoids
this: there is nothing new to roll out in order, because nothing about a
keyed write's shape changes. Only the escape case is new, and it changes
behavior (a previously-corrupted value is now protected), not a shape
anything already depends on.

The common case — a single non-identity codec, key rotation never
configured — also keeps writing the pre-#948 flat bytes unchanged, so this
fix changes nothing for the large majority of deployments that were never
at risk. The residual collision risk for flat-shaped rows already on disk
before this fix is unavoidable and stays; only new writes through
`encode_payload` get the unconditional guarantee.

**The nested shape carries the same residual, on day one.** Recognizing a
new shape at all is itself a new discriminator over an already-populated
log: a row written by identity before this fix shipped, with no escape
guard yet to apply, that happens to already match the nested shape reads as
ciphertext now. That is not a new category of risk — every reserved marker
this crate has introduced (`_harvest_offload_envelope`, `_harvest_undecodable`)
carried it once, at the moment it started being recognized — but it means
"by construction" describes what this fix writes going forward, not
history that already exists.

**Rollout.** The fleet-readiness gate (`codec_rotation::activate_codec_key`)
is unchanged by this fix: it still requires version-2 (flat, keyed) support
before a keyed codec may be activated, because that is still the only shape
a keyed write ever produces. The collision-escape path is not gated at
all — it can fire on a deployment that never rotates a key — so a pre-#1253
reader hitting an escaped envelope during a mixed-binary rollout window
sees the wrapped object as literal data (wrong, but not destructive) rather
than an error. Accepted and documented rather than gated: triggering it
needs both the pre-existing collision (independently remote) and a live
mixed-binary window.

Test: `payload_codec.rs`'s
`a_flat_envelope_shaped_plaintext_is_escaped_on_encode_and_round_trips` stores
the exact four-key version-2 collision shape (this issue's own reproduction)
through `encode_payload` under the identity codec, and asserts it round-trips
byte-identical instead of being stored verbatim.

**The escape guard checks every depth, not only the field root.** The
operator lossy-decode path (`decode_value_lossy`) recurses into every object
and array looking for an envelope, so it can find a collision nested inside
an otherwise ordinary payload. The escape guard originally checked only the
payload root. A field like `{"child": {"_harvest_codec_envelope": {...}}}`
has an ordinary root, so it passed the guard unescaped, then got decoded or
marked undecodable by any reader that recursed — the same collision, one
level deeper than the guard was looking. Fixed by checking the whole payload
tree, root and every descendant, before the identity fast path bails out
(`payload_or_a_descendant_is_a_codec_envelope`). A collision anywhere in the
tree now escapes the whole field, matching how a recursive reader would
otherwise misread it.

Test: `a_descendant_envelope_shaped_field_is_also_escaped_on_encode` proves
the escape, the round trip, and that `decode_value_lossy` decodes the
escape wrapper once and never re-examines the recovered descendant.
