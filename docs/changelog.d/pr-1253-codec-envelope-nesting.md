## Phase — Codec envelope nests, closing the business-data collision (issue #1253)

Found by automated review on autumn-foundation/autumn-harvest#1242 and filed
separately: the flat codec envelope (`_harvest_codec_envelope` as a sibling
key next to `codec_id`/`data`/`kid`) was structurally indistinguishable from
business data that happened to carry the same keys. #948 moved the version
discriminator from 1 to 2 to avoid reinterpreting old four-key plaintext, but
a *new* value shaped exactly like the version-2 envelope collided with it
too — bumping the version relocates this hazard rather than removing it,
because the ambiguity is structural, not a matter of which integer is
reserved.

**The fix (ADR-0003 addendum).** The envelope's only top-level key is now
`_harvest_codec_envelope`, mapping to an object carrying `codec_id`, `data`,
and an optional `kid`. "Envelope-ness" now depends on one fact — is the
discriminator the field's only key — rather than an exact sibling-key
combination, the same convention this module's own `_harvest_undecodable`
marker already used safely.

**Escaping, not just reshaping.** `PayloadCodecs::encode_payload`'s identity
fast path used to return a non-envelope-shaped payload completely unchanged —
the shortcut that made the original collision possible. It now checks
whether the payload is *already* shaped like a recognized envelope (nested or
a legacy flat one) before taking that shortcut, and if so, wraps it in a
nested envelope naming `codec_id: "identity"` instead of storing it verbatim.
Decoding reverses it like any other envelope, recovering the original value
byte-identical. This closes the collision by construction for everything
written from here on.

**Compatibility, no data rewrite.** `harvest_events` is append-only, so
historical rows keep their shape — decode still recognizes both pre-#1253
flat shapes (version 1, three keys; version 2, four keys with `kid`)
alongside the new nested one.

**Nesting is scoped to the escape case only — a keyed write stays flat.**
Review caught a rollout hazard in an earlier version of this fix that also
nested every keyed write: `codec_rotation::activate_codec_key`'s
fleet-readiness gate only runs when an operator calls it, but
`HarvestBuilder::payload_codec_key`/`active_payload_codec_key` register and
activate a key entirely locally at startup, with no fleet check at all. A
deployment already using that path would have started emitting envelopes
older binaries cannot parse the moment one worker upgraded. Keyed writes
now stay flat and byte-identical to pre-#1253 — nothing about their shape
changes, so nothing new needs a rollout order. Only the escape case is new
shape at all, and it is ungated by necessity (see below), not by oversight.
The common case — a single non-identity codec, rotation never configured —
also keeps writing the pre-#948 flat bytes unchanged.

**Rotation surface updated for the nested shape.** The SQL census predicate
(`codec_rotation::db::ENVELOPE_PREDICATE`) gained a third branch mirroring
the nested shape, kept in step with `codec_envelope_parts` by
`a_nested_envelope_is_counted_and_swept` /
`a_nested_near_envelope_is_neither_counted_nor_swept` — the former exercises
identity registered under a named rotation key (a supported "store in the
clear" configuration `codec_rotation`'s own doc already describes), proving
an escaped value gets genuinely encrypted once real rotation converts it.
The worker fleet-readiness gate (`activate_codec_key`) is unchanged: it
still requires version-2 (flat, keyed) support, because that is still the
only shape a keyed write ever produces.

**Residual, documented gaps.** Three, all accepted rather than gated:

1. The collision-escape path is not fleet-gated — it can fire on a
   deployment that never rotates a key at all. A pre-#1253 reader hitting
   an escaped envelope during a mixed-binary rollout window sees the
   wrapped object as literal data (wrong, but not destructive), not an
   error. Triggering it needs both the pre-existing collision
   (independently assessed as remote) and a live mixed-binary window.
2. Recognizing the nested shape at all is a new discriminator over an
   already-populated log: a row written by identity before this fix
   shipped, with no escape guard yet to apply, that happens to already
   match the nested shape reads as ciphertext now — the same category of
   risk the flat shapes already carried for rows written before #1253, not
   a new one. Every reserved marker this crate has introduced carried this
   exact risk once, at the moment it started being recognized.
3. `harvest_workflow_executions.input`/`output`, and similar denormalized
   queue and dead-letter columns, are populated directly from
   caller-supplied values and never go through `encode_payload` at all —
   `decode_value_lossy` can misread a coincidental collision there the same
   way it can on `harvest_events`. This predates #1253 and applied to the
   legacy flat shapes too. An escape guard on `encode_payload` cannot close
   it, because these columns never reach `encode_payload` to escape
   through; they stay directly queryable by design, and a codec envelope
   would break that. Out of scope for this fix.

The first two are not fixable without rewriting stored history, which the
append-only invariant forbids outside the two sanctioned exceptions in
`CLAUDE.md` (unchanged by this fix — still exactly two). The third is a
separate, larger design decision about which storage surfaces the codec
covers at all.

**The escape guard checks every depth, not only the field root.** The
operator lossy-decode path (`decode_value_lossy`) recurses into every
object and array looking for an envelope. The escape guard originally
checked only the payload root, so a field with an ordinary root but an
envelope-shaped descendant passed unescaped, then got decoded or marked
undecodable by any reader that recursed — the same collision, one level
deeper. Fixed by checking the whole payload tree before the identity fast
path bails out (`payload_or_a_descendant_is_a_codec_envelope`); a collision
anywhere in the tree now escapes the whole field.

No new `WorkflowEvent` variant, no migration, no change to the
adjacently-tagged event JSON contract.

Tests: 7 new `payload_codec.rs` unit tests (flat- and nested-shaped
collision escape and round-trip, a descendant-shaped collision escape and
round-trip, ordinary-payload non-interference, nested shape strictness
including malformed nested `kid`, the lossy read path against a nested
envelope, un-rotated real-codec byte-identity); 2 new
`codec_rotation_db_tests.rs` integration tests pinning the SQL predicate's
new nested branch against a real sweep — one exercising identity registered
under a rotation key, proving an escaped value gets genuinely encrypted
once real rotation converts it. Keyed-write tests are unchanged: nesting
never reaches that path.
