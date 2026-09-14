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
alongside the new nested one. Encoding is scoped narrowly: a write nests only
when a key is genuinely rotated active, or the collision-escape guard fires.
The common case — a single non-identity codec, rotation never configured —
keeps writing the pre-#948 flat bytes unchanged, so this fix changes nothing
for the deployments that were never at risk.

**Rotation surface updated in step.** The SQL census predicate
(`codec_rotation::db::ENVELOPE_PREDICATE`) gained a third branch mirroring
the nested shape, kept in step with `codec_envelope_parts` by
`a_nested_envelope_is_counted_and_swept` /
`a_nested_near_envelope_is_neither_counted_nor_swept`. The worker
fleet-readiness gate (`activate_codec_key`) now requires
`CODEC_ENVELOPE_VERSION_NESTED` support before a keyed codec may be
activated, replacing the version-2 requirement #948 introduced — same
rollout-ordering discipline, new version number.

**Residual, documented gap.** The collision-escape path is not fleet-gated
the way key activation is — it can fire on a deployment that never rotates a
key at all. A pre-#1253 reader hitting an escaped envelope during a
mixed-binary rollout window sees the wrapped object as literal data (wrong,
but not destructive) rather than an error. Accepted rather than gated:
triggering it needs both the pre-existing collision (independently assessed
as remote) and a live mixed-binary window. Flat-shaped rows already on disk
before this fix keep their pre-existing, un-eliminated collision risk — there
is no way to close that retroactively without rewriting stored history, which
the append-only invariant forbids outside the two sanctioned exceptions in
`CLAUDE.md` (unchanged by this fix — still exactly two).

No new `WorkflowEvent` variant, no migration, no change to the
adjacently-tagged event JSON contract.

Tests: 6 new `payload_codec.rs` unit tests (flat- and nested-shaped
collision escape and round-trip, ordinary-payload non-interference, nested
shape strictness, un-rotated real-codec byte-identity); 8 existing
`payload_codec.rs` tests updated for the nested write shape; 2 new
`codec_rotation_db_tests.rs` integration tests pinning the SQL predicate's
new branch against a real sweep.
