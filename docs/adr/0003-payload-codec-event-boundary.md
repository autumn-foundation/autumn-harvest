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

## Addendum (issue #1253): preventing new envelope/business-data collisions

**Scope, stated plainly up front.** This fix closes the collision for every
value the engine writes from here on. It does **not** fix a row already
written before this fix shipped — that is undecidable from the bytes alone,
and is documented below as a frozen, pre-existing risk, not resolved. Do
not read "the collision is prevented" as "old data is now safe."

### The collision

`codec_envelope_parts` recognizes an envelope by JSON shape alone: three or
four specific keys, a version number, an optional `kid`. Under the identity
codec — the common, un-rotated case — `encode_payload` used to store caller
input verbatim. Business data that happened to match a recognized shape was
then misread as ciphertext on the next read.

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

### The fix: escape at the one passthrough site

A fully re-nested envelope format — wrap the whole envelope under one
reserved key — would close the ambiguity for new data. It has a real cost: it
changes stored bytes for every rotated deployment, and needs a read-side
migration for the four-key keyed shape that already-rotated deployments
depend on today (pinned by `codec_rotation_db_tests.rs`). This fix does not
take that trade.

Instead, `codec_envelope_parts` stays frozen. It recognizes exactly the two
shapes it always has, so real rotation history keeps decoding unchanged. The
guard moves one layer up, to the one place caller-controlled bytes ever reach
storage un-transformed: `PayloadCodecs::encode_payload`'s identity-codec
branch. A real envelope is always engine-built from engine-chosen keys, so it
can never coincidentally equal business data. Only the passthrough branch can
produce the collision, so only there does the guard apply.

When a value about to be stored verbatim already matches a reserved envelope
shape — or the escape wrapper itself — it is wrapped once:
`{"_harvest_codec_escaped": <original>, "_harvest_codec_escaped_v": 1}`.
Every read path (`decode_payload`, `decode_value_lossy`,
`decode_error_string_lossy`) reverses exactly one layer before its normal
envelope check runs.

This is an escaping scheme, not a probability reduction. Encoding adds at
most one layer. Decoding removes exactly one layer when present. The two are
exact inverses for any JSON value, including plaintext that already happens
to look like an escaped value. The round trip holds by construction, not by
how unlikely a collision is.

**Why the wrapper carries two reserved keys, not one.** `decode_value_lossy`
(the operator read path) walks a whole record without knowing where payload
fields sit, and reverses the escape wrap at every depth it visits — it has
to, for the same reason `codec_envelope_parts` is shape-only rather than
field-position-aware. A single reserved key would then be an easier
accidental target for business data nested anywhere inside a field than the
three-or-four-key envelope shapes this fix protects against. Requiring two
specific keys to coincide keeps the same order of improbability the envelope
shapes already have, everywhere the wrapper is recognized — not only where
`encode_payload` itself applies it (a whole payload field's own top level,
never anything nested inside one).

### Migration / compatibility story

Nothing is rewritten. `codec_envelope_parts` recognizes the same two shapes
it always did, so every row written before this fix — real ciphertext and any
coincidental plaintext alike — decodes exactly as it did before.

What changes: no new instance of the collision can be created from this
release forward. Every value the engine writes from here on is either a real
engine-built envelope (never ambiguous) or already escaped if it would
otherwise collide.

**What does not change, stated without hedging: a row already written before
this fix is not fixed by this fix.** Business plaintext stored *before* this
fix shipped, shaped exactly like the four-key keyed envelope, stays as
ambiguous as it was the day #948 shipped. That is the same class of accepted
risk the three-key legacy shape has always carried, not a new one — but it is
real, and this PR does not close it. Closing it retroactively is undecidable
from the bytes alone — there is no oracle for "was this ever really
encrypted." A test pins this boundary directly rather than leaving it only
asserted in prose: `a_row_stored_before_this_fix_shipped_is_a_documented_residual_risk_not_a_fix`
in `payload_codec.rs` proves such a row still decodes as an `UnknownCodecKey`
error, exactly as it did before this PR. Fixing it for good needs a real
migration of stored history — issue #1253's option 2, re-nesting the whole
envelope format — which is a separate, larger change this PR deliberately
does not take on.

### Test evidence

`payload_codec.rs`'s `tests` module (search for "issue #1253") pins:

- The round trip for both envelope shapes, through
  `encode_payload`/`decode_payload`, through `decode_value_lossy` and
  `decode_error_string_lossy` (all three read paths), and through a full
  `encode_event`/`decode_event` cycle — the replay-fidelity form of the
  hazard.
- The sharpest variant: business data written under identity, decoded
  correctly *after* an operator later registers a key id that coincidentally
  matches the crafted `kid`.
- The escape wrapper's own two-key shape does not false-positive on a lone
  `_harvest_codec_escaped` field, at the top level or nested inside a lossy
  walk — and does not fire on the real-codec (non-identity) encode path at
  all.
- The residual-risk boundary above, and (in
  `codec_rotation_db_tests.rs`) that an escaped row is invisible to the SQL
  rotation census and sweep, the same as any other near-envelope.
- `testing.rs`'s replay-drift fixture guard now refuses a fixture holding an
  escaped field, the same way it already refuses one holding an undecoded
  codec envelope — a registry-free replayer cannot reverse either.
