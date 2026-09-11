## Phase — Prevent new codec-envelope/business-data collisions at write time (issue #1253)

**Scope up front.** This closes the collision for every value the engine
writes from here on. It does not, and cannot, fix a row already written
before this fix shipped — see "Migration / compatibility" below.

Filed against `codec_envelope_parts` (`payload_codec.rs`): the shape check
that recognizes a codec envelope, shared by strict replay decode and the
operator lossy read path, classifies a payload field by JSON shape alone.
Under the identity codec — the common, un-rotated case — `encode_payload`
stored caller input verbatim. Business data that happened to match a
recognized envelope shape was then misread as ciphertext on the next read.

The sharpest instance: a pre-#948 reader rejected the four-key keyed
envelope shape (`_harvest_codec_envelope: 2` plus `codec_id`, `data`,
`kid`) outright, so that exact shape was legitimate stored plaintext
before key rotation existed. After #948 shipped, the same stored bytes
read as an envelope — replay fails with `UnknownCodecKey`, or, if an
operator's key id happens to match the crafted `kid`, decodes to garbage
the rotation sweep re-encrypts permanently. Filed rather than fixed
against #1242 (which closed the equivalent four-key **version-1**
collision): the real fix changes ADR-0003's envelope representation
rather than anything rotation-specific.

**The fix.** `codec_envelope_parts` stays frozen — it recognizes exactly
the two shapes it always has, so already-rotated deployments keep
decoding real version-2 history unchanged. The guard moves one layer up,
to the one place caller-controlled bytes ever reach storage
un-transformed: `PayloadCodecs::encode_payload`'s identity-codec branch. A
value about to be stored verbatim that already matches a reserved
envelope shape — or the escape wrapper itself — is wrapped once:
`{"_harvest_codec_escaped": <original>, "_harvest_codec_escaped_v": 1}`.
Every read path (`decode_payload`, `decode_value_lossy`,
`decode_error_string_lossy`) reverses exactly one layer before its normal
envelope check runs. This is an escaping scheme, not a probability
reduction: encoding adds at most one layer, decoding removes exactly one
when present, and the two are exact inverses for any JSON value,
including plaintext that already happens to look like an escaped value.
The round trip holds by construction.

**Two reserved keys, not one.** `decode_value_lossy` walks a whole record
without knowing payload field boundaries, so it reverses the escape wrap
at any depth — a single reserved key would be an easier accidental target
for business data nested inside a field than the envelope shapes this fix
protects against. The wrapper's second key raises the bar back to the
same order of improbability the envelope shapes already have (review
finding: correctness/security angle).

A fully re-nested envelope format (wrap the whole envelope under one
reserved key) was considered and rejected for this PR: it changes stored
bytes for every rotated deployment and needs a read-side migration for
the four-key keyed shape real rotation deployments already depend on
(pinned by `codec_rotation_db_tests.rs`). The escape guard gets the same
by-construction guarantee for all new writes with zero migration risk.

**Migration / compatibility — read this before treating #1253 as closed.**
Nothing is rewritten. Every row written before this fix decodes exactly as
it did before. What changes: no new instance of the collision can be
created from this release forward.

What does **not** change: a row already written before this fix is not
fixed by this fix. Business plaintext stored *before* this fix, shaped
like the four-key keyed envelope, is a frozen, documented residual risk —
the same class of accepted risk the three-key legacy shape has always
carried, not a new one, but not resolved either. Retroactively
disambiguating it is undecidable from the bytes alone. A test pins this
boundary directly:
`a_row_stored_before_this_fix_shipped_is_a_documented_residual_risk_not_a_fix`.
Closing it for good needs issue #1253's option 2 (re-nest the whole
envelope format under one reserved key, with a real migration) — a
separate, larger change. See the ADR-0003 issue #1253 addendum for the
full design rationale.

**Review.** Developed red/green/refactor: three tests reproducing the
exact collision were confirmed failing (`UnknownCodecKey`/
`UnknownPayloadCodec`) before the escape guard was added, then passing
after. Reviewed from three independent angles (correctness/security,
backward-compatibility/migration honesty, test coverage); each surfaced
real findings that were fixed before this PR was opened:

- **Correctness/security** — found that the lossy read path's recursive
  unescape check was scoped more permissively than the write-time guard
  (which only ever escapes a whole payload field, never anything nested),
  making the single-key wrapper an easier accidental target for nested
  business data than the envelope shapes it protects against. Fixed by
  requiring a second reserved companion key on the wrapper, restoring the
  same order of improbability everywhere it is checked. Also found the
  `testing.rs` replay-drift fixture guard did not recognize an escaped
  field as opaque, risking a false-drift report; fixed by teaching
  `codec_opaque_fixture_reason` the new shape.
- **Backward-compatibility/migration honesty** — found the original
  framing ("close the collision") overclaimed relative to what the fix
  delivers for already-stored data, and that no test pinned the
  documented residual-risk boundary. Fixed by rewording every doc/commit
  to state the forward-only scope up front, and by adding the residual-risk
  test named above.
- **Test coverage** — found `a_registered_key_matching_the_crafted_kid_...`
  did not exercise the identity-passthrough path it claimed to (the first
  `register_key` call auto-activates, so encoding took the real-encryption
  branch), and that neither `decode_value_lossy` nor
  `decode_error_string_lossy` had a test for a real escaped top-level
  field. Fixed by reordering the test to write under identity first, and
  by adding both missing tests plus a DB-level census/sweep test in
  `codec_rotation_db_tests.rs`.

**Tests.** Unit tests in `payload_codec.rs` (search "issue #1253"): both
envelope shapes round trip through `encode_payload`/`decode_payload`,
`decode_value_lossy`, `decode_error_string_lossy`, and a full
`encode_event`/`decode_event` cycle; business data written under identity
decodes correctly after an operator later registers a matching key id;
the escape wrapper's two-key shape does not false-positive on a lone
`_harvest_codec_escaped` field (top-level or nested); the real-codec path
never escapes; ordinary non-colliding plaintext is stored byte-identical.
Integration test in `codec_rotation_db_tests.rs`: an escaped row is
invisible to the SQL rotation census and sweep. `testing.rs`: the
replay-drift fixture guard refuses a fixture holding an escaped field.

No new `WorkflowEvent` variant, no migration, no change to the SQL
rotation census predicate (an escape-wrapped value has neither `codec_id`
nor `data` at its top level, so it was never counted).

Doc updates: `docs/adr/0003-payload-codec-event-boundary.md` (issue
#1253 addendum), `docs/operations/read-path-decode.md` ("Known limits").
