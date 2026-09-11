## Phase — Document the codec-envelope/business-data collision; escape-guard fix attempted and reverted (issue #1253)

No code fix ships in this PR. Issue #1253 remains open. What follows is a
negative result worth recording: the cheapest-looking fix does not work, and
why.

**The collision.** `codec_envelope_parts` (`payload_codec.rs`) recognizes a
codec envelope by JSON shape alone: three or four specific keys, a version
number, an optional `kid`. Under the identity codec — the common, un-rotated
case — `encode_payload` stores caller input verbatim. Business data that
happens to match a recognized shape is then misread as ciphertext on the
next read. The sharpest instance: a pre-#948 reader rejected the four-key
keyed shape outright, so that exact shape was legitimate stored plaintext
before key rotation existed; after #948 shipped, the same stored bytes read
as an envelope. Replay fails with `UnknownCodecKey`, or, if an operator's
key id happens to match the crafted `kid`, decodes to garbage the rotation
sweep re-encrypts permanently. Filed rather than fixed against #1242 (which
closed the equivalent four-key **version-1** collision): the real fix
changes ADR-0003's envelope representation rather than anything
rotation-specific.

**What was tried.** A write-time escape guard: `encode_payload`'s
identity-codec branch would wrap a value that already matched a reserved
envelope shape in `{"_harvest_codec_escaped": <original>,
"_harvest_codec_escaped_v": 1}` before storing it, and every read path would
reverse exactly one layer first. Unit tests for `payload_codec.rs` in
isolation passed (encoding/decoding round trips through the new guard).

**Why it was reverted.** Full-suite CI did not pass. Multiple integration
tests across `read_path_decode_integration.rs`, `ui_integration.rs`, and
`codec_rotation_db_tests.rs` failed. All traced to the same root cause:
`encode_payload`'s identity branch is not exclusive to fresh, never-before-
touched caller input — `store::append_events` (the plain, non-keyed writer)
calls it unconditionally on every event, and that is *also* the standard way
this codebase's test suite seeds history simulating a codec-encrypting
deployment: build a real envelope via a real codec, then insert it as a
literal event field through the identity-only writer, relying on identity's
documented contract being an **unconditional, exact** no-op. The escape
guard broke that contract — a test asserting `stored event payload must
remain ciphertext byte-for-byte` caught a genuine codec envelope stored
wrapped in the escape marker instead of untouched.

This is not fixable by tightening the guard's shape check: the codebase has
no signal, at the point `encode_payload`'s identity branch runs, for "is
this fresh caller input, or a deliberate pass-through of bytes that must not
change." Closing issue #1253 for real needs one of:

- **Issue #1253's own option 2** — nest the whole envelope under one
  reserved key, with a real read-side migration for the four-key keyed shape
  already-rotated deployments depend on today. Structural, not
  probabilistic; the only option the issue itself called a genuine fix.
- **Thread real provenance through the write boundary** — distinguish fresh
  caller input from deliberate byte pass-through at every entry point that
  can supply either. Large surface, correctness-critical to get right.

Neither is a small incremental patch, and neither is attempted here.

**What ships in this PR.** `autumn-harvest/src/payload_codec.rs`,
`autumn-harvest/src/testing.rs`, and
`autumn-harvest/tests/integration/codec_rotation_db_tests.rs` are reverted
to byte-identical with `trunk-dev` — no production code change. Docs only:
`docs/adr/0003-payload-codec-event-boundary.md` gets an issue #1253
addendum recording the collision, the attempted fix, why it was reverted,
and the two real paths forward; `docs/operations/read-path-decode.md`'s
"Known limits" section is updated to match.

**Process note.** Developed red/green/refactor with the escape guard, and
reviewed from three independent angles (correctness/security,
backward-compatibility/migration honesty, test coverage) before opening the
PR — all of which passed on the unit-test surface those reviews covered.
None caught the `append_events`/identity-passthrough interaction; CI's real
integration suite did, on the very first run. The gap: every review and the
unit-test suite exercised `PayloadCodecs::encode_payload` directly or
through `encode_event`, never through the full `store::append_events` path
real (and test-fixture) callers actually use.
