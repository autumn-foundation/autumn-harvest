# Codec key rotation: skip the JSON round-trip in the re-encryption sweep

This note documents a profiling pass over
`codec_rotation::reencrypt_event_payload_fields_under` (issue #948) — the
per-row body of `sweep_codec_reencryption_once`, the background sweep that
lazily migrates `harvest_events` payloads off a retired codec key. It runs on
the timeout checker's scanner tick (500ms by default) for every shard on
which a keyed codec is registered and rotation has not yet converged, so this
is a real, recurring batch job on any deployment that rotates keys, not a
one-off migration script.

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`), both reproducible bit-for-bit on any machine.

## Workload

`benches/codec_rotation_reencrypt_profile.rs` builds a batch of 200 rows
(matching `codec_rotation::CODEC_ROTATION_DEFAULT_BATCH`) of serialized
workflow events — alternating `ActivityScheduled` (an `input` field) and
`ActivityCompleted` (an `output` field), each carrying a realistic
order-checkout-shaped JSON payload (nested `customer`/`shipping_address`
objects, a two-item `items` array, a `metadata` map) — every one encoded
under a retired key. This is the shape the very first sweep tick after a key
rotation sees: every row the scanner fetches still needs conversion, which is
exactly where the function's CPU/allocation cost is paid. (The
already-converted steady state that dominates a shard's *lifetime* is a
cheap map-lookup precheck, `has_non_active_key` returning `false`, which the
harness also exercises once per row before the conversion — mirroring the
real per-row order in `sweep_codec_reencryption_once` — but that path was
already flagged in the source as "a few map lookups and no allocation" and
is not the target here.) The harness repeats the batch 25 times, for 5,000
row conversions total.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench codec_rotation_reencrypt_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="codec_rotation_reencrypt_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (harness committed against unmodified `reencrypt_event_payload_fields_under`):

```
295,601,805 (100.0%)  PROGRAM TOTALS

 26,176,237 ( 8.86%)  _int_free
 17,382,690 ( 5.88%)  serde_json::ser::format_escaped_str_contents
 17,289,650 ( 5.85%)  base64::engine::general_purpose::GeneralPurpose::internal_decode
 15,907,289 ( 5.38%)  malloc
 15,407,769 ( 5.21%)  core::str::converts::from_utf8
 15,129,244 ( 5.12%)  _int_malloc
 13,948,688 ( 4.72%)  base64::engine::general_purpose::GeneralPurpose::internal_encode
 13,940,000 ( 4.72%)  serde_json::value::de::<impl Deserialize for Value>::deserialize'2
 12,265,824 ( 4.15%)  alloc::collections::btree::map::BTreeMap<K,V,A>::insert
 11,585,000 ( 3.92%)  serde_json::read::SliceRead::skip_to_escape
 10,615,394 ( 3.59%)  __memcmp_avx2_movbe
 10,545,416 ( 3.57%)  free
 10,414,235 ( 3.52%)  alloc::collections::btree::map::IntoIter<K,V,A>::dying_next
  8,960,000 ( 3.03%)  <SliceRead as Read>::parse_str
  ...
```

## Hypothesis

`reencrypt_event_payload_fields_under` migrates one payload field's
ciphertext from a retired key to the active one. Its two calls were:

```rust
let plaintext = codecs.decode_payload(field)?;                       // bytes -> Value
staged.push((key, codecs.encode_payload_under(target_key_id, &plaintext)?)); // Value -> bytes
```

`decode_payload` base64-decodes and codec-decodes the ciphertext to raw
plaintext bytes, then calls `serde_json::from_slice` to parse those bytes
into a `serde_json::Value` tree. `encode_payload_under` immediately calls
`serde_json::to_vec` on that same `Value` to turn it back into bytes before
codec-encoding and base64-encoding it again. The sweep never reads or
modifies the decoded structure — it exists solely to migrate ciphertext — so
every field of every row pays a full JSON deserialize-then-reserialize round
trip through `serde_json::Value`'s `BTreeMap`-backed `Map` (this crate does
not enable `preserve_order`) for a value nothing ever inspects.

That mechanism accounts for the JSON-machinery lines in the profile above:
`format_escaped_str_contents` (serializing), `Value::deserialize` +
`skip_to_escape` + `parse_str` + `from_utf8` (parsing), plus a large share of
the allocator traffic (`_int_free`/`malloc`/`_int_malloc`/`free`) and the
`BTreeMap::insert`/`IntoIter::dying_next` pair from building and then
immediately tearing down the intermediate `Value::Object` map. Summed, the
JSON-parse/serialize-specific lines alone are already **≈24%** of the
profile, well past the 5%-of-workload floor.

## Change

Added byte-level siblings to the two `PayloadCodecs` methods, `pub(crate)`
(no public API change): `decode_payload_bytes` stops at the raw plaintext
bytes instead of building a `serde_json::Value`; `encode_payload_bytes_under`
takes raw bytes instead of calling `serde_json::to_vec` on a `Value`. Both
existing `pub fn` methods (`decode_payload`, `encode_payload_under`) are
now thin wrappers that add the `Value` step, used unchanged by every caller
that actually needs the parsed value. `reencrypt_event_payload_fields_under`
switches to the byte-level pair, so migrating a field's ciphertext never
builds a `Value` tree for the plaintext:

```rust
let plaintext_bytes = codecs.decode_payload_bytes(field)?.ok_or_else(|| { ... })?;
staged.push((key, codecs.encode_payload_bytes_under(target_key_id, &plaintext_bytes)?));
```

`decode_payload_bytes` still **validates** the decoded bytes as well-formed
JSON — via `serde_json::from_slice::<serde::de::IgnoredAny>`, which walks the
document to confirm its syntax without allocating a `Value` for it — so a
codec that "successfully" decodes corrupt or mismatched ciphertext into
garbage bytes still fails the row exactly as `decode_payload` would, and the
sweep counts it unresolved rather than writing the garbage back out under a
new key. (An earlier version of this change dropped that validation
entirely; caught in review — see `a_field_that_decodes_to_non_json_bytes_is_left_completely_untouched`,
added as a regression test.)

Behavior is otherwise unchanged: the stored ciphertext bytes end up
identical to what the old bytes-through-`Value`-through-bytes path produced
(both paths ask the same codec to encode the same decoded, JSON-validated
plaintext bytes), and the field's plaintext is not merely equal but
literally untouched — it never leaves byte form. All 61 `codec_rotation`/
`payload_codec` unit tests (60 pre-existing plus the new regression test)
pass, including `decoded_plaintext_is_byte_identical_and_structure_is_untouched`.

## Measurement

| Metric | Before | After | Δ |
|---|---:|---:|---:|
| Instructions (`callgrind`, 5,000 row conversions) | 295,601,805 | 149,902,120 | **-49.29%** |
| Allocated bytes (`dhat`) | 52,914,292 | 27,698,742 | **-47.65%** |
| Allocated blocks (`dhat`) | 387,224 | 182,224 | **-52.94%** |

Post-change profile — `format_escaped_str_contents` and `Value::deserialize`
(building the `Value` tree) are gone; `skip_to_escape`/`from_trait`/
`ignore_str` remain because `IgnoredAny` still walks and validates every
string and structural token in the document, it just doesn't allocate a
container to hold them:

```
149,902,120 (100.0%)  PROGRAM TOTALS

 17,289,650 (11.53%)  base64::engine::general_purpose::GeneralPurpose::internal_decode
 13,948,688 ( 9.31%)  base64::engine::general_purpose::GeneralPurpose::internal_encode
 11,796,237 ( 7.87%)  _int_free
 11,585,000 ( 7.73%)  serde_json::read::SliceRead::skip_to_escape
 10,305,000 ( 6.87%)  serde_json::de::from_trait
  7,667,492 ( 5.11%)  __memcmp_avx2_movbe
  7,462,289 ( 4.98%)  malloc
  5,440,000 ( 3.63%)  <SliceRead as Read>::ignore_str
  5,085,416 ( 3.39%)  free
  4,855,460 ( 3.24%)  _int_malloc
  4,592,500 ( 3.06%)  autumn_harvest::codec_rotation::reencrypt_event_payload_fields_under
  4,414,235 ( 2.94%)  alloc::collections::btree::map::IntoIter<K,V,A>::dying_next
  4,230,000 ( 2.82%)  autumn_harvest::payload_codec::codec_envelope_parts
  3,295,824 ( 2.20%)  alloc::collections::btree::map::BTreeMap<K,V,A>::insert
```

The remaining `BTreeMap` traffic (`IntoIter::dying_next`, `BTreeMap::insert`)
is the envelope object itself (`codec_id`/`kid`/`data`), not the payload
content — that part is inherent to the stored shape, not this function's to
remove.

**Correctness**: `cargo test -p autumn-harvest --no-default-features --lib
codec_rotation::` (14 tests) and `cargo test -p autumn-harvest
--no-default-features --lib payload_codec::` (47 tests) both pass. Full
`cargo test -p autumn-harvest --no-default-features --lib` (2,256 tests)
passes. `cargo clippy -p autumn-harvest --lib --all-features -- -D warnings`
and `cargo clippy -p autumn-harvest --bench codec_rotation_reencrypt_profile
--all-features -- -D warnings` are clean. `cargo fmt --all -- --check` is
clean.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench codec_rotation_reencrypt_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="codec_rotation_reencrypt_profile") | .executable')

# Instruction count:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`CODEC_ROTATION_PROFILE_BATCH` (default 200) and `CODEC_ROTATION_PROFILE_REPS`
(default 25) control the batch size and repeat count.
