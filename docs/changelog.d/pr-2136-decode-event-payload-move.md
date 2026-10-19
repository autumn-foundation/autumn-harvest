## Perf — `decode_event` moves payload trees instead of rebuilding them (issue #2136)

`PayloadCodecs::decode_event` runs on every history read and replay load.
Serde decoded each payload `Value` by building a copy of it, one `BTreeMap`
node and one `Vec` at a time, then freed the original.

The decoder now lifts each codec-decoded payload out of `data` and leaves
`null` in its place. Serde decodes the small event that remains. Each
payload then moves into its field through the new crate-private
`WorkflowEvent::payload_slot`. That match is exhaustive, so a new variant
must state its payload fields. A payload that finds no slot takes a slow
path that puts it back and decodes again, so a missed field costs speed,
not data. The key set that serde sees does not change, so every decode
error stays the same.

No new `WorkflowEvent` variant, no wire-format change, no migration.
`encode_event` is unchanged.

Measured with `benches/payload_codec_profile.rs`
(`PAYLOAD_CODEC_PROFILE_REPS=20`): `decode_event` instructions -54.3%,
whole harness -19.1%, bytes allocated -36.9%. Decoding an
`ActivityCompleted` with a 50-item payload now makes 0 allocations, down
from 108. See `docs/performance-decode-event-payload-move.md`, which also
records the arena, mimalloc, jemalloc and `preserve_order` measurements
behind issue #2136.

Tests: `decode_allocations_do_not_grow_with_the_payload`
(`tests/decode_event_allocs.rs`); `decode_moves_every_payload_kind_into_its_field`,
`decode_keeps_ignoring_a_payload_key_the_variant_lacks` and
`decode_errors_still_name_the_bad_field` compare against the old
rebuilding decode; `every_arbitrary_event_decodes_on_the_fast_path`
(`fuzzing` feature) sweeps every variant through the derived `Arbitrary`.
