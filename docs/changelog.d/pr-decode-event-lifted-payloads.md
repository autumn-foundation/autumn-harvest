## Perf — `decode_event` moves payload trees instead of rebuilding them

`decode_event` reads a stored event out of a `serde_json::Value`. Serde
rebuilds a `Value` field node by node, even from a `Value`. So every payload
tree was torn down and rebuilt on every history read and replay load.

`event_from_wire` now lifts each non-null payload field out of `data` before
serde runs. Serde reads the small rest of the event. Each tree then moves into
its field through `WorkflowEvent::payload_slot_mut`. That match names every
variant, so a new variant does not compile until someone sorts it.

Every payload field is a `Value` or an `Option<Value>`. Both accept any JSON,
so the lift changes no error. Codec envelopes still decode first, in
`transform_event_data`, so the identity escape case (issue #1253) and keyed
codecs are unchanged.

Measured with `payload_codec_profile` at `PAYLOAD_CODEC_PROFILE_REPS=20`, under
callgrind and dhat: 16.6% fewer instructions (1,262.4M to 1,053.1M), 36.9%
fewer bytes allocated (323.9M to 204.3M), and 11.4% fewer allocation blocks
(1,912,782 to 1,694,676).

A load-path probe parsed stored event bytes to a `Value`, then ran
`decode_event`. It covered 9,072 events of about 351 bytes each. The whole
read fell from 31,202 to 26,682 instructions per event. The `decode_event`
step alone fell from 8,894 to 4,374.

No wire-format change. No migration. No new `WorkflowEvent` variant.
`harvest_events` is not touched.

Tests: `lifted_decode_matches_serde_for_every_payload_field` sets every
payload key of every payload-bearing variant. It checks that each key has a
slot and that the decode matches `serde_json::from_value`.
`decode_moves_the_payload_tree_instead_of_rebuilding_it` in
`autumn-harvest/tests/decode_event_allocs.rs` checks that `decode_event`
allocations do not grow with payload size.
