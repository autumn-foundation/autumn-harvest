## Perf — `decode_event` skips serde `Content` buffering (issue #1721)

`WorkflowEvent` is adjacently tagged. `serde_json::Map` sorts keys, so `data`
came before `type`. Serde then buffered each event in `Content` before it chose
a variant. That ran on every history read and replay load.

`decode_event` now feeds `type` first, then `data`. Serde reads `data` straight
into the variant. Any other shape uses the old `from_value` path, so error
messages do not change.

Measured with `payload_codec_profile`, 20 reps: 9.1% fewer instructions and
10% fewer allocation blocks. The `decode_event` call alone drops from 211 to 108
allocations on a 50-item payload.

`encode_event` is unchanged. Skipping its `to_value` pass needs per-variant
code for about 30 payload fields. A missed field would leave a payload
unencoded. That risk is not worth the gain.

No wire-format change. No migration. No `WorkflowEvent` change.
`harvest_events` is not touched.

Tests: `tests/decode_event_allocs.rs` pins the allocation budget and the error
paths.
