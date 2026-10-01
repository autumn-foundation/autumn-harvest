## Perf — `decode_event` skips serde `Content` buffering (issue #1721)

`WorkflowEvent` is adjacently tagged. `serde_json::Map` sorts keys, so `data`
comes before `type`. Serde then buffers each event in `Content` before it
chooses a variant. That runs on every history read and replay load.

`decode_event` now feeds `type` first, then `data`. Serde reads `data` straight
into the variant. Any other shape uses the old `from_value` path, so error
messages do not change.

Measured with `payload_codec_profile` at `PAYLOAD_CODEC_PROFILE_REPS=20`, under
callgrind and dhat: 9.1% fewer instructions (1,465.8M to 1,332.3M) and 10% fewer
allocation blocks (2,178,557 to 1,960,451). In the new test, one `decode_event`
call on a 50-item payload drops from 211 to 108 allocations.

`encode_event` is unchanged. Skipping its `to_value` pass needs per-variant
code for about 30 payload fields. A missed field would leave a payload
unencoded. That risk is not worth the gain.

No wire-format change. No migration. No `WorkflowEvent` change.
`harvest_events` is not touched.

Tests: `autumn-harvest/tests/decode_event_allocs.rs` compares the allocation count
and the results, including error text, against `serde_json::from_value`.
