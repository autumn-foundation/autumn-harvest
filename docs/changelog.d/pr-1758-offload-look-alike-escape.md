## Phase 3.37.1 — Offload look-alike escape (issue #1758)

Business data that carried `_harvest_offload_envelope: 1` was read back as a
dangling reference. Replay of that event failed with `PayloadOffload`.

`PayloadOffloader::offload_event_value` now offloads any fresh field that
carries the discriminator, whatever its size. The offloader path therefore
never stores a bare look-alike. This mirrors the codec escape from issue #1253.

- The stored shape does not change. An older binary reads every new row, so
  no fleet gate is needed. A nested shape would have needed one.
- The old "skip fields that are already envelopes" rule is removed. Every
  caller passes a fresh value, so the rule only hid look-alikes.
- The re-encryption sweep keeps its skip. On this path a skipped field is now
  always a real reference.
- **Known gap:** writers that bypass the offloader still store a bare
  look-alike. Examples are the workflow start input, external task completion,
  and any node without a store. Rows written before this fix are also not
  repaired. A follow-up must route these writers through the escape.
- No migration is needed.
- No new `WorkflowEvent` variant. `harvest_events` is not rewritten.

Tests: `payload_store::tests::business_data_shaped_like_offload_envelope_round_trips`
and sibling tests (discriminator-only, oversized, non-one marker, every field, no re-escape).
