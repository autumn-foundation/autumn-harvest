## Phase 3.37.1 — Offload look-alike escape (issue #1758)

Business data that carried `_harvest_offload_envelope: 1` was read back as a
dangling reference. Replay of that event failed with `PayloadOffload`.

`PayloadOffloader::offload_event_value` now offloads any fresh field that
carries the discriminator, whatever its size. A stored field with the
discriminator is therefore always a real reference. This mirrors the codec
escape from issue #1253.

- The stored shape does not change. An older binary reads every new row, so
  no fleet gate is needed. A nested shape would have needed one.
- The old "skip fields that are already envelopes" rule is removed. Every
  caller passes a fresh value, so the rule only hid look-alikes.
- The re-encryption sweep and the size cap no longer skip a look-alike, because
  the look-alike is never stored bare.
- Rows written before this fix are not repaired. No migration is needed.
- No new `WorkflowEvent` variant. `harvest_events` is not rewritten.

Tests: `payload_store::tests::business_data_shaped_like_offload_envelope_round_trips`
and four sibling tests (discriminator-only, oversized, non-integer, no re-escape).
