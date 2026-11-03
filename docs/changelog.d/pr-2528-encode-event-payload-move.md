## Phase X.Y — Encode moves payload trees (issue #2499)

`PayloadCodecs::encode_event` called `serde_json::to_value` on the whole
event. That copied each payload tree through serde, one key insert at a
time.

- `PayloadCodecs::encode_event_owned` is new. It lifts each payload out of
  the event, serializes the rest, and moves each encoded payload into the
  stored value. No payload tree is copied.
- `encode_event` keeps its signature. It clones the event and calls the
  owned variant. A clone is a cheaper copy than serde.
- `fork.rs` calls the owned variant.
- The decode path lost a second payload lift that moved nothing.
  `WorkflowEvent::payload_slot_mut` is gone, because it copied
  `payload_slot`.

The stored bytes do not change. `payload_codec_profile` drops 19.4% of its
instructions (1,062,066,352 to 855,768,802). `encode_event` drops 31.3%.

No new `WorkflowEvent` variant. No migration. The design record is
`docs/design/DESIGN-2499.md`, and the measurement is
`docs/performance-encode-event-payload-move.md`. Unit tests compare each
variant with the old encode under three codecs. An arbitrary sweep covers
each variant. `tests/encode_event_allocs.rs` pins the allocation count.
