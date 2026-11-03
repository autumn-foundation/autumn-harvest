# Design — Issue #2499: move payload trees on the encode path

`PayloadCodecs::encode_event` runs on every event append. It serializes the
whole event with `serde_json::to_value`, then encodes each payload field.
The issue profiled this path and found no change above the 5% floor. Its
one open option was an owned `encode_event` variant. That needs a human
decision, and this record makes it.

**No migration. No new `WorkflowEvent` variant. No change to stored bytes.**

---

## 0. Planning record

### 0.1 Facts

- `to_value` copies each payload tree through serde. Each map key is
  inserted again with a `BTreeMap` search. That copy, and the free of the
  copy, is most of the encode cost (issue profile, ranks 1 to 10).
- `Value::clone` also copies the tree. It clones a `BTreeMap` in key order
  and does no search. So a clone is a cheaper copy than `to_value`.
- `decode_event` already moves payloads into their fields
  (`docs/performance-decode-event-payload-move.md`). It uses
  `WorkflowEvent::payload_slot`.
- Each production caller borrows its events. `store.rs` keeps the slice for
  later use. `fork.rs` encodes a clone that it owns and then drops.
- The six `PAYLOAD_FIELD_KEYS` lookups on `data` cost about 5.45%. The
  issue rates a one-pass rewrite at under 5% alone.
- An `Option` payload is `skip_serializing_if = "Option::is_none"`. A
  `None` field is therefore absent from the wire.

### 0.2 The decision

Add `PayloadCodecs::encode_event_owned(WorkflowEvent)`. It lifts each
non-null payload out of its slot and leaves `null` in its place. It
serializes the small event that remains. Then it encodes each payload and
moves it into its `data` key. No payload tree is copied.

`encode_event(&event)` keeps its signature. It calls
`encode_event_owned(event.clone())`. The payload copy is now a clone, not a
serde rebuild. `fork.rs` calls the owned variant, so it copies nothing.

### 0.3 Brainstorm — how can the encode copy less?

| # | Idea | Verdict |
|---|------|---------|
| 1 | One pass over `data` instead of six lookups | Kept as a part of #2. Under 5% alone. |
| 2 | Owned `encode_event_owned`: lift, serialize, move back | **Chosen.** No payload copy for an owner. |
| 3 | Borrowed encode clones, then calls #2 | **Chosen.** A cheaper copy for each current caller. No API break. |
| 4 | Hand-written `Serialize` that skips payload fields | Rejected. 53 variants to keep in step by hand. |
| 5 | Owned events through `store::append_events*` | Deferred. The callers keep their events after the append. |
| 6 | `Arc` or `Cow` payloads in `WorkflowEvent` | Rejected. A public type change across each crate. |
| 7 | A faster global allocator | Rejected. A library must not set one (issue #2136). |
| 8 | Encode straight to bytes, with no `Value` | Rejected. The row and the offloader take a `Value`. |

### 0.4 Reverse brainstorm — how can this change break the store?

| # | Way to fail | Guard |
|---|-------------|-------|
| 1 | A payload key with no slot stays plaintext under a real codec. | The encode loop visits each `PAYLOAD_FIELD_KEYS` key that is in `data`, slot or not, as the rebuild did. |
| 2 | A lifted `Option` payload becomes `None`, so serde drops its key. | The lift leaves `Some(null)`. The key stays on the wire. |
| 3 | A lifted payload finds no key in `data`, so it is lost. | A check runs before any encode. On a miss, the payloads go back and the rebuild runs. |
| 4 | Fields encode in a new order, so a second codec error wins. | The loop encodes in `PAYLOAD_FIELD_KEYS` order, as before. |
| 5 | A null payload changes shape. | A null is never lifted. It encodes as before. |
| 6 | The escape case of issue #1253 is skipped. | Each payload goes through the same `encode_payload_owned`. |
| 7 | Key order changes under `preserve_order`. | Each payload replaces its `null` in place, so each key keeps its position. |
| 8 | A new variant omits a slot. | The arbitrary sweep asserts the fast path for each variant. A miss is a slow path, not a loss. |
| 9 | The clone costs more than `to_value` saves. | Callgrind measures it. See §2. |

### 0.5 Six thinking hats

- **White (facts).** See §0.1. The baseline is 1,042,822,471 instructions.
- **Red (feeling).** An owned API with one caller feels thin. The clone
  path gives each current caller a gain, so the API is not dead weight.
- **Black (risk).** A lost or plaintext payload is the worst outcome. Each
  row in §0.4 has a test.
- **Yellow (benefit).** Each append is cheaper, and no caller changes.
  A caller that owns its event can skip the copy.
- **Green (new ideas).** Pass owned events from the executor to the store
  (#5). That needs a new append API, so it is out of scope.
- **Blue (process).** Write the tests first. Make them pass. Then remove
  the duplicate `payload_slot_mut`, measure, and record the numbers.

## 1. Acceptance criteria

1. `encode_event` and `encode_event_owned` return the same `Value` and the
   same text as the rebuild, for each variant, under the identity codec, a
   real codec and a keyed codec.
2. A null payload, an absent `Option` payload and an envelope-shaped
   payload encode as before.
3. Each variant takes the fast path.
4. The slow path returns each payload to its field and matches the rebuild.
5. The owned encode makes the same number of allocations for a small and a
   large payload.
6. The profile harness shows a drop of at least 5% in instructions.

## 2. Measurement

See [`performance-encode-event-payload-move.md`](../performance-encode-event-payload-move.md).
