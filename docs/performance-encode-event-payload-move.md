# `PayloadCodecs::encode_event` — move payload trees instead of rebuilding them

Issue #2499 profiled the codec round trip. It found no change above the 5%
floor without an owned `encode_event` variant. This note records that
variant and its measurements. The design record is
[`docs/design/DESIGN-2499.md`](design/DESIGN-2499.md).

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Wall-clock timing is not admissible evidence on this shared-vCPU machine.

## The cost

`encode_event` runs on every event append. It called `serde_json::to_value`
on the whole event, then encoded each payload field in place.

`to_value` copies each payload tree through serde. It inserts each map key
again, with a `BTreeMap` search and a `memcmp` per step. That copy and its
free were most of the encode cost.

## The change

- **`encode_event_owned(WorkflowEvent)`** is new. It lifts each non-null
  payload out of its slot and leaves `null` in its place. Serde then writes
  a small event. Each payload is encoded and moves into its `data` key, so
  no tree is copied.
- **`encode_event(&WorkflowEvent)`** keeps its signature. It clones the
  event and calls `encode_event_owned`. `Value::clone` copies a map in key
  order with no search, so it is cheaper than the serde copy.
- **`fork.rs`** encodes a clone that it owns, so it calls
  `encode_event_owned` and copies nothing.
- **`event_from_wire`** lost its own payload lift. Its one caller,
  `decode_event_moving_payloads`, had already lifted each payload. So the
  second lift did six lookups per event and moved nothing.
- **`WorkflowEvent::payload_slot_mut`** is gone. It was a copy of
  `payload_slot`.

The stored value does not change:

- The encode visits each `PAYLOAD_FIELD_KEYS` key that is in `data`, in that
  order. A payload key with no slot is still encoded, as before.
- A lifted `Option` payload leaves `Some(null)`, so serde keeps its key.
- A lifted payload with no `data` key takes a slow path. The payloads go
  back into the event, and the slow path encodes the whole event as before.
- Each payload replaces its `null` in place, so each key keeps its position.

## Measurement

`benches/payload_codec_profile.rs`, unchanged, with
`PAYLOAD_CODEC_PROFILE_REPS=20`. It round-trips a 2,270-event history
through `encode_event` and `decode_event`. The harness uses the borrowed
`encode_event`, so these figures are what each current caller gets.

| Metric | Before | After | Change |
|---|---|---|---|
| Total instructions | 1,062,066,352 | 855,768,802 | **-19.4%** |
| `encode_event` inclusive | 526,805,033 | 361,828,659 | -31.3% |
| `decode_event` inclusive | 262,866,942 | 221,464,380 | -15.8% |
| Bytes allocated (dhat) | 204,334,356 | 204,713,343 | +0.2% |
| Allocation blocks (dhat) | 1,694,676 | 1,733,421 | +2.3% |
| Peak bytes (dhat) | 6,682,102 | 6,682,102 | 0 |

The encode change alone gave 905,658,015 instructions (-14.7%). The
removal of the second decode lift gave the rest. A second run of the final
build gave the same count.

The block count rises because the clone copies the small non-payload
strings, and serde then copies them again. The payload copy is the same
size either way.

`encode_event_owned` is 186,008,969 inclusive instructions of the
361,828,659 that `encode_event` costs. The rest is the clone. A caller that
owns its event skips it.

`tests/encode_event_allocs.rs` pins the result:
`owned_encode_allocations_do_not_grow_with_the_payload` asserts that a
1-item and a 50-item payload encode with the same allocation count. A clone
and the serde copy make the same number of allocations. So the borrowed
gain shows in instructions only.

## What is left

- `index_into_mut` is still 3.6%. These are the six lookups that
  `decode_event_moving_payloads` makes per event.
- About half of the borrowed cost is the clone. A store append API that
  takes owned events would remove it. The callers keep their events after the
  append, so that is a larger change.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench payload_codec_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_codec_profile") | .executable')
PAYLOAD_CODEC_PROFILE_REPS=20 valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=95 cg.out
callgrind_annotate --inclusive=yes cg.out | grep -E 'encode_event|decode_event'
PAYLOAD_CODEC_PROFILE_REPS=20 valgrind --tool=dhat "$BIN"
```

Artifacts are in `docs/perf-artifacts/encode-event-payload-move/`. The
flat profiles are `before-callgrind-flat.txt` and `after-callgrind-flat.txt`.
The inclusive figures are in `inclusive-summary.txt`, and the dhat totals
are in `dhat-summary.txt`.
