# `PayloadCodecs::decode_event` — move payload trees instead of rebuilding them

Issue #2136 asked whether an arena allocator could beat the `BTreeMap` cost
in replay. This note records the measurements behind the answer, and the
read-path change that came out of them.

Every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`).
Wall-clock timing is not admissible evidence on this shared-vCPU machine.

## The cost

`decode_event` runs on every history read and every replay load. It
decodes a stored event `Value` into a `WorkflowEvent`.

Serde cannot move a `Value` into a `Value` field. The `Value` deserializer
visits the tree, and the `Value` visitor builds a copy. Each map gets new
`BTreeMap` nodes and a fresh `insert` per key. Each array gets a new `Vec`.
The old tree is then freed. Strings move, so only the containers are copied.

Payloads are most of an event. So this copy was most of the decode cost:
decoding one `ActivityCompleted` with a 50-item document made 108
allocations, and every one of them belonged to the copy.

## The change

`decode_event` now lifts each payload field out of `data` after the codec
decodes it, and leaves `null` in its place. Serde then decodes a small
event. Each payload moves into its field through
`WorkflowEvent::payload_slot`.

- **Same errors.** The key set that serde sees does not change. A payload
  field accepts any JSON, so no error depends on its contents.
- **Same values.** A `Value` field takes the payload as is. An
  `Option<Value>` field takes `Some(payload)`. A JSON `null` is never
  lifted, so it decodes as before.
- **No data loss on drift.** `payload_slot` is an exhaustive match, so a
  new variant must state its payload fields. A payload that finds no slot
  takes a slow path: the decoder re-serializes the event, puts the payload
  back and decodes again. A key that the variant lacks is still ignored, as
  serde ignored it before.

`encode_event` is unchanged. `transform_event_data` lost its `encode` flag,
because `decode_event` was its only decode caller.

## Measurement

`benches/payload_codec_profile.rs` round-trips a 2,270-event mixed history
through `encode_event` and `decode_event`, with `PAYLOAD_CODEC_PROFILE_REPS=20`.

| Metric | Before | After | Change |
|---|---|---|---|
| Total instructions | 1,254,751,845 | 1,015,146,927 | -19.1% |
| `decode_event` inclusive | 469,336,943 | 214,619,192 | -54.3% |
| `encode_event` inclusive | 503,810,104 | 526,578,584 | +4.5% |
| Bytes allocated (dhat) | 323,856,444 | 204,334,356 | -36.9% |
| Allocation blocks (dhat) | 1,912,782 | 1,694,676 | -11.4% |

Both builds gave identical counts on a second run.

The `encode_event` code did not change. Its rise is glibc heap state. The
decoded event now owns the stored tree until it drops. So the next encode
gets its blocks from different free lists.

`tests/decode_event_allocs.rs` pins the result:
`decode_allocations_do_not_grow_with_the_payload` asserts that a 1-item and
a 50-item payload decode with the same allocation count. Both are now 0.

## Alternatives measured for issue #2136

These ran `benches/replay_profile.rs` (5,000 activities, 10,001 events)
with a temporary change to the global allocator or to a `serde_json`
feature. None of them shipped.

| Variant | Total instructions | Change | `match_history` inclusive |
|---|---|---|---|
| glibc malloc, `BTreeMap` (baseline) | 188,077,999 | — | 10,640,081 |
| Bump allocator with a no-op `free` (arena ceiling) | 101,608,411 | -46% | 10,654,373 |
| mimalloc | 115,922,883 | -38% | 10,679,206 |
| jemalloc (`tikv-jemallocator` 0.6.1) | 136,372,749 | -27% | 10,637,591 |
| `serde_json` `preserve_order` (`IndexMap`) | 234,889,300 | +25% | 22,151,120 |

- **An arena does not replace the `BTreeMap`.** It removes `malloc` and
  `free` cost. The `BTreeMap` work (`insert`, `dying_next`, drop glue,
  `eq`) is identical to the instruction in both runs.
- **The engine's matching does not allocate.** `match_history` costs the
  same under every allocator. The arena saving lands in harness payload
  construction, setup clones and payload drops.
- **A real arena does not fit `serde_json::Value`.** `Value` always uses
  the global allocator. A per-value allocator needs unstable Rust, and a
  global bump arena cannot free memory in a long-lived worker.
- **A general-purpose allocator is a binary decision.** A library must not
  set `#[global_allocator]`. A binary such as `harvest` could, after a
  wall-clock and RSS test on a multi-threaded worker.
- **`preserve_order` is slower.** `IndexMap` equality does a SipHash lookup
  per key. That doubles the strict replay comparison on small objects.
