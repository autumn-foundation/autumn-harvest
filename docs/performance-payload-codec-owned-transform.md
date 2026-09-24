# `payload_codec::{encode_payload, decode_payload}` — a per-field clone eliminated

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count/bytes
(`valgrind --tool=dhat`).

## 🎯 Workload

`payload_codec::PayloadCodecs::{encode_event, decode_event}` are the codec
transform `store.rs` runs on **every** workflow-event append (`encode_event`,
`store.rs` lines 150/558) and **every** history read or replay load
(`decode_event`, `store.rs` lines 1131/1349/1395/1441/1445). This is the
highest-call-volume path any profiling harness in this repo measures so
far: it fires on literally every event of every execution, not only when an
operator opts into a feature (`queue_fairness`, issue #515) or once per UI
request (`timeline`, issue #739).

The harness is `autumn-harvest/benches/payload_codec_profile.rs`, newly
added by this change. It builds a fixed, realistic 2,270-event mixed
history for one long-running execution — a `WorkflowStarted` carrying a
scheduled-carryover `last_completion_result` (issue #488), regular
activities in the 9:1 closed:open / 1-in-5-retried mix
`timeline_profile.rs` already established as realistic, local activities,
heartbeats, and a terminal `WorkflowCompleted` — with each payload field
carrying a representative nested order-processing document, not a bare
scalar. It round-trips every event through `encode_event` immediately
followed by `decode_event` on the result, 200 times (454,000 total event
round trips), against `PayloadCodecs::default()` — the identity codec, no
key ever registered — which `PayloadCodecs::any_keys`'s own doc comment
calls "the overwhelmingly common case".

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench payload_codec_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_codec_profile") | .executable')
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

## 📈 Profile

Callgrind's flat, self-cost profile does not attribute cost to
`encode_payload`/`decode_payload` directly — both are small enough that
LLVM inlines their callers' cost into generic `serde_json`/`BTreeMap`
symbols shared with the (unrelated, unavoidable) `serde_json::to_value`/
`from_value` conversion `encode_event`/`decode_event` also do. dhat's
per-call-stack allocation attribution does not have that problem: every
allocation site in `docs/perf-artifacts/payload-codec-owned-transform/before-dhat.json`
carries its full call stack, so summing every program point whose stack
includes `PayloadCodecs::encode_payload` or `PayloadCodecs::decode_payload`
gives an exact, unambiguous attribution
(`docs/perf-artifacts/payload-codec-owned-transform/dhat-attribution-summary.txt`):

```
total_bytes=5,864,636,320   total_blocks=41,447,698
target_bytes=2,398,018,842  (40.89%)
target_blocks=21,571,722    (52.05%)
```

**52.05% of all allocation blocks** in this realistic round-trip workload
trace to these two functions — clearing the 5%-of-workload floor by more
than 10x.

## 💡 Hypothesis

`decode_payload`'s not-an-envelope fast path — the common case on any
deployment that has not configured a non-identity codec, or for any
payload-bearing field a rotated deployment has not yet re-encrypted — does
`payload.clone()`: a full `serde_json::Value` tree clone, to hand back a
value that is not changing at all. `encode_payload`'s identity fast path
does the same after its unavoidable (issue #1253 correctness) collision
tree-walk. Both are called from exactly one production call site,
`transform_event_data`, which already owns the whole event `Value` tree —
`root: &mut Value` on an owned `Value`, at every call site in
`encode_event`/`decode_event`. Its `data.get_mut(key)` / `*payload = ...`
shape forces the callee to accept only `&Value`, so the callee clones data
the caller already owns outright. Moving that field out with
`std::mem::take` instead of borrowing it removes the clone on the fast
path unconditionally — one call, one field, on every event, every time.

## 🔧 Change

`autumn-harvest/src/payload_codec.rs`:

* New private `encode_payload_owned`/`decode_payload_owned`, identical to
  the existing public `encode_payload`/`decode_payload` except they take
  `payload: Value` instead of `payload: &Value`. Each fast path now returns
  the owned value directly instead of cloning it.
* The public `encode_payload`/`decode_payload` become one-line wrappers
  (`self.encode_payload_owned(payload.clone())`) — their signature and
  behavior are unchanged, so every other caller (the two call sites in
  `worker.rs`/`payload_codec.rs` itself that only hold a borrow) is
  unaffected.
* `transform_event_data`'s per-field loop now does
  `let owned = std::mem::take(payload); *payload = ...encode_payload_owned(owned)?...`
  instead of calling the borrowing API. `mem::take` leaves a `Value::Null`
  placeholder behind only for the instant before it is overwritten; on an
  `Err` the whole partially-transformed `value` is discarded by the `?` in
  `encode_event`/`decode_event`, so the placeholder is never observed.

**No behavior change.** `encode_payload`/`decode_payload`'s public
signatures, return values, and error conditions are untouched. All 65
`payload_codec::tests::*` unit tests pass unmodified.

## 📊 Measurement

Same harness, same machine and session, differing only by the diff above.

### Allocations (`valgrind --tool=dhat`)

| dhat | Before | After | Δ |
|---|---|---|---|
| Total blocks | 41,447,698 | 19,875,976 | -21,571,722 (**-52.05%**) |
| Total bytes  | 5,864,636,320 | 3,466,617,478 | -2,398,018,842 (**-40.89%**) |

Both deltas match the before-side attribution above almost exactly,
confirming the fix removed exactly the allocations it targeted and nothing
else. Both clear the ≥10%-reduction floor by a wide margin.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| | Instructions (Ir) |
|---|---|
| Before | 21,222,464,061 |
| After  | 13,393,996,412 |
| **Reduction** | **7,828,467,649 (36.89%)** |

Well clear of the ≥5%-of-workload floor too.

### Correctness

* `cargo fmt --all -- --check` — clean.
* `cargo test -p autumn-harvest --all-features --lib payload_codec` —
  **65 passed, 0 failed**.
* `cargo test -p autumn-harvest --all-features --lib` — **3,817 passed, 0
  failed, 1 ignored** (full library suite; this change's call site,
  `transform_event_data`, sits on the event read/write path every
  db-gated and pure-logic test in the crate exercises indirectly).
* `cargo clippy -p autumn-harvest --all-features --lib --bins --tests -- -D warnings`
  and `cargo clippy -p autumn-harvest --all-features --bench payload_codec_profile -- -D warnings`
  — both clean. `cargo clippy -p autumn-harvest --all-targets --all-features -- -D warnings`
  fails in this sandbox on pre-existing, unrelated lints in other bench
  targets (`dead_code` in `replay_profile_support.rs`, `missing_const_for_fn`
  in `critical_path_profile.rs`) that reproduce identically on
  `origin/trunk-dev` with no changes applied.
* `python3 docs/audits/comment-hygiene.py --base origin/trunk-dev` — clean,
  no Tier A findings, no Tier B regressions.

## 🔬 Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features \
  --bench payload_codec_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="payload_codec_profile") | .executable')

# Allocations:
valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"

# Instructions:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -20
```

Full artifacts:
`docs/perf-artifacts/payload-codec-owned-transform/{before,after}-callgrind-flat.txt`,
`{before,after}-dhat.json`, `dhat-attribution-summary.txt`.
