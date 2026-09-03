# Workflow-module guests (issue #967 — hot code swap)

These are the runtime-loadable **workflow** modules the `hot-code-swap` R&D
spike executes. They are the workflow-side counterpart to
`../wasm-guests/`, which hosts *activity* guests for issue #965.

The two share one host: a workflow module is invoked through the same
already-reviewed wasmtime embedding (`crate::wasm_activities`) with the same
deny-all capabilities and the same fuel / epoch / memory bounds. Only the
payload differs.

## The decide-loop ABI

A workflow module is a **pure decision function**, re-invoked once per await:

```text
run(DecideRequest) -> DecideResponse
```

```jsonc
// DecideRequest — every field, in wire order. `step` is FIRST, deliberately;
// see below. There is no `abi_version` field: the ABI version is a host-side
// constant (`hot_swap::DECIDE_ABI_VERSION`) and is deliberately NOT transmitted,
// so do not write a guest that requires or branches on one.
{"step":0,"workflow":"checkout","input":{...},"resolved":[...]}

// DecideResponse — one of:
{"kind":"await","activity":"charge","input":{...}}   // host awaits it, re-invokes at step+1
{"kind":"complete","output":{...}}                    // workflow returns Ok(output)
{"kind":"fail","error":"..."}                         // workflow returns Err(error)
```

`resolved[i]` is the outcome of the activity the guest asked for at step `i`,
either `{"kind":"ok","output":...}` or `{"kind":"err","error":"..."}`. The guest
therefore sees **only** history-backed values — never a host clock, never
randomness — which is what makes the hosted workflow replay-deterministic by
construction.

Transport is byte-for-byte the `memory` / `alloc` / `run` core-WASM contract
documented in [`../wasm-guests/README.md`](../wasm-guests/README.md): `run`
returns its output location packed as `(out_ptr << 32) | out_len`.

## Why `step` is the first field

The guests here are hand-written WAT, and a JSON parser in WAT would be all
noise and no signal. Because `DecideRequest` serialises `step` first, the
step digit sits at a **fixed byte offset 8** of the request
(`{`,`"`,`s`,`t`,`e`,`p`,`"`,`:`, then the digit), so a WAT guest reads its step
with a single `i32.load8_u` and branches.

That is a convenience of *these* guests, not a property of the ABI: a real
guest compiled from Rust/AssemblyScript/Go parses the JSON. The fixed offset
only holds for `step <= 9`; every guest here uses at most 3 steps.

The offset holds only because the host serialises the `DecideRequest` **struct**
directly, via `hot_swap::encode_decide_request`. Routing it through a
`serde_json::Value` would sort the keys alphabetically (a `Value`'s object is a
`BTreeMap`), putting `input` first and making every guest here read a step digit
that is really the `n` of `"input"`. The first cut of the spike did exactly that;
it is now pinned by
`the_hosts_encoder_never_reorders_keys_the_way_a_json_value_would`.

## The guests

| File | Build id it is published under | Behaviour |
|------|-------------------------------|-----------|
| `pipeline_v1.wat` | e.g. `wf-v1` | `charge` → complete `"v1-done"` |
| `pipeline_v2.wat` | e.g. `wf-v2` | `charge` → `notify` → complete `"v2-done"` |

`pipeline_v1` is byte-for-byte equivalent, as a command stream, to the
statically-linked `native_pipeline_v1` handler in
`autumn-harvest/tests/integration/hot_code_swap_tests.rs` — which is what the
cross-hosting replay proof in that file rests on.
