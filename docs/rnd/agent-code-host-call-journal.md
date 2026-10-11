# Journaled host calls for agent code — R&D spike (issue #2014)

**Status: R&D spike, behind the `wasm-activities` Cargo feature.** This page
answers issue #2014. It describes a working prototype and gives a go / no-go
verdict. The prototype is `autumn-harvest/src/wasm_journal.rs`. The plan and
its risk analysis are in `DESIGN-2014.md`.

## The question

Can `wasm_activities` run LLM-generated code with each host call journaled and
capability-gated? Can this give the main benefit of Golem inside a
conventional engine?

**Short answer: yes, for activities.** The prototype journals each host call
and gates each capability by name. A retry replays the journal and does not
repeat a finished side effect. Workflow history does not change. Three gaps
stand between the prototype and production. Section 7 lists them.

## 1. What exists today

The WASM activity spike (#965) runs a guest module on wasmtime. Its sandbox
has these properties (see [wasm-activities-spike.md](wasm-activities-spike.md)):

- A fuel budget, an epoch wall-clock deadline and a memory ceiling.
- A deny-all linker. A guest that imports an ungranted host function fails at
  instantiation as `SandboxDenied`.
- Three fixed grants: `env::now_millis`, `env::random_u64` and `env::env_get`.
- Optional Ed25519 publisher signatures (#1838).

Two things were missing for generated code:

- **No journal.** A host call left no record. A retry ran each call again.
- **No open grant set.** The three grants were fixed booleans. An embedder
  could not give a guest a new capability.

The two nondeterministic grants made a re-run differ from the first run.

## 2. The prototype

### 2.1 One import

The guest imports one function:

```text
harvest::host_call(name_ptr, name_len, req_ptr, req_len) -> i64
```

The name is a UTF-8 capability name, such as `crm.lookup`. The request is
JSON. The result is one of these:

| Result | Meaning |
|--------|---------|
| `>= 0` | The packed `(ptr << 32) \| len` of a response, as `run` returns it. The response is `{"ok": value}` or `{"err": message}`. |
| `HOST_CALL_DENIED` (-1) | The embedder did not grant the name. No handler ran. |
| `HOST_CALL_INVALID` (-2) | A bad pointer, a bad name or a bad request. Nothing is journaled. |
| `HOST_CALL_LIMIT` (-3) | The run reached the call budget or the journal byte budget. |

The host writes the response through the guest `alloc` export. The guest
never makes a second call to fetch a long response.

### 2.2 Capability grants

`HostCallGrants` maps a capability name to a Rust handler. The default grant
set is empty:

- With no grant, the host does not link `harvest::host_call`. Instantiation
  fails as a non-retryable `SandboxDenied`.
- A call to an ungranted name returns `HOST_CALL_DENIED`. The journal records
  a `Denied` entry, so an auditor sees each attempt.
- A journaled run links no ambient `env::` import. A clock or a random source
  is a named grant, so the journal records its value.

### 2.3 The journal

Each call appends one `HostCallEntry`:

| Field | Content |
|-------|---------|
| `seq` | The position of the call in the run. |
| `name` | The capability name. |
| `request` | The request as raw JSON text, exactly as the guest sent it. |
| `outcome` | `ok` with a raw JSON response, `err` with a message, or `denied`. |

The journal is plain JSON. The caller can persist it as heartbeat details.
The request and the response are text, not decoded values. A persisted
journal therefore replays byte for byte. A decoded float can change its last
digit on a round trip, and the replay would then diverge.

### 2.4 Replay and resume

`invoke_journaled` takes the journal of an earlier attempt:

1. It serves the recorded entries in order. A served call runs no handler.
2. When the recorded entries end, it runs live and appends new entries.
3. It also returns the journal after a failure. The next attempt resumes
   from it.

A replay checks each call against its entry:

- A different name or request is a divergence.
- A run that ends with entries left over is a divergence.
- A malformed journal is a divergence before the guest runs. A gap in `seq`,
  too many entries, too many bytes or a value over its bound makes it
  malformed.
- A divergence is a non-retryable `WasmJournalDivergence`. The journal stays
  unchanged.

A replay checks the grants again for each `ok` or `err` entry:

- A revoked grant is a divergence. Recorded data never reaches a guest that
  lost the capability.
- A recorded `denied` entry stays denied after a new grant.
- With an empty grant set, the import is not linked. A run with a full
  journal then fails as `SandboxDenied`.

### 2.5 Handler failures

A handler returns `Fatal` or `Transient`:

- **Fatal** is a final answer. The host journals it and gives the guest an
  `err` envelope. A replay gives the same answer.
- **Transient** is a temporary failure. The host journals nothing and fails
  the attempt as a retryable `HostCallFailed`. The retry calls the handler
  again.

If the host journaled a transient failure, each retry would replay that
failure.

### 2.6 Bounds

| Bound | Value | Effect |
|-------|-------|--------|
| `MAX_HOST_CALL_BYTES` (65536) | Request, response and escaped message size | The host rejects a larger request before it reads the bytes. A larger response becomes an `err` outcome. The host cuts a longer message. |
| `MAX_HOST_CALL_NAME_BYTES` (128) | Name size | A longer name is `HOST_CALL_INVALID`. |
| `MAX_HOST_CALLS` (256) | Calls per run | Later calls return `HOST_CALL_LIMIT`. The journal stops growing. |
| `MAX_JOURNAL_BYTES` (1048576) | Journal size in JSON, escapes included | A live call that could push the journal past it returns `HOST_CALL_LIMIT`. This bounds host memory and the persisted size. |

The host checks each budget before it parses the call, so a call over a
budget costs no parse. The host checks a request as JSON without a value
tree. It decodes the request only for a live handler.

The fuel, memory and wall-clock bounds of the sandbox apply unchanged. A
handler panic becomes a retryable `WasmTrap`, so the worker does not crash.
The host does not journal the panicked call, so the retry runs its handler
again.

### 2.7 Write order

The host journals a live call before it writes the response to the guest. A
later trap in the guest therefore keeps the call in the journal. The next
attempt replays it.

The handler gets `seq`. An activity id plus `seq` identifies a call
position. A downstream system must also compare the request, or key on a
request hash. A re-run after a lost entry can send another request at the
same position.

## 3. What the tests prove

The tests are unit tests in `src/wasm_journal.rs`. CI runs them in the
`Run all-features lib tests` step.

| Claim | Test |
|-------|------|
| A granted call runs once and is journaled. | `a_live_host_call_is_journaled` |
| A full replay runs no handler and gives the same output. | `a_replay_serves_the_journal_without_running_the_handler` |
| A retry replays the finished calls and runs only the rest. | `a_failed_attempt_resumes_from_its_journal_prefix` |
| A persisted journal replays a float byte for byte. | `a_persisted_journal_replays_floats_byte_for_byte` |
| An ungranted name is denied in-band and journaled. | `an_ungranted_host_call_is_denied_in_band_and_journaled` |
| A replayed denial stays denied after a new grant. | `a_replayed_denial_stays_denied_after_a_new_grant` |
| With no grant, the import is not linked. | `no_grant_means_the_host_call_import_is_not_linked` |
| A changed request on replay is a non-retryable divergence. | `a_divergent_request_on_replay_is_non_retryable` |
| A changed name on replay is a non-retryable divergence. | `a_divergent_name_on_replay_is_non_retryable` |
| A revoked grant is a divergence on replay. | `a_revoked_grant_is_a_divergence_on_replay` |
| Entries left over after the run are a divergence. | `an_unconsumed_journal_entry_is_a_divergence` |
| A failed replay with entries left stays retryable. | `a_failed_replay_with_entries_left_stays_retryable` |
| A malformed journal is rejected before the guest runs. | `a_malformed_journal_is_rejected_before_the_guest_runs` |
| A recorded outcome over its bound makes the journal malformed. | `an_oversized_recorded_outcome_is_a_malformed_journal` |
| A journal over the call budget is malformed. A journal at the budget is not. | `a_journal_over_the_call_budget_is_malformed` |
| A journaled run links no ambient import. | `a_journaled_run_links_no_ambient_import` |
| A fatal handler error is journaled and replayed. | `a_fatal_handler_error_is_journaled_and_replayed` |
| A transient handler error is retryable and not journaled. | `a_transient_handler_error_is_retryable_and_not_journaled` |
| An oversized request never reaches the handler. A request at the bound does. | `an_oversized_request_is_rejected_before_the_handler` |
| Each bad call is invalid, runs no handler and is not journaled. | `each_bad_call_is_invalid_and_not_journaled` |
| An oversized response is journaled as an error. | `an_oversized_response_is_journaled_as_an_error` |
| A response at the bound is served. | `a_response_at_the_bound_is_served` |
| An error message is bounded after JSON escaping. | `an_error_message_is_bounded_after_json_escaping` |
| The call budget bounds the journal, also on replay. | `the_host_call_budget_bounds_the_journal` |
| The byte budget bounds host memory and the persisted size. | `the_journal_byte_budget_bounds_host_memory` |
| The byte count covers JSON escapes and framing. | `byte_size_bounds_the_serialized_journal` |
| A handler panic becomes a `WasmTrap`. | `a_panicking_handler_is_contained_as_a_wasm_trap` |
| The handler gets a stable `seq`, also on resume. | `the_handler_receives_the_journal_sequence_number` |
| A cancelled run skips the handler. | `a_cancelled_run_skips_the_handler` |
| A guest trap keeps its finished calls in the journal. | `a_guest_trap_keeps_the_calls_it_made_in_the_journal` |
| A failed response write keeps the call in the journal. The retry does not repeat it. | `a_failed_response_write_keeps_the_call_in_the_journal` |
| The journal round-trips through JSON. | `the_journal_round_trips_through_json` |
| One call, live or replayed, takes less than 1 ms (manual run only). | `host_call_overhead_microbenchmark` |

The last test has `#[ignore]`. Run it with this command:

```sh
cargo test -p autumn-harvest --features wasm-activities --lib \
  wasm_journal::tests::host_call_overhead_microbenchmark -- --ignored --nocapture
```

## 4. Cost

The microbenchmark runs a guest that makes 200 calls to a trivial handler. It
compares the run with a one-call run of the same guest. Each run reports the
median of 31 samples. The table gives the range over three runs of an
unoptimized debug build:

| Path | Cost |
|------|------|
| Live call | 12–17 µs per call |
| Replayed call | 12–20 µs per call |
| One-call run, instantiate included | 0.26–0.57 ms |

A replayed call costs about the same as a live call to a trivial handler.
Both encode and decode JSON, and both write the response to the guest. The
per-run instantiate stays the larger cost. A real handler, such as an HTTP
call, costs far more than the journal.

These figures measure an in-memory journal. G1 adds one database round trip
per live call. Nothing measures that cost yet, and it likely dominates.

## 5. Comparison with Golem

Golem runs WASM components as durable workers. By default, it records each
host call in an operation log. After a crash, it replays the log to rebuild
the worker.

| Property | Golem | This prototype |
|----------|-------|----------------|
| Each host call recorded | Yes, by default | Yes |
| Replay without a repeated side effect | Yes | Yes, inside one activity |
| Capability control | WASI and component imports | Named grants on a deny-all linker |
| Unit of durability | The whole worker | One activity attempt |
| Durable state across steps | Guest memory, rebuilt by log replay | Workflow history, as today |
| Authoring surface | Any WASM language | Rust activities that run sandboxed guest code |

The prototype gets the main benefit, journaled side effects, at activity
scope. It does not make the guest itself durable across steps. The Rust
workflow keeps that role.

ADR 0002 makes workflow and activity authoring Rust-only. The activity stays
a Rust activity. The guest is sandboxed code that the activity runs, not a
second authoring surface. Each call crosses an explicit host-call boundary,
as ADR 0002 requires for non-Rust code.

## 6. Limits

- **Persistence is the caller's job.** The prototype returns the journal and
  does not store it. The worker batches heartbeat details. A crash can lose
  the last entries, and a lost entry runs again on the retry.
- **The journal is at-least-once at its edge.** A crash between a side effect
  and its journal write repeats that one effect. A downstream system can
  drop the repeat with the activity id, `seq` and a request hash.
- **Some calls leave no entry.** An invalid call, a call over a budget, a
  transient failure, a handler panic and a cancelled call are not
  journaled.
- **Host parse work is not charged to fuel.** The host checks a request of
  up to 64 KiB as JSON. Only the wall clock bounds a guest that repeats bad
  calls.
- **The journal is not bound to its module or input.** A retry after a hot
  swap replays responses that another module received, if each call matches.
  The format also has no version tag.
- **Pre-guest time is not charged.** `invoke_journaled` does not take the
  dispatch start, so resolve and compile time do not count against the
  deadline.
- **A handler is not interruptible.** The epoch deadline stops guest code
  only. A slow handler delays the deadline until it returns. A handler must
  bound its own time.
- **No worker wiring.** `HarvestBuilder::wasm_activity` does not accept grants
  yet. The prototype runs through `invoke_journaled` only.
- **Generated code still needs a build and a signature.** An LLM writes
  source, not WASM. A compile service must build the module. The signing key
  must belong to that service, not to the model.
- **The journal holds payloads.** A request or a response can hold personal
  data. Its store needs the codec and erasure rules of `harvest_events`.

## 7. Path to production

| Step | Scope | Rough cost |
|------|-------|------------|
| G1 — durable journal | A synchronous journal write per live call. Use a task-queue column or a new table. Apply the payload codec and the PII erasure rules. | ~3 weeks |
| G2 — worker wiring | Grants on `HarvestBuilder::wasm_activity`. Load the journal at attempt start. Bind it to the module hash and the input hash, and give the format a version. Pass the dispatch start. Add metrics for denied and diverged calls. | ~2 weeks |
| G3 — typed ABI | Move `host_call` to a WIT interface on the component model, as the WASM spike recommends. | ~1 quarter, shared with T1 |

The go needs G1 and G2. G3 can follow the T1 work in the hot-code-swap
report.

## 8. Verdict

**Verdict:** conditional go for journaled host calls in WASM activities. Not
yet for WASM workflows: tier T2 stays a conditional go that waits for T1 users
and demonstrated demand.

Conditions for the go:

1. G1 lands first. A journal that a crash can lose is not a journal.
2. The first production use names its grants and its compile and signing
   service.
3. The step stays an activity. ADR 0002 and the hot-code-swap verdict stand.

Reasons for the go:

- The prototype proves the semantics: no repeated side effect on replay,
  divergence detection, and an audit entry for each completed call.
- It needs no new `WorkflowEvent` variant, no migration and no change to
  replay.
- The added code is small and stays inside the existing sandbox.

## Sources

- `docs/rnd/hot-code-swap.md` §9, the costed tiers and the T2 verdict.
- [wasm-activities-spike.md](wasm-activities-spike.md), the sandbox (#965).
- [ADR 0002](../adr/0002-rust-native-execution-boundary.md), the Rust-only
  authoring boundary.
- [ADR 0004](../adr/0004-security-extras.md), signed modules (#1838).
- Golem documentation on durable workers and the operation log,
  [learn.golem.cloud](https://learn.golem.cloud), read 2026-10-11.
