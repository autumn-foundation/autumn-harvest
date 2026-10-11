# Design — Issue #2014: journaled, capability-gated host calls for agent code

Issue #2014 asks one question. Can `wasm_activities` run LLM-generated code
with each host call journaled and capability-gated? The deliverable is a
spike write-up under `docs/rnd/`, a prototype of one journaled host call, and
a go / no-go verdict.

**No migration. No new `WorkflowEvent` variant. No worker change. No
database.** The prototype is a new `wasm_journal` module behind the existing
`wasm-activities` feature. It adds one host import, `harvest::host_call`.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The sandbox links a host function only for a granted capability. An
  ungranted import fails at instantiation as a non-retryable `SandboxDenied`.
- The granted imports today are `env::now_millis`, `env::random_u64` and
  `env::env_get`. None of them is journaled. Two of them are nondeterministic.
- The guest ABI already exports `alloc`. The host can call it to place a
  response in guest memory, as it does for the activity input.
- A WASM activity records the same `ActivityScheduled` and `ActivityCompleted`
  events as a native one. Workflow replay never re-runs an activity body.
- So "replay" for agent code means a re-run of the activity body: a retry
  after a crash, or an offline re-run for audit. The journal serves both.
- `ActivityContext::heartbeat_details` gives a retry the last checkpoint that
  the previous attempt flushed. Heartbeat writes are batched, not synchronous.
- The WASM spike has no heartbeat import yet (`wasm-activities-spike.md` §4).
- ADR 0002 makes workflow and activity authoring Rust-only. The activity
  stays Rust. The guest is sandboxed code that it runs through an explicit
  host-call boundary.
- `hot-code-swap.md` §9 rates WASM activities "go" (T1) and WASM workflows
  "conditional go" (T2).

### 0.2 Brainstorm — how can a host call be journaled?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Journal each existing `env::` import. | Rejected. Each import needs its own journal shape. The grants stay fixed booleans, so an embedder cannot add a capability. |
| B2 | One generic import, `harvest::host_call(name, request)`. The embedder grants named handlers. The host journals each call as `(seq, name, request, outcome)`. | **Adopted.** One ABI, one journal shape, open capability set. |
| B3 | Journal into a new `WorkflowEvent` variant. | Rejected. It changes history and replay for a spike. Activity history stays unchanged. |
| B4 | Journal into a new Postgres table. | Deferred to GA. The spike proves the semantics in memory first. |
| B5 | Return the journal to the caller, even on failure. The caller persists it. | **Adopted.** The spike stays storage-free. The write-up names the GA store. |
| B6 | Use WASI 0.2 and the component model. | Rejected for the spike. The engine disables the component model today. The verdict lists it as a GA step. |
| B7 | Snapshot guest memory instead of a journal. | Rejected. Snapshots are large and opaque. A journal is small, and an auditor can read it. |
| B8 | Let the host write the response through the guest `alloc` export. | **Adopted.** A short guest buffer never forces a second call, so a side effect never runs twice for that reason. |

### 0.3 Reverse brainstorm — how can this design do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Generated code calls a capability that the embedder did not grant. | The host returns `HOST_CALL_DENIED` in-band and journals a `Denied` entry. The handler never runs. |
| R2 | A guest with no grant at all still reaches the host. | With no grant, `harvest::host_call` is not linked. Instantiation fails as `SandboxDenied`. |
| R3 | A replay runs a side effect again. | A journaled call is served from the journal. The handler is not called. A test counts the side effects. |
| R4 | A replay serves a response for a different request. | Replay compares the name and the request with the entry. A mismatch is a non-retryable `WasmJournalDivergence`. |
| R5 | A replay ends early and hides the divergence. | A run that leaves journal entries unconsumed is a divergence too. |
| R6 | An unjournaled `env::now_millis` or `env::random_u64` makes the replay diverge. | A journaled run links no ambient import. The embedder grants a clock or a random source as a named host call. |
| R7 | A transient handler failure is journaled, so each retry fails the same way. | A handler returns `Transient` or `Fatal`. Only `Fatal` is journaled. `Transient` fails the attempt as retryable and journals nothing. |
| R8 | A huge request burns host CPU outside the fuel budget. | The host rejects a request over `MAX_HOST_CALL_BYTES` before it reads the bytes. |
| R9 | A huge response bypasses the guest memory limit. | A response over `MAX_HOST_CALL_BYTES` is journaled as a `Fatal` error. The guest gets the error envelope. |
| R10 | A call loop grows the journal without bound. | After `MAX_HOST_CALLS` calls, the host returns `HOST_CALL_LIMIT`. Nothing more is journaled. |
| R11 | A handler panic crashes the worker. | A `catch_unwind` maps it to `WasmTrap`. The journal lock tolerates poison. |
| R12 | A crash between the side effect and the journal write runs the effect twice. | The host appends the entry before it writes the response. A lost write is still possible. The handler gets `seq`, which with the request forms an idempotency key. The write-up names the GA fix. |
| R13 | A slow handler holds the guest past its deadline. | Epoch interrupts do not reach host code. The write-up states this limit. The host skips the handler when the cancel token fired. |
| R14 | A malformed name or pointer reads out of bounds. | Each read is bounds-checked. A bad call returns `HOST_CALL_INVALID` and journals nothing. |
| R15 | A forged journal injects responses. | The journal is host data. The guest cannot write it. The GA store must be engine-owned, like heartbeat details. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | The sandbox, the deny-all linker, fuel, epoch and memory limits exist. Host imports are not journaled. Heartbeat details give a retry a checkpoint. |
| Red | Teams fear generated code that acts twice or acts in secret. A journal that an auditor can read is the reassurance they want. |
| Black | A journal does not make a side effect atomic. A slow handler escapes the epoch. LLM code still needs a compile step and a signature. |
| Yellow | One import gives an open capability set and a full audit trail. A retry resumes past its finished calls. History stays unchanged. |
| Green | The `seq` and a request hash give an idempotency key. A journal can feed replay-as-evaluation (#2001) for generated code. |
| Blue | Ship the prototype and the write-up. Verdict: conditional go for journaled activities, not yet for WASM workflows. |

### 0.5 Decisions

- Journal entry: `seq`, `name`, `request`, `outcome`. The outcome is `Ok`,
  `Err` or `Denied`.
- Guest ABI: `harvest::host_call(name_ptr, name_len, req_ptr, req_len) -> i64`.
  A non-negative result packs the response location, as `run` does. The
  response is a JSON envelope: `{"ok": value}` or `{"err": message}`.
  A negative result is an in-band code.
- Replay first, then live. A journal from a failed attempt is a prefix. The
  next attempt replays it and continues live.
- New non-retryable error type: `WasmJournalDivergence`.

---

## 1. TDD map

| Phase | Test | Proves |
|-------|------|--------|
| Red → green | `a_live_host_call_is_journaled` | A granted call runs once and the journal holds its request and response. |
| Red → green | `a_replay_serves_the_journal_without_running_the_handler` | A full journal gives the same output with no handler call. |
| Red → green | `a_failed_attempt_resumes_from_its_journal_prefix` | A retry replays the finished calls and runs only the rest live. |
| Red → green | `an_ungranted_host_call_is_denied_in_band_and_journaled` | R1. |
| Red → green | `no_grant_means_the_host_call_import_is_not_linked` | R2. |
| Red → green | `a_divergent_request_on_replay_is_non_retryable` | R4. |
| Red → green | `an_unconsumed_journal_entry_is_a_divergence` | R5. |
| Refactor: red → green | `a_malformed_journal_is_rejected_before_the_guest_runs` | R4 for a journal with a gap in `seq`. |
| Red → green | `a_journaled_run_links_no_ambient_import` | R6. |
| Red → green | `a_fatal_handler_error_is_journaled_and_replayed` | R7, the journaled half. |
| Red → green | `a_transient_handler_error_is_retryable_and_not_journaled` | R7, the retry half. |
| Red → green | `an_oversized_request_is_rejected_before_the_handler` | R8. |
| Red → green | `an_oversized_response_is_journaled_as_an_error` | R9. |
| Red → green | `the_host_call_budget_bounds_the_journal` | R10. |
| Red → green | `a_panicking_handler_is_contained_as_a_wasm_trap` | R11. |
| Red → green | `the_handler_receives_the_journal_sequence_number` | R12. |
| Red → green | `a_cancelled_run_skips_the_handler` | R13. |
| Red → green | `a_guest_trap_keeps_the_calls_it_made_in_the_journal` | Resume after a guest crash. |
| Red → green | `the_journal_round_trips_through_json` | The journal can be persisted as heartbeat details. |
| Refactor | `host_call_overhead_microbenchmark` (ignored) | The cost of one call, live and replayed. |
| Refactor | `agent_code_journal_docs` guard suite | The write-up cites each test and each bound, states a verdict and keeps short sentences. |

## 2. Acceptance criteria

| AC | Evidence |
|----|----------|
| Spike write-up under `docs/rnd/` | `docs/rnd/agent-code-host-call-journal.md`. |
| Prototype of one journaled host call | `harvest::host_call` in `autumn-harvest/src/wasm_journal.rs` and its unit tests. |
| Go / no-go verdict | The verdict section of the write-up, pinned by `agent_code_journal_docs`. |
