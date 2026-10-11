## R&D — journaled, capability-gated host calls for agent code (issue #2014)

**What shipped.** `docs/rnd/agent-code-host-call-journal.md` answers issue
#2014 with a working prototype and a verdict. The prototype is the new
`autumn_harvest::wasm_journal` module, behind the `wasm-activities` feature.
A guest imports one function, `harvest::host_call(name, request)`. The
embedder grants each capability by name through `HostCallGrants`. The host
journals each call as `seq`, name, request and outcome.
`invoke_journaled` replays an earlier journal in order and then runs live.
So a retry does not repeat a finished side effect, if the caller persists
the journal.

**Verdict.** Conditional go for journaled host calls in WASM activities. A
durable, synchronous journal store must land first. Not yet for WASM
workflows: tier T2 stays a conditional go that waits for demonstrated demand.

**Design.** With no grant, the import is not linked. A call to an ungranted
name is denied in-band and journaled. A journaled run links no ambient
`env::` import, so each nondeterministic value goes through the journal. A
changed request on replay, or a journal left unconsumed, is a non-retryable
`WasmJournalDivergence`. A transient handler failure is a retryable
`HostCallFailed` and is not journaled. The host bounds requests, responses,
names, the call count and the journal bytes. The journal stores raw JSON
text, so a persisted journal replays byte for byte. A replay checks each
grant again.

**Invariants.** No migration. No new `WorkflowEvent` variant. No worker or
replay change. `wasm_activities` gains one crate-private hook that links the
extra import.

**Tests.** Thirty-two unit tests in `src/wasm_journal.rs` cover record,
replay, resume, denial, divergence, revoked grants, malformed journals,
bounds at their edges, bad calls, panics and cancellation. Targeted mutation
runs confirm the key checks. One more test is an ignored microbenchmark. The
`agent_code_journal_docs` guard suite checks that the report cites each test
and each bound. The `lint` job runs it.

**Also.** `WasmJournalDivergence` joins the WASM faults that give no limit
sample. The wire-format test pins both new error types.
