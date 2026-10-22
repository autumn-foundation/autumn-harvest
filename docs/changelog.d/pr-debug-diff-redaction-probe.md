## Phase — `is_redacted_export` probes one key without a `Value` tree (perf)

`debugger::is_redacted_export` reads `payload_policy` with a streaming map
visitor instead of parsing the whole history into a `serde_json::Value`.
`harvest debug diff` and `ReplayDebugger::trace_json` call it before they parse
the same text again. Results are unchanged for every input, including repeated
keys, non-object documents and invalid JSON. On a 10,001-event history the
probe drops from 514,087,439 to 92,813,673 instructions across two files
(-81.9%), and the whole `debug diff` pipeline drops 23.7%. Evidence and the
reproduce command are in `docs/performance-debug-diff-redaction-probe.md`.
Harness: `benches/debugger_diff_profile.rs`.

No new `WorkflowEvent` variant, no new migration, no replay impact.
