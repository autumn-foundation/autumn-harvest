## Phase 3.x — the AEAD codec covers execution, signal and DLQ columns (issue #1979)

The codec used to encrypt the `harvest_events` payload fields only. Execution
input, output and memo, signal payloads and DLQ inputs stayed in clear.

- **New switch.** `HarvestBuilder::encode_payload_columns()`, or
  `PayloadCodecs::set_column_encoding(true)`, encodes
  `harvest_workflow_executions.input`, `.output` and `.memo`,
  `harvest_signals.payload`, `harvest_dead_letters.input`, and the workflow
  task's `harvest_task_queue.input` and `.output`. The default is off.
- **Readers first.** This release always decodes these columns. Upgrade every
  process, then turn the switch on.
- **Engine reads.** The workflow handler, signal ingest, the client handle,
  queries, retries, reruns, forks, DLQ replay, completion triggers and
  callbacks, cross-shard children, external awaits and the quota reconciler
  all decode first.
- **New APIs.** `PayloadCodecs::encode_column` / `decode_column`,
  `WorkflowExecution::decode_columns`, and `_with_codecs` forms of the signal
  send functions, `dlq::dead_letter` and `read_external_await_outcome`. The old
  forms keep their behavior.
- **Rotation.** The census and the sweep cover `codec_rotation::CODEC_COLUMNS`.
  Retirement stays blocked while a column holds the key.
- **Docs.** `docs/security-posture.md` has a coverage row for every `JSONB`
  column, with a reason for each clear one. Follow-up gaps are issue #2043.
- **Invariants.** No new `WorkflowEvent` variant and no migration.
  `harvest_events` gains no new writer.

**Tests.** `codec_column_coverage_tests` runs a real worker with an AES-256-GCM
codec. It checks ciphertext at rest and plaintext in the handler, the signal,
the result, a retry and a DLQ replay. `codec_rotation_db_tests` adds census,
sweep, retirement, erasure-race and undecodable-cell tests for the columns.
`replay_fidelity_is_byte_identical_across_a_sweep` now also checks the
execution columns. Unit tests pin the switch, the doc table against
`schema.rs`, and the codec-aware signal and DLQ writes.
