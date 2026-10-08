## Phase 3.x — the AEAD codec covers execution, signal and DLQ columns (issue #1979)

The codec used to encrypt the `harvest_events` payload fields only. Execution
input, output and memo, signal payloads and DLQ inputs stayed in clear.

- **New switch.** `HarvestBuilder::encode_payload_columns()` turns column
  encoding on. So does `PayloadCodecs::set_column_encoding(true)`. The default
  is off. The codec columns are:
  - `harvest_workflow_executions.input`, `.output` and `.memo`;
  - `harvest_signals.payload` and `harvest_dead_letters.input`;
  - the workflow task's `harvest_task_queue.input` and `.output`.
- **Readers first.** This release always decodes these columns. Upgrade every
  process, then turn the switch on.
- **Escape guard.** A new write escapes a value shaped like a codec envelope,
  with the switch on or off (issue #1253).
- **Engine reads.** These paths decode first:
  - the workflow handler, signal ingest, the client handle and queries;
  - retries, reruns, forks and DLQ replay;
  - completion triggers and callbacks, cross-shard children, external awaits
    and the quota reconciler.
- **New APIs.**
  - `PayloadCodecs`: `set_column_encoding`, `column_encoding`,
    `encode_column`, `encode_column_opt`, `encode_shared_column`,
    `decode_column` and `decode_column_opt`.
  - `WorkflowExecution::decode_columns`,
    `WorkflowHandleClient::payload_codecs` and
    `WorkflowHandleClient::with_stored_result_output`.
  - `_with_codecs` forms of `send_signal`, `send_signal_idempotent`,
    `send_signal_to_live_attempt`, `send_signal_from_resolved`,
    `resolve_and_signal_by_workflow_id`, `dlq::dead_letter`,
    `read_external_await_outcome` and `reconcile_quota_keys_from`.
  - `codec_rotation::CodecColumn`, `CODEC_COLUMNS` and
    `reencrypt_column_value_under`.
- **Breaking.**
  - `completion_callback::enqueue_completion_deliveries` and
    `quota_reconcile::spawn_quota_key_reconciler_for_shard` take a codec
    registry.
  - `BatchExecutorConfig` has a new `payload_codecs` field.
  - With column encoding on, a non-admin caller of
    `GET /workflows/{id}/result` gets the stored envelope (issue #608 rules).
- **Rotation.** The census and the sweep cover `codec_rotation::CODEC_COLUMNS`.
  Retirement stays blocked while a column holds the key. The column pass keeps
  to the batch budget and resumes where it stopped.
- **Docs.** `docs/security-posture.md` has a coverage row for every `JSONB`
  column, with a reason for each clear one. Follow-up gaps are issue #2043.
- **Invariants.** No new `WorkflowEvent` variant and no migration.
  `harvest_events` gains no new writer.

**Tests.** `codec_column_coverage_tests` runs a real worker with an AES-256-GCM
codec. It checks ciphertext at rest and plaintext in the handler, the signal,
the result, a retry and a DLQ replay. `codec_rotation_db_tests` adds census,
sweep, budget, retirement, erasure-race and undecodable-cell tests for the
columns. `replay_fidelity_is_byte_identical_across_a_sweep` now also checks the
execution columns. Unit tests pin the switch, the escape guard, the doc table
against `schema.rs`, and the codec-aware signal and DLQ writes.
