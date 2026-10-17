## Phase 3.x — the AEAD codec covers the last payload copies (issue #2043)

The first change for issue #2043 left four groups in clear. This change
encodes them. No payload column stays clear now.

- **Columns now encoded.**
  - `harvest_event_batches.buffered_payloads`. Each admission stores one
    envelope, so the SQL append and the element count still work.
  - `harvest_cross_shard_children.child_spec`. The whole spec is one envelope.
  - `harvest_schedules.workflow_input`.
  - `context_headers` on `harvest_workflow_executions` and
    `harvest_task_queue`, and the headers inside the start options of the
    debounce, throttle and batch rows.
- **Readers first.** This release always decodes these columns. Turn column
  encoding on only after every worker and every API node runs this release.
- **Engine reads.** These paths decode first:
  - the batch flush and fire, the cross-shard relay and the schedule tick;
  - the workflow worker, the activity worker, a retry and a rerun, for the
    headers;
  - a manual trigger and a backfill, for the schedule input.
- **Failure policy.** A value that does not decode never starts work without it.
  - A workflow run whose headers do not decode fails. An activity attempt fails
    with `PayloadDecodeFailed`. A workflow retry is skipped.
  - A batch, debounce or throttle row moves back and stays. A cross-shard
    child stays pending. A due schedule stays due, and a backfill slot counts as
    failed.
- **Schedules.** The reconciler compares the decoded input, so a new nonce is
  not drift, and a converged schedule is not written again. The patch route
  encodes a new input and keeps a stored one.
- **Exports.** An export includes the headers only for a caller who may decode.
  The retention archive never decodes, so it leaves an encoded header map out
  of the archived document. Plaintext headers never reach cold storage.
- **Erasure.** Erasure replaces an encoded cross-shard child spec with a
  tombstone. It needs no key.
- **Rotation.** `codec_rotation::CODEC_COLUMNS` lists each new column, with a
  cell shape. The census counts a buffer row once for each key. One swap
  converts every envelope element of a buffer.
- **New APIs.**
  - `PayloadCodecs::encode_headers`, `decode_headers` and `decode_headers_opt`.
  - `register_workflow_schedules_with_codecs`,
    `scheduler::update_workflow_schedule_with_codecs` and
    `scheduler::register_workflow_schedules_for_shard_with_codecs`.
  - `cross_shard_child::record_cross_shard_child_with_codecs`.
  - `history_export::export_headers`.
  - `codec_rotation::CellShape` and `reencrypt_column_cell_under`.
- **Breaking.**
  - With column encoding on, the schedule API and the UI show the stored
    envelope for a schedule input.
  - The plain writers keep the identity registry and store in clear:
    `register_workflow_schedules`, `update_workflow_schedule` and
    `record_cross_shard_child`. Call the `_with_codecs` form.
  - The byte cap of an event batch counts the stored bytes. Ciphertext reaches
    the cap sooner than plaintext.
- **Invariants.** No new `WorkflowEvent` variant and no migration.
  `harvest_events` gains no new writer.

**Tests.** `codec_column_coverage_tests` runs a real worker with an AES-256-GCM
codec. It checks ciphertext at rest and plaintext in the handler for the
headers, the batch buffer and the schedule input. It also checks that a value
which does not decode never starts work. `cross_shard_children_tests` covers
the spec, the relay and the erasure tombstone. `codec_rotation_db_tests`
seeds every swept column.
