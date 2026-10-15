## Phase 3.x — the AEAD codec covers the short-lived payload copies (issue #2043)

Issue #1979 left the short-lived payload copies in clear. A finished activity
task row stays until task retention removes it, and a callback dead letter
stays for good. So those copies were not short-lived. This change encodes
them.

- **Columns now encoded.**
  - The activity task row: `harvest_task_queue.input`, `.output` and
    `.heartbeat_details`.
  - `harvest_completion_deliveries.payload`, and the CALLBACK dead letter that
    copies it.
  - `harvest_debounce.last_input`, `harvest_start_throttle.input` and
    `harvest_completion_trigger_outbox.target_input`.
  - `harvest_batch_jobs.signal_payload`.
- **Columns that stay clear.** The coverage table in
  `docs/security-posture.md` gives a reason for each:
  `harvest_event_batches.buffered_payloads`,
  `harvest_cross_shard_children.child_spec`, `harvest_schedules.workflow_input`
  and `context_headers` on executions and tasks.
- **Readers first.** This release always decodes these columns. Upgrade every
  worker, then turn on column encoding. An older worker runs an activity on the
  envelope.
- **Engine reads.** These paths decode first:
  - activity dispatch, the heartbeat read-back of a retry, and session tasks;
  - the callback scanner, before it signs and sends the body;
  - the debounce, throttle and outbox fire paths;
  - the batch executor, before it sends each signal.
- **Failure policy.** An activity attempt whose input or checkpoint does not
  decode fails with `PayloadDecodeFailed`, and the retry policy applies. A
  scanner leaves the row pending and logs a warning with no payload text. A
  callback delivery backs off, and its dead letter keeps the stored body.
- **One envelope layer.** A dead letter and a requeued task keep one layer for
  every task type. The dead-letter writer takes the input as its source row
  stores it.
- **New APIs.**
  - `heartbeat::spawn_heartbeat_flusher_with_codecs`.
  - `completion_callback::fire_due_completion_deliveries_with_codecs`.
  - `debounce::admit_debounced_start_with_codecs`.
  - `throttle::reserve_or_defer_with_codecs` and
    `reserve_or_defer_or_shed_with_codecs`.
  - `batch::submit_batch_job_with_codecs`.
  - `queue::activity_row_output`.
  - `failure::ERROR_TYPE_PAYLOAD_DECODE_FAILED`.
- **Rotation.** `codec_rotation::CODEC_COLUMNS` lists each new column. The
  census and the sweep cover an activity task row now. A row written before
  this release can hold a clear value that has the shape of an envelope. The
  sweep reads such a value as an envelope.
- **Invariants.** No new `WorkflowEvent` variant and no migration.
  `harvest_events` gains no new writer.

**Tests.** `codec_column_coverage_tests` runs a real worker with an AES-256-GCM
codec. It checks ciphertext at rest and plaintext in the handler, in the retry
checkpoint, in the callback body, in the started run and in the batch signal.
`codec_rotation_db_tests` seeds every swept column and checks the census, the
sweep, the budget and the retirement gate. Unit tests pin the dead-letter
layer rule and the doc table against `schema.rs`.
