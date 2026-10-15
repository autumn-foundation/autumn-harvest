## Phase 3.x — the AEAD codec covers the short-lived payload copies (issue #2043)

Issue #1979 left the short-lived payload copies in clear. A finished activity
task row stays until task retention removes it, and a callback dead letter
stays until a redrive removes it. So those copies were not short-lived. This
change encodes them.

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
  and `context_headers` on executions and tasks. A session task keeps its
  session id in clear, because the id is not payload.
- **Readers first.** This release always decodes these columns. An older
  worker runs an activity on the envelope. An older scanner sends the envelope
  to a webhook, starts a run on it, or sends it as a signal. Turn column
  encoding on only after every worker runs this release. If it is on already,
  turn it off, finish the roll, and turn it on again.
- **Engine reads.** These paths decode first:
  - activity dispatch, the heartbeat read-back of a retry, and session tasks;
  - the callback scanner, before it signs and sends the body;
  - the debounce, throttle and outbox fire paths;
  - the batch executor, before it sends each signal.
- **Failure policy.**
  - An activity attempt whose input or checkpoint does not decode fails with
    `PayloadDecodeFailed`, and the retry policy applies.
  - The debounce, throttle and outbox scanners move the row back and log a
    warning with no payload text. A throttle row past its deadline is still
    dropped.
  - The batch executor leaves a signal job as it is. A cancel or terminate job
    never reads the payload, so a missing key does not block it.
  - The callback scanner counts a failed attempt, so the delivery backs off.
    After its last attempt, the dead letter keeps the stored body, and a
    redrive sends it.
- **Heartbeat.** The flusher encodes the checkpoint. A checkpoint that does not
  encode keeps the stored checkpoint, and the flush refreshes the heartbeat
  time only. The flusher then encodes it again on each tick until a newer
  heartbeat replaces it. An unchanged checkpoint reuses its last envelope, so a
  repeat does not spend a new random nonce.
- **One envelope layer.** A dead letter and a requeued task keep one layer for
  every task type. The dead-letter writer takes the input as its source row
  stores it.
- **New APIs.**
  - `heartbeat::spawn_heartbeat_flusher_with_codecs`.
  - `queue::record_heartbeat_liveness_sent_ago` and `queue::activity_row_output`.
  - `completion_callback::fire_due_completion_deliveries_with_codecs`.
  - `debounce::admit_debounced_start_with_codecs`.
  - `throttle::reserve_or_defer_with_codecs` and
    `reserve_or_defer_or_shed_with_codecs`.
  - `batch::submit_batch_job_with_codecs`.
  - `failure::ERROR_TYPE_PAYLOAD_DECODE_FAILED`.
- **Breaking.**
  - With column encoding on, a non-admin caller of
    `GET /workflows/{id}/stack` gets the stored checkpoint envelope.
  - `dlq::dead_letter_with_codecs` reads `NewDeadLetterEntry::input` as the
    source row stores it, for every task type.
  - A DLQ replay or redrive of an activity entry encodes its input when column
    encoding is on.
  - The plain writers keep the identity registry and store in clear:
    `admit_debounced_start`, `reserve_or_defer`, `reserve_or_defer_or_shed`,
    `submit_batch_job`, `fire_due_completion_deliveries` and
    `spawn_heartbeat_flusher_with`. Call the `_with_codecs` form.
- **Rotation.** `codec_rotation::CODEC_COLUMNS` lists each new column. The
  census and the sweep cover an activity task row now. A row written before
  this release can hold a clear value that has the shape of an envelope. The
  sweep reads such a value as an envelope. Drain the activity queues before you
  turn column encoding on, and wait for task retention before you retire a
  key.
- **Erasure.** Erasure (issue #495) does not reach the task rows or the
  outbox. The security posture page says so.
- **Invariants.** No new `WorkflowEvent` variant and no migration.
  `harvest_events` gains no new writer.

**Tests.** `codec_column_coverage_tests` runs a real worker with an AES-256-GCM
codec. It checks ciphertext at rest. It checks plaintext in the handler, the
retry checkpoint, the callback body, the started run and the batch signal. It
also checks that a value which does not decode never reaches a handler, a
webhook or a start. `codec_rotation_db_tests` seeds every swept column and
checks the census, the sweep, the budget and the retirement gate. Unit tests
pin the dead-letter layer rule, the session id read and the doc table against
`schema.rs`.
