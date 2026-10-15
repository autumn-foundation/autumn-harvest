## Phase 3.x — codec column follow-ups (issue #2054)

Three follow-ups from the review of issue #1979.

**Duplicate starts ignore the codec.** `start_workflow` held the encode error
of `input` and `memo` until a branch writes a run. A start that attaches to
an existing run, or is refused as a duplicate, no longer fails when the
active codec fails. It probes for the existing run before the insert, so the
placeholder payload is never stored. A start that needs a new run still
returns the codec error. Tests: `start_duplicate_codec_failure_tests`, one
per reuse policy.

**Admin column reads keep the second decode pass.** `decode_column_lossy`
still decodes a business value that is shaped like an envelope. The strict
engine read returns it as written. The code comment and
`docs/operations/read-path-decode.md` state the difference. Test:
`an_operator_read_decodes_a_business_envelope_that_the_engine_read_keeps`.

**The column sweep cursor is per runtime.** The process-global cursor map and
its database-identity key are gone. `PayloadCodecs` now holds the cursor for
each shard, shared by its clones. Two runtimes in one process never share a
cursor, even when a Unix socket hides the server address. No migration and no
new event variant. Tests: `column_cursor_tests` in `codec_rotation.rs`.
