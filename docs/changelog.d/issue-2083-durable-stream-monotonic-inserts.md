## Phase X.Y — Monotonic durable stream inserts (issue #2083)

`append_stream_chunks` admits only offsets above the highest stored real
offset. Before, a re-run could store an offset below a reader's cursor, and
no reader received it. The truncation marker is not a real offset. The
shard-rebalance copy has its own insert path and does not change. The change
also stops two overlapping writers from filling different holes and passing
the cap. No migration and no new event.

Tests: `a_hole_behind_the_highest_stored_offset_is_never_filled`,
`a_dropped_hole_chunk_writes_no_truncation_marker`,
`an_unordered_first_batch_stores_every_offset` and
`durable_stream_re_run_stores_no_chunk_below_a_cursor`.
