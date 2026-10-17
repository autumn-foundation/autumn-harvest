## Phase X.Y — Monotonic durable stream inserts (issue #2083)

`append_stream_chunks` admits only offsets above the highest stored real
offset. Before, a re-run could store an offset below a reader's cursor, and
no reader received it. The truncation marker is not a real offset. The
shard-rebalance copy has its own insert path and does not change. A per-run
advisory lock orders overlapping writers, such as a stale worker after a
lease reclaim, so they cannot pass the cap or insert out of order. No
migration and no new event.

Tests: `a_hole_behind_the_highest_stored_offset_is_never_filled`,
`a_dropped_hole_chunk_writes_no_truncation_marker`,
`an_unordered_first_batch_stores_every_offset`,
`a_mixed_batch_at_the_cap_marks_only_the_new_chunk`,
`overlapping_appends_for_one_run_are_serialized` and
`durable_stream_stores_no_chunk_below_a_live_readers_cursor`.
