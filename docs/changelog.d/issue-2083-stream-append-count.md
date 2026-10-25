## Phase — durable stream append skips the stored-row count (issue #2083)

`store::append_stream_chunks` counted every stored chunk of a run on each
append. It now reads the lowest and highest real offset in one statement and
skips the count while that span plus the new chunks fits the cap. Over the full
lifetime of 1,000 heavy-tailed streams, the count statement drops from 45,438
calls and 4,332,498 buffers to 20 calls and 7,228 buffers, and total buffers
fall 35.5%. The stored table is byte-identical (same md5). No migration, no
event and no schema change. See `docs/performance-stream-append-count.md`.
