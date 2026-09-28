## Phase — Redis dispatch reads reach every queue under slow round trips (issue #1754)

`RedisDispatch::read_across_queues` divided its blocking budget between the
queues and did not count the Redis round trip. On a slow link, the deadline
passed before the rotation reached the queues at its tail. An entry on a tail
queue then waited for a later `next()` call. CI saw this as a failure of
`a_queue_near_the_tail_of_a_long_rotation_still_gets_a_blocking_look`.

**What changed**

- A deadline that passes mid-lap no longer ends the read. The rest of the lap
  uses non-blocking `XREADGROUP` calls. So every queue gets one look after the
  initial pass. A deadline at a lap boundary still ends the read, and a
  single-queue read is unchanged.
- New `TaskDispatch::next_round_trips`, with a default of one per queue. It
  reports the most sequential round trips that one `next` call can make
  outside its wait. `RedisDispatch` reports its worst case: 11 per queue plus
  3. That covers the delayed-entry promotion, two read visits per queue that
  can each heal a missing consumer group, a surplus requeue whose script the
  server forgot, and the discard of malformed entries.
- The worker's read timeout is now the wait plus one call timeout for each
  round trip the installed channel reports. The Redis read therefore cannot
  time out while it holds claimed entries. A custom channel keeps the old
  bound of one call timeout per queue, so a stalled channel still reaches
  the timeout and the PostgreSQL fallback quickly. Each Redis command keeps
  its own 5 s response timeout (contract C4), so a stalled command still
  fails on its own.

No `WorkflowEvent` variant, migration or configuration change.

**Tests**

- `dispatch_read_timeout_scales_with_the_channel_round_trip_count` and
  `a_channel_reports_one_read_round_trip_per_queue_by_default` pin the
  timeout and the default count.
- `a_next_call_reports_its_worst_case_round_trips` pins the Redis count.
- The `autumn-harvest-redis` suite passes against a real Redis container.
