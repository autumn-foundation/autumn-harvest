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
- New `TaskDispatch::next_round_trips_per_queue`, with a default of one. It
  reports how many sequential round trips per queue one `next` call can make
  outside its wait. `RedisDispatch` reports six: two visits per queue, and
  each visit can heal a missing consumer group in three round trips.
- The worker's read timeout is now the wait plus one call timeout for each
  round trip the installed channel reports. The Redis read therefore cannot
  time out while it holds claimed entries. A custom channel keeps the old
  bound of one call timeout per queue, so a stalled channel still reaches
  the timeout and the PostgreSQL fallback quickly.

No `WorkflowEvent` variant, migration or configuration change.

**Tests**

- `dispatch_read_timeout_scales_with_the_channel_round_trip_count` and
  `a_channel_reports_one_read_round_trip_per_queue_by_default` pin the
  timeout and the default count.
- `a_read_reports_two_healed_visits_per_queue` pins the Redis count.
- The `autumn-harvest-redis` suite passes against a real Redis container.
