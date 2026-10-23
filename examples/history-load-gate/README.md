# history-load-gate

Measures the cold history load for the stage 2 gate (issue #2257).

Stage 2 of epic #2247 is an owner-local disk cache (#2258). It saves the
history read of a cold decision. The typed-state-snapshot spike set the
thresholds that would justify it: a cold share above 20% for a week, or a
cold p99 above 50 ms. This binary measures the bench half of that gate:

1. **One history.** It times one full history load of 10k and 50k events
   from Postgres, with `store::load_history_inflated`, the worker load
   path. A cold decision reads the history twice, so it also times two
   reads. It times the same history from a local SQLite file, with the
   schema and the read query of `autumn-harvest-sqlite`. Both sides decode
   each row through `PayloadCodecs::decode_event`.
2. **Owner restart.** It times the cold load of 1k, 10k and 100k live runs,
   on 1 connection and on 8, on each store. After a restart, each live run
   replays cold once.
   With the owner disk cache (#2258), it also times two passes on 8
   connections. The fill is the first decision of each run after a
   restart: a Postgres load and a sealed disk write. The next pass is the
   next decision of each run past the memory cache: a disk read and an
   anchor check.
3. **Cache cap.** It drives the real `WorkflowCache` with 1,000 entries
   over more live runs than that. It prints the cold share that the cap
   alone causes.
4. **Gate.** It prints the smallest measured size whose cold path passes
   50 ms at p99. It
   applies the thresholds to the fleet values. One trip gives `go`. Both
   values with no trip give `no-go`. Anything else is `undecided`.

The fleet half comes from `harvest.workflow.resident{outcome}` and
`harvest.workflow.resident.duration{outcome}`. The record doc gives the
queries.

## Run

Use a dedicated database, never a fleet database. The binary applies the
Harvest migrations, seeds the runs and deletes them after each step.

```sh
createdb history_load_gate
DATABASE_URL=postgres://postgres:postgres@localhost:5432/history_load_gate \
  cargo run -p history-load-gate --release
```

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | none | The Postgres database. |
| `SIZES` | `10000,50000` | The history sizes, in events. |
| `REPS` | 100 | The timed loads per size. |
| `PAYLOAD_BYTES` | 256 | The padding text in each payload. |
| `RUNS` | `1000,10000,100000` | The live-run counts of the warm-up. |
| `RUN_EVENTS` | 21 | The events of each live run. |
| `CONCURRENCY` | 8 | The connections of the parallel warm-up, on each store. |
| `MODEL_RUNS` | `500,1000,1250,2000,10000,100000` | The live runs per owner of the cache model. |
| `FLEET_COLD_SHARE` | none | The fleet cold share for a week, from 0 to 1. |
| `FLEET_COLD_P99_MS` | none | The fleet cold p99, in milliseconds. |

An empty list value skips its section. With `SIZES` and `RUNS` empty, the
binary opens no database and only applies the gate to the fleet values.

## Tests

```sh
cargo test -p history-load-gate --lib --bins
cargo test -p history-load-gate --test postgres
```

The Postgres test uses `HARVEST_TEST_DATABASE_URL` when it is set. Point it
at a dedicated database. Without it, the test starts a Postgres container.

Recorded results are in
[`docs/rnd/2026-10-22-stage-2-gate.md`](../../docs/rnd/2026-10-22-stage-2-gate.md).
