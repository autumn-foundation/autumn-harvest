# Owner disk cache — warm-up after a restart (issue #2258)

> **Status:** measurement record. It answers the last item of the issue:
> "Owner-restart warm-up improves on the gate issue's baseline." The
> baseline is [`2026-10-22-stage-2-gate.md`](2026-10-22-stage-2-gate.md).
> **Result: met for histories of about 100 events and more. Not met for
> the 21-event runs of the baseline table.**

## Question

The cache is disposable. The worker deletes every cache file at start, so
the first decision of each run after a restart still reads Postgres. The
cache can help only after that first decision. A run past the memory cache
then reads its local file and the Postgres delta, not the full history.

So this record times two passes after a restart, on 8 connections and 8
workers:

- **Fill.** The first decision of each run: a Postgres load and a sealed
  disk write.
- **Next pass.** The next decision of each run past the memory cache. With
  the cache: a disk read, then one Postgres query for the anchor event and
  the delta. Without the cache: one full Postgres load. That second number
  is the baseline "Postgres, 8 conns" column.

## Method

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432/history_load_gate \
  SIZES= MODEL_RUNS= RUNS=2000 RUN_EVENTS=101 \
  cargo run -p history-load-gate --release
```

`examples/history-load-gate/src/disk_side.rs` drives the real
`OwnerDiskCache`. The runs spread over 64 partitions. Each read checks the
anchor and loads the delta, as the worker does. The bench calls the cache
directly, so `owner_disk_cache_min_events` does not apply.

| | |
|:--|:--|
| Date | 2026-10-23 |
| Base | `trunk-dev` at `9cde627`, with this change |
| CPUs | 4 logical, Intel Xeon at 2.10 GHz, cloud container |
| Postgres | 16.15, local, one server |
| Payload | 256 bytes of text per event |
| Profile | `release` |

## Results

### The baseline shape: 21 events per run

| Live runs | Fill, disk cache | Next pass, Postgres | Next pass, disk cache | Postgres / disk |
|---:|---:|---:|---:|---:|
| 1,000 | 190 ms | 63 ms | 85 ms | 0.7× |
| 2,000 | 351 ms | 135 ms | 136 ms | 1.0× |
| 10,000 | 1,907 ms | 805 ms | 1,130 ms | 0.7× |
| 100,000 | 19,808 ms | 6,258 ms | 12,032 ms | 0.5× |

### Longer histories, 2,000 live runs

| Events per run | Fill, disk cache | Next pass, Postgres | Next pass, disk cache | Postgres / disk |
|---:|---:|---:|---:|---:|
| 51 | 643 ms | 276 ms | 306 ms | 0.9× |
| 101 | 969 ms | 496 ms | 288 ms | 1.7× |
| 201 | 1,558 ms | 777 ms | 511 ms | 1.5× |
| 401 | 2,965 ms | 1,515 ms | 891 ms | 1.7× |
| 1,001 | 6,955 ms | 3,411 ms | 1,759 ms | 1.9× |

## Reading

- **A long history warms up faster.** From about 100 events, the next
  pass is 1.5 to 1.9 times faster with the cache. The gain grows with the
  history, because the disk replaces the full Postgres read.
- **A short history does not.** Below about 100 events, one Postgres read
  costs no more than the query for the anchor and the delta. The disk read
  and the unsealing then make the cache slower, by up to 2 times at 100k
  runs of 21 events.
- **The fill costs more than a plain load.** It adds a sealed write per
  run, about 1.5 to 3 times the Postgres pass. It runs once per run after
  a restart.
- **A first version took two queries per hit.** The anchor event and the
  delta came from two queries. One query now reads both, and the hit path
  skips its own delta query.

## Change that follows

`WorkerConfig::owner_disk_cache_min_events` defaults to 100. A run with a
shorter history never goes to disk, so it keeps the Postgres path. A memory
miss of such a run reads only the in-memory index of the cache, not a file.

## Limits

- One box, local Postgres. A network round trip makes each Postgres read
  slower, which favours the cache.
- Both stores run warm.
- The events are synthetic activity pairs.
- The bench times loads, not decisions.

## See also

- [`2026-10-22-stage-2-gate.md`](2026-10-22-stage-2-gate.md): the gate and
  its baseline.
- [ADR 0012](../adr/0012-owner-disk-cache.md): the design.
