# Harvest end-to-end benchmark run (issue #941)

## Environment

| | |
|:--|:--|
| Logical CPUs | 4 |
| OS | linux / x86_64 |
| Profile | `bench` (release) |
| Harness | `autumn-harvest/tests/integration/e2e_bench_support.rs` |
| Workers | 1 per shard in the matrix cells, 8 concurrent workflows, 16 concurrent activities |
| Poll interval | 25 ms (LISTEN/NOTIFY wired per shard) |
| Pool size | 32 per shard, and per worker process in a capacity cell |
| Capacity | 4 worker process(es) per shard, 16 generator connections per shard, 20s steps x1.25 |
| Postgres | PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit |


## Results

| scenario | shards | metric | value | sound |
|:--|--:|:--|--:|:--|
| `capacity_canonical` | 1 | `sustainable_per_sec` | 76.30 | yes |
| `capacity_canonical` | 1 | `sustainable_per_shard_per_sec` | 76.30 | yes |
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 987.89 | yes |
| `capacity_canonical` | 1 | `steps` | 7.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth 0.4 ms/s; judged-window p99 273.3/294.9/290.3 ms; step p99 296.6 ms; generator wait p99 2.7 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth -0.2 ms/s; judged-window p99 280.4/309.3/300.4 ms; step p99 300.4 ms; generator wait p99 3.2 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope -0.0 runs/s; median growth -1.9 ms/s; judged-window p99 292.2/306.8/304.1 ms; step p99 289.2 ms; generator wait p99 3.8 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.1 runs/s; median growth -3.7 ms/s; judged-window p99 508.9/281.9/286.7 ms; step p99 371.8 ms; generator wait p99 5.6 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope 0.1 runs/s; median growth 2.2 ms/s; judged-window p99 257.9/283.1/285.9 ms; step p99 274.8 ms; generator wait p99 6.0 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope 0.1 runs/s; median growth 2.1 ms/s; judged-window p99 277.7/308.2/311.6 ms; step p99 308.2 ms; generator wait p99 7.1 ms; sustainable
* step 5: offered 76.3/s per shard; achieved 76.2; runs 1526; backlog slope 0.7 runs/s; median growth 0.9 ms/s; judged-window p99 1039.2/992.8/824.9 ms; step p99 987.9 ms; generator wait p99 10.9 ms; sustainable
* step 6: offered 95.4/s per shard; achieved 95.4; runs 1908; backlog slope 20.3 runs/s; median growth 273.9 ms/s; judged-window p99 2445.3/3941.8/5005.6 ms; step p99 4966.0 ms; generator wait p99 10.4 ms; **not sustainable**: the backlog grew 20.34 runs/s, over the 4.77 runs/s limit; the median latency grew 273.9 ms/s, over the 5 ms/s limit; the p99 grew from 2445.3 ms to 5005.6 ms, over the 4990.5 ms limit
* ladder: 7 step(s), stopped EngineBound; sustainable 76.3 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 170.2s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

