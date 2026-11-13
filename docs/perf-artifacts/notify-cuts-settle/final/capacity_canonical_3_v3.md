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
| `capacity_canonical` | 1 | `sustainable_per_sec` | 61.00 | yes |
| `capacity_canonical` | 1 | `sustainable_per_shard_per_sec` | 61.00 | yes |
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 327.49 | yes |
| `capacity_canonical` | 1 | `steps` | 6.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth -0.4 ms/s; judged-window p99 253.0/251.7/268.5 ms; step p99 289.2 ms; generator wait p99 3.7 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth -0.9 ms/s; judged-window p99 249.8/274.8/272.9 ms; step p99 261.4 ms; generator wait p99 2.8 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope 0.0 runs/s; median growth 2.0 ms/s; judged-window p99 252.9/273.5/307.2 ms; step p99 273.5 ms; generator wait p99 3.2 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.0 runs/s; median growth -2.2 ms/s; judged-window p99 298.6/271.1/274.8 ms; step p99 271.8 ms; generator wait p99 3.5 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope -0.0 runs/s; median growth -1.0 ms/s; judged-window p99 271.4/260.9/243.6 ms; step p99 260.9 ms; generator wait p99 5.3 ms; sustainable
* step 4 (noise only, retried): offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope 0.9 runs/s; median growth 6.4 ms/s; judged-window p99 309.3/271.5/894.1 ms; step p99 761.8 ms; generator wait p99 8.7 ms; **not sustainable**: the median latency grew 6.4 ms/s, over the 5 ms/s limit; the p99 grew from 309.3 ms to 894.1 ms, over the 718.6 ms limit
* step 4: offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope 0.3 runs/s; median growth 4.6 ms/s; judged-window p99 317.8/344.9/335.2 ms; step p99 327.5 ms; generator wait p99 10.1 ms; sustainable
* step 5 (noise only, retried): offered 76.3/s per shard; achieved 76.2; runs 1526; backlog slope 1.4 runs/s; median growth 16.4 ms/s; judged-window p99 546.2/661.9/713.2 ms; step p99 674.6 ms; generator wait p99 10.1 ms; **not sustainable**: the median latency grew 16.4 ms/s, over the 5 ms/s limit
* step 5: offered 76.3/s per shard; achieved 76.2; runs 1526; backlog slope 4.0 runs/s; median growth 75.8 ms/s; judged-window p99 519.2/414.2/1356.5 ms; step p99 1332.3 ms; generator wait p99 12.6 ms; **not sustainable**: the backlog grew 4.02 runs/s, over the 3.81 runs/s limit; the median latency grew 75.8 ms/s, over the 5 ms/s limit; the p99 grew from 519.2 ms to 1356.5 ms, over the 1138.4 ms limit
* ladder: 6 step(s), stopped EngineBound; sustainable 61.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 187.0s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

