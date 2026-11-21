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
| `capacity_noop` | 1 | `sustainable_per_sec` | 298.00 | yes |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | 298.00 | yes |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 59.48 | yes |
| `capacity_noop` | 1 | `steps` | 10.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth -0.3 ms/s; judged-window p99 63.6/67.4/54.3 ms; step p99 61.5 ms; generator wait p99 2.1 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 63.9/61.9/64.6 ms; step p99 61.8 ms; generator wait p99 2.0 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 59.2/63.0/59.2 ms; step p99 62.4 ms; generator wait p99 2.3 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope -0.0 runs/s; median growth 0.1 ms/s; judged-window p99 58.0/59.4/56.8 ms; step p99 57.3 ms; generator wait p99 2.5 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 47.4/40.9/43.2 ms; step p99 43.5 ms; generator wait p99 2.4 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 21.9/32.1/43.3 ms; step p99 40.0 ms; generator wait p99 2.6 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 19.5/18.2/18.8 ms; step p99 20.8 ms; generator wait p99 2.8 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 20.5/36.0/28.0 ms; step p99 30.8 ms; generator wait p99 3.2 ms; sustainable
* step 7 (noise only, retried): offered 238.4/s per shard; achieved 238.4; runs 4768; backlog slope 0.8 runs/s; median growth 0.5 ms/s; judged-window p99 37.6/34.2/179.3 ms; step p99 160.5 ms; generator wait p99 4.0 ms; **not sustainable**: the p99 grew from 37.6 ms to 179.3 ms, over the 175.3 ms limit
* step 7: offered 238.4/s per shard; achieved 238.4; runs 4768; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 39.7/37.7/38.4 ms; step p99 39.5 ms; generator wait p99 3.5 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.9; runs 5960; backlog slope -0.1 runs/s; median growth -0.2 ms/s; judged-window p99 57.5/65.1/54.9 ms; step p99 59.5 ms; generator wait p99 5.2 ms; sustainable
* step 9: offered 372.5/s per shard; achieved 372.1; runs 7450; backlog slope 19.5 runs/s; median growth 82.0 ms/s; judged-window p99 79.6/80.9/1098.9 ms; step p99 1073.6 ms; generator wait p99 29.8 ms; **not sustainable**: the backlog grew 19.49 runs/s, over the 18.62 runs/s limit; the median latency grew 82.0 ms/s, over the 5 ms/s limit; the p99 grew from 79.6 ms to 1098.9 ms, over the 259.2 ms limit
* ladder: 10 step(s), stopped EngineBound; sustainable 298.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 247.0s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

