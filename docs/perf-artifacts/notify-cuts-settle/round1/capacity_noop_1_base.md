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
| `capacity_noop` | 1 | `sustainable_per_sec` | 372.50 | yes |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | 372.50 | yes |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 93.02 | yes |
| `capacity_noop` | 1 | `steps` | 11.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 65.5/58.7/61.8 ms; step p99 60.6 ms; generator wait p99 2.1 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 66.5/68.8/73.1 ms; step p99 70.1 ms; generator wait p99 2.3 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 66.3/66.4/61.0 ms; step p99 66.3 ms; generator wait p99 2.5 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope 0.0 runs/s; median growth 0.2 ms/s; judged-window p99 61.7/60.7/62.4 ms; step p99 61.7 ms; generator wait p99 3.2 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.3 ms/s; judged-window p99 61.8/57.1/56.3 ms; step p99 58.0 ms; generator wait p99 2.4 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 53.4/50.0/48.2 ms; step p99 51.4 ms; generator wait p99 2.5 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.1 runs/s; median growth -0.0 ms/s; judged-window p99 83.4/42.9/26.2 ms; step p99 61.4 ms; generator wait p99 2.6 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 32.5/26.1/23.7 ms; step p99 33.0 ms; generator wait p99 3.7 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.4; runs 4768; backlog slope -0.0 runs/s; median growth -0.2 ms/s; judged-window p99 43.5/59.1/42.8 ms; step p99 50.0 ms; generator wait p99 4.8 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.8; runs 5960; backlog slope -2.9 runs/s; median growth -10.1 ms/s; judged-window p99 364.7/93.8/80.5 ms; step p99 339.2 ms; generator wait p99 19.5 ms; sustainable
* step 9: offered 372.5/s per shard; achieved 371.9; runs 7450; backlog slope 0.3 runs/s; median growth 0.9 ms/s; judged-window p99 75.4/84.6/104.3 ms; step p99 93.0 ms; generator wait p99 10.0 ms; sustainable
* step 10: offered 465.7/s per shard; achieved 465.2; runs 9314; backlog slope 83.4 runs/s; median growth 185.4 ms/s; judged-window p99 1155.6/2541.6/2868.7 ms; step p99 2852.8 ms; generator wait p99 506.6 ms; **not sustainable**: the backlog grew 83.42 runs/s, over the 23.29 runs/s limit; the median latency grew 185.4 ms/s, over the 5 ms/s limit; the p99 grew from 1155.6 ms to 2868.7 ms, over the 2411.2 ms limit; the generator wait p99 was 506.6 ms, over the 100 ms limit
* ladder: 11 step(s), stopped EngineBound; sustainable 372.5 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 249.0s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

