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
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 92.47 | yes |
| `capacity_noop` | 1 | `steps` | 11.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 10.7/25.6/10.8 ms; step p99 22.7 ms; generator wait p99 2.0 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 14.0/13.8/10.9 ms; step p99 13.8 ms; generator wait p99 2.1 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 62.8/53.9/57.4 ms; step p99 56.5 ms; generator wait p99 2.2 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 57.8/56.1/57.5 ms; step p99 55.4 ms; generator wait p99 2.3 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 51.4/50.8/49.4 ms; step p99 49.5 ms; generator wait p99 2.1 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 29.4/30.8/37.9 ms; step p99 35.0 ms; generator wait p99 2.1 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 18.3/52.7/16.9 ms; step p99 43.2 ms; generator wait p99 2.8 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 20.5/23.5/26.5 ms; step p99 24.8 ms; generator wait p99 3.1 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.4; runs 4768; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 38.8/47.5/45.6 ms; step p99 41.0 ms; generator wait p99 3.8 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.9; runs 5960; backlog slope -0.5 runs/s; median growth -0.1 ms/s; judged-window p99 57.4/543.3/62.4 ms; step p99 521.0 ms; generator wait p99 8.6 ms; sustainable
* step 9: offered 372.5/s per shard; achieved 372.0; runs 7450; backlog slope -0.0 runs/s; median growth 0.0 ms/s; judged-window p99 84.7/84.7/81.2 ms; step p99 92.5 ms; generator wait p99 10.7 ms; sustainable
* step 10: offered 465.7/s per shard; achieved 465.1; runs 9314; backlog slope 38.7 runs/s; median growth 90.4 ms/s; judged-window p99 815.1/1316.3/1736.3 ms; step p99 1724.4 ms; generator wait p99 285.2 ms; **not sustainable**: the backlog grew 38.72 runs/s, over the 23.29 runs/s limit; the median latency grew 90.4 ms/s, over the 5 ms/s limit; the p99 grew from 815.1 ms to 1736.3 ms, over the 1730.2 ms limit; the generator wait p99 was 285.2 ms, over the 100 ms limit
* ladder: 11 step(s), stopped EngineBound; sustainable 372.5 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 247.6s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

