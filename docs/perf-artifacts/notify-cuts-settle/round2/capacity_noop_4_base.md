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
| `capacity_noop` | 1 | `sustainable_per_sec` | 238.40 | yes |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | 238.40 | yes |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 61.03 | yes |
| `capacity_noop` | 1 | `steps` | 9.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 64.1/65.1/64.7 ms; step p99 64.7 ms; generator wait p99 2.0 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth 0.0 ms/s; judged-window p99 66.9/65.7/69.7 ms; step p99 67.2 ms; generator wait p99 2.5 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 66.8/61.2/65.4 ms; step p99 65.7 ms; generator wait p99 1.8 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 69.7/57.8/65.2 ms; step p99 64.0 ms; generator wait p99 3.1 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.2 ms/s; judged-window p99 60.8/57.9/56.6 ms; step p99 56.9 ms; generator wait p99 2.5 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 40.4/43.7/45.2 ms; step p99 45.0 ms; generator wait p99 2.4 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope 0.1 runs/s; median growth 0.0 ms/s; judged-window p99 21.3/93.5/41.9 ms; step p99 73.5 ms; generator wait p99 3.0 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope 0.0 runs/s; median growth 0.2 ms/s; judged-window p99 21.9/26.8/27.6 ms; step p99 28.2 ms; generator wait p99 3.0 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.3; runs 4768; backlog slope -0.0 runs/s; median growth -0.2 ms/s; judged-window p99 58.6/62.5/64.2 ms; step p99 61.0 ms; generator wait p99 4.9 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.8; runs 5960; backlog slope 16.6 runs/s; median growth 63.1 ms/s; judged-window p99 116.2/1106.8/1133.6 ms; step p99 1113.3 ms; generator wait p99 37.7 ms; **not sustainable**: the backlog grew 16.55 runs/s, over the 14.90 runs/s limit; the median latency grew 63.1 ms/s, over the 5 ms/s limit; the p99 grew from 116.2 ms to 1133.6 ms, over the 332.4 ms limit
* ladder: 9 step(s), stopped EngineBound; sustainable 238.4 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 205.1s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

