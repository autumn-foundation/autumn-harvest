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
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 374.53 | yes |
| `capacity_noop` | 1 | `steps` | 10.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 13.3/11.8/14.1 ms; step p99 26.8 ms; generator wait p99 1.5 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.2 ms/s; judged-window p99 15.9/14.4/12.7 ms; step p99 15.9 ms; generator wait p99 1.7 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 13.6/13.1/13.9 ms; step p99 14.6 ms; generator wait p99 1.5 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 10.3/9.4/12.1 ms; step p99 25.2 ms; generator wait p99 2.1 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 10.3/11.7/12.6 ms; step p99 13.5 ms; generator wait p99 2.0 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 12.1/15.0/23.4 ms; step p99 18.5 ms; generator wait p99 2.2 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.0 runs/s; median growth -0.2 ms/s; judged-window p99 34.2/29.1/21.3 ms; step p99 91.6 ms; generator wait p99 3.4 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope -0.1 runs/s; median growth -0.3 ms/s; judged-window p99 45.2/35.7/34.6 ms; step p99 40.2 ms; generator wait p99 4.0 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.3; runs 4768; backlog slope 0.0 runs/s; median growth 0.2 ms/s; judged-window p99 54.7/38.0/53.9 ms; step p99 53.4 ms; generator wait p99 4.6 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.6; runs 5960; backlog slope -0.4 runs/s; median growth -1.3 ms/s; judged-window p99 97.7/111.8/80.0 ms; step p99 374.5 ms; generator wait p99 25.5 ms; sustainable
* step 9: offered 372.5/s per shard; achieved 372.1; runs 7450; backlog slope 21.8 runs/s; median growth 59.7 ms/s; judged-window p99 286.0/828.9/980.7 ms; step p99 963.6 ms; generator wait p99 144.4 ms; **not sustainable**: the backlog grew 21.85 runs/s, over the 18.62 runs/s limit; the median latency grew 59.7 ms/s, over the 5 ms/s limit; the p99 grew from 286.0 ms to 980.7 ms, over the 672.0 ms limit; the generator wait p99 was 144.4 ms, over the 100 ms limit
* ladder: 10 step(s), stopped EngineBound; sustainable 298.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 226.6s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

