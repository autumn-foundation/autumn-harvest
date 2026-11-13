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
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 1265.01 | yes |
| `capacity_noop` | 1 | `steps` | 10.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 12.4/31.6/12.0 ms; step p99 26.8 ms; generator wait p99 1.7 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 13.4/10.0/11.3 ms; step p99 13.4 ms; generator wait p99 1.4 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth -0.0 ms/s; judged-window p99 12.0/11.8/10.7 ms; step p99 13.5 ms; generator wait p99 1.5 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 12.1/32.9/11.1 ms; step p99 19.0 ms; generator wait p99 2.0 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 13.4/16.0/13.3 ms; step p99 14.6 ms; generator wait p99 2.0 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 22.3/19.6/21.0 ms; step p99 22.5 ms; generator wait p99 2.6 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 33.7/103.3/27.1 ms; step p99 71.5 ms; generator wait p99 3.1 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.6; runs 3814; backlog slope 0.1 runs/s; median growth 0.7 ms/s; judged-window p99 44.7/51.4/63.8 ms; step p99 54.8 ms; generator wait p99 4.7 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.3; runs 4768; backlog slope -0.3 runs/s; median growth -0.9 ms/s; judged-window p99 78.4/72.0/69.7 ms; step p99 87.8 ms; generator wait p99 7.1 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.6; runs 5960; backlog slope -0.3 runs/s; median growth -2.4 ms/s; judged-window p99 1137.2/1281.5/494.1 ms; step p99 1265.0 ms; generator wait p99 32.5 ms; sustainable
* step 9: offered 372.5/s per shard; achieved 371.6; runs 7450; backlog slope 21.4 runs/s; median growth 69.7 ms/s; judged-window p99 403.8/781.3/1235.7 ms; step p99 1215.4 ms; generator wait p99 149.2 ms; **not sustainable**: the backlog grew 21.44 runs/s, over the 18.62 runs/s limit; the median latency grew 69.7 ms/s, over the 5 ms/s limit; the p99 grew from 403.8 ms to 1235.7 ms, over the 907.7 ms limit; the generator wait p99 was 149.2 ms, over the 100 ms limit
* ladder: 10 step(s), stopped EngineBound; sustainable 298.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 226.9s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

