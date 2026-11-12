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
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 812.18 | yes |
| `capacity_canonical` | 1 | `steps` | 7.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth 1.1 ms/s; judged-window p99 247.1/257.3/264.1 ms; step p99 264.1 ms; generator wait p99 2.6 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth 4.3 ms/s; judged-window p99 224.5/243.0/268.1 ms; step p99 261.6 ms; generator wait p99 3.6 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope 0.0 runs/s; median growth 0.5 ms/s; judged-window p99 269.8/251.1/228.8 ms; step p99 251.1 ms; generator wait p99 3.4 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope 0.0 runs/s; median growth 2.2 ms/s; judged-window p99 276.4/259.1/258.5 ms; step p99 303.1 ms; generator wait p99 4.2 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope -0.1 runs/s; median growth -1.1 ms/s; judged-window p99 338.3/293.6/298.9 ms; step p99 313.1 ms; generator wait p99 7.4 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 60.9; runs 1220; backlog slope -0.1 runs/s; median growth -3.0 ms/s; judged-window p99 399.5/422.5/669.6 ms; step p99 528.9 ms; generator wait p99 9.3 ms; sustainable
* step 5: offered 76.3/s per shard; achieved 76.3; runs 1526; backlog slope -0.1 runs/s; median growth -3.0 ms/s; judged-window p99 739.8/844.0/772.9 ms; step p99 812.2 ms; generator wait p99 11.4 ms; sustainable
* step 6: offered 95.4/s per shard; achieved 95.3; runs 1908; backlog slope 25.9 runs/s; median growth 354.9 ms/s; judged-window p99 4165.5/5793.0/7150.3 ms; step p99 7108.7 ms; generator wait p99 11.3 ms; **not sustainable**: the backlog grew 25.94 runs/s, over the 4.77 runs/s limit; the median latency grew 354.9 ms/s, over the 5 ms/s limit
* ladder: 7 step(s), stopped EngineBound; sustainable 76.3 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 172.7s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

