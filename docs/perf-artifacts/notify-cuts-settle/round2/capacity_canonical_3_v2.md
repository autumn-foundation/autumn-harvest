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
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 376.98 | yes |
| `capacity_canonical` | 1 | `steps` | 6.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth -2.2 ms/s; judged-window p99 295.8/282.1/219.8 ms; step p99 293.5 ms; generator wait p99 3.4 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth -0.4 ms/s; judged-window p99 276.6/272.8/247.1 ms; step p99 274.8 ms; generator wait p99 3.3 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope -0.0 runs/s; median growth -1.5 ms/s; judged-window p99 241.1/222.7/215.6 ms; step p99 231.1 ms; generator wait p99 3.3 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.1 runs/s; median growth -3.1 ms/s; judged-window p99 444.0/246.7/265.1 ms; step p99 399.5 ms; generator wait p99 5.4 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope 0.1 runs/s; median growth 2.6 ms/s; judged-window p99 240.6/257.9/248.8 ms; step p99 248.5 ms; generator wait p99 4.8 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope 0.3 runs/s; median growth 3.2 ms/s; judged-window p99 366.7/371.2/399.2 ms; step p99 377.0 ms; generator wait p99 9.0 ms; sustainable
* step 5: offered 76.3/s per shard; achieved 76.3; runs 1526; backlog slope 8.6 runs/s; median growth 91.0 ms/s; judged-window p99 1727.5/2076.8/2781.5 ms; step p99 2717.3 ms; generator wait p99 11.7 ms; **not sustainable**: the backlog grew 8.62 runs/s, over the 3.81 runs/s limit; the median latency grew 91.0 ms/s, over the 5 ms/s limit
* ladder: 6 step(s), stopped EngineBound; sustainable 61.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 146.6s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

