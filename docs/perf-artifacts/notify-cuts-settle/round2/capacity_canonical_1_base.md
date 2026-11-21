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
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 482.52 | yes |
| `capacity_canonical` | 1 | `steps` | 7.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth 0.7 ms/s; judged-window p99 295.2/294.5/280.7 ms; step p99 297.3 ms; generator wait p99 2.4 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope -0.0 runs/s; median growth -0.9 ms/s; judged-window p99 285.1/280.7/279.8 ms; step p99 285.1 ms; generator wait p99 2.4 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope -0.0 runs/s; median growth -0.9 ms/s; judged-window p99 260.4/279.6/304.3 ms; step p99 285.1 ms; generator wait p99 3.4 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.1 runs/s; median growth -2.4 ms/s; judged-window p99 595.6/621.1/283.3 ms; step p99 548.9 ms; generator wait p99 5.2 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope 0.1 runs/s; median growth 1.5 ms/s; judged-window p99 269.6/329.6/302.2 ms; step p99 311.4 ms; generator wait p99 7.7 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 61.0; runs 1220; backlog slope -0.2 runs/s; median growth -2.1 ms/s; judged-window p99 335.2/351.1/322.8 ms; step p99 367.4 ms; generator wait p99 10.0 ms; sustainable
* step 5 (noise only, retried): offered 76.3/s per shard; achieved 76.2; runs 1526; backlog slope 2.6 runs/s; median growth 47.3 ms/s; judged-window p99 1261.8/1095.6/1002.8 ms; step p99 1205.3 ms; generator wait p99 11.8 ms; **not sustainable**: the median latency grew 47.3 ms/s, over the 5 ms/s limit
* step 5: offered 76.3/s per shard; achieved 76.3; runs 1526; backlog slope -0.5 runs/s; median growth -7.4 ms/s; judged-window p99 505.2/486.3/415.1 ms; step p99 482.5 ms; generator wait p99 10.9 ms; sustainable
* step 6: offered 95.4/s per shard; achieved 95.4; runs 1908; backlog slope 14.4 runs/s; median growth 173.2 ms/s; judged-window p99 2238.3/2941.7/3746.0 ms; step p99 3707.2 ms; generator wait p99 9.6 ms; **not sustainable**: the backlog grew 14.43 runs/s, over the 4.77 runs/s limit; the median latency grew 173.2 ms/s, over the 5 ms/s limit
* ladder: 7 step(s), stopped EngineBound; sustainable 76.3 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 189.5s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

