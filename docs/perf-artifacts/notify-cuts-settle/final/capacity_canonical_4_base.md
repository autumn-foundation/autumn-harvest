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
| `capacity_canonical` | 1 | `p99_ms_at_sustainable` | 424.84 | yes |
| `capacity_canonical` | 1 | `steps` | 6.00 | yes |
| `capacity_canonical` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_canonical` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_canonical` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 25.0 starts/s per shard
* warmup (discarded): offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth -1.4 ms/s; judged-window p99 281.2/307.9/280.7 ms; step p99 299.9 ms; generator wait p99 2.5 ms; sustainable
* step 0: offered 25.0/s per shard; achieved 25.0; runs 500; backlog slope 0.0 runs/s; median growth 0.9 ms/s; judged-window p99 278.7/293.5/270.5 ms; step p99 282.3 ms; generator wait p99 2.8 ms; sustainable
* step 1: offered 31.3/s per shard; achieved 31.3; runs 626; backlog slope 0.0 runs/s; median growth 1.6 ms/s; judged-window p99 285.3/296.5/310.0 ms; step p99 288.9 ms; generator wait p99 4.6 ms; sustainable
* step 2: offered 39.1/s per shard; achieved 39.1; runs 782; backlog slope -0.2 runs/s; median growth -4.6 ms/s; judged-window p99 622.8/306.1/344.7 ms; step p99 513.5 ms; generator wait p99 6.0 ms; sustainable
* step 3: offered 48.8/s per shard; achieved 48.8; runs 976; backlog slope -0.1 runs/s; median growth -1.9 ms/s; judged-window p99 310.2/336.8/339.0 ms; step p99 336.9 ms; generator wait p99 7.4 ms; sustainable
* step 4: offered 61.0/s per shard; achieved 60.9; runs 1220; backlog slope -0.0 runs/s; median growth -1.3 ms/s; judged-window p99 424.8/444.3/401.3 ms; step p99 424.8 ms; generator wait p99 8.6 ms; sustainable
* step 5: offered 76.3/s per shard; achieved 76.3; runs 1526; backlog slope 18.6 runs/s; median growth 225.8 ms/s; judged-window p99 3901.4/5155.5/5751.5 ms; step p99 5701.0 ms; generator wait p99 12.5 ms; **not sustainable**: the backlog grew 18.62 runs/s, over the 3.81 runs/s limit; the median latency grew 225.8 ms/s, over the 5 ms/s limit
* ladder: 6 step(s), stopped EngineBound; sustainable 61.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 149.6s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

