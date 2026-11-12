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
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | 157.27 | yes |
| `capacity_noop` | 1 | `steps` | 10.00 | yes |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | yes |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | yes |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 66.8/67.3/70.9 ms; step p99 67.2 ms; generator wait p99 2.1 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope -0.0 runs/s; median growth -0.4 ms/s; judged-window p99 68.7/61.3/66.0 ms; step p99 66.6 ms; generator wait p99 2.9 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope 0.0 runs/s; median growth 0.3 ms/s; judged-window p99 64.9/67.2/62.2 ms; step p99 65.0 ms; generator wait p99 2.1 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope 0.0 runs/s; median growth 0.0 ms/s; judged-window p99 63.0/64.3/60.9 ms; step p99 63.0 ms; generator wait p99 2.6 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 53.5/56.0/60.6 ms; step p99 57.6 ms; generator wait p99 2.5 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 46.5/49.0/47.2 ms; step p99 48.7 ms; generator wait p99 2.4 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 22.8/31.9/24.8 ms; step p99 129.6 ms; generator wait p99 3.1 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope 0.0 runs/s; median growth 0.1 ms/s; judged-window p99 29.6/27.1/30.5 ms; step p99 30.4 ms; generator wait p99 3.1 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.3; runs 4768; backlog slope 0.5 runs/s; median growth 0.4 ms/s; judged-window p99 57.4/59.1/181.6 ms; step p99 161.7 ms; generator wait p99 5.5 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.6; runs 5960; backlog slope 0.5 runs/s; median growth 2.2 ms/s; judged-window p99 93.1/90.2/145.5 ms; step p99 157.3 ms; generator wait p99 14.9 ms; sustainable
* step 9 (noise only, retried): offered 372.5/s per shard; achieved 371.5; runs 7450; backlog slope 4.8 runs/s; median growth 16.1 ms/s; judged-window p99 114.6/225.4/332.0 ms; step p99 318.0 ms; generator wait p99 145.5 ms; **not sustainable**: the median latency grew 16.1 ms/s, over the 5 ms/s limit; the p99 grew from 114.6 ms to 332.0 ms, over the 329.2 ms limit; the generator wait p99 was 145.5 ms, over the 100 ms limit
* step 9: offered 372.5/s per shard; achieved 371.7; runs 7450; backlog slope 11.2 runs/s; median growth 0.7 ms/s; judged-window p99 253.7/142.9/1407.9 ms; step p99 1386.7 ms; generator wait p99 80.3 ms; **not sustainable**: the p99 grew from 253.7 ms to 1407.9 ms, over the 607.5 ms limit
* ladder: 10 step(s), stopped EngineBound; sustainable 298.0 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 248.2s
* sound: every published number on this row rests on a measured sample


Every scenario reported sound.

