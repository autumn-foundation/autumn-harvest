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
| `capacity_noop` | 1 | `sustainable_per_sec` | n/a | **no** |
| `capacity_noop` | 1 | `sustainable_per_shard_per_sec` | n/a | **no** |
| `capacity_noop` | 1 | `p99_ms_at_sustainable` | n/a | **no** |
| `capacity_noop` | 1 | `steps` | 9.00 | **no** |
| `capacity_noop` | 1 | `database_hosts` | 1.00 | **no** |
| `capacity_noop` | 1 | `worker_hosts` | 1.00 | **no** |


## Notes

### `capacity_noop` at 1 shard(s)

* topology: independent-servers
* host layout: 1 shard(s) on 1 database host(s) (localhost); a database shares the bench host with the workers and the generator
* worker layout: every worker process runs on the bench host
* open loop: 4 worker process(es) per shard, a 16-connection generator pool per shard, 20s steps from 50.0 starts/s per shard
* warmup (discarded): offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 38.6/14.3/11.6 ms; step p99 31.7 ms; generator wait p99 2.1 ms; sustainable
* step 0: offered 50.0/s per shard; achieved 50.0; runs 1000; backlog slope 0.0 runs/s; median growth -0.1 ms/s; judged-window p99 14.4/13.2/12.3 ms; step p99 15.9 ms; generator wait p99 1.2 ms; sustainable
* step 1: offered 62.5/s per shard; achieved 62.5; runs 1250; backlog slope -0.0 runs/s; median growth -0.0 ms/s; judged-window p99 13.6/14.1/15.1 ms; step p99 16.6 ms; generator wait p99 1.3 ms; sustainable
* step 2: offered 78.1/s per shard; achieved 78.1; runs 1562; backlog slope -0.0 runs/s; median growth -0.1 ms/s; judged-window p99 50.0/11.6/11.7 ms; step p99 24.2 ms; generator wait p99 2.0 ms; sustainable
* step 3: offered 97.7/s per shard; achieved 97.7; runs 1954; backlog slope -0.0 runs/s; median growth 0.1 ms/s; judged-window p99 12.4/13.7/12.8 ms; step p99 14.5 ms; generator wait p99 2.1 ms; sustainable
* step 4: offered 122.1/s per shard; achieved 122.1; runs 2442; backlog slope 0.0 runs/s; median growth 0.2 ms/s; judged-window p99 18.3/15.5/25.7 ms; step p99 24.5 ms; generator wait p99 2.9 ms; sustainable
* step 5: offered 152.6/s per shard; achieved 152.6; runs 3052; backlog slope -0.2 runs/s; median growth -0.2 ms/s; judged-window p99 131.2/22.2/24.1 ms; step p99 109.9 ms; generator wait p99 3.5 ms; sustainable
* step 6: offered 190.7/s per shard; achieved 190.7; runs 3814; backlog slope 0.0 runs/s; median growth 0.2 ms/s; judged-window p99 37.5/31.3/39.8 ms; step p99 39.9 ms; generator wait p99 3.9 ms; sustainable
* step 7: offered 238.4/s per shard; achieved 238.4; runs 4768; backlog slope -0.1 runs/s; median growth -0.2 ms/s; judged-window p99 48.5/45.8/41.3 ms; step p99 51.0 ms; generator wait p99 4.5 ms; sustainable
* step 8: offered 298.0/s per shard; achieved 297.7; runs 5960; backlog slope -9.1 runs/s; median growth -25.5 ms/s; judged-window p99 738.9/82.1/87.9 ms; step p99 715.8 ms; generator wait p99 107.1 ms; **generator-bound**: the generator wait p99 was 107.1 ms, over the 100 ms limit
* ladder: 9 step(s), stopped GeneratorBound; sustainable 238.4 starts/s per shard
* engine latency runs from admission to `completed_at`, both on the bench host clock; the generator wait, from schedule to admission, is judged separately
* wall clock: 205.2s
* **not published**: the generator could not offer the next step, so 238.4 starts/s is a generator limit, not the engine's


**Unsound scenarios (not publishable): capacity_noop@1**

