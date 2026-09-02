# Apparatus for assay #1: redis-adapter throughput ceiling

Non-production. Not a workspace member. See
[`../../0001-redis-adapter-throughput-ceiling.md`](../../0001-redis-adapter-throughput-ceiling.md)
for the question, pre-registration, and verdict this code was built to
answer.

```bash
redis-server --daemonize yes --port 6379 --save "" --appendonly no
BENCH_SECS=10 cargo run --release --manifest-path docs/assays/apparatus/0001-redis-throughput-bench/Cargo.toml
```
