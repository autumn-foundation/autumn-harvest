# `RedisTaskQueue::claim`: a duplicate `ensure_group` round trip on every poll

This note documents a syscall-count profiling pass over
`autumn_harvest_redis::RedisTaskQueue`'s `claim` -- the `TaskQueueAdapter`
entry point every worker embedding this crate calls in a tight poll loop.
Wall-clock timing is not admissible evidence on this (shared-vCPU) machine,
and this workload is dominated by network round trips to Redis rather than
CPU instructions, so it is not measured with `cargo bench` / criterion
timing, nor with `valgrind --tool=callgrind` (which would mostly attribute
cost to generic TCP-socket kernel-entry overhead rather than anything this
crate's own code controls). It is measured with `strace -f -c` instead --
a deterministic count of the socket syscalls (`writev`/`recvfrom`) `claim`
issues, per this agent's "strace -c / ltrace -- syscall counts, for I/O and
lock-related work" admissible-evidence category.

The baseline was committed separately, before this fix, in
`bce44dd` ("🔴 Bolt: claim_roundtrip_profile harness + baseline
(RedisTaskQueue::claim)"). This revision adds the "Change"/"Measurement"
sections below.

## Workload

`benches/claim_roundtrip_profile.rs` (new) drives a single worker polling a
single queue in steady state: `CLAIM_PROFILE_N` tasks (1,000 for the numbers
below) are enqueued via the public `enqueue` entry point, then `claim` is
called `CLAIM_PROFILE_N` times against that same queue -- the exact call
sequence a production worker's poll loop performs once its backlog is
non-empty. This is the real public entry point end-to-end (`enqueue` +
`claim`, not a synthetic microbenchmark of an internal helper picked in
isolation): every `claim` call pays for whatever round trips the production
code path actually issues, including the (empty, but still
Lua-script-invoking) delayed-task promotion sweep every real poll cycle
performs.

Requires a real, reachable Redis instance (`HARVEST_REDIS_TEST_URL`, the
same environment variable `tests/integration_redis.rs` reads) -- see the
harness's module doc for why it does not fall back to a `testcontainers`
instance the way that integration suite does (no Docker daemon in this
sandbox; a plain local `redis-server` stands in).

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest-redis --bench claim_roundtrip_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="claim_roundtrip_profile") | .executable')

redis-server --port 6390 --save '' --appendonly no &

HARVEST_REDIS_TEST_URL=redis://127.0.0.1:6390 CLAIM_PROFILE_N=1000 \
  strace -f -c -o strace.txt "$BIN"
```

Baseline (unmodified `HEAD`), `strace -f -c` totals for `N=1,000`:

```
 86.49    5.477930         222     24672       130 futex
  7.05    0.446437          41     10720           epoll_wait
  3.26    0.206638          34      6001           writev
  1.74    0.110136          25      4370           write
  1.10    0.069547          11      6100           recvfrom
```

`writev` and `recvfrom` are the socket-protocol syscalls (one write + one
read per Redis command round trip); they are exactly reproducible run to
run on the identical binary (confirmed: 6,001 / 6,100 on two independent
runs at `N=1,000`, byte-for-byte identical both times) -- unlike wall-clock
timing, which this repo's Bolt charter treats as inadmissible on this
shared-vCPU machine. `write`/`futex`/`epoll_wait` vary slightly run to run
(tokio scheduler jitter, one `write` for the harness's own stdout summary
line); they are not part of this finding's claim.

## Hypothesis

`RedisTaskQueue::claim_inner` (`autumn-harvest-redis/src/redis_queue.rs`)
begins each queue's iteration with:

```rust
self.ensure_group(queue).await?;
let _ = self.promote_due(queue).await?;
```

But `promote_due` itself begins with an *identical* call:

```rust
pub async fn promote_due(&self, queue_name: &str) -> RedisAdapterResult<usize> {
    self.ensure_group(queue_name).await?;
    // ... invoke the promotion Lua script ...
}
```

`ensure_group` issues one full Redis round trip (`XGROUP CREATE ... MKSTREAM`,
tolerating `BUSYGROUP`) with no caching -- so every single `claim` call
issues it **twice** against the exact same key: once directly, once one line
later inside `promote_due`. `ensure_group` is idempotent (`BUSYGROUP` is
explicitly tolerated as success), so calling it once versus twice before the
same `xread_options` call cannot change `claim`'s result -- the second call
is pure redundant network I/O, not defense-in-depth.

For an `n`-task `CLAIM_PROFILE_N` run this contributes exactly one extra
`ensure_group` round trip (one `writev` + one `recvfrom`) per `claim` call --
confirmed precisely by the arithmetic below, not merely estimated: each of
the `n` enqueues issues `ensure_group` + `XADD` (2 round trips), and each of
the `n` claims issues `ensure_group` (outer, the redundant one) +
`ensure_group` (inner, via `promote_due`) + the promotion script +
`xread_options` (4 round trips) = `2n + 4n = 6n` round trips total; at
`n = 1,000` that is exactly 6,000, matching the observed 6,001 `writev` (the
`+1` is the initial handshake `ConnectionManager::new` performs once at
connect time, outside the measured loop). Removing the redundant outer call
should therefore drop both `writev` and `recvfrom` by exactly `n` -- a
prediction sharp enough to be falsified by the after-measurement, not just a
directional guess.

## Change

`autumn-harvest-redis/src/redis_queue.rs`, `claim_inner`: drop the outer,
now-redundant `self.ensure_group(queue).await?` call. `promote_due` already
guarantees the group exists (it is called unconditionally, immediately
after, on the very next line) before `claim_inner` ever reaches the
`xread_options` call that actually needs the group to exist. `promote_due`
itself -- and its own doc comment's guarantee for callers who invoke it
directly, outside `claim` -- is untouched.

```diff
-            // Make sure the consumer group exists *and* any due delayed tasks
-            // are on the stream before we ask for them.
-            self.ensure_group(queue).await?;
-            let _ = self.promote_due(queue).await?;
+            // Make sure any due delayed tasks are on the stream before we ask
+            // for them. `promote_due` itself calls `ensure_group` first (it
+            // must, so the group exists before its Lua script's XADDs land),
+            // so a second `ensure_group` call here would be a redundant
+            // round trip to Redis on every single claim attempt -- the
+            // consumer group is idempotently ensured exactly once below.
+            let _ = self.promote_due(queue).await?;
```

No behavior change: `claim_inner` still guarantees the group exists (via
`promote_due`'s own call) before reading, exactly as before -- it just no
longer pays for the guarantee twice. `promote_due`'s own public contract
(callers who invoke it directly, outside `claim`, still get the same
ensure-then-promote guarantee) is unaffected: only its caller in
`claim_inner` changed.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by the one-line diff above, same `strace -f -c`
invocation, same local `redis-server`, same session, `N=1,000`.

| | `writev` | `recvfrom` |
|---|---:|---:|
| Before | 6,001 | 6,100 |
| After  | 5,001 | 5,096 |
| **Reduction** | **1,000 (16.66%)** | **1,004 (16.46%)** |

Exactly `n` (1,000) fewer `writev` calls -- the predicted one-round-trip-
per-claim removal, to the syscall (`recvfrom`'s reduction is 1,004, a few
reply-fragmentation-dependent reads off the round-trip-count prediction, not
a discrepancy in the mechanism). Both clear this agent's "measurable
reduction in syscall count" impact floor by a wide margin; reproduced
identically on two independent runs of each binary (6,001/6,100 twice for
"before", 5,001/5,096 twice for "after").

### Correctness

* `cargo fmt -p autumn-harvest-redis -- --check` -- clean.
* `cargo clippy -p autumn-harvest-redis --all-targets` -- clean for every
  line this change touches; a pre-existing, unrelated `clippy::pedantic`/
  `clippy::nursery` warning set inside `autumn-harvest`'s own `context.rs`
  (a dependency, not touched by this change) surfaces only when `-D
  warnings` is added on the command line, and reproduces identically on an
  unmodified checkout with `cargo clippy -p autumn-harvest --lib -- -D
  warnings` -- confirmed via `git stash` before writing this change, so it
  is an environment/toolchain condition on this sandbox (the workspace's
  `clippy::pedantic`/`clippy::nursery` are `warn`-level; `-D warnings`
  escalates all warnings, including this pre-existing dependency one, to a
  hard error), not something this PR introduces or could fix by touching
  `redis_queue.rs`. The warning count (10) is identical before and after
  this change.
* `cargo test -p autumn-harvest-redis --lib` -- **13 passed, 0 failed**,
  both before and after this change.
* `HARVEST_REDIS_TEST_URL=redis://127.0.0.1:6390 cargo test -p
  autumn-harvest-redis --test integration_redis` -- **6 passed, 0 failed**,
  both before and after this change, against a local `redis-server` (not a
  `testcontainers` Docker instance -- this sandbox has no Docker daemon;
  `try_start_redis`'s `HARVEST_REDIS_TEST_URL` path already exists in the
  test file for exactly this case).

No test's expected value needed to change: `claim`'s behavior (which task,
if any, gets returned) is identical -- only the number of times the
already-idempotent `ensure_group` call is issued changed.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest-redis --bench claim_roundtrip_profile \
  --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="claim_roundtrip_profile") | .executable')

redis-server --port 6390 --save '' --appendonly no &
sleep 1

redis-cli -p 6390 flushall
HARVEST_REDIS_TEST_URL=redis://127.0.0.1:6390 CLAIM_PROFILE_N=1000 \
  strace -f -c -o strace.txt "$BIN"
grep -E 'writev|recvfrom' strace.txt
```

`CLAIM_PROFILE_N` (default 300) controls the enqueue/claim count.
