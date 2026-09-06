//! Syscall-count profiling harness for [`RedisTaskQueue`]'s `claim` hot path
//! -- the poll loop every worker embedding this crate runs continuously
//! (`TaskQueueAdapter::claim`, the public entry point).
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, and this
//! workload is dominated by network round trips to Redis rather than CPU
//! instructions, so this binary is not measured with `cargo bench` /
//! criterion timing, nor with `valgrind --tool=callgrind` (which would mostly
//! count the same TCP-socket syscalls' kernel-entry overhead rather than
//! anything this crate's own code controls). It is driven directly under
//! `strace -f -c` instead -- a deterministic count of the socket syscalls
//! (`writev`/`write`/`recvfrom`) `claim` issues, per this agent's charter's
//! "strace -c / ltrace -- syscall counts, for I/O and lock-related work"
//! admissible-evidence category.
//!
//! `harness = false`, own `main()` -- same shape as
//! `autumn-harvest/benches/replay_profile.rs` and its siblings -- so the
//! compiled artifact is a plain executable a profiler can be pointed at
//! directly, with no criterion wall-clock loop diluting the measured work.
//!
//! # Workload
//!
//! A single worker polling a single queue in steady state: `CLAIM_PROFILE_N`
//! tasks (default 300) are enqueued via the public `enqueue` entry point,
//! then `claim` is called `CLAIM_PROFILE_N` times against that same queue --
//! the exact call sequence a production worker's poll loop performs once its
//! backlog is non-empty. A fresh, uniquely-prefixed `RedisTaskQueueConfig`
//! avoids colliding with any other data already on the target Redis instance,
//! matching the isolation convention `tests/integration_redis.rs` already
//! uses for this crate's own integration suite.
//!
//! This exercises the real `claim` -> `promote_due` -> `ensure_group` call
//! chain end-to-end (not a synthetic microbenchmark of a single function
//! picked in isolation): every `claim` call in this loop pays for whatever
//! round trips the production code path actually issues, including the
//! (empty, but still Lua-script-invoking) delayed-task promotion sweep every
//! real poll cycle performs.
//!
//! Requires a real, reachable Redis instance -- set `HARVEST_REDIS_TEST_URL`
//! (the same environment variable `tests/integration_redis.rs` reads) to its
//! connection URL before running. Unlike that test suite, this harness does
//! NOT fall back to a `testcontainers`-managed instance: a profiling harness
//! needs a specific, known-quiescent instance for its syscall counts to be
//! reproducible, and spinning up a fresh container on every invocation would
//! add its own (irrelevant, Docker-daemon-dependent) syscall noise to a trace
//! meant to isolate this crate's own Redis protocol usage.

use autumn_harvest_redis::{
    EnqueueParams, RedisTaskQueue, RedisTaskQueueConfig, TaskQueueAdapter, TaskType,
};

/// Reads `key` as a `usize`, using `default` only when the variable is
/// genuinely *absent*. A present-but-malformed value is a configuration
/// error, not silently substituted for the default.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

#[tokio::main]
async fn main() {
    let url = std::env::var("HARVEST_REDIS_TEST_URL").unwrap_or_else(|_| {
        panic!(
            "claim_roundtrip_profile requires a real Redis instance -- set \
             HARVEST_REDIS_TEST_URL (e.g. redis://127.0.0.1:6379) before running. \
             This profiling harness does not fall back to a testcontainers-managed \
             instance; see this file's module doc for why."
        )
    });
    let n = env_usize("CLAIM_PROFILE_N", 300);
    assert!(n > 0, "CLAIM_PROFILE_N must be at least 1, got 0");

    // Unique prefix per run so this harness's keys never collide with
    // anything else already on the target instance -- mirrors
    // `tests/integration_redis.rs::try_start_redis`'s isolation convention.
    let prefix = format!("bolt_claim_roundtrip_{}", uuid::Uuid::new_v4().simple());
    let config = RedisTaskQueueConfig {
        key_prefix: prefix,
        ..RedisTaskQueueConfig::default()
    };
    let queue = RedisTaskQueue::connect(&url, config)
        .await
        .unwrap_or_else(|e| panic!("failed to connect to {url:?}: {e}"));

    let queue_name = "claim_roundtrip_profile";
    for i in 0..n {
        queue
            .enqueue(EnqueueParams::new(
                queue_name,
                TaskType::Activity,
                serde_json::json!({"seq": i, "payload": "order line item"}),
            ))
            .await
            .unwrap_or_else(|e| panic!("enqueue #{i} failed: {e}"));
    }

    let queues = vec![queue_name.to_string()];
    let mut claimed = 0usize;
    for _ in 0..n {
        // The exact production entry point (`TaskQueueAdapter::claim`), not
        // an internal helper -- this is what a worker's poll loop calls.
        if queue
            .claim(&queues, "bolt-profile-worker")
            .await
            .unwrap_or_else(|e| panic!("claim failed: {e}"))
            .is_some()
        {
            claimed += 1;
        }
    }

    // Self-check: every enqueued task must have been claimable, or this
    // harness silently profiled a different (smaller) workload than
    // CLAIM_PROFILE_N advertises.
    assert_eq!(
        claimed, n,
        "expected all {n} enqueued tasks to be claimed, got {claimed} -- \
         either claim's consumer-group/promotion logic regressed, or this \
         harness's isolation prefix collided with pre-existing data"
    );

    println!("claim_roundtrip_profile: n={n} claimed={claimed}");
}
