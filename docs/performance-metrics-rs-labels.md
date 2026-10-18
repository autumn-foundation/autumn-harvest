# Per-attempt string allocations: `metrics-rs` labels and circuit-breaker keys

This page measures two string allocations on the per-activity-attempt hot
path, and the change that removes each one:

1. `MetricsRsRecorder` copied every label value into a new `String` on every
   sample.
2. `CircuitBreakerRegistry` allocated a `String` key for its state map on
   every policy-guarded call.

## 🎯 Workload

**Labels.** A worker records four `metrics`-crate samples for every activity
attempt: `harvest.queue.dispatched`, `harvest.queue.schedule_to_start`,
`harvest.activity.attempts` and `harvest.activity.duration`. Together they
carry eight label values: activity name, queue and build id. These values
come from a small fixed set: the registered activities, the worker's queues
and its build. ADR-0001 §7 keeps them low-cardinality.

`benches/metrics_rs_labels_profile.rs` drives exactly those four calls for
10,000 simulated attempts, cycling over four activity names. The recorder it
installs returns no-op handles. The measured cost is therefore the adapter's
own work, not an exporter's registry lookup.

**Circuit breaker.** For an activity with a circuit-breaker policy, the
worker calls the registry four times per attempt: `on_dispatch`,
`begin_claim`, `on_claim_result` and `end_claim`.
`benches/circuit_breaker_dispatch_profile.rs` drives that sequence for
10,000 simulated attempts over four tracked activities.

## 📈 Profile

**Labels.** Each sample built its labels with `value.to_owned()`. Per
attempt, that is 8 label `String`s plus the 4 label `Vec`s that the
`metrics` macros build, so 12 allocations. `malloc`, `free` and `_int_free`
together took 42.3% of the harness's instructions.

**Circuit breaker.** Three of the four calls ran
`states.entry(activity_name.to_string()).or_default()` under the registry
mutex. That allocates and frees a key on every call, even though the
tracked set is fixed when the registry is built. `end_claim` already used
`get_mut`.

## 💡 Hypothesis

**Labels.** Interning the label values as `&'static str` removes the 8
`String` allocations. A borrowed value is a `metrics` `SharedString` with no
copy. The 4 label `Vec`s belong to the `metrics` macros and stay.

**Circuit breaker.** Seeding one state per policy in `new` lets every
policy-guarded path use `get_mut`. That removes all 3 key allocations.

## 🔧 Change

**Labels.** `label_value` in `metrics_rs_adapter.rs` replaces `.to_owned()`
in the per-task metric families, from `record_workflow_started` through
`record_queue_oldest_pending_age`. A hit costs a lookup in a thread-local
set. So a hit takes no lock and touches no shared cache line. A miss takes a
shared lock, leaks one copy of the value and caches it in the thread. The
shared set holds at most 4,096 values. Past that cap, a new value is copied
as before. The cap bounds the leak if a caller breaks the low-cardinality
rule. When the set fills, the interner publishes a frozen copy of it. A
later miss reads that copy without the lock. It still resolves every value
that was interned, so a thread that starts after the cap still caches the
common names.

The first cut hashed both sets with the standard library's SipHash. That
cost more than the allocations it removed; see the measurement below. The
sets now hash with `foldhash`, which is already in the lockfile through
`hashbrown`. Its per-process seed still resists hash flooding.

`record_activity_failed` also writes its `non_retryable` label as a static
`"true"` or `"false"` instead of `bool::to_string`.

**Circuit breaker.** `CircuitBreakerRegistry::new` seeds one
`BreakerState` per policy. No path removes a state, so a tracked name always
has one. The policy-guarded paths look it up with `get_mut`. The operator
paths `force_open`, `force_close` and `list` keep `entry`. They run rarely,
and they keep their behaviour for a name with no policy.

## 📊 Measurement

### Allocations (`valgrind --tool=dhat`)

| Harness | Before | After | Change |
| --- | ---: | ---: | ---: |
| labels: blocks | 120,010 | 40,022 | -66.65% |
| labels: bytes | 5,623,644 | 4,804,216 | -14.57% |
| circuit breaker: blocks | 30,026 | 29 | -99.90% |
| circuit breaker: bytes | 337,496 | 6,944 | -97.94% |

The 4 blocks per attempt left in the labels harness are the `metrics`
macros' label `Vec`s. The 29 blocks left in the circuit-breaker harness are
one-time setup, such as the policy map and the seeded states.

### Instructions (Ir), `valgrind --tool=callgrind --branch-sim=no --cache-sim=no`

| Harness | Before | After | Change | Per attempt |
| --- | ---: | ---: | ---: | ---: |
| labels, SipHash sets (rejected) | 35,784,350 | 46,517,829 | +29.99% | 3,578 → 4,652 |
| labels, `foldhash` sets (shipped) | 35,784,350 | 31,859,333 | -10.97% | 3,578 → 3,186 |
| circuit breaker | 41,999,490 | 35,108,323 | -16.41% | 4,200 → 3,511 |

The SipHash row is why the interner hashes with `foldhash`. SipHash took
about 196 instructions per lookup on these short keys. One `malloc` and
`free` pair took less than that. With `foldhash`, `label_value` costs about
100 instructions per call, hash and thread-local access included.

Both harnesses are single-threaded, so the counts leave out allocator and
cache-line contention between worker threads. The thread-local hit path
avoids both, so a multi-threaded worker gains at least as much.

### Correctness

- `a_label_value_is_interned_once_across_calls_and_threads` checks that
  repeat calls, and calls on another thread, resolve to one shared copy.
- `a_full_interner_refuses_only_new_values` checks the cap. A full interner
  still resolves every value it holds, also on a thread that first meets the
  value after the cap.
- The existing `CapturingRecorder` tests in `metrics_rs_adapter.rs` check
  the exact label sets each family registers.
- The existing `circuit_breaker` unit tests and the loom model in
  `tests/loom_models.rs` cover the breaker's state machine.

## 🔬 Reproduce

```text
cargo bench -p autumn-harvest --features metrics-rs \
  --bench metrics_rs_labels_profile --bench circuit_breaker_dispatch_profile \
  --no-run --message-format=json \
  | jq -r 'select(.executable != null) | .executable'

# Allocations:
valgrind --tool=dhat --dhat-out-file=dhat.out <path>
# Instructions:
valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
  --callgrind-out-file=callgrind.out <path>
```

Build the "before" binaries from the commit before the change. Both
harnesses read a rep count from the environment:
`METRICS_LABELS_PROFILE_REPS` and `CIRCUIT_PROFILE_REPS`, each 10,000 by
default.
