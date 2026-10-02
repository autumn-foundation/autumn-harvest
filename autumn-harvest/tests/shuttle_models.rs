//! Shuttle concurrency models (issue #1800).
//!
//! loom cannot model tokio primitives. These models use Shuttle, which runs
//! async tasks under a controlled scheduler. They drive the real production
//! code, not a copy of its algorithm:
//!
//! * [`TunedSlotRuntime`] resizes a real `Semaphore` while dispatch tasks hold
//!   permits.
//! * [`run_heartbeat_flusher`] drains a real `mpsc` channel while an activity
//!   sends heartbeats.
//!
//! Under `cfg(shuttle)`, `crate::shuttle_sync` swaps the tokio types for the
//! `shuttle-tokio` types. Each model runs under the random scheduler and under
//! the PCT scheduler.
//!
//! # This whole file is a no-op under a normal build
//!
//! The `#![cfg(shuttle)]` gate makes `cargo test` compile an empty crate. Run
//! the models with:
//!
//! ```text
//! RUSTFLAGS="--cfg shuttle" cargo test -p autumn-harvest --no-default-features --test shuttle_models --release
//! ```
//!
//! Shuttle samples schedules. It does not explore all of them, as loom does.
//! A pass is strong evidence, not a proof. See `docs/testing/shuttle.md`.

#![cfg(shuttle)]

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autumn_harvest::heartbeat::{FlushOutcome, HeartbeatSink, run_heartbeat_flusher};
use autumn_harvest::slot_tuner::TunedSlotRuntime;
use serde_json::{Value, json};
use shuttle::future::{block_on, yield_now};
use shuttle_tokio::sync::{Semaphore, mpsc};
use shuttle_tokio_util::sync::CancellationToken;

/// Iterations per random-scheduler model.
const RANDOM_ITERATIONS: usize = 2_000;
/// Iterations per PCT model.
const PCT_ITERATIONS: usize = 2_000;
/// PCT bug depth. Depth 3 finds bugs that need two forced preemptions.
const PCT_DEPTH: usize = 3;

// ---------------------------------------------------------------------------
// slot_tuner
// ---------------------------------------------------------------------------

/// Total permits on the dispatch semaphore.
const MAX_SLOTS: usize = 4;

/// Two dispatch tasks race a tuner that grows and shrinks the live target.
///
/// The model checks three properties:
///
/// 1. At every tuner step, `withheld + live_target == max_slots`.
/// 2. Dispatch never holds more permits than the live target.
/// 3. After the race, the tuner settles on its target, and a drain of all
///    `max_slots` permits completes after `release_all_withheld`.
///
/// Property 3 also covers the background shrink task. If that task kept a
/// permit, the drain would deadlock, and Shuttle reports a deadlock as a
/// failure.
fn slot_tuner_model() {
    block_on(async {
        let semaphore = Arc::new(Semaphore::new(MAX_SLOTS));
        let mut runtime = TunedSlotRuntime::new(Arc::clone(&semaphore), 2, 1, MAX_SLOTS);
        let live_target = runtime.live_target_cell();
        let in_flight = Arc::new(AtomicUsize::new(0));

        let dispatchers: Vec<_> = (0..2)
            .map(|_| {
                let semaphore = Arc::clone(&semaphore);
                let live_target = Arc::clone(&live_target);
                let in_flight = Arc::clone(&in_flight);
                shuttle_tokio::spawn(async move {
                    for _ in 0..2 {
                        let permit = Arc::clone(&semaphore)
                            .acquire_owned()
                            .await
                            .expect("the semaphore stays open");
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        assert!(
                            now <= live_target.load(Ordering::SeqCst),
                            "dispatch holds {now} permits, above the live target {}",
                            live_target.load(Ordering::SeqCst)
                        );
                        yield_now().await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        drop(permit);
                    }
                })
            })
            .collect();

        for desired in [MAX_SLOTS, 1, 3, 1] {
            runtime.resize_toward(desired).await;
            assert_eq!(
                runtime.withheld_permits() + runtime.live_target(),
                MAX_SLOTS,
                "withheld + live_target must equal max_slots after resize_toward({desired})"
            );
            assert!(
                semaphore.available_permits() + in_flight.load(Ordering::SeqCst)
                    <= runtime.live_target(),
                "free + in-flight permits exceed the live target after resize_toward({desired})"
            );
            yield_now().await;
        }

        for dispatcher in dispatchers {
            dispatcher.await.expect("a dispatch task panicked");
        }

        // The last shrink can still wait in the semaphore queue. Retry until
        // it lands. Shuttle fails the run if this loop never ends.
        let target = 1;
        while runtime.live_target() != target {
            runtime.resize_toward(target).await;
            yield_now().await;
        }
        assert_eq!(semaphore.available_permits(), target);
        assert_eq!(runtime.withheld_permits(), MAX_SLOTS - target);

        runtime.release_all_withheld();
        let all = Arc::clone(&semaphore)
            .acquire_many_owned(u32::try_from(MAX_SLOTS).expect("small"))
            .await
            .expect("the semaphore stays open");
        assert_eq!(all.num_permits(), MAX_SLOTS);
    });
}

#[test]
fn slot_tuner_conserves_permits_random() {
    shuttle::check_random(slot_tuner_model, RANDOM_ITERATIONS);
}

#[test]
fn slot_tuner_conserves_permits_pct() {
    shuttle::check_pct(slot_tuner_model, PCT_ITERATIONS, PCT_DEPTH);
}

// ---------------------------------------------------------------------------
// heartbeat
// ---------------------------------------------------------------------------

/// Number of heartbeats the activity sends.
const HEARTBEATS: u64 = 5;

/// A sink that records each flushed payload. It reports a lost lease on the
/// flush number `lose_lease_on`, if set.
struct RecordingSink {
    flushed: Arc<Mutex<Vec<Value>>>,
    lose_lease_on: Option<usize>,
}

impl HeartbeatSink for RecordingSink {
    fn flush(&mut self, payload: Value) -> impl Future<Output = FlushOutcome> + Send {
        let flushed = Arc::clone(&self.flushed);
        let lose_lease_on = self.lose_lease_on;
        async move {
            let mut flushed = flushed.lock().expect("not poisoned");
            flushed.push(payload);
            if lose_lease_on == Some(flushed.len()) {
                FlushOutcome::LeaseLost
            } else {
                FlushOutcome::Continue
            }
        }
    }
}

fn progress(value: &Value) -> u64 {
    value["progress"].as_u64().expect("a progress payload")
}

/// An activity sends numbered heartbeats into a small channel. The flusher
/// drains it in parallel.
///
/// The model checks three properties:
///
/// 1. Flushes keep send order. A later flush never carries an older payload.
/// 2. The newest heartbeat is flushed, even when the channel fills up.
/// 3. After cancellation, the flusher stops.
fn heartbeat_order_model() {
    block_on(async {
        // A capacity below `HEARTBEATS` makes the sender wait on the flusher.
        let (tx, rx) = mpsc::channel(2);
        let cancel = CancellationToken::new();
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let sink = RecordingSink {
            flushed: Arc::clone(&flushed),
            lose_lease_on: None,
        };
        let flusher = shuttle_tokio::spawn(run_heartbeat_flusher(
            rx,
            cancel.clone(),
            Duration::from_secs(1),
            sink,
        ));

        let sender = shuttle_tokio::spawn(async move {
            for i in 0..HEARTBEATS {
                tx.send(json!({ "progress": i }))
                    .await
                    .expect("the flusher is still running");
            }
        });
        sender.await.expect("the sender panicked");

        // Shuttle fails the run if this loop never ends.
        while flushed
            .lock()
            .expect("not poisoned")
            .last()
            .map(progress)
            != Some(HEARTBEATS - 1)
        {
            yield_now().await;
        }

        cancel.cancel();
        flusher.await.expect("the flusher panicked");

        let flushed = flushed.lock().expect("not poisoned");
        let order: Vec<u64> = flushed.iter().map(progress).collect();
        assert!(
            order.windows(2).all(|pair| pair[0] < pair[1]),
            "flushes must keep send order: {order:?}"
        );
    });
}

/// The first flush reports a lost lease. The flusher must stop, cancel the
/// activity token, and flush nothing more.
fn heartbeat_lease_lost_model() {
    block_on(async {
        let (tx, rx) = mpsc::channel(2);
        let cancel = CancellationToken::new();
        let flushed = Arc::new(Mutex::new(Vec::new()));
        let sink = RecordingSink {
            flushed: Arc::clone(&flushed),
            lose_lease_on: Some(1),
        };
        let flusher = shuttle_tokio::spawn(run_heartbeat_flusher(
            rx,
            cancel.clone(),
            Duration::from_secs(1),
            sink,
        ));

        let sender = shuttle_tokio::spawn(async move {
            for i in 0..HEARTBEATS {
                // The flusher drops the receiver when it stops.
                if tx.send(json!({ "progress": i })).await.is_err() {
                    break;
                }
            }
        });

        flusher.await.expect("the flusher panicked");
        sender.await.expect("the sender panicked");

        assert!(
            cancel.is_cancelled(),
            "a lost lease must cancel the activity token"
        );
        assert_eq!(
            flushed.lock().expect("not poisoned").len(),
            1,
            "the flusher must stop after the lost lease"
        );
    });
}

#[test]
fn heartbeat_flush_keeps_send_order_random() {
    shuttle::check_random(heartbeat_order_model, RANDOM_ITERATIONS);
}

#[test]
fn heartbeat_flush_keeps_send_order_pct() {
    shuttle::check_pct(heartbeat_order_model, PCT_ITERATIONS, PCT_DEPTH);
}

#[test]
fn heartbeat_lease_lost_stops_the_flusher_random() {
    shuttle::check_random(heartbeat_lease_lost_model, RANDOM_ITERATIONS);
}

#[test]
fn heartbeat_lease_lost_stops_the_flusher_pct() {
    shuttle::check_pct(heartbeat_lease_lost_model, PCT_ITERATIONS, PCT_DEPTH);
}
