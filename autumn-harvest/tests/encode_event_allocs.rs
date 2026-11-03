//! Allocation budget for `PayloadCodecs::encode_event_owned` (issue #2499).
//!
//! The owned encode moves each payload tree into the stored `Value`. So its
//! allocation count must not grow with the payload size. As the payload
//! grows, the borrowed encode adds no more allocations than a clone adds.
//!
//! A clone and the old serde copy make the same number of allocations. The
//! gain of the clone is in instructions, so callgrind measures it. See
//! `docs/performance-encode-event-payload-move.md`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_codec::PayloadCodecs;
use autumn_harvest::types::ActivityExecId;
use serde_json::{Value, json};

struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

// SAFETY: every call forwards unchanged to `System`.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|n| n.set(n.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs_during<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let before = ALLOCS.with(Cell::get);
    let out = f();
    (out, ALLOCS.with(Cell::get) - before)
}

/// A payload with `n` line items, so allocation counts scale with payload size.
fn document(n: usize) -> Value {
    let items: Vec<Value> = (0..n)
        .map(|i| json!({"sku": format!("sku-{i}"), "qty": i, "tags": ["a", "b"]}))
        .collect();
    json!({"customer": {"id": 7, "name": "Ada"}, "items": items})
}

fn completed(output: Value) -> WorkflowEvent {
    WorkflowEvent::ActivityCompleted {
        activity_id: ActivityExecId::new(),
        output,
    }
}

/// A fixed time, so each encode writes a timestamp of the same length.
const fn fixed_time() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(1_800_000_000, 0).expect("valid time")
}

const fn started(input: Value, carryover: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input,
        timestamp: fixed_time(),
        last_completion_result: Some(carryover),
        last_error: None,
        scheduled_time: None,
    }
}

/// The owned encode moves each payload tree (issue #2499).
///
/// A copied tree costs allocations in proportion to its size. So the count
/// must not change with the payload size.
#[test]
fn owned_encode_allocations_do_not_grow_with_the_payload() {
    let codecs = PayloadCodecs::default();
    // Warm up, so a one-time setup cost does not land in the first count.
    drop(codecs.encode_event_owned(completed(document(1))));
    let encode = |event: WorkflowEvent| {
        let (stored, allocs) = allocs_during(|| codecs.encode_event_owned(event).expect("encode"));
        drop(stored);
        allocs
    };
    let (small, large) = (
        encode(completed(document(1))),
        encode(completed(document(50))),
    );
    assert_eq!(
        small, large,
        "owned encode made {small} allocations at 1 item and {large} at 50"
    );
    let (small, large) = (
        encode(started(document(1), document(1))),
        encode(started(document(50), document(50))),
    );
    assert_eq!(
        small, large,
        "owned encode made {small} allocations at 1 item and {large} at 50"
    );
}

/// The borrowed encode copies each payload once, as a clone does.
#[test]
fn borrowed_encode_copies_no_more_than_a_clone() {
    let codecs = PayloadCodecs::default();
    drop(codecs.encode_event(&completed(document(1))));
    let cost = |event: &WorkflowEvent| {
        let (stored, encode) = allocs_during(|| codecs.encode_event(event).expect("encode"));
        drop(stored);
        let (copy, clone) = allocs_during(|| event.clone());
        drop(copy);
        (encode, clone)
    };
    let (small, large) = (completed(document(1)), completed(document(50)));
    let ((small_encode, small_clone), (large_encode, large_clone)) = (cost(&small), cost(&large));
    assert!(
        large_encode - small_encode <= large_clone - small_clone,
        "encode grew by {} allocations; a clone grew by {}",
        large_encode - small_encode,
        large_clone - small_clone
    );
}

/// The owned encode stores what the borrowed encode stores.
#[test]
fn owned_and_borrowed_encode_store_the_same_value() {
    let codecs = PayloadCodecs::default();
    let event = started(document(3), document(2));
    let borrowed = codecs.encode_event(&event).expect("encode");
    let owned = codecs.encode_event_owned(event).expect("encode");
    assert_eq!(owned, borrowed);
}

/// The owned encode skips the copy that the borrowed encode makes.
#[test]
fn owned_encode_allocates_less_than_borrowed_encode() {
    let codecs = PayloadCodecs::default();
    drop(codecs.encode_event(&completed(document(1))));
    let event = completed(document(50));
    let (stored, borrowed) = allocs_during(|| codecs.encode_event(&event).expect("encode"));
    drop(stored);
    let (stored, owned) = allocs_during(|| codecs.encode_event_owned(event).expect("encode"));
    drop(stored);
    assert!(
        owned < borrowed,
        "owned encode made {owned} allocations; borrowed encode made {borrowed}"
    );
}
