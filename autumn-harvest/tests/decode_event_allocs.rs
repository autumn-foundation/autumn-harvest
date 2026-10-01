//! Allocation budget for `PayloadCodecs::decode_event` (issue #1721).
//!
//! `WorkflowEvent` is adjacently tagged. A map that lists `data` before `type`
//! forces serde to buffer the whole event into `Content` first. That doubles
//! the allocations on every history read. These tests pin the budget and the
//! error paths of the type-first decode.

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

#[test]
fn decode_allocations_stay_within_budget() {
    let codecs = PayloadCodecs::default();
    let n = 50;
    let wire = codecs
        .encode_event(&completed(document(n)))
        .expect("encode");
    let (event, allocs) = allocs_during(|| codecs.decode_event(wire).expect("decode"));
    drop(event);
    // Measured: 211 with `Content` buffering, 108 type-first. The budget sits
    // between them, at three allocations per line item.
    let budget = (n as u64) * 3;
    assert!(
        allocs <= budget,
        "decode_event made {allocs} allocations for {n} line items; budget {budget}"
    );
}

#[test]
fn decode_ignores_key_order_in_the_wire_value() {
    let codecs = PayloadCodecs::default();
    let original = completed(document(2));
    let wire = codecs.encode_event(&original).expect("encode");
    let decoded = codecs.decode_event(wire).expect("decode");
    assert_eq!(
        serde_json::to_value(&decoded).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
}

#[test]
fn decode_rejects_a_value_without_a_type_tag() {
    let codecs = PayloadCodecs::default();
    let err = codecs.decode_event(json!({"data": {}})).unwrap_err();
    assert!(err.to_string().contains("type"), "got: {err}");
}

#[test]
fn decode_rejects_an_unknown_event_type() {
    let codecs = PayloadCodecs::default();
    let err = codecs
        .decode_event(json!({"type": "NoSuchEvent", "data": {}}))
        .unwrap_err();
    assert!(err.to_string().contains("NoSuchEvent"), "got: {err}");
}

#[test]
fn decode_rejects_a_missing_data_field() {
    let codecs = PayloadCodecs::default();
    let err = codecs
        .decode_event(json!({"type": "ActivityCompleted"}))
        .unwrap_err();
    assert!(err.to_string().contains("data"), "got: {err}");
}

#[test]
fn decode_rejects_a_non_object_root() {
    let codecs = PayloadCodecs::default();
    assert!(codecs.decode_event(json!([1, 2])).is_err());
    assert!(codecs.decode_event(Value::Null).is_err());
}

#[test]
fn decode_rejects_a_wrongly_typed_field() {
    let codecs = PayloadCodecs::default();
    let wire = json!({"type": "ActivityCompleted", "data": {"activity_id": 5, "output": 1}});
    assert!(codecs.decode_event(wire).is_err());
}
