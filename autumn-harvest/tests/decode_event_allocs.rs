//! Allocation budget for `PayloadCodecs::decode_event` (issue #1721).
//!
//! `WorkflowEvent` is adjacently tagged. A map that lists `data` before `type`
//! forces serde to buffer the whole event into `Content` first. That doubles
//! the allocations on every history read. These tests pin the budget and prove
//! that the type-first decode matches `serde_json::from_value`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::payload_codec::{CodecError, PayloadCodec, PayloadCodecs};
use autumn_harvest::types::ActivityExecId;
use serde_json::{Value, json};
use std::sync::Arc;

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

/// Reverses the bytes, so a round trip proves the envelope decode ran.
struct ReverseCodec;

impl PayloadCodec for ReverseCodec {
    fn codec_id(&self) -> &'static str {
        "reverse"
    }
    fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(raw.iter().rev().copied().collect())
    }
    fn decode(&self, encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(encoded.iter().rev().copied().collect())
    }
}

fn reference_decode(value: Value) -> Result<WorkflowEvent, serde_json::Error> {
    serde_json::from_value(value)
}

#[test]
fn decode_allocates_less_than_from_value() {
    let codecs = PayloadCodecs::default();
    let wire = codecs
        .encode_event(&completed(document(50)))
        .expect("encode");
    let reference = wire.clone();
    let (event, old) = allocs_during(|| reference_decode(reference).expect("from_value"));
    drop(event);
    let (event, new) = allocs_during(|| codecs.decode_event(wire).expect("decode"));
    drop(event);
    // Measured at 50 items: 211 with `Content` buffering, 108 type-first.
    // The test compares the two paths, so a dependency bump cannot skew it.
    assert!(
        new * 3 <= old * 2,
        "decode_event made {new} allocations; from_value made {old}"
    );
}

#[test]
fn decode_round_trips_an_encoded_event() {
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
fn decode_round_trips_through_a_real_codec() {
    let mut codecs = PayloadCodecs::default();
    codecs.set_default(Arc::new(ReverseCodec));
    let original = completed(document(2));
    let wire = codecs.encode_event(&original).expect("encode");
    assert_eq!(wire["data"]["output"]["codec_id"], "reverse");
    let decoded = codecs.decode_event(wire).expect("decode");
    assert_eq!(
        serde_json::to_value(&decoded).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
}

#[test]
fn decode_accepts_type_and_data_in_either_text_order() {
    let id = ActivityExecId::new();
    let type_first = format!(
        r#"{{"type":"ActivityCompleted","data":{{"activity_id":"{id}","output":{{"a":1}}}}}}"#
    );
    let data_first = format!(
        r#"{{"data":{{"activity_id":"{id}","output":{{"a":1}}}},"type":"ActivityCompleted"}}"#
    );
    let codecs = PayloadCodecs::default();
    let decode = |text: &str| {
        let value: Value = serde_json::from_str(text).unwrap();
        serde_json::to_value(codecs.decode_event(value).expect("decode")).unwrap()
    };
    assert_eq!(decode(&type_first), decode(&data_first));
}

#[test]
fn decode_errors_match_from_value() {
    let id = ActivityExecId::new().to_string();
    let bad_inputs = [
        json!({"data": {}}),
        json!({"type": "NoSuchEvent", "data": {}}),
        json!({"type": "ActivityCompleted"}),
        json!({"type": "ActivityCompleted", "data": null}),
        json!({"type": "ActivityCompleted", "data": 5}),
        json!({"type": "ActivityCompleted", "data": {}}),
        json!({"type": "ActivityCompleted", "data": {"activity_id": 5, "output": 1}}),
        json!({"type": 5, "data": {}}),
        json!({"type": null, "data": {}}),
        json!({"type": "ActivityCompleted", "data": {"activity_id": id}, "extra": 1}),
        json!([1, 2]),
        Value::Null,
    ];
    let codecs = PayloadCodecs::default();
    for input in bad_inputs {
        let new = codecs.decode_event(input.clone()).unwrap_err().to_string();
        let old = reference_decode(input.clone()).unwrap_err().to_string();
        // `HarvestError` prefixes the serde message; the message itself must match.
        assert!(
            new.ends_with(&old),
            "input: {input}\n new: {new}\n old: {old}"
        );
    }
}

#[test]
fn decode_matches_from_value_on_valid_fallback_shapes() {
    let id = ActivityExecId::new().to_string();
    let inputs = [
        // An extra top-level key takes the fallback path.
        json!({"type": "ActivityCompleted", "data": {"activity_id": id, "output": 1}, "x": 1}),
        // Optional and defaulted fields may be absent or null.
        json!({"type": "WorkflowStarted", "data": {"input": null, "timestamp": "2026-01-01T00:00:00Z"}}),
        json!({"type": "WorkflowStarted", "data": {"input": 1, "timestamp": "2026-01-01T00:00:00Z",
            "last_completion_result": null, "last_error": null}}),
    ];
    let codecs = PayloadCodecs::default();
    for input in inputs {
        let new = codecs.decode_event(input.clone()).expect("decode");
        let old = reference_decode(input.clone()).expect("from_value");
        assert_eq!(
            serde_json::to_value(new).unwrap(),
            serde_json::to_value(old).unwrap(),
            "input: {input}"
        );
    }
}
