#![allow(clippy::all, clippy::pedantic, clippy::nursery)]
//! Declarative DAG node compensation — issue #780.
//!
//! ## The problem
//!
//! A `#[dag]` pipeline that half-succeeds leaves its completed nodes' side
//! effects **dangling**. Reserve inventory, charge the card, allocate a
//! shipment — then the label printer 500s. The DAG fails, and the inventory is
//! still held, the customer is still charged, the shipment slot is still
//! allocated. Until now the only way to undo that was to abandon the `#[dag]`
//! surface entirely and hand-write a `#[workflow]` around
//! [`Saga`](autumn_harvest::saga::Saga) — losing the graph you actually wanted
//! to declare.
//!
//! ## The solution: `.compensate(…)`
//!
//! Declare, per node, the activity that **undoes** it:
//!
//! ```text
//!   reserve_inventory ─▶ charge_payment ─▶ allocate_shipment ─▶ print_label ─▶ notify_customer
//!   (release_inventory)  (refund_payment)  (deallocate_shipment) (void_label)   (no compensator)
//!                                                                     ✗ fails
//!   unwind ◀────────────────────────────────────────────────────────────┘
//!   deallocate_shipment ─▶ refund_payment ─▶ release_inventory   (reverse topological / LIFO)
//! ```
//!
//! | Method                        | Effect                                                     |
//! |-------------------------------|------------------------------------------------------------|
//! | `.compensate(undo_fn)`        | Typed — the name is derived from the fn item, so typo-proof |
//! | `.compensate_named("undo")`   | Escape hatch when the fn item isn't in scope                |
//!
//! ## What runs, and what doesn't
//!
//! On a terminal DAG failure (`Err("one or more DAG tasks failed")`) the engine
//! unwinds **only** the nodes that BOTH succeeded AND declare a compensator, in
//! reverse topological (LIFO) order. A node that was skipped (trigger rule or
//! `.condition(…)`), never reached, or that failed itself is never compensated —
//! only a *successful* forward step has an effect to undo.
//!
//! A compensator receives the fixed envelope
//! `{"dag_compensate": <node>, "input": <the node's resolved forward input>,
//! "output": <the node's recorded output>}` and dispatches on the compensated
//! node's own queue.
//!
//! ## Invariants
//!
//! - **No new `WorkflowEvent` variant, no migration.** Compensation rides
//!   ordinary `ActivityScheduled`/`ActivityCompleted` events plus the existing
//!   `Saga` dedup markers (`saga_compensated:{seq}`), and feeds the existing
//!   `harvest.saga.compensated` / `harvest.saga.compensation_failed` counters.
//! - **Success path unchanged.** A DAG that succeeds never constructs a saga:
//!   the unwind lives entirely on the failure branch.
//! - **Cancellation does not auto-compensate** (`docs/saga.md`) — a cancelled
//!   run returns the original DAG error and dispatches zero compensators.
//! - **Unified-DAG only.** Build with `--features unified-dag-execution`.
//!
//! Run with:
//!   cargo run --example dag_compensation --features unified-dag-execution

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Forward activities — the fulfillment pipeline
// ---------------------------------------------------------------------------

/// **Reserve inventory.** Returns the reservation id the compensator releases.
#[activity(start_to_close = "30s")]
async fn reserve_inventory(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "reservation_id": "rsv-9001" }))
}

/// **Charge the customer.** Returns the charge id the compensator refunds.
#[activity(start_to_close = "30s")]
async fn charge_payment(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "charge_id": "ch_abc123", "amount_cents": 7_700 }))
}

/// **Allocate a shipment slot.** Returns the slot id the compensator frees.
#[activity(start_to_close = "30s")]
async fn allocate_shipment(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "shipment_id": "shp-77" }))
}

/// **Print the shipping label.** The realistic failure point — a third-party
/// label API that can 500 after everything upstream has already committed.
#[activity(start_to_close = "30s")]
async fn print_label(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!({ "label_id": "lbl-5" }))
}

/// **Notify the customer.** Declares **no** compensator: an email that was sent
/// cannot be un-sent, so there is nothing to undo.
#[activity(start_to_close = "30s")]
async fn notify_customer(_ctx: &ActivityContext) -> Result<Value, String> {
    Ok(json!("notified"))
}

// ---------------------------------------------------------------------------
// Compensators — each takes the `{dag_compensate, input, output}` envelope
// ---------------------------------------------------------------------------
//
// Compensate **by ID, read out of the envelope's `output`** — the same
// idempotency contract the rest of `docs/saga.md` specifies. Compensations
// re-run wholesale on replay, so `release_inventory(rsv-9001)` must be safe to
// call twice, while a hypothetical `release_most_recent_reservation()` would
// not be.

/// Undo `reserve_inventory` — releases the exact reservation it created.
#[activity(start_to_close = "30s")]
async fn release_inventory(_ctx: &ActivityContext, envelope: Value) -> Result<Value, String> {
    let reservation_id = envelope["output"]["reservation_id"]
        .as_str()
        .ok_or("compensator envelope is missing output.reservation_id")?;
    println!("  ↩ release_inventory({reservation_id})");
    Ok(Value::Null)
}

/// Undo `charge_payment` — refunds the exact charge it created.
#[activity(start_to_close = "30s")]
async fn refund_payment(_ctx: &ActivityContext, envelope: Value) -> Result<Value, String> {
    let charge_id = envelope["output"]["charge_id"]
        .as_str()
        .ok_or("compensator envelope is missing output.charge_id")?;
    println!("  ↩ refund_payment({charge_id})");
    Ok(Value::Null)
}

/// Undo `allocate_shipment` — frees the exact slot it allocated.
#[activity(start_to_close = "30s")]
async fn deallocate_shipment(_ctx: &ActivityContext, envelope: Value) -> Result<Value, String> {
    let shipment_id = envelope["output"]["shipment_id"]
        .as_str()
        .ok_or("compensator envelope is missing output.shipment_id")?;
    println!("  ↩ deallocate_shipment({shipment_id})");
    Ok(Value::Null)
}

/// Undo `print_label` — voids the exact label it printed.
#[activity(start_to_close = "30s")]
async fn void_label(_ctx: &ActivityContext, envelope: Value) -> Result<Value, String> {
    let label_id = envelope["output"]["label_id"]
        .as_str()
        .ok_or("compensator envelope is missing output.label_id")?;
    println!("  ↩ void_label({label_id})");
    Ok(Value::Null)
}

// ---------------------------------------------------------------------------
// The DAG
// ---------------------------------------------------------------------------

/// **The teaching shape.** Five nodes, four of them compensable, one
/// `.compensate(…)` call per node and zero hand-written rollback code.
///
/// If any node fails, every *succeeded* compensable node upstream of the
/// failure is undone in reverse topological order. `notify_customer` declares
/// no compensator, so it contributes nothing to the unwind.
#[dag]
fn fulfillment(dag: &mut DagBuilder) {
    let reserve = dag
        .activity(reserve_inventory)
        .compensate(release_inventory);
    let charge = dag
        .activity(charge_payment)
        .upstream(&reserve)
        .compensate(refund_payment);
    let allocate = dag
        .activity(allocate_shipment)
        .upstream(&charge)
        .compensate(deallocate_shipment);
    let label = dag
        .activity(print_label)
        .upstream(&allocate)
        .compensate(void_label);
    // A sent notification cannot be un-sent — no compensator.
    let _notify = dag.activity(notify_customer).upstream(&label);
}

fn main() {
    // Registration example only — not a runnable binary without an Autumn app.
    let _dags = dags![fulfillment];
    let _acts = activities![
        reserve_inventory,
        charge_payment,
        allocate_shipment,
        print_label,
        notify_customer,
        release_inventory,
        refund_payment,
        deallocate_shipment,
        void_label
    ];

    println!("dag_compensation example compiled successfully");
    println!();
    println!("Forward:");
    println!(
        "  reserve_inventory ─▶ charge_payment ─▶ allocate_shipment ─▶ print_label ─▶ notify_customer"
    );
    println!();
    println!("If print_label fails, the engine unwinds the succeeded compensable prefix");
    println!("in reverse topological (LIFO) order:");
    println!("  deallocate_shipment ─▶ refund_payment ─▶ release_inventory");
    println!();
    println!("`notify_customer` declares no compensator and is never part of an unwind.");
    println!("A successful run constructs no saga at all.");
}

// Gated on `testing` + `unified-dag-execution`: the example binary builds under
// `--features unified-dag-execution` alone, and the embedded harness/replay
// tests appear when `testing` is also enabled. Run with:
//   cargo test -p autumn-harvest --features testing,unified-dag-execution \
//     --example dag_compensation
#[cfg(all(test, feature = "testing", feature = "unified-dag-execution"))]
mod tests {
    use super::*;
    use autumn_harvest::event::WorkflowEvent;
    use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    /// Every `ActivityScheduled` name in history order, restricted to
    /// `of_interest` — the authoritative record of what actually dispatched.
    fn scheduled_names_among(events: &[WorkflowEvent], of_interest: &[&str]) -> Vec<String> {
        events
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::ActivityScheduled { name, .. }
                    if of_interest.contains(&name.as_str()) =>
                {
                    Some(name.clone())
                }
                _ => None,
            })
            .collect()
    }

    const COMPENSATORS: [&str; 4] = [
        "release_inventory",
        "refund_payment",
        "deallocate_shipment",
        "void_label",
    ];

    /// Forward node → (ledger resource it commits (`None` = no side effect),
    /// the key of the DISTINCT output blob it returns).
    ///
    /// Each node returns a different payload so an envelope assertion can only
    /// pass if the engine carried THAT node's recorded output (a single shared
    /// blob would make every `output` assertion pass regardless).
    const RESOURCES: [(&str, Option<&str>, &str); 5] = [
        ("reserve_inventory", Some("inventory"), "reservation_id"),
        ("charge_payment", Some("payment"), "charge_id"),
        ("allocate_shipment", Some("shipment"), "shipment_id"),
        ("print_label", Some("label"), "label_id"),
        ("notify_customer", None, "notification_id"),
    ];

    /// The per-node output value paired with the key above.
    const OUTPUT_VALUES: [(&str, &str); 5] = [
        ("reserve_inventory", "rsv-9001"),
        ("charge_payment", "ch_abc123"),
        ("allocate_shipment", "shp-77"),
        ("print_label", "lbl-5"),
        ("notify_customer", "ntf-1"),
    ];

    /// Compensator → the ledger resource it releases.
    const COMPENSATOR_RESOURCES: [(&str, &str); 4] = [
        ("release_inventory", "inventory"),
        ("refund_payment", "payment"),
        ("deallocate_shipment", "shipment"),
        ("void_label", "label"),
    ];

    type Ledger = Arc<Mutex<BTreeMap<String, i64>>>;

    fn bump(ledger: &Ledger, resource: &str, delta: i64) {
        *ledger
            .lock()
            .expect("ledger lock")
            .entry(resource.to_string())
            .or_insert(0) += delta;
    }

    /// Build a test env whose forward activities credit a ledger resource and
    /// whose compensators debit it, with `failing_node` returning an error
    /// (`None` = every node succeeds). A net-zero ledger at the end means zero
    /// uncompensated side effects.
    fn env_with_ledger(ledger: &Ledger, failing_node: Option<&'static str>) -> WorkflowTestEnv {
        let mut env = WorkflowTestEnv::new();

        for (node, resource, output_key) in RESOURCES {
            let resource = resource.map(str::to_string);
            let should_fail = failing_node == Some(node);
            let ledger = Arc::clone(ledger);
            let output_key = output_key.to_string();
            let output_value = OUTPUT_VALUES
                .iter()
                .find(|(n, _)| *n == node)
                .map(|(_, v)| (*v).to_string())
                .expect("every node declares an output value");
            env = env.mock_activity(node, move |_| {
                if should_fail {
                    // A failed step commits nothing, so it credits nothing.
                    return Err("label printer returned 500".to_string());
                }
                if let Some(resource) = &resource {
                    bump(&ledger, resource, 1);
                }
                // A DISTINCT blob per node, so an `output` assertion cannot pass
                // by accident.
                Ok(json!({ output_key.clone(): output_value.clone() }))
            });
        }

        for (compensator, resource) in COMPENSATOR_RESOURCES {
            let resource = resource.to_string();
            let ledger = Arc::clone(ledger);
            env = env.mock_activity(compensator, move |_| {
                bump(&ledger, &resource, -1);
                Ok(Value::Null)
            });
        }

        env
    }

    /// (a) **Happy path.** A DAG that declares compensators but succeeds never
    /// constructs a saga: zero compensator dispatches, zero saga markers.
    #[tokio::test]
    async fn successful_run_compensates_nothing() {
        let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));

        let outcome = env_with_ledger(&ledger, None)
            .run(__autumn_workflow_info_fulfillment().handler, Value::Null)
            .await;

        assert!(
            outcome.result.is_ok(),
            "the all-succeeding fulfillment DAG must succeed, got {:?}",
            outcome.result
        );
        assert_eq!(
            scheduled_names_among(outcome.events(), &COMPENSATORS),
            Vec::<String>::new(),
            "a successful DAG must dispatch NO compensators"
        );
        assert!(
            !outcome.events().iter().any(|e| matches!(
                e,
                WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("saga_")
            )),
            "a successful DAG must record NO saga markers"
        );

        // Every forward step committed exactly once and nothing was undone.
        let balances = ledger.lock().expect("ledger lock").clone();
        assert_eq!(
            balances,
            BTreeMap::from([
                ("inventory".to_string(), 1),
                ("label".to_string(), 1),
                ("payment".to_string(), 1),
                ("shipment".to_string(), 1),
            ]),
            "a successful run must leave every forward effect committed"
        );
    }

    /// (b) **The money test.** `print_label` fails after three upstream nodes
    /// have committed. The engine unwinds them in reverse topological order and
    /// the ledger nets to zero — **zero uncompensated side effects**.
    #[tokio::test]
    async fn failure_unwinds_in_reverse_order_leaving_no_uncompensated_effects() {
        let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));

        let outcome = env_with_ledger(&ledger, Some("print_label"))
            .run(__autumn_workflow_info_fulfillment().handler, Value::Null)
            .await;

        // A SUCCESSFUL unwind still surfaces the ORIGINAL DAG error.
        assert_eq!(
            outcome.result.as_ref().unwrap_err(),
            "one or more DAG tasks failed",
            "a successful unwind must return the original DAG error unchanged"
        );

        // Reverse topological (LIFO) over the succeeded compensable prefix.
        // `void_label` is absent: its node FAILED, so it has nothing to undo.
        assert_eq!(
            scheduled_names_among(outcome.events(), &COMPENSATORS),
            vec![
                "deallocate_shipment".to_string(),
                "refund_payment".to_string(),
                "release_inventory".to_string(),
            ],
            "compensators must dispatch in reverse topological (LIFO) order, and \
             the failed node's own compensator must never run"
        );

        // Zero uncompensated side effects: every resource nets to 0.
        for (resource, balance) in ledger.lock().expect("ledger lock").iter() {
            assert_eq!(
                *balance, 0,
                "resource '{resource}' was left uncompensated (balance {balance})"
            );
        }

        // The compensator receives the node's identity, resolved forward input,
        // and recorded output — nothing else.
        let envelope = outcome
            .events()
            .iter()
            .find_map(|e| match e {
                WorkflowEvent::ActivityScheduled { name, input, .. }
                    if name == "refund_payment" =>
                {
                    Some(input.clone())
                }
                _ => None,
            })
            .expect("refund_payment must have been scheduled");
        assert_eq!(
            envelope["dag_compensate"],
            json!("charge_payment"),
            "the envelope must identify the compensated node, got {envelope}"
        );
        // `charge_payment` returns a blob no other node returns, so this can only
        // pass if the engine carried THAT node's recorded output.
        assert_eq!(
            envelope["output"],
            json!({ "charge_id": "ch_abc123" }),
            "the envelope must carry the node's recorded output, got {envelope}"
        );
        // Pinned by exact value: `is_some()` would also accept `Null`, which is
        // exactly what dropping the forward-input capture produces.
        assert_eq!(
            envelope.get("input"),
            Some(&json!({
                "conf": Value::Null,
                "dag_task": "charge_payment",
            })),
            "the envelope must carry the node's RESOLVED FORWARD INPUT, got {envelope}"
        );
    }

    /// (c) **Self-check.** A compensated history replays deterministically.
    ///
    /// A failing DAG's handler returns `Err("one or more DAG tasks failed")`, and
    /// its history ends in a terminal `WorkflowFailed`. Since issue #952 a replay
    /// that reproduces that failure is a clean replay — `ReplaySucceeded` with the
    /// reproduced error in `ReplayReport::failure_message()`, which is precisely
    /// "reproduced the same terminal error with ZERO divergence". Pinning the
    /// exact error rules out a failure for some other reason.
    #[tokio::test]
    async fn compensated_history_replays_deterministically() {
        let ledger: Ledger = Arc::new(Mutex::new(BTreeMap::new()));
        let handler = __autumn_workflow_info_fulfillment().handler;

        let outcome = env_with_ledger(&ledger, Some("print_label"))
            .run(handler, Value::Null)
            .await;
        assert!(
            outcome.result.is_err(),
            "the DAG must fail to capture a history"
        );

        let report = outcome.replay_check(handler).await;
        // Issue #952: the history ends in a terminal `WorkflowFailed`, so a replay
        // that reproduces the same failure is a CLEAN replay and the error is read
        // through `failure_message()`. The assertion is unchanged in substance:
        // the ORIGINAL DAG error, deterministically, never a divergence.
        assert_eq!(
            report.failure_message(),
            Some("one or more DAG tasks failed"),
            "a compensating history must replay deterministically to the original \
             DAG error:\n{report}"
        );
        assert!(
            !matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
            "a compensating history must never replay as non-determinism:\n{report}"
        );
    }
}
