//! Deterministic (non-criterion) instruction/allocation-count profiling
//! harness for the replay of keyed-entity state snapshots (issue #2259).
//!
//! Wall-clock timing is not admissible on this machine. Drive the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! An `Entity::from_input(ctx, input).run(handler)` loop handles
//! `ENTITY_SNAPSHOT_REPLAY_PROFILE_N` ops. `ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE`
//! picks the state:
//!
//! - `counter` (default): an `i64` that each op adds to.
//! - `cart`: a shopping cart. The first 200 ops add an item of about 100
//!   bytes, so the state grows to about 20 KB. Each later op changes the
//!   quantity of one item.
//!
//! `ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT` picks the history layout:
//!
//! - `interleaved` (default): one op per decision. Each op signal is followed
//!   by its snapshot and its checkpoint decision.
//! - `batched`: every op signal arrives before the first decision. All
//!   signals come first, then the snapshots and the decisions.
//!
//! `WorkflowTestEnv` records the history with a frozen clock. Each decision of
//! the `interleaved` layout replays the whole history so far, so the
//! recording costs O(N²). Record it once, outside valgrind, with
//! `ENTITY_SNAPSHOT_REPLAY_PROFILE_MODE=record`. That mode writes the history
//! to the file that `ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY` names. The
//! default `replay` mode reads that file. With no file, it records in the
//! same process first.
//!
//! The replay calls `executor::run_workflow` once, through the bench function
//! `replay`. That entry point builds its context with `build_task_context`
//! and drives it with `drive_workflow`, as the cold replay of a worker
//! decision does. Read the inclusive cost of `replay`. It leaves out the
//! recording, the file read, the decode of the file and the history clone.
//! `ENTITY_SNAPSHOT_REPLAY_PROFILE_REPS` (default 1) repeats the replay.
//!
//! The replay must suspend at the next op wait with no other command, and
//! the handler must run once per op. So each recorded snapshot matched.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --features testing --bench owner_kernels_profile \
//!   --bench entity_snapshot_replay_profile --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.reason=="compiler-artifact" and .target.name=="entity_snapshot_replay_profile") | .executable')
//! ENTITY_SNAPSHOT_REPLAY_PROFILE_MODE=record ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE=cart \
//!   ENTITY_SNAPSHOT_REPLAY_PROFILE_N=800 ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY=cart-800.json "$BIN"
//! ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE=cart ENTITY_SNAPSHOT_REPLAY_PROFILE_N=800 \
//!   ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY=cart-800.json valgrind --tool=callgrind \
//!   --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::context::{WorkflowCommand, WorkflowContext};
use autumn_harvest::entity::{ENTITY_OP_SIGNAL, ENTITY_STATE_SIDE_EFFECT, Entity, EntityMessage};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::info::WorkflowHandlerFn;
use autumn_harvest::testing::{TestRunStatus, WorkflowTestEnv};
use autumn_harvest::types::ExecutionId;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The number of items that the `cart` shape adds before it only updates.
const CART_ITEMS: usize = 200;

/// The handler calls since the last reset.
static HANDLED: AtomicUsize = AtomicUsize::new(0);

type BoxedRun<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Counter,
    Cart,
}

impl Shape {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE") else {
            return Self::Counter;
        };
        match value.as_str() {
            "counter" => Self::Counter,
            "cart" => Self::Cart,
            other => {
                panic!(
                    "ENTITY_SNAPSHOT_REPLAY_PROFILE_SHAPE must be counter or cart, got {other:?}"
                )
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Cart => "cart",
        }
    }

    fn handler(self) -> WorkflowHandlerFn {
        match self {
            Self::Counter => counter,
            Self::Cart => cart,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Interleaved,
    Batched,
}

impl Layout {
    fn from_env() -> Self {
        let Ok(value) = std::env::var("ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT") else {
            return Self::Interleaved;
        };
        match value.as_str() {
            "interleaved" => Self::Interleaved,
            "batched" => Self::Batched,
            other => panic!(
                "ENTITY_SNAPSHOT_REPLAY_PROFILE_LAYOUT must be interleaved or batched, \
                 got {other:?}"
            ),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Interleaved => "interleaved",
            Self::Batched => "batched",
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|_| panic!("{key} must be an unsigned integer, got {value:?}"))
    })
}

/// The op of the `counter` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CounterOp {
    Add(i64),
}

/// One line of the `cart` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Item {
    sku: String,
    name: String,
    qty: u32,
    unit_cents: u64,
}

/// The state of the `cart` shape.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Cart {
    items: Vec<Item>,
    total_cents: u64,
}

/// The op of the `cart` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CartOp {
    Add {
        sku: String,
        name: String,
        unit_cents: u64,
    },
    SetQty {
        sku: String,
        qty: u32,
    },
}

fn sku(i: usize) -> String {
    format!("sku-{i:08}")
}

/// Op `i` of the `counter` shape.
fn counter_op(i: usize) -> CounterOp {
    CounterOp::Add(i64::try_from(i % 97).expect("a small number fits in i64") + 1)
}

/// Op `i` of the `cart` shape.
fn cart_op(i: usize) -> CartOp {
    if i < CART_ITEMS {
        CartOp::Add {
            sku: sku(i),
            name: format!("Merino wool crew sweater, colour {i:03}, size M"),
            unit_cents: 1_000 + (i as u64 * 37) % 9_000,
        }
    } else {
        CartOp::SetQty {
            sku: sku((i * 7) % CART_ITEMS),
            qty: u32::try_from(1 + i % 5).expect("a small number fits in u32"),
        }
    }
}

const fn apply_counter(count: i64, op: CounterOp) -> i64 {
    match op {
        CounterOp::Add(n) => count + n,
    }
}

fn apply_cart(mut cart: Cart, op: CartOp) -> Result<Cart, String> {
    match op {
        CartOp::Add {
            sku,
            name,
            unit_cents,
        } => {
            cart.total_cents += unit_cents;
            cart.items.push(Item {
                sku,
                name,
                qty: 1,
                unit_cents,
            });
        }
        CartOp::SetQty { sku, qty } => {
            let item = cart
                .items
                .iter_mut()
                .find(|item| item.sku == sku)
                .ok_or_else(|| format!("no item {sku}"))?;
            cart.total_cents -= u64::from(item.qty) * item.unit_cents;
            cart.total_cents += u64::from(qty) * item.unit_cents;
            item.qty = qty;
        }
    }
    Ok(cart)
}

/// The entity of the `counter` shape.
fn counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
    Box::pin(async move {
        let state = Entity::<i64>::from_input(ctx, input)
            .run(|count, op: CounterOp| async move {
                HANDLED.fetch_add(1, Ordering::Relaxed);
                Ok(apply_counter(count, op))
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(state))
    })
}

/// The entity of the `cart` shape.
fn cart(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
    Box::pin(async move {
        let state = Entity::<Cart>::from_input(ctx, input)
            .run(|cart, op: CartOp| async move {
                HANDLED.fetch_add(1, Ordering::Relaxed);
                apply_cart(cart, op)
            })
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(state))
    })
}

/// The op signal payload of op `i`.
fn message(shape: Shape, i: usize) -> Value {
    match shape {
        Shape::Counter => json!(EntityMessage::op(counter_op(i))),
        Shape::Cart => json!(EntityMessage::op(cart_op(i))),
    }
}

/// The state after `n` ops, as a snapshot stores it.
fn expected_state(shape: Shape, n: usize) -> Value {
    match shape {
        Shape::Counter => json!((0..n).map(counter_op).fold(0, apply_counter)),
        Shape::Cart => {
            let cart = (0..n)
                .map(cart_op)
                .try_fold(Cart::default(), apply_cart)
                .expect("every cart op applies");
            json!(cart)
        }
    }
}

/// A recorded history and what produced it.
#[derive(Serialize, Deserialize)]
struct Recording {
    shape: String,
    layout: String,
    n: usize,
    events: Vec<WorkflowEvent>,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a current-thread runtime builds")
}

/// Record `n` ops of `shape` in `layout` with `WorkflowTestEnv`.
fn record(rt: &tokio::runtime::Runtime, shape: Shape, layout: Layout, n: usize) -> Recording {
    let anchor = chrono::DateTime::from_timestamp(1_790_000_000, 0).expect("a fixed time");
    let env = WorkflowTestEnv::new().with_frozen_anchor(anchor);
    let env = match layout {
        Layout::Interleaved => env,
        Layout::Batched => (0..n).fold(env, |env, i| {
            env.queue_signal(ENTITY_OP_SIGNAL, message(shape, i))
        }),
    };
    let events = rt.block_on(async {
        let mut run = env.start(shape.handler(), json!({}));
        assert_eq!(run.run_until_blocked().await, TestRunStatus::Blocked);
        if layout == Layout::Interleaved {
            for i in 0..n {
                run.signal(ENTITY_OP_SIGNAL, message(shape, i))
                    .expect("the entity run is live");
                assert_eq!(
                    run.run_until_blocked().await,
                    TestRunStatus::Blocked,
                    "the entity must wait for the next op"
                );
            }
        }
        run.events().to_vec()
    });
    Recording {
        shape: shape.name().to_string(),
        layout: layout.name().to_string(),
        n,
        events,
    }
}

/// The values of the state snapshots of `events`.
fn snapshots(events: &[WorkflowEvent]) -> Vec<&Value> {
    events
        .iter()
        .filter_map(|event| match event {
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::EntityState,
                name: Some(name),
                value,
            } if name == ENTITY_STATE_SIDE_EFFECT => Some(value),
            _ => None,
        })
        .collect()
}

/// Check that `recording` holds one snapshot per op, plus the start one, and
/// that the last one is the expected state. It returns the serialized bytes
/// of all the snapshots.
fn check_recording(recording: &Recording, shape: Shape, layout: Layout, n: usize) -> usize {
    assert_eq!(
        recording.shape,
        shape.name(),
        "the history file has another shape"
    );
    assert_eq!(
        recording.layout,
        layout.name(),
        "the history file has another layout"
    );
    assert_eq!(recording.n, n, "the history file has another op count");
    let snapshots = snapshots(&recording.events);
    assert_eq!(snapshots.len(), n + 1, "each op must record one snapshot");
    let last = snapshots.last().expect("the start snapshot exists");
    assert_eq!(
        last["state"],
        expected_state(shape, n),
        "the last snapshot must hold the state after every op"
    );
    snapshots
        .iter()
        .map(|value| serde_json::to_vec(value).expect("a value serializes").len())
        .sum()
}

/// Replay `events` once, as the cold replay of a decision does.
#[inline(never)]
fn replay(
    rt: &tokio::runtime::Runtime,
    handler: WorkflowHandlerFn,
    events: Vec<WorkflowEvent>,
) -> WorkflowOutcome {
    let exec_id = ExecutionId::from_uuid(uuid::Uuid::from_u128(2259));
    rt.block_on(run_workflow(exec_id, events, handler, json!({})))
}

/// Check that the replay matched every recorded event and stopped at the
/// next op wait.
fn check_outcome(outcome: &WorkflowOutcome) {
    let WorkflowOutcome::Suspended { commands } = outcome else {
        panic!("the replay must suspend at the next op wait, got {outcome:?}");
    };
    assert_eq!(
        commands.len(),
        1,
        "the replay must emit one command: {commands:?}"
    );
    assert!(
        matches!(
            &commands[0],
            WorkflowCommand::WaitForSignal { signal_name, .. } if signal_name == ENTITY_OP_SIGNAL
        ),
        "the one command must be the op wait: {commands:?}"
    );
}

fn main() {
    let shape = Shape::from_env();
    let layout = Layout::from_env();
    let n = env_usize("ENTITY_SNAPSHOT_REPLAY_PROFILE_N", 200);
    let reps = env_usize("ENTITY_SNAPSHOT_REPLAY_PROFILE_REPS", 1);
    let mode = std::env::var("ENTITY_SNAPSHOT_REPLAY_PROFILE_MODE")
        .unwrap_or_else(|_| "replay".to_string());
    let file = std::env::var("ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY").ok();
    let rt = runtime();
    let (shape_name, layout_name) = (shape.name(), layout.name());

    match mode.as_str() {
        "record" => {
            let path = file.expect("record mode needs ENTITY_SNAPSHOT_REPLAY_PROFILE_HISTORY");
            let recording = record(&rt, shape, layout, n);
            let bytes = check_recording(&recording, shape, layout, n);
            let json = serde_json::to_vec(&recording).expect("the history serializes");
            std::fs::write(&path, &json).expect("the history file writes");
            println!(
                "entity_snapshot_replay_profile: mode=record shape={shape_name} \
                 layout={layout_name} n={n} events={} snapshot_bytes={bytes} file_bytes={}",
                recording.events.len(),
                json.len()
            );
        }
        "replay" => {
            let recording = file.map_or_else(
                || record(&rt, shape, layout, n),
                |path| {
                    let json = std::fs::read(&path).expect("the history file reads");
                    serde_json::from_slice(&json).expect("the history file decodes")
                },
            );
            let bytes = check_recording(&recording, shape, layout, n);
            for _ in 0..reps {
                HANDLED.store(0, Ordering::Relaxed);
                let outcome = replay(&rt, shape.handler(), recording.events.clone());
                check_outcome(&outcome);
                assert_eq!(
                    HANDLED.load(Ordering::Relaxed),
                    n,
                    "the replay must run the handler once per op"
                );
            }
            println!(
                "entity_snapshot_replay_profile: mode=replay shape={shape_name} \
                 layout={layout_name} n={n} reps={reps} events={} snapshot_bytes={bytes}",
                recording.events.len()
            );
        }
        other => {
            panic!("ENTITY_SNAPSHOT_REPLAY_PROFILE_MODE must be record or replay, got {other:?}")
        }
    }
}
