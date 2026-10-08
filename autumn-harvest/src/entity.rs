//! Keyed entity: serialized handlers over durable state for each key (issue #1975).
//!
//! An entity is sugar over a workflow. It adds no event variant and no
//! migration. See `docs/adr/0006-keyed-entity.md`.
//!
//! | Entity concept | Harvest part |
//! |---|---|
//! | Entity type | A root `#[workflow]` function that calls [`Entity::run`] |
//! | Entity key | The `workflow_id` |
//! | Operation | A signal named [`ENTITY_OP_SIGNAL`] |
//! | Send an operation | `signal_with_start` |
//! | Read state | The query [`ENTITY_STATE_QUERY`] |
//!
//! # Guarantees
//!
//! - **One handler at a time for each key.** One active run exists for each
//!   `(workflow_name, workflow_id)`. The loop awaits each handler to the end
//!   before it takes the next operation.
//! - **State survives a crash.** Replay rebuilds the state from the input,
//!   the recorded operations and the recorded activity results.
//! - **No lost operations at a checkpoint.** The loop carries waiting
//!   operations in the continue-as-new input while the input fits the
//!   workflow input cap. It runs the operations that do not fit first.
//! - **Atomic state changes.** A handler gets a copy of the state. Only an
//!   `Ok` result replaces the state. Activities that ran before an `Err` stay
//!   done.
//!
//! # Limits
//!
//! - The checkpoint input is the whole state. A state larger than the
//!   workflow input cap fails the run at its first checkpoint. Payload
//!   offload lifts this limit only when its threshold is at or below the cap.
//! - Continue-as-new works only in a root workflow. An entity cannot be a
//!   child workflow.
//! - [`Entity::max_ops_per_run`], an `execution_timeout` and the
//!   [`EntityCheckpoint`] wire form are part of replay. Change them as a
//!   versioned workflow change.
//! - A handler must await all the work it starts, and its error text must be
//!   deterministic. The error text rides in the checkpoint input.
//! - A handler panic fails the run, as in any workflow.
//!
//! # Example
//!
//! ```rust,ignore
//! use autumn_harvest::entity::{Entity, EntityCheckpoint};
//! use autumn_harvest::prelude::*;
//!
//! #[workflow]
//! async fn counter(
//!     ctx: &WorkflowContext,
//!     input: EntityCheckpoint<u64>,
//! ) -> Result<u64, String> {
//!     Entity::new(ctx, input)
//!         .run(|count, add: u64| async move { Ok(count + add) })
//!         .await
//!         .map_err(|e| e.to_string())
//! }
//! ```
//!
//! A client sends an operation with the generated stub:
//!
//! ```rust,ignore
//! use autumn_harvest::entity::{ENTITY_OP_SIGNAL, EntityCheckpoint, EntityMessage};
//! use autumn_harvest::TypedSignalWithStartOptions;
//!
//! CounterStub::signal_with_start(
//!     conn, &client, "counter-42", EntityCheckpoint::<u64>::default(),
//!     ENTITY_OP_SIGNAL, EntityMessage::op(5_u64),
//!     TypedSignalWithStartOptions { idempotency_key: Some(op_id), ..Default::default() },
//! ).await?;
//! ```

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::context::WorkflowContext;
use crate::error::HarvestResult;

/// The signal name that carries an [`EntityMessage`].
pub const ENTITY_OP_SIGNAL: &str = "harvest.entity.op";

/// The query that returns the last committed state.
pub const ENTITY_STATE_QUERY: &str = "harvest.entity.state";

/// The query that returns the [`EntityStats`].
pub const ENTITY_STATS_QUERY: &str = "harvest.entity.stats";

/// The side-effect id that records each checkpoint decision.
///
/// Replay reads the recorded decision. The live history size does not
/// change it.
pub const ENTITY_CHECKPOINT_SIDE_EFFECT: &str = "harvest.entity.checkpoint";

/// The side-effect id that records the byte budget of a checkpoint input.
///
/// The budget is the configured workflow input cap. A config change between
/// a run and its replay does not change what the run carried.
pub const ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT: &str = "harvest.entity.checkpoint_budget";

/// One message on the [`ENTITY_OP_SIGNAL`] signal.
///
/// The wire form is `{"kind":"op","op":<op>}` or `{"kind":"delete"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum EntityMessage<O> {
    /// Apply `op` to the state.
    #[non_exhaustive]
    Op {
        /// The operation for the handler.
        op: O,
    },
    /// Delete the entity.
    ///
    /// The run completes with the current state when no operation waits in
    /// history. Otherwise the state resets to its default and the loop goes
    /// on.
    Delete,
}

impl<O> EntityMessage<O> {
    /// A message that applies `op`.
    #[must_use]
    pub const fn op(op: O) -> Self {
        Self::Op { op }
    }

    /// A message that deletes the entity.
    #[must_use]
    pub const fn delete() -> Self {
        Self::Delete
    }
}

/// Counters for the life of one entity. They carry across checkpoints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct EntityStats {
    /// Operations whose handler returned `Ok`.
    pub applied: u64,
    /// Operations whose handler returned `Err`, or that did not decode.
    pub failed: u64,
    /// Checkpoints taken with continue-as-new.
    pub checkpoints: u64,
    /// The error of the most recent failed operation.
    pub last_error: Option<String>,
}

/// The input of an entity run.
///
/// A new entity starts from `{}` or `null`. A checkpoint writes the state,
/// the operations that still wait, and the counters.
///
/// The start input is trusted. A caller who may start the workflow may set
/// the first state and pending operations. Send `{}` from clients.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(bound(serialize = "S: Serialize"))]
pub struct EntityCheckpoint<S> {
    /// The committed state.
    pub state: S,
    /// Raw [`EntityMessage`] values to run before new signals.
    pub pending: Vec<Value>,
    /// Counters for the life of the entity.
    pub stats: EntityStats,
}

/// The decode shape of [`EntityCheckpoint`]. Each field may be absent or
/// `null`.
#[derive(Deserialize)]
#[serde(default, bound(deserialize = "S: DeserializeOwned + Default"))]
struct CheckpointWire<S> {
    #[serde(deserialize_with = "null_as_default")]
    state: S,
    #[serde(deserialize_with = "null_as_default")]
    pending: Vec<Value>,
    #[serde(deserialize_with = "null_as_default")]
    stats: EntityStats,
}

impl<S: Default> Default for CheckpointWire<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            pending: Vec::new(),
            stats: EntityStats::default(),
        }
    }
}

fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

impl<'de, S: DeserializeOwned + Default> Deserialize<'de> for EntityCheckpoint<S> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = Option::<CheckpointWire<S>>::deserialize(deserializer)?.unwrap_or_default();
        Ok(Self {
            state: wire.state,
            pending: wire.pending,
            stats: wire.stats,
        })
    }
}

impl<S: Default> EntityCheckpoint<S> {
    /// A checkpoint that starts from `state`.
    #[must_use]
    pub fn with_state(state: S) -> Self {
        Self {
            state,
            ..Self::default()
        }
    }
}

/// The handler loop of one entity run.
pub struct Entity<'a, S> {
    ctx: &'a WorkflowContext,
    checkpoint: EntityCheckpoint<S>,
    max_ops_per_run: Option<u64>,
}

impl<'a, S> Entity<'a, S>
where
    S: Serialize + DeserializeOwned + Default + Clone,
{
    /// A loop for the run that `ctx` drives, from `checkpoint`.
    #[must_use]
    pub const fn new(ctx: &'a WorkflowContext, checkpoint: EntityCheckpoint<S>) -> Self {
        Self {
            ctx,
            checkpoint,
            max_ops_per_run: None,
        }
    }

    /// Take a checkpoint after `n` operations in one run.
    ///
    /// This bound needs no event. The history checks still apply. A value of
    /// `0` counts as `1`. A change to `n` changes replay, so version it.
    #[must_use]
    pub const fn max_ops_per_run(mut self, n: u64) -> Self {
        self.max_ops_per_run = Some(if n == 0 { 1 } else { n });
        self
    }

    /// Run the loop until a delete ends the entity.
    ///
    /// `handler` gets a copy of the state and one operation. Its `Ok` value
    /// becomes the new state. An `Err` keeps the old state and counts as a
    /// failed operation. Capture `ctx` in the handler to call activities.
    ///
    /// Returns the final state after a delete. A checkpoint does not return.
    ///
    /// # Errors
    ///
    /// Returns the engine error of a signal wait, a side effect or a
    /// continue-as-new, for example [`HarvestError::Cancelled`].
    /// Returns [`HarvestError::PayloadTooLarge`] when the state alone is
    /// larger than the workflow input cap at a checkpoint.
    /// Returns [`HarvestError::Serialization`] if the state does not
    /// serialize at a checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if `continue_as_new` resolves. The engine never resolves it.
    ///
    /// [`HarvestError::Cancelled`]: crate::error::HarvestError::Cancelled
    /// [`HarvestError::PayloadTooLarge`]: crate::error::HarvestError::PayloadTooLarge
    /// [`HarvestError::Serialization`]: crate::error::HarvestError::Serialization
    pub async fn run<O, H, Fut>(self, mut handler: H) -> HarvestResult<S>
    where
        O: DeserializeOwned,
        H: FnMut(S, O) -> Fut,
        Fut: Future<Output = Result<S, String>>,
    {
        let Self {
            ctx,
            mut checkpoint,
            max_ops_per_run,
        } = self;
        let mut waiting: VecDeque<Value> = std::mem::take(&mut checkpoint.pending).into();
        let published = Arc::new(Mutex::new(Published::of(&checkpoint)));
        register_queries(ctx, &published);

        let mut run = RunCounters::default();
        let mut budget: Option<Option<u64>> = None;
        loop {
            let raw = match waiting.pop_front() {
                Some(raw) => raw,
                None => ctx.wait_for_signal(ENTITY_OP_SIGNAL).await?,
            };
            run.op_bytes += json_len(&raw)?;
            match serde_json::from_value::<EntityMessage<O>>(raw) {
                Ok(EntityMessage::Op { op }) => match handler(checkpoint.state.clone(), op).await {
                    Ok(next) => {
                        checkpoint.state = next;
                        checkpoint.stats.applied += 1;
                    }
                    Err(error) => record_failure(&mut checkpoint.stats, error),
                },
                Ok(EntityMessage::Delete) => {
                    waiting.extend(ctx.drain_signals_raw(ENTITY_OP_SIGNAL)?);
                    if waiting.is_empty() {
                        return Ok(checkpoint.state);
                    }
                    checkpoint.state = S::default();
                }
                Err(error) => record_failure(
                    &mut checkpoint.stats,
                    format!("entity message does not decode: {error}"),
                ),
            }
            run.ops += 1;
            *lock(&published) = Published::of(&checkpoint);

            if checkpoint_due(ctx, &run, max_ops_per_run)? {
                let limit = match budget {
                    Some(limit) => limit,
                    None => *budget.insert(
                        ctx.side_effect(ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT, || {
                            ctx.continue_as_new_input_budget()
                        })?,
                    ),
                };
                if !claim_waiting_ops(ctx, &checkpoint, &mut waiting, limit)? {
                    // An op does not fit. Run it in this run, then try again.
                    continue;
                }
                checkpoint.pending = waiting.into();
                checkpoint.stats.checkpoints += 1;
                ctx.continue_as_new(serde_json::to_value(&checkpoint)?)
                    .await?;
                unreachable!("continue_as_new never resolves while the run is active");
            }
        }
    }
}

/// Counters for one run. They reset at each checkpoint.
#[derive(Default)]
struct RunCounters {
    /// Messages taken in this run.
    ops: u64,
    /// Serialized bytes of those messages.
    op_bytes: u64,
}

/// The committed view that the queries read.
struct Published {
    state: Result<Value, String>,
    stats: EntityStats,
}

impl Published {
    fn of<S: Serialize>(checkpoint: &EntityCheckpoint<S>) -> Self {
        Self {
            state: serde_json::to_value(&checkpoint.state).map_err(|e| e.to_string()),
            stats: checkpoint.stats.clone(),
        }
    }
}

/// Lock the view. A panic in a query cannot leave it half written, so a
/// poisoned lock is safe to read.
fn lock(published: &Mutex<Published>) -> MutexGuard<'_, Published> {
    published.lock().unwrap_or_else(PoisonError::into_inner)
}

fn register_queries(ctx: &WorkflowContext, published: &Arc<Mutex<Published>>) {
    let view = Arc::clone(published);
    ctx.register_query_handler(ENTITY_STATE_QUERY, move |_: &Value| {
        lock(&view).state.clone()
    });
    let view = Arc::clone(published);
    ctx.register_query_handler(ENTITY_STATS_QUERY, move |_: &Value| {
        Ok(lock(&view).stats.clone())
    });
}

/// Claim the op signals that wait in history, while the checkpoint fits.
///
/// Returns `true` when every waiting op is in `waiting` and the input fits
/// `limit`. Returns `false` when an op does not fit. That op stays at the
/// end of `waiting`, so the loop runs it before the next checkpoint. An
/// input that does not fit with no op to carry still checkpoints. The
/// continue-as-new cap then reports the size, and no op is lost.
fn claim_waiting_ops<S: Serialize>(
    ctx: &WorkflowContext,
    checkpoint: &EntityCheckpoint<S>,
    waiting: &mut VecDeque<Value>,
    limit: Option<u64>,
) -> HarvestResult<bool> {
    let Some(limit) = limit else {
        waiting.extend(ctx.drain_signals_raw(ENTITY_OP_SIGNAL)?);
        return Ok(true);
    };
    // The base input has an empty `pending` array. Each op adds its own
    // bytes and at most one separator, so the sum is an upper bound.
    let mut size = json_len(checkpoint)?;
    for op in &*waiting {
        size += json_len(op)? + 1;
    }
    if !waiting.is_empty() && size > limit {
        return Ok(false);
    }
    while let Some(op) = ctx.try_wait_for_signal(ENTITY_OP_SIGNAL)? {
        size += json_len(&op)? + 1;
        waiting.push_back(op);
        if size > limit {
            return Ok(false);
        }
    }
    Ok(true)
}

fn json_len<T: Serialize + ?Sized>(value: &T) -> HarvestResult<u64> {
    Ok(serde_json::to_string(value)?.len() as u64)
}

fn record_failure(stats: &mut EntityStats, error: String) {
    stats.failed += 1;
    stats.last_error = Some(error);
}

/// Decide if the run takes a checkpoint after this op.
///
/// The op bound is a pure function of the run, so it needs no event. The
/// other checks read live values, and the side effect records the answer.
/// Replay reads that answer back.
///
/// The loaded history count does not include the events of the current
/// task. The run therefore also counts one event for each op it took, so a
/// long backlog in one task still reaches the threshold. Op bytes count
/// against a quarter of the history byte cap for the same reason.
fn checkpoint_due(
    ctx: &WorkflowContext,
    run: &RunCounters,
    max_ops_per_run: Option<u64>,
) -> HarvestResult<bool> {
    if max_ops_per_run.is_some_and(|max| run.ops >= max) {
        return Ok(true);
    }
    let policy = ctx.history_policy();
    let by_engine = ctx.should_continue_as_new();
    let by_events =
        ctx.history_event_count().saturating_add(run.ops) > policy.continue_as_new_threshold();
    let by_bytes = policy
        .byte_hard_cap()
        .is_some_and(|cap| run.op_bytes > cap / 4);
    let live = by_engine || by_events || by_bytes;
    ctx.side_effect(ENTITY_CHECKPOINT_SIDE_EFFECT, || live)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;
    use crate::context::WorkflowHistoryPolicy;
    use crate::event::{SideEffectKind, WorkflowEvent};
    use crate::testing::{ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
    use crate::types::ExecutionId;

    type BoxedRun<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

    /// The counter op: add `n`, or fail with `fail`.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum CounterOp {
        Add(i64),
        Fail,
    }

    fn checkpoint_of(input: Value) -> Result<EntityCheckpoint<i64>, String> {
        serde_json::from_value(input).map_err(|e| e.to_string())
    }

    fn apply(count: i64, op: &CounterOp) -> Result<i64, String> {
        match op {
            CounterOp::Add(n) => Ok(count + n),
            CounterOp::Fail => Err("op refused".to_string()),
        }
    }

    /// A counter entity with no activity.
    fn counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .run(|count, op: CounterOp| async move { apply(count, &op) })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    /// A counter entity that takes a checkpoint after two operations.
    fn bounded_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .max_ops_per_run(2)
                .run(|count, op: CounterOp| async move { apply(count, &op) })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    // Only `handlers_run_one_at_a_time` may use these counters.
    static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
    static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);

    /// Counts running handlers. The harness drops a suspended cycle, so the
    /// count goes down on drop, not at the end of the handler.
    struct InFlight;

    impl InFlight {
        fn enter() -> Self {
            let now = IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
            MAX_IN_FLIGHT.fetch_max(now, Ordering::SeqCst);
            Self
        }
    }

    impl Drop for InFlight {
        fn drop(&mut self) {
            IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// A counter entity whose handler awaits one activity for each op.
    fn activity_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .run(|count, op: CounterOp| async move {
                    let _guard = InFlight::enter();
                    let n = ctx
                        .execute_activity_raw("entity_step", json!(op), "default")
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(count + n.as_i64().unwrap_or_default())
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    /// A loop that calls `should_continue_as_new()` directly. It shows the
    /// replay hazard that the entity avoids.
    fn naive_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let mut count = input.as_i64().unwrap_or_default();
            loop {
                let raw = ctx
                    .wait_for_signal(ENTITY_OP_SIGNAL)
                    .await
                    .map_err(|e| e.to_string())?;
                let add: i64 = ctx
                    .side_effect("naive.add", || raw.as_i64().unwrap_or_default())
                    .map_err(|e| e.to_string())?;
                count += add;
                if ctx.should_continue_as_new() {
                    ctx.continue_as_new(json!(count))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            }
        })
    }

    fn op(n: i64) -> Value {
        json!(EntityMessage::op(CounterOp::Add(n)))
    }

    fn delete() -> Value {
        json!(EntityMessage::<CounterOp>::delete())
    }

    fn env_with(messages: &[Value]) -> WorkflowTestEnv {
        messages.iter().fold(WorkflowTestEnv::new(), |env, m| {
            env.queue_signal(ENTITY_OP_SIGNAL, m.clone())
        })
    }

    fn checkpoint_side_effects(events: &[WorkflowEvent]) -> Vec<Value> {
        events
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::SideEffectRecorded { name, value, .. }
                    if name.as_deref() == Some(ENTITY_CHECKPOINT_SIDE_EFFECT) =>
                {
                    Some(value.clone())
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn message_wire_form_is_tagged() {
        assert_eq!(op(3), json!({"kind": "op", "op": {"add": 3}}));
        assert_eq!(delete(), json!({"kind": "delete"}));
        let back: EntityMessage<CounterOp> =
            serde_json::from_value(json!({"kind": "delete"})).unwrap();
        assert_eq!(back, EntityMessage::Delete);
    }

    #[test]
    fn checkpoint_decodes_from_an_empty_object() {
        let cp: EntityCheckpoint<i64> = serde_json::from_value(json!({})).unwrap();
        assert_eq!(cp, EntityCheckpoint::default());
        let cp: EntityCheckpoint<i64> = serde_json::from_value(json!({"state": 7})).unwrap();
        assert_eq!(cp, EntityCheckpoint::with_state(7));
    }

    #[test]
    fn checkpoint_decodes_null_as_the_default() {
        let cp: EntityCheckpoint<i64> = serde_json::from_value(Value::Null).unwrap();
        assert_eq!(cp, EntityCheckpoint::default());
        let cp: EntityCheckpoint<Vec<i64>> =
            serde_json::from_value(json!({"state": null, "pending": null})).unwrap();
        assert_eq!(cp, EntityCheckpoint::default());
    }

    /// The budget is the cap, unless every size is accepted.
    #[test]
    fn the_checkpoint_budget_follows_the_cap_and_the_offload_threshold() {
        let budget = |cap: u64, threshold: Option<u64>| {
            WorkflowContext::for_replay(ExecutionId::new(), vec![])
                .with_payload_caps(cap, cap, cap, cap)
                .with_payload_offload_threshold(threshold)
                .continue_as_new_input_budget()
        };
        assert_eq!(budget(0, None), None, "no cap");
        assert_eq!(budget(100, None), Some(100), "no offload");
        assert_eq!(budget(100, Some(50)), None, "offload below the cap");
        assert_eq!(budget(100, Some(100)), None, "offload at the cap");
        assert_eq!(
            budget(100, Some(200)),
            Some(100),
            "offload above the cap leaves a rejected band"
        );
    }

    #[test]
    fn max_ops_per_run_of_zero_counts_as_one() {
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), vec![]);
        let entity = Entity::new(&ctx, EntityCheckpoint::<i64>::default()).max_ops_per_run(0);
        assert_eq!(entity.max_ops_per_run, Some(1));
    }

    #[tokio::test]
    async fn ops_apply_in_order_and_delete_returns_the_final_state() {
        let outcome = env_with(&[op(1), op(2), op(4), delete()])
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(7)));
        assert_eq!(
            checkpoint_side_effects(outcome.events()),
            vec![json!(false); 3],
            "the loop records one checkpoint decision for each op that does not end the run"
        );
        let report = outcome.replay_check(counter).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    #[tokio::test]
    async fn handlers_run_one_at_a_time() {
        IN_FLIGHT.store(0, Ordering::SeqCst);
        MAX_IN_FLIGHT.store(0, Ordering::SeqCst);
        let outcome = env_with(&[op(1), op(2), op(3), delete()])
            .mock_activity("entity_step", |input| {
                Ok(json!(
                    input.get("add").and_then(Value::as_i64).unwrap_or(0) * 10
                ))
            })
            .run(activity_counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(60)));
        assert_eq!(MAX_IN_FLIGHT.load(Ordering::SeqCst), 1);

        // Each activity completes before the next one is scheduled.
        let mut open = false;
        let mut scheduled = Vec::new();
        for event in outcome.events() {
            match event {
                WorkflowEvent::ActivityScheduled { input, .. } => {
                    assert!(!open, "an op started while another op ran");
                    open = true;
                    scheduled.push(input.clone());
                }
                WorkflowEvent::ActivityCompleted { .. } => open = false,
                _ => {}
            }
        }
        assert_eq!(
            scheduled,
            vec![json!({"add": 1}), json!({"add": 2}), json!({"add": 3})],
            "ops run in arrival order"
        );
    }

    #[tokio::test]
    async fn a_failed_handler_keeps_the_old_state() {
        let fail = json!(EntityMessage::op(CounterOp::Fail));
        let outcome = env_with(&[op(5), fail, op(1), delete()])
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(6)));
    }

    #[tokio::test]
    async fn a_message_that_does_not_decode_is_a_failed_op() {
        let outcome = env_with(&[op(5), json!({"kind": "op", "op": "nonsense"}), delete()])
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(5)));
    }

    #[tokio::test]
    async fn delete_with_queued_ops_resets_the_state_and_goes_on() {
        let outcome = env_with(&[op(5), delete(), op(1), delete()])
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(1)));
    }

    #[tokio::test]
    async fn a_checkpoint_carries_the_ops_that_wait() {
        let first = env_with(&[op(1), op(2), op(4), op(8), delete()])
            .run(bounded_counter, json!({}))
            .await;
        let carried: EntityCheckpoint<i64> =
            serde_json::from_value(first.result.clone().expect("checkpoint")).unwrap();
        assert!(
            first
                .events()
                .iter()
                .any(|e| matches!(e, WorkflowEvent::WorkflowContinuedAsNew { .. })),
            "two ops end the run with continue-as-new"
        );
        assert_eq!(carried.state, 3);
        assert_eq!(carried.pending, vec![op(4), op(8), delete()]);
        assert_eq!(carried.stats.applied, 2);
        assert_eq!(carried.stats.checkpoints, 1);

        // The next run takes the carried ops first and ends at the delete.
        let second = WorkflowTestEnv::new()
            .run(bounded_counter, json!(carried))
            .await;
        let carried_again: EntityCheckpoint<i64> =
            serde_json::from_value(second.result.clone().expect("checkpoint")).unwrap();
        assert_eq!(carried_again.state, 15);
        assert_eq!(carried_again.pending, vec![delete()]);

        let third = WorkflowTestEnv::new()
            .run(bounded_counter, json!(carried_again))
            .await;
        assert_eq!(third.result, Ok(json!(15)));
    }

    #[tokio::test]
    async fn stats_count_applied_and_failed_ops() {
        let fail = json!(EntityMessage::op(CounterOp::Fail));
        let first = env_with(&[op(1), fail])
            .run(bounded_counter, json!({}))
            .await;
        let carried: EntityCheckpoint<i64> =
            serde_json::from_value(first.result.expect("checkpoint")).unwrap();
        assert_eq!(carried.stats.applied, 1);
        assert_eq!(carried.stats.failed, 1);
        assert_eq!(carried.stats.last_error.as_deref(), Some("op refused"));
    }

    /// A recorded `true` decision replays to the same checkpoint, with the
    /// waiting ops in its input.
    #[tokio::test]
    async fn a_recorded_checkpoint_decision_replays() {
        let stats = EntityStats {
            applied: 1,
            checkpoints: 1,
            ..EntityStats::default()
        };
        let carried = EntityCheckpoint {
            state: 1_i64,
            pending: vec![op(2)],
            stats,
        };
        let events = vec![
            started(json!({})),
            signal(op(1)),
            signal(op(2)),
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                name: Some(ENTITY_CHECKPOINT_SIDE_EFFECT.to_string()),
                value: json!(true),
            },
            WorkflowEvent::SideEffectRecorded {
                kind: SideEffectKind::Custom,
                name: Some(ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT.to_string()),
                value: json!(2_097_152),
            },
            WorkflowEvent::WorkflowContinuedAsNew {
                new_exec_id: ExecutionId::new(),
                input: json!(carried),
                new_workflow_type: None,
            },
        ];
        let report = WorkflowReplayer::new()
            .register_fn("counter", counter)
            .replay_from_events(events)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    /// A replay with a larger loaded history keeps the recorded decisions.
    /// A naive loop over `should_continue_as_new()` diverges on the same
    /// history.
    #[tokio::test]
    async fn the_checkpoint_decision_is_stable_when_history_grows() {
        let small_threshold = WorkflowHistoryPolicy::default().with_continue_as_new_threshold(2);

        let outcome = env_with(&[op(1), op(2), op(3), delete()])
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(6)));
        let report = WorkflowReplayer::new()
            .with_history_policy(small_threshold)
            .register_fn("counter", counter)
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );

        let added = |n: i64| WorkflowEvent::SideEffectRecorded {
            kind: SideEffectKind::Custom,
            name: Some("naive.add".to_string()),
            value: json!(n),
        };
        let naive = vec![
            started(json!(0)),
            signal(json!(1)),
            added(1),
            signal(json!(2)),
            added(2),
        ];
        let report = WorkflowReplayer::new()
            .with_history_policy(small_threshold)
            .register_fn("naive_counter", naive_counter)
            .replay_from_events(naive)
            .await;
        assert!(
            matches!(report.status, ReplayStatus::NonDeterminismDetected { .. }),
            "a naive loop must diverge here: {report}"
        );
    }

    #[tokio::test]
    async fn queries_read_the_committed_state_and_stats() {
        let fail = json!(EntityMessage::op(CounterOp::Fail));
        let outcome = env_with(&[op(2), fail, op(3)])
            .run(counter, json!({}))
            .await;
        let waiting = outcome.result.clone().unwrap_err();
        assert!(
            waiting.contains("suspended with no resolvable commands"),
            "the entity waits for more ops: {waiting}"
        );

        let ctx = WorkflowContext::for_replay(ExecutionId::new(), outcome.events().to_vec());
        let drive = crate::executor::drive_query_replay(
            &ctx,
            counter,
            json!({}),
            std::time::Duration::from_secs(5),
        );
        assert!(
            matches!(drive, crate::executor::QueryReplayOutcome::Suspended),
            "{drive:?}"
        );
        assert_eq!(ctx.execute_query(ENTITY_STATE_QUERY).unwrap(), json!(5));
        let stats: EntityStats =
            serde_json::from_value(ctx.execute_query(ENTITY_STATS_QUERY).unwrap()).unwrap();
        assert_eq!(stats.applied, 2);
        assert_eq!(stats.failed, 1);
    }

    /// An op message with `pad_bytes` of padding. The decoder ignores the
    /// pad, so the op still applies.
    fn padded(n: i64, pad_bytes: usize) -> Value {
        json!({"kind": "op", "op": {"add": n}, "pad": "x".repeat(pad_bytes)})
    }

    /// Waiting ops that overflow the input cap run in this run. The
    /// checkpoint carries only what fits.
    #[tokio::test]
    async fn a_checkpoint_carries_only_what_fits_the_input_cap() {
        let big = 600 * 1024;
        let outcome = env_with(&[
            op(1),
            padded(2, big),
            padded(4, big),
            padded(8, big),
            padded(16, big),
        ])
        .run(one_op_counter, json!({}))
        .await;
        let input = outcome.result.clone().expect("checkpoint");
        let size = serde_json::to_string(&input).unwrap().len() as u64;
        assert!(
            size <= crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
            "the checkpoint input fits the cap: {size} bytes"
        );
        let carried: EntityCheckpoint<i64> = serde_json::from_value(input).unwrap();
        assert_eq!(carried.state, 3, "one padded op ran before the checkpoint");
        assert_eq!(carried.pending.len(), 3);
        assert_eq!(carried.stats.checkpoints, 1);

        let report = outcome.replay_check(one_op_counter).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    /// A counter entity that asks for a checkpoint after each op.
    fn one_op_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .max_ops_per_run(1)
                .run(|count, op: CounterOp| async move { apply(count, &op) })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    #[tokio::test]
    async fn a_cancelled_entity_stops_with_an_error() {
        let outcome = env_with(&[op(1)])
            .with_cancellation("stop")
            .run(counter, json!({}))
            .await;
        let error = outcome.result.unwrap_err();
        assert!(error.to_lowercase().contains("cancel"), "{error}");
    }

    /// With an execution timeout, each decision also records a deadline
    /// probe. The probe and the decision replay in the same order.
    #[tokio::test]
    async fn the_deadline_probe_and_the_decision_replay_together() {
        let outcome = env_with(&[op(1), op(2), delete()])
            .with_execution_timeout(chrono::Duration::hours(1))
            .run(counter, json!({}))
            .await;
        assert_eq!(outcome.result, Ok(json!(3)));
        let report = outcome.replay_check(counter).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    #[tokio::test]
    async fn a_start_input_that_does_not_decode_fails_the_run() {
        let outcome = WorkflowTestEnv::new()
            .run(counter, json!({"state": "x"}))
            .await;
        assert!(outcome.result.is_err());
    }

    #[tokio::test]
    async fn a_bad_carried_message_counts_as_failed() {
        let input = json!({"pending": [{"kind": "bogus"}, op(2), delete()]});
        let first = WorkflowTestEnv::new().run(bounded_counter, input).await;
        let carried: EntityCheckpoint<i64> =
            serde_json::from_value(first.result.expect("checkpoint")).unwrap();
        assert_eq!(carried.state, 2);
        assert_eq!(carried.stats.failed, 1);
        assert_eq!(carried.pending, vec![delete()]);
    }

    fn started(input: Value) -> WorkflowEvent {
        WorkflowEvent::WorkflowStarted {
            input,
            timestamp: chrono::Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }
    }

    fn signal(payload: Value) -> WorkflowEvent {
        WorkflowEvent::SignalReceived {
            signal_name: ENTITY_OP_SIGNAL.to_string(),
            payload,
        }
    }
}
