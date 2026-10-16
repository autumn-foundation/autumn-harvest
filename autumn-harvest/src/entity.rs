//! Keyed entity: serialized handlers over durable state for each key (issue #1975).
//!
//! An entity is sugar over a workflow. It adds no event variant and no
//! migration. See `docs/adr/0006-keyed-entity.md`.
//!
//! | Entity concept | Harvest part |
//! |---|---|
//! | Entity type | A root `#[workflow]` function that calls [`Entity::run`](crate::entity::Entity::run) |
//! | Entity key | The `workflow_id` |
//! | Operation | A signal named [`ENTITY_OP_SIGNAL`](crate::entity::ENTITY_OP_SIGNAL) |
//! | Send an operation | `signal_with_start` |
//! | Read state | The query [`ENTITY_STATE_QUERY`](crate::entity::ENTITY_STATE_QUERY) |
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
//!   done. The copy is `S::clone`, so `clone` must copy the whole value. A
//!   state that shares interior mutability, such as an `Arc<Mutex<_>>`,
//!   loses this guarantee.
//!
//! # Limits
//!
//! - The checkpoint input is the whole state. A state larger than the
//!   workflow input cap fails the run at its first checkpoint. Payload
//!   offload lifts this limit only when its threshold is at or below the cap.
//! - Continue-as-new works only in a root workflow. An entity cannot be a
//!   child workflow.
//! - [`Entity::max_ops_per_run`](crate::entity::Entity::max_ops_per_run), an `execution_timeout` and the
//!   [`EntityCheckpoint`](crate::entity::EntityCheckpoint) wire form are part of replay. Change them as a
//!   versioned workflow change.
//! - A handler must await all the work it starts, and its error text must be
//!   deterministic. The error text rides in the checkpoint input. The
//!   checkpoint keeps at most
//!   [`ENTITY_LAST_ERROR_MAX_BYTES`](crate::entity::ENTITY_LAST_ERROR_MAX_BYTES)
//!   bytes of the error text.
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
/// The budget is the configured workflow input cap. Each checkpoint attempt
/// records it. Replay of an earlier attempt reads the recorded budget, not
/// the current cap. A later attempt reads the current cap.
pub const ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT: &str = "harvest.entity.checkpoint_budget";

/// The maximum size in bytes of [`EntityStats::last_error`], marker included.
///
/// A longer error is cut at a `char` boundary and ends with `... [truncated]`.
/// The error rides in each checkpoint input. Without a bound, a huge error
/// stops later checkpoints. A decode error can also quote part of a bad
/// payload. Change this bound as a versioned workflow change.
pub const ENTITY_LAST_ERROR_MAX_BYTES: usize = 1024;

/// The text that ends a cut error.
const TRUNCATION_MARKER: &str = "... [truncated]";

const _: () = assert!(ENTITY_LAST_ERROR_MAX_BYTES > TRUNCATION_MARKER.len());

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
    ///
    /// It keeps at most [`ENTITY_LAST_ERROR_MAX_BYTES`] bytes. A longer error
    /// is cut and ends with `... [truncated]`.
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
    /// The copy is `S::clone`. Use plain data for `S`. A clone that shares
    /// state, such as an `Arc<Mutex<_>>`, lets a failed handler change the
    /// committed state.
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
        // The start input is trusted, but its error must obey the bound too.
        checkpoint.stats.last_error = checkpoint.stats.last_error.take().map(bound_error);
        let mut waiting: VecDeque<Value> = std::mem::take(&mut checkpoint.pending).into();
        let published = Arc::new(Mutex::new(Published::of(&checkpoint)));
        register_queries(ctx, &published);

        let mut run = RunCounters::default();
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

            if checkpoint_due(ctx, &mut run, max_ops_per_run)? {
                // Record the budget at each attempt. A cap that changes while a
                // handler waits then applies to the next attempt.
                let limit = ctx.side_effect(ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT, || {
                    ctx.continue_as_new_input_budget()
                })?;
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
    /// The loaded history size, as `(event count, bytes)`. A new task loads
    /// more events, so a new count means a new measure.
    loaded: Option<(u64, u64)>,
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
    // bytes and at most one separator, so the sum is an upper bound. The
    // checkpoint counter goes up by one before the write, which can add a
    // digit.
    let mut size = json_len(checkpoint)? + counter_growth(checkpoint.stats.checkpoints);
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

/// The bytes that `n + 1` adds over `n` in decimal JSON.
fn counter_growth(n: u64) -> u64 {
    let digits = |v: u64| u64::from(v.checked_ilog10().unwrap_or(0) + 1);
    n.checked_add(1).map_or(1, |next| digits(next) - digits(n))
}

fn json_len<T: Serialize + ?Sized>(value: &T) -> HarvestResult<u64> {
    Ok(serde_json::to_string(value)?.len() as u64)
}

/// Cut `error` to [`ENTITY_LAST_ERROR_MAX_BYTES`], marker included.
///
/// The cut lands on a `char` boundary. The loop below replaces
/// `str::floor_char_boundary`, which the MSRV lacks.
fn bound_error(error: String) -> String {
    if error.len() <= ENTITY_LAST_ERROR_MAX_BYTES {
        return error;
    }
    let mut end = ENTITY_LAST_ERROR_MAX_BYTES - TRUNCATION_MARKER.len();
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    // `truncate` would keep the buffer of the long error. A new string drops it.
    let mut bounded = String::with_capacity(end + TRUNCATION_MARKER.len());
    bounded.push_str(&error[..end]);
    bounded.push_str(TRUNCATION_MARKER);
    bounded
}

fn record_failure(stats: &mut EntityStats, error: String) {
    stats.failed += 1;
    stats.last_error = Some(bound_error(error));
}

/// Decide if the run takes a checkpoint after this op.
///
/// The op bound is a pure function of the run, so it needs no event. The
/// other checks read live values, and the side effect records the answer.
/// Replay reads that answer back.
///
/// The loaded history count does not include the events of the current
/// task. The run therefore also counts one event for each op it took, so a
/// long backlog in one task still reaches the threshold.
///
/// The byte check adds the loaded history bytes and this run's op bytes. The
/// loaded bytes include the activity results of earlier tasks. The check
/// trips at half the history byte cap, which leaves room for estimate error.
fn checkpoint_due(
    ctx: &WorkflowContext,
    run: &mut RunCounters,
    max_ops_per_run: Option<u64>,
) -> HarvestResult<bool> {
    if max_ops_per_run.is_some_and(|max| run.ops >= max) {
        return Ok(true);
    }
    let policy = ctx.history_policy();
    let by_engine = ctx.should_continue_as_new();
    let events = ctx.history_event_count();
    let by_events = events.saturating_add(run.ops) > policy.continue_as_new_threshold();
    let by_bytes = match policy.byte_hard_cap() {
        Some(cap) => {
            let loaded = match run.loaded {
                Some((count, bytes)) if count == events => bytes,
                _ => {
                    let bytes = ctx.loaded_history_bytes();
                    run.loaded = Some((events, bytes));
                    bytes
                }
            };
            over_byte_budget(loaded, run.op_bytes, cap)
        }
        None => false,
    };
    let live = by_engine || by_events || by_bytes;
    ctx.side_effect(ENTITY_CHECKPOINT_SIDE_EFFECT, || live)
}

/// Whether the history bytes reach half of `cap`.
const fn over_byte_budget(loaded: u64, op_bytes: u64, cap: u64) -> bool {
    loaded.saturating_add(op_bytes) > cap / 2
}

/// Kani proofs of the entity byte budget (issue #1975).
///
/// The `kani` CI job runs them. See `docs/testing/formal-methods.md`.
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// A counter grows by 0 or 1 bytes, and by 1 only when `n + 1` gains a
    /// digit. The subtraction never underflows.
    #[kani::proof]
    fn counter_growth_is_one_byte_at_a_power_of_ten() {
        let n: u64 = kani::any();
        let growth = counter_growth(n);
        assert!(growth <= 1);
        if n < u64::MAX {
            // A table, not `ilog10`, so the harness adds no symbolic divide.
            let powers: [u64; 19] = core::array::from_fn(|i| 10_u64.pow(i as u32 + 1));
            assert_eq!(growth == 1, powers.contains(&(n + 1)));
        }
        kani::cover!(growth == 1);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;
    use crate::context::{WorkflowCommand, WorkflowHistoryPolicy};
    use crate::event::{SideEffectKind, WorkflowEvent};
    use crate::executor::{WorkflowOutcome, drive_workflow};
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

    /// A loop that calls `should_continue_as_new()` directly.
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
                if add == 0 {
                    return Ok(json!(count));
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

    /// The values of the side effects recorded under `id`.
    fn recorded(events: &[WorkflowEvent], id: &str) -> Vec<Value> {
        events
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::SideEffectRecorded { name, value, .. }
                    if name.as_deref() == Some(id) =>
                {
                    Some(value.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn checkpoint_side_effects(events: &[WorkflowEvent]) -> Vec<Value> {
        recorded(events, ENTITY_CHECKPOINT_SIDE_EFFECT)
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
    fn the_byte_budget_is_half_the_cap() {
        assert!(!over_byte_budget(40, 10, 100));
        assert!(over_byte_budget(40, 11, 100));
        assert!(over_byte_budget(u64::MAX, 1, 100));
    }

    /// Large activity results fill the history while the ops stay tiny. The
    /// loaded history bytes still force a checkpoint before the byte cap.
    #[tokio::test]
    async fn large_activity_results_force_a_checkpoint() {
        let messages: Vec<Value> = (0..40).map(|_| op(1)).collect();
        let outcome = env_with(&messages)
            .mock_activity("entity_step", |_| Ok(json!("x".repeat(1 << 20))))
            .run(blob_counter, json!({}))
            .await;
        let carried: EntityCheckpoint<i64> =
            serde_json::from_value(outcome.result.expect("checkpoint")).unwrap();
        let half_cap = crate::context::DEFAULT_HISTORY_BYTE_HARD_CAP / 2;
        let applied = i64::try_from(half_cap >> 20).unwrap();
        assert!(
            carried.state > 0 && carried.state <= applied + 1,
            "the run checkpoints near half the byte cap, after {} ops",
            carried.state
        );
        assert_eq!(carried.stats.checkpoints, 1);
    }

    /// A counter entity whose handler stores a large activity result.
    fn blob_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .run(|count, _: CounterOp| async move {
                    ctx.execute_activity_raw("entity_step", json!(null), "default")
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(count + 1)
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    #[test]
    fn the_counter_growth_counts_a_new_digit() {
        assert_eq!(counter_growth(0), 0);
        assert_eq!(counter_growth(8), 0);
        assert_eq!(counter_growth(9), 1);
        assert_eq!(counter_growth(10), 0);
        assert_eq!(counter_growth(99), 1);
        assert_eq!(counter_growth(u64::MAX), 1);
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
        assert!(
            recorded(outcome.events(), ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT).is_empty(),
            "a run with no checkpoint attempt records no budget"
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

    /// An error above the input cap.
    const HUGE_ERROR_BYTES: usize = 3 << 20;

    /// A counter entity that asks for a checkpoint after each op. Each op
    /// fails with a huge error.
    fn huge_error_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .max_ops_per_run(1)
                .run(|_: i64, _: CounterOp| async move { Err("x".repeat(HUGE_ERROR_BYTES)) })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    /// A huge error does not block the checkpoint. The input fits the cap
    /// and replay succeeds.
    #[tokio::test]
    async fn a_huge_error_does_not_block_the_checkpoint() {
        let outcome = env_with(&[op(1)]).run(huge_error_counter, json!({})).await;
        let input = outcome
            .result
            .clone()
            .expect("the checkpoint fits the input cap");
        let carried: EntityCheckpoint<i64> = serde_json::from_value(input).unwrap();
        assert_eq!(carried.stats.failed, 1);
        let error = carried.stats.last_error.expect("the failure is recorded");
        assert!(
            error.len() <= ENTITY_LAST_ERROR_MAX_BYTES,
            "the error keeps {} bytes",
            error.len()
        );
        assert!(
            error.ends_with(TRUNCATION_MARKER),
            "the cut leaves a marker"
        );

        let report = outcome.replay_check(huge_error_counter).await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
    }

    /// A decode error can quote the payload. The bound cuts the quote.
    #[tokio::test]
    async fn a_decode_error_that_quotes_the_payload_is_bounded() {
        let bad = json!({"kind": "z".repeat(64 * 1024)});
        let outcome = env_with(&[bad]).run(one_op_counter, json!({})).await;
        let input = outcome.result.expect("checkpoint");
        let carried: EntityCheckpoint<i64> = serde_json::from_value(input).unwrap();
        let error = carried.stats.last_error.expect("the failure is recorded");
        assert!(error.starts_with("entity message does not decode: "));
        assert!(error.contains("zzz"), "the error quotes the payload");
        assert!(
            error.len() <= ENTITY_LAST_ERROR_MAX_BYTES,
            "the error keeps {} bytes",
            error.len()
        );
    }

    /// The start input is trusted, but its `last_error` obeys the bound too.
    #[tokio::test]
    async fn a_start_input_error_above_the_bound_is_cut() {
        let input = json!({"stats": {"last_error": "x".repeat(HUGE_ERROR_BYTES)}});
        let outcome = env_with(&[op(1)]).run(one_op_counter, input).await;
        let checkpoint = outcome.result.expect("the checkpoint fits the input cap");
        let carried: EntityCheckpoint<i64> = serde_json::from_value(checkpoint).unwrap();
        let error = carried.stats.last_error.expect("the start input set it");
        assert!(
            error.len() <= ENTITY_LAST_ERROR_MAX_BYTES,
            "the error keeps {} bytes",
            error.len()
        );
        assert!(
            error.ends_with(TRUNCATION_MARKER),
            "the cut leaves a marker"
        );
    }

    #[test]
    fn an_error_within_the_bound_stays_whole() {
        let exact = "e".repeat(ENTITY_LAST_ERROR_MAX_BYTES);
        assert_eq!(bound_error(exact.clone()), exact);
        assert_eq!(bound_error(String::new()), "");
    }

    #[test]
    fn the_bound_and_the_marker_are_wire_values() {
        assert_eq!(ENTITY_LAST_ERROR_MAX_BYTES, 1024);
        assert_eq!(TRUNCATION_MARKER, "... [truncated]");
    }

    /// The bound applies when the loop records the failure, so the stats
    /// query and the checkpoint carry the same text.
    #[test]
    fn a_recorded_failure_is_counted_and_bounded() {
        let mut stats = EntityStats::default();
        record_failure(&mut stats, "x".repeat(HUGE_ERROR_BYTES));
        assert_eq!(stats.failed, 1);
        let error = stats.last_error.expect("the failure is recorded");
        assert!(error.len() <= ENTITY_LAST_ERROR_MAX_BYTES);
        assert!(error.ends_with(TRUNCATION_MARKER));
    }

    /// The cut keeps the head of the error and frees the long buffer.
    #[test]
    fn a_longer_error_is_cut_to_the_bound_with_a_marker() {
        let kept = ENTITY_LAST_ERROR_MAX_BYTES - TRUNCATION_MARKER.len();
        let expected = format!("HEAD{}{TRUNCATION_MARKER}", "e".repeat(kept - 4));
        for len in [
            ENTITY_LAST_ERROR_MAX_BYTES + 1,
            ENTITY_LAST_ERROR_MAX_BYTES * 2,
            HUGE_ERROR_BYTES,
        ] {
            let cut = bound_error(format!("HEAD{}TAIL", "e".repeat(len - 8)));
            assert_eq!(cut, expected, "{len} bytes in");
            assert!(
                cut.capacity() <= 2 * ENTITY_LAST_ERROR_MAX_BYTES,
                "the buffer of the long error is gone"
            );
        }
    }

    /// Characters of 2, 3 and 4 bytes at four offsets. Most cuts land inside
    /// a character.
    #[test]
    fn the_cut_lands_on_a_char_boundary() {
        let kept = ENTITY_LAST_ERROR_MAX_BYTES - TRUNCATION_MARKER.len();
        for ch in ['é', '€', '😀'] {
            for lead in 0..4 {
                let wide = ch.to_string().repeat(ENTITY_LAST_ERROR_MAX_BYTES);
                let error = format!("{}{wide}", "a".repeat(lead));
                let cut = bound_error(error);
                let body = cut.strip_suffix(TRUNCATION_MARKER).expect("a marker");
                assert!(
                    body.chars().skip(lead).all(|c| c == ch),
                    "{ch} after {lead}"
                );
                assert!(cut.len() <= ENTITY_LAST_ERROR_MAX_BYTES);
                assert!(
                    kept - body.len() < ch.len_utf8(),
                    "the cut gives up less than one {ch}"
                );
            }
        }
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
    /// A naive loop over `should_continue_as_new()` replays clean on the
    /// same history (issue #2080).
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

        // The run finishes, so a call never sits at the end of its history.
        let outcome = env_with(&[json!(1), json!(2), json!(3), json!(0)])
            .run(naive_counter, json!(0))
            .await;
        assert_eq!(outcome.result, Ok(json!(6)));
        let report = WorkflowReplayer::new()
            .with_history_policy(small_threshold)
            .register_fn("naive_counter", naive_counter)
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "a naive loop must replay clean: {report}"
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

    /// One small op, then four ops of 600 KiB. The first checkpoint cannot
    /// carry all four under the default 2 MiB input cap.
    fn overflowing_ops() -> Vec<Value> {
        let big = 600 * 1024;
        vec![
            op(1),
            padded(2, big),
            padded(4, big),
            padded(8, big),
            padded(16, big),
        ]
    }

    /// Waiting ops that overflow the input cap run in this run. The
    /// checkpoint carries only what fits.
    #[tokio::test]
    async fn a_checkpoint_carries_only_what_fits_the_input_cap() {
        let outcome = env_with(&overflowing_ops())
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

    /// A counter entity that asks for a checkpoint after each op. Its
    /// handler awaits one activity.
    fn activity_one_op_counter(ctx: &WorkflowContext, input: Value) -> BoxedRun<'_> {
        Box::pin(async move {
            let state = Entity::new(ctx, checkpoint_of(input)?)
                .max_ops_per_run(1)
                .run(|count, op: CounterOp| async move {
                    ctx.execute_activity_raw("entity_step", json!(op), "default")
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(count + 1)
                })
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!(state))
        })
    }

    /// Each attempt records its budget. The first attempt here does not fit,
    /// so the run makes a second one.
    #[tokio::test]
    async fn each_checkpoint_attempt_records_its_budget() {
        let outcome = env_with(&overflowing_ops())
            .run(one_op_counter, json!({}))
            .await;
        assert!(outcome.result.is_ok(), "{:?}", outcome.result);
        let cap = json!(crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES);
        assert_eq!(
            recorded(outcome.events(), ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT),
            vec![cap.clone(), cap],
            "one record for each attempt"
        );
    }

    /// A deploy lowers the input cap while a handler waits on an activity.
    /// Replay restores the budget of the first attempt. The next attempt must
    /// size its input under the live cap, not under that budget.
    #[tokio::test]
    async fn a_lowered_cap_applies_to_the_next_attempt() {
        let outcome = env_with(&overflowing_ops())
            .mock_activity("entity_step", |_| Ok(json!(1)))
            .run(activity_one_op_counter, json!({}))
            .await;
        // End the history after the activity of the first op that follows the
        // first attempt.
        let cut = outcome
            .events()
            .iter()
            .enumerate()
            .filter(|(_, e)| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
            .nth(1)
            .map(|(index, _)| index + 1)
            .expect("the op after the first attempt ran an activity");
        let history = outcome.events()[..cut].to_vec();
        assert_eq!(
            recorded(&history, ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT).len(),
            1,
            "the first attempt is in the history"
        );

        // Three ops of 600 KiB fit the default cap but not half of it.
        let lowered = crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES / 2;
        let activity = crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES;
        let signal = crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES;
        let ctx = WorkflowContext::for_replay(ExecutionId::new(), history)
            .with_payload_caps(activity, activity, signal, lowered);
        let (cycle, ..) = drive_workflow(ctx, activity_one_op_counter, json!({}), None).await;
        let WorkflowOutcome::Suspended { commands } = cycle else {
            panic!("the run must go on, not fail: {cycle:?}");
        };
        let budgets: Vec<&Value> = commands
            .iter()
            .filter_map(|c| match c {
                WorkflowCommand::RecordSideEffect { name, value, .. }
                    if name.as_deref() == Some(ENTITY_CHECKPOINT_BUDGET_SIDE_EFFECT) =>
                {
                    Some(value)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            budgets,
            [&json!(lowered)],
            "the new attempt records the live budget"
        );
        let next_op = commands.iter().any(|c| {
            matches!(
                c,
                WorkflowCommand::ScheduleActivity { name, input, .. }
                    if name == "entity_step" && *input == json!({"add": 4})
            )
        });
        assert!(
            next_op,
            "the carried ops do not fit, so the loop runs the next op first"
        );
    }

    /// Replay reads the recorded budgets. A full history from the default cap
    /// replays under a lowered cap.
    #[tokio::test]
    async fn a_full_history_replays_under_a_lowered_cap() {
        let outcome = env_with(&overflowing_ops())
            .run(one_op_counter, json!({}))
            .await;
        let report = WorkflowReplayer::new()
            .with_payload_caps(
                crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
                crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
                crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES / 2,
            )
            .register_fn("one_op_counter", one_op_counter)
            .replay_from_events(outcome.events().to_vec())
            .await;
        assert!(
            matches!(report.status, ReplayStatus::ReplaySucceeded),
            "{report}"
        );
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
