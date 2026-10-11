//! Durable promises (issue #1985).
//!
//! A durable promise is a handle that a workflow creates and any caller
//! settles once, like a Restate awakeable. The workflow hands the token to an
//! external system, then waits:
//!
//! ```rust,no_run
//! # async fn example(ctx: &autumn_harvest::WorkflowContext) -> autumn_harvest::HarvestResult<()> {
//! let mut promise = ctx.new_promise()?;
//! let token = promise.id().to_string();
//! // Send `token` to the system that settles the promise.
//! # let _ = token;
//! match promise.wait::<serde_json::Value>().await? {
//!     Ok(value) => println!("resolved: {value}"),
//!     Err(rejected) => println!("rejected: {}", rejected.error),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Transport
//!
//! A promise is a signal. Its name is `harvest.promise:<key>`, and its
//! idempotency key is the same string. So a promise gets the signal
//! guarantees with no new table and no new event:
//!
//! - A settlement before the wait stays buffered.
//! - The first settlement wins. A later one is a no-op.
//! - Replay reads the recorded `SignalReceived` event.
//! - The token is recorded in a `SideEffectRecorded` event. Replay under a
//!   new execution id, for example in a reset fork, returns the same token.
//! - A promise can race a timer through [`DurablePromise::wait_timeout`] or
//!   `ctx.race().signal(..)`.
//!
//! # Settle a promise
//!
//! - `resolve` and `reject` (feature `db`), on a connection to the shard
//!   that holds the run.
//! - `ctx.resolve_promise` and `ctx.reject_promise`, from another workflow.
//! - `POST /workflows/{id}/signal/{signal_name}` with a [`PromiseSettlement`]
//!   body. Use the value from [`PromiseId::signal_name`].
//!
//! `signal::send_signal_idempotent` applies the settlement rules on every
//! path. A `harvest.promise:` signal always uses its name as its idempotency
//! key, and its payload must be a settlement.
//!
//! # Limits
//!
//! - A promise belongs to the run that created it.
//! - Continue-as-new moves an unconsumed settlement to the next run. A named
//!   promise ([`crate::WorkflowContext::promise`]) with the same key there
//!   receives it.
//! - A reset fork keeps the recorded token of a promise made before the
//!   reset point. That token names the source run. Settle the fork with
//!   `PromiseId::new(fork_execution_id, id.key())`.
//! - The token is not a secret. History shows it.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::context::WorkflowContext;
use crate::error::HarvestResult;
use crate::types::ExecutionId;

/// Prefix of the signal name that carries a promise settlement.
pub const PROMISE_SIGNAL_PREFIX: &str = "harvest.promise:";

/// Name of the side effect that records a promise token.
pub const PROMISE_SIDE_EFFECT_NAME: &str = "harvest.promise";

/// Maximum length of a promise key, in bytes.
pub const MAX_PROMISE_KEY_LEN: usize = 128;

/// Why a promise key or token is not valid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PromiseIdError {
    /// The key is empty.
    #[error("promise key is empty")]
    EmptyKey,
    /// The key is longer than [`MAX_PROMISE_KEY_LEN`] bytes.
    #[error("promise key is {0} bytes; the maximum is {MAX_PROMISE_KEY_LEN}")]
    KeyTooLong(usize),
    /// The key holds a character outside `[A-Za-z0-9._:-]`.
    #[error("promise key holds {0:?}; use only A-Z, a-z, 0-9, '.', '_', ':' and '-'")]
    InvalidKeyChar(char),
    /// The token has no `/` between the execution id and the key.
    #[error("promise token has no '/' separator")]
    MissingSeparator,
    /// The execution id part of the token is not a UUID.
    #[error("promise token has an invalid execution id: {0}")]
    InvalidExecutionId(String),
}

/// The address of one durable promise: a run and a key in that run.
///
/// The string form is `<execution-id>/<key>`. It is the token that a
/// workflow hands to the caller that settles the promise.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PromiseId {
    execution_id: ExecutionId,
    key: String,
}

impl PromiseId {
    /// Makes a promise address.
    ///
    /// # Errors
    ///
    /// Returns a [`PromiseIdError`] when `key` is empty, too long, or holds a
    /// character outside `[A-Za-z0-9._:-]`.
    pub fn new(execution_id: ExecutionId, key: impl Into<String>) -> Result<Self, PromiseIdError> {
        let key = key.into();
        validate_key(&key)?;
        Ok(Self { execution_id, key })
    }

    /// A promise address whose key is a UUID. A UUID is always a valid key.
    pub(crate) fn from_uuid(execution_id: ExecutionId, key: uuid::Uuid) -> Self {
        Self {
            execution_id,
            key: key.to_string(),
        }
    }

    /// The run that created the promise.
    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    /// The key of the promise in its run.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The signal name that carries the settlement.
    #[must_use]
    pub fn signal_name(&self) -> String {
        format!("{PROMISE_SIGNAL_PREFIX}{}", self.key)
    }

    /// The signal idempotency key. It makes the first settlement win.
    #[must_use]
    pub fn idempotency_key(&self) -> String {
        self.signal_name()
    }
}

fn validate_key(key: &str) -> Result<(), PromiseIdError> {
    if key.is_empty() {
        return Err(PromiseIdError::EmptyKey);
    }
    if key.len() > MAX_PROMISE_KEY_LEN {
        return Err(PromiseIdError::KeyTooLong(key.len()));
    }
    if let Some(bad) = key
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-')))
    {
        return Err(PromiseIdError::InvalidKeyChar(bad));
    }
    Ok(())
}

impl fmt::Display for PromiseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.execution_id, self.key)
    }
}

impl FromStr for PromiseId {
    type Err = PromiseIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (execution_id, key) = s.split_once('/').ok_or(PromiseIdError::MissingSeparator)?;
        let execution_id = execution_id
            .parse::<ExecutionId>()
            .map_err(|e| PromiseIdError::InvalidExecutionId(e.to_string()))?;
        Self::new(execution_id, key)
    }
}

impl Serialize for PromiseId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PromiseId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let token = String::deserialize(deserializer)?;
        token.parse().map_err(serde::de::Error::custom)
    }
}

/// The payload of a promise settlement signal.
///
/// The JSON form is `{"outcome":"resolved","value":…}` or
/// `{"outcome":"rejected","error":"…"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PromiseSettlement {
    /// The promise has a value.
    Resolved {
        /// The value that the waiting workflow receives.
        value: Value,
    },
    /// The promise failed.
    Rejected {
        /// Why the promise failed.
        error: String,
    },
}

impl PromiseSettlement {
    /// A settlement that resolves the promise with `value`.
    #[must_use]
    pub const fn resolved(value: Value) -> Self {
        Self::Resolved { value }
    }

    /// A settlement that rejects the promise with `error`.
    #[must_use]
    pub fn rejected(error: impl Into<String>) -> Self {
        Self::Rejected {
            error: error.into(),
        }
    }

    /// Decodes a settlement signal payload.
    ///
    /// Use it with the raw payload of a `ctx.race().signal(..)` branch.
    ///
    /// # Errors
    ///
    /// Returns [`crate::HarvestError::Serialization`] if `raw` is not a
    /// settlement or its value does not decode into `T`.
    pub fn decode<T: DeserializeOwned>(raw: Value) -> HarvestResult<Result<T, PromiseRejected>> {
        match serde_json::from_value::<Self>(raw)? {
            Self::Resolved { value } => Ok(Ok(serde_json::from_value(value)?)),
            Self::Rejected { error } => Ok(Err(PromiseRejected { error })),
        }
    }

    /// The signal payload for this settlement.
    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Resolved { value } => {
                serde_json::json!({ "outcome": "resolved", "value": value })
            }
            Self::Rejected { error } => {
                serde_json::json!({ "outcome": "rejected", "error": error })
            }
        }
    }
}

/// A promise that a caller rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("durable promise rejected: {error}")]
pub struct PromiseRejected {
    /// The reason that the caller gave.
    pub error: String,
}

/// A durable promise inside a workflow.
///
/// Make one with [`WorkflowContext::promise`] or
/// [`WorkflowContext::new_promise`].
#[must_use = "a promise records its token; wait on it or hand the token out"]
pub struct DurablePromise<'a> {
    context: &'a WorkflowContext,
    id: PromiseId,
    /// The settlement, after a wait takes it. A promise has one settlement,
    /// so a later wait reads it here. Another signal wait would park forever.
    settlement: Option<Value>,
}

impl fmt::Debug for DurablePromise<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurablePromise")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl<'a> DurablePromise<'a> {
    pub(crate) const fn new(context: &'a WorkflowContext, id: PromiseId) -> Self {
        Self {
            context,
            id,
            settlement: None,
        }
    }

    /// The address of this promise. Its string form is the token.
    #[must_use]
    pub const fn id(&self) -> &PromiseId {
        &self.id
    }

    /// Waits until a caller settles the promise.
    ///
    /// The outer error is an engine error, for example a replay drift. The
    /// inner error is a rejection. A wait takes `&mut self`, so the handle
    /// takes one wait at a time. Every later wait returns the same settlement.
    ///
    /// # Errors
    ///
    /// Returns [`crate::HarvestError::Serialization`] if the settlement or
    /// its value does not decode. Propagates all errors from
    /// [`WorkflowContext::wait_for_signal`].
    pub async fn wait<T: DeserializeOwned>(&mut self) -> HarvestResult<Result<T, PromiseRejected>> {
        if let Some(raw) = &self.settlement {
            return PromiseSettlement::decode(raw.clone());
        }
        let raw = self.context.wait_for_signal(&self.id.signal_name()).await?;
        PromiseSettlement::decode(self.settlement.insert(raw).clone())
    }

    /// Waits until a caller settles the promise, or until `timeout` passes.
    ///
    /// Returns `Ok(None)` when the durable timer fires first. A later
    /// settlement stays buffered. `timeout` is rounded up to whole seconds.
    ///
    /// # Errors
    ///
    /// Same as [`wait`](Self::wait), and the errors of
    /// [`WorkflowContext::wait_for_signal_timeout`].
    pub async fn wait_timeout<T: DeserializeOwned>(
        &mut self,
        timeout: Duration,
    ) -> HarvestResult<Option<Result<T, PromiseRejected>>> {
        if let Some(raw) = &self.settlement {
            return PromiseSettlement::decode(raw.clone()).map(Some);
        }
        // A timeout stores nothing, so a later wait can still take the settlement.
        let Some(raw) = self
            .context
            .wait_for_signal_timeout(&self.id.signal_name(), timeout)
            .await?
        else {
            return Ok(None);
        };
        PromiseSettlement::decode(self.settlement.insert(raw).clone()).map(Some)
    }
}

/// Applies the promise settlement rules to a signal before it is stored
/// (issue #1985).
///
/// Every signal path calls this through `signal::send_signal_idempotent`:
/// the Rust API, the HTTP route, the CLI and the cross-workflow outbox.
/// The HTTP route also calls it before its keyed dedupe probe. Otherwise a
/// mismatched key that matches an unrelated row reports a false success.
///
/// - A `harvest.promise:` signal always uses its own name as the
///   idempotency key. A missing key gets that value. A different key is an
///   error. So the first settlement wins on every path.
/// - Its payload must decode as a [`PromiseSettlement`]. A bad payload would
///   use up the promise and then fail the waiting run.
/// - Any other signal must not use a `harvest.promise:` key, because that
///   key would block the real settlement.
///
/// Returns the idempotency key to store.
///
/// # Errors
///
/// Returns [`crate::HarvestError::Config`] when a rule fails.
#[cfg(feature = "db")]
pub fn settlement_idempotency_key<'a>(
    signal_name: &'a str,
    payload: &Value,
    idempotency_key: Option<&'a str>,
) -> HarvestResult<Option<&'a str>> {
    if !signal_name.starts_with(PROMISE_SIGNAL_PREFIX) {
        if idempotency_key.is_some_and(|key| key.starts_with(PROMISE_SIGNAL_PREFIX)) {
            return Err(crate::HarvestError::Config(format!(
                "idempotency keys that start with '{PROMISE_SIGNAL_PREFIX}' are reserved \
                 for promise settlements"
            )));
        }
        return Ok(idempotency_key);
    }
    if idempotency_key.is_some_and(|key| key != signal_name) {
        return Err(crate::HarvestError::Config(format!(
            "a promise settlement must use its signal name '{signal_name}' as its idempotency key"
        )));
    }
    if let Err(e) = PromiseSettlement::deserialize(payload) {
        return Err(crate::HarvestError::Config(format!(
            "the payload of '{signal_name}' is not a promise settlement: {e}"
        )));
    }
    Ok(Some(signal_name))
}

/// Resolves a promise with `value`.
///
/// Returns `Ok(true)` for the first settlement and `Ok(false)` when the
/// promise already has one. `conn` must reach the shard that holds the run.
///
/// # Errors
///
/// Returns [`crate::HarvestError::NotFound`] if the run does not exist. Returns
/// an error if the run is terminal and the promise has no settlement. Returns
/// [`crate::HarvestError::Database`] if the insert fails.
#[cfg(feature = "db")]
pub async fn resolve(
    conn: &mut diesel_async::AsyncPgConnection,
    id: &PromiseId,
    value: Value,
) -> HarvestResult<bool> {
    settle(conn, id, &PromiseSettlement::resolved(value)).await
}

/// Rejects a promise with `error`.
///
/// Returns `Ok(true)` for the first settlement and `Ok(false)` when the
/// promise already has one.
///
/// # Errors
///
/// Same as [`resolve`].
#[cfg(feature = "db")]
pub async fn reject(
    conn: &mut diesel_async::AsyncPgConnection,
    id: &PromiseId,
    error: impl Into<String>,
) -> HarvestResult<bool> {
    settle(conn, id, &PromiseSettlement::rejected(error)).await
}

#[cfg(feature = "db")]
async fn settle(
    conn: &mut diesel_async::AsyncPgConnection,
    id: &PromiseId,
    settlement: &PromiseSettlement,
) -> HarvestResult<bool> {
    crate::signal::send_signal_idempotent(
        conn,
        id.execution_id(),
        &id.signal_name(),
        settlement.to_value(),
        Some(&id.idempotency_key()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec_id() -> ExecutionId {
        "0192f3a4-5b6c-7d8e-9f01-23456789abcd"
            .parse()
            .expect("valid uuid")
    }

    #[test]
    fn token_round_trips_through_its_string_form() {
        let id = PromiseId::new(exec_id(), "approval:step-1").expect("valid key");
        let token = id.to_string();
        assert_eq!(
            token,
            "0192f3a4-5b6c-7d8e-9f01-23456789abcd/approval:step-1"
        );
        assert_eq!(token.parse::<PromiseId>(), Ok(id));
    }

    #[test]
    fn token_round_trips_through_serde() {
        let id = PromiseId::new(exec_id(), "k").expect("valid key");
        let json = serde_json::to_value(&id).expect("serialize");
        assert_eq!(json, serde_json::json!(id.to_string()));
        let back: PromiseId = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn key_rules_are_enforced() {
        assert_eq!(PromiseId::new(exec_id(), ""), Err(PromiseIdError::EmptyKey));
        assert_eq!(
            PromiseId::new(exec_id(), "a/b"),
            Err(PromiseIdError::InvalidKeyChar('/'))
        );
        assert_eq!(
            PromiseId::new(exec_id(), "with space"),
            Err(PromiseIdError::InvalidKeyChar(' '))
        );
        let long = "k".repeat(MAX_PROMISE_KEY_LEN + 1);
        assert_eq!(
            PromiseId::new(exec_id(), long),
            Err(PromiseIdError::KeyTooLong(MAX_PROMISE_KEY_LEN + 1))
        );
        assert!(PromiseId::new(exec_id(), "k".repeat(MAX_PROMISE_KEY_LEN)).is_ok());
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        assert_eq!(
            "no-separator".parse::<PromiseId>(),
            Err(PromiseIdError::MissingSeparator)
        );
        assert!(matches!(
            "not-a-uuid/key".parse::<PromiseId>(),
            Err(PromiseIdError::InvalidExecutionId(_))
        ));
    }

    #[test]
    fn signal_name_and_idempotency_key_carry_the_prefix() {
        let id = PromiseId::new(exec_id(), "approval").expect("valid key");
        assert_eq!(id.signal_name(), "harvest.promise:approval");
        assert_eq!(id.idempotency_key(), id.signal_name());
    }

    #[test]
    fn settlement_payload_matches_its_serde_form() {
        for settlement in [
            PromiseSettlement::resolved(serde_json::json!({"ok": true})),
            PromiseSettlement::rejected("no"),
        ] {
            let value = settlement.to_value();
            assert_eq!(value, serde_json::to_value(&settlement).expect("serialize"));
            let back: PromiseSettlement = serde_json::from_value(value).expect("deserialize");
            assert_eq!(back, settlement);
        }
    }

    #[test]
    fn decode_separates_values_and_rejections() {
        let resolved: Result<u32, PromiseRejected> =
            PromiseSettlement::decode(PromiseSettlement::resolved(serde_json::json!(7)).to_value())
                .expect("decodes");
        assert_eq!(resolved, Ok(7));
        let rejected: Result<u32, PromiseRejected> =
            PromiseSettlement::decode(PromiseSettlement::rejected("no").to_value())
                .expect("decodes");
        assert_eq!(
            rejected,
            Err(PromiseRejected {
                error: "no".to_string()
            })
        );
        assert!(PromiseSettlement::decode::<u32>(serde_json::json!({"id": 1})).is_err());
    }

    #[cfg(feature = "db")]
    #[test]
    fn settlement_rules_force_the_promise_key() {
        let name = "harvest.promise:k";
        let ok = PromiseSettlement::resolved(serde_json::json!(1)).to_value();
        assert_eq!(
            settlement_idempotency_key(name, &ok, None).ok(),
            Some(Some(name))
        );
        assert_eq!(
            settlement_idempotency_key(name, &ok, Some(name)).ok(),
            Some(Some(name))
        );
        assert!(settlement_idempotency_key(name, &ok, Some("other")).is_err());
    }

    #[cfg(feature = "db")]
    #[test]
    fn settlement_rules_reject_a_malformed_payload() {
        let name = "harvest.promise:k";
        for bad in [
            serde_json::json!("ops"),
            serde_json::json!({"value": 1}),
            serde_json::json!({"outcome": "resolved"}),
        ] {
            assert!(
                settlement_idempotency_key(name, &bad, None).is_err(),
                "{bad} must be refused"
            );
        }
    }

    #[cfg(feature = "db")]
    #[test]
    fn settlement_rules_reserve_the_key_prefix_and_pass_other_signals() {
        let payload = serde_json::json!({"any": "thing"});
        assert!(settlement_idempotency_key("order", &payload, Some("harvest.promise:k")).is_err());
        assert_eq!(
            settlement_idempotency_key("order", &payload, Some("key-1")).ok(),
            Some(Some("key-1"))
        );
        assert_eq!(
            settlement_idempotency_key("order", &payload, None).ok(),
            Some(None)
        );
    }
}
