//! Fan-out failure tolerance and result writer (issue #1986).
//!
//! [`FanOutOptions`] configures
//! [`WorkflowContext::execute_activity_fan_out_raw_with`](crate::context::WorkflowContext::execute_activity_fan_out_raw_with)
//! and its typed sibling.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{HarvestError, HarvestResult};

/// The task-row header that asks the worker to write the result (issue #1986).
///
/// Only the engine sets it, on each row of a fan-out with the result writer.
/// The engine removes a copy that a caller supplies. An activity handler can
/// read it through `ActivityContext::headers`.
pub const RESULT_WRITER_HEADER: &str = "x-harvest-result-writer";

/// The discriminator key of a recorded [`StoredResult`].
///
/// The key is reserved. In a writer fan-out, a worker without a store fails
/// an activity whose result carries it. An older worker records such a
/// result as is, and the fan-out then reads it as a reference.
pub const STORED_RESULT_KEY: &str = "_harvest_stored_result";

/// How many item failures a fan-out tolerates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureTolerance {
    /// The fan-out tolerates no failure.
    #[default]
    None,
    /// Up to this many items can fail. A count over the item count tolerates
    /// every failure.
    Count(usize),
    /// Up to this percent of the items can fail, rounded down. A value over
    /// 100 counts as 100.
    Percent(u8),
}

impl FailureTolerance {
    /// The largest number of failures a fan-out of `total` items tolerates.
    #[must_use]
    pub fn max_failures(self, total: usize) -> usize {
        match self {
            Self::None => 0,
            Self::Count(n) => n,
            // Round down, so the fan-out never tolerates more than asked.
            Self::Percent(p) => total.saturating_mul(usize::from(p.min(100))) / 100,
        }
    }
}

/// Options for a fan-out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FanOutOptions {
    max_in_flight: Option<usize>,
    tolerance: FailureTolerance,
    result_writer: bool,
}

impl FanOutOptions {
    /// Options with no window, no tolerance and no result writer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Limit the items in flight at a time. A value of 0 counts as 1.
    #[must_use]
    pub const fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = Some(max_in_flight);
        self
    }

    /// Tolerate item failures up to `tolerance`.
    #[must_use]
    pub const fn with_tolerance(mut self, tolerance: FailureTolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Write each item result through the `PayloadStore`.
    ///
    /// The workflow worker needs a store, or a fresh dispatch fails with
    /// `HarvestError::Config`. An activity worker without a store records the
    /// value inline.
    #[must_use]
    pub const fn with_result_writer(mut self, result_writer: bool) -> Self {
        self.result_writer = result_writer;
        self
    }

    /// The window, if one is set.
    #[must_use]
    pub const fn max_in_flight(&self) -> Option<usize> {
        self.max_in_flight
    }

    /// The failure tolerance.
    #[must_use]
    pub const fn tolerance(&self) -> FailureTolerance {
        self.tolerance
    }

    /// Whether the worker writes each result through the `PayloadStore`.
    #[must_use]
    pub const fn result_writer(&self) -> bool {
        self.result_writer
    }
}

/// A reference to one item result in the `PayloadStore`.
///
/// The serde form of this type is for a manifest. History records another
/// form, through [`to_recorded_value`](Self::to_recorded_value).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredResult {
    /// The store that holds the blob.
    pub store_id: String,
    /// The key that `PayloadStore::put` returned.
    pub key: String,
    /// The blob length in bytes.
    pub len: u64,
    /// The hex SHA-256 of the blob.
    pub checksum: String,
}

/// The blob that the worker writes for one result.
///
/// The activity id makes each blob unique. So two runs never share a key,
/// and retention can delete a blob without a race on a shared key.
#[derive(Serialize, Deserialize)]
struct ResultBlob {
    activity_id: String,
    result: Value,
}

impl StoredResult {
    /// Make a reference.
    #[must_use]
    pub fn new(
        store_id: impl Into<String>,
        key: impl Into<String>,
        len: u64,
        checksum: impl Into<String>,
    ) -> Self {
        Self {
            store_id: store_id.into(),
            key: key.into(),
            len,
            checksum: checksum.into(),
        }
    }

    /// The form that `ActivityCompleted.output` records.
    #[must_use]
    pub fn to_recorded_value(&self) -> Value {
        serde_json::json!({
            STORED_RESULT_KEY: 1,
            "store_id": self.store_id,
            "key": self.key,
            "len": self.len,
            "checksum": self.checksum,
        })
    }

    /// Parse the form that `ActivityCompleted.output` records.
    ///
    /// This form differs from the serde form. Returns `None` when `value` does
    /// not carry [`STORED_RESULT_KEY`] or a field is missing.
    #[must_use]
    pub fn from_recorded_value(value: &Value) -> Option<Self> {
        let obj = value.as_object()?;
        if obj.get(STORED_RESULT_KEY).and_then(Value::as_i64) != Some(1) {
            return None;
        }
        Some(Self {
            store_id: obj.get("store_id")?.as_str()?.to_string(),
            key: obj.get("key")?.as_str()?.to_string(),
            len: obj.get("len")?.as_u64()?,
            checksum: obj.get("checksum")?.as_str()?.to_string(),
        })
    }

    /// Read the result back from `store` and decode it with `codecs`.
    ///
    /// Call this from an activity, never from workflow code. Use the store and
    /// codecs that the worker has. The worker encodes the result before it
    /// writes it. The reference is not proof of ownership: read only the
    /// references that your own runs recorded.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestError::PayloadOffload`] when the store fails, the
    /// store id is not `store`'s, or the length or checksum does not match.
    /// Returns a codec or serialization error when the blob does not decode.
    pub async fn fetch(
        &self,
        store: &dyn crate::payload_store::PayloadStore,
        codecs: &crate::payload_codec::PayloadCodecs,
    ) -> HarvestResult<Value> {
        if store.store_id() != self.store_id {
            return Err(HarvestError::PayloadOffload(format!(
                "stored result references store '{}', not '{}'",
                self.store_id,
                store.store_id()
            )));
        }
        let bytes = store
            .get(&self.key)
            .await
            .map_err(|e| HarvestError::PayloadOffload(e.0))?;
        if bytes.len() as u64 != self.len {
            return Err(HarvestError::PayloadOffload(format!(
                "length mismatch for stored result '{}' (expected {} bytes, got {})",
                self.key,
                self.len,
                bytes.len()
            )));
        }
        let actual = crate::payload_store::hex_sha256(&bytes);
        if actual != self.checksum {
            return Err(HarvestError::PayloadOffload(format!(
                "checksum mismatch for stored result '{}' (expected {}, got {actual})",
                self.key, self.checksum
            )));
        }
        let blob: ResultBlob = serde_json::from_slice(&bytes)?;
        codecs.decode_payload(&blob.result)
    }

    /// Encode `value` with `codecs` and write it to `store` (issue #1986).
    ///
    /// The worker calls this for a task row that carries
    /// [`RESULT_WRITER_HEADER`]. The returned reference is what history
    /// records. The caller records the blob for retention.
    #[cfg(feature = "db")]
    pub(crate) async fn write(
        store: &dyn crate::payload_store::PayloadStore,
        codecs: &crate::payload_codec::PayloadCodecs,
        activity_id: crate::types::ActivityExecId,
        value: &Value,
    ) -> HarvestResult<Self> {
        let blob = ResultBlob {
            activity_id: activity_id.to_string(),
            result: codecs.encode_payload(value)?,
        };
        let bytes = serde_json::to_vec(&blob)?;
        let checksum = crate::payload_store::hex_sha256(&bytes);
        let key = store
            .put(&bytes)
            .await
            .map_err(|e| HarvestError::PayloadOffload(e.0))?;
        Ok(Self::new(
            store.store_id(),
            key,
            bytes.len() as u64,
            checksum,
        ))
    }
}

/// The outcome of one fan-out item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FanOutItem<T> {
    /// The item result, inline.
    Value(T),
    /// A reference to the item result in the `PayloadStore`.
    Stored(StoredResult),
    /// The item failed. The text is the activity error.
    Failed(String),
}

/// The manifest of a fan-out: one item per input, in input order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanOutResults<T> {
    items: Vec<FanOutItem<T>>,
    tolerated: usize,
}

impl<T> FanOutResults<T> {
    pub(crate) const fn new(items: Vec<FanOutItem<T>>, tolerated: usize) -> Self {
        Self { items, tolerated }
    }

    /// The items, in input order.
    #[must_use]
    pub fn items(&self) -> &[FanOutItem<T>] {
        &self.items
    }

    /// The items, in input order, by value.
    #[must_use]
    pub fn into_items(self) -> Vec<FanOutItem<T>> {
        self.items
    }

    /// The number of items that failed.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item, FanOutItem::Failed(_)))
            .count()
    }

    /// The number of items that succeeded.
    #[must_use]
    pub fn succeeded_count(&self) -> usize {
        self.items.len() - self.failed_count()
    }

    /// The largest number of failures that the fan-out tolerates.
    #[must_use]
    pub const fn tolerated(&self) -> usize {
        self.tolerated
    }
}
