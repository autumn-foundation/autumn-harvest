//! Fan-out failure tolerance and result writer (issue #1986).
//!
//! [`FanOutOptions`] configures
//! [`WorkflowContext::execute_activity_fan_out_raw_with`](crate::context::WorkflowContext::execute_activity_fan_out_raw_with)
//! and its typed sibling.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How many item failures a fan-out tolerates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureTolerance {
    /// No failure is tolerated.
    #[default]
    None,
    /// Up to this many items can fail.
    Count(usize),
    /// Up to this percent of the items can fail.
    Percent(u8),
}

impl FailureTolerance {
    /// The largest number of failures a fan-out of `total` items tolerates.
    #[must_use]
    pub const fn max_failures(self, _total: usize) -> usize {
        0
    }
}

/// Options for a fan-out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

    /// Limit the items in flight at a time.
    #[must_use]
    pub const fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = Some(max_in_flight);
        self
    }

    /// Tolerate item failures up to `tolerance`.
    #[must_use]
    pub const fn tolerate(mut self, tolerance: FailureTolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    /// Write each item result through the `PayloadStore`.
    #[must_use]
    pub const fn write_results(mut self) -> Self {
        self.result_writer = true;
        self
    }

    /// The window, if one is set.
    #[must_use]
    pub const fn window(&self) -> Option<usize> {
        self.max_in_flight
    }

    /// The failure tolerance.
    #[must_use]
    pub const fn tolerance(&self) -> FailureTolerance {
        self.tolerance
    }

    /// Whether the worker writes each result through the `PayloadStore`.
    #[must_use]
    pub const fn writes_results(&self) -> bool {
        self.result_writer
    }
}

/// A reference to one item result in the `PayloadStore`.
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
    pub fn to_value(&self) -> Value {
        Value::Null
    }

    /// Parse the form that `ActivityCompleted.output` records.
    #[must_use]
    pub const fn from_value(_value: &Value) -> Option<Self> {
        None
    }

    /// Read the result back from `store` and decode it with `codecs`.
    ///
    /// # Errors
    ///
    /// Not implemented yet.
    pub async fn fetch(
        &self,
        _store: &dyn crate::payload_store::PayloadStore,
        _codecs: &crate::payload_codec::PayloadCodecs,
    ) -> crate::error::HarvestResult<Value> {
        Err(crate::error::HarvestError::Config("not implemented".into()))
    }
}

/// The outcome of one fan-out item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// The largest number of failures the fan-out tolerated.
    #[must_use]
    pub const fn tolerated(&self) -> usize {
        self.tolerated
    }
}
