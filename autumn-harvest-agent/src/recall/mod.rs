//! A long-horizon recall eval (issue #2455).
//!
//! An item states facts in early ops and asks about one of them in its last
//! op. Each item runs on one durable [`Agent`](crate::Agent) per memory
//! config, one op per session. [`ExactMatch`](crate::ExactMatch) scores the
//! answer of the last op. The report holds one row for each config and item.
//!
//! - [`RecallFixture::builtin`] is the fixture. Each item is original. No
//!   third-party dataset is in it.
//! - [`profiles`] gives the four configs: [`WINDOW`], [`NOTES`], [`MEMORY`]
//!   and [`EPISODES`]. A config is an [`AgentProfile`] whose kind is its
//!   name.
//! - [`ReaderModel`] answers only from its request. CI uses it to prove that
//!   the eval sees a loss.
//! - `sweep` runs the eval on SQLite. It needs the `sqlite` feature.
//!
//! See `docs/design/DESIGN-2455.md` and `docs/performance-agent-recall.md`.

mod reader;
#[cfg(feature = "sqlite")]
mod sweep;

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::agent::{AgentMemory, AgentProfile};
use crate::context_reset::ContextReset;
use crate::episode::{EpisodeSettings, InMemoryEpisodeStore};
use crate::harness::AgentHarness;
use crate::memory::InMemoryMemoryStore;
use crate::model::AgentModel;
use crate::score::Verdict;

pub use reader::ReaderModel;
#[cfg(feature = "sqlite")]
pub use sweep::sweep;

/// The config with the window only.
pub const WINDOW: &str = "window";

/// The config with the window and progress notes (issue #2453).
pub const NOTES: &str = "notes";

/// The config with the window and store memory, so the model has the
/// `memory` tool. It is the comparison for [`EPISODES`].
pub const MEMORY: &str = "memory";

/// The config with the window, store memory and episodes (issue #2454).
pub const EPISODES: &str = "episodes";

/// The system prompt of each config.
pub const SYSTEM: &str = "You talk with one user over many sessions. Your context can lose \
     earlier sessions. Keep what the user asks you to remember. When the user asks for a \
     stored value, reply with the value only, with no other words.";

/// The built-in fixture, as JSON.
const BUILTIN: &str = include_str!("fixture.json");

/// One item: facts in early ops, and a question in the last op.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallItem {
    /// The id. It is the name of the agent of the item, so it has no space.
    pub id: String,
    /// The user message of each op, in order. The last one is the question.
    pub ops: Vec<String>,
    /// The answer that scores 1.
    pub expected: String,
}

/// An error in a fixture.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FixtureError {
    /// The text is not a fixture.
    #[error("the fixture does not decode: {0}")]
    Json(#[from] serde_json::Error),
    /// An item has no op, so it has no question.
    #[error("the item {0:?} has no op")]
    NoOps(String),
    /// Two items have one id, so their rows cannot be told apart.
    #[error("two items have the id {0:?}")]
    Duplicate(String),
}

/// A list of items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecallFixture {
    /// The items, in run order.
    pub items: Vec<RecallItem>,
}

impl RecallFixture {
    /// The built-in fixture of this crate.
    ///
    /// # Errors
    ///
    /// Returns an error when the built-in JSON is not a valid fixture. A
    /// unit test keeps it valid.
    pub fn builtin() -> Result<Self, FixtureError> {
        Self::from_json(BUILTIN)
    }

    /// Read a fixture from JSON, in the shape `{"items": [...]}`.
    ///
    /// # Errors
    ///
    /// Returns an error when the JSON does not decode, when an item has no
    /// op, or when two items have one id.
    pub fn from_json(json: &str) -> Result<Self, FixtureError> {
        let fixture: Self = serde_json::from_str(json)?;
        let mut seen = BTreeSet::new();
        for item in &fixture.items {
            if item.ops.is_empty() {
                return Err(FixtureError::NoOps(item.id.clone()));
            }
            if !seen.insert(item.id.as_str()) {
                return Err(FixtureError::Duplicate(item.id.clone()));
            }
        }
        Ok(fixture)
    }

    /// The item `id`, if the fixture has it.
    #[must_use]
    pub fn item(&self, id: &str) -> Option<&RecallItem> {
        self.items.iter().find(|item| item.id == id)
    }
}

/// The outcome of one item under one config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RecallOutcome {
    /// Each op ran, and the scorer read the last answer.
    Scored {
        /// The answer of the last op.
        answer: String,
        /// The verdict of `ExactMatch`. A reject scores 0.
        verdict: Verdict,
    },
    /// The item did not run to its end. It has no score.
    Failed {
        /// Why the item failed.
        error: String,
    },
}

/// One item under one config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RecallRow {
    /// The config name.
    pub config: String,
    /// The item id.
    pub item: String,
    /// The outcome.
    pub outcome: RecallOutcome,
}

impl RecallRow {
    /// The score: the verdict score, 0 for a reject, `None` for a failure.
    #[must_use]
    pub fn score(&self) -> Option<f64> {
        match &self.outcome {
            RecallOutcome::Scored { verdict, .. } => {
                Some(verdict.as_score().map_or(0.0, crate::score::FiniteF64::get))
            }
            RecallOutcome::Failed { .. } => None,
        }
    }
}

/// The result of one sweep: one row for each config and item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RecallReport {
    /// The configs, in run order.
    pub configs: Vec<String>,
    /// The item ids, in run order.
    pub items: Vec<String>,
    /// The rows, config by config, item by item.
    pub rows: Vec<RecallRow>,
}

impl RecallReport {
    /// The row of `item` under `config`.
    #[must_use]
    pub fn row(&self, config: &str, item: &str) -> Option<&RecallRow> {
        self.rows
            .iter()
            .find(|row| row.config == config && row.item == item)
    }

    /// The score of `item` under `config`. `None` when the row is missing
    /// or failed.
    #[must_use]
    pub fn score(&self, config: &str, item: &str) -> Option<f64> {
        self.row(config, item).and_then(RecallRow::score)
    }

    /// The mean score of `config` over its scored rows. `None` when no row
    /// of the config has a score.
    #[must_use]
    pub fn mean(&self, config: &str) -> Option<f64> {
        let scores: Vec<f64> = self
            .rows
            .iter()
            .filter(|row| row.config == config)
            .filter_map(RecallRow::score)
            .collect();
        if scores.is_empty() {
            return None;
        }
        // An item count is far below 2^52, so the cast is exact.
        #[allow(clippy::cast_precision_loss)]
        let count = scores.len() as f64;
        Some(scores.iter().sum::<f64>() / count)
    }

    /// The report as a Markdown table: one line per item, one column per
    /// config, then the mean. A failed row reads `error`. The mean cell
    /// also shows the scored rows of the config, as `(scored/items)`.
    #[must_use]
    pub fn to_markdown(&self) -> String {
        let mut out = String::from("| item |");
        for config in &self.configs {
            let _ = write!(out, " {} |", cell(config));
        }
        out.push_str("\n|---|");
        for _ in &self.configs {
            out.push_str("---|");
        }
        out.push('\n');
        for item in &self.items {
            let _ = write!(out, "| {} |", cell(item));
            for config in &self.configs {
                let text = self.row(config, item).map_or_else(
                    || "missing".to_owned(),
                    |row| {
                        row.score()
                            .map_or_else(|| "error".to_owned(), |score| score.to_string())
                    },
                );
                let _ = write!(out, " {text} |");
            }
            out.push('\n');
        }
        out.push_str("| mean |");
        for config in &self.configs {
            let scored = self
                .rows
                .iter()
                .filter(|row| row.config == *config && row.score().is_some())
                .count();
            let mean = self
                .mean(config)
                .map_or_else(|| "n/a".to_owned(), |mean| format!("{mean:.2}"));
            let _ = write!(out, " {mean} ({scored}/{}) |", self.items.len());
        }
        out.push('\n');
        out
    }
}

/// A table cell with no column break.
fn cell(text: &str) -> String {
    text.replace('|', "\\|")
}

/// The four configs, each with a window of `window` messages.
///
/// - [`WINDOW`] keeps the window only.
/// - [`NOTES`] adds progress notes. The model saves them with
///   `save_progress`, and they stay across the window.
/// - [`MEMORY`] adds store memory. The model sees the `memory` tool.
/// - [`EPISODES`] adds store memory and the episode log. The model sees the
///   `memory` tool. A pass after each op writes facts into the memory
///   block, which each request shows. Compare it with [`MEMORY`].
#[must_use]
pub fn profiles(window: u32) -> Vec<AgentProfile> {
    let base = |kind: &str| AgentProfile::new(kind).system(SYSTEM).window(window);
    vec![
        base(WINDOW),
        // A reset needs a request cap. The profile sets none, so only the
        // notes tool is on.
        base(NOTES).context_reset(ContextReset::new(1, 90)),
        base(MEMORY).memory(AgentMemory::Store),
        base(EPISODES)
            .memory(AgentMemory::Store)
            .episodes(EpisodeSettings::default().every(1)),
    ]
}

/// A harness on `model` with the four [`profiles`], and the memory and
/// episode stores that [`MEMORY`] and [`EPISODES`] need.
///
/// The stores are in memory. A new process starts with empty stores, so a
/// rerun after a crash is not valid for [`MEMORY`] and [`EPISODES`].
#[must_use]
pub fn harness(model: Arc<dyn AgentModel>, window: u32) -> AgentHarness {
    let harness = AgentHarness::new(model)
        .memory(Arc::new(InMemoryMemoryStore::new()))
        .episodes(Arc::new(InMemoryEpisodeStore::default()));
    profiles(window)
        .into_iter()
        .fold(harness, AgentHarness::agent)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::score::Verdict;

    #[test]
    fn the_builtin_fixture_is_valid() {
        let fixture = RecallFixture::builtin().expect("the fixture is valid");
        assert!(fixture.items.len() >= 3);
        for item in &fixture.items {
            let id = crate::agent::AgentId::new(WINDOW, &item.id);
            assert!(id.is_ok(), "{} is an agent name", item.id);
            let question = item.ops.last().expect("a question");
            assert!(!question.contains(&item.expected), "{}", item.id);
        }
        // A key that ends another key would read the value of that key.
        let keys: Vec<&str> = fixture
            .items
            .iter()
            .flat_map(|item| item.ops.iter())
            .filter_map(|op| op.strip_prefix("Remember: the "))
            .filter_map(|op| op.split_once(" is ").map(|(key, _)| key))
            .collect();
        for key in &keys {
            for other in &keys {
                let clash = other.len() > key.len() && other.ends_with(&format!(" {key}"));
                assert!(!clash, "{key:?} ends {other:?}");
            }
        }
    }

    #[test]
    fn a_fixture_refuses_an_item_with_no_op_and_a_repeated_id() {
        let none = r#"{"items":[{"id":"a","ops":[],"expected":"x"}]}"#;
        assert!(matches!(
            RecallFixture::from_json(none),
            Err(FixtureError::NoOps(_))
        ));
        let twice = r#"{"items":[{"id":"a","ops":["q"],"expected":"x"},
                                 {"id":"a","ops":["q"],"expected":"x"}]}"#;
        assert!(matches!(
            RecallFixture::from_json(twice),
            Err(FixtureError::Duplicate(_))
        ));
    }

    #[test]
    fn each_config_is_a_profile_of_its_own_kind() {
        let kinds: Vec<String> = profiles(2).into_iter().map(|p| p.kind).collect();
        assert_eq!(kinds, [WINDOW, NOTES, MEMORY, EPISODES]);
        let harness = harness(ReaderModel::new(), 2);
        for kind in [WINDOW, NOTES, MEMORY, EPISODES] {
            assert_eq!(harness.agent_profile(kind).map(|p| p.window), Some(2));
        }
    }

    fn row(config: &str, item: &str, outcome: Option<bool>) -> RecallRow {
        let outcome = match outcome {
            Some(true) => RecallOutcome::Scored {
                answer: "a".into(),
                verdict: Verdict::score(1.0).expect("finite"),
            },
            Some(false) => RecallOutcome::Scored {
                answer: "b".into(),
                verdict: Verdict::reject("miss"),
            },
            None => RecallOutcome::Failed {
                error: "down".into(),
            },
        };
        RecallRow {
            config: config.into(),
            item: item.into(),
            outcome,
        }
    }

    proptest! {
        /// The table has one line per item and one cell per config. A mean
        /// reads only scored rows, so a failed row never moves it.
        #[test]
        fn the_table_lists_every_cell_and_the_mean_skips_failures(
            cells in proptest::collection::vec(
                proptest::collection::vec(proptest::option::of(any::<bool>()), 1..5),
                1..4,
            )
        ) {
            let configs: Vec<String> = (0..cells.len()).map(|c| format!("c{c}")).collect();
            let width = cells.iter().map(Vec::len).min().unwrap_or(1);
            let items: Vec<String> = (0..width).map(|i| format!("i{i}")).collect();
            let mut rows = Vec::new();
            for (config, column) in configs.iter().zip(&cells) {
                for (item, outcome) in items.iter().zip(column) {
                    rows.push(row(config, item, *outcome));
                }
            }
            let report = RecallReport { configs: configs.clone(), items: items.clone(), rows };
            let table = report.to_markdown();
            prop_assert_eq!(table.lines().count(), items.len() + 3);
            for line in table.lines() {
                prop_assert_eq!(line.matches('|').count(), configs.len() + 2);
            }
            prop_assert!(!table.contains("missing"));
            for (config, column) in configs.iter().zip(&cells) {
                let scored: Vec<f64> = column[..width]
                    .iter()
                    .filter_map(|cell| cell.map(|hit| if hit { 1.0 } else { 0.0 }))
                    .collect();
                let want = if scored.is_empty() {
                    None
                } else {
                    #[allow(clippy::cast_precision_loss)]
                    Some(scored.iter().sum::<f64>() / scored.len() as f64)
                };
                prop_assert_eq!(report.mean(config), want);
            }
        }
    }
}
