//! Controls that span a whole mesh of durable agents (issue #2272).
//!
//! A mesh is a set of agents that share an id. [`AgentProfile::mesh`] names
//! it. Each run, each model turn and each peer message of an agent carries
//! the id, and a durable agent publishes it as the search attribute
//! [`MESH_ATTR`].
//!
//! - A [`MeshBudget`] caps the tokens or the cost of a mesh in each period.
//!   The model-turn activity checks it against a [`MeshSpend`] source before
//!   the model call. A spent budget refuses the turn with
//!   [`MESH_BUDGET_EXCEEDED`]. A durable agent then parks until the period
//!   ends.
//! - `sqlite::pause_mesh` pauses every agent of a mesh and returns a
//!   [`MeshPause`] audit record.
//!
//! [`AgentProfile::mesh`]: crate::AgentProfile::mesh

use autumn_harvest::chrono::{DateTime, Datelike, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::agent::AgentId;
use crate::error::AgentError;
use crate::model::BoxFuture;

/// The search attribute that names the mesh of a run.
pub const MESH_ATTR: &str = "agent_mesh";

/// The activity error type of a model turn that a mesh budget refuses.
pub const MESH_BUDGET_EXCEEDED: &str = "MeshBudgetExceeded";

/// A budget period, in UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "period", rename_all = "snake_case")]
pub enum BudgetPeriod {
    /// A calendar day.
    Day,
    /// A calendar month.
    Month,
    /// Fixed windows of `secs` seconds from the Unix epoch.
    Every {
        /// The window length in seconds. Zero counts as one.
        secs: u64,
    },
}

impl BudgetPeriod {
    /// The start and the end of the period that holds `now`.
    #[must_use]
    pub fn window(self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        match self {
            Self::Day => {
                let start = Utc
                    .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
                    .single()
                    .unwrap_or(now);
                (start, start + autumn_harvest::chrono::Duration::days(1))
            }
            Self::Month => {
                let start = Utc
                    .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
                    .single()
                    .unwrap_or(now);
                let (year, month) = if now.month() == 12 {
                    (now.year() + 1, 1)
                } else {
                    (now.year(), now.month() + 1)
                };
                let end = Utc
                    .with_ymd_and_hms(year, month, 1, 0, 0, 0)
                    .single()
                    .unwrap_or(now);
                (start, end)
            }
            Self::Every { secs } => {
                let len = i64::try_from(secs.max(1)).unwrap_or(i64::MAX);
                let at = now.timestamp();
                let start = at - at.rem_euclid(len);
                let from = Utc.timestamp_opt(start, 0).single().unwrap_or(now);
                let until = Utc
                    .timestamp_opt(start.saturating_add(len), 0)
                    .single()
                    .unwrap_or(now);
                (from, until)
            }
        }
    }
}

/// A cap on the spend of one mesh in each period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshBudget {
    /// The mesh id.
    pub mesh: String,
    /// The period of the cap.
    pub period: BudgetPeriod,
    /// The most provider-reported tokens in a period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    /// The most cost in a period, in millionths of a US dollar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_usd_micros: Option<u64>,
}

impl MeshBudget {
    /// A budget of `mesh` for each calendar day, with no cap yet.
    #[must_use]
    pub fn daily(mesh: impl Into<String>) -> Self {
        Self::new(mesh, BudgetPeriod::Day)
    }

    /// A budget of `mesh` for each calendar month, with no cap yet.
    #[must_use]
    pub fn monthly(mesh: impl Into<String>) -> Self {
        Self::new(mesh, BudgetPeriod::Month)
    }

    /// A budget of `mesh` for each window of `every`, with no cap yet.
    #[must_use]
    pub fn every(mesh: impl Into<String>, every: std::time::Duration) -> Self {
        Self::new(
            mesh,
            BudgetPeriod::Every {
                secs: every.as_secs().max(1),
            },
        )
    }

    fn new(mesh: impl Into<String>, period: BudgetPeriod) -> Self {
        Self {
            mesh: mesh.into(),
            period,
            max_tokens: None,
            max_cost_usd_micros: None,
        }
    }

    /// Cap the tokens of a period.
    #[must_use]
    pub const fn max_tokens(mut self, max: u64) -> Self {
        self.max_tokens = Some(max);
        self
    }

    /// Cap the cost of a period, in millionths of a US dollar.
    #[must_use]
    pub const fn max_cost_usd_micros(mut self, max: u64) -> Self {
        self.max_cost_usd_micros = Some(max);
        self
    }

    /// Whether `spend` has reached a cap of this budget.
    #[must_use]
    pub fn is_spent(&self, spend: &Spend) -> bool {
        self.max_tokens.is_some_and(|max| spend.tokens >= max)
            || self
                .max_cost_usd_micros
                .is_some_and(|max| spend.cost_usd_micros >= max)
    }
}

/// What a mesh spent since a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Spend {
    /// Provider-reported tokens, input and output.
    pub tokens: u64,
    /// The cost in millionths of a US dollar. An unpriced call adds none.
    pub cost_usd_micros: u64,
}

/// Where the spend of a mesh comes from, such as the agent cost ledger.
///
/// The model-turn activity calls it before each turn of an agent with a
/// budget. A failed read refuses the turn and retries it, so the check
/// fails closed.
pub trait MeshSpend: Send + Sync + std::fmt::Debug {
    /// The spend of the runs of `mesh` since `since`.
    ///
    /// # Errors
    ///
    /// Returns an error when the source cannot read.
    fn spent<'a>(
        &'a self,
        mesh: &'a str,
        since: DateTime<Utc>,
    ) -> BoxFuture<'a, Result<Spend, AgentError>>;
}

/// The details of a model turn that a mesh budget refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshBudgetExceeded {
    /// The mesh id.
    pub mesh: String,
    /// When the period ends, in epoch milliseconds. A parked agent opens
    /// then.
    pub until_ms: i64,
}

/// The audit record of a mesh pause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshPause {
    /// The mesh id.
    pub mesh: String,
    /// Why the mesh was paused.
    pub reason: String,
    /// When the pause was sent, in epoch milliseconds.
    pub at_ms: i64,
    /// Each agent that got the pause.
    pub agents: Vec<AgentId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn a_day_runs_from_midnight_to_midnight() {
        let (start, end) = BudgetPeriod::Day.window(at("2026-10-23T17:05:00Z"));
        assert_eq!(start, at("2026-10-23T00:00:00Z"));
        assert_eq!(end, at("2026-10-24T00:00:00Z"));
    }

    #[test]
    fn a_month_ends_at_the_next_month() {
        let (start, end) = BudgetPeriod::Month.window(at("2026-12-31T23:59:59Z"));
        assert_eq!(start, at("2026-12-01T00:00:00Z"));
        assert_eq!(end, at("2027-01-01T00:00:00Z"));
    }

    #[test]
    fn a_fixed_window_starts_at_a_multiple_of_its_length() {
        let (start, end) = BudgetPeriod::Every { secs: 60 }.window(at("2026-10-23T17:05:30Z"));
        assert_eq!(start, at("2026-10-23T17:05:00Z"));
        assert_eq!(end, at("2026-10-23T17:06:00Z"));
    }

    #[test]
    fn a_budget_is_spent_at_its_cap() {
        let budget = MeshBudget::daily("m").max_tokens(10);
        assert!(!budget.is_spent(&Spend {
            tokens: 9,
            cost_usd_micros: 0
        }));
        assert!(budget.is_spent(&Spend {
            tokens: 10,
            cost_usd_micros: 0
        }));
        let cost = MeshBudget::daily("m").max_cost_usd_micros(5);
        assert!(cost.is_spent(&Spend {
            tokens: 0,
            cost_usd_micros: 5
        }));
        assert!(!MeshBudget::daily("m").is_spent(&Spend {
            tokens: u64::MAX,
            cost_usd_micros: 0
        }));
    }
}
