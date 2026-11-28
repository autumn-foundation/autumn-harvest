//! Swarm mode: many durable agents on one goal (issue #2814).
//!
//! A swarm is a flat set of peers that share one board. The [`mesh`]
//! module runs a delegation tree, where a parent hands a task down and
//! authority narrows. A swarm has no parent. Each member reads the board,
//! claims an open item, works on it in its own workspace, and submits.
//!
//! - **Board.** A swarm has work items in lanes, and posts on channels. A
//!   member reads the posts after a cursor, so it reads only what is new.
//!   Posts are append-only.
//! - **Fenced claims.** A claim takes an item with a lease. Each take
//!   increments the epoch of the item. A renew, a release or a submit must
//!   name the holder and the current epoch. A lapsed lease lets another
//!   member take the item, and that take fences the old holder out. A
//!   member holds at most [`SwarmPolicy::max_claims`] items.
//! - **Main.** The swarm keeps one versioned head over a workspace snapshot.
//!   A take checks main out into the workspace of the member, under
//!   [`SwarmPolicy::root`], and records the version as the base.
//! - **Merge gate.** A submit merges the files of the member onto the
//!   current main, path by path, against the base. The [`SwarmJudge`] of
//!   the operator scores the merged files. The merge lands only when the
//!   score passes the [`Bar`] over the score of main. A compare-and-swap on
//!   the main version makes two merges serial. Every verdict goes into an
//!   append-only log.
//!
//! The judge is on the [`Swarm`], never a tool. A member sees the verdict,
//! and the scores only when [`SwarmPolicy::reveal_scores`] is set. So the
//! members cannot fit their work to the eval. Hugging Face found that a
//! hidden eval and a gate that must beat main did more than a hierarchy, a
//! vote or strict teams.
//!
//! [`SwarmTools`] gives a member six tools: `swarm_board`, `swarm_post`,
//! `swarm_claim`, `swarm_renew`, `swarm_release` and `swarm_submit`. A
//! member is the session of its run. A durable agent has its agent id as
//! its session, so each durable agent is one member. The tools need the
//! workspace of a durable agent, see [`crate::AgentProfile::workspace`].
//!
//! The [`kernel`] holds the pure rules. `formal/verus/src/agent_swarm.rs`
//! proves them. See `docs/agent-swarm.md` for the whole pattern, and
//! `docs/design/DESIGN-2814.md` for the design.
//!
//! [`mesh`]: crate::mesh

mod board;
mod merge;
mod store;
mod tools;

pub mod kernel;

use std::time::Duration;

pub use autumn_harvest::swarm_board::{
    CommitOutcome, ItemQuery, ItemRow, MAX_READ, MainRow, MergeCommit, MergeRow, NewPost, PostRow,
};
use serde::{Deserialize, Serialize};

pub use self::board::{ClaimOutcome, NewItem, Swarm, SwarmJudge, item_status};
pub use self::kernel::{Bar, ClaimRefusal, FenceRefusal, ItemStatus};
pub use self::merge::{Files, MergeResult, three_way};
#[cfg(feature = "postgres")]
pub use self::store::PostgresSwarmStore;
#[cfg(feature = "sqlite")]
pub use self::store::SqliteSwarmStore;
pub use self::store::{InMemorySwarmStore, SwarmStore};
pub use self::tools::{
    BOARD_TOOL, CLAIM_TOOL, POST_TOOL, RELEASE_TOOL, RENEW_TOOL, SUBMIT_TOOL, SwarmTools,
};

/// The most bytes in a member id.
pub const MAX_MEMBER_LEN: usize = 128;

/// The most bytes in the title of an item.
pub const MAX_TITLE_LEN: usize = 200;

/// The most bytes in the text of an item.
pub const MAX_ITEM_BODY_LEN: usize = 16 * 1024;

/// The most bytes in the text of a post.
pub const MAX_POST_LEN: usize = 8 * 1024;

/// The default lease of a claim: 30 minutes.
pub const DEFAULT_LEASE: Duration = Duration::from_mins(30);

/// The default number of merge attempts of one submit. Each attempt after
/// the first follows a merge of another member.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 8;

/// The default grace of a prune: 15 minutes. The tool timeout is 10.
pub const DEFAULT_PRUNE_GRACE: Duration = Duration::from_mins(15);

/// The prefix of the workspace scope of the main snapshots of a swarm.
pub const MAIN_SCOPE_PREFIX: &str = "swarm:";

/// Whether `name` is a valid swarm, item, lane or channel name: 1 to 64
/// characters from `a-z`, `0-9`, `_` and `-`.
#[must_use]
pub fn valid_name(name: &str) -> bool {
    crate::mesh::valid_name(name)
}

/// Whether `member` is a valid member id: 1 to [`MAX_MEMBER_LEN`] bytes
/// with no control character. An agent id such as `coder/c1` is valid.
#[must_use]
pub fn valid_member(member: &str) -> bool {
    !member.is_empty() && member.len() <= MAX_MEMBER_LEN && !member.chars().any(char::is_control)
}

/// A swarm, an item, a lane or a channel name that is not valid.
fn check_name(what: &str, name: &str) -> Result<(), SwarmError> {
    if valid_name(name) {
        Ok(())
    } else {
        Err(SwarmError::Invalid(format!(
            "the {what} {name:?} must have 1 to 64 characters from a-z, 0-9, _ and -"
        )))
    }
}

/// A member id that is not valid.
fn check_member(member: &str) -> Result<(), SwarmError> {
    if valid_member(member) {
        Ok(())
    } else {
        Err(SwarmError::Invalid(format!(
            "the member {member:?} must have 1 to {MAX_MEMBER_LEN} bytes and no control character"
        )))
    }
}

/// A text over its bound.
fn check_len(what: &str, text: &str, max: usize) -> Result<(), SwarmError> {
    if text.len() <= max {
        Ok(())
    } else {
        Err(SwarmError::Invalid(format!(
            "the {what} has {} bytes; the bound is {max}",
            text.len()
        )))
    }
}

/// The name of a swarm.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SwarmId(String);

impl SwarmId {
    /// The swarm named `name`.
    ///
    /// # Errors
    ///
    /// Returns [`SwarmError::Invalid`] for a name that is not
    /// [`valid_name`].
    pub fn new(name: impl Into<String>) -> Result<Self, SwarmError> {
        let name = name.into();
        check_name("swarm", &name)?;
        Ok(Self(name))
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The workspace scope of the main snapshots: `swarm:<name>`.
    #[must_use]
    pub fn main_scope(&self) -> String {
        format!("{MAIN_SCOPE_PREFIX}{}", self.0)
    }
}

impl TryFrom<String> for SwarmId {
    type Error = SwarmError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::new(name)
    }
}

impl From<SwarmId> for String {
    fn from(id: SwarmId) -> Self {
        id.0
    }
}

impl std::fmt::Display for SwarmId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A score of the judge, in integer score units.
///
/// Integer units keep the bar exact and the same on every backend.
/// [`Score::from_f64`] maps a ratio such as a win rate to micro units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Score(i64);

impl Score {
    /// The score units in one whole point of [`Score::from_f64`].
    pub const UNITS_PER_POINT: f64 = 1_000_000.0;

    /// A score of `units`.
    #[must_use]
    pub const fn new(units: i64) -> Self {
        Self(units)
    }

    /// The score of `points`, rounded to micro units and clamped to the
    /// range of `i64`. Returns `None` for a value that is not finite.
    #[must_use]
    pub fn from_f64(points: f64) -> Option<Self> {
        if !points.is_finite() {
            return None;
        }
        // A float `as` cast saturates at the bounds of `i64`, and the value
        // is finite, so the cast clamps and never wraps.
        #[allow(clippy::cast_possible_truncation)]
        let units = (points * Self::UNITS_PER_POINT).round() as i64;
        Some(Self(units))
    }

    /// The score in units.
    #[must_use]
    pub const fn units(self) -> i64 {
        self.0
    }
}

/// The rules of one swarm. The operator sets them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmPolicy {
    /// The lease of a claim. A member renews it with `swarm_renew`.
    pub lease: Duration,
    /// The most items that one member holds at a time. 0 counts as 1.
    pub max_claims: u32,
    /// The bar that a merge must pass over the score of main.
    pub bar: Bar,
    /// Whether a member sees the scores of a verdict. The default is
    /// `false`, so the eval stays hidden.
    pub reveal_scores: bool,
    /// The workspace folder that holds the files of main, such as `repo`.
    /// An empty root is the whole workspace.
    pub root: String,
    /// The most merge attempts of one submit. 0 counts as 1.
    pub max_attempts: u32,
    /// How long a newer main must hold before a prune deletes an older
    /// snapshot. Keep it longer than the tool timeout, so a claim in flight
    /// writes its base before a prune can delete it.
    pub prune_grace: Duration,
}

impl Default for SwarmPolicy {
    fn default() -> Self {
        Self {
            lease: DEFAULT_LEASE,
            max_claims: 1,
            bar: Bar::Improve { margin: 0 },
            reveal_scores: false,
            root: String::new(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            prune_grace: DEFAULT_PRUNE_GRACE,
        }
    }
}

impl SwarmPolicy {
    /// The lease in milliseconds, at least 1 and at most `i64::MAX`.
    #[must_use]
    pub fn lease_ms(&self) -> i64 {
        i64::try_from(self.lease.as_millis())
            .unwrap_or(i64::MAX)
            .max(1)
    }

    /// The claim cap, at least 1.
    #[must_use]
    pub const fn claim_cap(&self) -> u32 {
        if self.max_claims == 0 {
            1
        } else {
            self.max_claims
        }
    }

    /// The attempt bound, at least 1.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        if self.max_attempts == 0 {
            1
        } else {
            self.max_attempts
        }
    }

    /// The root as a path prefix: empty, or the root and a `/`.
    #[must_use]
    pub fn prefix(&self) -> String {
        let root = self.root.trim_matches('/');
        if root.is_empty() {
            String::new()
        } else {
            format!("{root}/")
        }
    }
}

/// The verdict of one submit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The merge landed on main.
    Merged,
    /// The merged files did not pass the bar.
    Rejected,
    /// The member and main changed one path in different ways.
    Conflict,
    /// The member does not hold the item at the epoch that it named.
    Fenced,
    /// The judge failed. Nothing merged.
    JudgeFailed,
    /// Other merges kept moving main for every attempt.
    Contended,
}

impl Verdict {
    /// The stored name, such as `merged`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Rejected => "rejected",
            Self::Conflict => "conflict",
            Self::Fenced => "fenced",
            Self::JudgeFailed => "judge_failed",
            Self::Contended => "contended",
        }
    }

    /// The verdict with the stored name `name`.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        [
            Self::Merged,
            Self::Rejected,
            Self::Conflict,
            Self::Fenced,
            Self::JudgeFailed,
            Self::Contended,
        ]
        .into_iter()
        .find(|verdict| verdict.as_str() == name)
    }
}

/// A failure of a swarm call.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SwarmError {
    /// An input is not valid. The text says why.
    #[error("{0}")]
    Invalid(String),
    /// The swarm has no main head. Seed it first.
    #[error("the swarm {0} has no main; seed it first")]
    NoMain(String),
    /// The swarm has no item with this id.
    #[error("the swarm {swarm} has no item {item}")]
    NoItem {
        /// The swarm.
        swarm: String,
        /// The item id.
        item: String,
    },
    /// Another write kept winning the race for one row.
    #[error("the {0} kept changing under the call; try again")]
    Contended(String),
    /// The workspace store failed.
    #[error("the swarm workspace failed: {0}")]
    Workspace(String),
    /// The store failed.
    #[error("the swarm store failed: {0}")]
    Backend(String),
}

impl From<SwarmError> for crate::AgentError {
    fn from(err: SwarmError) -> Self {
        Self::new(crate::ErrorKind::Tool, err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_swarm_name_follows_the_peer_rules() {
        assert!(SwarmId::new("chatbot").is_ok());
        assert!(SwarmId::new("Chat Bot").is_err());
        assert!(SwarmId::new("").is_err());
        assert_eq!(SwarmId::new("s1").unwrap().main_scope(), "swarm:s1");
        let decoded: Result<SwarmId, _> = serde_json::from_str("\"bad name\"");
        assert!(decoded.is_err());
    }

    #[test]
    fn a_member_is_an_agent_id() {
        assert!(valid_member("coder/c1"));
        assert!(!valid_member(""));
        assert!(!valid_member("a\nb"));
        assert!(!valid_member(&"x".repeat(MAX_MEMBER_LEN + 1)));
    }

    #[test]
    fn a_float_score_maps_to_micro_units() {
        assert_eq!(Score::from_f64(0.5), Some(Score::new(500_000)));
        assert_eq!(Score::from_f64(f64::NAN), None);
        assert_eq!(Score::from_f64(f64::INFINITY), None);
        assert_eq!(Score::from_f64(1e300), Some(Score::new(i64::MAX)));
        assert_eq!(Score::from_f64(-1e300), Some(Score::new(i64::MIN)));
    }

    #[test]
    fn the_policy_bounds_its_numbers() {
        let policy = SwarmPolicy {
            max_claims: 0,
            max_attempts: 0,
            lease: Duration::ZERO,
            root: "/repo/".to_owned(),
            ..SwarmPolicy::default()
        };
        assert_eq!(policy.claim_cap(), 1);
        assert_eq!(policy.attempts(), 1);
        assert_eq!(policy.lease_ms(), 1);
        assert_eq!(policy.prefix(), "repo/");
        assert_eq!(SwarmPolicy::default().prefix(), "");
    }

    #[test]
    fn a_verdict_keeps_its_stored_name() {
        for verdict in [
            Verdict::Merged,
            Verdict::Rejected,
            Verdict::Conflict,
            Verdict::Fenced,
            Verdict::JudgeFailed,
            Verdict::Contended,
        ] {
            assert_eq!(Verdict::parse(verdict.as_str()), Some(verdict));
        }
        assert_eq!(Verdict::parse("maybe"), None);
    }
}
