//! The sampling policy of one agent kind (issue #2439).

use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::mesh::{MAX_ITEM_LEN, MAX_SET_LEN, valid_item, valid_name};
use crate::tool::ToolEffect;
use crate::types::MAX_PARALLEL_CHILDREN;

/// The most draws that a policy can allow in one strategy run.
pub const MAX_DRAWS: u32 = 256;

/// The most answer retries that a policy can allow (issue #2442).
pub const MAX_ANSWER_RETRIES: u32 = 16;

/// The keys of a sampling policy, and the key of the policy itself.
///
/// [`AgentTask`](crate::AgentTask), [`AgentOp`](crate::AgentOp) and
/// [`DrawsInput`](super::DrawsInput) refuse each of them on decode. So a run
/// cannot carry a policy, and cannot widen its own policy.
pub const SAMPLING_POLICY_FIELDS: &[&str] = &[
    "sampling_policy",
    "strategies",
    "max_draws",
    "max_concurrent_draws",
    "draw_effects",
    "scorers",
    "judge_models",
    "max_answer_retries",
    "cascade_models",
];

/// A strategy that runs more than one draw (epic #2438).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    /// Run N draws and keep the best (issue #2443).
    BestOfN,
    /// Draw until one passes the scorer (issue #2443).
    Rejection,
    /// Run N draws and take the most common answer (issue #2444).
    Vote,
    /// Run a cheap model first, and a stronger one on a low score (issue
    /// #2445).
    Cascade,
    /// Keep the best partial runs at each step (issue #2446).
    Beam,
}

impl Strategy {
    /// The wire name, such as `best_of_n`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BestOfN => "best_of_n",
            Self::Rejection => "rejection",
            Self::Vote => "vote",
            Self::Cascade => "cascade",
            Self::Beam => "beam",
        }
    }
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The id of a scorer (issue #2441), such as `exact_match`.
///
/// An id has 1 to 64 characters from `a-z`, `0-9`, `_` and `-`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ScorerId(String);

impl ScorerId {
    /// The id `id`, or `None` when it is not a valid name.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Option<Self> {
        let id = id.into();
        valid_name(&id).then_some(Self(id))
    }

    /// The id as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ScorerId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let id = String::deserialize(deserializer)?;
        Self::new(id).ok_or_else(|| serde::de::Error::custom("a scorer id is not a valid name"))
    }
}

impl std::fmt::Display for ScorerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The scorers that a kind may use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scorers {
    /// Each scorer that calls no model. No judge. The default.
    #[default]
    Deterministic,
    /// Only the scorers with these ids. A judge among them also needs its
    /// model in `judge_models`.
    Only(BTreeSet<ScorerId>),
}

/// The scorer that a strategy run asks for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScorerRef {
    /// The scorer id.
    pub id: ScorerId,
    /// The model of a judge scorer. `None` for a scorer that calls no
    /// model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub judge_model: Option<String>,
}

impl ScorerRef {
    /// A scorer that calls no model, or `None` for an id that is not
    /// valid.
    #[must_use]
    pub fn deterministic(id: impl Into<String>) -> Option<Self> {
        Some(Self {
            id: ScorerId::new(id)?,
            judge_model: None,
        })
    }

    /// A judge scorer that calls `model`, or `None` for an id that is not
    /// valid.
    #[must_use]
    pub fn judge(id: impl Into<String>, model: impl Into<String>) -> Option<Self> {
        Some(Self {
            id: ScorerId::new(id)?,
            judge_model: Some(model.into()),
        })
    }
}

/// The sampling policy of one agent kind.
///
/// Only an operator sets it. A missing key decodes as its default, and
/// each default is the safe value. An unknown key fails the decode, so an
/// older worker never drops a setting that a newer worker records. The run
/// of that older worker fails closed.
///
/// A key added later must skip its default on encode. An older worker then
/// fails only when the policy really holds the new setting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SamplingPolicy {
    /// The strategies that the kind may use. None by default, so each run
    /// is a single draw.
    #[serde(default)]
    pub strategies: BTreeSet<Strategy>,
    /// The most draws of one strategy run, across all rounds. 1 by default.
    #[serde(default = "one")]
    pub max_draws: u32,
    /// The most draws that run at the same time. 1 by default. The bound is
    /// [`MAX_PARALLEL_CHILDREN`].
    #[serde(default = "one")]
    pub max_concurrent_draws: u32,
    /// The tool effects that a draw may run. `[read_only]` by default.
    #[serde(default = "read_only")]
    pub draw_effects: BTreeSet<ToolEffect>,
    /// The scorers that the kind may use. Each scorer that calls no model
    /// by default.
    #[serde(default)]
    pub scorers: Scorers,
    /// The models that a judge scorer may call. None by default.
    #[serde(default)]
    pub judge_models: BTreeSet<String>,
    /// The cap of the in-loop answer check (issue #2442). 0 by default.
    #[serde(default)]
    pub max_answer_retries: u32,
    /// The harness models that a cascade tier may name. None by default,
    /// so the policy refuses each cascade.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub cascade_models: BTreeSet<String>,
}

const fn one() -> u32 {
    1
}

fn read_only() -> BTreeSet<ToolEffect> {
    BTreeSet::from([ToolEffect::ReadOnly])
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        Self {
            strategies: BTreeSet::new(),
            max_draws: one(),
            max_concurrent_draws: one(),
            draw_effects: read_only(),
            scorers: Scorers::Deterministic,
            judge_models: BTreeSet::new(),
            max_answer_retries: 0,
            cascade_models: BTreeSet::new(),
        }
    }
}

/// Why a policy is not valid.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum PolicyError {
    /// `max_draws` is 0 or above [`MAX_DRAWS`].
    #[error("max_draws is {value}; it must be 1 to {MAX_DRAWS}")]
    MaxDraws {
        /// The value.
        value: u32,
    },
    /// `max_concurrent_draws` is 0 or above [`MAX_PARALLEL_CHILDREN`].
    #[error("max_concurrent_draws is {value}; it must be 1 to {MAX_PARALLEL_CHILDREN}")]
    MaxConcurrentDraws {
        /// The value.
        value: u32,
    },
    /// `max_answer_retries` is above [`MAX_ANSWER_RETRIES`].
    #[error("max_answer_retries is {value}; it must be 0 to {MAX_ANSWER_RETRIES}")]
    MaxAnswerRetries {
        /// The value.
        value: u32,
    },
    /// A judge model is empty, longer than [`MAX_ITEM_LEN`] bytes, or holds
    /// a control character.
    #[error(
        "a judge model is empty, longer than {MAX_ITEM_LEN} bytes, or holds a control character"
    )]
    JudgeModel,
    /// A cascade model is empty, longer than [`MAX_ITEM_LEN`] bytes, or
    /// holds a control character.
    #[error(
        "a cascade model is empty, longer than {MAX_ITEM_LEN} bytes, or holds a control character"
    )]
    CascadeModel,
    /// A set has more than [`MAX_SET_LEN`] entries.
    #[error("{field} has more than {MAX_SET_LEN} entries")]
    TooMany {
        /// The key of the set.
        field: &'static str,
    },
}

impl SamplingPolicy {
    /// Check every bound. A store calls it before each set.
    ///
    /// # Errors
    ///
    /// Returns the first [`PolicyError`].
    pub fn validate(&self) -> Result<(), PolicyError> {
        if self.max_draws == 0 || self.max_draws > MAX_DRAWS {
            return Err(PolicyError::MaxDraws {
                value: self.max_draws,
            });
        }
        if self.max_concurrent_draws == 0 || self.max_concurrent_draws > MAX_PARALLEL_CHILDREN {
            return Err(PolicyError::MaxConcurrentDraws {
                value: self.max_concurrent_draws,
            });
        }
        if self.max_answer_retries > MAX_ANSWER_RETRIES {
            return Err(PolicyError::MaxAnswerRetries {
                value: self.max_answer_retries,
            });
        }
        if self.judge_models.len() > MAX_SET_LEN {
            return Err(PolicyError::TooMany {
                field: "judge_models",
            });
        }
        if self.cascade_models.len() > MAX_SET_LEN {
            return Err(PolicyError::TooMany {
                field: "cascade_models",
            });
        }
        if let Scorers::Only(ids) = &self.scorers
            && ids.len() > MAX_SET_LEN
        {
            return Err(PolicyError::TooMany { field: "scorers" });
        }
        if !self.judge_models.iter().all(|model| valid_item(model)) {
            return Err(PolicyError::JudgeModel);
        }
        if !self.cascade_models.iter().all(|model| valid_item(model)) {
            return Err(PolicyError::CascadeModel);
        }
        Ok(())
    }
}

/// Refuse each key of `value` that [`SAMPLING_POLICY_FIELDS`] names.
///
/// `what` names the type for the error. A value that is not an object
/// passes, so the typed decode reports its own error.
pub fn refuse_policy_fields<E: serde::de::Error>(
    value: &serde_json::Value,
    what: &str,
) -> Result<(), E> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    SAMPLING_POLICY_FIELDS
        .iter()
        .find(|field| object.contains_key(**field))
        .map_or(Ok(()), |field| {
            Err(E::custom(format_args!(
                "{what} may not carry the sampling policy field `{field}`; only an operator sets it"
            )))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_object_decodes_as_every_default() {
        let policy: SamplingPolicy = serde_json::from_str("{}").unwrap();
        assert_eq!(policy, SamplingPolicy::default());
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn an_unknown_key_fails_the_decode() {
        let err = serde_json::from_str::<SamplingPolicy>(r#"{"max_drawz": 3}"#).unwrap_err();
        assert!(err.to_string().contains("max_drawz"), "{err}");
    }

    #[test]
    fn the_wire_form_is_snake_case() {
        let policy = SamplingPolicy {
            strategies: BTreeSet::from([Strategy::BestOfN, Strategy::Vote]),
            scorers: Scorers::Only(BTreeSet::from([ScorerId::new("judge").unwrap()])),
            ..SamplingPolicy::default()
        };
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(
            value["strategies"],
            serde_json::json!(["best_of_n", "vote"])
        );
        assert_eq!(value["scorers"], serde_json::json!({"only": ["judge"]}));
        assert_eq!(value["draw_effects"], serde_json::json!(["read_only"]));
        let back: SamplingPolicy = serde_json::from_value(value).unwrap();
        assert_eq!(back, policy);
        assert_eq!(
            serde_json::to_value(SamplingPolicy::default()).unwrap()["scorers"],
            serde_json::json!("deterministic")
        );
    }

    #[test]
    fn a_scorer_id_must_be_a_name() {
        assert!(ScorerId::new("exact_match").is_some());
        assert!(ScorerId::new("Exact Match").is_none());
        assert!(serde_json::from_str::<ScorerId>(r#""a b""#).is_err());
    }

    #[test]
    fn validate_checks_every_bound() {
        let ok = SamplingPolicy {
            max_draws: MAX_DRAWS,
            max_concurrent_draws: MAX_PARALLEL_CHILDREN,
            max_answer_retries: MAX_ANSWER_RETRIES,
            ..SamplingPolicy::default()
        };
        assert!(ok.validate().is_ok());
        let cases = [
            SamplingPolicy {
                max_draws: 0,
                ..ok.clone()
            },
            SamplingPolicy {
                max_draws: MAX_DRAWS + 1,
                ..ok.clone()
            },
            SamplingPolicy {
                max_concurrent_draws: 0,
                ..ok.clone()
            },
            SamplingPolicy {
                max_concurrent_draws: MAX_PARALLEL_CHILDREN + 1,
                ..ok.clone()
            },
            SamplingPolicy {
                max_answer_retries: MAX_ANSWER_RETRIES + 1,
                ..ok.clone()
            },
            SamplingPolicy {
                judge_models: BTreeSet::from([String::new()]),
                ..ok.clone()
            },
            SamplingPolicy {
                judge_models: (0..=MAX_SET_LEN).map(|i| format!("m{i}")).collect(),
                ..ok
            },
        ];
        for case in cases {
            assert!(case.validate().is_err(), "{case:?}");
        }
    }

    #[test]
    fn the_cascade_models_skip_their_default_on_encode() {
        let value = serde_json::to_value(SamplingPolicy::default()).unwrap();
        assert!(value.get("cascade_models").is_none());
        let policy = SamplingPolicy {
            cascade_models: BTreeSet::from(["cheap".to_owned()]),
            ..SamplingPolicy::default()
        };
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(value["cascade_models"], serde_json::json!(["cheap"]));
        assert_eq!(
            serde_json::from_value::<SamplingPolicy>(value).unwrap(),
            policy
        );
        assert!(SAMPLING_POLICY_FIELDS.contains(&"cascade_models"));
    }

    #[test]
    fn validate_checks_each_cascade_model() {
        let bad = [
            BTreeSet::from([String::new()]),
            BTreeSet::from(["a\u{7}b".to_owned()]),
            BTreeSet::from(["m".repeat(MAX_ITEM_LEN + 1)]),
        ];
        for models in bad {
            let policy = SamplingPolicy {
                cascade_models: models,
                ..SamplingPolicy::default()
            };
            assert_eq!(policy.validate(), Err(PolicyError::CascadeModel));
        }
        let many = SamplingPolicy {
            cascade_models: (0..=MAX_SET_LEN).map(|i| format!("m{i}")).collect(),
            ..SamplingPolicy::default()
        };
        assert_eq!(
            many.validate(),
            Err(PolicyError::TooMany {
                field: "cascade_models"
            })
        );
    }

    #[test]
    fn the_field_guard_names_the_field() {
        let value = serde_json::json!({"input": "x", "max_draws": 3});
        let err = refuse_policy_fields::<serde_json::Error>(&value, "an AgentTask").unwrap_err();
        assert!(err.to_string().contains("max_draws"));
        assert!(
            refuse_policy_fields::<serde_json::Error>(&serde_json::json!({"input": "x"}), "x")
                .is_ok()
        );
        assert!(refuse_policy_fields::<serde_json::Error>(&serde_json::json!(3), "x").is_ok());
    }
}
