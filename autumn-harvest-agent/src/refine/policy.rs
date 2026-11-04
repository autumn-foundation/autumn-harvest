//! The operator refine policy of each agent kind (issue #2394).
//!
//! An operator sets a [`RefinePolicy`] for each kind. Each store of overlay
//! versions holds it, next to the versions. A missing policy means every
//! default, and the defaults never let an overlay go live without a person.
//!
//! A refine run reads the policy once, through the recorded activity
//! `agent_refine_policy`. An op reads it with its overlay, in the recorded
//! activity `agent_profile_overlay`. No `ProfileOverlay`, refine input or
//! `AgentOp` carries a policy field, so the loop cannot widen its own gate.
//!
//! The kernels [`parts_allowed`], [`decide_adoption`] and
//! [`gate_timed_out`] are pure. `formal/verus/src/refine_policy.rs` proves
//! them. The design is in `docs/design/DESIGN-2394.md`.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use autumn_harvest::chrono::{DateTime, TimeDelta, Utc};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::propose::ProposalAttempts;
use crate::agent::AgentProfile;
use crate::agent::overlay::{Ceilings, ProfileOverlay};
use crate::mesh::MAX_NAME_LEN;
use crate::policy::{Rule, ToolRules};
use crate::tool::ToolEffect;

/// The most auto adoptions that a policy may allow in one UTC day.
pub const MAX_AUTO_ADOPT_PER_DAY: u32 = 1_000;

/// The default cap of auto adoptions in one UTC day.
pub const DEFAULT_AUTO_ADOPT_PER_DAY: u32 = 1;

/// The shortest approval timeout.
pub const MIN_APPROVAL_TIMEOUT: Duration = Duration::from_mins(1);

/// The longest approval timeout: 90 days.
pub const MAX_APPROVAL_TIMEOUT: Duration = Duration::from_hours(90 * 24);

/// The default approval timeout: 7 days.
pub const DEFAULT_APPROVAL_TIMEOUT: Duration = Duration::from_hours(7 * 24);

/// The default size of the lesson library.
pub const DEFAULT_LESSON_ENTRIES: u32 = 64;

/// The largest lesson library.
pub const MAX_LESSON_ENTRIES: u32 = 4_096;

/// The most models in [`PolicyCeilings::models`].
pub const MAX_POLICY_MODELS: usize = 64;

/// The top-level keys of a [`RefinePolicy`], and the name of the policy.
///
/// A `ProfileOverlay`, a refine input and an `AgentOp` refuse each one.
pub const POLICY_KEYS: [&str; 7] = [
    "auto_adopt",
    "approval_timeout",
    "probe_effects",
    "ceilings",
    "proposal_attempts",
    "lessons",
    "refine_policy",
];

/// The first key of `value` that names a policy setting, or `None`.
///
/// A decoder of a type that must never carry a policy calls it first.
#[must_use]
pub fn policy_key(value: &Value) -> Option<&'static str> {
    let object = value.as_object()?;
    POLICY_KEYS
        .into_iter()
        .find(|key| object.contains_key(*key))
}

/// One part of an overlay that [`AutoAdopt::parts`] can name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlayPart {
    /// The system prompt.
    System,
    /// The guardrails.
    Guardrails,
    /// The tool guidance.
    ToolGuidance,
    /// The skills.
    Skills,
    /// The model.
    Model,
    /// `max_steps`, `max_total_tokens` and `max_output_tokens`.
    Bounds,
    /// The `read_only` flag.
    ReadOnly,
}

impl OverlayPart {
    /// Every part, in bit order.
    pub const ALL: [Self; 7] = [
        Self::System,
        Self::Guardrails,
        Self::ToolGuidance,
        Self::Skills,
        Self::Model,
        Self::Bounds,
        Self::ReadOnly,
    ];

    /// The bit of the part in a part mask.
    #[must_use]
    pub const fn bit(self) -> u8 {
        1 << self as u8
    }

    /// The name of the part, as JSON writes it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Guardrails => "guardrails",
            Self::ToolGuidance => "tool_guidance",
            Self::Skills => "skills",
            Self::Model => "model",
            Self::Bounds => "bounds",
            Self::ReadOnly => "read_only",
        }
    }
}

impl std::fmt::Display for OverlayPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// The mask of `parts`: one bit for each part.
#[must_use]
pub fn mask(parts: &BTreeSet<OverlayPart>) -> u8 {
    parts.iter().fold(0, |mask, part| mask | part.bit())
}

/// Each part of `candidate` that differs from `active`.
///
/// `bounds` covers the three `max_*` fields.
#[must_use]
pub fn changed_parts(active: &ProfileOverlay, candidate: &ProfileOverlay) -> BTreeSet<OverlayPart> {
    // A new overlay field fails to compile here until it maps to a part.
    let ProfileOverlay {
        system,
        guardrails,
        model,
        max_steps,
        max_total_tokens,
        max_output_tokens,
        read_only,
        tool_guidance,
        skills,
    } = candidate;
    let bounds = (*max_steps, *max_total_tokens, *max_output_tokens)
        != (
            active.max_steps,
            active.max_total_tokens,
            active.max_output_tokens,
        );
    [
        (OverlayPart::System, *system != active.system),
        (OverlayPart::Guardrails, *guardrails != active.guardrails),
        (
            OverlayPart::ToolGuidance,
            *tool_guidance != active.tool_guidance,
        ),
        (OverlayPart::Skills, *skills != active.skills),
        (OverlayPart::Model, *model != active.model),
        (OverlayPart::Bounds, bounds),
        (OverlayPart::ReadOnly, *read_only != active.read_only),
    ]
    .into_iter()
    .filter_map(|(part, changed)| changed.then_some(part))
    .collect()
}

/// Whether each bit of `changed` is in `allowed`.
///
/// `formal/verus/src/refine_policy.rs` proves it.
#[must_use]
pub const fn parts_allowed(changed: u8, allowed: u8) -> bool {
    changed & !allowed == 0
}

/// What [`decide_adoption`] decides for one refined overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdoptDecision {
    /// The overlay goes live without a person.
    Adopt,
    /// The overlay changes no part. Nothing goes live.
    Unchanged,
    /// A changed part is not in the allowed parts. A person decides.
    NeedsApproval,
    /// The daily cap is full. A person decides.
    CapReached,
}

/// The decision for an overlay that changes the parts `changed`.
///
/// `allowed` is the mask of the allowed parts. `used_today` counts the
/// auto adoptions of the UTC day. `cap` is the daily cap, or `None` for no
/// cap. An overlay that changes nothing is `Unchanged`, so an empty change
/// never goes live and never counts.
///
/// `formal/verus/src/refine_policy.rs` proves it.
#[must_use]
pub const fn decide_adoption(
    allowed: u8,
    changed: u8,
    used_today: u32,
    cap: Option<u32>,
) -> AdoptDecision {
    if changed == 0 {
        AdoptDecision::Unchanged
    } else if !parts_allowed(changed, allowed) {
        AdoptDecision::NeedsApproval
    } else {
        match cap {
            Some(limit) => {
                if used_today < limit {
                    AdoptDecision::Adopt
                } else {
                    AdoptDecision::CapReached
                }
            }
            None => AdoptDecision::Adopt,
        }
    }
}

/// Whether a gate that opened at `requested_ms` timed out at `now_ms`.
///
/// It times out when `now_ms` reaches `requested_ms + timeout_ms`. The
/// check does not overflow. `formal/verus/src/refine_policy.rs` proves it.
#[must_use]
pub const fn gate_timed_out(requested_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms >= requested_ms && now_ms - requested_ms >= timeout_ms
}

/// The state of an approval gate at one time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum GateState {
    /// The gate waits until `until`.
    Waiting {
        /// The time at which the gate times out.
        until: DateTime<Utc>,
    },
    /// The gate waited for its whole timeout.
    TimedOut,
}

/// The state at `now` of a gate that opened at `requested_at`.
///
/// A time before 1970 counts as 1970.
#[must_use]
pub fn gate_state(
    requested_at: DateTime<Utc>,
    now: DateTime<Utc>,
    timeout: ApprovalTimeout,
) -> GateState {
    let millis = |at: DateTime<Utc>| u64::try_from(at.timestamp_millis()).unwrap_or(0);
    let timeout_ms = u64::try_from(timeout.get().as_millis()).unwrap_or(u64::MAX);
    if gate_timed_out(millis(requested_at), millis(now), timeout_ms) {
        return GateState::TimedOut;
    }
    let wait = TimeDelta::from_std(timeout.get()).unwrap_or(TimeDelta::MAX);
    let until = requested_at
        .checked_add_signed(wait)
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    GateState::Waiting { until }
}

/// The cap of auto adoptions in one UTC day.
///
/// JSON writes a number from 0 to [`MAX_AUTO_ADOPT_PER_DAY`], or
/// `"unlimited"`. A cap of 0 adopts nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DailyCap {
    /// At most this many auto adoptions.
    Limited(u32),
    /// No cap.
    Unlimited,
}

impl DailyCap {
    /// The cap `n`.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError`] when `n` is above [`MAX_AUTO_ADOPT_PER_DAY`].
    pub const fn limited(n: u32) -> Result<Self, PolicyError> {
        if n <= MAX_AUTO_ADOPT_PER_DAY {
            Ok(Self::Limited(n))
        } else {
            Err(PolicyError::DailyCap(n))
        }
    }

    /// The stricter cap of `self` and `other`.
    #[must_use]
    pub const fn stricter(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unlimited, cap) | (cap, Self::Unlimited) => cap,
            (Self::Limited(a), Self::Limited(b)) => Self::Limited(if a < b { a } else { b }),
        }
    }

    /// The cap as a number, or `None` for no cap.
    #[must_use]
    pub const fn limit(self) -> Option<u32> {
        match self {
            Self::Limited(n) => Some(n),
            Self::Unlimited => None,
        }
    }
}

impl Default for DailyCap {
    fn default() -> Self {
        Self::Limited(DEFAULT_AUTO_ADOPT_PER_DAY)
    }
}

impl std::fmt::Display for DailyCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limited(n) => write!(f, "{n}"),
            Self::Unlimited => f.write_str("unlimited"),
        }
    }
}

impl Serialize for DailyCap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Limited(n) => serializer.serialize_u32(*n),
            Self::Unlimited => serializer.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for DailyCap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Wire {
            Number(u32),
            Text(String),
        }
        match Wire::deserialize(deserializer)? {
            Wire::Number(n) => Self::limited(n).map_err(D::Error::custom),
            Wire::Text(text) if text == "unlimited" => Ok(Self::Unlimited),
            Wire::Text(text) => Err(D::Error::custom(format!(
                "max_per_day must be a number or \"unlimited\", not {text:?}"
            ))),
        }
    }
}

/// Why a policy value is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PolicyError {
    /// A daily cap above [`MAX_AUTO_ADOPT_PER_DAY`].
    #[error("max_per_day must be from 0 to {MAX_AUTO_ADOPT_PER_DAY}, or \"unlimited\", not {0}")]
    DailyCap(u32),
    /// An approval timeout that does not parse or is out of range.
    #[error("approval_timeout must be from 1m to 90d, as <n><s|m|h|d>, not {0:?}")]
    ApprovalTimeout(String),
    /// A lesson library size out of range.
    #[error("lessons.max_entries must be from 1 to {MAX_LESSON_ENTRIES}, not {0}")]
    LessonEntries(u32),
    /// A model list that is empty, long, or holds a bad name.
    #[error("ceilings.models is not valid: {0}")]
    Models(&'static str),
}

/// How long the adoption gate waits: [`MIN_APPROVAL_TIMEOUT`] to
/// [`MAX_APPROVAL_TIMEOUT`].
///
/// JSON writes `<n><unit>`, with the unit `s`, `m`, `h` or `d`, such as
/// `"1h"` or `"7d"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApprovalTimeout(Duration);

impl ApprovalTimeout {
    /// The timeout `duration`.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError::ApprovalTimeout`] when it is out of range.
    pub fn new(duration: Duration) -> Result<Self, PolicyError> {
        if (MIN_APPROVAL_TIMEOUT..=MAX_APPROVAL_TIMEOUT).contains(&duration) {
            Ok(Self(duration))
        } else {
            Err(PolicyError::ApprovalTimeout(format!(
                "{}s",
                duration.as_secs()
            )))
        }
    }

    /// The timeout.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for ApprovalTimeout {
    fn default() -> Self {
        Self(DEFAULT_APPROVAL_TIMEOUT)
    }
}

impl std::fmt::Display for ApprovalTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secs = self.0.as_secs();
        let (n, unit) = [(86_400, 'd'), (3_600, 'h'), (60, 'm')]
            .into_iter()
            .find(|(size, _)| secs.is_multiple_of(*size))
            .map_or((secs, 's'), |(size, unit)| (secs / size, unit));
        write!(f, "{n}{unit}")
    }
}

impl std::str::FromStr for ApprovalTimeout {
    type Err = PolicyError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let refuse = || PolicyError::ApprovalTimeout(text.chars().take(32).collect());
        let split = text.len().checked_sub(1).ok_or_else(refuse)?;
        let (digits, unit) = text.split_at_checked(split).ok_or_else(refuse)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(refuse());
        }
        let n: u64 = digits.parse().map_err(|_| refuse())?;
        let size = match unit {
            "s" => 1,
            "m" => 60,
            "h" => 3_600,
            "d" => 86_400,
            _ => return Err(refuse()),
        };
        let secs = n.checked_mul(size).ok_or_else(refuse)?;
        Self::new(Duration::from_secs(secs)).map_err(|_| refuse())
    }
}

impl Serialize for ApprovalTimeout {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ApprovalTimeout {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

/// The overlay parts that may go live without a person, and the daily cap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoAdopt {
    /// The parts that may go live without a person. Empty by default, so
    /// every overlay waits.
    #[serde(default)]
    pub parts: BTreeSet<OverlayPart>,
    /// The cap of auto adoptions in one UTC day.
    #[serde(default)]
    pub max_per_day: DailyCap,
}

/// The models that an overlay may name: 1 to [`MAX_POLICY_MODELS`] names.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "BTreeSet<String>", into = "BTreeSet<String>")]
pub struct PolicyModels(BTreeSet<String>);

impl PolicyModels {
    /// The model list `names`.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError::Models`] for an empty or long list, or a name
    /// that is empty, long, or holds a control character.
    pub fn new(names: BTreeSet<String>) -> Result<Self, PolicyError> {
        if names.is_empty() {
            return Err(PolicyError::Models("the list is empty"));
        }
        if names.len() > MAX_POLICY_MODELS {
            return Err(PolicyError::Models("the list has more than 64 names"));
        }
        let bad = names.iter().any(|name| {
            name.is_empty()
                || name.chars().count() > MAX_NAME_LEN
                || name.chars().any(char::is_control)
        });
        if bad {
            return Err(PolicyError::Models(
                "a name is empty, long, or holds a control character",
            ));
        }
        Ok(Self(names))
    }

    /// The names.
    #[must_use]
    pub const fn names(&self) -> &BTreeSet<String> {
        &self.0
    }
}

impl TryFrom<BTreeSet<String>> for PolicyModels {
    type Error = PolicyError;

    fn try_from(names: BTreeSet<String>) -> Result<Self, Self::Error> {
        Self::new(names)
    }
}

impl From<PolicyModels> for BTreeSet<String> {
    fn from(models: PolicyModels) -> Self {
        models.0
    }
}

/// The highest bounds and the models that an overlay may set.
///
/// Each `None` value is the profile value. So the default policy lets an
/// overlay narrow its profile only. A set value replaces the profile value,
/// and it may be above it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyCeilings {
    /// The highest `max_steps`.
    #[serde(default)]
    pub max_steps: Option<NonZeroU32>,
    /// The highest `max_total_tokens`.
    #[serde(default)]
    pub max_total_tokens: Option<NonZeroU32>,
    /// The highest `max_output_tokens`.
    #[serde(default)]
    pub max_output_tokens: Option<NonZeroU32>,
    /// The models that an overlay may name.
    #[serde(default)]
    pub models: Option<PolicyModels>,
}

impl PolicyCeilings {
    /// The ceilings of `profile` under this policy.
    ///
    /// It starts from [`Ceilings::from_profile`]. Each set value replaces
    /// the profile value. `allow_clear_read_only` stays `false`.
    #[must_use]
    pub fn resolve(&self, profile: &AgentProfile) -> Ceilings {
        let base = Ceilings::from_profile(profile);
        let pick = |set: Option<NonZeroU32>, profile: Option<u32>| {
            set.map_or(profile, |value| Some(value.get()))
        };
        Ceilings {
            max_steps: pick(self.max_steps, base.max_steps),
            max_total_tokens: pick(self.max_total_tokens, base.max_total_tokens),
            max_output_tokens: pick(self.max_output_tokens, base.max_output_tokens),
            models: self
                .models
                .as_ref()
                .map_or(base.models, |models| models.names().clone()),
            allow_clear_read_only: false,
        }
    }
}

/// The size of the lesson library: 1 to [`MAX_LESSON_ENTRIES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct LessonEntries(u32);

impl LessonEntries {
    /// The size `n`.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError::LessonEntries`] when `n` is out of range.
    pub const fn new(n: u32) -> Result<Self, PolicyError> {
        if n >= 1 && n <= MAX_LESSON_ENTRIES {
            Ok(Self(n))
        } else {
            Err(PolicyError::LessonEntries(n))
        }
    }

    /// The size.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for LessonEntries {
    fn default() -> Self {
        Self(DEFAULT_LESSON_ENTRIES)
    }
}

impl TryFrom<u32> for LessonEntries {
    type Error = PolicyError;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::new(n)
    }
}

impl From<LessonEntries> for u32 {
    fn from(entries: LessonEntries) -> Self {
        entries.0
    }
}

/// The lesson library settings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lessons {
    /// The size of the lesson library.
    #[serde(default)]
    pub max_entries: LessonEntries,
}

/// Every [`ToolEffect`]. The `match` makes a new variant fail to compile
/// here, so [`RefinePolicy::probe_rules`] never lets a new effect through.
const fn every_effect() -> [ToolEffect; 4] {
    match ToolEffect::ReadOnly {
        ToolEffect::ReadOnly | ToolEffect::Internal | ToolEffect::Write | ToolEffect::External => {}
    }
    [
        ToolEffect::ReadOnly,
        ToolEffect::Internal,
        ToolEffect::Write,
        ToolEffect::External,
    ]
}

fn default_probe_effects() -> BTreeSet<ToolEffect> {
    BTreeSet::from([ToolEffect::ReadOnly])
}

/// The refine policy of one agent kind.
///
/// Each key is optional and takes its default. An unknown key fails to
/// decode, so an old worker never drops a setting that a new one wrote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefinePolicy {
    /// What may go live without a person.
    #[serde(default)]
    pub auto_adopt: AutoAdopt,
    /// How long the adoption gate waits.
    #[serde(default)]
    pub approval_timeout: ApprovalTimeout,
    /// The tool effects that a live probe may run.
    #[serde(default = "default_probe_effects")]
    pub probe_effects: BTreeSet<ToolEffect>,
    /// The highest bounds and the models that an overlay may set.
    #[serde(default)]
    pub ceilings: PolicyCeilings,
    /// The attempts of each proposer stage before it inherits.
    #[serde(default)]
    pub proposal_attempts: ProposalAttempts,
    /// The lesson library settings.
    #[serde(default)]
    pub lessons: Lessons,
}

impl Default for RefinePolicy {
    fn default() -> Self {
        Self {
            auto_adopt: AutoAdopt::default(),
            approval_timeout: ApprovalTimeout::default(),
            probe_effects: default_probe_effects(),
            ceilings: PolicyCeilings::default(),
            proposal_attempts: ProposalAttempts::default(),
            lessons: Lessons::default(),
        }
    }
}

impl RefinePolicy {
    /// The decision for `candidate` over `active`, with `used_today` auto
    /// adoptions in the UTC day.
    #[must_use]
    pub fn adoption(
        &self,
        active: &ProfileOverlay,
        candidate: &ProfileOverlay,
        used_today: u32,
    ) -> AdoptDecision {
        decide_adoption(
            mask(&self.auto_adopt.parts),
            mask(&changed_parts(active, candidate)),
            used_today,
            self.auto_adopt.max_per_day.limit(),
        )
    }

    /// Whether a live probe may run a tool with `effect`.
    #[must_use]
    pub fn probe_allows(&self, effect: ToolEffect) -> bool {
        self.probe_effects.contains(&effect)
    }

    /// The tool rules of a live probe.
    ///
    /// A tool with a listed effect runs. A tool with another effect, and a
    /// tool that the harness does not know, is denied. A probe puts the
    /// rules on top of the app policy with
    /// [`Strictest`](crate::policy::Strictest).
    #[must_use]
    pub fn probe_rules(&self) -> ToolRules {
        let deny = |effect: ToolEffect| {
            Rule::Deny(format!(
                "the refine policy does not let a probe run a {effect:?} tool"
            ))
        };
        let rules = every_effect()
            .into_iter()
            .filter(|effect| !self.probe_allows(*effect))
            .fold(ToolRules::new(), |rules, effect| {
                rules.effect(effect, deny(effect))
            });
        rules.unknown(Rule::Deny("a probe may only call known tools".to_owned()))
    }

    /// The state at `now` of a gate that opened at `requested_at`.
    #[must_use]
    pub fn gate_state(&self, requested_at: DateTime<Utc>, now: DateTime<Utc>) -> GateState {
        gate_state(requested_at, now, self.approval_timeout)
    }
}

/// One setting that differs between two policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PolicyChange {
    /// The dotted path of the setting, such as `auto_adopt.max_per_day`.
    pub path: String,
    /// The old value.
    pub old: Value,
    /// The new value.
    pub new: Value,
}

/// Each setting that differs between `old` and `new`, in key order.
#[must_use]
pub fn policy_changes(old: &RefinePolicy, new: &RefinePolicy) -> Vec<PolicyChange> {
    let encode = |policy: &RefinePolicy| serde_json::to_value(policy).unwrap_or(Value::Null);
    let mut out = Vec::new();
    walk("", &encode(old), &encode(new), &mut out);
    out
}

/// Add each leaf of `old` and `new` that differs to `out`. It goes down
/// into each object that both sides hold. A list is one leaf.
fn walk(prefix: &str, old: &Value, new: &Value, out: &mut Vec<PolicyChange>) {
    match (old, new) {
        (Value::Object(left), Value::Object(right)) => {
            let keys: BTreeSet<&String> = left.keys().chain(right.keys()).collect();
            for key in keys {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                let (a, b) = (left.get(key), right.get(key));
                walk(
                    &path,
                    a.unwrap_or(&Value::Null),
                    b.unwrap_or(&Value::Null),
                    out,
                );
            }
        }
        _ if old != new => out.push(PolicyChange {
            path: prefix.to_owned(),
            old: old.clone(),
            new: new.clone(),
        }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::agent::overlay::{OverlayError, apply};
    use crate::message::ToolCall;
    use crate::tool::FnTool;
    use proptest::prelude::*;
    use serde_json::json;

    fn system(text: &str) -> ProfileOverlay {
        ProfileOverlay {
            system: Some(text.to_owned()),
            ..ProfileOverlay::default()
        }
    }

    fn allowing(parts: &[OverlayPart]) -> RefinePolicy {
        RefinePolicy {
            auto_adopt: AutoAdopt {
                parts: parts.iter().copied().collect(),
                max_per_day: DailyCap::default(),
            },
            ..RefinePolicy::default()
        }
    }

    fn decode(value: Value) -> Result<RefinePolicy, serde_json::Error> {
        serde_json::from_value(value)
    }

    #[test]
    fn defaults_hold_for_every_setting() {
        let policy = decode(json!({})).expect("an empty policy decodes");
        assert_eq!(policy, RefinePolicy::default());
        assert!(policy.auto_adopt.parts.is_empty());
        assert_eq!(policy.auto_adopt.max_per_day, DailyCap::Limited(1));
        assert_eq!(policy.approval_timeout.get(), Duration::from_hours(7 * 24));
        assert_eq!(policy.probe_effects, BTreeSet::from([ToolEffect::ReadOnly]));
        assert_eq!(policy.ceilings, PolicyCeilings::default());
        assert_eq!(policy.proposal_attempts.get(), 2);
        assert_eq!(policy.lessons.max_entries.get(), 64);
        assert_eq!(
            serde_json::to_value(&policy).expect("encode"),
            json!({
                "auto_adopt": {"parts": [], "max_per_day": 1},
                "approval_timeout": "7d",
                "probe_effects": ["read_only"],
                "ceilings": {
                    "max_steps": null,
                    "max_total_tokens": null,
                    "max_output_tokens": null,
                    "models": null
                },
                "proposal_attempts": 2,
                "lessons": {"max_entries": 64}
            })
        );
    }

    #[test]
    fn defaults_make_a_system_overlay_wait() {
        let policy = RefinePolicy::default();
        assert_eq!(
            policy.adoption(&ProfileOverlay::default(), &system("New."), 0),
            AdoptDecision::NeedsApproval
        );
    }

    #[test]
    fn a_system_policy_adopts_only_a_system_change() {
        let policy = allowing(&[OverlayPart::System]);
        let base = system("Old.");
        assert_eq!(
            policy.adoption(&base, &system("New."), 0),
            AdoptDecision::Adopt
        );
        let wider = ProfileOverlay {
            tool_guidance: [("search".to_owned(), "Use it.".to_owned())].into(),
            ..system("New.")
        };
        assert_eq!(
            policy.adoption(&base, &wider, 0),
            AdoptDecision::NeedsApproval
        );
    }

    #[test]
    fn a_system_and_model_policy_adopts_both() {
        let policy = allowing(&[OverlayPart::System, OverlayPart::Model]);
        let candidate = ProfileOverlay {
            model: Some("m2".to_owned()),
            ..system("New.")
        };
        assert_eq!(
            policy.adoption(&ProfileOverlay::default(), &candidate, 0),
            AdoptDecision::Adopt
        );
    }

    #[test]
    fn the_daily_cap_counts_and_unlimited_does_not() {
        let mut policy = allowing(&[OverlayPart::System]);
        let (base, next) = (system("Old."), system("New."));
        assert_eq!(policy.adoption(&base, &next, 0), AdoptDecision::Adopt);
        assert_eq!(policy.adoption(&base, &next, 1), AdoptDecision::CapReached);
        policy.auto_adopt.max_per_day = DailyCap::Unlimited;
        assert_eq!(policy.adoption(&base, &next, 1), AdoptDecision::Adopt);
        assert_eq!(
            policy.adoption(&base, &next, u32::MAX),
            AdoptDecision::Adopt
        );
        policy.auto_adopt.max_per_day = DailyCap::Limited(0);
        assert_eq!(policy.adoption(&base, &next, 0), AdoptDecision::CapReached);
    }

    #[test]
    fn an_empty_change_never_goes_live() {
        let policy = allowing(&OverlayPart::ALL);
        let same = system("Same.");
        assert_eq!(policy.adoption(&same, &same, 0), AdoptDecision::Unchanged);
        assert_eq!(
            RefinePolicy::default().adoption(&same, &same, 0),
            AdoptDecision::Unchanged
        );
    }

    #[test]
    fn a_one_hour_gate_times_out_after_one_hour() {
        let policy = decode(json!({"approval_timeout": "1h"})).expect("decode");
        let opened = DateTime::parse_from_rfc3339("2026-11-03T10:00:00Z")
            .expect("time")
            .with_timezone(&Utc);
        let at = |minutes| opened + TimeDelta::minutes(minutes);
        assert_eq!(
            policy.gate_state(opened, at(59)),
            GateState::Waiting { until: at(60) }
        );
        assert_eq!(policy.gate_state(opened, at(60)), GateState::TimedOut);
        assert_eq!(policy.gate_state(opened, at(61)), GateState::TimedOut);
        assert_eq!(
            RefinePolicy::default().gate_state(opened, at(61)),
            GateState::Waiting {
                until: at(7 * 24 * 60)
            }
        );
    }

    #[test]
    fn a_clock_before_the_gate_still_waits() {
        let timeout = ApprovalTimeout::new(Duration::from_hours(1)).expect("1h");
        let opened = DateTime::from_timestamp(10_000, 0).expect("time");
        let earlier = DateTime::from_timestamp(5_000, 0).expect("time");
        assert!(matches!(
            gate_state(opened, earlier, timeout),
            GateState::Waiting { .. }
        ));
        assert!(!gate_timed_out(10, 5, 1));
        assert!(gate_timed_out(0, u64::MAX, u64::MAX));
        assert!(!gate_timed_out(u64::MAX, u64::MAX - 1, 0));
    }

    fn tool(name: &str, effect: ToolEffect) -> FnTool {
        FnTool::new(name, "A tool.", json!({"type": "object"}), |_| async {
            Ok(json!("ok"))
        })
        .effect(effect)
    }

    fn decided(rules: &ToolRules, tool: Option<&FnTool>, name: &str) -> bool {
        let call = ToolCall {
            id: "c1".to_owned(),
            name: name.to_owned(),
            arguments: json!({}),
        };
        let tool = tool.map(|tool| tool as &dyn crate::tool::Tool);
        matches!(
            rules.decide_now(&call, tool),
            crate::policy::ToolDecision::Allow
        )
    }

    #[test]
    fn probe_effects_gate_each_tool() {
        let internal = tool("note", ToolEffect::Internal);
        let write = tool("save", ToolEffect::Write);
        let read = tool("look", ToolEffect::ReadOnly);
        let wide = decode(json!({"probe_effects": ["read_only", "internal"]})).expect("decode");
        let rules = wide.probe_rules();
        assert!(decided(&rules, Some(&internal), "note"));
        assert!(decided(&rules, Some(&read), "look"));
        assert!(!decided(&rules, Some(&write), "save"));
        assert!(!decided(&rules, None, "ghost"), "an unknown tool is denied");
        assert!(wide.probe_allows(ToolEffect::Internal));
        assert!(!wide.probe_allows(ToolEffect::Write));

        let default = RefinePolicy::default();
        let rules = default.probe_rules();
        assert!(decided(&rules, Some(&read), "look"));
        assert!(!decided(&rules, Some(&internal), "note"));
        assert!(!decided(&rules, Some(&write), "save"));
        assert!(!default.probe_allows(ToolEffect::Internal));
        assert!(!default.probe_allows(ToolEffect::External));
    }

    fn registry() -> BTreeSet<String> {
        ["m1", "m2"].map(str::to_owned).into()
    }

    #[test]
    fn a_policy_ceiling_can_raise_the_profile_bound() {
        let profile = AgentProfile::new("support").max_steps(20).model("m1");
        let steps = |n| ProfileOverlay {
            max_steps: Some(n),
            ..ProfileOverlay::default()
        };
        let policy = decode(json!({"ceilings": {"max_steps": 40}})).expect("decode");
        let ceilings = policy.ceilings.resolve(&profile);
        assert_eq!(ceilings.max_steps, Some(40));
        assert!(apply(&profile, &steps(30), &ceilings, &registry()).is_ok());
        assert_eq!(
            apply(&profile, &steps(50), &ceilings, &registry()),
            Err(OverlayError::AboveCeiling { field: "max_steps" })
        );
        let narrow = RefinePolicy::default().ceilings.resolve(&profile);
        assert_eq!(narrow, Ceilings::from_profile(&profile));
        assert!(apply(&profile, &steps(30), &narrow, &registry()).is_err());
    }

    #[test]
    fn policy_models_replace_the_profile_model() {
        let profile = AgentProfile::new("support")
            .max_total_tokens(900)
            .model("m1");
        let policy = decode(json!({
            "ceilings": {"models": ["m1", "m2"], "max_total_tokens": 1000, "max_output_tokens": 50}
        }))
        .expect("decode");
        let ceilings = policy.ceilings.resolve(&profile);
        assert_eq!(ceilings.models, registry());
        assert_eq!(ceilings.max_total_tokens, Some(1_000));
        assert_eq!(ceilings.max_output_tokens, Some(50));
        assert_eq!(ceilings.max_steps, Some(profile.max_steps));
        assert!(!ceilings.allow_clear_read_only);
        // The profile sets no output cap, so the policy cap binds an
        // overlay that leaves it unset.
        let unset = ProfileOverlay {
            model: Some("m2".to_owned()),
            ..ProfileOverlay::default()
        };
        assert_eq!(
            apply(&profile, &unset, &ceilings, &registry()),
            Err(OverlayError::AboveCeiling {
                field: "max_output_tokens"
            })
        );
        let other = ProfileOverlay {
            max_output_tokens: Some(40),
            ..unset
        };
        assert!(apply(&profile, &other, &ceilings, &registry()).is_ok());
    }

    #[test]
    fn no_overlay_or_input_decodes_a_policy_key() {
        for key in POLICY_KEYS {
            let overlay = json!({ "system": "Hi.", key: {} });
            assert!(
                serde_json::from_value::<ProfileOverlay>(overlay).is_err(),
                "an overlay with {key} decodes"
            );
            let op = json!({"type": "user_message", "text": "hi", key: 1});
            assert!(
                serde_json::from_value::<crate::AgentOp>(op).is_err(),
                "an op with {key} decodes"
            );
            let control = json!({"type": "control", "action": "pause", key: 1});
            assert!(
                serde_json::from_value::<crate::AgentOp>(control).is_err(),
                "a control op with {key} decodes"
            );
            assert_eq!(policy_key(&json!({ key: 1 })), Some(key));
        }
        let op = json!({"type": "user_message", "text": "hi"});
        assert!(serde_json::from_value::<crate::AgentOp>(op).is_ok());
    }

    #[test]
    fn a_propose_request_refuses_each_policy_key() {
        let profile = AgentProfile::new("support").max_steps(5);
        let request = crate::refine::ProposeRequest {
            dimension: crate::refine::Dimension::Inv,
            evidence: crate::refine::Evidence::text("ran"),
            profile: profile.clone(),
            current: ProfileOverlay::default(),
            ceilings: Ceilings::from_profile(&profile),
            attempts: ProposalAttempts::default(),
        };
        let good = serde_json::to_value(&request).expect("encode");
        assert!(serde_json::from_value::<crate::refine::ProposeRequest>(good.clone()).is_ok());
        // `ceilings` is a field of the request: the resolved ceilings of
        // the recorded policy read. Each other key is refused.
        for key in POLICY_KEYS.into_iter().filter(|key| *key != "ceilings") {
            let mut bad = good.clone();
            bad[key] = json!(1);
            assert!(
                serde_json::from_value::<crate::refine::ProposeRequest>(bad).is_err(),
                "a request with {key} decodes"
            );
        }
    }

    #[test]
    fn each_bound_is_checked_when_it_decodes() {
        for bad in [
            json!({"auto_adopt": {"max_per_day": 1001}}),
            json!({"auto_adopt": {"max_per_day": "lots"}}),
            json!({"auto_adopt": {"parts": ["tools"]}}),
            json!({"auto_adopt": {"cap": 1}}),
            json!({"approval_timeout": "0s"}),
            json!({"approval_timeout": "59s"}),
            json!({"approval_timeout": "91d"}),
            json!({"approval_timeout": "1w"}),
            json!({"approval_timeout": "h"}),
            json!({"approval_timeout": "-1h"}),
            json!({"approval_timeout": "1 h"}),
            json!({"approval_timeout": 3600}),
            json!({"probe_effects": ["delete"]}),
            json!({"ceilings": {"max_steps": 0}}),
            json!({"ceilings": {"models": []}}),
            json!({"ceilings": {"models": [""]}}),
            json!({"ceilings": {"tools": 1}}),
            json!({"proposal_attempts": 0}),
            json!({"proposal_attempts": 9}),
            json!({"lessons": {"max_entries": 0}}),
            json!({"lessons": {"max_entries": 4097}}),
            json!({"unknown": 1}),
        ] {
            assert!(decode(bad.clone()).is_err(), "{bad} decodes");
        }
        let good = decode(json!({
            "auto_adopt": {"parts": ["system", "bounds"], "max_per_day": "unlimited"},
            "approval_timeout": "90d",
            "probe_effects": [],
            "proposal_attempts": 8,
            "lessons": {"max_entries": 4096}
        }))
        .expect("decode");
        assert_eq!(good.auto_adopt.max_per_day, DailyCap::Unlimited);
        assert!(good.probe_effects.is_empty());
    }

    #[test]
    fn a_timeout_writes_its_largest_whole_unit() {
        for (text, shown) in [
            ("60s", "1m"),
            ("120s", "2m"),
            ("90m", "90m"),
            ("24h", "1d"),
            ("36h", "36h"),
            ("7d", "7d"),
        ] {
            let timeout: ApprovalTimeout = text.parse().expect(text);
            assert_eq!(timeout.to_string(), shown);
            assert_eq!(shown.parse::<ApprovalTimeout>(), Ok(timeout));
        }
        assert!("99999999999999999999d".parse::<ApprovalTimeout>().is_err());
    }

    #[test]
    fn each_part_is_found_by_its_own_field() {
        let base = ProfileOverlay::default();
        let cases: [(ProfileOverlay, OverlayPart); 9] = [
            (system("New."), OverlayPart::System),
            (
                ProfileOverlay {
                    guardrails: Some("No.".to_owned()),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Guardrails,
            ),
            (
                ProfileOverlay {
                    tool_guidance: [("t".to_owned(), "x".to_owned())].into(),
                    ..ProfileOverlay::default()
                },
                OverlayPart::ToolGuidance,
            ),
            (
                ProfileOverlay {
                    skills: [(
                        "s".to_owned(),
                        crate::skill::SkillText::new("Sum.", "Body."),
                    )]
                    .into(),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Skills,
            ),
            (
                ProfileOverlay {
                    model: Some("m2".to_owned()),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Model,
            ),
            (
                ProfileOverlay {
                    max_steps: Some(3),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Bounds,
            ),
            (
                ProfileOverlay {
                    max_total_tokens: Some(3),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Bounds,
            ),
            (
                ProfileOverlay {
                    max_output_tokens: Some(3),
                    ..ProfileOverlay::default()
                },
                OverlayPart::Bounds,
            ),
            (
                ProfileOverlay {
                    read_only: Some(true),
                    ..ProfileOverlay::default()
                },
                OverlayPart::ReadOnly,
            ),
        ];
        for (candidate, part) in cases {
            assert_eq!(
                changed_parts(&base, &candidate),
                BTreeSet::from([part]),
                "{candidate:?}"
            );
            assert_eq!(
                changed_parts(&candidate, &base),
                BTreeSet::from([part]),
                "a removal is a change"
            );
        }
        assert!(changed_parts(&base, &base).is_empty());
    }

    #[test]
    fn a_policy_change_names_each_setting() {
        let old = RefinePolicy::default();
        let new = decode(json!({
            "auto_adopt": {"parts": ["system"], "max_per_day": "unlimited"},
            "ceilings": {"max_steps": 40}
        }))
        .expect("decode");
        let changes = policy_changes(&old, &new);
        let paths: Vec<_> = changes.iter().map(|change| change.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "auto_adopt.max_per_day",
                "auto_adopt.parts",
                "ceilings.max_steps"
            ]
        );
        assert_eq!(changes[0].old, json!(1));
        assert_eq!(changes[0].new, json!("unlimited"));
        assert_eq!(changes[2].old, Value::Null);
        assert_eq!(changes[2].new, json!(40));
        assert_eq!(policy_changes(&new, &new), []);
    }

    #[test]
    fn each_part_has_its_own_bit() {
        let all: BTreeSet<_> = OverlayPart::ALL.into_iter().collect();
        assert_eq!(mask(&all), 0b0111_1111);
        for part in OverlayPart::ALL {
            assert_eq!(part.bit().count_ones(), 1);
            let json = serde_json::to_value(part).expect("encode");
            assert_eq!(json, json!(part.name()));
        }
    }

    /// The rules of `decide_adoption`, written plainly.
    fn oracle(allowed: u8, changed: u8, used: u32, cap: Option<u32>) -> AdoptDecision {
        if changed == 0 {
            AdoptDecision::Unchanged
        } else if (0..8).any(|bit| changed & (1 << bit) != 0 && allowed & (1 << bit) == 0) {
            AdoptDecision::NeedsApproval
        } else if cap.is_some_and(|cap| used >= cap) {
            AdoptDecision::CapReached
        } else {
            AdoptDecision::Adopt
        }
    }

    proptest! {
        #[test]
        fn the_kernel_matches_the_oracle(
            allowed in any::<u8>(),
            changed in any::<u8>(),
            used in any::<u32>(),
            cap in proptest::option::of(any::<u32>()),
        ) {
            prop_assert_eq!(
                decide_adoption(allowed, changed, used, cap),
                oracle(allowed, changed, used, cap)
            );
            prop_assert_ne!(decide_adoption(0, changed, used, cap), AdoptDecision::Adopt);
        }

        #[test]
        fn the_gate_matches_wide_arithmetic(
            requested in any::<u64>(),
            now in any::<u64>(),
            timeout in any::<u64>(),
        ) {
            let want = u128::from(now) >= u128::from(requested) + u128::from(timeout);
            prop_assert_eq!(gate_timed_out(requested, now, timeout), want);
        }
    }
}
