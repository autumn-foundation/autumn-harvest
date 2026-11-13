//! Payload-schema compatibility contract and differ (issue #794).
//!
//! A payload change can break production in silence. Change the serde shape
//! of a workflow payload type and ship it. **In-flight executions then DLQ on
//! the next replay**, because recorded JSON in `harvest_events` no longer
//! deserializes into the new struct. `cargo build` stays green.
//!
//! Issue #373 already publishes each workflow's `input_schema` /
//! `output_schema` / `error_schema`. This module checks those schemas in as a
//! machine-readable **baseline** and diffs a later revision against it. It
//! classifies every delta as `compatible` or `breaking` from the
//! **replay-read perspective**: *old recorded JSON must still deserialize into
//! the new type*.
//!
//! # Scope of the analysis
//!
//! The differ covers the JSON Schema subset that the engine's
//! [`validate_against_schema`](crate::info::validate_against_schema)
//! enforces: `type`, `required`, `properties`, `enum`, `items`,
//! `additionalProperties`, `allOf`/`anyOf`/`oneOf`, `minLength`/`maxLength`,
//! `minimum`/`maximum`, and local `$ref`. It also reads numeric `format`. In a
//! `schemars`-derived schema, that is the **only** signal that tells `i64` from
//! `i32`. Every other keyword is either a pure annotation (stripped) or an
//! unanalysed constraint (reported breaking, fail closed).
//!
//! # Trust boundary
//!
//! The gate diffs the **published schema**, not the Rust type. A hand-written
//! [`with_input_schema_fn`](crate::info::WorkflowInfo::with_input_schema_fn)
//! schema — or a `#[schemars(with = "...")]` override — is *prose about* a type
//! and can silently disagree with it. Such a payload is outside the gate's
//! coverage. Deleting a schema to silence the gate is itself reported as
//! breaking ([`ChangeKind::SchemaRemoved`]).
//!
//! # No engine impact
//!
//! Pure `serde_json` analysis: no new `WorkflowEvent` variant, no event-schema
//! change, no migration, no database, no network. It is read-only.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::info::{ActivityInfo, SideEffectInfo, WorkflowInfo};

/// Schema-contract format version. Bump on a breaking change to the *artifact*
/// shape (not to a workflow's payload schema).
///
/// Version `2` adds the `activities` and `side_effects` sections (issue #1994).
/// A version `1` binary ignores unknown keys, so it would skip those sections
/// in silence. The bump makes it refuse the file instead.
pub const SCHEMA_CONTRACT_VERSION: &str = "2";

/// Older format versions that this build still reads.
///
/// Version `1` is version `2` with no `activities` and no `side_effects`, so
/// it reads as a contract with both sections empty.
const READABLE_LEGACY_CONTRACT_VERSIONS: &[&str] = &["1"];

/// Default checked-in baseline path, relative to the repository root.
pub const DEFAULT_SCHEMA_CONTRACT_PATH: &str = "docs/workflow-schema-contract.json";

/// Maximum recursion depth for the differ.
///
/// Mirrors [`crate::info::MAX_VALIDATION_DEPTH`] so the differ and the engine's
/// validator agree on how deep a payload schema may be. A recursive Rust type
/// (`struct Node { children: Vec<Node> }`) emits a genuine `$ref` cycle. So
/// the bound is mandatory. A hang in CI gets the gate deleted the same day.
const MAX_DIFF_DEPTH: usize = 128;

/// Hard cap on **stored** deltas, so a pathological pair of schemas cannot
/// produce an unbounded report.
///
/// This caps the report, never the verdict: [`SchemaContractDiff::breaking_count`]
/// keeps counting past it, and [`SchemaContractDiff::truncated`] marks the
/// report incomplete so callers can fail closed.
pub const MAX_DELTAS: usize = 10_000;

/// Which published schema a delta belongs to.
///
/// A workflow publishes `input`, `output` and `error`. An activity publishes
/// `input` and `output`. A side effect publishes `value` (issue #1994).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaRole {
    /// The input payload. A workflow reads it from `WorkflowStarted`. A
    /// retried activity reads its stored input.
    Input,
    /// The output payload. `WorkflowCompleted` records a workflow output.
    /// `ActivityCompleted` records an activity result, and the workflow decodes
    /// it on replay.
    Output,
    /// The workflow's error payload — read from `WorkflowFailed`.
    Error,
    /// A side-effect value, read from `SideEffectRecorded` (issue #1994).
    Value,
}

impl SchemaRole {
    /// Stable lowercase slug used in the artifact and in human output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
            Self::Error => "error",
            Self::Value => "value",
        }
    }

    /// The three workflow roles, in a stable order.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::Input, Self::Output, Self::Error]
    }

    /// The two activity roles, in a stable order (issue #1994).
    #[must_use]
    pub const fn activity() -> [Self; 2] {
        [Self::Input, Self::Output]
    }
}

/// What kind of registered item a delta or an acknowledgement is about
/// (issue #1994).
///
/// The default is [`Self::Workflow`]. It is left out of the JSON, so a
/// workflow delta serialises as it did before issue #1994.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SchemaSubject {
    /// A registered workflow type.
    #[default]
    Workflow,
    /// A registered activity type.
    Activity,
    /// A `ctx.side_effect(id, ..)` call site of one workflow.
    SideEffect,
}

impl SchemaSubject {
    /// Stable `snake_case` slug used in the artifact and in human output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::Activity => "activity",
            Self::SideEffect => "side_effect",
        }
    }

    /// `true` for [`Self::Workflow`]. Used to omit the default from the JSON.
    #[must_use]
    #[allow(clippy::trivially_copy_pass_by_ref)] // serde passes a reference
    pub const fn is_workflow(&self) -> bool {
        matches!(self, Self::Workflow)
    }
}

impl std::fmt::Display for SchemaSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The item a schema belongs to, as the differ sees it (issue #1994).
#[derive(Clone, Copy)]
struct Subject<'a> {
    kind: SchemaSubject,
    /// Workflow or activity name. For a side effect, the owning workflow.
    name: &'a str,
    side_effect: Option<&'a str>,
}

impl Subject<'_> {
    /// A delta about this subject with no payload path.
    fn delta(
        self,
        role: Option<SchemaRole>,
        change: ChangeKind,
        verdict: Verdict,
        reason: String,
    ) -> SchemaDelta {
        SchemaDelta {
            subject: self.kind,
            workflow: self.name.to_string(),
            side_effect: self.side_effect.map(str::to_string),
            role,
            field_path: String::new(),
            change,
            verdict,
            reason,
        }
    }
}

/// Human label for a subject, used in reports.
///
/// A workflow is `onboarding`. An activity is `activity:charge`. A side effect
/// is `onboarding/side_effect:pick`.
#[must_use]
pub fn subject_label(subject: SchemaSubject, name: &str, side_effect: Option<&str>) -> String {
    match subject {
        SchemaSubject::Workflow => name.to_string(),
        SchemaSubject::Activity => format!("activity:{name}"),
        SchemaSubject::SideEffect => {
            format!("{name}/side_effect:{}", side_effect.unwrap_or_default())
        }
    }
}

impl std::fmt::Display for SchemaRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Verdict for a single delta.
///
/// Deliberately two-valued to match the exit-code contract: `0` when every
/// delta is [`Compatible`](Verdict::Compatible), `1` when any delta is
/// [`Breaking`](Verdict::Breaking).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    /// Old recorded JSON still deserializes and still means the same thing.
    Compatible,
    /// Old recorded JSON either fails to deserialize, or silently loses meaning.
    Breaking,
}

impl Verdict {
    /// Stable lowercase slug used in human output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Compatible => "compatible",
            Self::Breaking => "breaking",
        }
    }
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The classified kind of a single change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// A workflow type appeared that the baseline did not have.
    WorkflowAdded,
    /// A workflow type in the baseline is no longer registered.
    WorkflowRemoved,
    /// A schema was published for a role that previously had none.
    SchemaAdded,
    /// A previously published schema was withdrawn — a coverage regression.
    SchemaRemoved,
    /// A property that old recorded JSON cannot contain is now required.
    RequiredPropertyAdded,
    /// A new optional property — old recorded JSON deserializes unchanged.
    OptionalPropertyAdded,
    /// A property present in the baseline is gone from the new type.
    PropertyRemoved,
    /// An optional property became required.
    PropertyBecameRequired,
    /// A required property became optional.
    PropertyBecameOptional,
    /// The value serde substitutes for an omitted optional key changed, so the
    /// same recorded JSON now deserializes to a different value.
    DefaultChanged,
    /// The accepted JSON type set shrank.
    TypeNarrowed,
    /// The accepted JSON type set grew.
    TypeWidened,
    /// An `enum` value that recorded data may contain is no longer accepted.
    EnumValueRemoved,
    /// A new `enum` value — a read type accepting a superset.
    EnumValueAdded,
    /// A `oneOf`/`anyOf` branch that recorded data may match is gone.
    VariantRemoved,
    /// A new `oneOf`/`anyOf` branch.
    VariantAdded,
    /// `additionalProperties` moved down the `absent > schema > false` lattice.
    AdditionalPropertiesRestricted,
    /// `additionalProperties` moved up the lattice.
    AdditionalPropertiesRelaxed,
    /// A constraint was introduced or tightened.
    ///
    /// Covers numeric/string/array bounds (`minimum`, `maxLength`, `maxItems`,
    /// …). It **also** covers a new constraint keyword on a value that had
    /// none: an `enum` or an `items` schema that appears for the first time.
    /// All three narrow the accepted set the same way, so they share one slug.
    /// The delta's `reason` names which one it was.
    BoundTightened,
    /// A constraint was relaxed or removed — the mirror of [`Self::BoundTightened`].
    BoundRelaxed,
    /// A numeric `format` no longer admits every value the baseline admitted.
    NumericFormatNarrowed,
    /// A numeric `format` widened, or was removed.
    NumericFormatWidened,
    /// A semantic (non-numeric) `format` was introduced or changed, so a
    /// recorded value the baseline accepted may no longer deserialize.
    StringFormatNarrowed,
    /// A semantic (non-numeric) `format` restriction was removed.
    StringFormatWidened,
    /// A tuple-form `items` array changed length.
    TupleArityChanged,
    /// An `allOf` conjunction was introduced, removed, or changed length.
    AllOfMembersChanged,
    /// A constraint keyword outside the analysed subset changed. Fail closed.
    UnanalysedConstraintChanged,
    /// The recursion bound was reached. Fail closed.
    DiffDepthCapReached,
    /// An acknowledgement in the artifact carries no justification.
    AcknowledgementMissingReason,
    /// An activity type appeared that the baseline did not have (issue #1994).
    ActivityAdded,
    /// An activity type in the baseline is no longer registered.
    ActivityRemoved,
    /// A side effect appeared that the baseline did not have (issue #1994).
    SideEffectAdded,
    /// A side effect in the baseline is no longer declared.
    SideEffectRemoved,
}

impl ChangeKind {
    /// Stable `snake_case` slug used in the artifact and in human output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkflowAdded => "workflow_added",
            Self::WorkflowRemoved => "workflow_removed",
            Self::SchemaAdded => "schema_added",
            Self::SchemaRemoved => "schema_removed",
            Self::RequiredPropertyAdded => "required_property_added",
            Self::OptionalPropertyAdded => "optional_property_added",
            Self::PropertyRemoved => "property_removed",
            Self::PropertyBecameRequired => "property_became_required",
            Self::PropertyBecameOptional => "property_became_optional",
            Self::DefaultChanged => "default_changed",
            Self::TypeNarrowed => "type_narrowed",
            Self::TypeWidened => "type_widened",
            Self::EnumValueRemoved => "enum_value_removed",
            Self::EnumValueAdded => "enum_value_added",
            Self::VariantRemoved => "variant_removed",
            Self::VariantAdded => "variant_added",
            Self::AdditionalPropertiesRestricted => "additional_properties_restricted",
            Self::AdditionalPropertiesRelaxed => "additional_properties_relaxed",
            Self::BoundTightened => "bound_tightened",
            Self::BoundRelaxed => "bound_relaxed",
            Self::NumericFormatNarrowed => "numeric_format_narrowed",
            Self::NumericFormatWidened => "numeric_format_widened",
            Self::StringFormatNarrowed => "string_format_narrowed",
            Self::StringFormatWidened => "string_format_widened",
            Self::TupleArityChanged => "tuple_arity_changed",
            Self::AllOfMembersChanged => "all_of_members_changed",
            Self::UnanalysedConstraintChanged => "unanalysed_constraint_changed",
            Self::DiffDepthCapReached => "diff_depth_cap_reached",
            Self::AcknowledgementMissingReason => "acknowledgement_missing_reason",
            Self::ActivityAdded => "activity_added",
            Self::ActivityRemoved => "activity_removed",
            Self::SideEffectAdded => "side_effect_added",
            Self::SideEffectRemoved => "side_effect_removed",
        }
    }
}

impl std::fmt::Display for ChangeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One classified difference between a baseline schema and the current one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaDelta {
    /// What kind of item the delta is about. Left out of the JSON for a
    /// workflow (issue #1994).
    #[serde(default, skip_serializing_if = "SchemaSubject::is_workflow")]
    pub subject: SchemaSubject,
    /// Registered workflow name. For an activity delta, the activity name. For
    /// a side-effect delta, the name of the workflow that owns it.
    pub workflow: String,
    /// The side-effect id, for a side-effect delta (issue #1994).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_effect: Option<String>,
    /// Which published schema this delta belongs to; `None` for a
    /// workflow-level delta.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<SchemaRole>,
    /// JSON Pointer into the *payload instance* (not the schema), e.g.
    /// `/email`. Empty string for the payload root.
    pub field_path: String,
    /// What changed.
    pub change: ChangeKind,
    /// Whether the change is safe for in-flight replay.
    pub verdict: Verdict,
    /// Operator-facing explanation of why.
    pub reason: String,
}

/// The full result of comparing a baseline contract against a current one.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaContractDiff {
    /// Every classified difference, in a stable order.
    pub deltas: Vec<SchemaDelta>,
    /// How many deltas are [`Verdict::Breaking`].
    pub breaking_count: usize,
    /// How many deltas are [`Verdict::Compatible`].
    pub compatible_count: usize,
    /// Set when the delta cap was hit and the report is truncated.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

impl SchemaContractDiff {
    /// `true` when at least one delta would break in-flight replay.
    #[must_use]
    pub const fn has_breaking(&self) -> bool {
        self.breaking_count > 0
    }

    /// Process exit code for this diff: `0` when every delta is compatible,
    /// `1` when any delta is breaking.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        if self.has_breaking() { 1 } else { 0 }
    }

    /// Only the breaking deltas.
    pub fn breaking(&self) -> impl Iterator<Item = &SchemaDelta> {
        self.deltas
            .iter()
            .filter(|d| d.verdict == Verdict::Breaking)
    }

    fn push(&mut self, delta: SchemaDelta) {
        // Count FIRST, then cap only the storage. A count inside the cap would
        // tally only the deltas that fit. A diff whose first `MAX_DELTAS`
        // entries are all compatible would then report `breaking_count == 0`
        // and exit `0`. Every breaking change past the cap would pass in
        // silence. The tally is the gate; the vector is only the report.
        match delta.verdict {
            Verdict::Breaking => self.breaking_count += 1,
            Verdict::Compatible => self.compatible_count += 1,
        }
        if self.deltas.len() >= MAX_DELTAS {
            self.truncated = true;
            return;
        }
        self.deltas.push(delta);
    }
}

/// A recorded, deliberate acceptance of a breaking change.
///
/// Written by [`WorkflowSchemaContract::acknowledged_update`] when a baseline is
/// regenerated over a breaking delta. This is an **append-only audit log**, not
/// a live suppression list. Once the baseline is regenerated, the delta no
/// longer appears. This entry is the permanent, reviewable record of why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcknowledgedBreakingChange {
    /// What kind of item the absorbed delta was about (issue #1994).
    #[serde(default, skip_serializing_if = "SchemaSubject::is_workflow")]
    pub subject: SchemaSubject,
    /// Registered name the absorbed delta belonged to. See
    /// [`SchemaDelta::workflow`].
    pub workflow: String,
    /// The side-effect id, for a side-effect delta (issue #1994).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_effect: Option<String>,
    /// Which published schema, if the delta was schema-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<SchemaRole>,
    /// JSON Pointer into the payload instance.
    pub field_path: String,
    /// The absorbed change kind.
    pub change: ChangeKind,
    /// Why the break was accepted. Must be non-empty — a blank justification is
    /// a rubber stamp and fails the gate.
    pub reason: String,
    /// Optional pointer to the changelog fragment, PR, or issue recording it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recorded_in: Option<String>,
}

/// Coverage counters, so a schema being *withdrawn* is visible at a glance.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCoverage {
    /// Registered workflow types in this contract.
    pub workflows_total: usize,
    /// How many publish an input schema.
    pub with_input_schema: usize,
    /// How many publish an output schema.
    pub with_output_schema: usize,
    /// How many publish an error schema.
    pub with_error_schema: usize,
    /// Registered activity types in this contract (issue #1994).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub activities_total: usize,
    /// How many activities publish an input schema.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub with_activity_input_schema: usize,
    /// How many activities publish an output schema.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub with_activity_output_schema: usize,
    /// Declared side effects in this contract (issue #1994).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub side_effects_total: usize,
    /// How many side effects publish a value schema.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub with_side_effect_value_schema: usize,
}

/// `true` for zero. Used to omit an unused coverage counter from the JSON.
#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes a reference
const fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// The published schemas for one registered workflow type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSchemaEntry {
    /// Registered workflow name.
    pub name: String,
    /// `#[workflow(description = "…")]`, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Canonicalised input schema, or `null` when unpublished.
    #[serde(default)]
    pub input_schema: Option<Value>,
    /// Canonicalised output schema, or `null` when unpublished.
    #[serde(default)]
    pub output_schema: Option<Value>,
    /// Canonicalised error schema, or `null` when unpublished.
    #[serde(default)]
    pub error_schema: Option<Value>,
}

impl WorkflowSchemaEntry {
    /// Materialise an entry from a registered [`WorkflowInfo`].
    #[must_use]
    pub fn from_info(info: &WorkflowInfo) -> Self {
        Self {
            name: info.name.to_string(),
            description: info.description.map(ToString::to_string),
            input_schema: info.input_schema.map(|f| f()),
            output_schema: info.output_schema.map(|f| f()),
            error_schema: info.error_schema.map(|f| f()),
        }
    }

    /// The schema published for `role`, if any.
    #[must_use]
    pub const fn schema_for(&self, role: SchemaRole) -> Option<&Value> {
        match role {
            SchemaRole::Input => self.input_schema.as_ref(),
            SchemaRole::Output => self.output_schema.as_ref(),
            SchemaRole::Error => self.error_schema.as_ref(),
            SchemaRole::Value => None,
        }
    }

    /// Strip annotations from every published schema, in place.
    fn canonicalize(&mut self) {
        for slot in [
            &mut self.input_schema,
            &mut self.output_schema,
            &mut self.error_schema,
        ] {
            if let Some(s) = slot.as_mut() {
                *s = canonicalize_schema(s);
            }
        }
    }
}

/// The published schemas for one registered activity type (issue #1994).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivitySchemaEntry {
    /// Registered activity name.
    pub name: String,
    /// Canonicalised input schema, or `null` when unpublished.
    #[serde(default)]
    pub input_schema: Option<Value>,
    /// Canonicalised output schema, or `null` when unpublished.
    #[serde(default)]
    pub output_schema: Option<Value>,
}

impl ActivitySchemaEntry {
    /// Materialise an entry from a registered [`ActivityInfo`].
    #[must_use]
    pub fn from_info(info: &ActivityInfo) -> Self {
        Self {
            name: info.name.to_string(),
            input_schema: info.input_schema.map(|f| f()),
            output_schema: info.output_schema.map(|f| f()),
        }
    }

    /// The schema published for `role`, if any.
    #[must_use]
    pub const fn schema_for(&self, role: SchemaRole) -> Option<&Value> {
        match role {
            SchemaRole::Input => self.input_schema.as_ref(),
            SchemaRole::Output => self.output_schema.as_ref(),
            SchemaRole::Error | SchemaRole::Value => None,
        }
    }

    /// Strip annotations from every published schema, in place.
    fn canonicalize(&mut self) {
        for slot in [&mut self.input_schema, &mut self.output_schema] {
            if let Some(s) = slot.as_mut() {
                *s = canonicalize_schema(s);
            }
        }
    }
}

/// The published value schema of one side effect (issue #1994).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SideEffectSchemaEntry {
    /// Name of the workflow that records the side effect.
    pub workflow: String,
    /// The id passed to `ctx.side_effect`.
    pub id: String,
    /// Canonicalised value schema, or `null` when unpublished.
    #[serde(default)]
    pub value_schema: Option<Value>,
}

impl SideEffectSchemaEntry {
    /// Materialise an entry from a declared [`SideEffectInfo`].
    #[must_use]
    pub fn from_info(info: &SideEffectInfo) -> Self {
        Self {
            workflow: info.workflow.to_string(),
            id: info.id.to_string(),
            value_schema: info.value_schema.map(|f| f()),
        }
    }

    /// Strip annotations from the published schema, in place.
    fn canonicalize(&mut self) {
        if let Some(s) = self.value_schema.as_mut() {
            *s = canonicalize_schema(s);
        }
    }
}

/// The human-readable ruleset, embedded in the artifact so a reviewer reading
/// the diff sees the rules the verdicts were computed under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityRules {
    /// The perspective the verdicts are computed from.
    pub perspective: String,
    /// Changes that fail the gate.
    pub breaking_changes: Vec<String>,
    /// Changes that pass.
    pub non_breaking_changes: Vec<String>,
    /// Keywords the differ reasons about.
    pub analysed_keywords: Vec<String>,
    /// Keywords stripped before comparison because they cannot affect
    /// deserialization.
    pub ignored_annotations: Vec<String>,
    /// What happens to a constraint keyword outside the analysed set.
    pub unanalysed_keyword_policy: String,
}

impl CompatibilityRules {
    /// The canonical ruleset for [`SCHEMA_CONTRACT_VERSION`].
    #[must_use]
    pub fn current() -> Self {
        Self {
            perspective:
                "replay-read: JSON already recorded in harvest_events must still deserialize \
                 into the new type, and must still mean the same thing"
                    .to_string(),
            breaking_changes: [
                "Adding a required property (no #[serde(default)])",
                "Removing a property (the recorded value is silently dropped; a hard \
                 deserialization failure under #[serde(deny_unknown_fields)])",
                "Renaming a property (surfaces as removed + added-required)",
                "Making an optional property required",
                "Narrowing a type (e.g. string to integer, or dropping null)",
                "Narrowing a numeric format (e.g. int64 to int32, i64 to u64)",
                "Introducing or changing a semantic format (e.g. String to Uuid, uuid to \
                 date-time): the format names the parser the field's type runs, so a recorded \
                 value it rejects no longer deserializes",
                "Removing an enum value or a oneOf/anyOf variant",
                "Restricting additionalProperties (absent > schema > false)",
                "Introducing or tightening a bound (minimum/maximum/minLength/maxLength)",
                "Changing tuple arity",
                "Withdrawing a previously published schema (coverage regression)",
                "Any change to a constraint keyword outside the analysed set (fail closed)",
                "A cardinality bound (minLength/maxLength/minItems/maxItems) that is present but \
                 is not a non-negative integer on either side: the engine's validator reads them \
                 with as_u64 and IGNORES anything else, so such a value is not a smaller bound \
                 but an absent one (fail closed)",
                "Removing an activity type: in-flight runs still decode its recorded results \
                 (issue #1994)",
                "Removing a side-effect declaration while its workflow stays. The declaration is \
                 not registered with the runtime, so its call site can still read recorded \
                 values (issue #1994)",
            ]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
            non_breaking_changes: [
                "Adding an optional property (Option<T> or #[serde(default)])",
                "Removing a required marker (making a property optional)",
                "Widening a type (e.g. integer to number, T to Option<T>)",
                "Widening a numeric format (e.g. int32 to int64)",
                "Removing a semantic format (e.g. Uuid to String)",
                "Adding an enum value, or a disjoint oneOf/anyOf variant",
                "Relaxing additionalProperties or a bound",
                "Adding a workflow type, or publishing a schema for the first time",
                "Removing a workflow type (gated more accurately by \
                 `harvest workflow-types reachability`, issue #520)",
                "Adding an activity type or a side effect, or removing a side effect together \
                 with its workflow (issue #1994)",
                "Editing any annotation (title, description, examples, …)",
            ]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
            analysed_keywords: ANALYSED_KEYWORDS.iter().map(|k| (*k).to_string()).collect(),
            ignored_annotations: ANNOTATION_KEYWORDS
                .iter()
                .map(|k| (*k).to_string())
                .collect(),
            unanalysed_keyword_policy:
                "Any change (added, modified or removed) to a constraint keyword outside the \
                 analysed set is reported breaking, as is a change to any sub-schema it reaches \
                 through `$ref`. Keywords interact — removing one does not reliably loosen a \
                 schema — so the differ fails closed and lets the author acknowledge."
                    .to_string(),
        }
    }
}

/// The checked-in baseline artifact.
///
/// Mirrors the `docs/api-contract.json` precedent: versioned, machine-readable,
/// regenerable, with the compatibility ruleset embedded.
///
/// Stored schemas are **canonicalised** (annotations stripped). `schemars`
/// copies a doc comment into `description`. So a doc-comment edit produces a
/// byte-identical artifact and never forces an unrelated regeneration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSchemaContract {
    /// Version of the crate that generated this artifact.
    pub version: String,
    /// [`SCHEMA_CONTRACT_VERSION`] — the artifact format version.
    pub contract_version: String,
    /// Human-facing note about what this file is and how to regenerate it.
    ///
    /// Regenerated metadata, not input: a hand-written or hand-trimmed baseline
    /// parses without it rather than hard-failing on a purely cosmetic field.
    #[serde(default = "default_description")]
    pub description: String,
    /// The ruleset the verdicts are computed under.
    ///
    /// Regenerated metadata (see [`Self::description`]). The verdicts always
    /// come from [`CompatibilityRules::current`] — this field documents the
    /// ruleset in the checked-in file, it is never read back as configuration.
    #[serde(default = "CompatibilityRules::current")]
    pub compatibility: CompatibilityRules,
    /// How many workflows publish each schema role.
    ///
    /// Regenerated metadata (see [`Self::description`]).
    #[serde(default)]
    pub coverage: SchemaCoverage,
    /// Append-only audit log of deliberately accepted breaking changes.
    #[serde(default)]
    pub acknowledged_breaking_changes: Vec<AcknowledgedBreakingChange>,
    /// Published schemas, sorted by workflow name and deduplicated.
    pub workflows: Vec<WorkflowSchemaEntry>,
    /// Published activity schemas, sorted by name and deduplicated
    /// (issue #1994). Left out of the JSON when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activities: Vec<ActivitySchemaEntry>,
    /// Published side-effect schemas, sorted by `(workflow, id)` and
    /// deduplicated (issue #1994). Left out of the JSON when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub side_effects: Vec<SideEffectSchemaEntry>,
}

/// Failure modes of contract parsing and baseline regeneration.
#[derive(Debug, thiserror::Error)]
pub enum SchemaContractError {
    /// The document was not valid JSON, or not a recognised contract shape.
    #[error("invalid workflow schema contract: {0}")]
    Parse(String),
    /// A breaking delta was present and no justification was supplied.
    #[error(
        "refusing to regenerate the baseline: {breaking} breaking change(s) present. \
         Re-run with an acknowledgement recording WHY the break is safe (e.g. in-flight \
         executions drained, or pinned by build routing per issue #171)."
    )]
    UnacknowledgedBreakingChange {
        /// How many breaking deltas were found.
        breaking: usize,
    },
    /// An acknowledgement was supplied but carried no justification.
    #[error("an acknowledgement must record WHY the break is safe; the reason was blank")]
    BlankAcknowledgement,
    /// The diff was truncated, so an audit record cannot be written for every
    /// absorbed breaking delta.
    #[error(
        "refusing to acknowledge a truncated diff: {breaking} breaking change(s) were found but \
         only {recorded} delta(s) were recorded, so the audit log would not name every change \
         being absorbed. Split the change into smaller steps."
    )]
    TruncatedAcknowledgement {
        /// How many deltas the capped report actually stored.
        recorded: usize,
        /// The true breaking tally, which keeps counting past the cap.
        breaking: usize,
    },
    /// The contract could not be serialised.
    #[error("failed to serialize the workflow schema contract: {0}")]
    Serialize(#[from] serde_json::Error),
}

impl WorkflowSchemaContract {
    /// Build a contract from a registry's [`WorkflowInfo`] values.
    ///
    /// Duplicate names collapse **last-wins**, mirroring the runtime's own
    /// `HashMap` collapse — otherwise the artifact could publish a schema the
    /// runtime never uses.
    #[must_use]
    pub fn from_infos<'a>(
        version: &str,
        infos: impl IntoIterator<Item = &'a WorkflowInfo>,
    ) -> Self {
        Self::from_entries(
            version,
            infos.into_iter().map(WorkflowSchemaEntry::from_info),
        )
    }

    /// Build a contract from already-materialised entries.
    ///
    /// Entries are canonicalised, deduplicated last-wins, and sorted by name.
    #[must_use]
    pub fn from_entries(
        version: &str,
        entries: impl IntoIterator<Item = WorkflowSchemaEntry>,
    ) -> Self {
        let mut by_name: BTreeMap<String, WorkflowSchemaEntry> = BTreeMap::new();
        for mut e in entries {
            e.canonicalize();
            by_name.insert(e.name.clone(), e);
        }
        let mut contract = Self {
            version: version.to_string(),
            contract_version: SCHEMA_CONTRACT_VERSION.to_string(),
            description: default_description(),
            compatibility: CompatibilityRules::current(),
            coverage: SchemaCoverage::default(),
            acknowledged_breaking_changes: Vec::new(),
            workflows: by_name.into_values().collect(),
            activities: Vec::new(),
            side_effects: Vec::new(),
        };
        contract.recount();
        contract
    }

    /// Add the published schemas of registered activities (issue #1994).
    ///
    /// Pass the same `ActivityInfo` values that the runtime registers. An
    /// activity with no schema is still listed, so coverage shows it. Duplicate
    /// names collapse last-wins, the same as [`Self::from_infos`].
    #[must_use]
    pub fn with_activities<'a>(
        mut self,
        infos: impl IntoIterator<Item = &'a ActivityInfo>,
    ) -> Self {
        let added = infos.into_iter().map(ActivitySchemaEntry::from_info);
        let all: Vec<ActivitySchemaEntry> = std::mem::take(&mut self.activities)
            .into_iter()
            .chain(added)
            .collect();
        self.activities = index_activities(all);
        self.recount();
        self
    }

    /// Add the published value schemas of declared side effects (issue #1994).
    ///
    /// The key is `(workflow, id)`. Duplicate keys collapse last-wins.
    #[must_use]
    pub fn with_side_effects<'a>(
        mut self,
        infos: impl IntoIterator<Item = &'a SideEffectInfo>,
    ) -> Self {
        let added = infos.into_iter().map(SideEffectSchemaEntry::from_info);
        let all: Vec<SideEffectSchemaEntry> = std::mem::take(&mut self.side_effects)
            .into_iter()
            .chain(added)
            .collect();
        self.side_effects = index_side_effects(all);
        self.recount();
        self
    }

    /// Recompute [`Self::coverage`] from the three sections.
    fn recount(&mut self) {
        self.coverage = SchemaCoverage {
            workflows_total: self.workflows.len(),
            with_input_schema: self
                .workflows
                .iter()
                .filter(|w| w.input_schema.is_some())
                .count(),
            with_output_schema: self
                .workflows
                .iter()
                .filter(|w| w.output_schema.is_some())
                .count(),
            with_error_schema: self
                .workflows
                .iter()
                .filter(|w| w.error_schema.is_some())
                .count(),
            activities_total: self.activities.len(),
            with_activity_input_schema: self
                .activities
                .iter()
                .filter(|a| a.input_schema.is_some())
                .count(),
            with_activity_output_schema: self
                .activities
                .iter()
                .filter(|a| a.output_schema.is_some())
                .count(),
            side_effects_total: self.side_effects.len(),
            with_side_effect_value_schema: self
                .side_effects
                .iter()
                .filter(|s| s.value_schema.is_some())
                .count(),
        };
    }

    /// Look up an activity entry by name.
    #[must_use]
    pub fn activity(&self, name: &str) -> Option<&ActivitySchemaEntry> {
        self.activities.iter().find(|a| a.name == name)
    }

    /// Look up a side-effect entry by its `(workflow, id)` key.
    #[must_use]
    pub fn side_effect(&self, workflow: &str, id: &str) -> Option<&SideEffectSchemaEntry> {
        self.side_effects
            .iter()
            .find(|s| s.workflow == workflow && s.id == id)
    }

    /// Parse a contract document.
    ///
    /// Accepts **either** a full contract object, **or** a bare array of
    /// `GET /workflows/registered` records — so
    /// `curl … /workflows/registered > current.json` works with no
    /// post-processing.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::Parse`] when the document is not valid
    /// JSON or is neither recognised shape.
    pub fn parse(json: &str) -> Result<Self, SchemaContractError> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| SchemaContractError::Parse(format!("not valid JSON: {e}")))?;

        if value.is_array() {
            let entries: Vec<WorkflowSchemaEntry> = serde_json::from_value(value).map_err(|e| {
                SchemaContractError::Parse(format!(
                    "not a `GET /workflows/registered` response array: {e}"
                ))
            })?;
            return Ok(Self::from_entries(UNKNOWN_VERSION, entries));
        }

        let mut parsed: Self = serde_json::from_value(value).map_err(|e| {
            SchemaContractError::Parse(format!(
                "not a workflow schema contract (expected an object with a `workflows` array, \
                 or a bare `GET /workflows/registered` array): {e}"
            ))
        })?;
        // Refuse a format version this binary does not implement. A future v2
        // may change what a verdict MEANS. A diff of it under v1 rules would
        // return confident answers from the wrong ruleset. A compatibility gate
        // must never fail that way.
        if parsed.contract_version != SCHEMA_CONTRACT_VERSION
            && !READABLE_LEGACY_CONTRACT_VERSIONS.contains(&parsed.contract_version.as_str())
        {
            return Err(SchemaContractError::Parse(format!(
                "contract_version `{}` is not supported by this build (expected `{}`). Upgrade \
                 the `harvest` CLI to match the checked-in baseline, or regenerate the baseline \
                 with this build",
                parsed.contract_version, SCHEMA_CONTRACT_VERSION
            )));
        }
        // A version 1 file has no activity or side-effect content. A file that
        // claims version 1 and carries some would make a version 1 binary skip
        // that content in silence, so refuse it.
        if parsed.contract_version != SCHEMA_CONTRACT_VERSION
            && (!parsed.activities.is_empty()
                || !parsed.side_effects.is_empty()
                || parsed
                    .acknowledged_breaking_changes
                    .iter()
                    .any(|a| !a.subject.is_workflow()))
        {
            return Err(SchemaContractError::Parse(format!(
                "contract_version `{}` has no activities or side effects, but this file has \
                 some. Set contract_version to `{SCHEMA_CONTRACT_VERSION}`, or regenerate the \
                 baseline with `harvest schema update`",
                parsed.contract_version
            )));
        }
        // Re-derive the invariants a hand-edited file may have broken:
        // canonical schemas, sorted+unique entries, and accurate coverage.
        let rebuilt = Self::from_entries(&parsed.version, std::mem::take(&mut parsed.workflows));
        parsed.workflows = rebuilt.workflows;
        parsed.activities = index_activities(std::mem::take(&mut parsed.activities));
        parsed.side_effects = index_side_effects(std::mem::take(&mut parsed.side_effects));
        parsed.recount();
        Ok(parsed)
    }

    /// Serialise to the pretty-printed form written to disk.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::Serialize`] if serialisation fails.
    pub fn to_json_pretty(&self) -> Result<String, SchemaContractError> {
        let mut s = serde_json::to_string_pretty(self)?;
        s.push('\n');
        Ok(s)
    }

    /// Look up an entry by workflow name.
    #[must_use]
    pub fn entry(&self, name: &str) -> Option<&WorkflowSchemaEntry> {
        self.workflows.iter().find(|w| w.name == name)
    }

    /// Regenerate this baseline from `current`, refusing any breaking delta.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::UnacknowledgedBreakingChange`] when the
    /// diff contains a breaking delta — use [`Self::acknowledged_update`] to
    /// record a justification instead.
    pub fn compatible_update(&self, current: &Self) -> Result<Self, SchemaContractError> {
        let diff = diff_schema_contracts(self, current);
        if diff.has_breaking() {
            return Err(SchemaContractError::UnacknowledgedBreakingChange {
                breaking: diff.breaking_count,
            });
        }
        Ok(self.rebase_onto(current, Vec::new()))
    }

    /// Regenerate this baseline from `current`, recording every absorbed
    /// breaking delta as an [`AcknowledgedBreakingChange`].
    ///
    /// The acknowledgement is an append-only audit record. After this call the
    /// baseline matches `current`, so the next check is clean. The git diff
    /// shows both the schema change and the justification.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::BlankAcknowledgement`] when `reason` is
    /// empty or whitespace-only.
    pub fn acknowledged_update(
        &self,
        current: &Self,
        reason: &str,
        recorded_in: Option<&str>,
    ) -> Result<Self, SchemaContractError> {
        if reason.trim().is_empty() {
            return Err(SchemaContractError::BlankAcknowledgement);
        }
        let diff = diff_schema_contracts(self, current);
        // The artifact promises an audit record for EVERY absorbed breaking
        // delta. But the stored listing stops at `MAX_DELTAS`, and the rebase
        // takes the whole current contract. So a truncated diff would absorb
        // the excess with no record of it. Refuse rather than write an audit
        // log that under-reports what was acknowledged.
        if diff.truncated {
            return Err(SchemaContractError::TruncatedAcknowledgement {
                recorded: diff.deltas.len(),
                breaking: diff.breaking_count,
            });
        }
        let acks: Vec<AcknowledgedBreakingChange> = diff
            .breaking()
            .map(|d| AcknowledgedBreakingChange {
                subject: d.subject,
                workflow: d.workflow.clone(),
                side_effect: d.side_effect.clone(),
                role: d.role,
                field_path: d.field_path.clone(),
                change: d.change,
                reason: reason.trim().to_string(),
                recorded_in: recorded_in.map(str::to_string),
            })
            .collect();
        Ok(self.rebase_onto(current, acks))
    }

    /// Build the next baseline: `current`'s schemas, this baseline's audit log
    /// plus `new_acks`.
    fn rebase_onto(&self, current: &Self, new_acks: Vec<AcknowledgedBreakingChange>) -> Self {
        let mut next = Self::from_entries(&current.version, current.workflows.clone());
        next.activities = index_activities(current.activities.clone());
        next.side_effects = index_side_effects(current.side_effects.clone());
        next.recount();
        if next.version == UNKNOWN_VERSION {
            // A bare `/workflows/registered` body carries no crate version;
            // keep the baseline's rather than regressing it to a placeholder.
            next.version.clone_from(&self.version);
        }
        next.acknowledged_breaking_changes
            .clone_from(&self.acknowledged_breaking_changes);
        next.acknowledged_breaking_changes.extend(new_acks);
        next
    }
}

/// Canonicalise, deduplicate last-wins and sort activity entries by name.
fn index_activities(
    entries: impl IntoIterator<Item = ActivitySchemaEntry>,
) -> Vec<ActivitySchemaEntry> {
    let mut by_name: BTreeMap<String, ActivitySchemaEntry> = BTreeMap::new();
    for mut e in entries {
        e.canonicalize();
        by_name.insert(e.name.clone(), e);
    }
    by_name.into_values().collect()
}

/// Canonicalise, deduplicate last-wins and sort side-effect entries by
/// `(workflow, id)`.
fn index_side_effects(
    entries: impl IntoIterator<Item = SideEffectSchemaEntry>,
) -> Vec<SideEffectSchemaEntry> {
    let mut by_key: BTreeMap<(String, String), SideEffectSchemaEntry> = BTreeMap::new();
    for mut e in entries {
        e.canonicalize();
        by_key.insert((e.workflow.clone(), e.id.clone()), e);
    }
    by_key.into_values().collect()
}

/// Placeholder version for a contract parsed from a bare
/// `GET /workflows/registered` array, which carries no crate version.
const UNKNOWN_VERSION: &str = "unknown";

/// Human-readable header stamped into every generated artifact.
///
/// The stripped-keyword list is GENERATED from [`ANNOTATION_KEYWORDS`], not
/// written by hand. The artifact-vs-code drift guard cannot catch a
/// hand-written sentence: a stale claim is stale on both sides and compares
/// equal. This one had already fallen behind twice. `default` and non-numeric
/// `format` became analysed keywords, while the prose still told operators
/// both were stripped. A derived list cannot contradict the ruleset it
/// describes.
fn default_description() -> String {
    let stripped = ANNOTATION_KEYWORDS.join("/");
    format!(
        "Versioned machine-readable baseline of every registered workflow type's published \
         input/output/error JSON Schema (issue #373), and of activity input/output and \
         side-effect value schemas (issue #1994), used by `harvest schema check` to gate \
         backward-incompatible payload changes before they DLQ in-flight executions (issue \
         #794). Stored schemas are canonicalised: only the pure annotations ({stripped}) are \
         stripped, so a doc-comment edit never dirties this file. Every other keyword is \
         compared — see `compatibility.analysed_keywords` for the set the differ classifies, and \
         `compatibility.unanalysed_keyword_policy` for what happens to the rest. Regenerate with \
         `harvest schema update`; a breaking change must be acknowledged with a recorded \
         justification. See docs/workflow-schema-contract-guide.md."
    )
}

// ── Keyword partition ────────────────────────────────────────────────────────

/// Keywords the differ reasons about — exactly the subset the engine's own
/// validator enforces, plus numeric `format`.
const ANALYSED_KEYWORDS: &[&str] = &[
    "type",
    "required",
    "properties",
    "enum",
    "items",
    "additionalProperties",
    "allOf",
    "anyOf",
    "oneOf",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "minItems",
    "maxItems",
    "format",
    "$ref",
    // Handled by `diff_serde_default`, which needs the parent's `required` set
    // to know whether the default is live. Listed here so the fail-closed
    // sweep does not also flag it on a required field, where it is dead.
    "default",
];

/// Pure annotations: stripped before comparison because no value of theirs can
/// change whether a recorded payload deserializes.
const ANNOTATION_KEYWORDS: &[&str] = &[
    "title",
    "description",
    "$comment",
    "$schema",
    "$id",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
];

/// Containers that hold sub-schemas rather than constraining an instance. They
/// are reached through `$ref`, never compared directly.
const CONTAINER_KEYWORDS: &[&str] = &["definitions", "$defs"];

/// Keywords whose value is a MAP keyed by author-chosen names.
///
/// Their keys are payload field names and definition names, not schema
/// keywords. So [`ANNOTATION_KEYWORDS`] must never be applied to them. A field
/// called `description` is a field. Its deletion would hide it from the
/// differ: a type change on it would canonicalise identically on both sides.
///
/// Most of these map a name to a sub-schema, but not all. Draft-07
/// `dependencies` takes *either* a sub-schema or an array of property names.
/// 2019-09 split it into `dependentSchemas` (the first form) and
/// `dependentRequired` (the second). The shape of the KEYS puts all of them
/// here, not the values. Canonicalisation of an array of names is a no-op, so
/// one code path serves both.
const SCHEMA_MAP_KEYWORDS: &[&str] = &[
    "properties",
    "patternProperties",
    "definitions",
    "$defs",
    "dependentSchemas",
    "dependentRequired",
    "dependencies",
];

/// Keywords whose value is INSTANCE data rather than a sub-schema.
///
/// A recursion into one would rewrite the very value the differ compares. An
/// object-valued `default` with a `description` key is payload data, not an
/// annotated schema. `enum` belongs to this class too but has its own arm,
/// because its members are sorted (an `enum` is a set).
const INSTANCE_VALUE_KEYWORDS: &[&str] = &["default", "const"];

/// Bounds whose *increase* narrows the accepted set.
const LOWER_BOUNDS: &[&str] = &["minimum", "minLength", "minItems"];
/// Bounds whose *decrease* narrows the accepted set.
const UPPER_BOUNDS: &[&str] = &["maximum", "maxLength", "maxItems"];

/// `true` for the bounds that count things (characters, elements) rather than
/// measure a value. JSON Schema requires a non-negative integer for these. The
/// engine's `validate_node` reads them with `as_u64`. So the validator ignores
/// any other value in silence: it is not a smaller bound.
fn is_cardinality_bound(key: &str) -> bool {
    matches!(key, "minLength" | "maxLength" | "minItems" | "maxItems")
}

/// Inclusive value range of a numeric `format`, as `(min, max)`.
///
/// `schemars` records integer width and signedness **only** in `format`. The
/// `type` of both `i64` and `i32` is `"integer"`. Without `format`,
/// `i64 -> i32` (a textbook break) would be invisible.
fn numeric_format_range(format: &str) -> Option<(i128, i128)> {
    let r = match format {
        "int8" => (i128::from(i8::MIN), i128::from(i8::MAX)),
        "int16" => (i128::from(i16::MIN), i128::from(i16::MAX)),
        "int32" => (i128::from(i32::MIN), i128::from(i32::MAX)),
        "int64" => (i128::from(i64::MIN), i128::from(i64::MAX)),
        "int128" => (i128::MIN, i128::MAX),
        "uint8" => (0, i128::from(u8::MAX)),
        "uint16" => (0, i128::from(u16::MAX)),
        "uint32" => (0, i128::from(u32::MAX)),
        "uint64" => (0, i128::from(u64::MAX)),
        "uint128" => (0, i128::MAX),
        // Floats are compared by exactly-representable integer range, so
        // `int64 -> double` is correctly breaking (values above 2^53 lose
        // precision) while `int32 -> double` is a genuine widening.
        "float" => (-(1i128 << 24), 1i128 << 24),
        "double" => (-(1i128 << 53), 1i128 << 53),
        _ => return None,
    };
    Some(r)
}

/// `true` when `format` names a numeric width.
///
/// The distinction is about which LATTICE applies, not about which formats
/// matter. A numeric width is orderable — `int32` sits strictly inside `int64`
/// — so those are compared by range. Every other value (`uuid`, `date-time`,
/// `email`, …) is an opaque label with no ordering, so it is compared for
/// equality by [`diff_format`]'s semantic arm.
fn is_numeric_format(format: &str) -> bool {
    numeric_format_range(format).is_some()
}

/// `true` when `format` names an **integral** width (`int*`/`uint*`).
///
/// The range lattice alone cannot separate `float` from `int32`. The int32
/// integer range is strictly wider than the exactly-representable float range.
/// So a pure range comparison calls `float -> int32` a widening. It is not: a
/// recorded `1.5` is a good float and deserializes into no integer type at
/// all. Integrality is a second, independent axis.
fn is_integral_format(format: &str) -> bool {
    format.starts_with("int") || format.starts_with("uint")
}

// ── Canonicalisation ─────────────────────────────────────────────────────────

/// Strip everything from `schema` that cannot affect whether recorded JSON
/// deserializes, and normalise the representations `schemars` uses
/// interchangeably.
///
/// Applied to both sides before diffing **and** to the schemas stored in the
/// baseline, so an annotation-only edit produces zero deltas *and* a
/// byte-identical artifact.
///
/// Specifically:
/// - drops [`ANNOTATION_KEYWORDS`];
/// - sorts `enum` arrays (an `enum` is a set; order is not semantic);
/// - collapses a `oneOf`/`anyOf` of singleton-`enum` branches into one flat
///   `enum`. `schemars` flips between the two forms when a *single* unit
///   variant gains a doc comment. Without this, that would read as "every
///   variant removed and two branches added".
#[must_use]
pub fn canonicalize_schema(schema: &Value) -> Value {
    canonicalize_node(schema, 0).unwrap_or_else(|| schema.clone())
}

/// Canonical form of `schema`, or `None` when it already is canonical.
///
/// A `None` result means the canonical form equals the input, so a caller
/// can borrow the input and skip the deep copy. Stored contracts are already
/// canonical, so the diff walk takes this path for most nodes.
fn canonicalize_node(schema: &Value, depth: usize) -> Option<Value> {
    if depth >= MAX_DIFF_DEPTH {
        return None;
    }
    match schema {
        Value::Array(items) => {
            let canon: Vec<Option<Value>> = items
                .iter()
                .map(|i| canonicalize_node(i, depth + 1))
                .collect();
            if canon.iter().all(Option::is_none) {
                return None;
            }
            Some(Value::Array(
                canon
                    .into_iter()
                    .zip(items)
                    .map(|(c, original)| c.unwrap_or_else(|| original.clone()))
                    .collect(),
            ))
        }
        Value::Object(obj) => {
            // One slot per entry: `Some(None)` keeps the value, `Some(Some(v))`
            // replaces it, `None` drops the entry as an annotation.
            let mut slots: Vec<Option<Option<Value>>> = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                if ANNOTATION_KEYWORDS.contains(&k.as_str()) {
                    slots.push(None);
                } else if k == "format" || INSTANCE_VALUE_KEYWORDS.contains(&k.as_str()) {
                    // Instance data, not a schema: preserve it byte-for-byte.
                    slots.push(Some(None));
                } else if k == "enum" {
                    let sorted = sorted_enum(v);
                    slots.push(Some((sorted != *v).then_some(sorted)));
                } else if SCHEMA_MAP_KEYWORDS.contains(&k.as_str()) {
                    // A map of author-chosen NAMES to sub-schemas: the keys are not
                    // keywords, so the annotation filter must not see them.
                    slots.push(Some(canonicalize_schema_map(v, depth + 1)));
                } else {
                    slots.push(Some(canonicalize_node(v, depth + 1)));
                }
            }
            // Only these keys can trigger the whole-object rewrites below.
            let may_rewrite = ["oneOf", "anyOf", "allOf"]
                .iter()
                .any(|k| obj.contains_key(*k));
            if !may_rewrite && slots.iter().all(|s| matches!(s, Some(None))) {
                return None;
            }
            let mut out = Map::new();
            for ((k, v), slot) in obj.iter().zip(slots) {
                if let Some(canon) = slot {
                    out.insert(k.clone(), canon.unwrap_or_else(|| v.clone()));
                }
            }
            collapse_unit_enum_branches(&mut out);
            flatten_single_member_all_of(&mut out);
            (out != *obj).then_some(Value::Object(out))
        }
        _ => None,
    }
}

/// Canonicalise a map of author-chosen NAMES to sub-schemas.
///
/// Every key stays verbatim, because it is a payload field or definition name,
/// not a schema keyword. Every value is canonicalised as a schema. That is the
/// whole difference between a schema object and a `properties` map.
/// [`canonicalize_node`] cannot tell them apart from shape alone: both are JSON
/// objects. Returns `None` when every value is already canonical.
fn canonicalize_schema_map(value: &Value, depth: usize) -> Option<Value> {
    if depth >= MAX_DIFF_DEPTH {
        return None;
    }
    let Value::Object(map) = value else {
        // Malformed (a `properties` that is not an object). Leave it to the
        // fail-closed sweep rather than guessing at a shape it does not have.
        return None;
    };
    let canon: Vec<Option<Value>> = map
        .values()
        .map(|v| canonicalize_node(v, depth + 1))
        .collect();
    if canon.iter().all(Option::is_none) {
        return None;
    }
    Some(Value::Object(
        map.iter()
            .zip(canon)
            .map(|((k, v), c)| (k.clone(), c.unwrap_or_else(|| v.clone())))
            .collect(),
    ))
}

/// Collapse `{"allOf": [X]}` into `X`.
///
/// The `schemars` `RemoveRefSiblings` pass rewrites a `$ref` with a sibling
/// keyword into `{"allOf": [{"$ref": …}], <siblings>}`. The trigger is as small
/// as **a doc comment on a struct-typed field**. Without this, one new `///`
/// line restructures the schema in silence. The differ would then report the
/// whole referenced subtree as rewritten.
///
/// Only a lone member is lifted, and only when the parent has no other
/// analysed keyword. With siblings present, the conjunction is real and must
/// be compared as one (see [`diff_all_of`]). Annotations are already stripped
/// when this runs, so the common case is a bare single-member wrapper.
fn flatten_single_member_all_of(obj: &mut Map<String, Value>) {
    let Some(members) = obj.get("allOf").and_then(Value::as_array) else {
        return;
    };
    let [Value::Object(only)] = members.as_slice() else {
        return;
    };
    let only = only.clone();

    // A sibling analysed keyword means the parent genuinely conjoins something
    // with the member; lifting would silently drop one side of that.
    if obj
        .keys()
        .any(|k| k != "allOf" && ANALYSED_KEYWORDS.contains(&k.as_str()))
    {
        return;
    }
    // Same for a key the member also defines: merging is not well-defined.
    if only.keys().any(|k| obj.contains_key(k)) {
        return;
    }

    obj.remove("allOf");
    for (k, v) in only {
        obj.insert(k, v);
    }
}

/// Sort an `enum` array by its canonical JSON text, so a reordering alone is
/// not a change.
fn sorted_enum(value: &Value) -> Value {
    value.as_array().map_or_else(
        || value.clone(),
        |arr| {
            let mut vals: Vec<Value> = arr.clone();
            vals.sort_by_key(std::string::ToString::to_string);
            vals.dedup();
            Value::Array(vals)
        },
    )
}

/// Collapse `{"oneOf"|"anyOf": [{"enum": [v]}, …]}` into `{"enum": [v, …]}`.
///
/// `schemars` emits a unit-only Rust enum as a flat `enum` array. It switches
/// to a `oneOf` of singleton `enum`s as soon as **one** variant has a doc
/// comment. Without this the two forms of the same type read as a total rewrite.
fn collapse_unit_enum_branches(obj: &mut Map<String, Value>) {
    for key in ["oneOf", "anyOf"] {
        // Never overwrite a sibling the differ analyses. A node with BOTH
        // `enum` and `oneOf` is meaningful to the runtime, because
        // `validate_against_schema` enforces both. A collapse here would
        // clobber a real constraint. A later change to it would then be
        // invisible, or read in the wrong direction.
        //
        // This also makes the loop itself safe. Once `oneOf` collapses into
        // `enum`, the `anyOf` pass sees that `enum` and leaves it alone. It
        // does not overwrite the values the first pass just wrote.
        if obj.contains_key("enum") {
            continue;
        }
        let Some(branches) = obj.get(key).and_then(Value::as_array) else {
            continue;
        };
        if branches.is_empty() {
            continue;
        }
        let mut values: Vec<Value> = Vec::new();
        let all_singleton_enums = branches.iter().all(|b| {
            let Some(bo) = b.as_object() else {
                return false;
            };
            // Only `type` and `enum` may remain after annotation stripping.
            if bo.keys().any(|k| k != "enum" && k != "type") {
                return false;
            }
            match bo.get("enum").and_then(Value::as_array) {
                Some(vals) if vals.len() == 1 => {
                    values.push(vals[0].clone());
                    true
                }
                _ => false,
            }
        });
        // `oneOf` requires EXACTLY one match, so two branches pinned to the same
        // value make that value match twice and be rejected. Merging them into
        // one `enum` array — which `sorted_enum` then dedupes — would erase that
        // difference and report the change compatible. `anyOf` is "at least
        // one", where duplicate branches are genuinely harmless, so it still
        // collapses (and must, or every unit-only enum would churn).
        let duplicated = key == "oneOf" && {
            let mut seen: BTreeSet<String> = BTreeSet::new();
            values.iter().any(|v| !seen.insert(v.to_string()))
        };
        if all_singleton_enums && !duplicated {
            // EVERY branch must agree on `type`, including all agreeing on
            // having none. Keeping only the branches that declare one would be
            // wrong twice over. When they disagree, there is no single type to
            // lift. A collapse would DROP them all, and two schemas with
            // opposite meanings would canonicalize alike.
            // `[{string,"x"},{integer,1}]` and the same pair with the types
            // swapped (which accepts nothing at all) both flatten to
            // `enum:["x",1]`. And when only some declare one, lifting it would
            // constrain the branches that deliberately did not. Either way the
            // collapse is abandoned, not approximated. It exists to stop unit
            // enums churning, not to reinterpret them.
            //
            // Compared as a whole `Value` so the array spelling
            // (`["string","null"]`) is covered too; reading `as_str` treated it
            // as an absent type and dropped it.
            let mut branch_types = branches.iter().map(|b| b.get("type"));
            let first = branch_types.next().flatten();
            if !branch_types.all(|t| t == first) {
                continue;
            }
            let lifted_type = first.cloned();
            // A parent `type` that disagrees with the shared branch type is a
            // contradictory schema, not a normalisation opportunity. A rewrite
            // would change what the node means. Leave the whole node alone.
            if let (Some(existing), Some(lifted)) = (obj.get("type"), lifted_type.as_ref())
                && existing != lifted
            {
                continue;
            }
            obj.remove(key);
            if let Some(t) = lifted_type {
                obj.insert("type".to_string(), t);
            }
            obj.insert("enum".to_string(), sorted_enum(&Value::Array(values)));
        }
    }
}

// ── The escape hatch, verified ───────────────────────────────────────────────

/// The fields that identify *which* change an acknowledgement covers.
///
/// The subject and the side-effect id are part of it (issue #1994). Without
/// them, an ack for workflow `charge` would also cover activity `charge`.
type AckKey = (
    SchemaSubject,
    String,
    Option<String>,
    Option<SchemaRole>,
    String,
    ChangeKind,
);

/// The identity that an acknowledgement covers.
fn ack_key(a: &AcknowledgedBreakingChange) -> AckKey {
    (
        a.subject,
        a.workflow.clone(),
        a.side_effect.clone(),
        a.role,
        a.field_path.clone(),
        a.change,
    )
}

/// The identity that an acknowledgement of `d` must carry.
fn delta_key(d: &SchemaDelta) -> AckKey {
    (
        d.subject,
        d.workflow.clone(),
        d.side_effect.clone(),
        d.role,
        d.field_path.clone(),
        d.change,
    )
}

/// Breaking deltas in `diff` that `head` does not acknowledge.
///
/// [`WorkflowSchemaContract::acknowledged_update`] refuses to absorb a breaking
/// change without a justification. But a contributor can bypass the tool by
/// hand-editing the artifact. The ordinary check is then trivially clean,
/// because the artifact and the freshly generated contract agree. The only
/// remaining signal is the artifact's own change since the PREVIOUS revision.
///
/// Pass `diff` = the diff of the *base revision's* artifact against the
/// current generated contract. Pass `base` = that base artifact, and `head` =
/// the artifact as it stands now. Every breaking delta must be covered by an
/// acknowledgement that is **new in `head`**. An entry already present at
/// `base` was recorded for an earlier change. Its reuse would let the same
/// field break twice against a single record. Matching is a multiset
/// difference over `(workflow, role, field_path, change)`, so N breaks need N
/// records.
///
/// A record whose `reason` is blank buys **no** coverage. Every authoring path
/// already refuses one. [`WorkflowSchemaContract::acknowledged_update`] returns
/// [`SchemaContractError::BlankAcknowledgement`], and [`diff_schema_contracts`]
/// reports [`ChangeKind::AcknowledgementMissingReason`]. But neither sees this
/// function's `head`, which the escape-hatch mode loads apart from the two
/// contracts it diffs. That mode can run alone, without the ordinary check whose
/// baseline *is* the head artifact. Without this rule, a hand-written rubber
/// stamp would then absorb a real break, since coverage matches on identity alone.
///
/// Returns the deltas with no covering record — empty means the escape hatch was
/// used correctly (or nothing broke).
#[must_use]
pub fn unacknowledged_breaking<'d>(
    diff: &'d SchemaContractDiff,
    base: &WorkflowSchemaContract,
    head: &WorkflowSchemaContract,
) -> Vec<&'d SchemaDelta> {
    let mut available: BTreeMap<AckKey, usize> = BTreeMap::new();
    for a in head
        .acknowledged_breaking_changes
        .iter()
        .filter(|a| !a.reason.trim().is_empty())
    {
        *available.entry(ack_key(a)).or_default() += 1;
    }
    // Consume the records the base revision already carried: they are not new.
    for a in &base.acknowledged_breaking_changes {
        if let Some(n) = available.get_mut(&ack_key(a)) {
            *n = n.saturating_sub(1);
        }
    }
    diff.breaking()
        .filter(|d| match available.get_mut(&delta_key(d)) {
            Some(n) if *n > 0 => {
                *n -= 1;
                false
            }
            _ => true,
        })
        .collect()
}

/// Acknowledgement records present at `base` that `head` no longer carries.
///
/// The log is promised **append-only**, and [`unacknowledged_breaking`] leans on
/// that: it subtracts the base revision's records so a stale one cannot cover a
/// fresh break. A contributor can *retarget* an existing record: edit its
/// workflow/role/path/change to name this PR's break. That defeats the
/// subtraction, because the identity it looked for is no longer in the base
/// multiset. The diff cannot see the rewrite, because the artifact and the
/// generated contract still agree. The multiset check cannot see it either,
/// because the retargeted record looks new. Its one observable signature is
/// that a record the previous revision carried has vanished.
///
/// Matching is a multiset difference over the same
/// `(workflow, role, field_path, change)` identity `unacknowledged_breaking`
/// uses. So an edit to only a record's `reason` or `recorded_in` is **not**
/// reported. Those fields cannot make a record cover a different break. A flag
/// on a typo fix would block legitimate work for no safety gain.
///
/// Returns the dropped records — empty means the log grew, or stayed put.
#[must_use]
pub fn dropped_acknowledgements<'b>(
    base: &'b WorkflowSchemaContract,
    head: &WorkflowSchemaContract,
) -> Vec<&'b AcknowledgedBreakingChange> {
    let mut available: BTreeMap<AckKey, usize> = BTreeMap::new();
    for a in &head.acknowledged_breaking_changes {
        *available.entry(ack_key(a)).or_default() += 1;
    }
    base.acknowledged_breaking_changes
        .iter()
        .filter(|a| match available.get_mut(&ack_key(a)) {
            Some(n) if *n > 0 => {
                *n -= 1;
                false
            }
            _ => true,
        })
        .collect()
}

// ── The differ ───────────────────────────────────────────────────────────────

/// Compare a baseline contract against the current one.
///
/// Both sides are canonicalised first, so annotation churn never produces a
/// delta.
#[must_use]
pub fn diff_schema_contracts(
    baseline: &WorkflowSchemaContract,
    current: &WorkflowSchemaContract,
) -> SchemaContractDiff {
    let mut diff = SchemaContractDiff::default();

    // A blank acknowledgement is a rubber stamp: fail the gate rather than let
    // a hand-edited artifact smuggle one in.
    for ack in baseline
        .acknowledged_breaking_changes
        .iter()
        .chain(&current.acknowledged_breaking_changes)
    {
        if ack.reason.trim().is_empty() {
            diff.push(SchemaDelta {
                subject: ack.subject,
                workflow: ack.workflow.clone(),
                side_effect: ack.side_effect.clone(),
                role: ack.role,
                field_path: ack.field_path.clone(),
                change: ChangeKind::AcknowledgementMissingReason,
                verdict: Verdict::Breaking,
                reason: format!(
                    "an acknowledged breaking change ({}) records no justification; an \
                     acknowledgement without a reason is a rubber stamp",
                    ack.change
                ),
            });
        }
    }

    let base_names: BTreeSet<&str> = baseline.workflows.iter().map(|w| w.name.as_str()).collect();
    let cur_names: BTreeSet<&str> = current.workflows.iter().map(|w| w.name.as_str()).collect();

    for name in cur_names.difference(&base_names) {
        diff.push(SchemaDelta {
            subject: SchemaSubject::Workflow,
            workflow: (*name).to_string(),
            side_effect: None,
            role: None,
            field_path: String::new(),
            change: ChangeKind::WorkflowAdded,
            verdict: Verdict::Compatible,
            reason: "new workflow type — no recorded history exists for it yet".to_string(),
        });
    }

    for name in base_names.difference(&cur_names) {
        diff.push(SchemaDelta {
            subject: SchemaSubject::Workflow,
            workflow: (*name).to_string(),
            side_effect: None,
            role: None,
            field_path: String::new(),
            change: ChangeKind::WorkflowRemoved,
            verdict: Verdict::Compatible,
            reason: "workflow type is no longer registered. Payload compatibility is not the \
                     right question here: run `harvest workflow-types reachability` (issue #520), \
                     which counts actual non-terminal executions of this type before you delete \
                     the handler"
                .to_string(),
        });
    }

    for name in base_names.intersection(&cur_names) {
        let (Some(b), Some(c)) = (baseline.entry(name), current.entry(name)) else {
            continue;
        };
        let subject = Subject {
            kind: SchemaSubject::Workflow,
            name,
            side_effect: None,
        };
        for role in SchemaRole::all() {
            diff_role(
                subject,
                role,
                b.schema_for(role),
                c.schema_for(role),
                &mut diff,
            );
        }
    }

    diff_activities(baseline, current, &mut diff);
    diff_side_effects(baseline, current, &mut diff);

    diff.deltas.sort_by(|a, b| {
        (
            a.subject,
            &a.workflow,
            &a.side_effect,
            a.role,
            &a.field_path,
            a.change,
        )
            .cmp(&(
                b.subject,
                &b.workflow,
                &b.side_effect,
                b.role,
                &b.field_path,
                b.change,
            ))
    });
    diff
}

/// Diff the activity sections (issue #1994).
///
/// The schema rules match the workflow walk. An added activity is compatible.
/// A removed one is breaking. The generator list is not tied to the runtime
/// registry. A workflow decodes a recorded result whether or not a handler
/// exists. A silent removal would let a later re-add change the type unchecked.
fn diff_activities(
    baseline: &WorkflowSchemaContract,
    current: &WorkflowSchemaContract,
    diff: &mut SchemaContractDiff,
) {
    let base: BTreeSet<&str> = baseline
        .activities
        .iter()
        .map(|a| a.name.as_str())
        .collect();
    let cur: BTreeSet<&str> = current.activities.iter().map(|a| a.name.as_str()).collect();
    let subject = |name| Subject {
        kind: SchemaSubject::Activity,
        name,
        side_effect: None,
    };
    for name in cur.difference(&base) {
        diff.push(subject(name).delta(
            None,
            ChangeKind::ActivityAdded,
            Verdict::Compatible,
            "new activity type. No recorded history exists for it yet".to_string(),
        ));
    }
    for name in base.difference(&cur) {
        diff.push(subject(name).delta(
            None,
            ChangeKind::ActivityRemoved,
            Verdict::Breaking,
            "the current contract no longer lists this activity, so its payloads are no longer \
             checked. In-flight runs still decode its recorded results on replay. Acknowledge \
             the removal after those runs end and its queued tasks drain"
                .to_string(),
        ));
    }
    for name in base.intersection(&cur) {
        let (Some(b), Some(c)) = (baseline.activity(name), current.activity(name)) else {
            continue;
        };
        for role in SchemaRole::activity() {
            diff_role(
                subject(name),
                role,
                b.schema_for(role),
                c.schema_for(role),
                diff,
            );
        }
    }
}

/// Diff the side-effect sections (issue #1994).
///
/// An added side effect is compatible. A removed one is breaking: the
/// declaration is not registered with the runtime, so its call site can
/// outlive it. A silent removal would stop the check with the value still read.
///
/// One exception: the owning workflow left the contract too. That removal is
/// compatible and is gated elsewhere (issue #520), so its side effects follow it.
fn diff_side_effects(
    baseline: &WorkflowSchemaContract,
    current: &WorkflowSchemaContract,
    diff: &mut SchemaContractDiff,
) {
    let key = |s: &SideEffectSchemaEntry| (s.workflow.clone(), s.id.clone());
    let base: BTreeSet<(String, String)> = baseline.side_effects.iter().map(key).collect();
    let cur: BTreeSet<(String, String)> = current.side_effects.iter().map(key).collect();
    for (workflow, id) in cur.difference(&base) {
        diff.push(
            Subject {
                kind: SchemaSubject::SideEffect,
                name: workflow,
                side_effect: Some(id),
            }
            .delta(
                None,
                ChangeKind::SideEffectAdded,
                Verdict::Compatible,
                "new side effect. No recorded value exists for it yet".to_string(),
            ),
        );
    }
    let base_workflows: BTreeSet<&str> =
        baseline.workflows.iter().map(|w| w.name.as_str()).collect();
    let cur_workflows: BTreeSet<&str> = current.workflows.iter().map(|w| w.name.as_str()).collect();
    for (workflow, id) in base.difference(&cur) {
        let owner_removed = base_workflows.contains(workflow.as_str())
            && !cur_workflows.contains(workflow.as_str());
        let (verdict, reason) = if owner_removed {
            (
                Verdict::Compatible,
                "the owning workflow is no longer registered, so its side effects go with it. \
                 `harvest workflow-types reachability` (issue #520) gates that removal",
            )
        } else {
            (
                Verdict::Breaking,
                "the current contract no longer declares this side effect. The declaration is \
                 not registered with the runtime, so the call site can still read recorded \
                 values. Acknowledge the removal if the call site is gone too",
            )
        };
        diff.push(
            Subject {
                kind: SchemaSubject::SideEffect,
                name: workflow,
                side_effect: Some(id),
            }
            .delta(
                None,
                ChangeKind::SideEffectRemoved,
                verdict,
                reason.to_string(),
            ),
        );
    }
    for (workflow, id) in base.intersection(&cur) {
        let (Some(b), Some(c)) = (
            baseline.side_effect(workflow, id),
            current.side_effect(workflow, id),
        ) else {
            continue;
        };
        diff_role(
            Subject {
                kind: SchemaSubject::SideEffect,
                name: workflow,
                side_effect: Some(id),
            },
            SchemaRole::Value,
            b.value_schema.as_ref(),
            c.value_schema.as_ref(),
            diff,
        );
    }
}

fn diff_role(
    subject: Subject<'_>,
    role: SchemaRole,
    baseline: Option<&Value>,
    current: Option<&Value>,
    diff: &mut SchemaContractDiff,
) {
    match (baseline, current) {
        (None, None) => {}
        (None, Some(_)) => diff.push(subject.delta(
            Some(role),
            ChangeKind::SchemaAdded,
            Verdict::Compatible,
            format!(
                "a {role} schema is now published; recorded payloads were previously \
                 unconstrained by this gate"
            ),
        )),
        (Some(_), None) => diff.push(subject.delta(
            Some(role),
            ChangeKind::SchemaRemoved,
            Verdict::Breaking,
            format!(
                "the published {role} schema was withdrawn. This does not break replay by \
                 itself, but it removes this payload from the gate's coverage — deleting a \
                 schema must never be a silent way to stop being checked"
            ),
        )),
        (Some(b), Some(c)) => {
            let bc = canonicalize_node(b, 0);
            let cc = canonicalize_node(c, 0);
            let (bc, cc) = (bc.as_ref().unwrap_or(b), cc.as_ref().unwrap_or(c));
            let mut ctx = DiffCtx {
                subject,
                role,
                base_root: bc,
                cur_root: cc,
                visited: BTreeMap::new(),
                active_hits: Vec::new(),
                pending_branch_guards: Vec::new(),
                suppressed_changes: 0,
            };
            diff_node(&mut ctx, bc, cc, "", 0, diff);
        }
    }
}

/// Per-schema walk state.
struct DiffCtx<'a> {
    subject: Subject<'a>,
    role: SchemaRole,
    base_root: &'a Value,
    cur_root: &'a Value,
    /// `$ref` pairs already expanded, so a recursive type terminates. The
    /// value records whether that pair's subtree produced any delta, so a later
    /// memo HIT can still tell a caller "this changed" — see
    /// [`DiffCtx::suppressed_changes`].
    visited: BTreeMap<(String, String), PairVisit>,
    /// Pairs whose traversal a back-edge landed in *while it was still running*,
    /// appended in visit order. A caller samples the length around its own
    /// recursion; anything new is a pair whose verdict cannot be known yet.
    active_hits: Vec<(String, String)>,
    /// Overlap checks that could not be decided when they ran, because the
    /// branch back-edged into a pair still being traversed. Re-examined as each
    /// of those pairs settles — see [`run_pending_branch_guards`].
    pending_branch_guards: Vec<PendingBranchGuard>,
    /// How many times a memo hit swallowed a subtree that had CHANGED.
    ///
    /// The memo makes the diff linear. But it also makes the second visit to a
    /// pair emit nothing. One caller, [`diff_branch_guarding_rebind`], runs its
    /// overlap check only when the branch emitted something. A shared
    /// definition reached first from an ordinary property and then from a
    /// `oneOf` branch would silence that check entirely. This counter is
    /// monotonic like `visited`. A caller samples it around its own recursion
    /// and treats an increase as "changed, but not inspectable here".
    suppressed_changes: usize,
}

/// Where a `$ref` pair is in the traversal.
///
/// The distinction is what separates "already compared, and it did not change"
/// from "cannot say yet". Collapsing them to one `bool` is what let a
/// self-recursive branch read its own unfinished traversal as unchanged.
#[derive(Clone, Copy)]
enum PairVisit {
    /// Being traversed right now. A back-edge lands here; its verdict is not
    /// decidable until the frame that opened it returns.
    InProgress,
    /// Traversal finished. `true` when the subtree produced any delta.
    Done(bool),
}

/// A branch-overlap check parked until the pair it depends on settles.
///
/// Owned rather than borrowed, because the check outlives the traversal frame
/// that registered it. It only fires for a branch that back-edged into an
/// active pair, which means a genuinely recursive type. So the clone is off the
/// hot path.
struct PendingBranchGuard {
    /// Every active pair this branch reached. The branch changed iff *any* of
    /// them did, so the first to settle as changed fires the check and the
    /// record is consumed.
    on_pairs: Vec<(String, String)>,
    cur: Value,
    rivals: Vec<Value>,
    is_anyof: bool,
    path: String,
    sub_path: String,
}

impl DiffCtx<'_> {
    fn delta(
        &self,
        field_path: &str,
        change: ChangeKind,
        verdict: Verdict,
        reason: String,
    ) -> SchemaDelta {
        SchemaDelta {
            field_path: field_path.to_string(),
            ..self.subject.delta(Some(self.role), change, verdict, reason)
        }
    }
}

/// Resolve a chain of local `$ref`s against `root`, returning the target and the
/// pointer that produced it.
fn resolve_ref<'a>(root: &'a Value, schema: &'a Value) -> (&'a Value, Option<String>) {
    let mut node = schema;
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    let mut last: Option<String> = None;
    while let Some(r) = node.get("$ref").and_then(Value::as_str) {
        if !seen.insert(r) {
            break; // cycle inside the ref chain itself
        }
        let Some(stripped) = r.strip_prefix('#') else {
            break; // external refs are not resolvable offline
        };
        let Some(target) = root.pointer(stripped) else {
            break;
        };
        node = target;
        last = Some(r.to_string());
    }
    (node, last)
}

#[allow(clippy::too_many_lines)]
fn diff_node(
    ctx: &mut DiffCtx<'_>,
    baseline: &Value,
    current: &Value,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    if depth >= MAX_DIFF_DEPTH {
        diff.push(ctx.delta(
            path,
            ChangeKind::DiffDepthCapReached,
            Verdict::Breaking,
            format!(
                "schema nesting exceeds the maximum supported depth ({MAX_DIFF_DEPTH}); the \
                 remainder of this branch was not compared"
            ),
        ));
        return;
    }

    // Resolve `$ref` on BOTH sides. A change inside `definitions.Inner` leaves
    // `properties.inner` byte-identical, so without this the most common nested
    // break is invisible.
    let (b, b_ref) = resolve_ref(ctx.base_root, baseline);
    let (c, c_ref) = resolve_ref(ctx.cur_root, current);
    if b_ref.is_some() || c_ref.is_some() {
        let key = (
            b_ref.unwrap_or_else(|| path.to_string()),
            c_ref.unwrap_or_else(|| path.to_string()),
        );
        if let Some(&state) = ctx.visited.get(&key) {
            // Already expanded this pair: either a recursive type, or a
            // definition reached by a second path. Coinductively compatible —
            // anything reachable from here was compared the first time.
            //
            // `visited` is deliberately MONOTONIC (no `remove` on the way out).
            // Scoped to the current stack, it would be a cycle guard only.
            // Then a schema whose definitions cross-reference fans out
            // exponentially: a few KB of `$ref`s never finishes. The memo
            // makes each pair O(1) and the whole diff linear in the number of
            // distinct pairs.
            //
            // Emitting nothing is right for the DIFF. It is wrong for a caller
            // that reads "emitted nothing" as "did not change". Take the same
            // definition reached from an ordinary property and from a `oneOf`
            // branch. A compatible relaxation would silence the branch-overlap
            // check. So the pair's own verdict is replayed on the counter, and
            // the signal survives without a second walk of the subtree.
            //
            // A pair still IN PROGRESS has no verdict to replay. This is a
            // back-edge into the traversal that this call is nested in, so
            // whether it changed is not yet knowable. "No" would be a guess in
            // the unsafe direction. "Yes" would report every unchanged
            // recursive type as breaking. Record the back-edge and let the
            // caller park its decision until the pair settles.
            match state {
                PairVisit::Done(true) => ctx.suppressed_changes += 1,
                PairVisit::Done(false) => {}
                PairVisit::InProgress => ctx.active_hits.push(key),
            }
            return;
        }
        ctx.visited.insert(key.clone(), PairVisit::InProgress);
        let before_total = diff.breaking_count + diff.compatible_count;
        let before_suppressed = ctx.suppressed_changes;
        // A chain that terminates on a CYCLE lands on a node that STILL carries
        // `$ref` (`resolve_ref` breaks out rather than looping). A recursion
        // would re-resolve to this same pair, hit the memo above, and return.
        // The landing node's OWN keywords would never be compared, and
        // `{"$ref":"#","type":"string"}` -> `"integer"` would read as no change.
        // The engine's `validate_node` breaks the cycle the same way and then
        // enforces exactly those siblings, so fall through and compare them.
        //
        // Only the cycle-terminated case falls through. When resolution reaches
        // a ref-free target, the siblings of the referring node are correctly
        // discarded. That is draft-07 `$ref` semantics, and the engine's own
        // `schema = resolved` reassignment does the same.
        if b.get("$ref").is_none() && c.get("$ref").is_none() {
            diff_node(ctx, b, c, path, depth + 1, diff);
        } else {
            // The landing node of a CYCLE. Termination still holds: the memo was
            // already inserted, so anything under it that walks back around the
            // cycle resolves to this same pair and returns.
            diff_node_body(ctx, b, c, path, depth, diff);
        }
        // Record what this pair's subtree did, for every later memo hit. A
        // nested pair that was itself swallowed counts too, or the suppression
        // would simply move one level down.
        let changed = diff.breaking_count + diff.compatible_count > before_total
            || ctx.suppressed_changes > before_suppressed;
        ctx.visited.insert(key.clone(), PairVisit::Done(changed));
        // The pair now has a verdict, so any check that back-edged into it while
        // it was running can finally be resolved.
        if changed {
            run_pending_branch_guards(ctx, &key, diff);
        }
        return;
    }

    diff_node_body(ctx, b, c, path, depth, diff);
}

/// Everything [`diff_node`] does once `$ref` resolution and memoisation are
/// settled: compare two already-resolved nodes keyword by keyword.
///
/// Split out so `diff_node` can measure exactly what the subtree of one `$ref`
/// pair contributes. The cycle-landing case falls through to the same body
/// that the ref-free case reaches by recursion. Both must be inside the
/// measurement.
fn diff_node_body(
    ctx: &mut DiffCtx<'_>,
    b: &Value,
    c: &Value,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let (Some(bo), Some(co)) = (b.as_object(), c.as_object()) else {
        // A non-object schema node: JSON Schema's boolean form (`true`/`false`)
        // or something malformed. None of the keyword comparisons apply. But
        // the two sides can still differ: `false` rejects everything, `true`
        // accepts everything. So a change here is reported, not skipped. Equal
        // non-object nodes are genuinely no change.
        if b != c {
            diff.push(ctx.delta(
                path,
                ChangeKind::UnanalysedConstraintChanged,
                Verdict::Breaking,
                format!(
                    "this schema node is not an object (JSON Schema's boolean form, or \
                     malformed) and changed from `{b}` to `{c}`; the differ compares keywords \
                     and cannot analyse it, so the change is reported conservatively"
                ),
            ));
        }
        return;
    };

    // A `$ref` that survived resolution is one the differ could NOT follow:
    // external (`https://…`), dangling, or a cycle inside the chain. Both sides
    // are then compared as opaque nodes — and `$ref` sits in ANALYSED_KEYWORDS,
    // which excludes it from `diff_unanalysed`'s fail-closed sweep. Without this
    // check, swapping one unresolvable target for another reads as NO CHANGE AT
    // ALL. Runs before the branch-set split so it cannot be skipped by an early
    // return.
    diff_unresolved_ref(ctx, bo, co, path, diff);

    // When exactly one side is a `oneOf`/`anyOf` container, `diff_branches`
    // lifts the flat side into a one-element branch set and compares it there
    // IN FULL. A flat comparison of the keywords as well would double-count.
    // The `properties` of the flat side would read as "removed" merely because
    // the other side moved them inside a branch. `T` -> `Option<T>` in the
    // schemars non-primitive form (`{"anyOf":[{"$ref":…},{"type":"null"}]}`)
    // is exactly this shape. An error here would report the single most
    // common widening as breaking.
    if branch_set(bo).is_some() != branch_set(co).is_some() {
        guard_container_siblings(ctx, bo, co, path, diff);
        diff_branches(ctx, bo, co, path, depth, diff);
        return;
    }

    diff_types(ctx, bo, co, path, diff);
    diff_enum(ctx, bo, co, path, diff);
    diff_format(ctx, bo, co, path, diff);
    diff_bounds(ctx, bo, co, path, diff);
    diff_properties(ctx, bo, co, path, depth, diff);
    diff_additional_properties(ctx, bo, co, path, depth, diff);
    diff_items(ctx, bo, co, path, depth, diff);
    diff_branches(ctx, bo, co, path, depth, diff);
    diff_all_of(ctx, bo, co, path, depth, diff);
    diff_unanalysed(ctx, bo, co, path, diff);
}

/// Compare `allOf` conjunctions.
///
/// `allOf` is an **AND**: a value must satisfy every member. So a new member
/// is a narrowing, and that shape would otherwise slip through.
/// `{"type":"integer"}` → `{"allOf":[{"type":"integer"},{"minimum":10}]}`
/// rejects a recorded `1`, while every keyword of the flat comparison is
/// unchanged.
///
/// `allOf` sits in [`ANALYSED_KEYWORDS`], which excludes it from
/// [`diff_unanalysed`]'s fail-closed sweep, so this is the *only* thing
/// standing between that change and a `compatible` verdict.
///
/// Members are paired **by position**. `allOf` is an ordered array in both
/// `schemars` output and hand-written schemas. Only a positional pair lets a
/// change *inside* a member be reported at its real path, not as a wholesale
/// replacement.
///
/// A length change is reported rather than guessed:
///
/// - **added** conjunct → narrowing, breaking.
/// - **removed** conjunct → also breaking, fail-closed. Removal looks like a
///   loosening, but `allOf` members interact with the parent. Draft-07
///   `additionalProperties` does **not** see into subschemas. So a dropped
///   member that declared `properties` turns those keys into "additional" and
///   *tightens* the schema. The unanalysed-keyword policy uses the same
///   reasoning. The differ does not model keyword interactions, so it refuses
///   to guess and lets the author acknowledge.
///
/// The most common `schemars` shape never reaches here: a lone
/// `{"allOf":[{"$ref":…}]}` wrapper. `RemoveRefSiblings` produces it when a
/// field gains a doc comment. [`flatten_single_member_all_of`] lifts it during
/// canonicalisation, so a `///` edit stays a no-op.
fn diff_all_of(
    ctx: &mut DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let base = bo.get("allOf").and_then(Value::as_array);
    let cur = co.get("allOf").and_then(Value::as_array);

    match (base, cur) {
        (None, None) => {}
        (None, Some(c)) => diff.push(ctx.delta(
            path,
            ChangeKind::AllOfMembersChanged,
            Verdict::Breaking,
            format!(
                "an `allOf` conjunction of {} schema(s) was introduced; every recorded value \
                 must now satisfy all of them, so any value that fails one is rejected",
                c.len()
            ),
        )),
        (Some(b), None) => diff.push(ctx.delta(
            path,
            ChangeKind::AllOfMembersChanged,
            Verdict::Breaking,
            format!(
                "an `allOf` conjunction of {} schema(s) was removed; this usually loosens the \
                 schema, but `allOf` members interact with the parent (draft-07 \
                 `additionalProperties` does not see inside them), so the differ reports it \
                 rather than guessing. Acknowledge it if the removal is genuinely a widening",
                b.len()
            ),
        )),
        (Some(b), Some(c)) if b.len() == c.len() => {
            for (i, (bm, cm)) in b.iter().zip(c.iter()).enumerate() {
                diff_node(ctx, bm, cm, &format!("{path}/allOf[{i}]"), depth + 1, diff);
            }
        }
        (Some(b), Some(c)) => diff.push(ctx.delta(
            path,
            ChangeKind::AllOfMembersChanged,
            Verdict::Breaking,
            format!(
                "the `allOf` conjunction changed from {} to {} member(s); members are paired \
                 by position, so a length change cannot be compared member-by-member and is \
                 reported conservatively",
                b.len(),
                c.len()
            ),
        )),
    }
}

/// Constraint keywords a `oneOf`/`anyOf` container carries *alongside* its
/// branches. JSON Schema conjoins these with the branch set, but the branch
/// comparison drops them, so their presence is reported rather than ignored.
///
/// schemars never emits this shape; it is reachable only from a hand-written
/// schema.
fn container_sibling_keys(obj: &Map<String, Value>) -> BTreeSet<&str> {
    obj.keys()
        .map(String::as_str)
        .filter(|k| {
            *k != "oneOf"
                && *k != "anyOf"
                && !ANNOTATION_KEYWORDS.contains(k)
                && !CONTAINER_KEYWORDS.contains(k)
        })
        .collect()
}

/// Fail closed when a branch container carries sibling constraint keywords
/// that the branch-set comparison would silently drop.
fn guard_container_siblings(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    // Only the *container* side's siblings are at risk: the flat side is
    // lifted whole into the branch set, so nothing of it is dropped.
    let container = if branch_set(bo).is_some() { bo } else { co };
    let siblings = container_sibling_keys(container);
    if siblings.is_empty() {
        return;
    }
    diff.push(ctx.delta(
        path,
        ChangeKind::UnanalysedConstraintChanged,
        Verdict::Breaking,
        format!(
            "a `oneOf`/`anyOf` container also carries the constraint keyword(s) {} \
             alongside its branches; the differ compares branch sets only, so this \
             node was not fully analysed and is reported conservatively",
            siblings.into_iter().collect::<Vec<_>>().join(", ")
        ),
    ));
}

/// The set of JSON types a schema node accepts. An absent `type` accepts all.
fn type_set(obj: &Map<String, Value>) -> Option<BTreeSet<String>> {
    let node = obj.get("type")?;
    node.as_str().map_or_else(
        || {
            node.as_array().map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
        },
        |s| Some(std::iter::once(s.to_string()).collect()),
    )
}

/// `true` when every JSON value accepted by `base` is accepted by `cur`.
///
/// `integer` is a subset of `number`: every recorded integer is a valid number.
fn type_set_admits(base: &BTreeSet<String>, cur: &BTreeSet<String>) -> bool {
    base.iter()
        .all(|t| cur.contains(t) || (t == "integer" && cur.contains("number")))
}

fn diff_types(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let (base, cur) = (type_set(bo), type_set(co));
    match (base, cur) {
        (None, Some(c)) => diff.push(ctx.delta(
            path,
            ChangeKind::TypeNarrowed,
            Verdict::Breaking,
            format!(
                "this value accepted any JSON type and is now restricted to {}",
                join_sorted(&c)
            ),
        )),
        (Some(_), None) => diff.push(ctx.delta(
            path,
            ChangeKind::TypeWidened,
            Verdict::Compatible,
            "the type restriction was removed".to_string(),
        )),
        (Some(b), Some(c)) if b != c => {
            if type_set_admits(&b, &c) {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::TypeWidened,
                    Verdict::Compatible,
                    format!(
                        "type widened from {} to {}",
                        join_sorted(&b),
                        join_sorted(&c)
                    ),
                ));
            } else {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::TypeNarrowed,
                    Verdict::Breaking,
                    format!(
                        "type narrowed from {} to {}; recorded JSON holding a {} value no longer \
                         deserializes",
                        join_sorted(&b),
                        join_sorted(&c),
                        join_sorted(&b.difference(&c).cloned().collect())
                    ),
                ));
            }
        }
        _ => {}
    }
}

fn join_sorted(set: &BTreeSet<String>) -> String {
    set.iter().cloned().collect::<Vec<_>>().join("|")
}

fn diff_enum(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let (Some(b), Some(c)) = (
        bo.get("enum").and_then(Value::as_array),
        co.get("enum").and_then(Value::as_array),
    ) else {
        // An `enum` appearing or disappearing is a constraint change handled by
        // the unanalysed sweep (appearing) or is a pure relaxation (vanishing).
        if bo.contains_key("enum") && !co.contains_key("enum") {
            diff.push(ctx.delta(
                path,
                ChangeKind::BoundRelaxed,
                Verdict::Compatible,
                "the enum restriction was removed".to_string(),
            ));
        } else if !bo.contains_key("enum") && co.contains_key("enum") {
            diff.push(ctx.delta(
                path,
                ChangeKind::BoundTightened,
                Verdict::Breaking,
                "this value was unconstrained and is now restricted to a fixed set of enum values"
                    .to_string(),
            ));
        } else if bo.get("enum") != co.get("enum") {
            // Both revisions carry the key, because the arms above ruled out an
            // absent one. Yet at least one is not an array, so the differ
            // cannot read what it restricts. A validator ignores the unreadable
            // side and enforces the readable one. So `"x"` -> `["x"]` newly
            // rejects every recorded value outside the set. Every flat-compared
            // keyword stays unchanged. Fail closed, as the unanalysed sweep
            // does. `enum` is nominally analysed, so it never reaches that
            // sweep.
            diff.push(ctx.delta(
                path,
                ChangeKind::UnanalysedConstraintChanged,
                Verdict::Breaking,
                "the `enum` keyword changed and at least one revision is not a JSON array, so what \
                 it restricts cannot be determined; a malformed enum constrains nothing at runtime \
                 while a well-formed one does"
                    .to_string(),
            ));
        }
        return;
    };
    let bset: BTreeSet<String> = b.iter().map(ToString::to_string).collect();
    let cset: BTreeSet<String> = c.iter().map(ToString::to_string).collect();
    let removed: Vec<String> = bset.difference(&cset).cloned().collect();
    let added: Vec<String> = cset.difference(&bset).cloned().collect();
    if !removed.is_empty() {
        diff.push(ctx.delta(
            path,
            ChangeKind::EnumValueRemoved,
            Verdict::Breaking,
            format!(
                "enum value(s) {} removed; recorded data may still contain them and would fail to \
                 deserialize (serde: `unknown variant`)",
                removed.join(", ")
            ),
        ));
    }
    if !added.is_empty() {
        diff.push(ctx.delta(
            path,
            ChangeKind::EnumValueAdded,
            Verdict::Compatible,
            format!(
                "enum value(s) {} added; a read type accepting a superset is safe",
                added.join(", ")
            ),
        ));
    }
}

/// Compare `format` on both the numeric-width lattice and the semantic axis.
///
/// A numeric width is orderable, so `int32 -> int64` is a widening. A semantic
/// format (`uuid`, `date-time`, `ip`, …) is an opaque label. `schemars` records
/// it because the Rust type of the field PARSES the string. That type's
/// `Deserialize` rejects a string that does not parse. So a new or changed one
/// narrows what recorded JSON still reads. Only its removal is safe.
///
/// **Accepted cost:** a purely descriptive `#[schemars(format = "email")]` on a
/// plain `String`, where nothing validates, is flagged too. The differ cannot
/// see the Rust type, only the schema. The alternative is a curated allowlist
/// of "really assertive" formats, which is silently wrong for any custom
/// `JsonSchema` impl. This is a one-time, acknowledgeable flag. The mirror
/// mistake would let a real `String -> Uuid` break merge silently.
fn diff_format(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let b = bo.get("format").and_then(Value::as_str);
    let c = co.get("format").and_then(Value::as_str);
    match (b, c) {
        (Some(bf), Some(cf)) if bf != cf => {
            let (Some(br), Some(cr)) = (numeric_format_range(bf), numeric_format_range(cf)) else {
                // At least one side is a semantic label, so there is no shared
                // lattice to compare on. Two different labels name two
                // different parsers; a recorded value that satisfied the old
                // one need not satisfy the new one.
                diff.push(ctx.delta(
                    path,
                    ChangeKind::StringFormatNarrowed,
                    Verdict::Breaking,
                    format!(
                        "`format` changed from `{bf}` to `{cf}`; a semantic format names the \
                         parser the field's type runs, so a recorded value accepted by `{bf}` may \
                         fail to deserialize under `{cf}`"
                    ),
                ));
                return;
            };
            // Fractional -> integral is breaking on the integrality axis, no
            // matter how the ranges compare: a recorded `1.5` deserializes into
            // no integer type. Checked before the range lattice, which would
            // otherwise call `float -> int32` a widening.
            if !is_integral_format(bf) && is_integral_format(cf) {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::NumericFormatNarrowed,
                    Verdict::Breaking,
                    format!(
                        "numeric format changed from the fractional {bf} to the integral {cf}; a \
                         recorded non-integer value fails to deserialize (serde: `invalid type: \
                         floating point …, expected {cf}`), regardless of range"
                    ),
                ));
            } else if cr.0 <= br.0 && cr.1 >= br.1 {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::NumericFormatWidened,
                    Verdict::Compatible,
                    format!("numeric width widened from {bf} to {cf}"),
                ));
            } else {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::NumericFormatNarrowed,
                    Verdict::Breaking,
                    format!(
                        "numeric width narrowed from {bf} to {cf}; a recorded value outside the \
                         new range fails to deserialize (serde: `invalid value: integer …, \
                         expected {cf}`). Note the JSON `type` is unchanged — width lives only in \
                         `format`"
                    ),
                ));
            }
        }
        (None, Some(cf)) if is_numeric_format(cf) => diff.push(ctx.delta(
            path,
            ChangeKind::NumericFormatNarrowed,
            Verdict::Breaking,
            format!("a numeric width ({cf}) was introduced where the value was unconstrained"),
        )),
        (Some(bf), None) if is_numeric_format(bf) => diff.push(ctx.delta(
            path,
            ChangeKind::NumericFormatWidened,
            Verdict::Compatible,
            format!("the {bf} numeric width restriction was removed"),
        )),
        (None, Some(cf)) => diff.push(ctx.delta(
            path,
            ChangeKind::StringFormatNarrowed,
            Verdict::Breaking,
            format!(
                "`format: {cf}` was introduced where the value was unconstrained; `schemars` \
                 emits it because the field's type PARSES the value (`String -> Uuid` is the \
                 canonical case), so a recorded value that is not a valid `{cf}` fails to \
                 deserialize"
            ),
        )),
        (Some(bf), None) => diff.push(ctx.delta(
            path,
            ChangeKind::StringFormatWidened,
            Verdict::Compatible,
            format!(
                "the `{bf}` format restriction was removed; every recorded value that satisfied \
                 it is still accepted by the unconstrained type"
            ),
        )),
        _ => {}
    }
}

fn diff_bounds(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    for (keys, tighter_is_greater) in [(LOWER_BOUNDS, true), (UPPER_BOUNDS, false)] {
        for key in keys {
            let (b_raw, c_raw) = (bo.get(*key), co.get(*key));
            // A bound that is PRESENT but unreadable cannot be placed on the
            // tighten/relax lattice. As `None` it would look absent. A
            // malformed value would then read as "`minimum` was removed" =>
            // COMPATIBLE. Two different malformed values would produce no
            // delta at all.
            //
            // "Readable" has to mean what the ENGINE'S validator reads, not
            // merely what is a JSON number. `validate_node` takes a cardinality
            // (`minLength`/`maxLength`/`minItems`/`maxItems`) through `as_u64`
            // and a numeric bound (`minimum`/`maximum`) through `as_f64`. With
            // `as_f64` for both, a cardinality the validator IGNORES (`1.5`)
            // would share a lattice with one it ENFORCES (`1`). `1.5 -> 1`
            // would then score a relaxation. The effective constraint actually
            // went from none to "rejects the empty string".
            let unreadable = |v: Option<&Value>| {
                v.is_some_and(|v| {
                    if is_cardinality_bound(key) {
                        v.as_u64().is_none()
                    } else {
                        v.as_f64().is_none()
                    }
                })
            };
            if unreadable(b_raw) || unreadable(c_raw) {
                if b_raw != c_raw {
                    diff.push(ctx.delta(
                        path,
                        ChangeKind::UnanalysedConstraintChanged,
                        Verdict::Breaking,
                        format!(
                            "`{key}` is present but is not a JSON number on at least one side, so \
                             it cannot be placed on the tighten/relax lattice; the change is \
                             reported breaking (fail closed)"
                        ),
                    ));
                }
                continue;
            }
            match (b_raw, c_raw) {
                (None, Some(cv)) => diff.push(ctx.delta(
                    path,
                    ChangeKind::BoundTightened,
                    Verdict::Breaking,
                    format!(
                        "`{key}` was introduced ({cv}); previously-valid recorded values may now \
                         be rejected"
                    ),
                )),
                (Some(_), None) => diff.push(ctx.delta(
                    path,
                    ChangeKind::BoundRelaxed,
                    Verdict::Compatible,
                    format!("`{key}` was removed"),
                )),
                (Some(bv), Some(cv)) => {
                    // Compared EXACTLY, not through `f64` with an absolute
                    // tolerance. See `cmp_json_numbers`: both a sub-epsilon
                    // change and a >2^53 integer change are real narrowings
                    // that an `f64::EPSILON` guard reports as no delta.
                    let Some(ord) = cmp_json_numbers(bv, cv) else {
                        diff.push(ctx.delta(
                            path,
                            ChangeKind::UnanalysedConstraintChanged,
                            Verdict::Breaking,
                            format!(
                                "`{key}` changed from {bv} to {cv} but the two values are not \
                                 comparable (NaN); the change cannot be placed on the \
                                 tighten/relax lattice and is reported breaking (fail closed)"
                            ),
                        ));
                        continue;
                    };
                    if ord == Ordering::Equal {
                        continue;
                    }
                    // `ord` is `bv.cmp(cv)`: for a LOWER bound, tightening
                    // means the value went UP, i.e. `bv < cv`.
                    let tightened = if tighter_is_greater {
                        ord == Ordering::Less
                    } else {
                        ord == Ordering::Greater
                    };
                    if tightened {
                        diff.push(ctx.delta(
                            path,
                            ChangeKind::BoundTightened,
                            Verdict::Breaking,
                            format!(
                                "`{key}` tightened from {bv} to {cv}; previously-valid recorded \
                                 values may now be rejected"
                            ),
                        ));
                    } else {
                        diff.push(ctx.delta(
                            path,
                            ChangeKind::BoundRelaxed,
                            Verdict::Compatible,
                            format!("`{key}` relaxed from {bv} to {cv}"),
                        ));
                    }
                }
                (None, None) => {}
            }
        }
    }
}

/// Compare two JSON numbers **without** losing precision to `f64`.
///
/// Reading a bound through `as_f64` and testing `(b - c).abs() > f64::EPSILON`
/// is lossy in both directions that matter:
///
/// * `f64::EPSILON` (~2.2e-16) is machine epsilon **at 1.0**, so used as an
///   absolute tolerance it swallows any smaller change. `minimum: 1e-20` →
///   `2e-20` is a genuine narrowing — it rejects values the baseline accepted —
///   yet compares "equal".
/// * `f64` has a 53-bit mantissa, so distinct integers above 2^53 collapse:
///   `9007199254740993` and `9007199254740992` become the same `f64`. A bound
///   on an `i64` field can move without producing a delta.
///
/// Integers are therefore compared exactly through `i128` (every JSON integer
/// `serde_json` produces fits in `i64` or `u64`). The MIXED pair (one integer,
/// one float) must not fall back either. An `f64` copy of the integer brings
/// back the mantissa collapse above. `9007199254740993` against the float
/// `9007199254740992.0` rounds to a tie, which hides a genuine tightening.
/// [`cmp_i128_f64`] keeps the integer exact instead. Only a float/float pair
/// uses `f64`, where `f64` *is* both values' own representation and nothing
/// JSON preserved is lost. `None` means incomparable
/// (NaN), which callers report fail-closed rather than treating as unchanged.
fn cmp_json_numbers(a: &Value, b: &Value) -> Option<Ordering> {
    let exact = |v: &Value| {
        v.as_i64()
            .map(i128::from)
            .or_else(|| v.as_u64().map(i128::from))
    };
    match (exact(a), exact(b)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        (Some(x), None) => cmp_i128_f64(x, b.as_f64()?),
        // `cmp_i128_f64` answers "integer vs float"; this pair is the reverse.
        (None, Some(y)) => cmp_i128_f64(y, a.as_f64()?).map(Ordering::reverse),
        (None, None) => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// Compare an exact integer against an `f64` **without** rounding the integer.
///
/// `f.floor()` is a whole-valued `f64`, which converts to `i128` exactly
/// whenever it is in range, so the integer half of the comparison stays exact.
/// Any fractional remainder then breaks the tie: if `x == floor(f)` and `f` has
/// a fraction, `f` is the larger value.
fn cmp_i128_f64(x: i128, f: f64) -> Option<Ordering> {
    // 2^127. `i128::MAX` is one less. So any `f` at or beyond this magnitude is
    // outside the integer range, and its sign alone decides the comparison.
    const I128_LIMIT: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
    if f.is_nan() {
        return None;
    }
    let floor = f.floor();
    if floor >= I128_LIMIT {
        return Some(Ordering::Less);
    }
    if floor < -I128_LIMIT {
        return Some(Ordering::Greater);
    }
    #[allow(clippy::cast_possible_truncation)]
    let whole = floor as i128;
    Some(match x.cmp(&whole) {
        // Equal against the floor: `f` wins the tie iff it has a fraction.
        Ordering::Equal if f > floor => Ordering::Less,
        other => other,
    })
}

fn required_set(obj: &Map<String, Value>) -> BTreeSet<String> {
    obj.get("required")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn diff_properties(
    ctx: &mut DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let empty = Map::new();
    let bp = bo
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let cp = co
        .get("properties")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let breq = required_set(bo);
    let creq = required_set(co);
    if bp.is_empty() && cp.is_empty() && breq == creq {
        return;
    }
    let deny_unknown = co.get("additionalProperties").and_then(Value::as_bool) == Some(false);

    // `required` may name a key that has no entry in `properties`. That is
    // legal JSON Schema, and the whole schema when `properties` is absent.
    // Those names must be in the walk. `{"required":["a"]}` →
    // `{"required":["a","b"]}` demands a key recorded JSON never had. A walk
    // of the property maps alone would never visit `b`.
    let names: BTreeSet<&String> = bp
        .keys()
        .chain(cp.keys())
        .chain(breq.iter())
        .chain(creq.iter())
        .collect();
    for name in names {
        let child = format!("{path}/{}", escape_pointer_token(name));
        match (bp.get(name), cp.get(name)) {
            (None, Some(added)) => {
                diff_property_added(ctx, name, added, bo, creq.contains(name), &child, diff);
            }
            (Some(_), None) => {
                let extra = if deny_unknown {
                    " Because the new type denies unknown fields, this is a HARD deserialization \
                     failure, not a silent drop"
                } else {
                    ""
                };
                diff.push(ctx.delta(
                    &child,
                    ChangeKind::PropertyRemoved,
                    Verdict::Breaking,
                    format!(
                        "property `{name}` removed; the value recorded for it is silently dropped \
                         on replay, so the workflow no longer observes it. If this is a rename, \
                         add `#[serde(alias = \"{name}\")]` to the new field AND acknowledge this \
                         delta — an alias is deserialization-only and never appears in the \
                         published schema, so the gate cannot see it and keeps reporting the \
                         rename.{extra}"
                    ),
                ));
            }
            (Some(b), Some(c)) => {
                match (breq.contains(name), creq.contains(name)) {
                    (false, true) => diff.push(ctx.delta(
                        &child,
                        ChangeKind::PropertyBecameRequired,
                        Verdict::Breaking,
                        format!(
                            "property `{name}` became required; recorded JSON that omitted it \
                             fails to deserialize"
                        ),
                    )),
                    (true, false) => diff.push(ctx.delta(
                        &child,
                        ChangeKind::PropertyBecameOptional,
                        Verdict::Compatible,
                        format!("property `{name}` became optional"),
                    )),
                    _ => {}
                }
                diff_serde_default(
                    ctx,
                    name,
                    b,
                    c,
                    &child,
                    !breq.contains(name) && !creq.contains(name),
                    diff,
                );
                diff_node(ctx, b, c, &child, depth + 1, diff);
            }
            // Named only by `required`, with no `properties` entry on either
            // side.
            (None, None) => diff_bare_required(ctx, name, &child, &breq, &creq, diff),
        }
    }
}

/// Classify a property the baseline did not declare.
///
/// Required is the obvious break. The subtle one is the *optional* case. Serde
/// IGNORES an undeclared key but PARSES a declared one. So a recorded payload
/// that carried this key as an extra now has it type-checked. That is only
/// provable when the baseline declared what an extra had to be. The "limits"
/// section of `docs/workflow-schema-contract-guide.md` says why a fully open
/// object stays compatible.
fn diff_property_added(
    ctx: &DiffCtx<'_>,
    name: &str,
    added: &Value,
    bo: &Map<String, Value>,
    now_required: bool,
    child: &str,
    diff: &mut SchemaContractDiff,
) {
    if now_required {
        diff.push(ctx.delta(
            child,
            ChangeKind::RequiredPropertyAdded,
            Verdict::Breaking,
            format!(
                "required property `{name}` added; JSON recorded before this change has no such \
                 key and fails to deserialize (serde: `missing field \\`{name}\\``). Make it \
                 `Option<T>` or add `#[serde(default)]` to keep in-flight executions replayable"
            ),
        ));
        return;
    }
    if let Some(extras) = bo
        .get("additionalProperties")
        .filter(|v| v.is_object())
        .filter(|extras| {
            !declared_admits_baseline_extras(extras, ctx.base_root, added, ctx.cur_root)
        })
    {
        diff.push(ctx.delta(
            child,
            ChangeKind::RequiredPropertyAdded,
            Verdict::Breaking,
            format!(
                "optional property `{name}` added, but the baseline declared undeclared keys as \
                 `{extras}` and the new type does not accept every value that allowed. Serde \
                 ignores an undeclared key and PARSES a declared one, so a recorded `{name}` that \
                 was previously carried along as an extra now fails to deserialize"
            ),
        ));
        return;
    }
    diff.push(ctx.delta(
        child,
        ChangeKind::OptionalPropertyAdded,
        Verdict::Compatible,
        format!("optional property `{name}` added"),
    ));
}

/// Compare the `default` serde will substitute for an omitted key.
///
/// `default` is a pure annotation to a *validator*: no value of it changes
/// whether a payload validates. That is why it used to be stripped outright.
/// It is not an annotation to *serde*. `#[serde(default = "retries")]` leaves
/// the field optional and records the computed fallback here. (Probed against
/// `schemars` 0.8.22, which emits `"default": 3` for a `retries()` returning
/// `3`.) Change the function to return `5`. Every recorded payload that
/// omitted the key then deserializes to a different value. The JSON is the
/// same, but the meaning differs. This was invisible while both revisions had
/// the key stripped before comparison.
///
/// `live` keeps this from firing on documentation. serde consults the default
/// only for a key that a recorded payload could have omitted. The new type
/// must also still accept the key as absent. So the caller passes "optional on
/// BOTH sides". A `default` beside a required key is dead, because nothing
/// omits it. A key that only just became optional was required when the
/// payloads were written, so none of them omitted it either.
fn diff_serde_default(
    ctx: &DiffCtx<'_>,
    name: &str,
    base: &Value,
    cur: &Value,
    path: &str,
    live: bool,
    diff: &mut SchemaContractDiff,
) {
    if !live {
        return;
    }
    let (b, c) = (
        base.as_object().and_then(|o| o.get("default")),
        cur.as_object().and_then(|o| o.get("default")),
    );
    if b == c {
        return;
    }
    let describe = |v: Option<&Value>| v.map_or_else(|| "absent".to_string(), Value::to_string);
    diff.push(ctx.delta(
        path,
        ChangeKind::DefaultChanged,
        Verdict::Breaking,
        format!(
            "the default substituted for an omitted `{name}` changed from {} to {}; recorded JSON \
             that omitted the key still deserializes, but the workflow now observes a different \
             value than it did when the payload was written. Acknowledge it if no in-flight \
             execution can have omitted the key",
            describe(b),
            describe(c)
        ),
    ));
}

/// Compare a key named only by `required`, with no `properties` entry.
///
/// Legal JSON Schema, and the whole shape when `properties` is absent. There is
/// nothing to recurse into, but the requirement itself still changes what
/// recorded JSON must contain.
fn diff_bare_required(
    ctx: &DiffCtx<'_>,
    name: &str,
    child: &str,
    breq: &BTreeSet<String>,
    creq: &BTreeSet<String>,
    diff: &mut SchemaContractDiff,
) {
    match (breq.contains(name), creq.contains(name)) {
        (false, true) => diff.push(ctx.delta(
            child,
            ChangeKind::RequiredPropertyAdded,
            Verdict::Breaking,
            format!(
                "`{name}` became a required key (it has no `properties` entry, so only its \
                 presence is constrained); recorded JSON that omitted it fails to deserialize"
            ),
        )),
        (true, false) => diff.push(ctx.delta(
            child,
            ChangeKind::PropertyBecameOptional,
            Verdict::Compatible,
            format!("`{name}` is no longer a required key"),
        )),
        _ => {}
    }
}

/// Rank on the `additionalProperties` lattice: `absent/true` (2) accepts more
/// than a schema (1), which accepts more than `false` (0).
///
/// A value that is neither a boolean nor an object is not a schema, and a
/// validator ignores it. So it constrains nothing and ranks with `true`, not
/// with a real schema. A schema rank for it hid a genuine restriction. A fix
/// of `"additionalProperties": "oops"` to `{"type":"string"}` left both
/// revisions at rank 1. No delta was emitted, yet every recorded non-string
/// extra is newly rejected. The recursion below is already guarded on
/// `is_object`, so it cannot descend into the malformed side either.
fn additional_properties_rank(obj: &Map<String, Value>) -> u8 {
    match obj.get("additionalProperties") {
        Some(Value::Bool(false)) => 0,
        Some(Value::Object(_)) => 1,
        None | Some(_) => 2,
    }
}

fn diff_additional_properties(
    ctx: &mut DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let (br, cr) = (
        additional_properties_rank(bo),
        additional_properties_rank(co),
    );
    if cr < br {
        diff.push(ctx.delta(
            path,
            ChangeKind::AdditionalPropertiesRestricted,
            Verdict::Breaking,
            "`additionalProperties` was restricted; recorded payloads carrying keys the new type \
             does not declare are now rejected (this is what `#[serde(deny_unknown_fields)]` \
             emits)"
                .to_string(),
        ));
    } else if cr > br {
        diff.push(ctx.delta(
            path,
            ChangeKind::AdditionalPropertiesRelaxed,
            Verdict::Compatible,
            "`additionalProperties` was relaxed".to_string(),
        ));
    }
    // A `HashMap<String, V>` has no `properties` at all — only
    // `additionalProperties` — so the map's value type is otherwise invisible.
    if let (Some(b), Some(c)) = (
        bo.get("additionalProperties").filter(|v| v.is_object()),
        co.get("additionalProperties").filter(|v| v.is_object()),
    ) {
        diff_node(ctx, b, c, &format!("{path}/{{*}}"), depth + 1, diff);
    }
}

fn diff_items(
    ctx: &mut DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    match (bo.get("items"), co.get("items")) {
        (Some(Value::Object(_)), Some(Value::Object(_))) => {
            let (b, c) = (&bo["items"], &co["items"]);
            diff_node(ctx, b, c, &format!("{path}/[]"), depth + 1, diff);
        }
        // Tuple form: `schemars` emits a Rust tuple struct as an `items` array.
        // The engine's own validator ignores this shape entirely; do not inherit
        // that blind spot.
        (Some(Value::Array(b)), Some(Value::Array(c))) => {
            if b.len() == c.len() {
                for (i, (bi, ci)) in b.iter().zip(c.iter()).enumerate() {
                    diff_node(ctx, bi, ci, &format!("{path}/[{i}]"), depth + 1, diff);
                }
            } else {
                diff.push(ctx.delta(
                    path,
                    ChangeKind::TupleArityChanged,
                    Verdict::Breaking,
                    format!(
                        "tuple length changed from {} to {}; recorded arrays no longer \
                         deserialize",
                        b.len(),
                        c.len()
                    ),
                ));
            }
        }
        (Some(_), Some(_)) => diff.push(
            ctx.delta(
                path,
                ChangeKind::UnanalysedConstraintChanged,
                Verdict::Breaking,
                "`items` changed between the single-schema (array) and tuple (fixed-length) forms"
                    .to_string(),
            ),
        ),
        (Some(_), None) => diff.push(ctx.delta(
            path,
            ChangeKind::BoundRelaxed,
            Verdict::Compatible,
            "the `items` restriction was removed".to_string(),
        )),
        (None, Some(_)) => diff.push(
            ctx.delta(
                path,
                ChangeKind::BoundTightened,
                Verdict::Breaking,
                "an `items` restriction was introduced where array elements were unconstrained"
                    .to_string(),
            ),
        ),
        (None, None) => {}
    }
}

/// What pins a `oneOf`/`anyOf` branch to a distinguishable shape.
///
/// Held structurally, not as a boolean. Disjointness is a property of a *pair*
/// of branches, not of one branch alone. Two branches can each carry a
/// discriminator and still overlap. An internal tag on `tag` and an internal
/// tag on `kind` are each a real discriminator. Yet the recorded payload
/// `{"tag":"A","kind":"B"}` satisfies both. Only [`Discriminator`]s that pin
/// the **same** location to different values prove that a value cannot satisfy
/// both. See [`discriminators_disjoint`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Discriminator {
    /// Internally tagged: required property `name` pinned to a single `value`.
    Tag { name: String, value: Value },
    /// A singleton `enum`, which pins the ENTIRE instance to one value.
    Unit(Value),
    /// serde's externally-tagged `{"Variant": payload}` shape: `name` is the
    /// single required and single declared property, under
    /// `additionalProperties: false`. The closed object is what makes the proof
    /// hold — it is why each branch rejects every sibling's required key.
    Variant(String),
    /// Nothing readable pins this branch.
    None,
}

/// `true` when no single JSON value can satisfy both discriminators.
///
/// Deliberately answers only for **same-kind** pairs. Take a cross-kind pair,
/// such as an internal tag beside an externally-tagged variant. It can overlap
/// whenever the tag name coincides with the variant name. A unit `enum` that
/// holds an object can coincide with either. So the proof is refused, and the
/// caller falls back to the type check or fails closed. Real serde enums are
/// homogeneous in representation. Schemars *does* emit one mixed shape: unit
/// variants beside struct variants in an externally-tagged enum. The `type`
/// check that runs first already separates it (`string` against `object`). So
/// the refusal here costs no derived schema.
fn discriminators_disjoint(a: &Discriminator, b: &Discriminator) -> bool {
    match (a, b) {
        // Same tag location, different pinned value: no single value carries
        // two values at one key. Different locations prove nothing — a payload
        // can carry both keys. Probed against `schemars` 0.8.22: a real
        // `#[serde(tag = "…")]` enum emits the SAME tag name on every branch,
        // so demanding it costs no derived schema.
        (
            Discriminator::Tag {
                name: na,
                value: va,
            },
            Discriminator::Tag {
                name: nb,
                value: vb,
            },
        ) => na == nb && va != vb,
        // A singleton `enum` pins the whole instance, so two different ones
        // cannot share a value.
        (Discriminator::Unit(x), Discriminator::Unit(y)) => x != y,
        // Both branches are closed objects declaring exactly their own variant
        // key, so each rejects the other's required key as undeclared.
        (Discriminator::Variant(x), Discriminator::Variant(y)) => x != y,
        _ => false,
    }
}

/// Identity key for a `oneOf`/`anyOf` branch, plus what pins it.
///
/// Branches must be matched by identity, never by index. `schemars` collapses
/// every unit variant into a single branch that it emits **first**. So the
/// first new unit variant shifts every existing branch, and a positional diff
/// reports a false-positive storm.
///
/// `root` is the document the branch came from. A branch is resolved through
/// `$ref` **before** it is keyed. So a bare `{"$ref": "#/definitions/Inner"}`
/// keys identically to the inlined `{"type":"object", …}` it points at.
/// Without that, `T` -> `Option<T>` in the schemars non-primitive form
/// (`{"anyOf":[{"$ref":…},{"type":"null"}]}`) would read as "the object
/// variant was removed". That is a false breaking verdict on the single most
/// common widening.
fn branch_key(branch: &Value, root: &Value) -> (String, Discriminator) {
    let (branch, _) = resolve_ref(root, branch);
    let Some(obj) = branch.as_object() else {
        return (branch.to_string(), Discriminator::None);
    };
    let req = required_set(obj);
    // Internally tagged: a property pinned to a single `enum`/`const` value.
    //
    // The tag proves disjointness only when it is REQUIRED. With an optional
    // `tag` pinned to `"A"`, the empty object already validates. A sibling
    // branch whose optional `tag` is pinned to `"B"` also accepts it, so the
    // two overlap. The `oneOf` exactly-one rule then rejects a recorded `{}`.
    // Probed against `schemars` 0.8.22: a real `#[serde(tag = "…")]` enum emits
    // the discriminant in `required`, so this costs no legitimate shape.
    if let Some(props) = obj.get("properties").and_then(Value::as_object) {
        for (name, sub) in props {
            // A singleton `enum` is the ONLY spelling that proves anything. The
            // engine's `validate_node` enforces `enum`, and has no `const` arm
            // at all. An unenforced `const` accepts every value, exactly like
            // an unrecognised `type` name. So two branches "pinned" to
            // different `const`s both accept a recorded tag, and `oneOf`
            // rejects it. `const` still keys the branch for IDENTITY, which
            // never claims disjointness. Without it, two differently-tagged
            // branches would key alike and mask a removal.
            let enum_tag = match sub.get("enum").and_then(Value::as_array) {
                Some(vals) if vals.len() == 1 => Some(vals[0].clone()),
                _ => None,
            };
            let Some(t) = enum_tag.clone().or_else(|| sub.get("const").cloned()) else {
                continue;
            };
            let d = match (enum_tag, req.contains(name)) {
                (Some(v), true) => Discriminator::Tag {
                    name: name.clone(),
                    value: v,
                },
                _ => Discriminator::None,
            };
            return (format!("tag:{name}={t}"), d);
        }
    }
    // Unit variant: a singleton `enum`.
    if let Some(vals) = obj.get("enum").and_then(Value::as_array)
        && vals.len() == 1
    {
        return (
            format!("unit:{}", vals[0]),
            Discriminator::Unit(vals[0].clone()),
        );
    }
    // Externally tagged: `{"Variant": payload}`. That is exactly one required
    // property, exactly one declared property, and `additionalProperties:
    // false`. Together they are what `schemars` emits for this representation
    // (probed against 0.8.22).
    //
    // All three are load-bearing for the disjointness proof: each branch
    // REJECTS every required key of a sibling. Without the closed object, an
    // ordinary one-required-field struct qualifies. Two of those are not
    // disjoint at all. `{"id":…,"name":…}` satisfies both `{required:["id"]}`
    // and `{required:["name"]}` as an undeclared extra. The closure now comes
    // from the schema. An earlier argument relied on the serde *serializer*
    // emitting one key. That holds for data serde wrote. But the gate also
    // accepts hand-written schemas (`with_input_schema_fn`) and JSON from
    // `POST /workflows/{name}/start`.
    let prop_count = obj
        .get("properties")
        .and_then(Value::as_object)
        .map_or(0, Map::len);
    let closed = obj.get("additionalProperties").and_then(Value::as_bool) == Some(false);
    // The KEY is the single required property's name alone; only the
    // *discriminator* also demands the object be closed around it.
    //
    // With `prop_count` in the key, the key was unstable under a compatible
    // edit. Take a one-required-field struct behind `Option<T>` that gains an
    // optional field. Its key flips from `variant:a` to `type:object`. The
    // branch then reads as removed-and-re-added. That is a false BREAKING on
    // one of the most common widenings there is.
    if req.len() == 1
        && let Some(name) = req.iter().next()
    {
        let d = if prop_count == 1 && closed {
            Discriminator::Variant(name.clone())
        } else {
            Discriminator::None
        };
        return (format!("variant:{name}"), d);
    }
    // Only reachable for an *unresolvable* `$ref` (external, or a dangling
    // local pointer): `resolve_ref` above already inlined every resolvable one.
    if let Some(r) = obj.get("$ref").and_then(Value::as_str) {
        return (format!("ref:{r}"), Discriminator::None);
    }
    if let Some(ts) = type_set(obj) {
        return (format!("type:{}", join_sorted(&ts)), Discriminator::None);
    }
    (branch.to_string(), Discriminator::None)
}

/// `true` when the two schemas' declared `type` sets cannot share an instance.
///
/// Each side is resolved against **its own** root. So a `$ref` in the baseline
/// node cannot resolve against the `definitions` of the current node, or the
/// reverse. `integer` is widened to also cover `number`, because a recorded
/// integer satisfies both.
///
/// Returns `false` — proves nothing — whenever either side has no readable
/// `type`, which is what keeps every caller failing closed.
fn type_sets_disjoint(a: &Value, a_root: &Value, b: &Value, b_root: &Value) -> bool {
    let widened = |v: &Value, root: &Value| -> Option<BTreeSet<String>> {
        let (r, _) = resolve_ref(root, v);
        let mut set = type_set(r.as_object()?)?;
        if set.contains("integer") {
            set.insert("number".to_string());
        }
        Some(set)
    };
    matches!(
        (widened(a, a_root), widened(b, b_root)),
        (Some(x), Some(y)) if x.is_disjoint(&y)
    )
}

/// `true` when a newly declared property still accepts every value the baseline
/// allowed under that key as an *extra*.
///
/// Overlap is not enough, so this is deliberately not the negation of
/// [`type_sets_disjoint`]. Serde IGNORES an undeclared key but PARSES a
/// declared one. So every recorded value that satisfied the baseline
/// `additionalProperties` is now type-checked against the new property. The
/// question is containment (`extras ⊆ declared`), not intersection. Extras
/// typed `["string","integer"]` and a new `string` property share `string`.
/// Yet a recorded `1` was accepted as an extra before and is rejected now.
///
/// Containment of the extras `type` is not containment of the extras: every
/// OTHER constraint keyword narrows further. A new property typed `string` with
/// `"enum": ["x"]` contains the extras type `string` and still rejects a
/// recorded `"y"`. A proof of the general case is full schema containment. So
/// anything the extras schema does not already demand *identically* fails
/// closed. There are two exceptions. `type` has its own containment rule
/// below. `default` supplies a value for an ABSENT key, and a recorded extra is
/// by definition present, so it cannot reject one.
///
/// Each side resolves against **its own** root so a `$ref` in one cannot
/// accidentally resolve against the other's `definitions`.
fn declared_admits_baseline_extras(
    extras: &Value,
    extras_root: &Value,
    declared: &Value,
    declared_root: &Value,
) -> bool {
    let (extras, _) = resolve_ref(extras_root, extras);
    let (declared, _) = resolve_ref(declared_root, declared);
    let extras_obj = extras.as_object();
    if let Some(d) = declared.as_object()
        && d.iter().any(|(k, v)| {
            k != "type"
                && k != "default"
                && !ANNOTATION_KEYWORDS.contains(&k.as_str())
                && extras_obj.and_then(|e| e.get(k)) != Some(v)
        })
    {
        return false;
    }
    let types = |v: &Value| -> Option<BTreeSet<String>> { type_set(v.as_object()?) };
    match (types(extras), types(declared)) {
        // The new property restricts nothing, so it admits every recorded extra.
        (_, None) => true,
        // The baseline accepted extras of EVERY JSON type (an object with no
        // readable `type`, e.g. `{"minLength": 3}`). A property that restricts
        // the type at all cannot admit all of them.
        (None, Some(_)) => false,
        // Defers to the same containment helper that `diff_types` uses. So an
        // unrecognised name on the declared side fails closed identically:
        // `type_set_admits` cannot show it covers the baseline types.
        (Some(base), Some(cur)) => type_set_admits(&base, &cur),
    }
}

/// The `type` names the engine's `type_matches` actually enforces.
///
/// Anything else falls through that match to `_ => true`: a typo, a
/// hand-written `"bogus"`, a draft-2020 name. So the engine accepts EVERY value
/// for it. Such a set is the universal set and is therefore disjoint from
/// nothing.
const RECOGNISED_JSON_TYPES: &[&str] = &[
    "array", "boolean", "integer", "null", "number", "object", "string",
];

/// [`type_sets_disjoint`], but refusing the proof when either side names a type
/// the engine does not enforce.
///
/// The restriction belongs to the DIRECTION the proof is used in, not to
/// disjointness itself, which is why it lives here rather than inside
/// [`type_sets_disjoint`].
///
/// This wrapper proves disjointness to justify a **compatible** verdict. So an
/// unrecognised name must not qualify. `{"type":"bogus"}` and
/// `{"type":"string"}` are disjoint as *strings*. But the engine accepts a
/// recorded string under both. So it matches two `oneOf` branches and is
/// rejected: a false COMPATIBLE.
///
/// A caller that proves a **breaking** verdict needs the opposite treatment.
/// There, an unreadable name means the baseline accepted every type. A
/// recorded value really can fail the new constraint, so a refused proof would
/// LOSE a true break. Such a caller must use the raw predicate. Where the
/// question is containment, not overlap, it uses
/// [`declared_admits_baseline_extras`]. The unifying rule: an unreadable type
/// name never buys a COMPATIBLE verdict.
fn types_disjoint_for_safety(a: &Value, b: &Value, root: &Value) -> bool {
    let readable = |v: &Value| {
        let (r, _) = resolve_ref(root, v);
        r.as_object().and_then(type_set).is_some_and(|s| {
            s.iter()
                .all(|t| RECOGNISED_JSON_TYPES.contains(&t.as_str()))
        })
    };
    readable(a) && readable(b) && type_sets_disjoint(a, root, b, root)
}

/// `true` when no single JSON value can satisfy both branches.
///
/// Used to decide whether a change to an earlier `anyOf` branch can steal
/// recorded data from a later one. Conservative by design: it must never claim
/// disjointness it cannot demonstrate, so anything it cannot read returns
/// `false` and the caller fails closed.
///
/// Two proofs are accepted:
/// - **Type.** Disjoint `type` sets can share no instance, provided every name
///   in them is one the engine enforces — see [`types_disjoint_for_safety`].
///   `integer` is widened to also cover `number` first, because a recorded
///   integer satisfies both.
/// - **Discriminator.** Two branches pinned at the SAME location to different
///   values, per [`discriminators_disjoint`]. Carrying a discriminator each is
///   not enough: an internal tag on `tag` beside one on `kind` are both real
///   discriminators, and `{"tag":"A","kind":"B"}` satisfies both.
fn branches_provably_disjoint(a: &Value, b: &Value, root: &Value) -> bool {
    if types_disjoint_for_safety(a, b, root) {
        return true;
    }
    let (_, da) = branch_key(a, root);
    let (_, db) = branch_key(b, root);
    discriminators_disjoint(&da, &db)
}

/// A change kind that alters WHICH instances a schema matches.
///
/// Only an added *optional* property is exempt, and only on an OPEN branch. A
/// schema without `additionalProperties: false` already accepted that key as an
/// undeclared extra. So its declaration changes nothing about which payloads
/// validate. Under `additionalProperties: false` the same edit is a genuine
/// widening. The branch rejected that payload before and accepts it now, so
/// `denies_unknown` withdraws the exemption. Everything else, including the
/// "unknown" outcomes, alters the match set and the rebind guard fails closed.
const fn alters_match_set(change: ChangeKind, denies_unknown: bool) -> bool {
    denies_unknown || !matches!(change, ChangeKind::OptionalPropertyAdded)
}

/// Normalise a node into its branch set: an explicit `oneOf`/`anyOf`, or the
/// node itself as a one-element set.
///
/// This is what makes `T` -> `Option<T>` compatible for the non-primitive
/// representation `{"anyOf":[{"$ref":…},{"type":"null"}]}`.
fn branch_set(obj: &Map<String, Value>) -> Option<(&'static str, Vec<Value>)> {
    for key in ["oneOf", "anyOf"] {
        if let Some(arr) = obj.get(key).and_then(Value::as_array) {
            let kw = if key == "oneOf" { "oneOf" } else { "anyOf" };
            return Some((kw, arr.clone()));
        }
    }
    None
}

/// Compare the branch **container keyword** itself, which is a constraint.
///
/// `anyOf` means "at least one branch matches"; `oneOf` means "exactly one".
/// So `anyOf` → `oneOf` **narrows** even when every branch is untouched. With
/// `integer` and `number` branches, a recorded integer matches both. `anyOf`
/// accepts it and `oneOf` rejects it. Only the *current* keyword is carried
/// into the branch comparison. Without this, the branch maps compare equal and
/// the change emits nothing at all.
///
/// Also guards the both-present case. [`branch_set`] selects `oneOf` first, so
/// an `anyOf` sibling on the same node is ignored. `anyOf` sits in
/// [`ANALYSED_KEYWORDS`], so the fail-closed sweep of [`diff_unanalysed`] skips
/// it too. A change to the ignored sibling would otherwise be invisible.
fn diff_branch_container_keyword(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    bb: Option<&(&'static str, Vec<Value>)>,
    cb: Option<&(&'static str, Vec<Value>)>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    // A node carrying BOTH keywords is outside what `branch_set` models.
    let both = |o: &Map<String, Value>| o.contains_key("oneOf") && o.contains_key("anyOf");
    if (both(bo) || both(co))
        && (bo.get("oneOf") != co.get("oneOf") || bo.get("anyOf") != co.get("anyOf"))
    {
        diff.push(
            ctx.delta(
                path,
                ChangeKind::UnanalysedConstraintChanged,
                Verdict::Breaking,
                "this node carries BOTH `oneOf` and `anyOf`; only one is analysed as the branch \
             container, so a change to the other cannot be classified and is reported breaking \
             (fail closed). Acknowledge it if it is safe"
                    .to_string(),
            ),
        );
        return;
    }
    // Past that guard, both keywords carry byte-identical values on both
    // sides. But `branch_set` traverses only ONE of them. So a `$ref` inside
    // the other can still point at a target that was rewritten. Nothing else
    // would catch it. `oneOf`/`anyOf` are ANALYSED keywords, so the fail-closed
    // sweep in `diff_unanalysed` skips them. `definitions`/`$defs` are excluded
    // there as containers "reached through `$ref`". The engine enforces the two
    // keywords as independent blocks. So a narrowed target of the ignored
    // sibling really can reject a value the analysed branch set still accepts.
    if both(bo) || both(co) {
        // Derive the ignored keyword from what `branch_set` actually selected so
        // the two cannot drift; fall back to the sibling of the pair.
        let ignored = if bb.or(cb).map(|(k, _)| *k) == Some("anyOf") {
            "oneOf"
        } else {
            "anyOf"
        };
        if let Some(value) = bo.get(ignored) {
            diff_refs_under_unanalysed(ctx, ignored, value, path, diff);
        }
    }

    let (Some((bk, _)), Some((ck, _))) = (bb, cb) else {
        // One side is a flat node lifted into a one-element branch set; there
        // is no baseline keyword to compare against.
        return;
    };
    if bk == ck {
        return;
    }
    let (change, verdict, why) = if *ck == "oneOf" {
        (
            ChangeKind::BoundTightened,
            Verdict::Breaking,
            "`anyOf` accepts a value that matches two or more branches; `oneOf` requires exactly \
             one, so recorded data matching several branches is now rejected",
        )
    } else {
        (
            ChangeKind::BoundRelaxed,
            Verdict::Compatible,
            "`oneOf` required exactly one match; `anyOf` accepts at least one, so strictly more \
             is accepted",
        )
    };
    diff.push(ctx.delta(
        path,
        change,
        verdict,
        format!("the branch container changed from `{bk}` to `{ck}`: {why}"),
    ));
}

/// Report a reordering of `anyOf` branches.
///
/// `anyOf` is what `schemars` emits for `#[serde(untagged)]`, and serde binds
/// the **first** matching variant in declaration order. Take two overlapping
/// variants reordered: `Int(i64), Float(f64)` → `Float(f64), Int(i64)`. That
/// silently rebinds recorded data to a *different* variant. It still
/// deserializes, but it no longer means the same thing. This gate exists to
/// catch precisely that failure. Branches are matched by key in a `BTreeMap`,
/// which erases order, so the reorder must be detected separately.
///
/// `oneOf` requires exactly one match, so declaration order cannot affect
/// binding there and is not checked.
///
/// Only branches present on **both** sides are compared. An addition
/// legitimately shifts positions (`T` → `Option<T>` appends a `null` branch).
/// The add/remove rules judge it instead.
///
/// Reported breaking with no attempt to prove that the branches overlap. This
/// differ does not model schema disjointness in general. A reorder of the
/// variants of an untagged enum is a deliberate act. Disjoint variants can be
/// acknowledged.
fn diff_anyof_branch_order(
    ctx: &DiffCtx<'_>,
    base_keys: &[String],
    cur_keys: &[String],
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let common: BTreeSet<&String> = base_keys.iter().filter(|k| cur_keys.contains(k)).collect();
    let seq = |keys: &[String]| -> Vec<String> {
        keys.iter()
            .filter(|k| common.contains(k))
            .cloned()
            .collect()
    };
    let (b_seq, c_seq) = (seq(base_keys), seq(cur_keys));
    if b_seq == c_seq {
        return;
    }
    diff.push(ctx.delta(
        path,
        ChangeKind::UnanalysedConstraintChanged,
        Verdict::Breaking,
        format!(
            "`anyOf` branches were reordered ({} -> {}). For `#[serde(untagged)]`, serde binds \
             the FIRST matching variant in declaration order, so recorded data that matched an \
             earlier variant may now bind to a different one — it still deserializes, but it no \
             longer means the same thing. Acknowledge it if the variants are disjoint",
            b_seq.join(", "),
            c_seq.join(", ")
        ),
    ));
}

/// The branches of both sides keyed for matching, plus the current keys in
/// DECLARATION order. The maps erase that order, but the serde untagged
/// binding rule depends on it.
struct KeyedBranches {
    base_by_key: BTreeMap<String, Value>,
    cur_by_key: BTreeMap<String, (Value, Discriminator)>,
    cur_keys: Vec<String>,
}

/// Key both sides' branches, checking `anyOf` declaration order on the way.
///
/// Each side is keyed against *its own* document root, so `$ref` targets are
/// resolved before comparison. A branch does not appear to move merely
/// because one side inlined it and the other referenced it.
///
/// Keys are computed in DECLARATION ORDER first, because the returned maps
/// erase that order. For `anyOf`, order *is* the serde untagged binding rule
/// (see [`diff_anyof_branch_order`]).
fn key_branches(
    ctx: &DiffCtx<'_>,
    base_branches: &[Value],
    cur_branches: &[Value],
    both_anyof: bool,
    path: &str,
    diff: &mut SchemaContractDiff,
) -> KeyedBranches {
    let base_keys: Vec<String> = base_branches
        .iter()
        .map(|b| branch_key(b, ctx.base_root).0)
        .collect();
    let cur_keyed: Vec<(String, Discriminator)> = cur_branches
        .iter()
        .map(|b| branch_key(b, ctx.cur_root))
        .collect();
    let cur_keys: Vec<String> = cur_keyed.iter().map(|(k, _)| k.clone()).collect();

    if both_anyof {
        diff_anyof_branch_order(ctx, &base_keys, &cur_keys, path, diff);
    }

    let base_by_key: BTreeMap<String, Value> = base_keys
        .into_iter()
        .zip(base_branches.iter().cloned())
        .collect();
    let cur_by_key: BTreeMap<String, (Value, Discriminator)> = cur_keyed
        .into_iter()
        .zip(cur_branches.iter().cloned())
        .map(|((k, d), b)| (k, (b, d)))
        .collect();
    KeyedBranches {
        base_by_key,
        cur_by_key,
        cur_keys,
    }
}

fn diff_branches(
    ctx: &mut DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let bb = branch_set(bo);
    let cb = branch_set(co);
    if bb.is_none() && cb.is_none() {
        return;
    }
    diff_branch_container_keyword(ctx, bo, co, bb.as_ref(), cb.as_ref(), path, diff);
    let base_branches = bb.as_ref().map_or_else(
        || vec![Value::Object(strip_branch_keywords(bo))],
        |(_, v)| v.clone(),
    );
    let cur_kw = cb.as_ref().map_or("oneOf", |(k, _)| *k);
    let cur_branches = cb.as_ref().map_or_else(
        || vec![Value::Object(strip_branch_keywords(co))],
        |(_, v)| v.clone(),
    );

    let both_anyof = cur_kw == "anyOf" && bb.as_ref().is_some_and(|(k, _)| *k == "anyOf");
    let KeyedBranches {
        base_by_key,
        cur_by_key,
        cur_keys,
    } = key_branches(ctx, &base_branches, &cur_branches, both_anyof, path, diff);

    // Branches are matched across the two sides BY KEY. Two branches that
    // share a key collapse in the map, and one is silently dropped. That would
    // hide a variant REMOVAL behind a same-keyed survivor and report it
    // compatible.
    if base_by_key.len() != base_branches.len() || cur_by_key.len() != cur_branches.len() {
        diff_ambiguous_branches(
            ctx,
            &base_branches,
            &cur_branches,
            cur_kw == "anyOf",
            path,
            depth,
            diff,
        );
        return;
    }

    // Which siblings a change to `key` has to be checked against.
    //
    // For `anyOf` the binding rule is FIRST match, so only the branches
    // declared AFTER this one can lose data to it. For `oneOf` the rule is
    // EXACTLY one match and order is irrelevant, so overlapping with any
    // sibling in either direction rejects the payload.
    let rivals_of = |key: &str| -> Vec<Value> {
        let Some(i) = cur_keys.iter().position(|k| k == key) else {
            return Vec::new();
        };
        let range: Vec<&String> = if cur_kw == "anyOf" {
            cur_keys[i + 1..].iter().collect()
        } else {
            cur_keys.iter().filter(|k| *k != &cur_keys[i]).collect()
        };
        range
            .into_iter()
            .filter_map(|k| cur_by_key.get(k).map(|(v, _)| v.clone()))
            .collect()
    };

    for (key, b) in &base_by_key {
        match cur_by_key.get(key) {
            Some((c, _)) => diff_branch_guarding_rebind(
                ctx,
                b,
                c,
                &rivals_of(key),
                cur_kw == "anyOf",
                path,
                &format!("{path}/<{}>", key.replace('/', "~1")),
                depth,
                diff,
            ),
            None => diff.push(ctx.delta(
                path,
                ChangeKind::VariantRemoved,
                Verdict::Breaking,
                format!(
                    "variant `{key}` is no longer accepted; recorded data matching it fails to \
                     deserialize"
                ),
            )),
        }
    }
    diff_added_branches(
        &*ctx,
        &base_by_key,
        &cur_by_key,
        &cur_keys,
        cur_kw,
        path,
        diff,
    );
}

/// Classify each branch present in `cur` but not in `base`.
fn diff_added_branches(
    ctx: &DiffCtx<'_>,
    base_by_key: &BTreeMap<String, Value>,
    cur_by_key: &BTreeMap<String, (Value, Discriminator)>,
    cur_keys: &[String],
    cur_kw: &str,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    // For `anyOf` (the serde untagged shape) the binding rule is FIRST match
    // in declaration order. So a branch inserted AHEAD of a pre-existing one
    // can capture recorded data that used to bind to the later branch. A
    // branch added after every pre-existing branch cannot rebind anything.
    // That is the `T` -> `Option<T>` shape, which appends a `null` branch.
    let last_existing = cur_keys
        .iter()
        .rposition(|k| base_by_key.contains_key(k))
        .unwrap_or(0);
    // Disjointness is a property of the WHOLE branch set, not of the added
    // branch. See the comment on the compatible arm below. It is also a
    // property of each PAIR: two branches can each carry a discriminator and
    // still overlap. Take an internal tag on `tag` beside one on `kind`:
    // `{"tag":"A","kind":"B"}` matches both. So every pair is checked, and no
    // branch is reduced to a boolean.
    let branches: Vec<&Value> = cur_by_key.values().map(|(v, _)| v).collect();
    let pairwise_disjoint = branches.iter().enumerate().all(|(i, x)| {
        branches[i + 1..]
            .iter()
            .all(|y| branches_provably_disjoint(x, y, ctx.cur_root))
    });
    for (key, (branch, _)) in cur_by_key {
        if base_by_key.contains_key(key) {
            continue;
        }
        let inserted_ahead = cur_kw == "anyOf"
            && cur_keys
                .iter()
                .position(|k| k == key)
                .is_some_and(|i| i < last_existing);
        if inserted_ahead {
            diff.push(ctx.delta(
                path,
                ChangeKind::VariantAdded,
                Verdict::Breaking,
                format!(
                    "variant `{key}` inserted AHEAD of a pre-existing branch of this `anyOf`; for \
                     `#[serde(untagged)]` serde binds the FIRST matching variant in declaration \
                     order, so recorded data that used to bind to a later branch can be silently \
                     rebound to this one (e.g. prepending `f64` before `i64` captures every \
                     recorded integer). Append it after every existing branch instead"
                ),
            ));
            continue;
        }
        // `anyOf` means "at least one", so an appended branch can never reject a
        // previously-valid instance. `oneOf` means "exactly one", so an added
        // branch that OVERLAPS an existing one turns a single match into two and
        // rejects it.
        //
        // A tag on the ADDED branch alone proves nothing: it says the new branch
        // is narrow, not that the branches it now sits beside are. Adding the
        // singleton `{"enum":["x"]}` next to a broad `{"type":"string"}` makes
        // the recorded value `"x"` match two branches. Disjointness holds only
        // when EVERY PAIR of branches is provably disjoint. The serde tagged
        // shapes are like that: a recorded payload matches exactly one branch
        // however many variants are added. `oneOf` binding does not depend on
        // order.
        if cur_kw == "anyOf" || pairwise_disjoint {
            diff.push(ctx.delta(
                path,
                ChangeKind::VariantAdded,
                Verdict::Compatible,
                format!("variant `{key}` added"),
            ));
        } else {
            let added_overlaps = branches.iter().any(|o| {
                !std::ptr::eq(*o, branch) && !branches_provably_disjoint(branch, o, ctx.cur_root)
            });
            let why = if added_overlaps {
                "the added branch is not provably disjoint from an existing branch — either it \
                 carries no variant tag, or its tag pins a DIFFERENT location than theirs (a \
                 payload can carry both keys at once)"
            } else {
                "two EXISTING branches of this `oneOf` are not provably disjoint from each other, \
                 so adding a branch cannot be shown to keep every recorded payload matching \
                 exactly one"
            };
            diff.push(ctx.delta(
                path,
                ChangeKind::VariantAdded,
                Verdict::Breaking,
                format!(
                    "variant `{key}` added to a `oneOf` and {why}; `oneOf` requires EXACTLY one \
                     match, so an overlapping branch rejects previously-valid recorded data"
                ),
            ));
        }
    }
}

/// Recurse into one branch, guarding `anyOf`'s first-match binding.
///
/// `anyOf` is what `schemars` emits for `#[serde(untagged)]`, and serde binds
/// the FIRST matching variant in declaration order. So a change to a branch
/// that is not last can silently move recorded data onto it, or off it. That
/// holds even when the change is otherwise compatible for the read contract.
/// Say branch 0 makes `a` optional, and branch 1 requires `b`. A recorded
/// `{"b": …}` that used to bind to variant B now matches branch 0 and binds
/// to variant A. It still deserializes, but it means something else.
///
/// `oneOf` has the mirror-image hazard. Order is irrelevant there, but the
/// rule is EXACTLY one match. **Widen** a branch until it overlaps a sibling.
/// A payload that used to match one branch then matches two, and is rejected.
/// Baseline branches `{"maximum":0}` and `{"minimum":1}` accept a recorded `1`
/// exactly once; relaxing the first to `{"maximum":2}` is a compatible-looking
/// bound relaxation that makes `1` invalid. Narrowing
/// needs no guard: it cannot create an overlap, and the payloads it drops are
/// already reported by the narrowing itself.
///
/// `rivals` is whichever set applies — the branches declared after this one for
/// `anyOf`, every other branch for `oneOf`. When this branch is provably
/// disjoint from all of them, no instance can move either way and the change is
/// judged on its own merits.
#[allow(clippy::too_many_arguments)]
fn diff_branch_guarding_rebind(
    ctx: &mut DiffCtx<'_>,
    base: &Value,
    cur: &Value,
    rivals: &[Value],
    is_anyof: bool,
    path: &str,
    sub_path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    let before_total = diff.breaking_count + diff.compatible_count;
    let before_stored = diff.deltas.len();
    let before_suppressed = ctx.suppressed_changes;
    let before_active = ctx.active_hits.len();
    diff_node(ctx, base, cur, sub_path, depth + 1, diff);

    // Taken unconditionally, so the log never accumulates entries from a branch
    // that had no rivals to check against.
    let back_edges: Vec<_> = ctx.active_hits.drain(before_active..).collect();
    if rivals.is_empty() {
        return;
    }
    let emitted = (diff.breaking_count + diff.compatible_count) - before_total;
    // A branch that is a `$ref` to a definition ANOTHER path already visited
    // emits nothing. The memo that keeps this diff linear returns on the
    // second visit. A read of that as "unchanged" let a compatible relaxation
    // of a shared definition slip an overlap past this guard. The relaxation is
    // reported once, at the ordinary property that reached the definition
    // first. The branch context never learns about it.
    let swallowed = ctx.suppressed_changes > before_suppressed;
    if emitted == 0 && !swallowed {
        // Nothing OBSERVED, but a back-edge means the observation is not
        // finished. This branch is a `$ref` to a pair whose traversal encloses
        // this very call. So "did the branch change" is exactly "does that pair
        // change". Only the frame that opened it can answer. Park the check;
        // do not resolve it either way. A guess of "unchanged" hides a real
        // rebind. A guess of "changed" reports every unchanged recursive type
        // as breaking, which would block its own baseline forever.
        if !back_edges.is_empty() {
            ctx.pending_branch_guards.push(PendingBranchGuard {
                on_pairs: back_edges,
                cur: cur.clone(),
                rivals: rivals.to_vec(),
                is_anyof,
                path: path.to_string(),
                sub_path: sub_path.to_string(),
            });
        }
        return;
    }
    let cur_root = ctx.cur_root;
    // Whether the branch AS IT NOW STANDS rejects undeclared keys. This decides
    // if adding an optional property widened it (see `alters_match_set`).
    let denies_unknown = {
        let (resolved, _) = resolve_ref(cur_root, cur);
        resolved
            .as_object()
            .and_then(|o| o.get("additionalProperties"))
            .and_then(Value::as_bool)
            == Some(false)
    };
    // A delta dropped by the storage cap cannot be inspected, so treat the
    // branch as changed rather than assuming the unseen deltas were benign.
    // A swallowed change has no deltas here either, so it is treated the same
    // way. Assume it might have widened, and let the disjointness check below
    // decide. That keeps the fail-closed direction. A shared definition whose
    // branch rivals are provably disjoint still reports nothing.
    let stored = &diff.deltas[before_stored.min(diff.deltas.len())..];
    let all_stored = !swallowed && stored.len() == emitted;
    if all_stored
        && !stored
            .iter()
            .any(|d| alters_match_set(d.change, denies_unknown))
    {
        return;
    }
    // Under `oneOf` only a WIDENING can create an overlap, and a branch that
    // already reported a breaking narrowing needs no second verdict. A delta
    // dropped by the storage cap is unreadable, so `all_stored` decides
    // conservatively that the change might have widened.
    if !is_anyof && all_stored && stored.iter().any(|d| d.verdict == Verdict::Breaking) {
        return;
    }
    emit_rebind_if_overlapping(ctx, cur, rivals, is_anyof, path, sub_path, diff);
}

/// The tail of [`diff_branch_guarding_rebind`]: the branch is known to have
/// changed, so report it unless every rival is provably disjoint from it.
///
/// Split out because two callers reach this point with no local deltas to
/// inspect. One has a change that the memo swallowed. The other was parked on
/// a pair that had not settled yet. So the delta-shape filters above do not
/// apply to them. They are exactly the fail-closed cases that the disjointness
/// check exists to adjudicate.
fn emit_rebind_if_overlapping(
    ctx: &DiffCtx<'_>,
    cur: &Value,
    rivals: &[Value],
    is_anyof: bool,
    path: &str,
    sub_path: &str,
    diff: &mut SchemaContractDiff,
) {
    let cur_root = ctx.cur_root;
    if rivals
        .iter()
        .all(|l| branches_provably_disjoint(cur, l, cur_root))
    {
        return;
    }
    let why = if is_anyof {
        "of this `anyOf` changed what it matches, and it is not provably disjoint from a branch \
         declared after it. For `#[serde(untagged)]` serde binds the FIRST matching variant, so \
         recorded data can be silently REBOUND to a different variant — it still deserializes, \
         but it no longer means the same thing"
    } else {
        "of this `oneOf` widened what it matches, and it is not provably disjoint from a sibling \
         branch. `oneOf` requires EXACTLY one match, so a recorded payload that used to match \
         this branch alone can now match two and be rejected"
    };
    diff.push(ctx.delta(
        path,
        ChangeKind::UnanalysedConstraintChanged,
        Verdict::Breaking,
        format!(
            "branch `{sub_path}` {why}. Acknowledge it if the branches are disjoint on grounds \
             the differ cannot see"
        ),
    ));
}

/// Resolve every parked overlap check that was waiting on `key`, now that the
/// pair has settled as CHANGED.
///
/// A record that names several pairs fires on the first of them to change and
/// is removed. So a branch that back-edged into two active pairs is reported
/// once. Records whose pairs all settle as unchanged are never drained. The
/// branch genuinely did not change. That is why the decision was parked
/// instead of guessed.
fn run_pending_branch_guards(
    ctx: &mut DiffCtx<'_>,
    key: &(String, String),
    diff: &mut SchemaContractDiff,
) {
    if ctx.pending_branch_guards.is_empty() {
        return;
    }
    // Taken out of `ctx` first so the checks below can borrow it immutably.
    // They only read `cur_root` and never recurse, so nothing can register a
    // new record while this runs.
    let all = std::mem::take(&mut ctx.pending_branch_guards);
    let (due, keep): (Vec<_>, Vec<_>) = all.into_iter().partition(|p| p.on_pairs.contains(key));
    ctx.pending_branch_guards = keep;
    for p in due {
        emit_rebind_if_overlapping(
            ctx,
            &p.cur,
            &p.rivals,
            p.is_anyof,
            &p.path,
            &p.sub_path,
            diff,
        );
    }
}

/// Compare a branch set the differ cannot key, by POSITION.
///
/// Two branches that share a key (two multi-field object variants of a
/// `#[serde(untagged)]` enum both key as `type:object`) cannot be matched
/// across revisions by name. An unconditional fail-closed would report an
/// *unchanged* set breaking. The collision fires on both sides, so the
/// baseline of that workflow could never pass the gate again. That blocks
/// unrelated work for good. Instead, when the two sides have the same NUMBER
/// of branches, they are compared pairwise by position. An unchanged set yields
/// zero deltas. The recursion still catches a nested change, including a
/// `$ref` target change one level down.
///
/// A LENGTH change is genuinely unclassifiable: the differ cannot say which
/// branch was dropped. So that still fails closed.
///
/// The accepted cost is that REORDERING ambiguous branches produces positional
/// deltas that do not correspond to a real edit. They are acknowledgeable, and
/// for `anyOf` a reorder is a genuine rebind risk anyway.
fn diff_ambiguous_branches(
    ctx: &mut DiffCtx<'_>,
    base_branches: &[Value],
    cur_branches: &[Value],
    is_anyof: bool,
    path: &str,
    depth: usize,
    diff: &mut SchemaContractDiff,
) {
    if base_branches.len() != cur_branches.len() {
        diff.push(ctx.delta(
            path,
            ChangeKind::UnanalysedConstraintChanged,
            Verdict::Breaking,
            format!(
                "the number of branches in this `oneOf`/`anyOf` changed ({} -> {}) and the \
                 branches are indistinguishable to the differ (no variant tag, unit `enum`, or \
                 single-property external tag), so it cannot say which branch was added or \
                 removed; reported breaking (fail closed). Acknowledge it if it is safe",
                base_branches.len(),
                cur_branches.len()
            ),
        ));
        return;
    }
    for (i, (b, c)) in base_branches.iter().zip(cur_branches).enumerate() {
        // Same rule as `rivals_of` in `diff_branch_set`, by index instead of by
        // key. `anyOf` binds the FIRST match, so only the branches declared
        // after this one can lose data to it. `oneOf` requires EXACTLY one
        // match and is order-independent. So a LAST branch widened until it
        // overlaps an EARLIER one is just as fatal. A later-only rule would
        // give it no rivals at all and score the widening on its own merits.
        let rivals: Vec<Value> = if is_anyof {
            cur_branches[i + 1..].to_vec()
        } else {
            cur_branches
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, v)| v.clone())
                .collect()
        };
        diff_branch_guarding_rebind(
            ctx,
            b,
            c,
            &rivals,
            is_anyof,
            path,
            &format!("{path}/[{i}]"),
            depth,
            diff,
        );
    }
}

/// A node minus its branch keywords, so a bare schema can be treated as a
/// one-element branch set.
fn strip_branch_keywords(obj: &Map<String, Value>) -> Map<String, Value> {
    obj.iter()
        .filter(|(k, _)| k.as_str() != "oneOf" && k.as_str() != "anyOf")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Report a change to a `$ref` the differ could not resolve.
///
/// [`resolve_ref`] follows `#/…` pointers until it runs out; anything left
/// carrying a `$ref` at comparison time is external, dangling, or cyclic. Such a
/// node is opaque. The differ cannot read the target to say whether the new one
/// accepts every payload the old one did. So any change to it fails closed.
///
/// **Documented limitation:** when both sides carry the *same* unresolvable
/// `$ref`, a change made *behind* that reference (in the external document) is
/// outside this gate's reach. Keep published schemas self-contained.
fn diff_unresolved_ref(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let b = bo.get("$ref").and_then(Value::as_str);
    let c = co.get("$ref").and_then(Value::as_str);
    // Equal (including both-absent) is genuinely no change at this node.
    if b == c {
        return;
    }
    let show = |v: Option<&str>| v.map_or_else(|| "absent".to_string(), |s| format!("`{s}`"));
    diff.push(ctx.delta(
        path,
        ChangeKind::UnanalysedConstraintChanged,
        Verdict::Breaking,
        format!(
            "an unresolvable `$ref` (external, dangling, or cyclic) changed from {} to {}; the \
             differ cannot read the target to classify the change, so it is reported breaking \
             (fail closed). Acknowledge it if it is safe",
            show(b),
            show(c)
        ),
    ));
}

fn diff_unanalysed(
    ctx: &DiffCtx<'_>,
    bo: &Map<String, Value>,
    co: &Map<String, Value>,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let is_tracked = |k: &str| {
        ANALYSED_KEYWORDS.contains(&k)
            || ANNOTATION_KEYWORDS.contains(&k)
            || CONTAINER_KEYWORDS.contains(&k)
    };
    let keys: BTreeSet<&String> = bo
        .keys()
        .chain(co.keys())
        .filter(|k| !is_tracked(k.as_str()))
        .collect();
    for key in keys {
        if bo.get(key) == co.get(key) {
            // Byte-identical is not the same as unchanged: the value may point
            // INTO the schema with `$ref`. `definitions`/`$defs` are filtered
            // out above as containers "reached through `$ref`". But only the
            // ANALYSED traversal follows refs. Nothing analysed points at a
            // definition that only an unanalysed keyword references. A rewrite
            // of that definition would then be invisible, even though the
            // same change inline is breaking.
            if let Some(v) = bo.get(key) {
                diff_refs_under_unanalysed(ctx, key, v, path, diff);
            }
            continue;
        }
        // Fail closed in BOTH directions. "Removing a constraint always loosens"
        // is false in JSON Schema — keywords interact. Dropping a
        // `patternProperties` entry while `additionalProperties: false` remains
        // TIGHTENS the schema.
        diff.push(ctx.delta(
            path,
            ChangeKind::UnanalysedConstraintChanged,
            Verdict::Breaking,
            format!(
                "the `{key}` keyword changed. It is outside the subset this gate analyses, so the \
                 change cannot be classified and is reported breaking (fail closed). Acknowledge \
                 it if it is safe"
            ),
        ));
    }
}

/// Collect every LOCAL `$ref` target reachable from `value`, transitively.
///
/// Returns RFC 6901 pointers (the `#`-stripped tail), so each can be fed
/// straight to [`Value::pointer`]. `out` doubles as the cycle guard, so a
/// self-referential definition terminates. External (`https://…`) refs are
/// skipped: the differ cannot read their targets either way, and
/// [`diff_ref_pair`] already fails closed when such a ref *changes*.
fn collect_local_refs(root: &Value, value: &Value, out: &mut BTreeSet<String>, depth: usize) {
    if depth >= MAX_DIFF_DEPTH {
        return;
    }
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                if k != "$ref" {
                    collect_local_refs(root, v, out, depth + 1);
                    continue;
                }
                let Some(pointer) = v.as_str().and_then(|r| r.strip_prefix('#')) else {
                    continue;
                };
                if !out.insert(pointer.to_string()) {
                    continue; // already expanded (or a cycle)
                }
                if let Some(target) = root.pointer(pointer) {
                    collect_local_refs(root, target, out, depth + 1);
                }
            }
        }
        Value::Array(items) => {
            for v in items {
                collect_local_refs(root, v, out, depth + 1);
            }
        }
        _ => {}
    }
}

/// Fail closed when a sub-schema referenced by an *unchanged* unanalysed
/// constraint has itself changed.
///
/// Both roots are canonicalised before the diff, so annotation churn inside the
/// target stays invisible — the same promise the analysed traversal makes. A
/// target that resolves on one side only compares as different, which is the
/// intended fail-closed outcome.
fn diff_refs_under_unanalysed(
    ctx: &DiffCtx<'_>,
    key: &str,
    value: &Value,
    path: &str,
    diff: &mut SchemaContractDiff,
) {
    let mut pointers = BTreeSet::new();
    collect_local_refs(ctx.base_root, value, &mut pointers, 0);
    // Sharing `pointers` across both roots is safe: a target reachable on only
    // one side must itself differ, and that difference is reported below.
    collect_local_refs(ctx.cur_root, value, &mut pointers, 0);

    for pointer in &pointers {
        if ctx.base_root.pointer(pointer) == ctx.cur_root.pointer(pointer) {
            continue;
        }
        diff.push(ctx.delta(
            path,
            ChangeKind::UnanalysedConstraintChanged,
            Verdict::Breaking,
            format!(
                "the `{key}` keyword is unchanged, but the `#{pointer}` sub-schema it references \
                 was rewritten. `{key}` is outside the subset this gate analyses, so the effect \
                 cannot be classified and is reported breaking (fail closed). Acknowledge it if \
                 it is safe"
            ),
        ));
        // One delta per keyword is enough signal; the rest would be noise.
        break;
    }
}

/// Escape a property name for use in an RFC 6901 JSON Pointer.
fn escape_pointer_token(name: &str) -> String {
    name.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_schema_is_borrowed_and_dirty_schema_is_rewritten() {
        let canonical = json!({
            "type": "object",
            "properties": {"description": {"type": "string"}},
            "enum": ["a", "b"],
            "oneOf": [{"type": "string", "enum": ["a", "b"]}, {"type": "integer"}]
        });
        assert!(canonicalize_node(&canonical, 0).is_none());
        assert_eq!(canonicalize_schema(&canonical), canonical);

        let dirty = json!({
            "description": "doc",
            "properties": {"a": {"title": "t", "type": "string"}},
            "enum": ["b", "a"]
        });
        let want = json!({"properties": {"a": {"type": "string"}}, "enum": ["a", "b"]});
        assert_eq!(canonicalize_node(&dirty, 0), Some(want.clone()));
        assert_eq!(canonicalize_schema(&dirty), want);
    }

    #[test]
    fn numeric_format_ranges_encode_width_and_signedness() {
        assert!(
            numeric_format_range("int64").unwrap().1 > numeric_format_range("int32").unwrap().1
        );
        assert_eq!(numeric_format_range("uint32").unwrap().0, 0);
        assert!(numeric_format_range("uuid").is_none());
        assert!(!is_numeric_format("date-time"));
        assert!(is_numeric_format("int8"));
    }

    #[test]
    fn int64_is_not_admitted_by_double() {
        // Values above 2^53 lose precision, so `i64 -> f64` is a real narrowing.
        let b = numeric_format_range("int64").unwrap();
        let c = numeric_format_range("double").unwrap();
        assert!(!(c.0 <= b.0 && c.1 >= b.1));
        // ...but `i32 -> f64` is exact.
        let small = numeric_format_range("int32").unwrap();
        assert!(c.0 <= small.0 && c.1 >= small.1);
    }

    #[test]
    fn type_set_admits_integer_as_number() {
        let int: BTreeSet<String> = std::iter::once("integer".to_string()).collect();
        let num: BTreeSet<String> = std::iter::once("number".to_string()).collect();
        assert!(type_set_admits(&int, &num));
        assert!(!type_set_admits(&num, &int));
    }

    #[test]
    fn branch_key_is_stable_and_reads_the_discriminator() {
        let root = json!({});
        let (k, d) = branch_key(&json!({"type":"string","enum":["C"]}), &root);
        assert_eq!(k, "unit:\"C\"");
        assert_eq!(d, Discriminator::Unit(json!("C")));

        // Serde's externally-tagged shape: the closed object is what makes each
        // branch reject its siblings' key.
        let (k, d) = branch_key(
            &json!({"type":"object","required":["A"],"properties":{"A":{}},
                    "additionalProperties":false}),
            &root,
        );
        assert_eq!(k, "variant:A");
        assert_eq!(d, Discriminator::Variant("A".to_string()));

        // The same shape left OPEN keys identically, because the key must stay
        // stable under a compatible edit. But it proves nothing: `{"A":…,"B":…}`
        // also satisfies a sibling requiring `B`.
        let (k, d) = branch_key(
            &json!({"type":"object","required":["A"],"properties":{"A":{}}}),
            &root,
        );
        assert_eq!(k, "variant:A");
        assert_eq!(d, Discriminator::None);

        let (k, d) = branch_key(&json!({"type":"integer"}), &root);
        assert_eq!(k, "type:integer");
        assert_eq!(
            d,
            Discriminator::None,
            "an untagged scalar branch is not provably disjoint"
        );
    }

    #[test]
    fn a_tag_proves_disjointness_only_at_the_same_key() {
        let tag = |name: &str, v: &str| Discriminator::Tag {
            name: name.to_string(),
            value: json!(v),
        };
        assert!(
            discriminators_disjoint(&tag("kind", "A"), &tag("kind", "B")),
            "same key, different values: no payload carries both"
        );
        assert!(
            !discriminators_disjoint(&tag("tag", "A"), &tag("kind", "B")),
            "different keys prove nothing — `{{\"tag\":\"A\",\"kind\":\"B\"}}` satisfies both"
        );
        assert!(!discriminators_disjoint(
            &tag("kind", "A"),
            &tag("kind", "A")
        ));
        // Cross-kind pairs are refused: a tag key can coincide with a variant
        // name. The `type` check runs first and covers the mixed shape
        // `schemars` really emits (a unit variant beside a struct variant).
        assert!(!discriminators_disjoint(
            &tag("A", "x"),
            &Discriminator::Variant("B".to_string())
        ));
        assert!(!discriminators_disjoint(
            &Discriminator::None,
            &Discriminator::Variant("B".to_string())
        ));
    }

    #[test]
    fn branch_key_resolves_a_ref_so_inlined_and_referenced_branches_match() {
        // The `Option<Inner>` widening depends on this. schemars emits
        // `{"anyOf":[{"$ref":"#/definitions/Inner"},{"type":"null"}]}`. So the
        // referenced branch must key identically to the inlined baseline node,
        // or the object variant reads as removed.
        let root = json!({"definitions":{"Inner":{"type":"object","properties":{"z":{}}}}});
        let via_ref = branch_key(&json!({"$ref":"#/definitions/Inner"}), &root).0;
        let inlined = branch_key(&json!({"type":"object","properties":{"z":{}}}), &root).0;
        assert_eq!(via_ref, inlined, "a $ref branch must key as its target");
        // An unresolvable ref falls back to the literal, never a false match.
        let dangling = branch_key(&json!({"$ref":"#/definitions/Missing"}), &root).0;
        assert_eq!(dangling, "ref:#/definitions/Missing");
    }

    #[test]
    fn additional_properties_lattice_is_ordered() {
        let absent = Map::new();
        let schema: Map<String, Value> =
            serde_json::from_value(json!({"additionalProperties":{"type":"string"}})).unwrap();
        let denied: Map<String, Value> =
            serde_json::from_value(json!({"additionalProperties":false})).unwrap();
        assert!(
            additional_properties_rank(&absent) > additional_properties_rank(&schema)
                && additional_properties_rank(&schema) > additional_properties_rank(&denied)
        );
    }

    #[test]
    fn pointer_tokens_are_rfc6901_escaped() {
        assert_eq!(escape_pointer_token("a/b~c"), "a~1b~0c");
    }
}
