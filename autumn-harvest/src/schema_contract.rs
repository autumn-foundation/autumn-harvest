//! Payload-schema compatibility contract and differ (issue #794).
//!
//! A workflow engine's hardest operational footgun is invisible: change the
//! serde shape of a workflow payload type, ship it, and **in-flight executions
//! DLQ on the next replay** because recorded JSON in `harvest_events` no longer
//! deserializes into the new struct. `cargo build` stays green.
//!
//! Issue #373 already publishes each workflow's `input_schema` /
//! `output_schema` / `error_schema`. This module turns those published schemas
//! into a checked-in, machine-readable **baseline** and diffs a later revision
//! against it, classifying every delta as `compatible` or `breaking` from the
//! **replay-read perspective**: *old recorded JSON must still deserialize into
//! the new type*.
//!
//! # Scope of the analysis
//!
//! The differ reasons about exactly the JSON Schema keyword subset the engine's
//! own [`validate_against_schema`](crate::info::validate_against_schema)
//! enforces — `type`, `required`, `properties`, `enum`, `items`,
//! `additionalProperties`, `allOf`/`anyOf`/`oneOf`, `minLength`/`maxLength`,
//! `minimum`/`maximum`, and local `$ref` — plus numeric `format`, which is the
//! **only** signal distinguishing `i64` from `i32` in a `schemars`-derived
//! schema. Every other keyword is either a pure annotation (stripped) or an
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

use crate::info::WorkflowInfo;

/// Schema-contract format version. Bump on a breaking change to the *artifact*
/// shape (not to a workflow's payload schema).
pub const SCHEMA_CONTRACT_VERSION: &str = "1";

/// Default checked-in baseline path, relative to the repository root.
pub const DEFAULT_SCHEMA_CONTRACT_PATH: &str = "docs/workflow-schema-contract.json";

/// Maximum recursion depth for the differ.
///
/// Mirrors [`crate::info::MAX_VALIDATION_DEPTH`] so the differ and the engine's
/// validator agree on how deep a payload schema may be. A recursive Rust type
/// (`struct Node { children: Vec<Node> }`) emits a genuine `$ref` cycle, so a
/// bound is mandatory: a hang in CI gets the gate deleted the same day.
const MAX_DIFF_DEPTH: usize = 128;

/// Hard cap on **stored** deltas, so a pathological pair of schemas cannot
/// produce an unbounded report.
///
/// This caps the report, never the verdict: [`SchemaContractDiff::breaking_count`]
/// keeps counting past it, and [`SchemaContractDiff::truncated`] marks the
/// report incomplete so callers can fail closed.
pub const MAX_DELTAS: usize = 10_000;

/// Which of a workflow's three published schemas a delta belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaRole {
    /// The workflow's input payload — read from `WorkflowStarted` on replay.
    Input,
    /// The workflow's output payload — read from `WorkflowCompleted`.
    Output,
    /// The workflow's error payload — read from `WorkflowFailed`.
    Error,
}

impl SchemaRole {
    /// Stable lowercase slug used in the artifact and in human output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
            Self::Error => "error",
        }
    }

    /// All three roles, in a stable order.
    #[must_use]
    pub const fn all() -> [Self; 3] {
        [Self::Input, Self::Output, Self::Error]
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
    /// …) **and** the introduction of a constraint keyword where the value was
    /// previously unconstrained — an `enum` or an `items` schema appearing for
    /// the first time. All three narrow the accepted set the same way, so they
    /// share one slug; the delta's `reason` names which one it was.
    BoundTightened,
    /// A constraint was relaxed or removed — the mirror of [`Self::BoundTightened`].
    BoundRelaxed,
    /// A numeric `format` no longer admits every value the baseline admitted.
    NumericFormatNarrowed,
    /// A numeric `format` widened, or was removed.
    NumericFormatWidened,
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
            Self::TupleArityChanged => "tuple_arity_changed",
            Self::AllOfMembersChanged => "all_of_members_changed",
            Self::UnanalysedConstraintChanged => "unanalysed_constraint_changed",
            Self::DiffDepthCapReached => "diff_depth_cap_reached",
            Self::AcknowledgementMissingReason => "acknowledgement_missing_reason",
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
    /// Registered workflow name.
    pub workflow: String,
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
        // Count FIRST, then cap only the storage. Counting inside the cap would
        // make `breaking_count` a tally of the deltas that happened to fit: a
        // diff whose first `MAX_DELTAS` entries are all compatible would report
        // `breaking_count == 0` and exit `0`, silently passing every breaking
        // change past the cap. The tally is the gate; the vector is only the
        // report.
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
/// a live suppression list: once the baseline is regenerated the delta no longer
/// appears, and this entry is the permanent, reviewable record of why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcknowledgedBreakingChange {
    /// Registered workflow name the absorbed delta belonged to.
    pub workflow: String,
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
                "Removing an enum value or a oneOf/anyOf variant",
                "Restricting additionalProperties (absent > schema > false)",
                "Introducing or tightening a bound (minimum/maximum/minLength/maxLength)",
                "Changing tuple arity",
                "Withdrawing a previously published schema (coverage regression)",
                "Any change to a constraint keyword outside the analysed set (fail closed)",
            ]
            .into_iter()
            .map(ToString::to_string)
            .collect(),
            non_breaking_changes: [
                "Adding an optional property (Option<T> or #[serde(default)])",
                "Removing a required marker (making a property optional)",
                "Widening a type (e.g. integer to number, T to Option<T>)",
                "Widening a numeric format (e.g. int32 to int64)",
                "Adding an enum value, or a disjoint oneOf/anyOf variant",
                "Relaxing additionalProperties or a bound",
                "Adding a workflow type, or publishing a schema for the first time",
                "Removing a workflow type (gated more accurately by \
                 `harvest workflow-types reachability`, issue #520)",
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
                 analysed set is reported breaking. Keywords interact — removing one does not \
                 reliably loosen a schema — so the differ fails closed and lets the author \
                 acknowledge."
                    .to_string(),
        }
    }
}

/// The checked-in baseline artifact.
///
/// Mirrors the `docs/api-contract.json` precedent: versioned, machine-readable,
/// regenerable, with the compatibility ruleset embedded.
///
/// Stored schemas are **canonicalised** (annotations stripped) so that editing a
/// doc comment — which `schemars` copies into `description` — produces a
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
        let workflows: Vec<WorkflowSchemaEntry> = by_name.into_values().collect();
        let coverage = SchemaCoverage {
            workflows_total: workflows.len(),
            with_input_schema: workflows
                .iter()
                .filter(|w| w.input_schema.is_some())
                .count(),
            with_output_schema: workflows
                .iter()
                .filter(|w| w.output_schema.is_some())
                .count(),
            with_error_schema: workflows
                .iter()
                .filter(|w| w.error_schema.is_some())
                .count(),
        };
        Self {
            version: version.to_string(),
            contract_version: SCHEMA_CONTRACT_VERSION.to_string(),
            description: DEFAULT_CONTRACT_DESCRIPTION.to_string(),
            compatibility: CompatibilityRules::current(),
            coverage,
            acknowledged_breaking_changes: Vec::new(),
            workflows,
        }
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
        // may change what a verdict MEANS, and silently diffing it under v1
        // rules would hand back confident answers computed from the wrong
        // ruleset — the one failure mode a compatibility gate must never have.
        if parsed.contract_version != SCHEMA_CONTRACT_VERSION {
            return Err(SchemaContractError::Parse(format!(
                "contract_version `{}` is not supported by this build (expected `{}`). Upgrade \
                 the `harvest` CLI to match the checked-in baseline, or regenerate the baseline \
                 with this build",
                parsed.contract_version, SCHEMA_CONTRACT_VERSION
            )));
        }
        // Re-derive the invariants a hand-edited file may have broken:
        // canonical schemas, sorted+unique entries, and accurate coverage.
        let rebuilt = Self::from_entries(&parsed.version, std::mem::take(&mut parsed.workflows));
        parsed.workflows = rebuilt.workflows;
        parsed.coverage = rebuilt.coverage;
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
    /// The acknowledgement is an append-only audit record: after this call the
    /// baseline matches `current`, so the next check is clean, and the git diff
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
        // delta, but the stored listing is capped at `MAX_DELTAS` while the
        // rebase takes the whole current contract — so a truncated diff would
        // silently absorb the excess with no record of it. Refuse rather than
        // write an audit log that under-reports what was acknowledged.
        if diff.truncated {
            return Err(SchemaContractError::TruncatedAcknowledgement {
                recorded: diff.deltas.len(),
                breaking: diff.breaking_count,
            });
        }
        let acks: Vec<AcknowledgedBreakingChange> = diff
            .breaking()
            .map(|d| AcknowledgedBreakingChange {
                workflow: d.workflow.clone(),
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

/// Placeholder version for a contract parsed from a bare
/// `GET /workflows/registered` array, which carries no crate version.
const UNKNOWN_VERSION: &str = "unknown";

const DEFAULT_CONTRACT_DESCRIPTION: &str = "Versioned machine-readable baseline of every \
     registered workflow type's published input/output/error JSON Schema (issue #373), used by \
     `harvest schema check` to gate backward-incompatible payload changes before they DLQ \
     in-flight executions (issue #794). Stored schemas are canonicalised: annotations \
     (title/description/examples/default/non-numeric format) are stripped so a doc-comment edit \
     never dirties this file. Regenerate with `harvest schema update`; a breaking change must be \
     acknowledged with a recorded justification. See docs/workflow-schema-contract-guide.md.";

fn default_description() -> String {
    DEFAULT_CONTRACT_DESCRIPTION.to_string()
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
    "default",
    "deprecated",
    "readOnly",
    "writeOnly",
];

/// Containers that hold sub-schemas rather than constraining an instance. They
/// are reached through `$ref`, never compared directly.
const CONTAINER_KEYWORDS: &[&str] = &["definitions", "$defs"];

/// Bounds whose *increase* narrows the accepted set.
const LOWER_BOUNDS: &[&str] = &["minimum", "minLength", "minItems"];
/// Bounds whose *decrease* narrows the accepted set.
const UPPER_BOUNDS: &[&str] = &["maximum", "maxLength", "maxItems"];

/// Inclusive value range of a numeric `format`, as `(min, max)`.
///
/// `schemars` records integer width and signedness **only** in `format` — the
/// `type` of both `i64` and `i32` is `"integer"` — so ignoring it would make
/// `i64 -> i32` (a textbook break) invisible.
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

/// `true` when `format` names a numeric width. Every other `format` value
/// (`uuid`, `date-time`, `email`, …) is a pure annotation here: the engine's
/// validator never reads it, and a string stays a string.
fn is_numeric_format(format: &str) -> bool {
    numeric_format_range(format).is_some()
}

/// `true` when `format` names an **integral** width (`int*`/`uint*`).
///
/// The range lattice alone cannot separate `float` from `int32`: int32's
/// integer range is strictly wider than float's exactly-representable one, so a
/// pure range comparison calls `float -> int32` a widening. It is not — a
/// recorded `1.5` is a perfectly good float and deserializes into no integer
/// type at all. Integrality is a second, independent axis.
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
/// - drops [`ANNOTATION_KEYWORDS`] and every non-numeric `format`;
/// - sorts `enum` arrays (an `enum` is a set; order is not semantic);
/// - collapses a `oneOf`/`anyOf` of singleton-`enum` branches into one flat
///   `enum` — `schemars` flips between the two forms when a *single* unit
///   variant gains a doc comment, which would otherwise read as "every variant
///   removed and two branches added".
#[must_use]
pub fn canonicalize_schema(schema: &Value) -> Value {
    canonicalize_node(schema, 0)
}

fn canonicalize_node(schema: &Value, depth: usize) -> Value {
    if depth >= MAX_DIFF_DEPTH {
        return schema.clone();
    }
    match schema {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|i| canonicalize_node(i, depth + 1))
                .collect(),
        ),
        Value::Object(obj) => {
            let mut out = Map::new();
            for (k, v) in obj {
                if ANNOTATION_KEYWORDS.contains(&k.as_str()) {
                    continue;
                }
                if k == "format" {
                    if v.as_str().is_some_and(is_numeric_format) {
                        out.insert(k.clone(), v.clone());
                    }
                    continue;
                }
                if k == "enum" {
                    out.insert(k.clone(), sorted_enum(v));
                    continue;
                }
                out.insert(k.clone(), canonicalize_node(v, depth + 1));
            }
            collapse_unit_enum_branches(&mut out);
            flatten_single_member_all_of(&mut out);
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Collapse `{"allOf": [X]}` into `X`.
///
/// `schemars`' `RemoveRefSiblings` pass rewrites any `$ref` that carries a
/// sibling keyword into `{"allOf": [{"$ref": …}], <siblings>}` — and the
/// trigger is as small as **a doc comment on a struct-typed field**. Without
/// this, adding one `///` line silently restructures the schema, and the differ
/// would report the whole referenced subtree as rewritten.
///
/// Only a lone member is lifted, and only when the parent carries no other
/// analysed keyword: with siblings present the conjunction is real and must be
/// compared as one (see [`diff_all_of`]). Annotations are already stripped by
/// the time this runs, so the common case is a bare single-member wrapper.
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
/// `schemars` emits a unit-only Rust enum as a flat `enum` array, but switches
/// to a `oneOf` of singleton `enum`s as soon as **one** variant carries a doc
/// comment. Without this the two forms of the same type read as a total rewrite.
fn collapse_unit_enum_branches(obj: &mut Map<String, Value>) {
    for key in ["oneOf", "anyOf"] {
        // Never overwrite a sibling the differ analyses. A node carrying BOTH
        // `enum` and `oneOf` is meaningful to the runtime — `validate_against_schema`
        // enforces both — so collapsing here would clobber a real constraint and
        // make a later change to it invisible (or invert its direction).
        //
        // This also makes the loop itself safe: once `oneOf` collapses into
        // `enum`, the `anyOf` pass sees that `enum` and leaves it alone rather
        // than overwriting the values the first pass just wrote.
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
        if all_singleton_enums {
            let types: BTreeSet<String> = branches
                .iter()
                .filter_map(|b| b.get("type").and_then(Value::as_str))
                .map(ToString::to_string)
                .collect();
            let lifted_type = (types.len() == 1)
                .then(|| types.into_iter().next())
                .flatten();
            // A parent `type` that disagrees with the branches' shared type is a
            // contradictory schema, not a normalisation opportunity — rewriting
            // it would change what the node means. Leave the whole node alone.
            if let (Some(existing), Some(lifted)) = (
                obj.get("type").and_then(Value::as_str),
                lifted_type.as_deref(),
            ) && existing != lifted
            {
                continue;
            }
            obj.remove(key);
            if let Some(t) = lifted_type {
                obj.insert("type".to_string(), Value::String(t));
            }
            obj.insert("enum".to_string(), sorted_enum(&Value::Array(values)));
        }
    }
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
                workflow: ack.workflow.clone(),
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
            workflow: (*name).to_string(),
            role: None,
            field_path: String::new(),
            change: ChangeKind::WorkflowAdded,
            verdict: Verdict::Compatible,
            reason: "new workflow type — no recorded history exists for it yet".to_string(),
        });
    }

    for name in base_names.difference(&cur_names) {
        diff.push(SchemaDelta {
            workflow: (*name).to_string(),
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
        for role in SchemaRole::all() {
            diff_role(
                name,
                role,
                b.schema_for(role),
                c.schema_for(role),
                &mut diff,
            );
        }
    }

    diff.deltas.sort_by(|a, b| {
        (&a.workflow, a.role, &a.field_path, a.change).cmp(&(
            &b.workflow,
            b.role,
            &b.field_path,
            b.change,
        ))
    });
    diff
}

fn diff_role(
    workflow: &str,
    role: SchemaRole,
    baseline: Option<&Value>,
    current: Option<&Value>,
    diff: &mut SchemaContractDiff,
) {
    match (baseline, current) {
        (None, None) => {}
        (None, Some(_)) => diff.push(SchemaDelta {
            workflow: workflow.to_string(),
            role: Some(role),
            field_path: String::new(),
            change: ChangeKind::SchemaAdded,
            verdict: Verdict::Compatible,
            reason: format!(
                "a {role} schema is now published; recorded payloads were previously \
                 unconstrained by this gate"
            ),
        }),
        (Some(_), None) => diff.push(SchemaDelta {
            workflow: workflow.to_string(),
            role: Some(role),
            field_path: String::new(),
            change: ChangeKind::SchemaRemoved,
            verdict: Verdict::Breaking,
            reason: format!(
                "the published {role} schema was withdrawn. This does not break replay by \
                 itself, but it removes this payload from the gate's coverage — deleting a \
                 schema must never be a silent way to stop being checked"
            ),
        }),
        (Some(b), Some(c)) => {
            let bc = canonicalize_schema(b);
            let cc = canonicalize_schema(c);
            let mut ctx = DiffCtx {
                workflow,
                role,
                base_root: &bc,
                cur_root: &cc,
                visited: BTreeSet::new(),
            };
            diff_node(&mut ctx, &bc, &cc, "", 0, diff);
        }
    }
}

/// Per-schema walk state.
struct DiffCtx<'a> {
    workflow: &'a str,
    role: SchemaRole,
    base_root: &'a Value,
    cur_root: &'a Value,
    /// `$ref` pairs already expanded, so a recursive type terminates.
    visited: BTreeSet<(String, String)>,
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
            workflow: self.workflow.to_string(),
            role: Some(self.role),
            field_path: field_path.to_string(),
            change,
            verdict,
            reason,
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
        if !ctx.visited.insert(key) {
            // Already expanded this pair: either a recursive type, or a
            // definition reached by a second path. Coinductively compatible —
            // anything reachable from here was compared the first time.
            //
            // `visited` is deliberately MONOTONIC (no `remove` on the way out).
            // Scoping it to the current stack would make it a cycle guard only,
            // and a schema whose definitions cross-reference fans out
            // exponentially: a few KB of `$ref`s never finishes. Memoising
            // instead makes each pair O(1) and the whole diff linear in the
            // number of distinct pairs.
            return;
        }
        diff_node(ctx, b, c, path, depth + 1, diff);
        return;
    }

    let (Some(bo), Some(co)) = (b.as_object(), c.as_object()) else {
        // A non-object schema node: JSON Schema's boolean form (`true`/`false`)
        // or something malformed. None of the keyword comparisons apply, but
        // the two sides can still differ — `false` rejects everything, `true`
        // accepts everything — so a change here is reported rather than
        // skipped. Equal non-object nodes are genuinely no change.
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
    // IN FULL. Comparing the two nodes' keywords flat as well would
    // double-count: the flat side's `properties` would read as "removed"
    // merely because the other side moved them inside a branch. `T` ->
    // `Option<T>` in schemars' non-primitive form
    // (`{"anyOf":[{"$ref":…},{"type":"null"}]}`) is exactly this shape, so
    // getting this wrong would report the single most common widening as
    // breaking.
    if branch_set(bo).is_some() != branch_set(co).is_some() {
        guard_container_siblings(ctx, bo, co, path, diff);
        diff_branches(ctx, bo, co, path, depth, diff);
        return;
    }

    diff_types(ctx, bo, co, path, diff);
    diff_enum(ctx, bo, co, path, diff);
    diff_numeric_format(ctx, bo, co, path, diff);
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
/// `allOf` is an **AND**: a value must satisfy every member. That makes adding
/// a member a narrowing, which is exactly the shape that would otherwise slip
/// through — `{"type":"integer"}` → `{"allOf":[{"type":"integer"},{"minimum":10}]}`
/// rejects a recorded `1` while every keyword the flat comparison looks at is
/// unchanged.
///
/// `allOf` sits in [`ANALYSED_KEYWORDS`], which excludes it from
/// [`diff_unanalysed`]'s fail-closed sweep, so this is the *only* thing
/// standing between that change and a `compatible` verdict.
///
/// Members are paired **by position**: `allOf` is an ordered array in both
/// `schemars` output and hand-written schemas, and a positional pair is the
/// only pairing that lets a change *inside* a member be reported at its real
/// path instead of as a wholesale replacement.
///
/// A length change is reported rather than guessed:
///
/// - **added** conjunct → narrowing, breaking.
/// - **removed** conjunct → also breaking, fail-closed. Removal looks like a
///   loosening, but `allOf` members interact with the parent: draft-07
///   `additionalProperties` does **not** see into subschemas, so dropping a
///   member that declared `properties` turns those keys into "additional" and
///   *tightens* the schema. This is the same reasoning as the unanalysed-keyword
///   policy — the differ does not model keyword interactions, so it refuses to
///   guess and lets the author acknowledge.
///
/// The overwhelmingly common `schemars` shape — a lone `{"allOf":[{"$ref":…}]}`
/// wrapper produced by `RemoveRefSiblings` when a field gains a doc comment —
/// never reaches here: [`flatten_single_member_all_of`] lifts it during
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
        // Constraining a previously unconstrained node narrows it.
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

fn diff_numeric_format(
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
            // A bound that is PRESENT but not a JSON number (`"minimum": "10"`
            // in a hand-written schema) cannot be placed on the tighten/relax
            // lattice. Reading it through `as_f64` alone would collapse it to
            // `None` — indistinguishable from absent — so a malformed value
            // would be reported as "`minimum` was removed" => COMPATIBLE, and
            // two different malformed values would produce no delta at all.
            let unreadable = |v: Option<&Value>| v.is_some_and(|v| v.as_f64().is_none());
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
/// `serde_json` produces fits in `i64` or `u64`). Only a genuine float falls
/// back to `f64`, where `f64` *is* the value's own representation and nothing
/// JSON preserved is lost. `None` means incomparable (NaN), which callers
/// report fail-closed rather than treating as unchanged.
fn cmp_json_numbers(a: &Value, b: &Value) -> Option<Ordering> {
    let exact = |v: &Value| {
        v.as_i64()
            .map(i128::from)
            .or_else(|| v.as_u64().map(i128::from))
    };
    if let (Some(x), Some(y)) = (exact(a), exact(b)) {
        return Some(x.cmp(&y));
    }
    a.as_f64()?.partial_cmp(&b.as_f64()?)
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

    // `required` may name a key that has no entry in `properties` — legal JSON
    // Schema, and the whole schema when `properties` is absent entirely. Those
    // names must be in the walk: `{"required":["a"]}` → `{"required":["a","b"]}`
    // demands a key recorded JSON never had, and iterating the property maps
    // alone would never visit `b`.
    let names: BTreeSet<&String> = bp
        .keys()
        .chain(cp.keys())
        .chain(breq.iter())
        .chain(creq.iter())
        .collect();
    for name in names {
        let child = format!("{path}/{}", escape_pointer_token(name));
        match (bp.get(name), cp.get(name)) {
            (None, Some(_)) => {
                if creq.contains(name) {
                    diff.push(ctx.delta(
                        &child,
                        ChangeKind::RequiredPropertyAdded,
                        Verdict::Breaking,
                        format!(
                            "required property `{name}` added; JSON recorded before this change \
                             has no such key and fails to deserialize (serde: `missing field \
                             \\`{name}\\``). Make it `Option<T>` or add `#[serde(default)]` to \
                             keep in-flight executions replayable"
                        ),
                    ));
                } else {
                    diff.push(ctx.delta(
                        &child,
                        ChangeKind::OptionalPropertyAdded,
                        Verdict::Compatible,
                        format!("optional property `{name}` added"),
                    ));
                }
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
                         add `#[serde(alias = \"{name}\")]` to the new field.{extra}"
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
                diff_node(ctx, b, c, &child, depth + 1, diff);
            }
            // Named only by `required`, with no `properties` entry on either
            // side.
            (None, None) => diff_bare_required(ctx, name, &child, &breq, &creq, diff),
        }
    }
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
fn additional_properties_rank(obj: &Map<String, Value>) -> u8 {
    match obj.get("additionalProperties") {
        None | Some(Value::Bool(true)) => 2,
        Some(Value::Bool(false)) => 0,
        Some(_) => 1,
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

/// A stable identity for a `oneOf`/`anyOf` branch.
///
/// Branches must be matched by identity, never by index: `schemars` collapses
/// every unit variant into a single branch that it emits **first**, so adding
/// the first unit variant shifts every existing branch and a positional diff
/// reports a false-positive storm.
///
/// Returns `(key, is_discriminated)`. A discriminated branch carries a variant
/// tag, which proves it is disjoint from its siblings — the condition under
/// which adding a `oneOf` branch is safe.
/// Identity key for a `oneOf`/`anyOf` branch, plus whether the branch is
/// provably *discriminated* (so adding a sibling cannot make previously-valid
/// data ambiguous under `oneOf`'s exactly-one rule).
///
/// `root` is the document the branch came from: a branch is resolved through
/// `$ref` **before** it is keyed, so a bare `{"$ref": "#/definitions/Inner"}`
/// keys identically to the inlined `{"type":"object", …}` it points at. Without
/// that, `T` -> `Option<T>` in schemars' non-primitive form
/// (`{"anyOf":[{"$ref":…},{"type":"null"}]}`) would read as "the object variant
/// was removed" — a false breaking verdict on the single most common widening.
fn branch_key(branch: &Value, root: &Value) -> (String, bool) {
    let (branch, _) = resolve_ref(root, branch);
    let Some(obj) = branch.as_object() else {
        return (branch.to_string(), false);
    };
    // Internally tagged: a property pinned to a single `enum`/`const` value.
    if let Some(props) = obj.get("properties").and_then(Value::as_object) {
        for (name, sub) in props {
            let tag = sub.get("const").cloned().or_else(|| {
                match sub.get("enum").and_then(Value::as_array) {
                    Some(vals) if vals.len() == 1 => Some(vals[0].clone()),
                    _ => None,
                }
            });
            if let Some(t) = tag {
                return (format!("tag:{name}={t}"), true);
            }
        }
    }
    // Unit variant: a singleton `enum`.
    if let Some(vals) = obj.get("enum").and_then(Value::as_array)
        && vals.len() == 1
    {
        return (format!("unit:{}", vals[0]), true);
    }
    // Externally tagged: `{"Variant": payload}` — exactly one required property
    // AND exactly one declared property, which is what `serde` emits for this
    // representation. Requiring both is what separates a real variant from an
    // ordinary struct that merely happens to have a single mandatory field:
    // two such structs are NOT disjoint (`{"id":…,"name":…}` satisfies both),
    // so keying them as discriminated variants would call an added `oneOf`
    // branch compatible when it can reject previously-valid data.
    //
    // Disjointness here rests on `serde`'s own externally-tagged *serializer*
    // emitting exactly one key, so a payload already in `harvest_events` cannot
    // match two variant branches at once.
    let req = required_set(obj);
    let prop_count = obj
        .get("properties")
        .and_then(Value::as_object)
        .map_or(0, Map::len);
    if req.len() == 1
        && prop_count == 1
        && let Some(name) = req.iter().next()
    {
        return (format!("variant:{name}"), true);
    }
    // Only reachable for an *unresolvable* `$ref` (external, or a dangling
    // local pointer): `resolve_ref` above already inlined every resolvable one.
    if let Some(r) = obj.get("$ref").and_then(Value::as_str) {
        return (format!("ref:{r}"), false);
    }
    if let Some(ts) = type_set(obj) {
        return (format!("type:{}", join_sorted(&ts)), false);
    }
    (branch.to_string(), false)
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
/// Switching `anyOf` → `oneOf` therefore **narrows** even when every branch is
/// untouched: with `integer` and `number` branches, a recorded integer matches
/// both, which `anyOf` accepts and `oneOf` rejects. Only the *current* keyword
/// is carried into the branch comparison, so without this the branch maps
/// compare equal and the change emits nothing at all.
///
/// Also guards the both-present case: [`branch_set`] selects `oneOf` first, so
/// an `anyOf` sibling on the same node is ignored — and `anyOf` sits in
/// [`ANALYSED_KEYWORDS`], so [`diff_unanalysed`]'s fail-closed sweep skips it
/// too. A change to the ignored sibling would otherwise be invisible.
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
/// the **first** matching variant in declaration order. Reordering two
/// overlapping variants (`Int(i64), Float(f64)` → `Float(f64), Int(i64)`)
/// therefore silently rebinds recorded data to a *different* variant: it still
/// deserializes, but it no longer means the same thing — precisely the failure
/// this gate exists to catch. Branches are matched by key in a `BTreeMap`,
/// which erases order, so the reorder must be detected separately.
///
/// `oneOf` requires exactly one match, so declaration order cannot affect
/// binding there and is not checked.
///
/// Only branches present on **both** sides are compared: an addition
/// legitimately shifts positions (`T` → `Option<T>` appends a `null` branch)
/// and is judged by the add/remove rules instead.
///
/// Reported breaking without attempting to prove the branches overlap —
/// deciding schema disjointness in general is not something this differ models,
/// and reordering variants of an untagged enum is a deliberate act. Disjoint
/// variants can be acknowledged.
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

/// Key both sides' branches, checking `anyOf` declaration order on the way.
///
/// Each side is keyed against *its own* document root, so `$ref` targets are
/// resolved before comparison and a branch does not appear to move merely
/// because it was inlined on one side and referenced on the other.
///
/// Keys are computed in DECLARATION ORDER first because the returned maps erase
/// that order — and for `anyOf`, order *is* serde's untagged binding rule (see
/// [`diff_anyof_branch_order`]).
fn key_branches(
    ctx: &DiffCtx<'_>,
    base_branches: &[Value],
    cur_branches: &[Value],
    both_anyof: bool,
    path: &str,
    diff: &mut SchemaContractDiff,
) -> (BTreeMap<String, Value>, BTreeMap<String, (Value, bool)>) {
    let base_keys: Vec<String> = base_branches
        .iter()
        .map(|b| branch_key(b, ctx.base_root).0)
        .collect();
    let cur_keyed: Vec<(String, bool)> = cur_branches
        .iter()
        .map(|b| branch_key(b, ctx.cur_root))
        .collect();

    if both_anyof {
        let cur_keys: Vec<String> = cur_keyed.iter().map(|(k, _)| k.clone()).collect();
        diff_anyof_branch_order(ctx, &base_keys, &cur_keys, path, diff);
    }

    let base_by_key: BTreeMap<String, Value> = base_keys
        .into_iter()
        .zip(base_branches.iter().cloned())
        .collect();
    let cur_by_key: BTreeMap<String, (Value, bool)> = cur_keyed
        .into_iter()
        .zip(cur_branches.iter().cloned())
        .map(|((k, d), b)| (k, (b, d)))
        .collect();
    (base_by_key, cur_by_key)
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
    let (base_by_key, cur_by_key) =
        key_branches(ctx, &base_branches, &cur_branches, both_anyof, path, diff);

    // Branches are matched across the two sides BY KEY, so two branches sharing
    // a key collapse in the map and one is silently dropped — which would hide
    // a variant REMOVAL behind a same-keyed survivor and report it compatible.
    // Ambiguous keying means the differ cannot say which branch is which, so it
    // fails closed rather than guessing.
    if base_by_key.len() != base_branches.len() || cur_by_key.len() != cur_branches.len() {
        diff.push(
            ctx.delta(
                path,
                ChangeKind::UnanalysedConstraintChanged,
                Verdict::Breaking,
                "two or more branches of this `oneOf`/`anyOf` are indistinguishable to the differ \
             (no variant tag, unit `enum`, or single-property external tag), so branches cannot \
             be matched across revisions and a removal could pass unnoticed; the comparison is \
             reported breaking (fail closed). Acknowledge it if it is safe"
                    .to_string(),
            ),
        );
        return;
    }

    for (key, b) in &base_by_key {
        match cur_by_key.get(key) {
            Some((c, _)) => diff_node(
                ctx,
                b,
                c,
                &format!("{path}/<{}>", key.replace('/', "~1")),
                depth + 1,
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
    for (key, (_, discriminated)) in &cur_by_key {
        if base_by_key.contains_key(key) {
            continue;
        }
        // `anyOf` means "at least one", so an added branch can never reject a
        // previously-valid instance. `oneOf` means "exactly one", so an added
        // branch that OVERLAPS an existing one turns a single match into two and
        // rejects it — safe only when the branch carries a variant tag proving
        // disjointness.
        if cur_kw == "anyOf" || *discriminated {
            let note = if cur_kw == "anyOf" {
                " (note: for `#[serde(untagged)]`, serde binds the FIRST matching variant in \
                 declaration order — inserting a permissive variant ahead of an existing one can \
                 silently rebind recorded data to a different branch)"
            } else {
                ""
            };
            diff.push(ctx.delta(
                path,
                ChangeKind::VariantAdded,
                Verdict::Compatible,
                format!("variant `{key}` added{note}"),
            ));
        } else {
            diff.push(ctx.delta(
                path,
                ChangeKind::VariantAdded,
                Verdict::Breaking,
                format!(
                    "variant `{key}` added to a `oneOf` without a variant tag proving it is \
                     disjoint from the existing branches; `oneOf` requires EXACTLY one match, so \
                     an overlapping branch rejects previously-valid recorded data"
                ),
            ));
        }
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
/// node is opaque — the differ cannot read the target to say whether the new one
/// accepts every payload the old one did — so any change to it fails closed.
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

/// Escape a property name for use in an RFC 6901 JSON Pointer.
fn escape_pointer_token(name: &str) -> String {
    name.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn branch_key_is_stable_and_marks_discriminated_branches() {
        let root = json!({});
        let (k, d) = branch_key(&json!({"type":"string","enum":["C"]}), &root);
        assert_eq!(k, "unit:\"C\"");
        assert!(d);
        let (k, d) = branch_key(
            &json!({"type":"object","required":["A"],"properties":{"A":{}}}),
            &root,
        );
        assert_eq!(k, "variant:A");
        assert!(d);
        let (k, d) = branch_key(&json!({"type":"integer"}), &root);
        assert_eq!(k, "type:integer");
        assert!(!d, "an untagged scalar branch is not provably disjoint");
    }

    #[test]
    fn branch_key_resolves_a_ref_so_inlined_and_referenced_branches_match() {
        // The `Option<Inner>` widening depends on this: schemars emits
        // `{"anyOf":[{"$ref":"#/definitions/Inner"},{"type":"null"}]}`, so the
        // referenced branch must key identically to the inlined baseline node
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
