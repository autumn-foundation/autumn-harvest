//! Verdict, finding and boundary types — the analyzer's public output vocabulary.

use serde::{Deserialize, Serialize};

/// The three taint kinds tracked by the analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaintKind {
    /// The bits of a value differ between the original run and a replay.
    Value,
    /// The value is history-derived but its iteration/sequence order is hash-seeded.
    Order,
    /// A branch condition is Value- or Order-tainted (command emission is control-dependent).
    Control,
}

/// Why a finding is a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FindingKind {
    /// A tainted value flows into a command-emitting sink argument.
    TaintedSinkArgument,
    /// A command-emitting sink is control-dependent on a tainted branch.
    ControlDependentSink,
    /// A forbidden effect (sleep, spawn, I/O, select-combinator) is reachable.
    ForbiddenEffect,
}

/// A program point: function body path plus a basic-block label (and optional source hint).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Site {
    /// MIR body path, e.g. `seeded::wf_x::{closure#0}` or `<impl at src/lib.rs:5:1: 5:15>::next`.
    pub function: String,
    /// Basic block label, e.g. `bb12`.
    pub block: String,
    /// What happened there: the callee path, the static name, the switch operand, ...
    pub what: String,
    /// Best-effort source hint (`file:line:col`) recovered from impl/closure spans or `debug` names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// One hop of a source→sink trace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hop {
    /// The function this hop is in.
    pub function: String,
    /// Human-readable step: `calls helper::<HashMap<String, u32>> [T := HashMap<String, u32>]`,
    /// `reads static COUNTER`, `iterates HashMap`, `branches on tainted value`, `emits execute_activity`.
    pub step: String,
}

/// A concrete nondeterminism finding with its source→sink trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub taint: TaintKind,
    pub source: Site,
    pub sink: Site,
    /// Ordered hops from the workflow entry to the source, then to the sink.
    pub trace: Vec<Hop>,
    /// One-line human message.
    pub message: String,
}

/// Named analysis boundaries — the honest `unknown` reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BoundaryKind {
    DynDispatch,
    IndirectCall,
    Ffi,
    UnsafeRawPointer,
    InlineAsm,
    ExternalCrateBody,
    UnmodeledCtxMethod,
    UnresolvedGeneric,
    Recursion,
    MirParse,
    MissingBody,
    DropGlue,
}

impl BoundaryKind {
    /// Every boundary kind the analyzer can emit, in stable order (mirrored by the report's boundary table).
    pub const ALL: [Self; 12] = [
        Self::DynDispatch,
        Self::IndirectCall,
        Self::Ffi,
        Self::UnsafeRawPointer,
        Self::InlineAsm,
        Self::ExternalCrateBody,
        Self::UnmodeledCtxMethod,
        Self::UnresolvedGeneric,
        Self::Recursion,
        Self::MirParse,
        Self::MissingBody,
        Self::DropGlue,
    ];

    /// Kebab-case name as printed in reports (`dyn-dispatch`, `ffi`, ...).
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::DynDispatch => "dyn-dispatch",
            Self::IndirectCall => "indirect-call",
            Self::Ffi => "ffi",
            Self::UnsafeRawPointer => "unsafe-raw-pointer",
            Self::InlineAsm => "inline-asm",
            Self::ExternalCrateBody => "external-crate-body",
            Self::UnmodeledCtxMethod => "unmodeled-ctx-method",
            Self::UnresolvedGeneric => "unresolved-generic",
            Self::Recursion => "recursion",
            Self::MirParse => "mir-parse",
            Self::MissingBody => "missing-body",
            Self::DropGlue => "drop-glue",
        }
    }
}

/// A boundary hit while analyzing a workflow.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Boundary {
    pub kind: BoundaryKind,
    /// The offending path/callee/static, e.g. `<dyn Fetcher as Fetcher>::get`.
    pub detail: String,
    pub site: Site,
}

/// Three-valued verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "kebab-case")]
pub enum Verdict {
    ProvenDeterministic,
    NondeterminismFound { findings: Vec<Finding> },
    Unknown { boundaries: Vec<Boundary> },
}

impl Verdict {
    /// Kebab-case verdict name as printed in reports.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::ProvenDeterministic => "proven-deterministic",
            Self::NondeterminismFound { .. } => "nondeterminism-found",
            Self::Unknown { .. } => "unknown",
        }
    }
}

/// The verdict for one `#[workflow]` fn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVerdict {
    /// Fully-qualified path of the workflow fn (`crate::module::name`).
    pub workflow: String,
    /// Crate name the workflow lives in.
    pub crate_name: String,
    pub verdict: Verdict,
    /// Boundaries hit even when the verdict is `nondeterminism-found` (kept so nothing is hidden).
    pub boundaries: Vec<Boundary>,
    /// `Some(justification)` when an allowlist entry suppressed the verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<String>,
}
