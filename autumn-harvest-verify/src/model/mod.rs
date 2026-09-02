//! Table-driven model of sources, sinks, sanctioned primitives, forbidden effects,
//! sanitizers, reductions and trusted crates. The builtin model is
//! `harvest-verify.model.toml`, embedded with `include_str!` and overlayable via
//! `--model <file>` (union of rows; later rows for the same `path` override).

use serde::{Deserialize, Serialize};

use crate::TaintKind;

/// The builtin model text.
pub const BUILTIN_MODEL_TOML: &str = include_str!("../../harvest-verify.model.toml");

/// A path pattern: matched as a `::`-segment suffix of the callee path with generic
/// arguments stripped (so `SystemTime::now` matches `std::time::SystemTime::now` and
/// `<HashMap<K, V> as IntoIterator>::into_iter` is matched via `receiver = "HashMap"`,
/// `path = "into_iter"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRule {
    pub path: String,
    /// Receiver / self type name (last segment, generics stripped) for trait/inherent methods.
    #[serde(default)]
    pub receiver: Option<String>,
    pub kind: TaintKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkRule {
    /// Method name on `WorkflowContext` (or another receiver via `receiver`).
    pub path: String,
    #[serde(default = "default_ctx")]
    pub receiver: String,
    /// Zero-based argument indexes (excluding `self`) whose taint is a finding; empty = all.
    #[serde(default)]
    pub args: Vec<usize>,
    /// Zero-based closure-argument indexes that must NOT be descended (e.g. `side_effect`'s closure).
    #[serde(default)]
    pub opaque_closure_args: Vec<usize>,
    pub reason: String,
}

fn default_ctx() -> String {
    "WorkflowContext".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CtxMethodRule {
    pub path: String,
    #[serde(default = "default_ctx")]
    pub receiver: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForbiddenRule {
    pub path: String,
    #[serde(default)]
    pub receiver: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SanitizerRule {
    pub path: String,
    #[serde(default)]
    pub receiver: Option<String>,
    /// Which taint kind the sanitizer clears on its receiver/result.
    pub clears: TaintKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrustedCrate {
    pub name: String,
    pub reason: String,
}

/// The whole model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Model {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub source: Vec<SourceRule>,
    #[serde(default)]
    pub sink: Vec<SinkRule>,
    /// Ctx primitives whose return values are deterministic (recorded in history).
    #[serde(default)]
    pub sanctioned: Vec<CtxMethodRule>,
    /// Ctx methods that are neither sinks nor sources (observability, metadata, history-clean reads).
    #[serde(default)]
    pub non_sink: Vec<CtxMethodRule>,
    /// Ctx methods that register handler closures (analyzed as entry-adjacent bodies).
    #[serde(default)]
    pub handler_registration: Vec<CtxMethodRule>,
    #[serde(default)]
    pub forbidden: Vec<ForbiddenRule>,
    #[serde(default)]
    pub sanitizer: Vec<SanitizerRule>,
    /// Order-killing reductions (`len`, `count`, `sum`, ...).
    #[serde(default)]
    pub reduction: Vec<SanitizerRule>,
    #[serde(default)]
    pub trusted: Vec<TrustedCrate>,
}

impl Model {
    /// The builtin model.
    ///
    /// # Errors
    /// If the embedded TOML fails to parse (a build-time defect, surfaced as an error, not a panic).
    pub fn builtin() -> crate::Result<Self> {
        Self::from_toml(BUILTIN_MODEL_TOML)
    }

    /// Parse a model from TOML text.
    ///
    /// # Errors
    /// On malformed TOML.
    pub fn from_toml(text: &str) -> crate::Result<Self> {
        toml::from_str(text).map_err(|e| crate::Error::Model(e.to_string()))
    }

    /// Overlay another model: union of rows; a later row with the same `path` (+ receiver) replaces the earlier one.
    #[must_use]
    pub fn merged_with(self, overlay: Self) -> Self {
        let _ = overlay;
        todo!("RED phase: implemented in GREEN")
    }
}
