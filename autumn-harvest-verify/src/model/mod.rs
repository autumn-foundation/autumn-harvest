//! The determinism model, in data rather than in `match` arms.
//!
//! Table-driven model of sources, sinks, sanctioned primitives, forbidden
//! effects, sanitizers, reductions and trusted crates. The builtin model is
//! `harvest-verify.model.toml`, embedded with `include_str!` and overlayable via
//! `--model <file>` (union of rows; later rows for the same key override).
//! [`matcher`] implements how a row is matched against a call site.

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::TaintKind;

pub mod callee;
pub mod matcher;

pub use callee::CalleePath;
pub use matcher::CallClass;

/// The builtin model text.
pub const BUILTIN_MODEL_TOML: &str = include_str!("../../harvest-verify.model.toml");

/// A taint origin.
///
/// The `path` is matched as a `::`-segment suffix of the callee path with
/// generic arguments stripped, so `SystemTime::now` matches
/// `std::time::SystemTime::now`, and
/// `<HashMap<K, V> as IntoIterator>::into_iter` is matched via
/// `receiver = "HashMap"`, `path = "into_iter"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRule {
    pub path: String,
    /// Receiver / self type name (last segment, generics stripped) for trait/inherent methods.
    #[serde(default)]
    pub receiver: Option<String>,
    /// Optional disambiguator: a `::`-segment **suffix** of the fully-qualified declared
    /// type of the call's destination local, as printed by the `let _N: T;` declarations.
    ///
    /// MIR prints callee paths *trimmed* (`std::env::var` is printed as bare `var`) but
    /// `let` declarations *fully qualified*, so a bare-suffix rule such as `var` would
    /// otherwise match any user function of that name. Requiring
    /// `dest_type = "std::result::Result<std::string::String, std::env::VarError>"`
    /// pins the row to the real `std::env::var` without keying on an unstable path prefix.
    /// `None` = the destination type is not consulted.
    #[serde(default)]
    pub dest_type: Option<String>,
    pub kind: TaintKind,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct CtxMethodRule {
    pub path: String,
    #[serde(default = "default_ctx")]
    pub receiver: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForbiddenRule {
    pub path: String,
    #[serde(default)]
    pub receiver: Option<String>,
    /// Same disambiguator as [`SourceRule::dest_type`]: a suffix of the destination
    /// local's fully-qualified declared type (e.g. `tokio::time::Sleep` pins the bare
    /// `sleep` suffix to `tokio::time::sleep`).
    #[serde(default)]
    pub dest_type: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SanitizerRule {
    pub path: String,
    #[serde(default)]
    pub receiver: Option<String>,
    /// Same disambiguator as [`SourceRule::dest_type`]. It is what makes
    /// `collect` expressible as a sanitizer: only
    /// `collect` *into* a `BTreeMap`/`BTreeSet` kills `Order`, and the
    /// collection chosen is visible only in the destination local's declared type.
    #[serde(default)]
    pub dest_type: Option<String>,
    /// Which taint kind the sanitizer clears on its receiver/result.
    pub clears: TaintKind,
    pub reason: String,
}

/// A rule keyed on a *type* name rather than a call path.
///
/// Used by [`Model::ambient_type`]: the interior-mutability rows in `[[source]]`
/// (`Mutex::lock`, `AtomicU64::load`, `RefCell::borrow`, `LazyLock`'s `Deref::deref`, ...)
/// are only non-deterministic **sources** when their receiver is *ambient* — a `static`,
/// a `static mut`, a `thread_local!`, or a value reachable from one. Applied to a
/// non-ambient local (a `Mutex` created inside the workflow body) the same call merely
/// propagates the receiver's taint.
///
/// The analyzer therefore reads these two tables together: a `[[source]]` row on such a
/// receiver means "propagate the receiver's taint through this call, **and** treat a
/// `static` whose declared type appears in `[[ambient_type]]` as an ambient root".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeRule {
    /// Type name (last `::` segment, generic arguments stripped), e.g. `AtomicU64`.
    pub name: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedCrate {
    pub name: String,
    pub reason: String,
}

/// A body-less callee that is provably std/core/alloc even though rustc printed
/// its path trimmed to a bare segment.
///
/// The default for a callee with no body in the analyzed set is a named
/// [`crate::BoundaryKind::ExternalCrateBody`] boundary: an unemitted dependency
/// really is code the analysis never saw. std is the exception, because it is
/// modelled by the `[[source]]` / `[[sanitizer]]` / `[[reduction]]` tables
/// instead — but only when the analyzer can *tell* it is std, which the declared
/// types at the call site usually settle. These rows cover the rest: free
/// functions whose whole signature is primitives, so no type at the call site is
/// rooted at `std::`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StdFreeFnRule {
    /// `::`-segment suffix of the callee path, exactly as [`SourceRule::path`].
    pub path: String,
    pub reason: String,
}

/// The whole model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Interior-mutable / lazily-initialised types whose `static` instances are ambient
    /// taint roots (see [`TypeRule`]).
    #[serde(default)]
    pub ambient_type: Vec<TypeRule>,
    /// Body-less std/core/alloc free functions rustc prints with no crate root
    /// and no std-rooted type at the call site (see [`StdFreeFnRule`]).
    #[serde(default)]
    pub std_free_fn: Vec<StdFreeFnRule>,
}

impl Model {
    /// The builtin model.
    ///
    /// The embedded TOML is parsed once per process (`OnceLock`) and cloned, so
    /// calling this in a loop is cheap.
    ///
    /// # Errors
    /// If the embedded TOML fails to parse (a build-time defect, surfaced as an error, not a panic).
    pub fn builtin() -> crate::Result<Self> {
        Self::builtin_ref().cloned()
    }

    /// The builtin model, parsed once and borrowed.
    ///
    /// # Errors
    /// If the embedded TOML fails to parse.
    pub fn builtin_ref() -> crate::Result<&'static Self> {
        static BUILTIN: OnceLock<std::result::Result<Model, String>> = OnceLock::new();
        match BUILTIN.get_or_init(|| Self::from_toml(BUILTIN_MODEL_TOML).map_err(|e| e.to_string()))
        {
            Ok(model) => Ok(model),
            Err(message) => Err(crate::Error::Model(message.clone())),
        }
    }

    /// Parse a model from TOML text.
    ///
    /// # Errors
    /// On malformed TOML.
    pub fn from_toml(text: &str) -> crate::Result<Self> {
        toml::from_str(text).map_err(|e| crate::Error::Model(e.to_string()))
    }

    /// Read a model from a TOML file.
    ///
    /// # Errors
    /// On i/o failure or malformed TOML (the path is named in either case).
    pub fn load(path: &Path) -> crate::Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| crate::Error::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        Self::from_toml(&text).map_err(|e| crate::Error::Model(format!("{}: {e}", path.display())))
    }

    /// The builtin model with `paths` overlaid on it, left to right (D8).
    ///
    /// # Errors
    /// If the builtin or any overlay fails to load or parse.
    pub fn load_with_overlays(paths: &[std::path::PathBuf]) -> crate::Result<Self> {
        let mut model = Self::builtin()?;
        for path in paths {
            model = model.merged_with(Self::load(path)?);
        }
        Ok(model)
    }

    /// Overlay another model on this one (D8).
    ///
    /// The result is the **union** of the rows: an overlay row whose
    /// `(path, receiver, dest_type)` key is already present *replaces* the
    /// earlier row in place (keeping its position, so the model stays readable
    /// as "builtin, then additions"), and any other overlay row is appended.
    /// `[[trusted]]` and `[[ambient_type]]` rows are keyed on `name`.
    /// The `version` string is taken from the overlay when it sets a non-empty
    /// one, so a verdict can always be traced to the rules that produced it.
    #[must_use]
    pub fn merged_with(self, overlay: Self) -> Self {
        Self {
            version: if overlay.version.is_empty() {
                self.version
            } else {
                overlay.version
            },
            source: merge_rows(self.source, overlay.source, |r| {
                (r.path.clone(), r.receiver.clone(), r.dest_type.clone())
            }),
            sink: merge_rows(self.sink, overlay.sink, |r| {
                (r.path.clone(), Some(r.receiver.clone()), None)
            }),
            sanctioned: merge_rows(self.sanctioned, overlay.sanctioned, ctx_key),
            non_sink: merge_rows(self.non_sink, overlay.non_sink, ctx_key),
            handler_registration: merge_rows(
                self.handler_registration,
                overlay.handler_registration,
                ctx_key,
            ),
            forbidden: merge_rows(self.forbidden, overlay.forbidden, |r| {
                (r.path.clone(), r.receiver.clone(), r.dest_type.clone())
            }),
            sanitizer: merge_rows(self.sanitizer, overlay.sanitizer, sanitizer_key),
            reduction: merge_rows(self.reduction, overlay.reduction, sanitizer_key),
            trusted: merge_rows(self.trusted, overlay.trusted, |r| {
                (r.name.clone(), None, None)
            }),
            ambient_type: merge_rows(self.ambient_type, overlay.ambient_type, |r| {
                (r.name.clone(), None, None)
            }),
            std_free_fn: merge_rows(self.std_free_fn, overlay.std_free_fn, |r| {
                (r.path.clone(), None, None)
            }),
        }
    }

    /// True when a body-less callee is one of the modelled std free functions.
    #[must_use]
    pub fn is_std_free_fn(&self, callee: &CalleePath) -> bool {
        self.std_free_fn
            .iter()
            .any(|rule| callee.ends_with_path(&rule.path))
    }
}

/// The key a row is deduplicated on: `(path, receiver, dest_type)`.
type RowKey = (String, Option<String>, Option<String>);

fn ctx_key(rule: &CtxMethodRule) -> RowKey {
    (rule.path.clone(), Some(rule.receiver.clone()), None)
}

fn sanitizer_key(rule: &SanitizerRule) -> RowKey {
    (
        rule.path.clone(),
        rule.receiver.clone(),
        rule.dest_type.clone(),
    )
}

/// Union of two row lists: a later row with an existing key replaces it in place.
fn merge_rows<T>(base: Vec<T>, overlay: Vec<T>, key: impl Fn(&T) -> RowKey) -> Vec<T> {
    let mut rows: Vec<T> = Vec::with_capacity(base.len().saturating_add(overlay.len()));
    let mut index: HashMap<RowKey, usize> = HashMap::new();
    for row in base.into_iter().chain(overlay) {
        let k = key(&row);
        if let Some(&at) = index.get(&k) {
            if let Some(slot) = rows.get_mut(at) {
                *slot = row;
            }
        } else {
            index.insert(k, rows.len());
            rows.push(row);
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn overlay(text: &str) -> Model {
        Model::from_toml(text).expect("overlay fixture must parse")
    }

    #[test]
    fn the_builtin_model_is_parsed_once_and_is_non_empty() {
        let a = Model::builtin_ref().expect("builtin");
        let b = Model::builtin_ref().expect("builtin");
        assert!(std::ptr::eq(a, b), "the builtin model must be cached");
        assert!(!a.version.is_empty());
        assert!(!a.source.is_empty() && !a.sink.is_empty());
    }

    #[test]
    fn merging_is_a_union_with_in_place_replacement() {
        let base = overlay(
            r#"
version = "base"
[[source]]
path = "SystemTime::now"
kind = "value"
reason = "base"
[[source]]
path = "iter"
receiver = "HashMap"
kind = "order"
reason = "base order"
"#,
        );
        let merged = base.merged_with(overlay(
            r#"
version = "over"
[[source]]
path = "SystemTime::now"
kind = "value"
reason = "overridden"
[[source]]
path = "new_v4"
receiver = "Uuid"
kind = "value"
reason = "added"
"#,
        ));
        assert_eq!(merged.version, "over");
        assert_eq!(merged.source.len(), 3, "{:?}", merged.source);
        assert_eq!(
            merged.source.first().map(|r| r.reason.as_str()),
            Some("overridden")
        );
        assert_eq!(
            merged.source.get(1).map(|r| r.path.as_str()),
            Some("iter"),
            "an untouched row keeps its position"
        );
        assert_eq!(
            merged.source.get(2).map(|r| r.path.as_str()),
            Some("new_v4")
        );
    }

    #[test]
    fn the_receiver_and_dest_type_are_part_of_the_key() {
        let base = overlay(
            r#"
[[sanitizer]]
path = "collect"
dest_type = "std::collections::BTreeMap"
clears = "order"
reason = "base"
"#,
        );
        let merged = base.merged_with(overlay(
            r#"
[[sanitizer]]
path = "collect"
dest_type = "std::collections::BTreeSet"
clears = "order"
reason = "different destination, different row"
"#,
        ));
        assert_eq!(merged.sanitizer.len(), 2);
    }

    #[test]
    fn an_overlay_can_declare_a_std_free_fn() {
        // The escape hatch for the `external-crate-body` default: a std free
        // function whose whole signature is primitives, so nothing at the call
        // site names its crate.
        let model = overlay(
            r#"
[[std_free_fn]]
path = "black_box"
reason = "core::hint::black_box; identity on a primitive."
"#,
        );
        assert!(model.is_std_free_fn(&CalleePath::parse("black_box")));
        assert!(model.is_std_free_fn(&CalleePath::parse("hint::black_box")));
        assert!(!model.is_std_free_fn(&CalleePath::parse("now_ish")));
        assert!(!Model::default().is_std_free_fn(&CalleePath::parse("black_box")));
    }

    #[test]
    fn an_unknown_table_or_field_is_rejected() {
        assert!(Model::from_toml("[[sourcez]]\npath = \"x\"\n").is_err());
        assert!(
            Model::from_toml(
                "[[source]]\npath = \"x\"\nkind = \"value\"\nreason = \"r\"\nreceivr = \"y\"\n"
            )
            .is_err()
        );
    }

    #[test]
    fn an_empty_overlay_version_keeps_the_base_version() {
        let base = overlay("version = \"base\"\n");
        assert_eq!(base.merged_with(Model::default()).version, "base");
    }
}
