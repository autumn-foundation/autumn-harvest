//! `harvest-verify`: a semantic, MIR-level determinism verifier for
//! `#[workflow]` functions (issue #962, R&D prototype).
//!
//! The analyzer consumes the textual MIR that **stable** `rustc --emit=mir`
//! produces, builds an interprocedural call graph with generic substitution,
//! and runs a three-kind taint analysis (`Value`, `Order`, `Control`) from
//! non-deterministic sources to `WorkflowContext` command sinks. Every
//! `#[workflow]` fn receives exactly one of three verdicts:
//! [`Verdict::ProvenDeterministic`], [`Verdict::NondeterminismFound`] or
//! [`Verdict::Unknown`] — never a binary answer that hides an analysis
//! boundary.
//!
//! See `docs/rnd/determinism-static-analysis.md` (feasibility report) and
//! `docs/harvest-verify.md` (user guide).

pub mod allowlist;
pub mod analysis;
pub mod driver;
pub mod entry;
pub mod mir;
pub mod model;
pub mod report;
pub mod resolve;
pub mod verdict;

pub use allowlist::Allowlist;
pub use model::Model;
pub use report::Report;
pub use verdict::{
    Boundary, BoundaryKind, Finding, FindingKind, Hop, Site, TaintKind, Verdict, WorkflowVerdict,
};

/// Errors surfaced by the tool (exit code 2 at the CLI).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("i/o error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("cargo failed: {0}")]
    Cargo(String),
    #[error("model error: {0}")]
    Model(String),
    #[error("allowlist error: {0}")]
    Allowlist(String),
    #[error("{0}")]
    Other(String),
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// CLI entry: parses `argv` (tolerating the `harvest-verify` subcommand token cargo inserts),
/// runs the tool and returns the process exit code (0 clean, 1 findings, 2 tool error).
#[must_use]
pub fn cli_main(argv: Vec<String>) -> i32 {
    let _ = argv;
    todo!("RED phase: implemented in GREEN")
}

/// Run-time options that do not concern the cargo build itself.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Extra model TOML files overlaid on the builtin model, in order.
    pub model_overlays: Vec<std::path::PathBuf>,
    /// Allowlist file (`harvest-verify.allow.toml`); `None` = no allowlist.
    pub allowlist: Option<std::path::PathBuf>,
    /// Extra source roots for `<impl at file:l:c>` resolution (workspace root is always included).
    pub source_roots: Vec<std::path::PathBuf>,
    /// Pre-emitted `.mir` files/dirs to analyze instead of (or in addition to) building.
    pub mir_paths: Vec<std::path::PathBuf>,
    /// `unknown` verdicts and unused allowlist entries fail the run.
    pub strict: bool,
}

/// End-to-end: emit MIR for `build` (unless `opts.mir_paths` covers everything and
/// `build` is empty), parse, resolve, analyze, apply the allowlist, and return the report.
///
/// # Errors
/// On cargo/build failure, malformed model or allowlist, or unreadable inputs.
pub fn verify(build: &driver::BuildRequest, opts: &Options) -> Result<Report> {
    let _ = (build, opts);
    todo!("RED phase: implemented in GREEN")
}
