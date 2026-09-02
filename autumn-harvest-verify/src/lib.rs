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
pub mod pipeline;
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

/// Output format for the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    /// Human-readable report (the default).
    Text,
    /// The [`Report`] as JSON on stdout.
    Json,
}

/// `cargo harvest-verify` — semantic determinism verification of `#[workflow]` fns.
// Cargo-style flags are booleans; a state machine would obscure, not clarify.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, clap::Parser)]
#[command(
    name = "cargo-harvest-verify",
    bin_name = "cargo harvest-verify",
    about = "Semantic (MIR-level) determinism verifier for autumn-harvest #[workflow] functions",
    long_about = None,
)]
pub struct Cli {
    /// Path to the Cargo.toml of the workspace to analyze.
    #[arg(long, value_name = "PATH")]
    pub manifest_path: Option<std::path::PathBuf>,
    /// Package to analyze (repeatable).
    #[arg(short = 'p', long = "package", value_name = "SPEC")]
    pub package: Vec<String>,
    /// Analyze the package's library target.
    #[arg(long)]
    pub lib: bool,
    /// Analyze this example target (repeatable).
    #[arg(long = "example", value_name = "NAME")]
    pub example: Vec<String>,
    /// Analyze every example whose required features are enabled.
    #[arg(long)]
    pub all_examples: bool,
    /// Analyze this binary target (repeatable).
    #[arg(long = "bin", value_name = "NAME")]
    pub bin: Vec<String>,
    /// Comma-separated feature list passed to cargo (repeatable).
    #[arg(long, value_name = "FEATURES")]
    pub features: Vec<String>,
    /// Do not enable the packages' default features.
    #[arg(long)]
    pub no_default_features: bool,
    /// Build into this directory instead of `<workspace>/target/harvest-verify`.
    #[arg(long, value_name = "DIR")]
    pub target_dir: Option<std::path::PathBuf>,
    /// Analyze pre-emitted `.mir` files or directories instead of building (repeatable).
    #[arg(long = "mir", value_name = "PATH")]
    pub mir: Vec<std::path::PathBuf>,
    /// Extra root for resolving `<impl at file:line:col>` bodies (repeatable).
    #[arg(long = "source-root", value_name = "DIR")]
    pub source_root: Vec<std::path::PathBuf>,
    /// Model TOML overlaid on the builtin model, applied left to right (repeatable).
    #[arg(long = "model", value_name = "FILE")]
    pub model: Vec<std::path::PathBuf>,
    /// Allowlist file (`harvest-verify.allow.toml`).
    #[arg(long, value_name = "FILE")]
    pub allowlist: Option<std::path::PathBuf>,
    /// Fail on `unknown` verdicts and unused allowlist entries too.
    #[arg(long)]
    pub strict: bool,
    /// Output format.
    #[arg(long, value_enum, default_value = "text")]
    pub format: Format,
    /// Also print the analyzed/proven/unknown/found/allowed triple on stderr.
    #[arg(long)]
    pub report: bool,
    /// Print every analysis boundary name, one per line, and exit 0.
    #[arg(long)]
    pub list_boundaries: bool,
}

/// CLI entry: parses `argv` (tolerating the `harvest-verify` subcommand token cargo inserts),
/// runs the tool and returns the process exit code (0 clean, 1 findings, 2 tool error).
#[must_use]
pub fn cli_main(argv: Vec<String>) -> i32 {
    use clap::Parser as _;

    // `cargo harvest-verify ...` execs `cargo-harvest-verify harvest-verify ...`.
    let mut argv = argv;
    if argv.get(1).is_some_and(|a| a == "harvest-verify") {
        argv.remove(1);
    }
    let cli = match Cli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(err) => {
            // `--help` / `--version` are a successful run that prints to stdout.
            let usage_error = err.use_stderr();
            let _ = err.print();
            return if usage_error { 2 } else { 0 };
        }
    };

    if cli.list_boundaries {
        for kind in BoundaryKind::ALL {
            println!("{}", kind.name());
        }
        return 0;
    }

    match run(&cli) {
        Ok(report) => {
            let rendered = match cli.format {
                Format::Text => report.render_text(),
                Format::Json => match report.render_json() {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return 2;
                    }
                },
            };
            println!("{rendered}");
            if cli.report {
                let summary = report.summary();
                eprintln!(
                    "analyzed {}: proven {}, unknown {}, found {}, allowed {}",
                    summary.analyzed,
                    summary.proven,
                    summary.unknown,
                    summary.found,
                    summary.allowed
                );
            }
            report.exit_code(cli.strict)
        }
        Err(e) => {
            eprintln!("error: {e}");
            2
        }
    }
}

/// Validate the inputs the CLI owns, then hand off to [`verify`].
fn run(cli: &Cli) -> Result<Report> {
    // Fail on a bad allowlist or model *before* spending minutes in cargo: a
    // tool error the user can fix in a second should be reported in a second.
    if let Some(path) = &cli.allowlist {
        Allowlist::load(path)?;
    }
    let _ = Model::load_with_overlays(&cli.model)?;
    for path in &cli.mir {
        if !path.exists() {
            return Err(Error::Io {
                path: path.display().to_string(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no such .mir file or directory",
                ),
            });
        }
    }

    let build = driver::BuildRequest {
        manifest_path: cli.manifest_path.clone(),
        packages: cli.package.clone(),
        lib: cli.lib,
        examples: cli.example.clone(),
        all_examples: cli.all_examples,
        bins: cli.bin.clone(),
        features: cli.features.clone(),
        no_default_features: cli.no_default_features,
        target_dir: cli.target_dir.clone(),
    };
    let opts = Options {
        model_overlays: cli.model.clone(),
        allowlist: cli.allowlist.clone(),
        source_roots: cli.source_root.clone(),
        mir_paths: cli.mir.clone(),
        strict: cli.strict,
    };
    verify(&build, &opts)
}

/// Emit MIR for `build` and return the files, without analyzing them.
///
/// The debug half of the driver: it is what a `harvest-verify` run does before
/// any parsing happens, and it is exercised on its own by the (ignored) driver
/// smoke test so a cargo-integration break is distinguishable from an analysis
/// break.
///
/// # Errors
/// See [`driver::emit_mir`].
pub fn emit_only(build: &driver::BuildRequest) -> Result<Vec<driver::EmittedMir>> {
    driver::emit_mir(build)
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
    pipeline::run(build, opts)
}
