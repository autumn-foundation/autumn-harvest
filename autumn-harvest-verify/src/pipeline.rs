//! The end-to-end run: emit, parse, resolve, analyze, allowlist, report.
//!
//! This is the only place that knows the order of the stages, and the only
//! place that turns a MIR body path into the *user-visible* workflow path. MIR
//! prints a trimmed item path (`wf_uuid_in_helper`), while every consumer — the
//! corpus oracle, the allowlist, the report — names a workflow by its full
//! `crate::module::fn`. The module segment survives in exactly one place: the
//! async shim's return type, `{async fn body of wf_uuid_in_helper::wf_uuid_in_helper()}`,
//! which [`qualified_workflow`] reads back.

use std::collections::BTreeSet;
use std::path::PathBuf;

use crate::driver::{self, BuildRequest, EmittedMir};
use crate::verdict::{Boundary, BoundaryKind, Site, Verdict, WorkflowVerdict};
use crate::{Allowlist, Model, Options, Report, analysis, entry, mir, resolve};

/// The rustc releases the MIR parser has been exercised against (D1).
///
/// Matched on `major.minor` only: a patch release never changes how MIR is
/// printed, and pinning one would make every run of a fresh toolchain warn.
const VALIDATED_RUSTC: &[&str] = &["1.94", "1.95", "1.96", "1.97", "1.98"];

/// Emit MIR for `build`, analyze it, and return the report.
///
/// # Errors
/// On cargo/build failure, a malformed model or allowlist, or unreadable inputs.
pub fn run(build: &BuildRequest, opts: &Options) -> crate::Result<Report> {
    let mut warnings: Vec<String> = Vec::new();
    let mut emitted: Vec<EmittedMir> = Vec::new();
    if !build.is_empty() {
        let (mirs, notes) = driver::emit_mir_with_warnings(build)?;
        emitted.extend(mirs);
        warnings.extend(notes);
    }
    emitted.extend(driver::collect_mir_paths(&opts.mir_paths));
    emitted.sort_by(|a, b| a.path.cmp(&b.path));
    emitted.dedup_by(|a, b| a.path == b.path);

    let model = Model::load_with_overlays(&opts.model_overlays)?;
    let rustc_version = driver::rustc_version();
    if !is_validated_rustc(&rustc_version) {
        warnings.push(format!(
            "the MIR parser is validated on rustc {}; this run used `{rustc_version}`. \
             Other versions may print paths and types differently, which can make model \
             rows stop matching — run the corpus tests \
             (`cargo test -p autumn-harvest-verify --test corpus`) on your toolchain \
             before trusting a clean result",
            VALIDATED_RUSTC.join(", ")
        ));
    }

    let mut docs = Vec::with_capacity(emitted.len());
    for item in &emitted {
        let raw = std::fs::read(&item.path).map_err(|e| crate::Error::Io {
            path: item.path.display().to_string(),
            source: e,
        })?;
        let display = item.path.display().to_string();
        // A dump that is not valid UTF-8 is a corrupt dump, not a tool error: it
        // becomes a `mir-parse` boundary on every workflow of that crate, so one
        // damaged file can never abort the analysis of the others.
        let (text, lossy) = match String::from_utf8(raw) {
            Ok(text) => (text, false),
            Err(e) => (String::from_utf8_lossy(e.as_bytes()).into_owned(), true),
        };
        let mut doc = mir::parse(&item.crate_name, &display, &text);
        if lossy {
            warnings.push(format!(
                "{display} is not valid UTF-8; it was decoded lossily and every workflow \
                 in `{}` carries a `mir-parse` boundary",
                item.crate_name
            ));
            doc.parse_failures.push(mir::ParseFailure {
                item: display.clone(),
                reason: "the dump is not valid UTF-8 and was decoded lossily".to_string(),
                line: 0,
            });
        }
        docs.push(doc);
    }

    let roots = source_roots(build, opts, &emitted);
    let entries = entry::discover(&docs);
    // Zero entries is a *reported* outcome, never a quiet clean run: with no
    // entry there is no per-workflow verdict to carry a boundary, so the only
    // place the failure can surface is the run itself.
    let discovery_failed = entries.is_empty();
    if discovery_failed {
        let failures: usize = docs.iter().map(|d| d.parse_failures.len()).sum();
        warnings.push(format!(
            "no #[workflow] entry points were discovered in the analyzed MIR \
             ({failures} parse failure{}); nothing was verified, so this run \
             proves nothing — under `--strict` it is a failure",
            if failures == 1 { "" } else { "s" }
        ));
    }
    let parse_failures = parse_failure_boundaries(&docs);
    let program = resolve::Program::build(docs, &roots)?;
    let (mut workflows, analysis_warnings) =
        analysis::analyze_with_warnings(&program, &model, &entries);
    warnings.extend(analysis_warnings);

    for (verdict, entry) in workflows.iter_mut().zip(&entries) {
        verdict.workflow = qualified_workflow(&program, entry);
        if let Some(extra) = parse_failures.get(&entry.crate_name) {
            // Re-derive rather than append: a boundary added after `assemble`
            // must still downgrade `proven-deterministic` to `unknown`, which is
            // the whole content of AC2.
            analysis::verdict::attach_boundaries(verdict, extra.clone());
        }
    }
    workflows.sort_by(|a, b| a.workflow.cmp(&b.workflow));

    let mut unused_allowlist = Vec::new();
    if let Some(path) = &opts.allowlist {
        let allowlist = Allowlist::load(path)?;
        apply_allowlist(&allowlist, &mut workflows, &mut unused_allowlist);
    }

    Ok(Report {
        model_version: model.version,
        rustc_version,
        workflows,
        unused_allowlist,
        warnings,
        discovery_failed,
    })
}

/// Suppress the verdicts an allowlist entry covers, and report the entries that
/// matched nothing.
///
/// Only `nondeterminism-found` and `unknown` consume an entry. A
/// `proven-deterministic` workflow has nothing to suppress, and marking it
/// `allowed` would spend the entry on it twice over: the run's `allowed` count
/// would claim a suppressed problem that does not exist, and the entry would
/// never be reported as unused — so the justification for a bug that has since
/// been fixed would sit in the file forever, silently ready to hide the *next*
/// finding on that workflow. Leaving it unused is what makes it deletable.
fn apply_allowlist(
    allowlist: &Allowlist,
    workflows: &mut [WorkflowVerdict],
    unused: &mut Vec<String>,
) {
    let mut used: BTreeSet<String> = BTreeSet::new();
    for workflow in workflows.iter_mut() {
        if matches!(workflow.verdict, Verdict::ProvenDeterministic) {
            continue;
        }
        if let Some(justification) = allowlist.justification(&workflow.workflow) {
            workflow.allowed = Some(justification.to_string());
            used.insert(workflow.workflow.clone());
        }
    }
    unused.extend(
        allowlist
            .unused(&used)
            .into_iter()
            .map(|entry| entry.workflow.clone()),
    );
}

/// `crate::module::fn` for an entry, recovered from the async shim's return type.
fn qualified_workflow(program: &resolve::Program, entry: &entry::Entry) -> String {
    let qualified = program
        .body(&program.body_id_in(&entry.crate_name, &entry.workflow))
        .and_then(|body| async_body_path(&body.return_ty))
        .unwrap_or_else(|| entry.workflow.clone());
    format!("{}::{}", entry.crate_name, qualified)
}

/// `{async fn body of m::f()}` → `m::f`.
fn async_body_path(ty: &str) -> Option<String> {
    let at = ty.find("{async fn body of ")?;
    let rest = ty.get(at.saturating_add("{async fn body of ".len())..)?;
    let end = rest.find("()}")?;
    let path = rest.get(..end)?.trim();
    (!path.is_empty()).then(|| path.to_string())
}

/// Directories `<impl at FILE:l:c>` paths are resolved against.
fn source_roots(
    build: &BuildRequest,
    opts: &Options,
    emitted: &[EmittedMir],
) -> resolve::SourceRoots {
    let mut roots: Vec<PathBuf> = Vec::new();
    if (!build.is_empty() || build.manifest_path.is_some())
        && let Ok(root) = driver::workspace_root(build.manifest_path.as_deref())
    {
        roots.push(root);
    }
    roots.extend(opts.source_roots.iter().cloned());
    // Pre-emitted `.mir` inputs with no cargo context: their sources are usually
    // beside them (the checked-in fixtures) or in the current directory.
    for item in emitted {
        if let Some(parent) = item.path.parent() {
            roots.push(parent.to_path_buf());
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }
    roots.dedup();
    resolve::SourceRoots { roots }
}

/// A `mir-parse` boundary per crate whose dump did not fully parse (D1).
fn parse_failure_boundaries(
    docs: &[mir::MirDoc],
) -> std::collections::BTreeMap<String, Vec<Boundary>> {
    let mut out: std::collections::BTreeMap<String, Vec<Boundary>> =
        std::collections::BTreeMap::new();
    for doc in docs {
        for failure in &doc.parse_failures {
            out.entry(doc.crate_name.clone())
                .or_default()
                .push(Boundary {
                    kind: BoundaryKind::MirParse,
                    detail: format!("{}: {}", failure.item, failure.reason),
                    site: Site {
                        function: failure.item.clone(),
                        block: String::new(),
                        what: failure.reason.clone(),
                        hint: Some(format!("{}:{}", doc.path, failure.line)),
                    },
                });
        }
    }
    out
}

/// True when `version` (the `rustc -Vv` first line) is one of [`VALIDATED_RUSTC`].
fn is_validated_rustc(version: &str) -> bool {
    let Some(number) = version.split_whitespace().nth(1) else {
        return false;
    };
    let mut parts = number.split('.');
    let (Some(major), Some(minor)) = (parts.next(), parts.next()) else {
        return false;
    };
    let major_minor = format!("{major}.{minor}");
    VALIDATED_RUSTC.contains(&major_minor.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_validated_rustc_set_is_matched_on_major_minor() {
        assert!(is_validated_rustc("rustc 1.98.0 (88d9e12ae 2026-08-18)"));
        assert!(is_validated_rustc("rustc 1.94.1 (e408947bf 2026-03-25)"));
        assert!(is_validated_rustc("rustc 1.95.0-nightly (abc 2026-04-01)"));
        assert!(
            !is_validated_rustc("rustc 1.99.0 (0000 2026-09-01)"),
            "a version outside the exercised set must warn"
        );
        assert!(!is_validated_rustc("rustc 2.0.0 (0000 2026-09-01)"));
        assert!(!is_validated_rustc("unknown"));
    }

    #[test]
    fn the_module_segment_comes_from_the_async_shims_return_type() {
        assert_eq!(
            async_body_path("{async fn body of wf_uuid_in_helper::wf_uuid_in_helper()}").as_deref(),
            Some("wf_uuid_in_helper::wf_uuid_in_helper")
        );
        assert_eq!(async_body_path("u64"), None);
    }
}
