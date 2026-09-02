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
use crate::verdict::{Boundary, BoundaryKind, Site, WorkflowVerdict};
use crate::{Allowlist, Model, Options, Report, analysis, entry, mir, resolve};

/// The rustc release the MIR parser is validated against (D1).
const VALIDATED_RUSTC: &str = "1.94";

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
    if !rustc_version.contains(VALIDATED_RUSTC) {
        warnings.push(format!(
            "the MIR parser is validated on rustc {VALIDATED_RUSTC}.x; this run used \
             `{rustc_version}`. A format change surfaces as a `mir-parse` boundary, \
             never as a wrong verdict"
        ));
    }

    let mut docs = Vec::with_capacity(emitted.len());
    for item in &emitted {
        let text = std::fs::read_to_string(&item.path).map_err(|e| crate::Error::Io {
            path: item.path.display().to_string(),
            source: e,
        })?;
        docs.push(mir::parse(
            &item.crate_name,
            &item.path.display().to_string(),
            &text,
        ));
    }

    let roots = source_roots(build, opts, &emitted);
    let entries = entry::discover(&docs);
    let parse_failures = parse_failure_boundaries(&docs);
    let program = resolve::Program::build(docs, &roots)?;
    let mut workflows = analysis::analyze(&program, &model, &entries);

    for (verdict, entry) in workflows.iter_mut().zip(&entries) {
        verdict.workflow = qualified_workflow(&program, entry);
        if let Some(extra) = parse_failures.get(&entry.crate_name) {
            for boundary in extra {
                if !verdict.boundaries.contains(boundary) {
                    verdict.boundaries.push(boundary.clone());
                }
            }
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
    })
}

/// Suppress the verdicts an allowlist entry covers, and report the entries that
/// matched nothing.
fn apply_allowlist(
    allowlist: &Allowlist,
    workflows: &mut [WorkflowVerdict],
    unused: &mut Vec<String>,
) {
    let mut used: BTreeSet<String> = BTreeSet::new();
    for workflow in workflows.iter_mut() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_module_segment_comes_from_the_async_shims_return_type() {
        assert_eq!(
            async_body_path("{async fn body of wf_uuid_in_helper::wf_uuid_in_helper()}").as_deref(),
            Some("wf_uuid_in_helper::wf_uuid_in_helper")
        );
        assert_eq!(async_body_path("u64"), None);
    }
}
