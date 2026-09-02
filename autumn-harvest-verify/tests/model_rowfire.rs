//! The model-row ratchet (issue #962, follow-up #2 of the feasibility report).
//!
//! The `Atomic<T>` incident is the reason this file exists. Between rustc 1.94
//! and 1.98 the compiler changed how it *spells* a type in MIR; the parser was
//! unaffected, no `mir-parse` boundary was raised, and five seeded bugs
//! silently became `proven-deterministic` because the model rows keyed on the
//! old spelling stopped matching anything. Nothing was red. The rows were still
//! there, still correct-looking, and dead.
//!
//! So: for every row of every keyed model table, this test asks whether the row
//! matched **at least one real call site** in the corpus MIR plus the checked-in
//! `.mir` fixtures — and compares the set of rows that did not against
//! `tests/model_unfired_rows.txt`. Two failure directions, both loud:
//!
//! * a row is unfired and **not** listed ⇒ rot. Either the corpus lost the case
//!   that exercised it, or the row's key stopped matching the spelling rustc
//!   now prints. Re-run the corpus before believing the row is fine.
//! * a row is listed and **does** fire ⇒ a stale entry. Delete the line; the
//!   list is a record of what is *not* covered, and an entry that has quietly
//!   started working makes the rest of the list less believable.
//!
//! The list may therefore only shrink on its own. Adding a line is allowed only
//! with a justification in the file saying why the row cannot fire here.
//!
//! Regenerate with `HARVEST_VERIFY_UPDATE_ROWFIRE=1 cargo test -p
//! autumn-harvest-verify --test model_rowfire`.
//!
//! **What this test is not.** "A row fires" means the matcher classified some
//! call with it. It says nothing about whether the resulting verdict was right —
//! that is `corpus.rs`'s job. This is the coverage question only: is the row
//! still connected to reality?

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use autumn_harvest_verify::Model;
use autumn_harvest_verify::driver::{self, BuildRequest};
use autumn_harvest_verify::mir::{self, Terminator};
use autumn_harvest_verify::model::callee::{CalleePath, TypeName};
use autumn_harvest_verify::model::matcher::CallClass;

/// The same five packages `corpus.rs` analyzes, emitted into the same
/// directory, so this test rides on that build instead of paying for a second.
const ALL_CORPUS_PACKAGES: [&str; 5] = [
    "harvest-verify-corpus-seeded",
    "harvest-verify-corpus-clean",
    "harvest-verify-corpus-boundary",
    "harvest-verify-corpus-helpers",
    "harvest-verify-corpus-helpers-deep",
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("autumn-harvest-verify has a parent")
        .to_path_buf()
}

fn ratchet_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("model_unfired_rows.txt")
}

/// Every `.mir` this test reasons over: the corpus emit plus the checked-in
/// fixtures. The fixtures matter because several rows are exercised only by a
/// hand-trimmed dump of real `autumn-harvest` code.
fn documents() -> Vec<mir::MirDoc> {
    let root = workspace_root();
    let build = BuildRequest {
        manifest_path: Some(root.join("Cargo.toml")),
        packages: ALL_CORPUS_PACKAGES
            .iter()
            .map(|p| (*p).to_string())
            .collect(),
        lib: true,
        target_dir: Some(root.join("target/harvest-verify/corpus")),
        ..BuildRequest::default()
    };
    let mut inputs = driver::emit_mir(&build).expect("emit corpus MIR");
    inputs.extend(driver::collect_mir_paths(&[Path::new(env!(
        "CARGO_MANIFEST_DIR"
    ))
    .join("tests")
    .join("fixtures")]));

    let mut docs = Vec::new();
    for input in inputs {
        let text = std::fs::read_to_string(&input.path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", input.path.display()));
        docs.push(mir::parse(
            &input.crate_name,
            &input.path.display().to_string(),
            &text,
        ));
    }
    assert!(!docs.is_empty(), "no MIR to check the model against");
    docs
}

/// `<table>:<path>[@<receiver>]` — the key a ratchet line carries.
fn key(table: &str, path: &str, receiver: Option<&str>) -> String {
    receiver.map_or_else(
        || format!("{table}:{path}"),
        |receiver| format!("{table}:{path}@{receiver}"),
    )
}

/// Every keyed row of the model, as `(key, human description)`.
///
/// `[[trusted]]` is deliberately absent: a trusted-crate row fires on the crate
/// *root* of a path, which the corpus reaches only incidentally, and its purpose
/// is to say "assume clean" rather than to match a specific call.
fn model_rows(model: &Model) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    for rule in &model.source {
        rows.insert(
            key("source", &rule.path, rule.receiver.as_deref()),
            rule.reason.clone(),
        );
    }
    for rule in &model.sink {
        rows.insert(
            key("sink", &rule.path, Some(&rule.receiver)),
            rule.reason.clone(),
        );
    }
    for rule in &model.forbidden {
        rows.insert(
            key("forbidden", &rule.path, rule.receiver.as_deref()),
            rule.reason.clone(),
        );
    }
    for rule in &model.sanitizer {
        rows.insert(
            key("sanitizer", &rule.path, rule.receiver.as_deref()),
            rule.reason.clone(),
        );
    }
    for rule in &model.reduction {
        rows.insert(
            key("reduction", &rule.path, rule.receiver.as_deref()),
            rule.reason.clone(),
        );
    }
    for rule in &model.ambient_type {
        rows.insert(key("ambient_type", &rule.name, None), rule.reason.clone());
    }
    rows
}

/// The keys of the rows that matched at least one call site (or, for
/// `[[ambient_type]]`, at least one declared type) in `docs`.
fn fired_keys(model: &Model, docs: &[mir::MirDoc]) -> BTreeSet<String> {
    let mut fired = BTreeSet::new();
    for doc in docs {
        for body in &doc.bodies {
            // `[[ambient_type]]` rows are matched against declared types, not
            // calls: the analyzer asks `is_ambient_type` of a static's type and
            // of the locals reachable from one.
            for ty in body.locals.values() {
                note_ambient(model, ty, &mut fired);
            }
            for block in &body.blocks {
                let Terminator::Call { callee, dest, .. } = &block.terminator else {
                    continue;
                };
                let Some(callee) = callee else {
                    continue;
                };
                // The analyzer substitutes generics into the callee text before
                // classifying; here the printed text is used as-is, so a row
                // that only matches after substitution can read as unfired.
                // That is a conservative direction: it over-reports, and the
                // ratchet file records the over-report explicitly.
                let parsed = CalleePath::parse(callee);
                let declared = body.locals.get(&dest.local).map(String::as_str);
                for class in model.classify(&parsed, declared) {
                    if let Some(hit) = class_key(&class) {
                        fired.insert(hit);
                    }
                }
            }
        }
        for item in &doc.statics {
            note_ambient(model, &item.ty, &mut fired);
        }
    }
    fired
}

/// Record every `[[ambient_type]]` row that `ty` matches, directly or through
/// one layer of the transparent containers `is_ambient_type` peels.
fn note_ambient(model: &Model, ty: &str, fired: &mut BTreeSet<String>) {
    if !model.is_ambient_type(ty) {
        return;
    }
    let direct = TypeName::parse(ty).name;
    for rule in &model.ambient_type {
        // `is_ambient_type` peels containers before matching, so a match on the
        // outer name and a match on any inner argument are both credible; the
        // row is credited when its name appears as a type name anywhere in the
        // spelling, which is what "this row is why the type is ambient" means.
        if rule.name == direct || ty.contains(&rule.name) {
            fired.insert(key("ambient_type", &rule.name, None));
        }
    }
}

/// The ratchet key of one classification, or `None` for the classes that are
/// not keyed rows (`Trusted`, `UnmodeledCtxMethod`, `Unclassified`, and the
/// `WorkflowContext` tables, which `model_coverage.rs` already pins exhaustively
/// against `context.rs`).
fn class_key(class: &CallClass<'_>) -> Option<String> {
    match class {
        CallClass::Source(rule) => Some(key("source", &rule.path, rule.receiver.as_deref())),
        CallClass::Sink(rule) => Some(key("sink", &rule.path, Some(&rule.receiver))),
        CallClass::Forbidden(rule) => Some(key("forbidden", &rule.path, rule.receiver.as_deref())),
        CallClass::Sanitizer(rule) => Some(key("sanitizer", &rule.path, rule.receiver.as_deref())),
        CallClass::Reduction(rule) => Some(key("reduction", &rule.path, rule.receiver.as_deref())),
        CallClass::Sanctioned(_)
        | CallClass::NonSink(_)
        | CallClass::HandlerRegistration(_)
        | CallClass::Trusted(_)
        | CallClass::UnmodeledCtxMethod(_)
        | CallClass::Unclassified => None,
    }
}

const HEADER: &str = "\
# Model rows with no witness in the corpus MIR or the checked-in .mir fixtures.
#
# Generated by `tests/model_rowfire.rs`; regenerate with
#   HARVEST_VERIFY_UPDATE_ROWFIRE=1 cargo test -p autumn-harvest-verify --test model_rowfire
#
# THIS LIST IS A RATCHET. A line may be REMOVED — that is a row gaining
# coverage, which is the direction this file exists to encourage. A line may be
# ADDED only with a comment above it saying why the row cannot fire on this
# corpus (it models an API the corpus does not use, a platform we do not build,
# a spelling only reachable through generic substitution, ...). Adding a line
# without that comment converts a coverage regression into a silent one, which
# is exactly the failure mode of the rustc 1.94 -> 1.98 `Atomic<T>` incident:
# five rows stopped matching, nothing turned red, and five seeded bugs came back
# `proven-deterministic`.
#
# Format: <table>:<path>[@<receiver>], one per line. Blank lines and `#`
# comments are ignored.
";

/// Read the checked-in list, dropping comments and blanks.
fn read_ratchet() -> BTreeSet<String> {
    let path = ratchet_path();
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}\nGenerate it with \
             HARVEST_VERIFY_UPDATE_ROWFIRE=1 cargo test -p autumn-harvest-verify \
             --test model_rowfire",
            path.display()
        )
    });
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .collect()
}

#[test]
fn every_model_row_either_fires_on_the_corpus_or_is_recorded_as_unfired() {
    let model = Model::builtin().expect("the builtin model parses");
    let docs = documents();
    let rows = model_rows(&model);
    let fired = fired_keys(&model, &docs);

    let unfired: BTreeSet<String> = rows
        .keys()
        .filter(|k| !fired.contains(k.as_str()))
        .cloned()
        .collect();

    // Per-table matrix, always printed: the counts are the number this test
    // exists to make visible, and a reader should not have to fail it to see them.
    let mut per_table: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for row in rows.keys() {
        let table = row.split_once(':').map_or("?", |(t, _)| t);
        let entry = per_table.entry(table).or_insert((0, 0));
        entry.0 += 1;
        if !fired.contains(row.as_str()) {
            entry.1 += 1;
        }
    }
    println!("model row firing over {} .mir document(s):", docs.len());
    for (table, (total, missing)) in &per_table {
        println!(
            "  {table:<13} {:>4} fired / {total:>4} rows  ({missing} unfired)",
            total - missing
        );
    }

    if std::env::var("HARVEST_VERIFY_UPDATE_ROWFIRE").as_deref() == Ok("1") {
        let mut out = String::from(HEADER);
        for row in &unfired {
            out.push_str(row);
            out.push('\n');
        }
        std::fs::write(ratchet_path(), out).expect("write the ratchet file");
        println!(
            "HARVEST_VERIFY_UPDATE_ROWFIRE=1: wrote {} unfired row(s) to {}",
            unfired.len(),
            ratchet_path().display()
        );
        return;
    }

    let recorded = read_ratchet();
    let rot: Vec<&String> = unfired.difference(&recorded).collect();
    let stale: Vec<&String> = recorded.difference(&unfired).collect();

    assert!(
        rot.is_empty(),
        "{} model row(s) match nothing in the corpus MIR and are not recorded as \
         unfired. A row that stops matching is invisible: the analyzer keeps \
         running, the corpus keeps passing, and whatever the row protected is no \
         longer protected — that is exactly how the rustc 1.94 -> 1.98 \
         `Atomic<T>` change cost five detections.\n\
         Check first whether rustc changed how it spells the thing the row is \
         keyed on. If the row genuinely cannot fire here, add it to {} WITH a \
         comment saying why.\n\
         Newly unfired:\n{}",
        rot.len(),
        ratchet_path().display(),
        rot.iter()
            .map(|k| format!("  {k}  — {}", rows.get(*k).map_or("", String::as_str)))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    assert!(
        stale.is_empty(),
        "{} line(s) in {} name rows that now DO fire. Delete them: the file is a \
         record of what is not covered, and entries that have quietly started \
         working make the remaining ones less believable.\n{}",
        stale.len(),
        ratchet_path().display(),
        stale
            .iter()
            .map(|k| format!("  {k}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}

#[test]
fn the_ratchet_file_names_only_rows_that_exist() {
    // A row that is renamed away leaves its ratchet line behind, and that line
    // then suppresses nothing while looking like it suppresses something.
    let model = Model::builtin().expect("the builtin model parses");
    let rows = model_rows(&model);
    let orphans: Vec<String> = read_ratchet()
        .into_iter()
        .filter(|line| !rows.contains_key(line.as_str()))
        .collect();
    assert!(
        orphans.is_empty(),
        "{} line(s) in {} name model rows that no longer exist. Delete them.\n{}",
        orphans.len(),
        ratchet_path().display(),
        orphans
            .iter()
            .map(|k| format!("  {k}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}
