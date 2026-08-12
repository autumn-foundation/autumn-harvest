//! Guards that keep the issue-#966 R&D report honest against the code it audits.
//!
//! `docs/rnd/sqlite-feasibility.md` is a leadership decision document whose
//! central claim is an *exhaustive* module-by-module inventory of every
//! Postgres-coupled surface in core — the issue words it as "verifiable
//! against a `grep`-level audit". A hand-written inventory of 40-odd modules
//! is exactly the kind of artefact that is true the day it is written and
//! quietly false one merge later: a new module lands with a `SKIP LOCKED`
//! claim, nobody re-reads the R&D doc, and the "exhaustive" claim silently
//! becomes a lie that still renders perfectly.
//!
//! These guards make the claim falsifiable. They re-derive the coupling
//! inventory from `autumn-harvest/src/*.rs` at test runtime and assert the
//! report covers it, in both directions:
//!
//!   * every coupled module the detector finds is inventoried (no silent gap),
//!   * every module the report inventories actually exists and is actually
//!     coupled (no phantom rows surviving a rename or a decoupling),
//!   * every inventory row carries an explicit (a)/(b)/(c) classification, so
//!     a module cannot be listed and then left unjudged,
//!   * the per-mechanism counts quoted in the report match live greps, and
//!   * the evidence the report cites (the cross-backend replay test, the
//!     `default-features = false` seam) actually exists.
//!
//! The detector below is deliberately *precise* rather than generous: a bare
//! `INTERVAL` token matches Rust constants such as `DEFAULT_WORKER_POLL_INTERVAL`,
//! and a bare `diesel` matches the guardrail lint's prose listing forbidden
//! crates. Both were false positives in the first cut of this audit. Matching
//! `INTERVAL '`/`make_interval(` and `diesel::`/`diesel_async` keeps the
//! inventory signal rather than noise — an inventory padded with false
//! positives would discredit the report just as badly as one with holes.
//!
//! Deliberately *not* covered: the report's prose judgements — the seam
//! sizing, the cost estimates, the go/no-go reasoning. Those are argument, not
//! fact, and a guard that froze them would stop the document being revisable.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR` (`<repo>/autumn-harvest`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn report_path() -> PathBuf {
    repo_root().join("docs/rnd/sqlite-feasibility.md")
}

fn read_report() -> String {
    let path = report_path();
    std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "issue #966 deliverable 1 is missing: could not read {}: {err}\n\
             The R&D report is the primary deliverable of the spike — the success \
             metric requires leadership to green-light or kill the direction from \
             the report alone, without reading the spike code.",
            path.display()
        )
    })
}

/// Every coupling mechanism the issue names, with the precise token that
/// identifies real usage (see the module doc on why these are not bare words).
const MECHANISMS: &[(&str, &[&str])] = &[
    (
        "diesel",
        &["diesel::", "diesel_async", "diesel_migrations", "#[diesel("],
    ),
    ("skip-locked", &["SKIP LOCKED"]),
    ("listen/notify", &["pg_notify", "LISTEN "]),
    ("advisory-lock", &["pg_advisory"]),
    ("to_regclass", &["to_regclass"]),
    ("gen_random_uuid", &["gen_random_uuid"]),
    (
        "interval-sql",
        &["INTERVAL '", "make_interval(", "sql_types::Interval"],
    ),
];

/// Re-derives the Postgres-coupling inventory from the core crate's sources.
///
/// Returns `module name -> sorted set of mechanisms`, for every module that
/// exhibits at least one. This is the ground truth the report is checked
/// against; it is computed fresh on every run so the audit cannot go stale.
fn detect_coupled_modules() -> BTreeMap<String, BTreeSet<&'static str>> {
    let src = repo_root().join("autumn-harvest/src");
    let entries = std::fs::read_dir(&src)
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", src.display()));

    let mut found: BTreeMap<String, BTreeSet<&'static str>> = BTreeMap::new();
    for entry in entries {
        let path = entry.expect("readable dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let module = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("module file stem")
            .to_string();
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()));

        let mut mechanisms = BTreeSet::new();
        for (label, tokens) in MECHANISMS {
            if tokens.iter().any(|token| body.contains(token)) {
                mechanisms.insert(*label);
            }
        }
        if !mechanisms.is_empty() {
            found.insert(module, mechanisms);
        }
    }
    assert!(
        !found.is_empty(),
        "detector found no Postgres-coupled modules at all — the detector is \
         broken (or the source layout moved), not the report"
    );
    found
}

/// The report's inventory rows: Markdown table lines whose first cell is a
/// backticked module name.
///
/// Returns `module name -> whole row`, so callers can assert on the row's
/// other cells (notably the classification).
fn inventory_rows(report: &str) -> BTreeMap<String, String> {
    let section = markdown_section(report, "Postgres-coupling inventory").unwrap_or_else(|| {
        panic!(
            "the report must contain a section headed \"Postgres-coupling inventory\" \
             holding the module-by-module table"
        )
    });

    let mut rows = BTreeMap::new();
    for line in section.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }
        let Some(first_cell) = trimmed.split('|').nth(1) else {
            continue;
        };
        let cell = first_cell.trim();
        // A module cell is exactly a backticked identifier: `queue`.
        let Some(name) = cell.strip_prefix('`').and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            continue;
        }
        rows.insert(name.to_string(), trimmed.to_string());
    }
    rows
}

/// Extracts the body of a `##`/`###` section by heading substring, up to the
/// next heading of the same-or-shallower depth.
fn markdown_section<'a>(document: &'a str, heading_contains: &str) -> Option<&'a str> {
    let mut start: Option<usize> = None;
    let mut depth = 0usize;
    let mut offset = 0usize;
    for line in document.lines() {
        let line_len = line.len() + 1;
        if line.starts_with('#') {
            let this_depth = line.chars().take_while(|c| *c == '#').count();
            match start {
                // A heading at the same or shallower depth closes the section.
                Some(begin) if this_depth <= depth => return Some(&document[begin..offset]),
                None if line.contains(heading_contains) => {
                    start = Some(offset);
                    depth = this_depth;
                }
                // Deeper heading inside the section, or a heading before it starts.
                Some(_) | None => {}
            }
        }
        offset += line_len;
    }
    // Unterminated section: runs to end of document.
    start.map(|begin| &document[begin..])
}

#[test]
fn report_exists_and_is_decision_ready() {
    let report = read_report();
    // The sections leadership needs in order to decide without reading code.
    for heading in [
        "Decision",
        "Postgres-coupling inventory",
        "StorageBackend",
        "Cost to the Postgres path",
        "Test-suite portability",
        "Recommendation",
        "hub",
        "Timebox",
        "embedded backends",
    ] {
        assert!(
            markdown_section(&report, heading).is_some(),
            "issue #966 requires the report to cover {heading:?}, but no heading \
             contains that text.\nHeadings present:\n{}",
            report
                .lines()
                .filter(|l| l.starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
}

#[test]
fn inventory_covers_every_postgres_coupled_core_module() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let missing: Vec<String> = detected
        .iter()
        .filter(|(module, _)| !rows.contains_key(*module))
        .map(|(module, mechanisms)| {
            format!(
                "| `{module}` | {} | (?) | |",
                mechanisms.iter().copied().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();

    assert!(
        missing.is_empty(),
        "the report's Postgres-coupling inventory claims to be exhaustive but \
         omits {} module(s). Issue #966 requires an inventory \"verifiable against \
         a grep-level audit\".\nAdd these rows (classification still to fill in):\n{}",
        missing.len(),
        missing.join("\n")
    );
}

#[test]
fn inventory_lists_no_module_that_is_not_actually_coupled() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let phantom: Vec<&String> = rows
        .keys()
        .filter(|module| !detected.contains_key(*module))
        .collect();

    assert!(
        phantom.is_empty(),
        "the inventory lists {} module(s) that no longer exist or are no longer \
         Postgres-coupled: {phantom:?}\nA stale row is as misleading as a missing \
         one — delete them, or the audit overstates the coupling.",
        phantom.len()
    );
}

#[test]
fn every_inventoried_module_carries_an_abc_classification() {
    let report = read_report();
    let rows = inventory_rows(&report);
    assert!(
        !rows.is_empty(),
        "the inventory table parsed to zero rows — the table format changed and \
         these guards are no longer reading it"
    );

    let unjudged: Vec<&String> = rows
        .iter()
        .filter(|(_, row)| !(row.contains("(a)") || row.contains("(b)") || row.contains("(c)")))
        .map(|(module, _)| module)
        .collect();

    assert!(
        unjudged.is_empty(),
        "issue #966 requires each coupled surface to be classified (a) trivially \
         trait-able, (b) needs a semantic substitute, or (c) fundamentally \
         Postgres-shaped. These rows carry no classification: {unjudged:?}"
    );
}

#[test]
fn every_inventoried_module_records_the_mechanisms_grep_finds() {
    let report = read_report();
    let detected = detect_coupled_modules();
    let rows = inventory_rows(&report);

    let mut wrong = Vec::new();
    for (module, mechanisms) in &detected {
        let Some(row) = rows.get(module) else {
            continue; // covered by the exhaustiveness guard
        };
        let absent: Vec<&str> = mechanisms
            .iter()
            .copied()
            .filter(|m| !row.contains(m))
            .collect();
        if !absent.is_empty() {
            wrong.push(format!("`{module}` does not record {absent:?}"));
        }
    }

    assert!(
        wrong.is_empty(),
        "inventory rows must name every mechanism the audit detects for that \
         module, so a reader can see *why* it is coupled:\n{}",
        wrong.join("\n")
    );
}

#[test]
fn mechanism_counts_quoted_in_the_report_are_current() {
    let report = read_report();
    let detected = detect_coupled_modules();

    // Per-mechanism module counts, recomputed live. The report publishes these
    // as `<mechanism> ... <n> modules`; a stale number here is the classic
    // way an audit rots without anyone noticing.
    for (label, _) in MECHANISMS {
        let count = detected.values().filter(|ms| ms.contains(label)).count();
        // Singular/plural so the guard never forces ungrammatical prose ("1 modules").
        let needle = if count == 1 {
            "1 module".to_string()
        } else {
            format!("{count} modules")
        };
        let section = markdown_section(&report, "Coupling mechanisms")
            .unwrap_or_else(|| panic!("the report must contain a \"Coupling mechanisms\" section"));
        let line = section
            .lines()
            .find(|l| l.contains(label))
            .unwrap_or_else(|| panic!("no line in \"Coupling mechanisms\" mentions {label:?}"));
        assert!(
            line.contains(&needle),
            "the report says of {label:?}:\n  {line}\nbut a live audit finds \
             {count} modules. Update the count (expected the row to contain \
             {needle:?})."
        );
    }

    let total = detected.len();
    assert!(
        report.contains(&format!("{total} of the")),
        "the report should state the headline coupling figure as \"{total} of the \
         N modules\"; a live audit currently finds {total} coupled modules"
    );
}

#[test]
fn cited_cross_backend_replay_evidence_actually_exists() {
    let report = read_report();
    let cross_backend =
        repo_root().join("autumn-harvest-sqlite/tests/integration/cross_backend.rs");
    let body = std::fs::read_to_string(&cross_backend).unwrap_or_else(|err| {
        panic!(
            "issue #966 requires one cross-backend replay test proving event-log \
             portability; cannot read {}: {err}",
            cross_backend.display()
        )
    });

    // The report must cite a test by name, and that test must exist. Citing
    // evidence that does not exist is the failure mode this guards.
    assert!(
        report.contains("sqlite_history_replays_on_core_replayer"),
        "the report must cite the cross-backend replay test by name so a reader \
         can verify the portability invariant themselves"
    );
    assert!(
        body.contains("fn sqlite_history_replays_on_core_replayer"),
        "the report cites `sqlite_history_replays_on_core_replayer`, but no such \
         test exists in {}",
        cross_backend.display()
    );
}

#[test]
fn zero_regression_to_the_postgres_path_is_structurally_true() {
    // The spike's first invariant: saying "no" stays free, and the Postgres
    // build is untouched. Both halves are checkable from the manifests rather
    // than asserted in prose.
    let core_manifest = std::fs::read_to_string(repo_root().join("autumn-harvest/Cargo.toml"))
        .expect("core manifest readable");
    assert!(
        !core_manifest.contains("rusqlite"),
        "core must not depend on rusqlite — the SQLite backend is a downstream \
         companion crate, never a core dependency"
    );
    assert!(
        !core_manifest.contains("autumn-harvest-sqlite"),
        "core must not depend on the SQLite crate; the dependency edge points \
         the other way"
    );

    let sqlite_manifest =
        std::fs::read_to_string(repo_root().join("autumn-harvest-sqlite/Cargo.toml"))
            .expect("sqlite manifest readable");
    assert!(
        sqlite_manifest.contains("default-features = false"),
        "the SQLite crate must depend on core with `default-features = false` so \
         it pulls the determinism core without Diesel/Postgres"
    );
}
