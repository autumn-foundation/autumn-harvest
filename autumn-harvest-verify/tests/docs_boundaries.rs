//! The verify-crate half of a bidirectional guard on the R&D report's
//! soundness-boundaries table.
//!
//! `Verdict::Unknown` is only honest if the reader can see *what* was not
//! analyzed. The report therefore publishes a boundaries table, and that table
//! has to stay equal to [`BoundaryKind::ALL`] — the list the tool can actually
//! emit. A table that drifts is worse than none: it advertises a boundary set
//! the tool no longer has, so a `proven-deterministic` verdict gets read as
//! stronger than it is.
//!
//! The guard is deliberately split across two crates, because neither can see
//! both ends alone:
//!
//!   * **this test** (in `autumn-harvest-verify`, which owns `BoundaryKind`)
//!     asserts every kebab-case name in `BoundaryKind::ALL` appears in the
//!     report's boundaries table — no boundary is undocumented;
//!   * **`autumn-harvest`'s `determinism_static_analysis_docs`** asserts the
//!     report's table contains exactly a hard-coded list — no row is invented.
//!     It cannot import `BoundaryKind`: `autumn-harvest-verify` dev-depends on
//!     `autumn-harvest`, so the reverse dependency would be a cycle. That test
//!     names this one as the reason its list can be hard-coded.
//!
//! Together they are bidirectional. Alone, either half can be satisfied by a
//! stale table.

use std::path::{Path, PathBuf};

use autumn_harvest_verify::BoundaryKind;

/// `<repo>`, i.e. the parent of `CARGO_MANIFEST_DIR`.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn report_path() -> PathBuf {
    repo_root().join("docs/rnd/determinism-static-analysis.md")
}

/// Read the report with line endings normalised, or fail with the reason the
/// report exists at all.
fn read_report() -> String {
    let path = report_path();
    let body = std::fs::read_to_string(&path).unwrap_or_else(|err| {
        panic!(
            "issue #962 AC1's primary deliverable is missing: could not read {}: {err}\n\
             The feasibility report is what leadership reads to green-light or kill \
             the direction; the prototype is its evidence, not its substitute.",
            path.display()
        )
    });
    body.replace("\r\n", "\n")
}

/// The `## Soundness boundaries` section, from its heading to the next `## `.
fn boundaries_section(report: &str) -> String {
    const HEADING: &str = "\n## Soundness boundaries";
    let start = report.find(HEADING).unwrap_or_else(|| {
        panic!(
            "the report must have a `## Soundness boundaries` section. A verdict of \
             `proven-deterministic` is only honest with its boundary set attached, \
             and the recommended wording is \"no non-determinism found, under model \
             M, up to boundaries B\" — B has to be printed somewhere a reader can \
             find it."
        )
    });
    let rest = &report[start + HEADING.len()..];
    let end = rest.find("\n## ").unwrap_or(rest.len());
    rest[..end].to_string()
}

#[test]
fn every_boundary_kind_is_documented_in_the_report() {
    let report = read_report();
    let section = boundaries_section(&report);

    let missing: Vec<&str> = BoundaryKind::ALL
        .iter()
        .map(|kind| kind.name())
        .filter(|name| !section.contains(*name))
        .collect();

    assert!(
        missing.is_empty(),
        "{} boundary kind(s) the analyzer can emit are absent from the report's \
         `## Soundness boundaries` table. Every one is a case where the tool \
         answers `unknown` and the reader deserves to know why:\n  {}\n\
         (The reverse direction — no invented rows — is guarded by \
         `autumn-harvest`'s `determinism_static_analysis_docs::\
         report_boundary_table_matches_the_code`.)",
        missing.len(),
        missing.join("\n  ")
    );
}

#[test]
fn boundary_names_are_stable_kebab_case() {
    // The report, the CLI's `--list-boundaries` output and the JSON format all
    // print these strings verbatim, so a rename is a breaking change to three
    // surfaces at once.
    for kind in BoundaryKind::ALL {
        let name = kind.name();
        assert!(
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "boundary name {name:?} must be kebab-case ASCII: it is printed \
             verbatim into the report table, the `--list-boundaries` output and \
             the JSON verdict"
        );
        assert!(
            !name.starts_with('-') && !name.ends_with('-'),
            "boundary name {name:?} must not start or end with a dash"
        );
    }

    let mut seen: Vec<&str> = BoundaryKind::ALL.iter().map(|k| k.name()).collect();
    let before = seen.len();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len(),
        before,
        "two `BoundaryKind` variants print the same name, so one of them can \
         never be told apart in a report or a JSON verdict"
    );
}

#[test]
fn report_states_the_verdict_wording_that_carries_the_boundaries() {
    let report = read_report();
    assert!(
        report.contains("under model") && report.contains("boundaries"),
        "the report must state the verdict wording that carries the model version \
         and the boundary set (\"no non-determinism found, under model M, up to \
         boundaries B\"). A bare `proven-deterministic` printed without them is the \
         dishonest form of this tool's output."
    );
}
