//! Guards the docs of the inference strategies: the guide section, the
//! ADR, both indexes, the changelog entry and the CI steps of the example.
//!
//! The design is in `docs/design/DESIGN-2450.md`.

use std::path::{Path, PathBuf};

/// The guide that holds the section.
const GUIDE: &str = "docs/agent-adapter.md";

/// The heading of the section.
const SECTION: &str = "## 28. Inference strategies";

/// The ADR of the strategies.
const ADR: &str = "docs/adr/0017-inference-strategies.md";

/// The command that runs the example.
const RUN_EXAMPLE: &str = "cargo run -p agent-strategies";

/// The strategies that the section table lists, by policy name.
const STRATEGIES: [&str; 6] = [
    "single",
    "best_of_n",
    "rejection",
    "vote",
    "cascade",
    "beam",
];

/// The names that the example prints, one per strategy that it runs.
const EXAMPLE_NAMES: [&str; 4] = ["single draw", "best-of-3", "vote", "cascade"];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// The text of the level-2 section that opens with `heading`.
fn section<'a>(text: &'a str, heading: &str) -> &'a str {
    let start = text
        .find(&format!("\n{heading}\n"))
        .unwrap_or_else(|| panic!("the text lacks `{heading}`"))
        + 1;
    let body = &text[start..];
    let end = body[heading.len()..]
        .find("\n## ")
        .map_or(body.len(), |at| at + heading.len());
    &body[..end]
}

/// The table rows of `text` whose first cell is `` `name` ``.
fn row<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let cell = format!("| `{name}` |");
    text.lines().find(|line| line.starts_with(&cell))
}

#[test]
fn the_section_names_the_example_command() {
    let guide = read(GUIDE);
    let body = section(&guide, SECTION);
    assert!(
        body.contains(RUN_EXAMPLE),
        "the section must name the example"
    );
}

#[test]
fn the_table_has_a_row_with_four_cells_for_each_strategy() {
    let guide = read(GUIDE);
    let body = section(&guide, SECTION);
    assert!(body.contains("| Strategy | Draws | It pays off when | Policy setting |"));
    for name in STRATEGIES {
        let line = row(body, name).unwrap_or_else(|| panic!("no table row for `{name}`"));
        assert_eq!(
            line.matches(" | ").count(),
            3,
            "four cells for `{name}`: {line}"
        );
        if name != "single" {
            assert!(
                line.contains(&format!("`{name}` in `strategies`")),
                "the row names the policy setting of `{name}`: {line}"
            );
        }
    }
}

#[test]
fn the_section_links_the_adr_and_the_design() {
    let guide = read(GUIDE);
    let body = section(&guide, SECTION);
    assert!(body.contains("(adr/0017-inference-strategies.md)"));
    assert!(body.contains("(design/DESIGN-2450.md)"));
    assert!(body.contains("(#19-sampling-policy-issue-2439)"));
}

#[test]
fn the_adr_records_the_four_decisions_in_order() {
    let adr = read(ADR);
    let decision = section(&adr, "## Decision");
    let marks = [
        "1. **Strategies are workflows over child runs.**",
        "2. **Scores come from verifiers.**",
        "3. **A draw that the run does not select only reads.**",
        "4. **What is not adopted.**",
    ];
    let mut from = 0;
    for mark in marks {
        let at = decision[from..]
            .find(mark)
            .unwrap_or_else(|| panic!("the ADR lacks `{mark}` in order"));
        from += at + mark.len();
    }
    assert!(decision.contains("log-probabilities"));
    assert!(decision.contains("`ReadOnly`"));
}

#[test]
fn both_indexes_name_the_adr() {
    let index = read("docs/adr/README.md");
    assert!(
        index.contains(
            "| [0017](0017-inference-strategies.md) | Inference strategies are workflows over durable child runs |"
        ),
        "the ADR index must list ADR 0017"
    );
    let design = read("docs/design/README.md");
    assert!(design.contains("(../adr/0017-inference-strategies.md)"));
}

#[test]
fn a_changelog_entry_names_the_example() {
    let dir = repo_root().join("docs/changelog.d");
    let mut texts: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("list {}: {e}", dir.display()))
        .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path()).ok())
        .collect();
    // A collation moves each fragment into these two files.
    texts.push(read("CHANGELOG.md"));
    texts.push(read("docs/shipped-work.md"));
    assert!(
        texts
            .iter()
            .any(|text| text.contains("examples/agent-strategies")),
        "a changelog entry must name the example"
    );
}

#[test]
fn ci_runs_the_example_and_checks_each_strategy_name() {
    let ci = read(".github/workflows/ci.yml");
    assert!(
        ci.contains("cargo test -p agent-strategies"),
        "ci.yml must run the example tests"
    );
    let start = ci
        .find(RUN_EXAMPLE)
        .unwrap_or_else(|| panic!("ci.yml must run `{RUN_EXAMPLE}`"));
    let step = &ci[start..];
    let step = &step[..step.find("\n      - ").unwrap_or(step.len())];
    for name in EXAMPLE_NAMES {
        assert!(
            step.contains(&format!("grep -qF \"{name}: answer\"")),
            "the CI step must check the output for `{name}`"
        );
    }
}

#[test]
fn ci_runs_these_guards_on_docs_only_changes() {
    let ci = read(".github/workflows/ci.yml");
    assert!(
        ci.contains("--test strategies_docs"),
        "the job doc-snippets must run these guards"
    );
}

#[test]
fn the_mesh_and_adapter_sections_describe_sample() {
    let mesh = read("docs/agent-mesh.md");
    let durable = section(&mesh, "## 13. Durable peers (issue #2269)");
    for needle in [
        "\"sample\"",
        "`max_draws`",
        "`draws_exceeded`",
        "`fan_out_exhausted`",
        "`strategy_needs_durable_peer`",
    ] {
        assert!(
            durable.contains(needle),
            "mesh section 13 must name {needle}"
        );
    }
    let guide = read(GUIDE);
    let agents = section(&guide, "## 15. Durable agents");
    assert!(agents.contains("### A strategy through `delegate`"));
    assert!(agents.contains("`strategy_needs_durable_peer`"));
    let design = read("docs/design/DESIGN-2449.md");
    assert!(
        !design.contains("cannot ask for a strategy"),
        "the limit of DESIGN-2449 is gone"
    );
}
