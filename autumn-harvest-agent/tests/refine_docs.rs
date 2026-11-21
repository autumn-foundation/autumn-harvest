//! Guards the docs of the refine loop: the guide section, the ADR, the
//! ADR index, the changelog entry and the CI step of the example.
//!
//! The design is in `docs/design/DESIGN-2383.md`.

use std::path::{Path, PathBuf};

/// The guide that holds the section.
const GUIDE: &str = "docs/agent-adapter.md";

const SECTION: &str = "## 25. Refine an agent";

/// The heading of the section that holds the settings table.
const POLICY_SECTION: &str = "## 18. The refine policy (issue #2394)";

const ADR: &str = "docs/adr/0016-agent-refinement.md";

const ADR_INDEX: &str = "docs/adr/README.md";

/// The command of the guide that runs the example.
const RUN_EXAMPLE: &str =
    "cargo run -p autumn-harvest-agent --features sqlite,eval --example agent_refine";

/// The refine policy settings that shape the loop.
const LOOP_SETTINGS: [&str; 10] = [
    "auto_adopt.parts",
    "auto_adopt.max_per_day",
    "approval_timeout",
    "probe_effects",
    "ceilings.max_steps",
    "ceilings.max_total_tokens",
    "ceilings.max_output_tokens",
    "ceilings.models",
    "proposal_attempts",
    "lessons.max_entries",
];

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

/// The GitHub anchor of each heading in `text`, outside code fences.
///
/// A repeated heading gets the suffix `-1`, `-2` and so on, as GitHub
/// gives it.
fn anchors(text: &str) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let mut bases: Vec<String> = Vec::new();
    let mut fenced = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        let title = line.trim_start_matches('#');
        if fenced || title.len() == line.len() || !title.starts_with(' ') {
            continue;
        }
        let slug: String = title
            .trim()
            .to_lowercase()
            .chars()
            .filter_map(|c| match c {
                ' ' => Some('-'),
                c if c.is_alphanumeric() || c == '-' || c == '_' => Some(c),
                _ => None,
            })
            .collect();
        let repeats = bases.iter().filter(|base| **base == slug).count();
        bases.push(slug.clone());
        found.push(if repeats == 0 {
            slug
        } else {
            format!("{slug}-{repeats}")
        });
    }
    found
}

/// The text of a level-1 heading, without the ADR number.
fn adr_title(text: &str) -> &str {
    let heading = text.lines().next().unwrap_or_default();
    let rest = heading.trim_start_matches("# ADR ");
    let rest = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    rest.trim_start_matches([' ', ':', '-', '\u{2014}'])
}

/// The ADR files of `docs/adr/`, sorted.
fn adr_files() -> Vec<String> {
    let dir = repo_root().join("docs/adr");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("list {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| {
            Path::new(name).extension().is_some_and(|ext| ext == "md") && name != "README.md"
        })
        .collect();
    names.sort();
    names
}

#[test]
fn the_guide_has_the_section() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    for fact in [
        "offline",
        "budget identity",
        "ceilings",
        "probe",
        "`read_only`",
        "adoption gate",
        "lesson library",
        "RefinePolicy",
    ] {
        assert!(text.contains(fact), "{SECTION} must cover `{fact}`");
    }
}

#[test]
fn the_section_links_each_part_and_the_adr() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    for link in [
        "#16-propose-an-overlay-issue-2379",
        "#18-the-refine-policy-issue-2394",
        "#22-refine-a-kind-issue-2380",
        "#23-the-adoption-gate",
        "#24-the-lesson-library",
        "adr/0016-agent-refinement.md",
        "adr/0014-operator-refine-policy.md",
    ] {
        assert!(
            text.contains(&format!("]({link})")),
            "{SECTION} must link `{link}`"
        );
    }
}

#[test]
fn each_link_of_the_section_names_a_real_heading() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    let known = anchors(&guide);
    for link in text.split("](#").skip(1) {
        let anchor = link.split(')').next().unwrap_or_default();
        assert!(
            known.iter().any(|known| known == anchor),
            "{SECTION} links `#{anchor}`, and no heading has that anchor"
        );
    }
}

#[test]
fn the_anchor_scan_skips_fences_and_numbers_repeats() {
    let text = "## A b\n```sh\n## Not a heading\n```\n### A b\n## A b c\n## C (d #1)\n";
    assert_eq!(anchors(text), ["a-b", "a-b-1", "a-b-c", "c-d-1"]);
}

#[test]
fn the_section_names_each_loop_setting() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    for setting in LOOP_SETTINGS {
        assert!(
            text.contains(&format!("`{setting}`")),
            "{SECTION} must name the setting `{setting}`"
        );
    }
    let table = section(&guide, POLICY_SECTION);
    for setting in LOOP_SETTINGS {
        assert!(
            table.contains(&format!("`{setting}`")),
            "{POLICY_SECTION} no longer has `{setting}`; update this guard"
        );
    }
}

#[test]
fn the_section_links_the_settings_table_and_copies_no_table() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    assert!(
        !text.contains("| Setting |"),
        "{SECTION} must link the settings table of {POLICY_SECTION}, not copy it"
    );
}

#[test]
fn the_section_names_the_example_command() {
    let guide = read(GUIDE);
    let text = section(&guide, SECTION);
    assert!(
        text.contains(RUN_EXAMPLE),
        "{SECTION} must show `{RUN_EXAMPLE}`"
    );
    assert!(
        text.contains("examples/agent_refine.rs"),
        "{SECTION} must name the example file"
    );
}

#[test]
fn the_adr_records_the_four_decisions_in_order() {
    let adr = read(ADR);
    assert!(
        adr.starts_with("# ADR 0016: "),
        "{ADR} must open with its number"
    );
    assert!(
        adr.contains("\n## Status\n\nAccepted"),
        "{ADR} must record an accepted decision"
    );
    for heading in ["## Context", "## Decision", "## Consequences"] {
        assert!(
            adr.contains(&format!("\n{heading}\n")),
            "{ADR} lacks `{heading}`"
        );
    }
    let decision = section(&adr, "## Decision");
    let mut last = 0;
    for title in [
        "**Operators decide.**",
        "**Offline, not inline.**",
        "**Overlays stay under operator ceilings.**",
        "**A person adopts, and probes only read.**",
    ] {
        let at = decision
            .find(title)
            .unwrap_or_else(|| panic!("{ADR} `## Decision` lacks {title}"));
        assert!(at > last, "{ADR} must keep {title} in issue order");
        last = at;
    }
    for fact in [
        "safe default",
        "cannot change",
        "narrow",
        "tools",
        "mandate",
        "rules",
        "scripts",
        "plugins",
    ] {
        assert!(
            decision.to_lowercase().contains(fact),
            "{ADR} `## Decision` must state `{fact}`"
        );
    }
    for link in [
        "0014-operator-refine-policy.md",
        "../design/DESIGN-2383.md",
        "../agent-adapter.md#25-refine-an-agent",
    ] {
        assert!(
            adr.contains(&format!("]({link})")),
            "{ADR} must link `{link}`"
        );
    }
}

#[test]
fn the_adr_number_is_unique() {
    let owners: Vec<String> = adr_files()
        .into_iter()
        .filter(|name| name.starts_with("0016-"))
        .collect();
    assert_eq!(
        owners,
        ["0016-agent-refinement.md"],
        "ADR number 0016 must name one file only"
    );
}

#[test]
fn the_adr_index_lists_each_adr_in_file_order() {
    let index = read(ADR_INDEX);
    let mut last = 0;
    for name in adr_files() {
        let at = index
            .find(&format!("]({name})"))
            .unwrap_or_else(|| panic!("{ADR_INDEX} must link `{name}`"));
        assert!(at > last, "{ADR_INDEX} must list `{name}` in file order");
        last = at;
    }
}

#[test]
fn each_row_of_the_adr_index_holds_the_title_of_its_file() {
    let index = read(ADR_INDEX);
    for name in adr_files() {
        let row = index
            .lines()
            .find(|line| line.contains(&format!("]({name})")))
            .unwrap_or_else(|| panic!("{ADR_INDEX} must link `{name}`"));
        let adr = read(&format!("docs/adr/{name}"));
        let title = adr_title(&adr);
        assert!(
            row.ends_with(&format!("| {title} |")),
            "{ADR_INDEX} must hold the title `{title}` of `{name}`: {row}"
        );
    }
}

#[test]
fn each_link_of_the_adr_index_resolves() {
    let index = read(ADR_INDEX);
    let dir = repo_root().join("docs/adr");
    for link in index.split("](").skip(1) {
        let target = link.split(')').next().unwrap_or_default();
        if target.starts_with("http") || target.starts_with('#') {
            continue;
        }
        let path = dir.join(target.split('#').next().unwrap_or_default());
        assert!(
            path.exists(),
            "{ADR_INDEX} links `{target}`, which does not exist"
        );
    }
}

#[test]
fn the_design_readme_links_the_adr_and_the_index() {
    let readme = read("docs/design/README.md");
    for link in ["../adr/0016-agent-refinement.md", "../adr/README.md"] {
        assert!(
            readme.contains(&format!("]({link})")),
            "docs/design/README.md must link `{link}`"
        );
    }
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
            .any(|text| text.contains("autumn-harvest-agent/examples/agent_refine.rs")),
        "a changelog entry must name the example"
    );
}

/// The text of the CI job `name`, up to the next job.
fn job<'a>(ci: &'a str, name: &str) -> &'a str {
    let header = format!("\n  {name}:\n");
    let start = ci
        .find(&header)
        .unwrap_or_else(|| panic!("ci.yml lacks the job `{name}`"))
        + 1;
    let body = &ci[start..];
    let end = body
        .match_indices("\n  ")
        .skip(1)
        .find(|(at, _)| {
            body[at + 3..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_lowercase())
        })
        .map_or(body.len(), |(at, _)| at);
    &body[..end]
}

#[test]
fn ci_runs_the_example() {
    let ci = read(".github/workflows/ci.yml");
    let line = ci
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("run: cargo run -p autumn-harvest-agent "))
        .filter(|line| line.ends_with(" --example agent_refine"))
        .unwrap_or_else(|| panic!("ci.yml must run the example agent_refine"));
    let features = line
        .split("--features ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .unwrap_or_default();
    for feature in ["sqlite", "eval"] {
        assert!(
            features.split(',').any(|f| f == feature),
            "the CI run of the example must enable `{feature}`: {line}"
        );
    }
}

#[test]
fn ci_runs_these_guards_on_docs_only_changes() {
    let ci = read(".github/workflows/ci.yml");
    let snippets = job(&ci, "doc-snippets");
    assert!(
        !snippets.contains("outputs.code"),
        "the job doc-snippets must run on docs-only changes"
    );
    assert!(
        snippets.contains("--test refine_docs"),
        "the job doc-snippets must run these guards"
    );
}
