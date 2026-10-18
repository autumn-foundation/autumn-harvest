//! Guards for the OpenTelemetry workflow conventions proposal (issue #2015).
//!
//! `docs/otel-workflow-semconv/` holds a draft proposal and its Weaver model.
//! The draft generalizes ADR 0001 for upstream. These guards keep it true to
//! the ADR, to the `ATTR_*` constants and to the upstream model shape:
//!
//! * Each ADR 0001 span maps to its own proposed span. A changed kind is noted.
//! * Each `ATTR_*` constant maps to a proposed attribute or has a reason.
//! * The model holds no Harvest name, and the page tables match it.
//! * The replay rule survives, and the submission status is recorded.
//! * Each prose sentence and each YAML text has 25 words or fewer.
//! * A pinned script runs the upstream Weaver policy checks on the model.
//!   A workflow runs it, and the page records its pin.
//! * The filing follows the upstream process. Section 9 fits the issue form.
//!   A project proposal fills the community template. Each link in the
//!   issue text is a checked fact.
//!
//! The suite is pure: no database, no async, no feature gate. It runs on
//! every OS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde_yaml::Value;

use super::docs_guard_support::{check_stanza, issue_refs, prose_sentences, section, step_stanza};

const PAGE: &str = "docs/otel-workflow-semconv/README.md";
const REGISTRY: &str = "docs/otel-workflow-semconv/model/workflow/registry.yaml";
const SPANS: &str = "docs/otel-workflow-semconv/model/workflow/spans.yaml";
const ADR: &str = "docs/adr/0001-otel-trace-contract.md";
const TELEMETRY_RS: &str = "autumn-harvest/src/telemetry.rs";
const TELEMETRY_PAGE: &str = "docs/telemetry.md";
const FILTER: &str = "--test integration otel_workflow_semconv_docs::";
const WEAVER_SCRIPT: &str = "scripts/check-otel-workflow-semconv.sh";
const WEAVER_WORKFLOW: &str = ".github/workflows/otel-semconv.yml";
const MODEL_PATHS: &str = "docs/otel-workflow-semconv/**";
const MAX_SENTENCE_WORDS: usize = 25;

/// Attributes that a span may reference from the general upstream registry.
const GENERAL_ATTRIBUTES: &[&str] = &["error.type"];

/// The span kinds that upstream accepts.
const SPAN_KINDS: &[&str] = &["internal", "producer", "consumer", "client", "server"];

/// Attributes that every span must require.
const ALWAYS_REQUIRED: &[&str] = &["workflow.system.name", "workflow.operation.name"];

/// Phrases that each span note must hold: its start and end boundaries, and
/// its status on error.
const NOTE_PHRASES: &[&str] = &[
    "The span starts when",
    "It ends when",
    "the span status SHOULD be `Error`",
];

/// The only condition under which a span may make the run id optional.
const RUN_ID_CONDITION: &str = "If the run id is available before the span ends.";

/// The condition under which every span must carry `error.type`.
const ERROR_CONDITION: &str = "If the operation ends in an error.";

/// The page sections that the guards read.
const MAPPING_SECTION: &str = "## 7. Mapping from Harvest";
const ATTRIBUTES_SECTION: &str = "## 4. Attributes";
const SPANS_SECTION: &str = "## 5. Spans";
const REPLAY_SECTION: &str = "## 6. Replay";
const ISSUE_TEXT_SECTION: &str = "## 9. Upstream issue text";
const SUBMISSION_SECTION: &str = "## 10. Submission";
const STATUS_PREFIX: &str = "**Submission status:**";
const SCOPE_SECTION: &str = "## 3. Scope and prior art";
const FACTS_HEADING: &str = "### Facts checked";
const DECLINE_PREFIX: &str = "If the project decides not to submit";

/// The draft community project proposal.
const PROPOSAL: &str = "docs/otel-workflow-semconv/project-proposal.md";

/// The upstream issue form for new conventions, its area for a new area and
/// its text field.
const ISSUE_FORM: &str = "new-conventions.yaml";
const ISSUE_AREA: &str = "`area:new`";
const ISSUE_FIELD: &str = "Propose new conventions";

/// The sections of the community `project-template.md` that are not
/// "Optional" or "Post-Approval". The script checks the proposal against
/// the template itself.
const PROJECT_SECTIONS: &[&str] = &[
    "## Background and description",
    "### Current challenges",
    "### Goals, objectives, and requirements",
    "## Deliverables",
    "## Staffing / Help Wanted",
    "### SIG",
    "### Required staffing",
    "#### Project Leads(s)",
    "#### Other Staffing",
    "### Sponsorship",
    "#### TC Sponsor",
    "#### GC Liaison",
    "## Expected Timeline",
];

/// The proposal sections that name a person. Each one links a GitHub handle,
/// `[@name](…)`, or starts with `Open.`
const STAFFING_SLOTS: &[&str] = &[
    "#### Project Leads(s)",
    "#### TC Sponsor",
    "#### GC Liaison",
];

/// The open upstream issue that claims the same `workflow.*` namespace.
const PRIOR_ISSUE: &str = "open-telemetry/semantic-conventions#1688";

/// Links into this repository. They are not external facts.
const OWN_REPO: &str = "https://github.com/autumn-foundation/autumn-harvest/";

/// The phrase that precedes each copy of the community pin in the docs.
const COMMUNITY_PIN_PREFIX: &str = "community commit `";

/// Code lines of the Weaver script that must appear exactly, with the reason.
const WEAVER_SCRIPT_LINES: &[(&str, &str)] = &[
    ("set -euo pipefail", "a failed command must fail the script"),
    (
        r#"*@sha256:*) image="${registry}/${image}" ;;"#,
        "the script must refuse an image with no digest",
    ),
    (r#"place "${model}""#, "run 1 must check this model"),
    (r#"place "${broken}""#, "run 2 must check the broken copy"),
    (
        r#"if run_weaver >"${work}/broken.log" 2>&1; then"#,
        "the broken copy must fail, so a pass is not vacuous",
    ),
    (
        r#"grep -qxE "[[:space:]]*${line}[[:space:]]*" "${form}" || {"#,
        "the form must hold each line of section 9 as a whole line",
    ),
    (
        r#"[ -r "${proposal}" ] || {"#,
        "a missing proposal must fail the script",
    ),
    (
        r#"grep -vE '\((Optional|Post-Approval)\)$' || true)""#,
        "a required section is a heading that is not Optional or Post-Approval",
    ),
    (
        r#"grep -vxF -f <(printf '%s\n' "${clean}") <<<"${required}" || [ $? -eq 1 ]"#,
        "each required heading must be a whole line of the proposal",
    ),
    (
        r#"grep -xF -f <(printf '%s\n' "${guidance}") <<<"${clean}" || [ $? -eq 1 ]"#,
        "the proposal must replace the guidance text of the template",
    ),
    (
        r#"missing="$(missing_sections "${proposal}")""#,
        "the script must check this proposal",
    ),
    (
        r#"if [ -z "$(missing_sections "${work}/proposal-broken.md")" ]; then"#,
        "a proposal without a required section must fail, so a pass is not vacuous",
    ),
];

/// Text that some code line of the Weaver script must hold, with the reason.
const WEAVER_SCRIPT_PARTS: &[(&str, &str)] = &[
    (
        r#"clone_at "${semconv_repo}" "${semconv_commit}""#,
        "the script must check the pinned upstream commit",
    ),
    (
        r#"clone_at "${semconv_repo}" "refs/tags/${baseline_tag}""#,
        "the baseline must be the pinned release",
    ),
    (
        "dependencies.Dockerfile",
        "the Weaver image comes from the upstream pin",
    ),
    (
        "^WEAVER_PACKAGES_REPO=",
        "the policy packages come from the upstream pin",
    ),
    (
        "=~ ^[0-9a-f]{40}$",
        "the policy packages must be pinned to a commit",
    ),
    ("--network none", "the container must use local mounts only"),
    (
        "registry check --v2",
        "the flags must match `make check-policies`",
    ),
    (
        "--baseline-registry=/home/weaver/baseline",
        "the flags must match `make check-policies`",
    ),
    ("policies/check/naming_conventions", "upstream checks names"),
    ("policies/check/stability", "upstream checks stability"),
    (
        "policies/check/entity_associations",
        "upstream checks entities",
    ),
    (
        "policies/check/backwards-compatibility",
        "upstream checks compatibility",
    ),
    (
        r#"model="${root}/docs/otel-workflow-semconv/model/workflow""#,
        "the script must check this model",
    ),
    (
        r#"cp -R "$1" "${semconv}/model/workflow""#,
        "the model must sit at its upstream path",
    ),
    (
        "weaver/source/workflow/",
        "run 1 must fail on a warning for the model",
    ),
    (
        "grep -qF 'naming_convention_attribute_namespace_collision'",
        "the broken copy must fail on a package policy, not another error",
    ),
    (
        r#"clone_at "${community_repo}" "${community_commit}""#,
        "the script must read the pinned community tree",
    ),
    (
        r#"community_commit="$(git ls-remote "${community_repo}" refs/heads/main"#,
        "`SEMCONV_LATEST=1` must also read community `main`",
    ),
    (
        r#"proposal="${root}/docs/otel-workflow-semconv/project-proposal.md""#,
        "the script must check this proposal",
    ),
    (
        ".github/ISSUE_TEMPLATE/new-conventions.yaml",
        "section 9 follows this upstream form",
    ),
    ("- area:new", "the form must offer the area of section 9"),
    (
        "label: Propose new conventions",
        "the form must have the field of section 9",
    ),
    (
        "project-template.md",
        "the proposal follows the community template",
    ),
];

/// The job `if` that skips a draft pull request, and nothing else.
const DRAFT_SKIP: &str =
    "github.event_name != 'pull_request' || github.event.pull_request.draft == false";

/// The replay rules that section 6 must state. Each rule is a set of
/// phrases that one numbered item must hold.
const REPLAY_RULES: &[&[&str]] = &[
    &["MUST set `workflow.replay`"],
    &[
        "MUST NOT emit a span for an operation that a replay cycle reads from history",
        "The first live application of an operation still gets its span",
        "`execute_activity`",
        "`schedule_activity`",
        "`start_child_workflow`",
    ],
    &["SHOULD start a new root span"],
    &["SHOULD add a span link"],
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
        .unwrap_or_else(|err| panic!("cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

fn page_section<'a>(page: &'a str, heading: &str) -> &'a str {
    section(page, heading, "\n## ").unwrap_or_else(|| panic!("{PAGE} must have `{heading}`"))
}

fn yaml(relative: &str) -> Value {
    serde_yaml::from_str(&read(relative))
        .unwrap_or_else(|err| panic!("{relative} is not valid YAML: {err}"))
}

/// The `groups` list of a Weaver model file.
fn groups(model: &Value, file: &str) -> Vec<Value> {
    model
        .get("groups")
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{file} must have a `groups` list"))
        .clone()
}

fn str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn sequence(value: &Value, key: &str) -> Vec<Value> {
    value
        .get(key)
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default()
}

/// Collect `(key, value)` pairs into a map. A repeated key fails the guard.
fn unique<V>(what: &str, pairs: impl IntoIterator<Item = (String, V)>) -> BTreeMap<String, V> {
    let mut map = BTreeMap::new();
    for (key, value) in pairs {
        assert!(
            map.insert(key.clone(), value).is_none(),
            "{what} lists `{key}` twice"
        );
    }
    map
}

/// The value of the `id` key, or a failed guard.
fn id_of(value: &Value, file: &str) -> String {
    str_field(value, "id")
        .unwrap_or_else(|| panic!("each entry in {file} needs an `id`"))
        .to_owned()
}

/// Every attribute of the registry, by id.
fn registry_attributes() -> BTreeMap<String, Value> {
    let attributes = groups(&yaml(REGISTRY), REGISTRY)
        .into_iter()
        .flat_map(|group| sequence(&group, "attributes"))
        .map(|attribute| (id_of(&attribute, REGISTRY), attribute));
    unique(REGISTRY, attributes)
}

/// Every span group, by id.
fn span_groups() -> BTreeMap<String, Value> {
    let spans = groups(&yaml(SPANS), SPANS)
        .into_iter()
        .map(|group| (id_of(&group, SPANS), group));
    unique(SPANS, spans)
}

/// The `ref` ids of a span group, each with its requirement level.
///
/// Weaver defaults a missing level to `recommended`. An entry with no `ref`
/// fails the guard, so an inline attribute cannot skip the registry.
fn span_refs(group: &Value) -> BTreeMap<String, Value> {
    let refs = sequence(group, "attributes").into_iter().map(|attribute| {
        let id = str_field(&attribute, "ref")
            .unwrap_or_else(|| panic!("each span attribute in {SPANS} must be a `ref`"))
            .to_owned();
        let level = attribute
            .get("requirement_level")
            .cloned()
            .unwrap_or_else(|| Value::from("recommended"));
        (id, level)
    });
    unique(SPANS, refs)
}

/// The `note` on the `ref` entry for `attribute` in a span group.
fn ref_note(group: &Value, attribute: &str) -> Option<String> {
    sequence(group, "attributes")
        .iter()
        .find(|entry| str_field(entry, "ref") == Some(attribute))
        .and_then(|entry| str_field(entry, "note").map(str::to_owned))
}

/// The type of a registry attribute: a scalar name, or `enum`.
fn type_name(attribute: &Value) -> &str {
    match attribute.get("type") {
        Some(Value::String(name)) => name,
        Some(kind) if kind.get("members").is_some() => "enum",
        _ => "",
    }
}

/// The trimmed cells of a Markdown table row, or `None`.
fn cells(line: &str) -> Option<Vec<&str>> {
    let row = line.trim().strip_prefix('|')?.strip_suffix('|')?;
    Some(row.split('|').map(str::trim).collect())
}

/// The text of a cell that is one inline code span, or `None`.
fn code(cell: &str) -> Option<&str> {
    cell.strip_prefix('`')?.strip_suffix('`')
}

/// The rows of each table in `text` whose first cell is a code span with
/// `prefix`, by that name, with the remaining cells.
fn table_rows(text: &str, prefix: &str) -> BTreeMap<String, Vec<String>> {
    let rows = text
        .lines()
        .filter_map(cells)
        .filter_map(|row| {
            let name = code(row.first()?)?;
            let rest = row[1..].iter().map(|cell| (*cell).to_owned()).collect();
            Some((name.to_owned(), rest))
        })
        .filter(|(name, _)| name.starts_with(prefix));
    unique(PAGE, rows)
}

/// The mapping table: each Harvest name with its target cell and its note.
fn mapping(page: &str) -> BTreeMap<String, (String, String)> {
    table_rows(page_section(page, MAPPING_SECTION), "harvest.")
        .into_iter()
        .map(|(name, mut rest)| {
            rest.resize(2, String::new());
            let note = rest.pop().unwrap_or_default();
            let target = rest.pop().unwrap_or_default();
            (name, (target, note))
        })
        .collect()
}

/// Each ADR 0001 §2 span name with its kind, in lower case.
fn adr_spans(adr: &str) -> BTreeMap<String, String> {
    let catalogue = section(adr, "### 2. Span catalogue", "\n### ")
        .unwrap_or_else(|| panic!("{ADR} must have `### 2. Span catalogue`"));
    let spans = catalogue.split("\n#### 2.").skip(1).map(|entry| {
        let name = entry
            .split('`')
            .nth(1)
            .unwrap_or_else(|| panic!("no span name in ADR entry: {entry}"));
        let kind = entry
            .lines()
            .filter_map(cells)
            .find(|row| row.first() == Some(&"Kind"))
            .and_then(|row| code(row.get(1)?))
            .unwrap_or_else(|| panic!("ADR span {name} has no `Kind` row"));
        (name.to_owned(), kind.to_ascii_lowercase())
    });
    unique(ADR, spans)
}

/// The value of each `pub const ATTR_*: &str` in `source`.
///
/// A declaration can wrap after `=`, so the value is read up to the `;`.
fn attr_constants(source: &str) -> BTreeSet<String> {
    source
        .split("pub const ATTR_")
        .skip(1)
        .map(|declaration| {
            let declaration = declaration.split(';').next().unwrap_or_default();
            declaration
                .split('"')
                .nth(1)
                .unwrap_or_else(|| panic!("no string value in `pub const ATTR_{declaration}`"))
                .to_owned()
        })
        .collect()
}

/// Upstream attribute and group ids: lower-case dot-separated snake case.
fn is_well_named(id: &str) -> bool {
    id.split('.').count() >= 2
        && id.split('.').all(|part| {
            part.starts_with(|c: char| c.is_ascii_lowercase())
                && part
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        })
}

/// A Weaver requirement level: a plain level, or a level with a condition.
fn is_requirement_level(level: &Value) -> bool {
    if let Some(plain) = level.as_str() {
        return matches!(plain, "required" | "recommended" | "opt_in");
    }
    level.get("conditionally_required").is_some() || level.get("recommended").is_some()
}

/// The items of each numbered list in `text`, each joined into one line.
///
/// An item starts with `N. ` and runs to the next item or a blank line.
fn numbered_items(text: &str) -> Vec<String> {
    let mut items: Vec<String> = Vec::new();
    let mut open = false;
    for line in text.lines().map(str::trim) {
        let number = line
            .split_once(". ")
            .filter(|(n, _)| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
        if let Some((_, body)) = number {
            items.push(body.to_owned());
            open = true;
        } else if line.is_empty() {
            open = false;
        } else if open && let Some(item) = items.last_mut() {
            item.push(' ');
            item.push_str(line);
        }
    }
    items
}

/// The paragraph that starts with `prefix`, joined into one line.
fn paragraph(text: &str, prefix: &str) -> Option<String> {
    let start = text.find(prefix)? + prefix.len();
    let body = &text[start..];
    let end = body.find("\n\n").unwrap_or(body.len());
    Some(body[..end].split_whitespace().collect::<Vec<_>>().join(" "))
}

/// The body of the first ```` ```markdown ```` fence in `text`.
fn markdown_fence(text: &str) -> Option<&str> {
    let start = text.find("```markdown\n")? + "```markdown\n".len();
    let end = text[start..].find("\n```")? + start;
    Some(&text[start..end])
}

/// Whether `url` points at one upstream issue or pull request.
fn is_upstream_item(text: &str) -> bool {
    text.split("https://github.com/open-telemetry/")
        .skip(1)
        .any(|rest| {
            let mut parts = rest.split('/').skip(1);
            matches!(parts.next(), Some("issues" | "pull"))
                && parts
                    .next()
                    .is_some_and(|number| number.starts_with(|c: char| c.is_ascii_digit()))
        })
}

/// The value of the shell assignment `name="value"` at the start of a line.
fn pin<'a>(script: &'a str, name: &str) -> Option<&'a str> {
    script.lines().find_map(|line| {
        line.strip_prefix(name)?
            .strip_prefix("=\"")?
            .split('"')
            .next()
    })
}

/// The full upstream commit SHA that the Weaver script pins.
fn pinned_commit(script: &str) -> Option<&str> {
    pinned_sha(script, "semconv_commit")
}

/// The full commit SHA that the script assigns to `name`.
fn pinned_sha<'a>(script: &'a str, name: &str) -> Option<&'a str> {
    pin(script, name).filter(|sha| {
        sha.len() == 40
            && sha
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// The external links in `text`: each URL and each `owner/repo#N` ref.
///
/// A URL may sit in a Markdown link, in angle brackets or in emphasis. A URL
/// that holds a `<` placeholder is not a link yet, and a link into this
/// repository is not external, so both are skipped.
fn external_refs(text: &str) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    for token in text.split_whitespace() {
        let at = token.find("https://").or_else(|| token.find("http://"));
        if let Some(at) = at {
            let url = &token[at..];
            let end = url
                .find([')', '>', ']', '*', '"', '\'', '`'])
                .unwrap_or(url.len());
            let url = url[..end].trim_end_matches(['.', ',', ';', ':']);
            if !url.contains('<') && !url.starts_with(OWN_REPO) {
                refs.insert(url.to_owned());
            }
            continue;
        }
        let item = token
            .trim_start_matches(['(', '[', '`', '*'])
            .trim_end_matches(['.', ',', ';', ':', ')', ']', '`', '*']);
        let is_item = item.split_once('#').is_some_and(|(repo, number)| {
            repo.split('/').count() == 2
                && !repo.starts_with('/')
                && !repo.ends_with('/')
                && !number.is_empty()
                && number.bytes().all(|b| b.is_ascii_digit())
        });
        if is_item {
            refs.insert(item.to_owned());
        }
    }
    refs
}

/// Whether `text` is a date in the form `YYYY-MM-DD`.
fn is_iso_date(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(at, b)| match at {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
}

/// The text under the heading line `heading`, up to the next heading.
fn heading_body<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    let start = doc
        .lines()
        .position(|line| line.trim_end() == heading)
        .map(|line| {
            doc.lines()
                .take(line + 1)
                .map(|l| l.len() + 1)
                .sum::<usize>()
        })?;
    let body = doc.get(start..).unwrap_or_default();
    let end = body
        .lines()
        .scan(0, |offset, line| {
            let at = *offset;
            *offset += line.len() + 1;
            Some((at, line))
        })
        .find(|(_, line)| line.starts_with('#'))
        .map_or(body.len(), |(at, _)| at);
    Some(&body[..end])
}

/// The next heading line after the heading line `heading`.
fn next_heading<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    doc.lines()
        .skip_while(|line| line.trim_end() != heading)
        .skip(1)
        .find(|line| line.starts_with('#'))
        .map(str::trim_end)
}

/// The number of `#` marks that start `heading`.
fn heading_level(heading: &str) -> usize {
    heading.bytes().take_while(|b| *b == b'#').count()
}

/// Whether a table cell is part of a separator row, such as `---` or `:--`.
fn is_separator(cell: &str) -> bool {
    !cell.is_empty() && cell.chars().all(|c| matches!(c, '-' | ':'))
}

/// The trimmed lines of a shell script that are not comments.
fn code_lines(script: &str) -> Vec<&str> {
    script
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

/// The string items under `key` of `value`.
fn strings(value: &Value, key: &str) -> Vec<String> {
    sequence(value, key)
        .iter()
        .filter_map(|item| item.as_str().map(str::to_owned))
        .collect()
}

/// Every YAML text that a reader sees: briefs, notes and conditions.
fn yaml_texts() -> Vec<(String, String)> {
    fn collect(value: &Value, owner: &str, out: &mut Vec<(String, String)>) {
        match value {
            Value::Mapping(map) => {
                let owner = str_field(value, "id")
                    .or_else(|| str_field(value, "ref"))
                    .unwrap_or(owner);
                for (key, child) in map {
                    let prose = matches!(
                        key.as_str(),
                        Some("brief" | "note" | "conditionally_required")
                    );
                    match child.as_str() {
                        Some(text) if prose => out.push((owner.to_owned(), text.to_owned())),
                        _ => collect(child, owner, out),
                    }
                }
            }
            Value::Sequence(items) => {
                for item in items {
                    collect(item, owner, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    for file in [REGISTRY, SPANS] {
        collect(&yaml(file), file, &mut out);
    }
    out
}

#[test]
fn proposal_cites_the_issue_and_its_sources() {
    let page = read(PAGE);
    let refs = issue_refs(&page);
    for issue in [2015, 1838] {
        assert!(refs.contains(&issue), "{PAGE} must cite #{issue}");
    }
    assert!(
        page.contains("../adr/0001-otel-trace-contract.md"),
        "{PAGE} must link ADR 0001"
    );
}

#[test]
fn every_adr_span_maps_to_its_own_proposed_span() {
    let adr = adr_spans(&read(ADR));
    assert_eq!(adr.len(), 8, "{ADR} §2 must list eight spans: {adr:?}");
    let mapping = mapping(&read(PAGE));
    let spans = span_groups();
    let mut targets = BTreeSet::new();
    for (name, kind) in &adr {
        let (target, note) = mapping
            .get(name)
            .unwrap_or_else(|| panic!("{PAGE} section 7 must map the ADR span `{name}`"));
        let target = code(target)
            .unwrap_or_else(|| panic!("`{name}` must map to a span id, not `{target}`"));
        let group = spans
            .get(target)
            .unwrap_or_else(|| panic!("`{name}` maps to `{target}`, which {SPANS} lacks"));
        assert!(targets.insert(target), "two ADR spans map to `{target}`");
        let proposed = str_field(group, "span_kind").unwrap_or_default();
        let noted = format!("Harvest uses `{}`", kind.to_ascii_uppercase());
        assert!(
            proposed == kind || note.contains(&noted),
            "`{target}` changes the ADR kind of `{name}`. Its note must say: {noted}"
        );
    }
    let unmapped: Vec<_> = spans
        .keys()
        .filter(|id| !targets.contains(id.as_str()))
        .collect();
    assert!(unmapped.is_empty(), "no ADR span maps to {unmapped:?}");
}

#[test]
fn every_attr_constant_is_mapped_or_excluded_with_a_reason() {
    let constants = attr_constants(&read(TELEMETRY_RS));
    assert!(
        constants.len() >= 10,
        "ATTR_* extraction rotted: {constants:?}"
    );
    let mapping = mapping(&read(PAGE));
    let registry = registry_attributes();
    for constant in &constants {
        let (target, note) = mapping
            .get(constant)
            .unwrap_or_else(|| panic!("{PAGE} section 7 must map `{constant}`"));
        if let Some(id) = code(target) {
            assert!(
                registry.contains_key(id),
                "`{constant}` maps to `{id}`, which {REGISTRY} lacks"
            );
        } else {
            assert!(
                target.starts_with("Not proposed") && note.split_whitespace().count() >= 3,
                "`{constant}` needs a target attribute, or `Not proposed` with a reason"
            );
        }
    }
}

#[test]
fn the_model_is_generalized_and_well_named() {
    for file in [REGISTRY, SPANS] {
        let text = read(file).to_ascii_lowercase();
        assert!(
            !text.contains("harvest"),
            "{file} must not name Harvest. Keep engine names in the page mapping."
        );
    }
    for id in registry_attributes().keys() {
        assert!(
            id.starts_with("workflow."),
            "`{id}` must be in `workflow.*`"
        );
        assert!(
            is_well_named(id),
            "`{id}` must be lower-case dot-separated snake case"
        );
    }
    for id in span_groups().keys() {
        assert!(
            id.starts_with("span.workflow."),
            "`{id}` must start `span.workflow.`"
        );
        assert!(
            is_well_named(id),
            "`{id}` must be lower-case dot-separated snake case"
        );
    }
}

/// Check the note boundaries, the error status, the run id condition and
/// `error.type` of one span group.
fn check_span_contract(id: &str, group: &Value, refs: &BTreeMap<String, Value>) {
    let note = str_field(group, "note")
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    for phrase in NOTE_PHRASES {
        assert!(
            note.contains(phrase),
            "the note of `{id}` must say: {phrase}"
        );
    }
    let end = note
        .split("It ends when")
        .nth(1)
        .and_then(|rest| rest.split('.').next())
        .unwrap_or_default();
    assert!(
        end.contains("fail"),
        "the end boundary of `{id}` must cover a failure: It ends when{end}."
    );
    let run_id = refs.get("workflow.run.id");
    let run_id_ok = run_id.is_some_and(|level| {
        level.as_str() == Some("required")
            || level.get("conditionally_required").and_then(Value::as_str) == Some(RUN_ID_CONDITION)
    });
    assert!(
        run_id_ok,
        "`{id}` must require `workflow.run.id`, or require it: {RUN_ID_CONDITION}"
    );
    if note.contains("SHOULD be a child of the `execute_workflow` span") {
        let start = note
            .split("The span starts when")
            .nth(1)
            .and_then(|rest| rest.split('.').next())
            .unwrap_or_default();
        assert!(
            start.contains("`execute_workflow` cycle"),
            "`{id}` is a child of `execute_workflow`, so it must start inside that cycle"
        );
    }
    if id.contains("_signal.") {
        assert_eq!(
            refs.get("workflow.signal.name").and_then(Value::as_str),
            Some("required"),
            "`{id}` must require `workflow.signal.name`: its span name omits the signal"
        );
    }
    let error = refs
        .get("error.type")
        .and_then(|level| level.get("conditionally_required").and_then(Value::as_str));
    assert_eq!(
        error,
        Some(ERROR_CONDITION),
        "`{id}` must make `error.type` conditionally required: {ERROR_CONDITION}"
    );
}

#[test]
fn the_model_has_the_weaver_shape() {
    for group in groups(&yaml(REGISTRY), REGISTRY) {
        assert_eq!(str_field(&group, "type"), Some("attribute_group"));
        let id = str_field(&group, "id").unwrap_or_default();
        assert!(
            id.starts_with("registry.workflow"),
            "bad registry group `{id}`"
        );
        assert!(str_field(&group, "brief").is_some(), "`{id}` needs a brief");
    }
    for (id, attribute) in registry_attributes() {
        assert_eq!(
            str_field(&attribute, "stability"),
            Some("development"),
            "`{id}` must be `development`"
        );
        assert!(
            str_field(&attribute, "brief").is_some(),
            "`{id}` needs a brief"
        );
        match type_name(&attribute) {
            "boolean" => {}
            // Each span table repeats an enum example, so it can contradict
            // the operation value that the span pins.
            "enum" => assert!(
                attribute.get("examples").is_none(),
                "`{id}` must have no `examples`. Its members are the values"
            ),
            "string" | "int" => assert!(
                attribute.get("examples").is_some(),
                "`{id}` needs `examples`"
            ),
            other => panic!("`{id}` has an unknown type `{other}`"),
        }
    }

    let registry = registry_attributes();
    for (id, group) in span_groups() {
        assert_eq!(
            str_field(&group, "type"),
            Some("span"),
            "`{id}` must be a span"
        );
        assert_eq!(
            str_field(&group, "stability"),
            Some("development"),
            "`{id}` must be `development`"
        );
        assert!(str_field(&group, "brief").is_some(), "`{id}` needs a brief");
        let kind = str_field(&group, "span_kind").unwrap_or_default();
        assert!(
            SPAN_KINDS.contains(&kind),
            "`{id}` has a bad span_kind `{kind}`"
        );
        assert!(
            id.ends_with(&format!(".{kind}")),
            "`{id}` must end with `.{kind}`"
        );
        let refs = span_refs(&group);
        check_span_contract(&id, &group, &refs);
        for attribute in ALWAYS_REQUIRED {
            assert_eq!(
                refs.get(*attribute).and_then(Value::as_str),
                Some("required"),
                "`{id}` must require `{attribute}`"
            );
        }
        for (attribute, level) in refs {
            assert!(
                registry.contains_key(&attribute)
                    || GENERAL_ATTRIBUTES.contains(&attribute.as_str()),
                "`{id}` refers to `{attribute}`, which no registry defines"
            );
            assert!(
                is_requirement_level(&level),
                "`{id}` gives `{attribute}` a bad requirement_level"
            );
        }
    }
}

#[test]
fn the_page_tables_match_the_model() {
    let page = read(PAGE);
    let registry = registry_attributes();
    let attributes = table_rows(page_section(&page, ATTRIBUTES_SECTION), "workflow.");
    assert_eq!(
        attributes.keys().collect::<Vec<_>>(),
        registry.keys().collect::<Vec<_>>(),
        "{PAGE} section 4 must list each registry attribute"
    );
    for (id, rest) in &attributes {
        assert_eq!(
            rest.first().map(String::as_str),
            Some(type_name(&registry[id])),
            "{PAGE} section 4 gives `{id}` the wrong type"
        );
    }

    let spans = span_groups();
    let rows = table_rows(page_section(&page, SPANS_SECTION), "span.workflow.");
    assert_eq!(
        rows.keys().collect::<Vec<_>>(),
        spans.keys().collect::<Vec<_>>(),
        "{PAGE} section 5 must list each span group"
    );
    for (id, rest) in &rows {
        let kind = str_field(&spans[id], "span_kind").unwrap_or_default();
        assert_eq!(
            rest.first().and_then(|cell| code(cell)),
            Some(kind.to_ascii_uppercase().as_str()),
            "{PAGE} section 5 gives `{id}` the wrong kind"
        );
    }

    let issue = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION))
        .unwrap_or_else(|| panic!("{PAGE} section 9 must hold a ```markdown fence"));
    for name in registry.keys().map(String::as_str).chain(
        spans
            .keys()
            .filter_map(|id| id.strip_prefix("span.workflow.")?.split('.').next()),
    ) {
        assert!(
            issue.contains(&format!("`{name}`")),
            "{PAGE} section 9 must name `{name}`"
        );
    }
}

#[test]
fn the_operation_enum_names_one_span_each() {
    let registry = registry_attributes();
    let operation = registry
        .get("workflow.operation.name")
        .expect("the registry must define `workflow.operation.name`");
    let members: BTreeSet<String> = operation
        .get("type")
        .and_then(|kind| kind.get("members"))
        .and_then(Value::as_sequence)
        .expect("`workflow.operation.name` must be an enum")
        .iter()
        .filter_map(|member| str_field(member, "value").map(str::to_owned))
        .collect();
    let mut operations = BTreeSet::new();
    for (id, group) in span_groups() {
        let operation = id
            .strip_prefix("span.workflow.")
            .and_then(|rest| rest.split('.').next())
            .unwrap_or_default()
            .to_owned();
        let pinned = format!("MUST be `{operation}`");
        assert!(
            ref_note(&group, "workflow.operation.name").is_some_and(|note| note.contains(&pinned)),
            "`{id}` must pin the operation value in its ref note: {pinned}"
        );
        assert!(
            operations.insert(operation),
            "two span groups share an operation: `{id}`"
        );
    }
    assert_eq!(
        members, operations,
        "each enum member must name exactly one span"
    );
}

#[test]
fn the_replay_rule_is_normative() {
    let registry = registry_attributes();
    let replay = registry
        .get("workflow.replay")
        .expect("the registry must define `workflow.replay`");
    assert_eq!(str_field(replay, "type"), Some("boolean"));
    let spans = span_groups();
    let execute = spans
        .iter()
        .find(|(id, _)| id.starts_with("span.workflow.execute_workflow."))
        .map(|(_, group)| group)
        .expect("the model must define the execute_workflow span");
    assert_eq!(
        span_refs(execute)
            .get("workflow.replay")
            .and_then(Value::as_str),
        Some("required"),
        "the execute_workflow span must require `workflow.replay`"
    );
    let page = read(PAGE);
    let rules = numbered_items(page_section(&page, REPLAY_SECTION));
    for rule in REPLAY_RULES {
        assert!(
            rules
                .iter()
                .any(|item| rule.iter().all(|phrase| item.contains(phrase))),
            "{PAGE} section 6 must have one numbered rule that holds: {rule:?}"
        );
    }
}

#[test]
fn the_submission_status_is_recorded() {
    let page = read(PAGE);
    let status = paragraph(&page, STATUS_PREFIX)
        .unwrap_or_else(|| panic!("{PAGE} must have a `{STATUS_PREFIX}` paragraph"));
    let state = status
        .trim()
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or_default();
    match state {
        "submitted" => assert!(
            is_upstream_item(&status),
            "a submitted status must link one upstream issue or pull request"
        ),
        "declined" => assert!(
            status.split_whitespace().count() > 3,
            "a declined status must give the reason"
        ),
        "pending" => {
            let text = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION));
            assert!(
                text.is_some_and(|body| body.split_whitespace().count() > 50),
                "a pending status needs the issue text in section 9"
            );
            let body = text.unwrap_or_default();
            assert!(
                !body.contains("/trunk-dev/") && body.contains("/tree/<commit>/"),
                "the section 9 link must name a `<commit>` placeholder, not a branch"
            );
        }
        other => panic!("the status must be `pending`, `submitted` or `declined`, not `{other}`"),
    }
}

#[test]
fn the_weaver_check_script_mirrors_upstream() {
    let script = read(WEAVER_SCRIPT);
    assert!(
        pinned_commit(&script).is_some(),
        "{WEAVER_SCRIPT} must pin `semconv_commit=\"<40-hex SHA>\"`, not a branch"
    );
    assert!(
        pin(&script, "baseline_tag").is_some_and(|tag| tag.starts_with('v')),
        "{WEAVER_SCRIPT} must pin `baseline_tag=\"v<release>\"`"
    );
    assert!(
        pinned_sha(&script, "community_commit").is_some(),
        "{WEAVER_SCRIPT} must pin `community_commit=\"<40-hex SHA>\"`, not a branch"
    );
    let code = code_lines(&script);
    let missing: Vec<String> = WEAVER_SCRIPT_LINES
        .iter()
        .filter(|(line, _)| !code.contains(line))
        .chain(
            WEAVER_SCRIPT_PARTS
                .iter()
                .filter(|(part, _)| !code.iter().any(|line| line.contains(part))),
        )
        .map(|(text, reason)| format!("`{text}`: {reason}"))
        .collect();
    assert!(
        missing.is_empty(),
        "{WEAVER_SCRIPT} lacks these code lines:\n{}",
        missing.join("\n")
    );
}

/// The steps of `job` that run the Weaver script.
fn script_steps(job: &Value) -> Vec<Value> {
    sequence(job, "steps")
        .into_iter()
        .filter(|step| str_field(step, "run").is_some_and(|run| run.trim() == WEAVER_SCRIPT))
        .collect()
}

#[test]
fn the_weaver_check_runs_when_the_model_changes() {
    let workflow = yaml(WEAVER_WORKFLOW);
    let on = workflow
        .get("on")
        .unwrap_or_else(|| panic!("{WEAVER_WORKFLOW} must have `on`"));
    for event in ["pull_request", "push"] {
        let trigger = on
            .get(event)
            .unwrap_or_else(|| panic!("{WEAVER_WORKFLOW} must run on `{event}`"));
        let paths = strings(trigger, "paths");
        for path in [MODEL_PATHS, WEAVER_SCRIPT, WEAVER_WORKFLOW] {
            assert!(
                paths.iter().any(|p| p == path),
                "{WEAVER_WORKFLOW} `{event}` must list the path `{path}`"
            );
        }
    }
    assert!(
        strings(&on["pull_request"], "types").contains(&"ready_for_review".to_owned()),
        "{WEAVER_WORKFLOW} must run when a draft becomes ready"
    );
    assert!(
        strings(&on["push"], "branches").contains(&"trunk-dev".to_owned()),
        "{WEAVER_WORKFLOW} must run on a push to `trunk-dev`"
    );
    for event in ["workflow_dispatch", "schedule"] {
        assert!(
            on.get(event).is_some(),
            "{WEAVER_WORKFLOW} must run on `{event}`"
        );
    }

    let jobs = &workflow["jobs"];
    let pinned = jobs
        .get("pinned")
        .unwrap_or_else(|| panic!("{WEAVER_WORKFLOW} must have a `pinned` job"));
    assert_eq!(
        str_field(pinned, "if"),
        Some(DRAFT_SKIP),
        "the `pinned` job may skip a draft and nothing else"
    );
    let steps = script_steps(pinned);
    assert!(
        !steps.is_empty(),
        "the `pinned` job must run `{WEAVER_SCRIPT}`"
    );
    assert!(
        pinned.get("continue-on-error").is_none(),
        "the `pinned` job must fail when the script fails"
    );
    for step in &steps {
        for key in ["if", "continue-on-error", "env"] {
            assert!(
                step.get(key).is_none(),
                "the `pinned` step that runs the script must have no `{key}`"
            );
        }
    }

    let latest = jobs
        .get("latest")
        .unwrap_or_else(|| panic!("{WEAVER_WORKFLOW} must have a `latest` job"));
    assert!(
        script_steps(latest)
            .iter()
            .any(|step| step["env"]["SEMCONV_LATEST"].as_str() == Some("1")),
        "the `latest` job must run `{WEAVER_SCRIPT}` with `SEMCONV_LATEST: \"1\"`"
    );
}

#[test]
fn the_page_records_the_weaver_check() {
    let page = read(PAGE);
    let script = read(WEAVER_SCRIPT);
    let sha = pinned_commit(&script).unwrap_or_else(|| panic!("{WEAVER_SCRIPT} must pin a commit"));
    let short = format!("`{}`", &sha[..7]);
    let baseline = pin(&script, "baseline_tag")
        .unwrap_or_else(|| panic!("{WEAVER_SCRIPT} must pin a baseline"));
    let community = pinned_sha(&script, "community_commit")
        .unwrap_or_else(|| panic!("{WEAVER_SCRIPT} must pin a community commit"));
    let community = format!("`{}`", &community[..7]);
    for (file, text) in [(PAGE, page.as_str()), (PROPOSAL, read(PROPOSAL).as_str())] {
        for (at, _) in text.match_indices(COMMUNITY_PIN_PREFIX) {
            let copy = text
                .get(at + COMMUNITY_PIN_PREFIX.len() - 1..)
                .unwrap_or_default();
            assert!(
                copy.starts_with(&community),
                "each `{COMMUNITY_PIN_PREFIX}…` in {file} must name the pin {community}"
            );
        }
    }
    let submission = page_section(&page, SUBMISSION_SECTION);
    let link = format!("../../{WEAVER_SCRIPT}");
    for (text, what) in [
        (link.as_str(), "the script link"),
        (short.as_str(), "the pinned commit"),
        (&format!("`{baseline}`"), "the pinned baseline"),
        (community.as_str(), "the pinned community commit"),
    ] {
        assert!(
            submission.contains(text),
            "{SUBMISSION_SECTION} must name {what}, {text}"
        );
    }
    let steps = numbered_items(submission);
    let step = |needle: &str| steps.iter().position(|step| step.contains(needle));
    let check = step("check-otel-workflow-semconv.sh");
    let filing = step("file the text");
    assert!(
        check.is_some() && filing.is_some() && check < filing,
        "a submission step must run the Weaver check before the step that files the text"
    );
    let issue = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION)).unwrap_or_default();
    assert!(
        issue.contains("`make check-policies`") && issue.contains(&short),
        "the issue text must say that the model passes `make check-policies` at {short}"
    );
}

#[test]
fn prose_sentences_and_yaml_texts_stay_short() {
    let page = read(PAGE);
    let issue = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION))
        .unwrap_or_default()
        .to_owned();
    let mut texts = vec![
        (PAGE.to_owned(), page),
        (format!("{PAGE} section 9"), issue),
        (PROPOSAL.to_owned(), read(PROPOSAL)),
    ];
    texts.extend(yaml_texts());
    let long: Vec<String> = texts
        .iter()
        .flat_map(|(source, text)| {
            prose_sentences(text)
                .into_iter()
                .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
                .map(move |sentence| format!("{source}: {sentence}"))
        })
        .collect();
    assert!(
        long.is_empty(),
        "sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

#[test]
fn the_adr_and_the_telemetry_page_link_the_proposal() {
    assert!(
        read(ADR).contains("../otel-workflow-semconv/README.md"),
        "{ADR} must link the proposal"
    );
    assert!(
        read(TELEMETRY_PAGE).contains("otel-workflow-semconv/README.md"),
        "{TELEMETRY_PAGE} must link the proposal"
    );
}

/// A docs-only change skips the `test` matrix, so `lint` must run these guards.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read(".github/workflows/ci.yml");
    let lint = workflow
        .find("\n  lint:")
        .expect("ci.yml must define `lint`");
    let test = workflow
        .find("\n  test:")
        .expect("ci.yml must define `test`");
    let stanza = step_stanza(&workflow[lint..test], FILTER).unwrap_or_else(|| {
        panic!(
            "the `lint` job in ci.yml must run these guards. Add this step:\n\
             \x20     - name: Run the OTel workflow conventions proposal guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

#[test]
fn the_issue_text_fits_the_upstream_form() {
    let page = read(PAGE);
    let section = page_section(&page, ISSUE_TEXT_SECTION);
    for (text, what) in [
        (ISSUE_FORM, "the upstream issue form"),
        (ISSUE_AREA, "the area to pick"),
        (ISSUE_FIELD, "the field that takes the text"),
    ] {
        assert!(
            section.contains(text),
            "{ISSUE_TEXT_SECTION} must name {what}, {text}"
        );
    }
}

#[test]
fn the_draft_cites_the_overlapping_upstream_issue() {
    let page = read(PAGE);
    let issue = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION)).unwrap_or_default();
    for (text, what) in [
        (page_section(&page, SCOPE_SECTION), SCOPE_SECTION),
        (issue, "the issue text"),
    ] {
        assert!(
            text.contains(PRIOR_ISSUE),
            "{what} must cite {PRIOR_ISSUE}, which claims the same `workflow.*` namespace"
        );
    }
}

#[test]
fn the_project_proposal_covers_the_template() {
    let proposal = read(PROPOSAL);
    assert!(
        issue_refs(&proposal).contains(&2015),
        "{PROPOSAL} must cite #2015"
    );
    assert!(
        proposal.contains("README.md#7-mapping-from-harvest"),
        "{PROPOSAL} must link the list of Harvest gaps in section 7"
    );
    assert!(
        read(PAGE).contains("(project-proposal.md)"),
        "{PAGE} must link {PROPOSAL}"
    );
    let mut last = 0;
    for heading in PROJECT_SECTIONS {
        let at = proposal
            .lines()
            .position(|line| line.trim_end() == *heading)
            .unwrap_or_else(|| panic!("{PROPOSAL} must have the template section `{heading}`"));
        assert!(
            at > last,
            "{PROPOSAL} `{heading}` must follow the template order"
        );
        last = at;
        let body = heading_body(&proposal, heading).unwrap_or_default();
        let subsection = next_heading(&proposal, heading).filter(|next| {
            heading_level(next) > heading_level(heading)
                && (PROJECT_SECTIONS.contains(next) || next.ends_with("(Optional)"))
        });
        assert!(
            !body.trim().is_empty() || subsection.is_some(),
            "{PROPOSAL} `{heading}` must have text or a template subsection"
        );
    }
    let relative: Vec<&str> = proposal
        .split("](")
        .skip(1)
        .filter(|target| !target.starts_with("https://") && !target.starts_with('#'))
        .map(|target| target.split(')').next().unwrap_or_default())
        .collect();
    assert!(
        relative.is_empty(),
        "{PROPOSAL} is filed in another repository, so each link must be absolute: {relative:?}"
    );
    for slot in STAFFING_SLOTS {
        let body = heading_body(&proposal, slot).unwrap_or_default().trim();
        assert!(
            body.starts_with("Open.") || body.contains("[@"),
            "{PROPOSAL} `{slot}` must link a GitHub handle or start with `Open.`"
        );
    }
}

#[test]
fn every_cited_link_is_a_checked_fact() {
    let page = read(PAGE);
    let issue = markdown_fence(page_section(&page, ISSUE_TEXT_SECTION)).unwrap_or_default();
    let proposal = read(PROPOSAL);
    let facts = section(
        page_section(&page, SUBMISSION_SECTION),
        FACTS_HEADING,
        "\n#",
    )
    .unwrap_or_else(|| panic!("{SUBMISSION_SECTION} must have `{FACTS_HEADING}`"));
    let rows: Vec<Vec<&str>> = facts
        .lines()
        .filter_map(cells)
        .filter(|row| !row.iter().all(|cell| is_separator(cell)))
        .skip(1)
        .collect();
    assert!(!rows.is_empty(), "`{FACTS_HEADING}` must have a table");
    for row in &rows {
        assert!(
            row.last().is_some_and(|date| is_iso_date(date)),
            "each fact must end with the date of its check, YYYY-MM-DD: {row:?}"
        );
    }
    let refs = external_refs(issue);
    assert!(!refs.is_empty(), "the issue text must cite its sources");
    let unchecked: Vec<String> = refs
        .into_iter()
        .chain(external_refs(&proposal))
        .filter(|link| !rows.iter().any(|row| external_refs(row[0]).contains(link)))
        .collect();
    assert!(
        unchecked.is_empty(),
        "each link in the issue text and the proposal needs a row in `{FACTS_HEADING}`: \
         {unchecked:?}"
    );
}

#[test]
fn the_submission_steps_follow_the_upstream_process() {
    let page = read(PAGE);
    let submission = page_section(&page, SUBMISSION_SECTION);
    let steps = numbered_items(submission);
    let step = |needles: &[&str]| {
        let found: Vec<usize> = steps
            .iter()
            .enumerate()
            .filter(|(_, step)| needles.iter().all(|needle| step.contains(needle)))
            .map(|(at, _)| at)
            .collect();
        match found.as_slice() {
            [one] => *one,
            _ => panic!("{SUBMISSION_SECTION} needs exactly one step that holds {needles:?}"),
        }
    };
    let check = step(&["Run the script", "check-otel-workflow-semconv.sh"]);
    let issue = step(&["file the text", ISSUE_FORM]);
    let status = step(&["`submitted`"]);
    let project = step(&["open-telemetry/community", "(project-proposal.md)"]);
    let upstream = step(&["upstream pull request", "TC", "GC"]);
    assert!(
        check < issue && issue < status && issue < project && project < upstream,
        "the steps must run the check, file the issue, then set the status, open the \
         project proposal, and open the upstream pull request after the TC and the GC"
    );
    for committee in ["Technical Committee (TC)", "Governance Committee (GC)"] {
        assert!(
            submission.contains(committee),
            "{SUBMISSION_SECTION} must say that the project needs the {committee}"
        );
    }
    let decline = paragraph(submission, DECLINE_PREFIX)
        .unwrap_or_else(|| panic!("{SUBMISSION_SECTION} must have a `{DECLINE_PREFIX}` paragraph"));
    assert!(
        decline.contains("`declined`") && decline.contains("registry"),
        "the decline paragraph must set `declined` and name the separate-registry fallback"
    );
}

#[test]
fn helpers_parse_their_inputs() {
    assert_eq!(cells("| `a` | b |"), Some(vec!["`a`", "b"]));
    assert_eq!(cells("not a row"), None);
    assert_eq!(code("`x.y`"), Some("x.y"));
    assert_eq!(code("x.y"), None);

    let page = "## 7. Mapping from Harvest\n\n| Harvest | Target | Note |\n|---|---|---|\n\
                | `harvest.queue` | `workflow.queue.name` | |\n\
                | `harvest.shard.id` | Not proposed. | Engine detail. |\n\
                | `other.name` | `x` | |\n\n## 8. Next\n| `harvest.late` | `y` | |\n";
    let parsed = mapping(page);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed["harvest.queue"].0, "`workflow.queue.name`");
    assert_eq!(parsed["harvest.shard.id"].1, "Engine detail.");

    let adr = "### 2. Span catalogue\n\n#### 2.1 `harvest.a.b` — one\n\n| Property | Value |\n\
               |---|---|\n| Kind | `PRODUCER` |\n\n#### 2.2 `harvest.c.d` — two\n\n| Kind | `INTERNAL` |\n\
               \n### 3. Next\n#### 2.9 `harvest.z` — late\n| Kind | `CLIENT` |\n";
    assert_eq!(
        adr_spans(adr),
        BTreeMap::from([
            ("harvest.a.b".to_owned(), "producer".to_owned()),
            ("harvest.c.d".to_owned(), "internal".to_owned()),
        ])
    );

    let source = "pub const ATTR_A: &str = \"harvest.a\";\nconst ATTR_B: &str = \"b\";\n\
                  pub const ATTR_LONG: &str =\n    \"harvest.long\";\n\
                  pub const METRIC_C: &str = \"harvest.c\";\n";
    assert_eq!(
        attr_constants(source),
        BTreeSet::from(["harvest.a".to_owned(), "harvest.long".to_owned()])
    );

    assert!(is_well_named("workflow.run.id"));
    assert!(is_well_named("span.workflow.execute_workflow.internal"));
    assert!(!is_well_named("workflow"));
    assert!(!is_well_named("Workflow.name"));
    assert!(!is_well_named("workflow..name"));
    assert!(!is_well_named("workflow.run-id"));

    assert_eq!(
        paragraph("x\n\n**S:** pending. Two\nlines.\n\nNext.", "**S:**").as_deref(),
        Some("pending. Two lines.")
    );
    assert_eq!(markdown_fence("a\n```markdown\nbody\n```\n"), Some("body"));
    assert_eq!(
        numbered_items("Intro MUST.\n\n1. One\n   more.\n2. Two.\n\nAfter 3. x.\n"),
        vec!["One more.".to_owned(), "Two.".to_owned()]
    );
    assert!(is_upstream_item(
        "submitted: https://github.com/open-telemetry/semantic-conventions/issues/42"
    ));
    assert!(!is_upstream_item(
        "submitted: https://github.com/open-telemetry/semantic-conventions"
    ));
    let sha = "331541c4057c53d133c32efadaa7b656636eda77";
    assert_eq!(
        pinned_commit(&format!("x\nsemconv_commit=\"{sha}\"\n")),
        Some(sha)
    );
    assert_eq!(pinned_commit("semconv_commit=\"main\""), None);
    assert_eq!(
        pinned_commit(&format!("semconv_commit=\"{}\"", sha.to_uppercase())),
        None
    );
    assert_eq!(pinned_commit("no pin"), None);
    assert_eq!(
        pinned_commit(&format!(
            "# semconv_commit=\"{sha}\"\nsemconv_commit=\"main\"\n"
        )),
        None
    );
    assert_eq!(
        pin("a=\"1\"\nbaseline_tag=\"v1.2.0\"\n", "baseline_tag"),
        Some("v1.2.0")
    );
    assert_eq!(
        code_lines("#!/bin/sh\n  # note\n\n  run x  \n"),
        vec!["run x"]
    );
    assert_eq!(
        pinned_sha(&format!("community_commit=\"{sha}\""), "community_commit"),
        Some(sha)
    );
    assert_eq!(
        external_refs("See dapr/dapr#9213, (https://a.b/c). Not #12 or a/b#x or /a/b#1."),
        BTreeSet::from(["dapr/dapr#9213".to_owned(), "https://a.b/c".to_owned()])
    );
    assert_eq!(
        external_refs("[x](https://a.b/l) <https://a.b/m> **https://a.b/n** http://a.b/o"),
        BTreeSet::from([
            "https://a.b/l".to_owned(),
            "https://a.b/m".to_owned(),
            "https://a.b/n".to_owned(),
            "http://a.b/o".to_owned(),
        ])
    );
    assert!(external_refs("https://x.y/tree/<commit>/z").is_empty());
    assert!(external_refs(&format!("{OWN_REPO}tree/abc/docs")).is_empty());
    assert!(is_separator("---"));
    assert!(is_separator(":--:"));
    assert!(!is_separator(""));
    assert!(!is_separator("`a-b`"));
    assert!(is_iso_date("2026-10-18"));
    assert!(!is_iso_date("2026-1-18"));
    assert!(!is_iso_date("Oct 2026"));
    let doc_md = "# T\n\n## A\n\ntext\n\n## B\n### B1\n\nmore\n#### B2\n";
    assert_eq!(heading_body(doc_md, "## A").map(str::trim), Some("text"));
    assert_eq!(heading_body(doc_md, "## B").map(str::trim), Some(""));
    assert_eq!(heading_body(doc_md, "#### B2").map(str::trim), Some(""));
    assert_eq!(heading_body(doc_md, "## C"), None);
    assert_eq!(next_heading(doc_md, "## B"), Some("### B1"));
    assert_eq!(next_heading(doc_md, "#### B2"), None);
    assert_eq!(heading_level("### B1"), 3);
    let doc: Value = serde_yaml::from_str("p: [a, 1, b]").expect("valid YAML");
    assert_eq!(strings(&doc, "p"), vec!["a".to_owned(), "b".to_owned()]);
    assert!(is_requirement_level(&Value::from("opt_in")));
    assert!(!is_requirement_level(&Value::from("optional")));
}
