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
const STATUS_PREFIX: &str = "**Submission status:**";

/// The replay rules that section 6 must state. Each rule is a set of
/// phrases that one numbered item must hold.
const REPLAY_RULES: &[&[&str]] = &[
    &["MUST set `workflow.replay`"],
    &[
        "MUST NOT emit a span for an operation that history already records",
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
            "boolean" | "enum" => {}
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
        }
        other => panic!("the status must be `pending`, `submitted` or `declined`, not `{other}`"),
    }
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
    assert!(is_requirement_level(&Value::from("opt_in")));
    assert!(!is_requirement_level(&Value::from("optional")));
}
