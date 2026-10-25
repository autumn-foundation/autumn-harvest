//! Guards for the decision owner ADR (issue #2249).
//!
//! ADR 0009 records the stage 1 owner model: decision partitions and the
//! fenced owner lease. These guards check facts, not prose:
//!
//! * The ADR records each decision that the issue lists.
//! * The ADR has one sequence diagram for the four owner flows.
//! * Each stage 1 question of the research report has an answer or a
//!   deferral to an issue of the epic.
//! * The glossaries and the `CLAUDE.md` invariant name the new unit.
//! * Each symbol and test that the ADR cites is real.
//! * Each prose sentence has 25 words or fewer.
//!
//! Pure: no database, no async, no feature gate. Runs on every OS.

use std::path::{Path, PathBuf};

use super::docs_guard_support::{check_stanza, issue_refs, prose_sentences, section, step_stanza};

const ADR: &str = "docs/adr/0009-decision-owner.md";
const REPORT: &str = "reports/Actor ownership prior art for Harvest.md";
const LOCK_TEST: &str = "autumn-harvest/tests/integration/decision_owner_lock_modes_db_tests.rs";
const FILTER: &str = "--test integration decision_owner_docs::";
const MAX_SENTENCE_WORDS: usize = 25;

/// The child issues of epic #2247 that stage 1 can defer to.
///
/// Issue #2249 is this ADR, so a row cannot defer to it.
const DEFERRAL_ISSUES: &[u32] = &[2248, 2250, 2251, 2252, 2253, 2254, 2255, 2256];

/// One `###` heading prefix per decision that issue #2249 lists.
const DECISIONS: &[&str] = &[
    "### 1. Name",
    "### 2. Count",
    "### 3. Hash input",
    "### 4. Lease placement",
    "### 5. Epoch",
    "### 6. Lock order",
    "### 7. Fenced commit",
    "### 8. Lease TTL",
    "### 9. Drain",
    "### 10. Tenant cells",
    "### 11. Rollout",
];

/// The four flows the sequence diagram must show.
const FLOWS: &[&str] = &["Acquire", "Commit", "Takeover", "Fenced commit"];

/// Each stage 1 question or risk of the report: the key phrase in the
/// report, then the key the ADR table row starts with.
const STAGE_1_QUESTIONS: &[(&str, &str)] = &[
    (
        "Should the hash input include `workflow_name`",
        "Hash input",
    ),
    (
        "How do partitions interact with tenant cells",
        "Tenant cells",
    ),
    (
        "Should graceful drain hand partitions over explicitly",
        "Drain",
    ),
    ("what lease TTL makes sense", "Lease TTL"),
    ("**foreign writers stay foreign**", "Foreign writers"),
    ("invalidates another owner's cache", "Cache invalidation"),
    (
        "**an acknowledgment must never come from owner memory**",
        "Acknowledgement",
    ),
    (
        "**lease expiry during a long decision**",
        "Lease expiry mid-decision",
    ),
    (
        "**two `FOR SHARE` rows on every append**",
        "Cost of two fences",
    ),
    ("a **mixed fleet during rollout**", "Mixed fleet"),
    (
        "filters the workflow-task claim to `partition = ANY($owned)`",
        "Routing",
    ),
    (
        "**group commit across executions in one partition**",
        "Group commit",
    ),
    (
        "The owner must serialize per execution",
        "Serialization unit",
    ),
    ("Cover four nemeses", "Model nemeses"),
    ("Port Golem's end-to-end test", "Frozen-owner test"),
    ("| Stage 1 measurement |", "Pass conditions"),
];

/// The paragraph of the report that asks the open stage 1 questions.
const OPEN_QUESTIONS_START: &str = "The open questions for stage 1 are these.";

/// Questions in that paragraph. A new question there must get a row.
const OPEN_QUESTION_COUNT: usize = 4;

/// The paragraph of the report that lists the stage 1 risks in bold.
const RISKS_START: &str = "The main stage 1 risks are specific.";

/// Bold risks in that paragraph. A new risk there must get a row.
const RISK_COUNT: usize = 5;

/// Symbols the ADR cites: the symbol, its source file, and its declaration.
const CITED_CODE: &[(&str, &str, &str)] = &[
    (
        "assert_decision_fence",
        "autumn-harvest/src/replication.rs",
        "pub async fn assert_decision_fence(",
    ),
    (
        "requeue_claimed_workflow_task_after_fence",
        "autumn-harvest/src/queue.rs",
        "pub async fn requeue_claimed_workflow_task_after_fence(",
    ),
    (
        "harvest_decision_owner_fence",
        "autumn-harvest/migrations/20261020132402_harvest_decision_owner/up.sql",
        "CREATE OR REPLACE FUNCTION harvest_decision_owner_fence(",
    ),
    (
        "OwnerKey",
        "autumn-harvest/src/decision_owner.rs",
        "pub struct OwnerKey {",
    ),
    (
        "acquire_many",
        "autumn-harvest/src/decision_owner.rs",
        "pub async fn acquire_many(",
    ),
    (
        "DEFAULT_SCANNER_LEASE_TTL",
        "autumn-harvest/src/scanner_lease.rs",
        "pub const DEFAULT_SCANNER_LEASE_TTL: Duration = Duration::from_secs(10);",
    ),
    (
        "MIN_LEASE_TICKS",
        "autumn-harvest/src/scanner_lease.rs",
        "pub const MIN_LEASE_TICKS: u32 = 3;",
    ),
    (
        "MAX_SCANNER_LEASE_TTL",
        "autumn-harvest/src/scanner_lease.rs",
        "pub const MAX_SCANNER_LEASE_TTL: Duration = Duration::from_secs(300);",
    ),
    (
        "ShardRouter",
        "autumn-harvest/src/shard.rs",
        "pub struct ShardRouter {",
    ),
    (
        "with_shard_assignments",
        "autumn-harvest/src/builder.rs",
        "pub fn with_shard_assignments(",
    ),
    (
        "resolve_shard_assignments",
        "autumn-harvest/src/builder.rs",
        "fn resolve_shard_assignments(",
    ),
    (
        "assert_fence",
        "autumn-harvest/src/replication.rs",
        "pub async fn assert_fence(",
    ),
    (
        "bump_generation",
        "autumn-harvest/src/replication.rs",
        "pub async fn bump_generation(",
    ),
    (
        "check_paused_and_park",
        "autumn-harvest/src/worker.rs",
        "pub async fn check_paused_and_park(",
    ),
    (
        "continue_as_new_as_type",
        "autumn-harvest/src/context.rs",
        "pub async fn continue_as_new_as_type(",
    ),
];

/// The lock-mode tests that prove the renewal and takeover rules.
/// The owner pin tests that the ADR cites, in the issue #2251 suite.
const OWNER_TEST: &str = "autumn-harvest/tests/integration/decision_owner_db_tests.rs";
const OWNER_TESTS: &[&str] = &[
    "a_decision_takes_the_owner_row_before_the_execution_row",
    "a_multi_row_renewal_locks_rows_in_ascending_order",
];

const LOCK_TESTS: &[&str] = &[
    "a_renewal_does_not_wait_for_an_in_flight_commit",
    "a_commit_does_not_wait_for_an_in_flight_renewal",
    "an_acquisition_waits_for_an_in_flight_commit",
    "an_acquisition_still_waits_after_a_renewal",
    "a_commit_does_not_queue_behind_a_waiting_acquisition",
    "a_plain_epoch_update_does_not_wait_for_a_commit",
    "a_for_share_commit_blocks_a_renewal",
    "a_scanner_style_upsert_does_not_wait_for_a_commit",
    "a_local_idle_timeout_frees_a_frozen_commit",
    "a_skip_locked_acquisition_skips_an_in_flight_commit",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

/// Read a repo file with CRLF folded to LF, so a Windows checkout matches.
fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("issue #2249: cannot read {}: {err}", path.display()))
        .replace("\r\n", "\n")
}

/// The body of the `##` section that `heading` opens, up to the next `##`.
fn h2_section<'a>(doc: &'a str, heading: &str) -> &'a str {
    section(doc, &format!("\n{heading}\n"), "\n## ")
        .unwrap_or_else(|| panic!("{ADR} has no `{heading}` section"))
}

/// The first cell and the rest of each data row of the tables in `text`.
fn table_rows(text: &str) -> Vec<(String, String)> {
    text.lines()
        .filter(|line| line.starts_with('|') && !is_separator(line))
        .skip(1)
        .filter_map(|line| {
            let cells: Vec<&str> = line.trim_matches('|').splitn(2, '|').collect();
            match cells.as_slice() {
                [first, rest] => Some((first.trim().to_owned(), rest.trim().to_owned())),
                _ => None,
            }
        })
        .collect()
}

/// A table separator line such as `|---|:---:|`.
fn is_separator(line: &str) -> bool {
    line.contains('-') && line.chars().all(|c| matches!(c, '|' | '-' | ':' | ' '))
}

/// The first line citation in `doc`, such as `file.rs:42` or `file.md:42`.
fn cites_a_line_number(doc: &str) -> Option<String> {
    for ext in [".rs:", ".md:", ".rs#L", ".toml:", ".yml:", ".sql:"] {
        for (at, _) in doc.match_indices(ext) {
            let rest = &doc[at + ext.len()..];
            if rest.starts_with(|c: char| c.is_ascii_digit()) {
                let start = doc[..at].rfind(char::is_whitespace).map_or(0, |i| i + 1);
                let end = at
                    + ext.len()
                    + rest
                        .find(|c: char| !c.is_ascii_digit())
                        .unwrap_or(rest.len());
                return Some(doc[start..end].to_owned());
            }
        }
    }
    None
}

#[test]
fn adr_records_an_accepted_decision() {
    let adr = read(ADR);
    assert!(
        adr.starts_with("# ADR 0009: "),
        "{ADR} must open with its ADR number"
    );
    assert!(
        adr.contains("## Status\n\nAccepted (issue #2249)."),
        "{ADR} must record an accepted decision for issue #2249"
    );
    for heading in [
        "## Context",
        "## Decision",
        "## Stage 1 questions",
        "## Options considered",
        "## Consequences",
    ] {
        assert!(
            adr.contains(&format!("\n{heading}\n")),
            "{ADR} lacks `{heading}`"
        );
    }
    assert!(
        adr.contains("../design/DESIGN-2249.md"),
        "{ADR} must link its planning record"
    );
}

#[test]
fn adr_number_is_unique() {
    let dir = repo_root().join("docs/adr");
    let owners: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|err| panic!("cannot list {}: {err}", dir.display()))
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.starts_with("0009-"))
        .collect();
    assert_eq!(
        owners,
        ["0009-decision-owner.md"],
        "ADR number 0009 must name one file only"
    );
}

#[test]
fn adr_records_every_decision_the_issue_lists() {
    let adr = read(ADR);
    let decision = h2_section(&adr, "## Decision");
    let mut last = None;
    for heading in DECISIONS {
        let at = decision
            .find(&format!("\n{heading}"))
            .unwrap_or_else(|| panic!("{ADR} `## Decision` lacks `{heading}`"));
        assert!(
            last.is_none_or(|previous| at > previous),
            "{ADR} must keep `{heading}` in issue order"
        );
        last = Some(at);
    }
    for fact in [
        "decision partition",
        "512",
        "`workflow_id` alone",
        "`FOR KEY SHARE`",
        "`FOR UPDATE`",
        "A renewal never changes the epoch",
    ] {
        assert!(decision.contains(fact), "{ADR} must state: {fact}");
    }
}

#[test]
fn adr_has_the_four_flow_sequence_diagram() {
    let adr = read(ADR);
    let diagram = section(&adr, "```mermaid\nsequenceDiagram\n", "\n```")
        .unwrap_or_else(|| panic!("{ADR} must hold a mermaid `sequenceDiagram`"));
    let notes: Vec<&str> = diagram
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("Note over"))
        .collect();
    for flow in FLOWS {
        assert!(
            notes.iter().any(|note| note.contains(flow)),
            "{ADR} sequence diagram must label the `{flow}` flow in a `Note over` line"
        );
    }
}

#[test]
fn every_stage_1_question_has_an_answer_or_a_deferral() {
    let report = read(REPORT);
    let open = section(&report, OPEN_QUESTIONS_START, "\n")
        .unwrap_or_else(|| panic!("{REPORT} lost the stage 1 open questions"));
    assert_eq!(
        open.matches('?').count(),
        OPEN_QUESTION_COUNT,
        "{REPORT} changed its stage 1 questions. Add a row to {ADR} and to STAGE_1_QUESTIONS"
    );
    let risks = section(&report, RISKS_START, "\n")
        .unwrap_or_else(|| panic!("{REPORT} lost the stage 1 risk paragraph"));
    assert_eq!(
        risks.matches("**").count() / 2,
        RISK_COUNT,
        "{REPORT} changed its stage 1 risks. Add a row to {ADR} and to STAGE_1_QUESTIONS"
    );
    let stage_1 = section(&report, "### Stage 1:", "\n### Stage 2:")
        .unwrap_or_else(|| panic!("{REPORT} lost its stage 1 section"));

    let adr = read(ADR);
    let rows = table_rows(h2_section(&adr, "## Stage 1 questions"));
    assert_eq!(
        rows.len(),
        STAGE_1_QUESTIONS.len(),
        "{ADR} `## Stage 1 questions` must have one row per question"
    );
    for (phrase, key) in STAGE_1_QUESTIONS {
        assert!(
            stage_1.contains(phrase),
            "{REPORT} stage 1 no longer says `{phrase}`. Update STAGE_1_QUESTIONS"
        );
        let (_, rest) = rows
            .iter()
            .find(|(first, _)| first == key)
            .unwrap_or_else(|| panic!("{ADR} has no `{key}` question row"));
        let answer = rest.rsplit('|').next().unwrap_or_default().trim();
        if let Some(deferral) = answer.strip_prefix("Deferred to ") {
            let cited = issue_refs(deferral);
            assert!(
                !cited.is_empty() && cited.iter().all(|n| DEFERRAL_ISSUES.contains(n)),
                "the `{key}` deferral must cite an issue of the epic: {answer}"
            );
        } else {
            assert!(
                answer.starts_with("Answered"),
                "the `{key}` row must start its last cell with `Answered` or `Deferred to #`"
            );
        }
    }
}

#[test]
fn adr_cites_only_real_code() {
    let adr = read(ADR);
    for (symbol, file, declaration) in CITED_CODE {
        assert!(
            adr.contains(&format!("`{symbol}`")),
            "{ADR} must cite `{symbol}`"
        );
        assert!(
            read(file).contains(declaration),
            "{ADR} cites `{symbol}`, but {file} lacks `{declaration}`"
        );
    }
    assert!(
        read("docs/architecture.md").contains("| `workflow_id`, shard, queue | **Kept** |"),
        "{ADR} relies on continue-as-new keeping `workflow_id`, shard and queue"
    );
    let owner_suite = read(OWNER_TEST);
    for test in OWNER_TESTS {
        assert!(
            adr.contains(&format!("`{test}`")),
            "{ADR} must cite `{test}`"
        );
        assert!(
            owner_suite.contains(&format!("async fn {test}(")),
            "{OWNER_TEST} lacks `{test}`"
        );
    }
    let suite = read(LOCK_TEST);
    for test in LOCK_TESTS {
        assert!(
            adr.contains(&format!("`{test}`")),
            "{ADR} must cite `{test}`"
        );
        assert!(
            suite.contains(&format!("async fn {test}(")),
            "{LOCK_TEST} lacks `{test}`"
        );
    }
}

/// The body of `async fn name(` in `suite`, up to the next test.
fn test_body<'a>(suite: &'a str, name: &str) -> Option<&'a str> {
    let start = suite.find(&format!("async fn {name}("))?;
    let body = &suite[start..];
    let open = body.find('{')?;
    let end = body.find("#[tokio::test]").unwrap_or(body.len());
    Some(&body[open..end])
}

/// Each row of the lock-mode table agrees with the assert of its test.
#[test]
fn lock_mode_table_matches_the_tests() {
    let adr = read(ADR);
    let decision = h2_section(&adr, "## Decision");
    let table = section(decision, "| First transaction |", "\n\n")
        .unwrap_or_else(|| panic!("{ADR} lost the lock-mode table"));
    let suite = read(LOCK_TEST);
    let mut proved = Vec::new();
    for line in table
        .lines()
        .filter(|l| l.starts_with('|') && !is_separator(l))
    {
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        let [_, _, waits, proof] = cells.as_slice() else {
            panic!("{ADR} lock-mode row must have four cells: {line}");
        };
        let expected = if waits.starts_with("Yes") {
            "Outcome::Waited"
        } else if waits.starts_with("No") {
            "Outcome::Ran"
        } else {
            panic!("{ADR} `Second waits` must start with Yes or No: {line}");
        };
        for test in proof.split(',').map(|t| t.trim().trim_matches('`')) {
            let body =
                test_body(&suite, test).unwrap_or_else(|| panic!("{LOCK_TEST} lacks `{test}`"));
            assert!(
                body.contains(&format!("{expected},")),
                "{ADR} says `{waits}` for `{test}`, but the test does not assert {expected}"
            );
            proved.push(test.to_owned());
        }
    }
    let mut cited: Vec<String> = LOCK_TESTS.iter().map(|t| (*t).to_owned()).collect();
    cited.sort();
    proved.sort();
    assert_eq!(
        proved, cited,
        "the lock-mode table must cite each lock test once"
    );
}

#[test]
fn adr_cites_no_line_numbers() {
    let adr = read(ADR);
    assert_eq!(
        cites_a_line_number(&adr),
        None,
        "{ADR} must cite a symbol, not a line number, because line numbers drift"
    );
}

#[test]
fn glossaries_define_the_decision_partition() {
    let sharding = read("docs/sharding.md");
    let glossary = section(&sharding, "\n## Glossary\n", "\n## ")
        .unwrap_or_else(|| panic!("docs/sharding.md must hold a `## Glossary`"));
    for term in [
        "**Shard**",
        "**Decision partition**",
        "**Owner**",
        "**Owner row**",
        "**Owner epoch**",
        "**Foreign writer**",
        "**Owner mode**",
    ] {
        assert!(
            glossary.contains(term),
            "docs/sharding.md glossary lacks {term}"
        );
    }
    assert!(
        glossary.contains("adr/0009-decision-owner.md"),
        "docs/sharding.md glossary must link ADR 0009"
    );

    let architecture = read("docs/architecture.md");
    let terms = section(&architecture, "\n### Terms\n", "\n#")
        .unwrap_or_else(|| panic!("docs/architecture.md must hold a `### Terms` list"));
    for needle in ["**decision partition**", "sharding.md#glossary"] {
        assert!(
            terms.contains(needle),
            "docs/architecture.md `### Terms` lacks {needle}"
        );
    }
}

#[test]
fn claude_md_states_the_owner_fence_invariant() {
    let claude = read("CLAUDE.md");
    let invariants = section(&claude, "\n## Engine Invariants\n", "\n## ")
        .unwrap_or_else(|| panic!("CLAUDE.md lost `## Engine Invariants`"));
    let rule = section(invariants, "\n### Decision owner fence", "\n### ")
        .unwrap_or_else(|| panic!("CLAUDE.md engine invariants lack the owner fence rule"));
    for fact in [
        "docs/adr/0009-decision-owner.md",
        "`FOR KEY SHARE`",
        "`FOR UPDATE`",
        "epoch",
        "never retries",
    ] {
        assert!(
            rule.contains(fact),
            "the CLAUDE.md owner fence rule lacks {fact}"
        );
    }
}

#[test]
fn prose_sentences_stay_short() {
    let long: Vec<String> = prose_sentences(&read(ADR))
        .into_iter()
        .filter(|sentence| sentence.split_whitespace().count() > MAX_SENTENCE_WORDS)
        .collect();
    assert!(
        long.is_empty(),
        "{ADR} has prose sentences over {MAX_SENTENCE_WORDS} words. Split each one:\n{}",
        long.join("\n")
    );
}

/// The foreign writer table of decision 12 (issue #2253) cites each writer
/// by file and function. The ADR cites no line numbers, so this guard
/// checks that each cited function is still declared in its file.
#[test]
fn foreign_writer_table_cites_real_functions() {
    let adr = read(ADR);
    let decision = section(&adr, "\n### 12. Foreign writers", "\n## ")
        .unwrap_or_else(|| panic!("{ADR} has no decision 12"));
    let rows = table_rows(decision);
    assert!(
        rows.len() >= 20,
        "{ADR} decision 12 lists each writer class"
    );
    let mut checked = 0;
    for (class, rest) in &rows {
        let writer = rest.split('|').next().unwrap_or_default();
        let mut file: Option<String> = None;
        for symbol in writer.split('`').skip(1).step_by(2) {
            if std::path::Path::new(symbol)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("rs"))
            {
                file = Some(read(&format!("autumn-harvest/src/{symbol}")));
                continue;
            }
            let source = file
                .as_deref()
                .unwrap_or_else(|| panic!("the `{class}` row names a function before a file"));
            assert!(
                source.contains(&format!("fn {symbol}("))
                    || source.contains(&format!("fn {symbol}<")),
                "the `{class}` row cites `{symbol}`, which its file does not declare"
            );
            checked += 1;
        }
    }
    assert!(checked >= 40, "the table cites each writer function");
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
             \x20     - name: Run the decision owner ADR guards (also on docs-only PRs)\n\
             \x20       run: \"cargo test -p autumn-harvest --no-default-features --features \
             testing {FILTER}\""
        )
    });
    check_stanza(stanza, FILTER).unwrap_or_else(|reason| panic!("{reason}. Stanza:\n{stanza}"));
}

/// The failover page of issue #2254.
const FAILOVER_PAGE: &str = "docs/performance-owner-failover.md";

/// The suite that measures the failover page.
const FAILOVER_SUITE: &str = "autumn-harvest/tests/integration/owner_failover_tests.rs";

/// The failover tests that the page cites.
const FAILOVER_TESTS: &[&str] = &[
    "a_frozen_owner_is_fenced_after_a_takeover",
    "a_frozen_commit_transaction_does_not_block_the_takeover",
    "after_kill_9_the_next_decision_runs_within_ttl_plus_one_second",
    "scale_out_from_4_to_8_workers_recovers_within_two_ttls",
];

/// The failover page cites each failover test, and each test exists. The
/// ADR links the page (issue #2254).
#[test]
fn failover_page_cites_the_failover_tests() {
    let page = read(FAILOVER_PAGE);
    let suite = read(FAILOVER_SUITE);
    for test in FAILOVER_TESTS {
        assert!(
            page.contains(&format!("`{test}`")),
            "{FAILOVER_PAGE} must cite `{test}`"
        );
        assert!(
            suite.contains(&format!("async fn {test}(")),
            "{FAILOVER_SUITE} lacks `{test}`"
        );
    }
    assert!(
        read(ADR).contains("performance-owner-failover.md"),
        "{ADR} must link {FAILOVER_PAGE}"
    );
}

/// The decider page of issue #2255.
const DECIDER_PAGE: &str = "docs/performance-owner-decider.md";

/// The suite that proves the decider.
const DECIDER_SUITE: &str = "autumn-harvest/tests/integration/owner_decider_tests.rs";

/// The decider tests that the page cites.
const DECIDER_TESTS: &[&str] = &[
    "a_signal_acknowledged_just_before_an_epoch_bump_is_never_lost",
    "a_signal_hinted_to_an_owner_that_loses_its_epoch_is_never_lost",
    "an_owner_takes_workflow_tasks_off_the_claim_path",
];

/// The fused fence that decision 13 cites.
const FUSED_FENCE: (&str, &str) = (
    "autumn-harvest/migrations/20261023015935_harvest_owner_decider/up.sql",
    "CREATE OR REPLACE FUNCTION harvest_decision_fence(",
);

/// The decider page cites each decider test, and each test exists.
/// Decision 13 names the suite, links the page and cites the fused fence,
/// which its migration declares (issue #2255).
#[test]
fn decider_page_and_decision_13_cite_real_tests() {
    let page = read(DECIDER_PAGE);
    let suite = read(DECIDER_SUITE);
    let adr = read(ADR);
    let decision = section(&adr, "\n### 13. Owner decider", "\n## ")
        .unwrap_or_else(|| panic!("{ADR} has no decision 13"));
    for test in DECIDER_TESTS {
        assert!(
            page.contains(&format!("`{test}`")),
            "{DECIDER_PAGE} must cite `{test}`"
        );
        assert!(
            suite.contains(&format!("async fn {test}(")),
            "{DECIDER_SUITE} lacks `{test}`"
        );
    }
    for link in ["owner_decider_tests.rs", "performance-owner-decider.md"] {
        assert!(
            decision.contains(link),
            "{ADR} decision 13 must cite {link}"
        );
    }
    let (migration, declaration) = FUSED_FENCE;
    assert!(
        decision.contains("`harvest_decision_fence`") && read(migration).contains(declaration),
        "{ADR} decision 13 cites `harvest_decision_fence`, and {migration} declares it"
    );
}

#[test]
fn helpers_parse_their_inputs() {
    let table = "| Key | Question | Answer |\n|---|---|---|\n| A | q? | Answered. |\n";
    assert_eq!(
        table_rows(table),
        [("A".to_owned(), "q? | Answered.".to_owned())]
    );
    assert_eq!(
        cites_a_line_number("see `store.rs:976` here"),
        Some("`store.rs:976".to_owned())
    );
    assert_eq!(cites_a_line_number("see `store.rs` and a.md#x"), None);
    assert_eq!(
        cites_a_line_number("see store.rs#L9"),
        Some("store.rs#L9".to_owned())
    );
    assert!(is_separator("| --- |:--:|"));
    assert!(!is_separator("| a-b | c |"));
    let suite = "#[tokio::test]\nasync fn one() {\n    x(Outcome::Ran);\n}\n\n\
                 #[tokio::test]\nasync fn two() {\n    x(Outcome::Waited);\n}\n";
    assert_eq!(
        test_body(suite, "one"),
        Some("{\n    x(Outcome::Ran);\n}\n\n")
    );
    assert!(test_body(suite, "two").is_some_and(|b| b.contains("Waited")));
}
