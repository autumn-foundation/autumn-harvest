//! Anti-drift guard for the Temporal migration guide (issue #947).
//!
//! A migration guide that quietly rots is worse than none: a stale mapping
//! sends a real evaluator down a dead end. This test keeps three promises
//! mechanically rather than by review discipline alone:
//!
//! 1. the guide and its worked example both exist, and the guide covers the
//!    required ground — every required section, and at least 25
//!    concept-mapping table rows (issue #947 AC1's literal minimum);
//! 2. every `#NNN` issue citation inside the guide actually appears in
//!    `docs/shipped-work.md` — the repository's own record of what has shipped — so a
//!    claim can never cite a number nobody can verify (issue #947 AC1's
//!    "verifiable in under a minute" bar, and the "no unshipped capability
//!    presented as shipped" bar);
//! 3. the guide is linked from every place a reader would look for it:
//!    `README.md`, `docs/getting-started/README.md`, and — completing the
//!    forward reference `docs/comparison.md` shipped with a `_planned_`
//!    placeholder — `docs/comparison.md` itself (issue #947 AC5).

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const GUIDE_PATH: &str = "docs/migrating-from-temporal.md";
const EXAMPLE_NAME: &str = "temporal_port_subscription_renewal";

const REQUIRED_SECTIONS: &[&str] = &[
    "## Scope and audience",
    "## Non-goals",
    "## Concept mapping",
    "### Workflows and activities",
    "### Signals",
    "### Queries and updates",
    "### Timers and continue-as-new",
    "### Child workflows",
    "### Schedules",
    "### Versioning and determinism",
    "### Worker placement and build routing",
    "### Operational lifecycle",
    "## No equivalent yet",
    "## Workflow-porting checklist",
    "## Dual-run cutover playbook",
    "## Worked example",
    "## Related",
];

/// One literal substring per named primitive the guide must cover. This is
/// a *coverage* check (each of these must be mentioned), not a row-count
/// check — the row count is verified separately by
/// `concept_mapping_table_has_at_least_25_rows` against the actual markdown
/// table structure, so the two checks cannot silently drift apart.
const REQUIRED_CONCEPTS: &[&str] = &[
    "`#[workflow]`",
    "`#[activity]`",
    "SignalWithStart",
    "signal-with-start",
    "`setHandler`",
    "register_signal_handler",
    "`condition(",
    "wait_for_signal",
    "Idempotency-Key",
    "`@workflow.query`",
    "register_query_handler",
    "`@workflow.update`",
    "register_update_handler",
    "UpdateWithStart",
    "update_with_start",
    "`sleep(",
    "ctx.timer(",
    "sleep_until",
    "cancellable",
    "continueAsNew",
    "continue_as_new",
    "cross-type",
    "last_completion_result",
    "executeChild",
    "spawn_child_workflow",
    "ParentClosePolicy",
    "fan-out",
    "Promise.race",
    "ctx.race()",
    "child-or-deadline",
    "Temporal Schedules",
    "OverlapPolicy",
    "Catchup",
    "bounded",
    "Calendar",
    "business-day",
    "jitter",
    "updateSchedule",
    "describeSchedule",
    "GetVersion",
    "Patched",
    "DeprecatePatch",
    "Build ID",
    "build ramp",
    "SideEffect",
    "Local Activity",
    "Worker Sessions",
    "Reset Workflow Execution",
    "Terminate",
    "Cancel",
    "Pause",
    "Search Attributes",
    "WorkflowReplayer",
    "determinism",
    "Nexus",
    "multi-region",
    "non-Rust",
];

#[test]
fn guide_and_example_exist() {
    let guide = workspace_path(GUIDE_PATH);
    assert!(
        guide.is_file(),
        "expected {GUIDE_PATH} to exist (issue #947)"
    );

    let example = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(format!("{EXAMPLE_NAME}.rs"));
    assert!(
        example.is_file(),
        "expected examples/{EXAMPLE_NAME}.rs to exist (issue #947 worked example)"
    );
}

#[test]
fn guide_covers_every_required_section() {
    let guide = read_doc(GUIDE_PATH);
    let missing: Vec<&&str> = REQUIRED_SECTIONS
        .iter()
        .filter(|section| !guide.contains(**section))
        .collect();
    assert!(
        missing.is_empty(),
        "migration guide is missing required section(s): {missing:?}"
    );
}

#[test]
fn guide_covers_every_required_concept() {
    let guide = read_doc(GUIDE_PATH);
    let missing: Vec<&&str> = REQUIRED_CONCEPTS
        .iter()
        .filter(|concept| !guide.contains(**concept))
        .collect();
    assert!(
        missing.is_empty(),
        "migration guide is missing required concept coverage: {missing:?}"
    );
}

/// AC1's literal bar: "a concept-mapping table ... covering at minimum
/// workflows/activities, signals, ... (25+ items)". Counts actual markdown
/// table data rows inside the `## Concept mapping` section (from that
/// heading to the next `## ` heading), excluding header and separator rows
/// from each of the per-category sub-tables.
#[test]
fn concept_mapping_table_has_at_least_25_rows() {
    let guide = read_doc(GUIDE_PATH);
    let section = section_body(&guide, "## Concept mapping");

    let mut data_rows = 0usize;
    for raw_line in section.lines() {
        let line = raw_line.trim();
        if !(line.starts_with('|') && line.ends_with('|')) {
            continue;
        }
        if is_separator_row(line) {
            continue;
        }
        if line.contains("Temporal primitive") {
            // Column header row.
            continue;
        }
        data_rows += 1;
    }

    assert!(
        data_rows >= 25,
        "concept-mapping table must have at least 25 data rows, found {data_rows}"
    );
}

#[test]
fn every_cited_issue_number_appears_in_the_shipped_work_record() {
    let guide = read_doc(GUIDE_PATH);
    let shipped_work = read_doc("docs/shipped-work.md");

    let cited = issue_refs(&guide);
    let known = issue_refs(&shipped_work);

    let unverifiable: Vec<u32> = cited.difference(&known).copied().collect();
    assert!(
        unverifiable.is_empty(),
        "migration guide cites issue number(s) not found anywhere in \
         docs/shipped-work.md, so they cannot be verified against the \
         repository's own shipped-work record: {unverifiable:?}"
    );
    assert!(
        !cited.is_empty(),
        "migration guide cites no issue numbers at all -- every shipped claim must link an \
         issue (issue #947 AC1)"
    );
}

#[test]
fn guide_states_the_history_import_non_goal_with_a_concrete_reason() {
    let guide = read_doc(GUIDE_PATH);
    let non_goals = section_body(&guide, "## Non-goals");

    assert!(
        non_goals.contains("HistoryEvent"),
        "Non-goals must name Temporal's own event type (HistoryEvent) to explain the \
         incompatibility, not just assert unsupported"
    );
    assert!(
        non_goals.contains("WorkflowEvent"),
        "Non-goals must name harvest's own event type (WorkflowEvent) to contrast against \
         Temporal's HistoryEvent"
    );
    assert!(
        non_goals.to_lowercase().contains("drain"),
        "Non-goals must state that in-flight Temporal executions drain rather than migrate"
    );
}

#[test]
fn worked_example_is_referenced_by_name_in_the_guide() {
    let guide = read_doc(GUIDE_PATH);
    assert!(
        guide.contains(EXAMPLE_NAME),
        "the guide's worked-example section must reference the compiling example file by name \
         so a reader can find and run it"
    );
}

#[test]
fn guide_is_linked_from_readme_and_getting_started_index() {
    for (path, label) in [
        ("README.md", "README.md"),
        (
            "docs/getting-started/README.md",
            "docs/getting-started/README.md",
        ),
    ] {
        let doc = read_doc(path);
        assert!(
            doc.contains("migrating-from-temporal.md"),
            "{label} must link to docs/migrating-from-temporal.md"
        );
    }
}

/// Completes the bidirectional link `docs/comparison.md` shipped with a
/// `_planned_` placeholder pointing at issue #947.
#[test]
fn comparison_page_links_back_to_the_migration_guide() {
    let comparison = read_doc("docs/comparison.md");
    assert!(
        comparison.contains("migrating-from-temporal.md"),
        "docs/comparison.md must link to the migration guide, completing the bidirectional \
         cross-reference it shipped as a placeholder for"
    );
    assert!(
        !comparison.contains("Temporal migration guide** — _planned_"),
        "docs/comparison.md still carries the unresolved `_planned_` placeholder for the \
         migration guide -- replace it with a real link now that the guide exists"
    );
}

/// Issue #1219, gap 1: a schedule-driven workflow type bypasses the
/// app-level cutover flag entirely, since the Temporal server starts its
/// executions directly. The playbook must say so, and must send the reader
/// to harvest's own schedule pause and catchup primitives rather than leave
/// them to guess.
///
/// Five PR reviews (Codex, all P1) found real defects here. The first said
/// Temporal exposes a per-firing list an operator can inspect and accept or
/// reject; it does not. The second said harvest's `CatchupPolicy::SkipAll`
/// suppresses an entire missed interval; it does not, it fires the oldest
/// slot. The third found the "pause after create" sequence this fix then
/// tried left a race window for a scheduler tick to fire into. The fourth
/// found a gap in that fix too. Letting the reader create the harvest
/// schedule "any time before" cutover, even paused, still let a slot come
/// due and fire on resume. The fix now creates the harvest schedule
/// already paused, at the cutover timestamp itself, so no slot is ever
/// due before it exists. The fifth found a side effect of that fix.
/// Creating a schedule at an arbitrary cutover timestamp re-anchors an
/// interval schedule's phase. `Schedule::Interval` computes its first
/// slot from the creation moment, not from the original schedule's own
/// phase.
///
/// A twelfth review found two more gaps, in the pause-then-list capture
/// added for the schedule-driven-type routing record. Pausing first
/// closes the two ordering gaps that review found. But a store behind
/// Temporal's own visibility can still lag its executions. A query
/// right after the pause can still omit one. The same capture also only
/// ever ran forward, at cutover. A rollback resumes the Temporal
/// Schedule, and nothing captured the ids it fires after that.
#[test]
fn dual_run_playbook_covers_schedule_driven_cutover() {
    let guide = read_doc(GUIDE_PATH);
    let playbook = flatten_whitespace(section_body(&guide, "## Dual-run cutover playbook"));

    assert!(
        playbook.contains("Temporal Schedule"),
        "the playbook must name the schedule-driven-type gap (issue #1219): the Temporal \
         server starts a Temporal Schedule's executions directly, bypassing the app-level flag"
    );
    assert!(
        playbook.contains("CatchupWindow"),
        "the playbook must name Temporal's real per-schedule catchup primitive, not an \
         invented per-firing accept/reject list (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("cutover timestamp"),
        "the playbook must gate the missed interval and the harvest schedule's activation on \
         one defined cutover timestamp, not on pausing alone (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("WorkflowSchedule::with_paused(true)"),
        "the playbook must create the harvest schedule already paused, in the same insert, \
         not enabled-then-paused -- the gap a scheduler tick could fire into (issue #1219, \
         PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("An interval schedule re-anchors its phase to that creation moment")
            && playbook.contains("Schedule::Cron"),
        "the playbook must warn that `Schedule::Interval` re-anchors its phase to the \
         creation moment, and offer `Schedule::Cron` for a cadence whose phase must survive \
         the cutover (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("Do not create the harvest schedule earlier and leave it paused"),
        "the playbook must not let the reader create the harvest schedule ahead of the \
         cutover timestamp even paused, since a slot can still come due and fire on resume \
         (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("SkipAll` still fires the oldest missed slot"),
        "the playbook must correct the record: `CatchupPolicy::SkipAll` still fires one slot, \
         it does not suppress an entire missed interval (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("/admin/schedules/{id}/resume"),
        "the playbook must point to harvest's own schedule resume primitive to activate the \
         pre-paused harvest schedule at the cutover timestamp (issue #229, issue #1219)"
    );
    assert!(
        playbook.contains("CatchupPolicy"),
        "the playbook must point to the catchup-policy primitive for the harvest schedule's \
         backlog decision (issue #484, issue #1219)"
    );
    assert!(
        playbook.contains("Temporal's own visibility can lag behind its executions")
            && playbook.contains("eventually consistent"),
        "the playbook must warn that an eventually consistent Temporal visibility store can \
         still omit a just-started or just-finished execution right after the pause, \
         independent of the pause-then-list ordering fix (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("A rollback of a schedule-driven type reverses this capture")
            && playbook.contains("resume the Temporal Schedule"),
        "the playbook must cover the reverse direction of the schedule-driven capture: a \
         rollback resumes the Temporal Schedule, and every id fired after that point must \
         resolve to Temporal even though it postdates the forward-cutover snapshot \
         (issue #1219, PR #1473 Codex P1)"
    );
}

/// Issue #1219, gap 2: a follow-up operation (signal, query, update, cancel)
/// against one already-started execution must route to whichever engine
/// hosts it right now. It must never route by the current flag value, or
/// by a fact fixed at start time.
///
/// Six PR reviews (Codex, five P1 and one P2) found real gaps here across
/// five successive attempts at this rule. A persisted "record at start
/// time" cannot cover a schedule-driven execution, or one that predates
/// the record. Comparing a start time to a cutover timestamp fails when a
/// Temporal slot fires late.
///
/// A live "query harvest, active beats terminal" resolution fixes both
/// of those. It creates a new gap, though. It misroutes an ordinary
/// completed run's own follow-ups. A terminal execution is not the same
/// thing as a stale one.
///
/// The fix returns to a persisted record. This time it writes a fresh
/// record at every new start, on either engine, overwriting the last
/// one. A terminal execution's record still names the engine that ran
/// it. A rollback's new start gets a fresh record of its own. Harvest's
/// by-id resolution (issue #805) still resolves the current execution
/// once the record names harvest. That family covers signals, queries,
/// and cancellations only, not updates.
///
/// Applying the resolved-engine language to step 7's forward handoff also
/// named the wrong engine for its final read. That execution never ran on
/// harvest at all.
///
/// A later PR review found two more gaps in the persisted-record fix.
/// Harvest's own schedule tick starts an execution the same way
/// Temporal's schedule does. Neither has a flag decision point to write
/// a record at.
///
/// The record also only ever named the current owner, not every
/// generation a reused id ever had. A follow-up against a superseded
/// generation needs its own captured handle from when that generation
/// was current. Issue #805 already expects this same discipline of a
/// stale exec id.
///
/// A further review found three gaps past that.
///
/// Writing the record after the start, not before, leaves an
/// undetectable inconsistency if the two ever split. An execution can
/// exist with no record, defaulting to the wrong engine.
///
/// Routing a schedule-driven type by which schedule is active broke a
/// rule the guide already stated. An in-flight execution stays on the
/// engine that started it. A pre-cutover Temporal firing would misroute
/// once harvest's schedule took over. The fix writes the record for
/// every harvest-side start instead, scheduled or flag-routed alike.
/// Schedule-driven types then use the same mechanism as any other.
///
/// Reading an execution id and sending an update to it are two separate
/// calls. A continue-as-new between them can seal the id before the
/// update reaches it. `admit_update` does not follow that chain the way
/// it follows a retry chain.
///
/// One more review found the reconciliation retry itself unsafe under
/// two reuse policies. `AllowDuplicateFailedOnly` and
/// `TerminateIfRunning` both start a genuine second execution once the
/// first reaches a terminal state. A silently succeeded first attempt
/// can do exactly that before the retry runs. Restrict the retry to
/// `AllowDuplicate` or `RejectDuplicate`, the two policies that
/// never replace a prior execution outright.
///
/// A tenth review found three more gaps. The scheduled-firing fix
/// assumed a hook that does not exist. Harvest's built-in scheduler
/// starts an execution directly. It exposes no callback for a reader to
/// write a record at. Capture a schedule-driven type's Temporal-side
/// ids once, at cutover, instead. The harvest-only reuse-policy advice
/// also does not carry over to Temporal, whose own policy of the same
/// name means something different. Retrying a lost update response is
/// not safe either. Every admission mints a fresh update id, so a
/// retry can run the update a second time.
///
/// An eleventh review found a gap in that same capture step. Querying
/// Temporal's in-flight executions before pausing the schedule misses
/// two cases. An execution that finishes just before the query is not
/// in-flight, so the query skips it. A firing that starts in the gap
/// between the query and the pause is not captured either. Both then
/// misroute as harvest's. Pausing first, then listing every execution
/// of that type, open and closed alike, closes both gaps.
#[test]
fn dual_run_playbook_covers_follow_up_engine_routing() {
    let guide = read_doc(GUIDE_PATH);
    let playbook = flatten_whitespace(section_body(&guide, "## Dual-run cutover playbook"));

    assert!(
        playbook.to_lowercase().contains("follow-up"),
        "the playbook must name follow-up operations (signal, query, update, cancel) as a \
         distinct routing concern from a new start (issue #1219)"
    );
    assert!(
        playbook.contains("WorkflowIdReusePolicy") && playbook.contains("rollback"),
        "the playbook must name workflow-id reuse across a rollback as a hazard the \
         resolution must survive (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("A terminal execution is not the same thing as a")
            && playbook.contains("misroutes it"),
        "the playbook must distinguish an ordinary terminal execution from a superseded one \
         -- routing every terminal run's follow-ups to the other engine breaks a plain \
         completed-run query (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("record before you")
            && playbook.contains("Overwrite any earlier record for the same id"),
        "the playbook must write the routing record before the start, not after, so a crash \
         between the two leaves a detectable inconsistency rather than a real execution with \
         no record -- and it must overwrite the previous record on every new start \
         (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("Reconcile a record that names an engine with no matching execution"),
        "the playbook must tell the reader how to recover from the one bad state \
         write-before-start can leave behind: a record naming an engine with nothing \
         actually running there yet (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("WorkflowIdReusePolicy::AllowDuplicate")
            && playbook.contains("Neither is safe for this retry"),
        "the playbook must restrict the reconciliation retry to a reuse policy that returns \
         the original execution or refuses outright -- AllowDuplicateFailedOnly and \
         TerminateIfRunning can each start a genuine second execution once the first reaches \
         a terminal state, duplicating its side effects (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("Treat a missing record as Temporal"),
        "the playbook must resolve a not-yet-ported execution's missing record to Temporal \
         (issue #1219)"
    );
    assert!(
        playbook.contains("A schedule-driven type has no hook to write this record")
            && playbook.contains("Temporal's own visibility API"),
        "the playbook must not claim harvest's built-in scheduler exposes a callback to write \
         the routing record -- it does not -- and must instead capture a schedule-driven \
         type's in-flight Temporal ids once, at cutover, through Temporal's own visibility \
         API (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("On the harvest side, retry it")
            && playbook.contains("Temporal's own reject-duplicate equivalent"),
        "the playbook must scope the AllowDuplicate/RejectDuplicate reconciliation guidance to \
         harvest only -- Temporal's own reuse policy of the same name means something \
         different (a new execution once the prior closes, not \"return the original \
         regardless of state\") (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("Do not retry the update call itself if its result goes missing")
            && playbook.contains("mints a fresh update id"),
        "the playbook must warn that retrying a lost update response can run the update's own \
         logic a second time, since every admission mints a fresh update id with no dedup key \
         (issue #1219, PR #1473 Codex P1)"
    );
}

/// Issue #1219, gap 2 (continued): once a follow-up knows which engine
/// owns an execution, it still has to resolve and address that execution
/// correctly. This covers the resolution mechanics. Among them: harvest's
/// by-id family, the update route it lacks, and the continue-as-new race
/// between resolving an id and using it. Also covered: the activity check
/// a reused id needs before a new start. So is the negative flag-routing
/// rule, and step 7's own handoff as a special case of the general rule.
///
/// A twelfth review found that activity check has its own race. Two
/// concurrent requests for the same reused id can each pass it, then
/// start on different engines. Querying both engines first does not
/// serialize anything. Neither engine's query is transactional with
/// the other one, or with the start. The fix names the gap. It asks
/// the reader to hold their own lock around the whole check-then-start
/// sequence instead.
#[test]
fn dual_run_playbook_covers_follow_up_resolution_mechanics() {
    let guide = read_doc(GUIDE_PATH);
    let playbook = flatten_whitespace(section_body(&guide, "## Dual-run cutover playbook"));

    assert!(
        playbook.contains("This record names the current owner only")
            && playbook.contains("needs its own engine and execution id"),
        "the playbook must scope the routing record to the current owner only, matching \
         harvest's own by-id resolution (issue #805) -- a follow-up against a superseded \
         generation needs its own captured handle, not a lookup through this record \
         (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("/workflows/by-id/{workflow_name}/{workflow_id}"),
        "the playbook must resolve the current execution on the named engine through \
         harvest's own by-id resolution (issue #805) once the record says harvest \
         (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("The family has no update route")
            && playbook.contains("/workflows/{id}/update/{update_name}"),
        "the playbook must not route an update through the by-id family, which does not \
         carry one -- it must resolve the execution id first and use the exec-id update \
         route (issue #1219, PR #1473 Codex P2)"
    );
    assert!(
        playbook.contains("admit_update` resolves only a workflow-level")
            && playbook.contains("continue-as-new chain"),
        "the playbook must warn that the two-step update recipe (resolve id, then send \
         update) is not atomic: a continue-as-new in between can seal the id, and \
         `admit_update` does not follow that chain the way it follows a retry chain \
         (issue #1219, PR #1473 Codex P2)"
    );
    assert!(
        playbook.contains("is not still active")
            && playbook.contains("Temporal's own visibility API"),
        "the playbook must tell the reader to confirm the previous execution under a reused \
         id is not still active on its own engine, checking both engines, before starting a \
         new one elsewhere (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("This check-then-start sequence has its own race")
            && playbook.contains("application-level lock"),
        "the playbook must name the residual race in its own check-then-start sequence -- two \
         concurrent requests for a reused id can each observe no active run and then start on \
         different engines, since neither engine's query is transactional with the other or \
         with the start call -- and tell the reader to serialize it with their own \
         application-level lock (issue #1219, PR #1473 Codex P1)"
    );
    assert!(
        playbook.contains("Pause the Temporal Schedule first")
            && playbook.contains("open and closed executions, not only the in-flight ones"),
        "the playbook must pause the Temporal Schedule before capturing its schedule-driven \
         type's ids, and capture open and closed executions alike -- querying in-flight \
         executions before the pause misses one that finishes just before the query and one \
         the schedule fires in the gap before the pause takes effect (issue #1219, PR #1473 \
         Codex P1)"
    );
    assert!(
        playbook.contains("Never route it by the flag's current value"),
        "the playbook must state the negative rule too: never route a follow-up by \
         re-consulting the current flag value (issue #1219)"
    );
    assert!(
        playbook.contains("is a special case of step 1's general follow-up-routing rule"),
        "step 7's cancel-signal routing must itself be framed as a special case of the \
         general follow-up-routing rule, not a standalone exception (issue #1219)"
    );
    assert!(
        playbook.contains("that record is missing or still names Temporal")
            && playbook.contains("Temporal, not harvest"),
        "step 7's forward handoff drains an execution that only ever ran on Temporal -- both \
         the cancel and the final read must resolve to Temporal, since no start has ever \
         routed this entity to harvest (issue #1219, PR #1473 Codex P1)"
    );
}

/// Issue #1219, gap 2 (continued): the worked example's own `cancel` signal
/// is the concrete hazard the issue names. Sent to the wrong engine, it
/// either no-ops or spuriously starts a new execution. The commentary must
/// cross-link the general routing rule, not just show the signal in
/// isolation.
#[test]
fn worked_example_commentary_cross_links_engine_routing_rule() {
    let guide = read_doc(GUIDE_PATH);
    let commentary = flatten_whitespace(section_body(&guide, "### What changed, and why"));

    assert!(
        commentary.contains("(workflow_name, workflow_id) -> engine` record"),
        "the worked example's commentary must cross-link the general engine-routing rule for \
         its own `cancel` signal (issue #1219)"
    );
}

#[test]
fn ci_workflow_exercises_the_worked_example() {
    let ci = read_doc(".github/workflows/ci.yml");
    assert!(
        ci.contains(&format!("--example {EXAMPLE_NAME}")),
        "the worked example must be exercised by a CI step (`cargo test ... --example \
         {EXAMPLE_NAME}`), not just present on disk"
    );
}

/// The guide embeds the harvest side of the worked example as a Rust code
/// fence, next to the Temporal TypeScript original, so a reader can compare
/// the two without leaving the page. A copy that drifts from the real,
/// compiling file is worse than no copy at all -- it teaches the wrong
/// thing. Keep the embedded snippet byte-identical to the source of truth.
#[test]
fn worked_example_code_block_matches_the_real_file() {
    let guide = read_doc(GUIDE_PATH);
    let example = read_normalized(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join(format!("{EXAMPLE_NAME}.rs")),
    );

    let doc_start = "### Harvest (Rust)\n\n```rust\n";
    let start = guide.find(doc_start).unwrap_or_else(|| {
        panic!("expected to find the '{doc_start:?}' code fence in {GUIDE_PATH}")
    }) + doc_start.len();
    let end = guide[start..]
        .find("\n```\n")
        .unwrap_or_else(|| panic!("expected the Harvest (Rust) code fence to close"));
    let doc_snippet = guide[start..start + end].trim();

    let file_start = "use autumn_harvest::prelude::*;";
    let file_start_idx = example
        .find(file_start)
        .unwrap_or_else(|| panic!("expected {EXAMPLE_NAME}.rs to start with {file_start:?}"));
    let file_end_marker = "\n\nfn main()";
    let file_end_idx = example[file_start_idx..]
        .find(file_end_marker)
        .unwrap_or_else(|| panic!("expected a {file_end_marker:?} marker after the workflow fn"));
    let file_snippet = example[file_start_idx..file_start_idx + file_end_idx].trim();

    assert_eq!(
        doc_snippet, file_snippet,
        "the guide's embedded `### Harvest (Rust)` code block has drifted from the real \
         examples/{EXAMPLE_NAME}.rs file -- keep the two byte-identical"
    );
}

/// `.github/workflows/ci.yml` skips its `test` job's steps entirely on a
/// docs-only PR (every step there is gated on `changes.outputs.code ==
/// 'true'`), and a PR touching only `docs/migrating-from-temporal.md` is
/// exactly a docs-only PR. Without a step outside that gate, these guards
/// -- the ones enforcing every promise this module makes -- would never
/// execute on the change class most likely to break them: a stale link, a
/// drifted citation, an edited worked-example snippet.
///
/// `docs/performance.md` and `docs/rnd/sqlite-feasibility.md` hit this same
/// gap before this module existed and fixed it the same way: a dedicated
/// step in the ungated `lint` job. This asserts that step exists for this
/// module too, lives in `lint` (not the gated `test` matrix), and has not
/// grown an `if:` condition that would put it back behind the same gate.
#[test]
fn guards_run_on_docs_only_changes() {
    let workflow = read_normalized(&workspace_path(".github/workflows/ci.yml"));

    // The step must exist, and must name this module as its filter -- a step
    // that ran some *other* test would satisfy a looser check while leaving
    // these guards just as unexecuted.
    let step = workflow
        .lines()
        .find(|line| line.contains("--test integration migrating_from_temporal_docs::"))
        .expect(
            "ci.yml must run the migrating_from_temporal_docs guards from a step that is \
             not gated on `changes.outputs.code`, or a docs-only PR -- the change class \
             these guards exist for -- skips them entirely",
        );
    assert!(
        step.trim_start().starts_with("run:"),
        "expected the guard invocation to be a step `run:` line, found: {step}"
    );

    // It must live in `lint`, the ungated job. `test` is gated per-step on
    // `changes.outputs.code`, so a step there proves nothing for docs-only PRs.
    let lint_start = workflow
        .find("\n  lint:")
        .expect("ci.yml must define a `lint` job");
    let test_start = workflow
        .find("\n  test:")
        .expect("ci.yml must define a `test` job");
    let step_at = workflow
        .find("--test integration migrating_from_temporal_docs::")
        .expect("located above");
    assert!(
        step_at > lint_start && step_at < test_start,
        "the migrating_from_temporal_docs guard step must live in the ungated `lint` job; \
         a step in the `test` matrix is gated on `changes.outputs.code` and so does not \
         run on a docs-only PR"
    );

    // And it must be unconditional. A step that grew an `if:` is back behind
    // a gate -- which is the exact regression this test exists to prevent.
    let block: &str = &workflow[lint_start..test_start];
    let step_idx = block
        .find("--test integration migrating_from_temporal_docs::")
        .expect("step is inside the lint block");
    let step_line_start = block[..step_idx].rfind("\n      - name:").unwrap_or(0);
    let stanza = &block[step_line_start..step_idx];
    assert!(
        !stanza.contains("\n        if:"),
        "the migrating_from_temporal_docs guard step has acquired an `if:` condition. It \
         must run unconditionally: a condition is how these guards would stop running on \
         docs-only PRs again. Stanza:\n{stanza}"
    );
}

/// Collapse every run of whitespace, including a hand-wrapped line break, to
/// one space.
///
/// This guide hand-wraps prose at roughly 80 columns. A multi-word phrase
/// assertion against the raw text can span a wrap point and silently miss a
/// match that is present to a human reader. Flatten first, so a phrase
/// assertion is robust to where the author happened to wrap the line.
fn flatten_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Read a file with line endings normalised to `\n`.
///
/// Every needle search in this module -- markdown headers, code-fence
/// markers, table separators, workflow-file structural boundaries such as
/// `"\n  lint:"` -- is anchored on `\n`. A Windows checkout hands
/// `fs::read_to_string` `\r\n` line endings by default, so each needle
/// silently misses and a test fails with a panic that has nothing to do
/// with the file's actual contents. Normalising once here (`read_doc`
/// delegates to this too) keeps every needle search platform-agnostic
/// rather than spreading `\r?` handling across each one.
fn read_normalized(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .replace("\r\n", "\n")
}

fn is_separator_row(line: &str) -> bool {
    let inner = line.trim_matches('|');
    !inner.is_empty()
        && inner.contains('-')
        && inner
            .chars()
            .all(|c| c == '-' || c == ':' || c == '|' || c.is_whitespace())
}

/// Returns the text between `heading` and the next line starting with
/// `"## "` (or end of document).
fn section_body<'a>(doc: &'a str, heading: &str) -> &'a str {
    let start = doc
        .find(heading)
        .unwrap_or_else(|| panic!("expected to find heading {heading:?}"));
    let after_heading = &doc[start + heading.len()..];
    let end = after_heading.find("\n## ").unwrap_or(after_heading.len());
    &after_heading[..end]
}

fn issue_refs(text: &str) -> BTreeSet<u32> {
    let mut refs = BTreeSet::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'#' {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            let digits = end - start;
            if digits > 0
                && digits <= 6
                && let Ok(n) = text[start..end].parse::<u32>()
            {
                refs.insert(n);
            }
            i = end.max(i + 1);
        } else {
            i += 1;
        }
    }
    refs
}

fn read_doc(relative: &str) -> String {
    read_normalized(&workspace_path(relative))
}

fn workspace_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative)
}
