//! Guards that keep `docs/partitioned-events.md` honest (issue #958).
//!
//! No DB, no feature gate. Each assertion pins a claim that is either
//! **load-bearing for an operator following the runbook under pressure** or
//! **dangerous to quietly lose in a later editing pass**.
//!
//! The ones that matter most are the honest limits: that the whole retention
//! pass is *not* made fast by this change, that reads do not prune, and that
//! the foreign key is gone. Every one of those reads like a nit in review and
//! costs an operator a bad decision if it disappears.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn doc() -> String {
    let path = repo_root().join("docs/partitioned-events.md");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// Collapse every run of whitespace to one space.
///
/// Assertions below are about phrases the doc must contain, and Markdown wraps
/// prose at whatever column the author happened to stop at. Matching raw text
/// would make a guard fail — or, worse, silently need weakening — every time
/// someone reflowed a paragraph. The claim being pinned is the sentence, not
/// its line breaks.
fn flat(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Body of the `## <heading>` section, up to the next `## `.
fn section<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    let needle = format!("\n## {heading}");
    let start = doc.find(&needle)? + 1;
    let rest = &doc[start..];
    let end = rest[1..].find("\n## ").map_or(rest.len(), |i| i + 1);
    Some(&rest[..end])
}

// ── The design decision a reviewer will question first ─────────────────────

#[test]
fn the_doc_explains_why_the_partition_key_is_not_timestamp() {
    let d = flat(&doc());
    assert!(
        d.contains("UNIQUE (workflow_exec_id, event_id, timestamp)"),
        "the doc must show the constraint a `timestamp` key would produce — \
         it is the whole reason the key is a separate column, and without it \
         the design reads as an arbitrary choice"
    );
    assert!(
        d.contains("optimistic-concurrency detector"),
        "and must say what that constraint IS: losing it means two workers \
         advancing the same workflow stop colliding"
    );
}

#[test]
fn the_doc_records_that_postgres_forbids_the_stamping_trigger() {
    let d = flat(&doc());
    assert!(
        d.contains("moving row to another partition during a BEFORE FOR EACH ROW"),
        "the exact Postgres error must be recorded. A future contributor WILL \
         propose stamping the cohort from the execution's created_at — it is \
         the obvious design — and needs to find out here rather than from a \
         production incident"
    );
    assert!(
        d.contains("silently"),
        "and must record the nastier half: the trigger silently succeeds when \
         the pre- and post-trigger destinations coincide, so it looks like it \
         works"
    );
}

// ── The honest limits ──────────────────────────────────────────────────────

#[test]
fn the_doc_states_that_the_whole_pass_is_not_made_fast_by_this_change() {
    let raw = doc();
    let s = flat(section(&raw, "Measured").expect("a `## Measured` section"));
    assert!(
        s.contains("per-execution candidate loop"),
        "the doc must name what actually dominates a retention pass. An \
         operator who reads only the delete counts will expect their pass to \
         get faster and be surprised"
    );
    assert!(
        s.contains("Not met by this change alone"),
        "and must say plainly which half of the Success Metric is not met"
    );
    assert!(
        s.contains("on either layout"),
        "and that the remaining cost is layout-independent, so it is not \
         evidence against partitioning"
    );
}

#[test]
fn the_doc_warns_that_reads_do_not_prune_and_gives_the_sizing_rule() {
    let d = flat(&doc());
    assert!(
        d.contains("Reads do not prune"),
        "the read-amplification cost must be stated, not buried"
    );
    assert!(
        d.contains("retention horizon ÷ cohort width"),
        "and it must come with the sizing rule that bounds it — a warning \
         without a remedy is not actionable"
    );
}

#[test]
fn the_doc_states_that_the_foreign_key_is_dropped_and_what_replaces_it() {
    let d = flat(&doc());
    assert!(
        d.contains("drops `harvest_events_workflow_exec_id_fkey`"),
        "dropping a foreign key is exactly the kind of thing that must never \
         be a surprise found later in a schema diff"
    );
    assert!(
        d.contains("harvest_events_require_execution"),
        "and the doc must name what restores its insert-time half"
    );
    assert!(
        d.contains("orphan event rows"),
        "and be explicit that the delete-time cascade is NOT restored"
    );
}

#[test]
fn the_doc_warns_that_a_long_running_execution_pins_its_cohorts() {
    let d = flat(&doc());
    assert!(
        d.contains("pins the cohorts it wrote into"),
        "the straggler case is the first thing an operator will hit that looks \
         like a bug and is not"
    );
    assert!(
        d.contains("off by default"),
        "and the doc must say the straggler fallback is opt-in, so the default \
         configuration's 'zero row deletes' claim stays true"
    );
}

// ── The runbook ────────────────────────────────────────────────────────────

#[test]
fn the_migration_runbook_separates_the_online_steps_from_the_lock_window() {
    let raw = doc();
    let s = flat(section(&raw, "Enabling it").expect("an `## Enabling it` section"));
    for needle in [
        "CREATE UNIQUE INDEX CONCURRENTLY",
        "NOT VALID",
        "VALIDATE CONSTRAINT",
        "SHARE UPDATE EXCLUSIVE",
        "ACCESS EXCLUSIVE",
        "lock_timeout",
    ] {
        assert!(
            s.contains(needle),
            "the large-live-table runbook must name `{needle}` — an operator \
             deciding whether they need a maintenance window has to be able to \
             tell which steps block writes and which do not"
        );
    }
    assert!(
        s.contains("Recheck the cutover"),
        "a cutover baked in when the plan was printed can be stale by the time \
         it runs; the doc must say so and say which direction is safe"
    );
}

#[test]
fn the_doc_tells_an_operator_how_to_answer_why_space_has_not_come_back() {
    let raw = doc();
    let s = flat(section(&raw, "Operating it").expect("an `## Operating it` section"));
    assert!(
        s.contains("harvest partition status"),
        "there must be one command to run"
    );
    // Sourced from the code, not copied: a hand-written list here would go
    // stale the moment a reason is reworded or a fourth is added, and the guard
    // would stay green while the doc stopped matching what an operator sees.
    for reason in autumn_harvest::partition::SWEEP_REASONS {
        assert!(
            s.contains(reason),
            "every reason string the sweeper can emit must be explained in the \
             doc: `{reason}`. An operator meeting one in a status dump needs to \
             know what to do about it"
        );
    }
}

#[test]
fn the_doc_states_that_the_migration_is_inert_and_the_layout_opt_in() {
    let d = flat(&doc());
    assert!(
        d.contains("is **inert**"),
        "the single most important fact for the 99% of deployments that will \
         never opt in: applying the migration changes nothing"
    );
    assert!(
        d.contains("half-converted cluster is a supported state"),
        "and per-shard independence must be stated — an operator converting a \
         20-shard cluster needs to know a partial rollout is safe"
    );
}

#[test]
fn the_doc_gives_the_default_partition_its_purpose() {
    let d = flat(&doc());
    assert!(
        d.contains("no partition of relation found"),
        "the DEFAULT partition exists to prevent one specific failure — an \
         append error that stalls a live workflow — and the doc must name it, \
         or a later cleanup will remove the partition as redundant"
    );
}
