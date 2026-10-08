//! Guards that keep the cross-region DR documentation honest (issue #954).
//!
//! No DB, no feature gate. Each assertion below pins a claim that is either
//! **load-bearing for an operator following the runbook under pressure** or
//! **dangerous to quietly lose in a later editing pass**. The two we care most
//! about are the honest limits: fencing does not stop a live, partitioned old
//! primary, and a promoted *logical* standby needs its sequences advanced.
//! Both are the kind of caveat that reads like a nit in review and costs a
//! forked history in an incident.

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn read_doc(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

fn topology() -> String {
    read_doc("docs/cross-region-dr.md")
}

fn runbook() -> String {
    read_doc("docs/runbooks/cross-region-failover.md")
}

/// Body of the `## <heading>` section, up to the next `## `.
fn section<'a>(doc: &'a str, heading: &str) -> Option<&'a str> {
    let needle = format!("\n## {heading}");
    let start = doc.find(&needle)? + 1;
    let rest = &doc[start..];
    let end = rest[1..].find("\n## ").map_or(rest.len(), |i| i + 1);
    Some(&rest[..end])
}

// ── AC1: topology, and no new infrastructure in core ───────────────────────

#[test]
fn the_topology_doc_states_that_core_ships_no_replication() {
    let doc = topology();
    assert!(
        doc.contains("stock Postgres"),
        "the doc must say replication is stock Postgres, not something Harvest ships"
    );
    for absent in [
        "logical replication",
        "physical",
        "publication",
        "subscription",
    ] {
        assert!(
            doc.to_lowercase().contains(absent),
            "the topology doc must cover {absent}"
        );
    }
}

#[test]
fn the_topology_doc_states_the_no_new_event_variant_invariant() {
    let doc = topology();
    assert!(
        doc.contains("WorkflowEvent"),
        "the doc must state the no-new-event-variant invariant explicitly (issue #954 AC2)"
    );
    assert!(
        doc.contains("harvest_shard_generation"),
        "the doc must name the fencing table an operator will query"
    );
}

// ── The two honest limits ──────────────────────────────────────────────────

/// The most important paragraph in the whole feature.
///
/// A reader who takes "fencing prevents split-brain" unqualified will skip the
/// step that actually isolates the old primary, and a partitioned old region
/// will keep writing to its own database while believing it cannot. The doc
/// must say so *and* must say the isolation step is mandatory.
#[test]
fn the_topology_doc_refuses_to_oversell_the_fence() {
    let doc = topology();
    let limits = section(&doc, "What fencing does not do")
        .expect("the doc must carry a `## What fencing does not do` section");
    assert!(
        limits.to_lowercase().contains("partition"),
        "the limits section must name the partitioned-old-primary case: {limits}"
    );
    assert!(
        limits.contains("mandatory") || limits.contains("MUST"),
        "the limits section must mark isolating the old primary as mandatory, not advisory"
    );

    let rb = runbook();
    assert!(
        rb.to_lowercase().contains("isolate"),
        "the runbook must carry an explicit isolate-the-old-primary step"
    );
}

/// Logical replication does not replicate sequence values.
///
/// Discovered by the drill, not by reading: `harvest_events.id` is a
/// `BIGSERIAL`, so a promoted logical standby holds every replicated row while
/// its sequence still sits where it started, and the new primary's first append
/// dies on a duplicate key. Both docs must carry the step.
#[test]
fn both_docs_carry_the_sequence_advance_step_for_logical_standbys() {
    for (name, doc) in [("topology", topology()), ("runbook", runbook())] {
        assert!(
            doc.contains("sequence"),
            "the {name} doc must cover the un-replicated-sequence hazard"
        );
        assert!(
            doc.contains("advance_sequences_after_promotion") || doc.contains("harvest dr promote"),
            "the {name} doc must name the tool that advances sequences after promotion"
        );
    }
}

// ── AC3: the runbook's order is the whole safety argument ──────────────────

/// Fence → promote → verify → start workers, in that order.
///
/// Starting workers before verification is how a bad restore becomes a
/// corrupted live region; verifying before fencing is how the old region gets
/// to keep writing during the verification window. The order is the procedure.
#[test]
fn the_runbook_orders_fence_before_promote_before_verify_before_workers() {
    let rb = runbook().to_lowercase();
    let fence = rb.find("### 1.").expect("step 1 must exist");
    let promote = rb.find("### 2.").expect("step 2 must exist");
    let verify = rb.find("### 3.").expect("step 3 must exist");
    let workers = rb.find("### 4.").expect("step 4 must exist");
    assert!(fence < promote && promote < verify && verify < workers);

    let step1 = &rb[fence..promote];
    assert!(step1.contains("fence"), "step 1 must be the fence: {step1}");
    let step3 = &rb[verify..workers];
    assert!(
        step3.contains("verify"),
        "step 3 must be the verification: {step3}"
    );
    assert!(
        step3.contains("replay") && step3.contains("scanner"),
        "verification must reuse the backup-verify resumability checks (replayer sample, \
         scanner dry-run): {step3}"
    );
}

/// A physical standby is read-only until it is promoted.
///
/// `harvest dr fence` issues `LOCK TABLE` and `UPDATE`, so the literal
/// fence-then-promote order works only for a **logical** standby (an ordinary
/// writable database). On the physical topology the command simply fails, and
/// an operator following the runbook is stuck at step 2 with the clock running.
/// The runbook has to say so, and the safety argument has to survive the
/// reordering — it does, because it rests on nothing writing before the fence,
/// and workers do not start until step 4.
#[test]
fn the_runbook_defers_the_fence_past_promotion_for_physical_standbys() {
    let rb = runbook();
    let lower = rb.to_lowercase();
    assert!(
        lower.contains("read-only until"),
        "the runbook must say a physical standby cannot be written to before promotion"
    );
    // The deferred bump must actually appear in the promote step, not merely be
    // described in prose.
    let promote = rb
        .find("### 2.")
        .and_then(|start| {
            rb[start..]
                .find("### 3.")
                .map(|end| &rb[start..start + end])
        })
        .expect("step 2 must exist");
    assert!(
        promote.contains("pg_ctl promote"),
        "step 2 must carry the physical promotion command"
    );
    assert!(
        promote.contains("harvest dr fence"),
        "step 2 must carry the deferred fence for the physical topology, not just mention it: \
         {promote}"
    );
    // And the safety argument must be stated, not left implicit.
    assert!(
        lower.contains("workers do not start until step 4")
            || lower.contains("no worker starts until step 4"),
        "the runbook must explain why deferring the fence is still safe"
    );
}

#[test]
fn the_runbook_documents_fail_back() {
    let rb = runbook();
    let failback = section(&rb, "Fail-back")
        .expect("AC3 requires a documented fail-back procedure (`## Fail-back`)");
    assert!(
        failback.contains("generation") || failback.contains("epoch"),
        "fail-back must explain what happens to the write-authority epoch"
    );
    assert!(
        failback.len() > 400,
        "fail-back must be a real procedure, not a sentence"
    );
}

// ── AC4: what the RPO number actually means at failover time ───────────────

#[test]
fn the_runbook_states_plainly_what_lag_means_at_failover_time() {
    let rb = runbook();
    let rpo = section(&rb, "RPO: what the number means")
        .expect("AC4 requires the runbook to state plainly what lag means at failover");
    assert!(
        rpo.contains("re-execute") || rpo.contains("re-execution"),
        "AC4: the runbook must say side effects from the lost window may RE-EXECUTE \
         under the at-least-once contract: {rpo}"
    );
    assert!(
        rpo.contains("at-least-once"),
        "AC4: the runbook must tie the lost window to the at-least-once contract"
    );
    assert!(
        rpo.contains("harvest.replication.lag_seconds"),
        "the runbook must name the metric operators will read"
    );
    assert!(
        rpo.contains("unknown") || rpo.contains("absent"),
        "the runbook must explain that a MISSING lag series is worse than a large one, \
         not better: {rpo}"
    );
}

// ── AC5: multi-shard skew honesty ──────────────────────────────────────────

#[test]
fn the_topology_doc_names_the_cross_shard_skew_hazards() {
    let doc = topology();
    let skew =
        section(&doc, "Multi-shard skew").expect("AC5 requires a `## Multi-shard skew` section");
    assert!(
        skew.contains("Requested"),
        "AC5 names the outbox `*Requested`-without-its-terminal hazard: {skew}"
    );
    assert!(
        skew.contains("parent") && skew.contains("child"),
        "AC5 names the parent/child skew hazard: {skew}"
    );
    let discipline = skew.to_lowercase();
    assert!(
        discipline.contains("fence all") && discipline.contains("verify all"),
        "AC5 requires the same 'fence all, verify all, then start workers' discipline \
         as the restore runbook: {skew}"
    );
}

// ── AC6: the drill is runnable, and says how ───────────────────────────────

#[test]
fn the_runbook_carries_a_runnable_drill_with_the_three_proofs() {
    let rb = runbook();
    let drill = section(&rb, "Failover drill").expect("AC6 requires a `## Failover drill` section");
    assert!(
        drill.contains("cross_region_dr_tests"),
        "the drill must point at the integration suite that proves it: {drill}"
    );
    assert!(
        drill.contains("wal_level"),
        "the drill must state the wal_level=logical prerequisite"
    );
    assert!(
        drill.contains("15 minutes") || drill.contains("15-minute"),
        "the drill must state the RTO target it is measuring against"
    );
    for proof in ["cannot claim", "resume", "lag"] {
        assert!(
            drill.contains(proof),
            "the drill must name the '{proof}' proof: {drill}"
        );
    }
}

/// The one thing an operator must never do to recover a fenced worker.
#[test]
fn both_docs_forbid_re_pinning_a_fenced_worker() {
    for (name, doc) in [("topology", topology()), ("runbook", runbook())] {
        let lower = doc.to_lowercase();
        assert!(
            lower.contains("restart"),
            "the {name} doc must say a fenced worker is recovered by RESTARTING it"
        );
        assert!(
            lower.contains("never")
                && (lower.contains("adopt") || lower.contains("re-pin") || lower.contains("repin")),
            "the {name} doc must state that a worker must NEVER adopt the new epoch"
        );
    }
}

/// The docs must not promise something the code does not do.
#[test]
fn the_docs_do_not_promise_automatic_failover() {
    for (name, doc) in [("topology", topology()), ("runbook", runbook())] {
        let lower = doc.to_lowercase();
        assert!(
            lower.contains("operator-initiated") || lower.contains("operator initiated"),
            "the {name} doc must state that failover is operator-initiated, never automatic"
        );
    }
}
