//! Stratified in-flight history sampling for the replay-drift gate (issue #798).
//!
//! Replay determinism is harvest's load-bearing production invariant: a code
//! change that alters the command sequence of a `#[workflow]` function
//! terminally fails every in-flight execution whose recorded history no longer
//! matches. This module carries the **pure** vocabulary shared by the three
//! parties in the gate loop, so the wire shape cannot drift between them:
//!
//! 1. the management API (`GET /admin/history/export-sample`) — which *produces*
//!    a stratified, cross-shard sample of currently non-terminal executions,
//! 2. the `harvest` CLI — which *writes* the sample to a bundle directory
//!    alongside a [`SampleManifest`], and
//! 3. [`crate::testing::ReplayVerifier::replay_bundle`] — which *reads* that
//!    bundle back and gates CI on any divergence.
//!
//! Nothing here touches the database, so every rule below is unit-testable
//! without Postgres.
//!
//! # Why "in flight" is a narrower set than it looks
//!
//! Issue #798 specifies a default state set of `RUNNING,SUSPENDED,PAUSED`, but
//! `SUSPENDED` is **not a persisted state** — the `harvest_workflow_executions`
//! state `CHECK` constraint forbids it, and a workflow parked on a timer,
//! signal, activity, or child workflow keeps `state = 'RUNNING'` with a
//! detached `worker_id`. [`normalize_sample_states`] therefore accepts
//! `SUSPENDED` as a documented **alias for `RUNNING`**, so the issue's literal
//! default resolves to the correct non-terminal set, [`IN_FLIGHT_STATES`].
//!
//! Terminal states are rejected outright rather than silently sampled: the gate
//! exists to protect runs that still need to resume, and a terminal execution
//! can never resume.

use serde::{Deserialize, Serialize};

/// The persisted non-terminal states — the executions the drift gate protects.
///
/// `PAUSED` (issue #383) is a non-terminal *active* state and must be
/// enumerated everywhere in-flight runs are, so it sits alongside `RUNNING`
/// rather than being silently omitted.
pub const IN_FLIGHT_STATES: &[&str] = &["RUNNING", "PAUSED"];

/// Default number of executions sampled per registered workflow type.
pub const DEFAULT_PER_WORKFLOW_SAMPLE: usize = 50;

/// Upper bound on `per_workflow`, so one request cannot ask for an unbounded
/// slice of the fleet.
pub const MAX_PER_WORKFLOW_SAMPLE: usize = 500;

/// Upper bound on the total number of histories one sample request returns,
/// across every workflow type and every shard.
pub const MAX_SAMPLE_TOTAL: usize = 2_000;

/// Request-wide budget for the serialized bytes one sample export may hold in
/// memory before responding.
///
/// [`MAX_SAMPLE_TOTAL`] bounds the fixture *count*, but the per-execution
/// `max_bytes` ceiling is caller-raisable and applies to each document
/// independently, so the count cap alone leaves the *aggregate* unbounded: at
/// the 10 MiB default that is ~20 GiB of documents accumulated before a single
/// response is serialized. The management API is a shared service, so a routine
/// authenticated CI export must not be able to exhaust it.
///
/// The budget is checked *before* fetching each history — the size of a document
/// is not knowable until it is loaded — so peak retained bytes are bounded by
/// this budget plus at most one document. That is a bound only if one document is
/// itself bounded, so the sample route also **rejects** a `max_bytes` above this
/// budget: without that ceiling the budget starts at zero, the first candidate is
/// therefore always fetched and retained however large it is, and a single
/// multi-gigabyte history exhausts the API despite this cap. With the ceiling,
/// peak retained is at most twice this budget.
///
/// A per-document limit larger than the whole response budget is also incoherent
/// — such a document could never be returned within the budget — so it is
/// refused at the edge rather than loaded and then failed.
///
/// Hitting the budget is **never silent**: the export stops and
/// [`SampleManifest::truncated_by_size`] records it, so the gate reports a
/// bundle that is smaller than the sample the operator asked for rather than
/// certifying a build against a quietly-trimmed subset.
pub const MAX_SAMPLE_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// How the stratified sample orders candidates within each workflow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SampleOrder {
    /// Oldest in-flight executions first (the default).
    ///
    /// This is the risk-weighted choice: the longer a run has been in flight,
    /// the more of its history was recorded by *older* code, so it is the most
    /// likely to diverge against a candidate build — and the most expensive to
    /// lose, since it has the most work already invested.
    #[default]
    Oldest,
    /// Most recently started executions first.
    Newest,
}

impl SampleOrder {
    /// The wire/query-parameter spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Oldest => "oldest",
            Self::Newest => "newest",
        }
    }

    /// Parse a query-parameter value.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when `value` is not a known ordering.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "oldest" => Ok(Self::Oldest),
            "newest" => Ok(Self::Newest),
            other => Err(format!(
                "unknown sample_order '{other}'; expected one of [\"oldest\", \"newest\"]"
            )),
        }
    }
}

/// Cross-shard completeness of a sample.
///
/// A sample drawn while a shard was unreachable covers less than the fleet.
/// That is reported, never hidden — an operator gating a deploy needs to know
/// the difference between "no drift" and "no drift *in the part we could see*".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStatus {
    /// Every expected shard was inspected.
    Complete,
    /// At least one shard was inspected and at least one was unreachable.
    Partial,
    /// No shard could be inspected.
    Unavailable,
}

impl SampleStatus {
    /// Derive the status from the inspected / unavailable shard counts.
    #[must_use]
    pub const fn from_counts(inspected: usize, unavailable: usize) -> Self {
        if unavailable == 0 {
            Self::Complete
        } else if inspected == 0 {
            Self::Unavailable
        } else {
            Self::Partial
        }
    }

    /// Whether every expected shard was inspected.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

/// Per-workflow-type sample coverage: how many were sampled, out of how many
/// are actually in flight.
///
/// This is the anti-silent-truncation record required by issue #798 AC2. An
/// operator reading `sampled: 50, in_flight_total: 4000` knows the gate
/// verified a slice, not the fleet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleWorkflowCoverage {
    /// The registered workflow type name.
    pub workflow_name: String,
    /// How many executions of this type the sample actually contains.
    pub sampled: u64,
    /// How many executions of this type were in flight when the sample was
    /// drawn, across every inspected shard.
    pub in_flight_total: u64,
    /// The execution ids the export actually wrote a fixture for, sorted.
    ///
    /// The inventory the gate reconciles the delivered bundle against. Counts
    /// alone — even per-type counts — accept any *substitution* that preserves
    /// them: swap one execution of this type for a duplicate of another, and
    /// `sampled` still matches while a real in-flight history was never
    /// replayed. Only identities can tell "the bundle I was given" apart from
    /// "a bundle with the right shape".
    ///
    /// Exactly the survivors, so this agrees with
    /// [`sampled`](Self::sampled) by construction: a candidate the export
    /// selected but could not fetch contributes neither an id nor a count (it
    /// is reported through
    /// [`SampleManifest::export_failures`](SampleManifest::export_failures)
    /// instead).
    ///
    /// `#[serde(default)]` so a manifest written before this field existed
    /// reads back as empty rather than failing the whole bundle. An empty list
    /// is therefore *no claim*, not a claim of zero — the gate falls back to
    /// the per-type count check for such a manifest rather than reporting every
    /// fixture as unexpected.
    #[serde(default)]
    pub sampled_execution_ids: Vec<crate::types::ExecutionId>,
}

impl SampleWorkflowCoverage {
    /// Whether the in-flight population exceeded what was sampled.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        self.sampled < self.in_flight_total
    }
}

/// The bundle manifest written alongside the exported histories.
///
/// [`crate::testing::ReplayVerifier::replay_bundle`] reads this back so the CI
/// gate can report — and optionally enforce — the coverage the sample actually
/// achieved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleManifest {
    /// When the sample was drawn.
    pub generated_at: chrono::DateTime<chrono::Utc>,
    /// Cross-shard completeness of the sample.
    pub status: SampleStatus,
    /// The normalized non-terminal states the sample was drawn from.
    pub states: Vec<String>,
    /// Per-workflow-type coverage, sorted by workflow name.
    pub per_workflow: Vec<SampleWorkflowCoverage>,
    /// Total histories in the bundle.
    pub sampled_total: u64,
    /// Total in-flight executions across every inspected shard.
    pub in_flight_total: u64,
    /// Human-readable reasons for each shard that could not be inspected.
    pub unavailable_shards: Vec<String>,
    /// The shard ids that were successfully inspected.
    pub inspected_shards: Vec<i32>,
    /// Whether the export stopped early because it reached
    /// [`MAX_SAMPLE_RESPONSE_BYTES`].
    ///
    /// Distinct from [`is_truncated`](Self::is_truncated), and the distinction
    /// is what an operator acts on: that method reports the *intended*
    /// truncation of sampling `per_workflow` out of a larger population, while
    /// this reports an *unplanned* resource limit. The fix differs — narrow the
    /// export (fewer states, one shard, a lower `max_bytes`) rather than raise
    /// `--per-workflow`.
    ///
    /// `#[serde(default)]` so a manifest written before this field existed
    /// reads back as `false` rather than failing the whole bundle.
    #[serde(default)]
    pub truncated_by_size: bool,
    /// How many candidates the sample *selected* but the export could not
    /// produce a fixture for — a history over `max_bytes`, or a shard that
    /// became unreadable between selection and fetch.
    ///
    /// Load-bearing, because no other field can express it. The export records
    /// a failure instead of a document and continues, so
    /// [`sampled_total`](Self::sampled_total) counts only the *survivors* and
    /// therefore agrees exactly with the fixture count in the bundle. Without
    /// this the gate replays a silently smaller, biased sample — biased against
    /// the largest histories, which are the longest-running and so the most
    /// likely to span the code change under test — and reports it as a pass.
    ///
    /// `#[serde(default)]` so a manifest written before this field existed
    /// reads back as `0` rather than failing the whole bundle.
    #[serde(default)]
    pub export_failures: u64,
}

impl SampleManifest {
    /// The file name the manifest is written under inside a bundle directory.
    ///
    /// Deliberately not `manifest.json`: the bundle is a flat directory of
    /// `*.json` history fixtures, and the replay gate walks every `*.json` file
    /// in it. A distinctive, greppable name means the manifest can be excluded
    /// by name with zero risk of colliding with a real fixture.
    pub const FILE_NAME: &'static str = "harvest-sample-manifest.json";

    /// Whether every expected shard was inspected.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.status.is_complete()
    }

    /// Whether any workflow type's in-flight population exceeded its sample.
    #[must_use]
    pub const fn is_truncated(&self) -> bool {
        self.sampled_total < self.in_flight_total
    }

    /// Whether the bundle holds **fewer fixtures than the sample selected**.
    ///
    /// Deliberately distinct from [`is_truncated`](Self::is_truncated). That
    /// method reports the *intended* truncation of drawing `per_workflow` out of
    /// a larger population — the normal, documented mode of the gate, and never
    /// on its own a reason to fail. This one reports that the export could not
    /// deliver even the slice it chose, which is an *unplanned* shortfall the
    /// gate must not certify a build against.
    ///
    /// Both causes are the same defect from the gate's point of view — the
    /// verified set is a silent, biased subset of the requested one — so they
    /// share one predicate rather than two rungs a caller could handle
    /// inconsistently:
    ///
    /// * [`truncated_by_size`](Self::truncated_by_size) — the export hit
    ///   [`MAX_SAMPLE_RESPONSE_BYTES`] and stopped early.
    /// * [`export_failures`](Self::export_failures) — individual selected
    ///   candidates could not be fetched.
    #[must_use]
    pub const fn is_incomplete_export(&self) -> bool {
        self.truncated_by_size || self.export_failures > 0
    }
}

/// Normalize a caller-supplied set of execution states into the persisted
/// non-terminal states to sample.
///
/// * An empty input yields [`IN_FLIGHT_STATES`].
/// * `SUSPENDED` is accepted as an alias for `RUNNING` (a parked run's
///   persisted state *is* `RUNNING`; see the module docs).
/// * Terminal states are rejected — replaying terminal executions is explicitly
///   out of scope for the drift gate.
/// * Matching is case-insensitive; the result is deduplicated and sorted so a
///   given request always produces the same query and the same manifest.
///
/// # Errors
///
/// Returns a human-readable message when a state is terminal or unknown. The
/// caller maps this to `400 Bad Request` — an unrecognized state is never
/// silently dropped, which would narrow the sample without telling anyone.
pub fn normalize_sample_states<S: AsRef<str>>(raw: &[S]) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = Vec::new();
    let push = |state: &str, out: &mut Vec<String>| {
        if !out.iter().any(|existing| existing == state) {
            out.push(state.to_string());
        }
    };

    for entry in raw {
        for token in entry.as_ref().split(',') {
            let trimmed = token.trim();
            if trimmed.is_empty() {
                continue;
            }
            let upper = trimmed.to_ascii_uppercase();
            match upper.as_str() {
                // `SUSPENDED` is not a persisted state; a parked run is RUNNING.
                "SUSPENDED" | "RUNNING" => push("RUNNING", &mut out),
                "PAUSED" => push("PAUSED", &mut out),
                other if crate::erase::is_terminal_state(other) => {
                    return Err(format!(
                        "state '{trimmed}' is terminal; the replay-drift gate samples only \
                         non-terminal executions (one of {IN_FLIGHT_STATES:?}, or the \
                         'SUSPENDED' alias for RUNNING)"
                    ));
                }
                _ => {
                    return Err(format!(
                        "unknown workflow state '{trimmed}'; expected one of {IN_FLIGHT_STATES:?} \
                         (or the 'SUSPENDED' alias for RUNNING)"
                    ));
                }
            }
        }
    }

    if out.is_empty() {
        out = IN_FLIGHT_STATES.iter().map(|s| (*s).to_string()).collect();
    }
    out.sort();
    Ok(out)
}

/// Clamp a caller-supplied `per_workflow` into `1..=MAX_PER_WORKFLOW_SAMPLE`.
///
/// Clamping (rather than rejecting) mirrors the existing batch-export `limit`
/// handling: an over-large request is bounded, and the manifest's coverage
/// numbers make the bound visible.
#[must_use]
pub const fn clamp_per_workflow(requested: usize) -> usize {
    if requested < 1 {
        1
    } else if requested > MAX_PER_WORKFLOW_SAMPLE {
        MAX_PER_WORKFLOW_SAMPLE
    } else {
        requested
    }
}

/// Merge per-shard coverage rows into one fleet-wide, name-sorted view.
///
/// A workflow type observed on several shards has its `sampled`,
/// `in_flight_total` and `sampled_execution_ids` merged, so `in_flight_total`
/// is the true cross-shard population rather than whichever shard answered
/// last.
#[must_use]
pub fn merge_coverage(per_shard: &[Vec<SampleWorkflowCoverage>]) -> Vec<SampleWorkflowCoverage> {
    let mut merged: std::collections::BTreeMap<String, SampleWorkflowCoverage> =
        std::collections::BTreeMap::new();
    for shard in per_shard {
        for row in shard {
            let entry =
                merged
                    .entry(row.workflow_name.clone())
                    .or_insert_with(|| SampleWorkflowCoverage {
                        workflow_name: row.workflow_name.clone(),
                        sampled: 0,
                        in_flight_total: 0,
                        sampled_execution_ids: Vec::new(),
                    });
            entry.sampled = entry.sampled.saturating_add(row.sampled);
            entry.in_flight_total = entry.in_flight_total.saturating_add(row.in_flight_total);
            entry
                .sampled_execution_ids
                .extend(row.sampled_execution_ids.iter().copied());
        }
    }
    // Sorted so the manifest is byte-stable across shard iteration order: two
    // exports of the same sample must produce the same manifest, or a diff of
    // two CI artifacts reports noise.
    for entry in merged.values_mut() {
        entry
            .sampled_execution_ids
            .sort_unstable_by_key(crate::types::ExecutionId::as_uuid);
    }
    merged.into_values().collect()
}

/// Build a filesystem-safe fixture file name for one sampled execution.
///
/// The execution id is a UUID and therefore inherently safe; the workflow name
/// is registry-supplied but is sanitized anyway so a hostile or merely awkward
/// workflow name can never escape the bundle directory (`../`, absolute paths,
/// path separators, NUL) when the CLI writes the file.
#[must_use]
pub fn fixture_file_name(workflow_name: &str, execution_id: &str) -> String {
    let mut safe: String = workflow_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if safe.is_empty() {
        safe.push_str("workflow");
    }
    let safe_id: String = execution_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .take(64)
        .collect();
    format!("{safe}--{safe_id}.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_states_default_to_the_in_flight_set() {
        let states = normalize_sample_states::<&str>(&[]).unwrap();
        assert_eq!(states, vec!["PAUSED".to_string(), "RUNNING".to_string()]);
    }

    #[test]
    fn suspended_is_an_alias_for_running() {
        // Issue #798's literal default is RUNNING,SUSPENDED,PAUSED. SUSPENDED is
        // not a persisted state, so it must fold into RUNNING rather than either
        // erroring or silently matching nothing.
        let states = normalize_sample_states(&["RUNNING,SUSPENDED,PAUSED"]).unwrap();
        assert_eq!(states, vec!["PAUSED".to_string(), "RUNNING".to_string()]);
    }

    #[test]
    fn suspended_alone_resolves_to_running() {
        let states = normalize_sample_states(&["SUSPENDED"]).unwrap();
        assert_eq!(states, vec!["RUNNING".to_string()]);
    }

    #[test]
    fn states_are_case_insensitive_and_deduplicated() {
        let states = normalize_sample_states(&["running", "RUNNING", " Paused "]).unwrap();
        assert_eq!(states, vec!["PAUSED".to_string(), "RUNNING".to_string()]);
    }

    #[test]
    fn terminal_states_are_rejected_with_a_specific_message() {
        for terminal in [
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ] {
            let err = normalize_sample_states(&[terminal]).unwrap_err();
            assert!(err.contains("terminal"), "{terminal}: {err}");
        }
    }

    #[test]
    fn unknown_states_are_rejected_never_silently_dropped() {
        let err = normalize_sample_states(&["RUNNING", "NOT_A_STATE"]).unwrap_err();
        assert!(err.contains("NOT_A_STATE"), "{err}");
    }

    #[test]
    fn per_workflow_is_clamped_into_range() {
        assert_eq!(clamp_per_workflow(0), 1);
        assert_eq!(clamp_per_workflow(1), 1);
        assert_eq!(clamp_per_workflow(50), 50);
        assert_eq!(
            clamp_per_workflow(MAX_PER_WORKFLOW_SAMPLE),
            MAX_PER_WORKFLOW_SAMPLE
        );
        assert_eq!(
            clamp_per_workflow(MAX_PER_WORKFLOW_SAMPLE + 1),
            MAX_PER_WORKFLOW_SAMPLE
        );
        assert_eq!(clamp_per_workflow(usize::MAX), MAX_PER_WORKFLOW_SAMPLE);
    }

    #[test]
    fn coverage_truncation_is_reported() {
        let full = SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 10,
            in_flight_total: 10,
            sampled_execution_ids: Vec::new(),
        };
        assert!(!full.truncated());
        let partial = SampleWorkflowCoverage {
            workflow_name: "wf".into(),
            sampled: 10,
            in_flight_total: 4000,
            sampled_execution_ids: Vec::new(),
        };
        assert!(partial.truncated());
    }

    #[test]
    fn merge_sums_across_shards_and_sorts_by_name() {
        // Identities from two different shards must both survive the merge --
        // a manifest that dropped one shard's ids would declare a bundle the
        // gate could never reconcile.
        let alpha_shard0 = crate::types::ExecutionId::new();
        let alpha_shard1 = crate::types::ExecutionId::new();
        let zeta_id = crate::types::ExecutionId::new();

        let shard0 = vec![
            SampleWorkflowCoverage {
                workflow_name: "zeta".into(),
                sampled: 2,
                in_flight_total: 9,
                sampled_execution_ids: vec![zeta_id],
            },
            SampleWorkflowCoverage {
                workflow_name: "alpha".into(),
                sampled: 3,
                in_flight_total: 5,
                sampled_execution_ids: vec![alpha_shard0],
            },
        ];
        let shard1 = vec![SampleWorkflowCoverage {
            workflow_name: "alpha".into(),
            sampled: 1,
            in_flight_total: 7,
            sampled_execution_ids: vec![alpha_shard1],
        }];

        let merged = merge_coverage(&[shard0, shard1]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].workflow_name, "alpha");
        assert_eq!(merged[0].sampled, 4, "sampled sums across shards");
        assert_eq!(
            merged[0].in_flight_total, 12,
            "in-flight population sums across shards"
        );

        let mut expected = vec![alpha_shard0, alpha_shard1];
        expected.sort_unstable_by_key(crate::types::ExecutionId::as_uuid);
        assert_eq!(
            merged[0].sampled_execution_ids, expected,
            "identities merge across shards and land in a stable order"
        );
        assert_eq!(merged[1].workflow_name, "zeta");
        assert_eq!(merged[1].sampled_execution_ids, vec![zeta_id]);
    }

    #[test]
    fn merge_of_nothing_is_empty() {
        assert!(merge_coverage(&[]).is_empty());
        assert!(merge_coverage(&[vec![]]).is_empty());
    }

    #[test]
    fn status_is_derived_from_shard_counts() {
        assert_eq!(SampleStatus::from_counts(2, 0), SampleStatus::Complete);
        assert_eq!(SampleStatus::from_counts(1, 1), SampleStatus::Partial);
        assert_eq!(SampleStatus::from_counts(0, 2), SampleStatus::Unavailable);
        assert!(SampleStatus::from_counts(2, 0).is_complete());
        assert!(!SampleStatus::from_counts(1, 1).is_complete());
    }

    #[test]
    fn sample_order_round_trips_and_rejects_unknown() {
        assert_eq!(SampleOrder::parse("oldest").unwrap(), SampleOrder::Oldest);
        assert_eq!(SampleOrder::parse(" NEWEST ").unwrap(), SampleOrder::Newest);
        assert_eq!(SampleOrder::default(), SampleOrder::Oldest);
        assert_eq!(SampleOrder::Oldest.as_str(), "oldest");
        assert!(SampleOrder::parse("random").is_err());
    }

    #[test]
    fn fixture_file_names_cannot_escape_the_bundle_directory() {
        let name = fixture_file_name("../../etc/passwd", "abc-123");
        assert!(!name.contains('/'), "{name}");
        assert!(!name.contains(".."), "{name}");
        // Each of the six `.`/`/` characters maps to `_`; the trailing UUID keeps
        // the whole file name unique even when two workflow names sanitize alike.
        assert_eq!(name, "______etc_passwd--abc-123.json");
    }

    #[test]
    fn fixture_file_names_survive_awkward_workflow_names() {
        assert_eq!(fixture_file_name("", "id"), "workflow--id.json");
        assert_eq!(fixture_file_name("a b:c", "id"), "a_b_c--id.json");
        // A long name is bounded so the path stays writable on every platform.
        let long = fixture_file_name(&"x".repeat(500), &"y".repeat(500));
        assert!(long.len() <= 64 + 2 + 64 + 5, "{}", long.len());
    }

    #[test]
    fn manifest_file_name_is_distinct_from_a_plausible_fixture_name() {
        assert!(
            std::path::Path::new(SampleManifest::FILE_NAME)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
        );
        assert!(SampleManifest::FILE_NAME.starts_with("harvest-"));
    }

    #[test]
    fn manifest_round_trips_through_json() {
        let manifest = SampleManifest {
            generated_at: chrono::Utc::now(),
            status: SampleStatus::Partial,
            states: vec!["RUNNING".into()],
            per_workflow: vec![SampleWorkflowCoverage {
                workflow_name: "wf".into(),
                sampled: 1,
                in_flight_total: 2,
                sampled_execution_ids: vec![crate::types::ExecutionId::new()],
            }],
            sampled_total: 1,
            in_flight_total: 2,
            unavailable_shards: vec!["shard 1: down".into()],
            inspected_shards: vec![0],
            truncated_by_size: false,
            export_failures: 0,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"status\":\"partial\""), "{json}");
        let back: SampleManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, manifest);
        assert!(back.is_truncated());
        assert!(!back.is_complete());
    }

    /// A manifest written before `truncated_by_size` existed must still parse.
    ///
    /// The gate reads bundles produced by whatever version exported them, so a
    /// missing field has to default rather than fail the whole bundle — which
    /// would turn an older artifact into a blocked gate.
    #[test]
    fn manifest_without_truncated_by_size_defaults_to_false() {
        let legacy = serde_json::json!({
            "generated_at": "2026-01-01T00:00:00Z",
            "status": "complete",
            "states": ["RUNNING"],
            "per_workflow": [],
            "sampled_total": 0,
            "in_flight_total": 0,
            "unavailable_shards": [],
            "inspected_shards": [0],
        });
        let manifest: SampleManifest = serde_json::from_value(legacy).unwrap();
        assert!(!manifest.truncated_by_size);
        assert_eq!(
            manifest.export_failures, 0,
            "a pre-#798 manifest must read back as a complete export, not block \
             every legacy bundle"
        );
        assert!(!manifest.is_incomplete_export());
    }

    /// The two shortfall causes are different defects with different fixes, but
    /// the gate must treat them identically: either one means the bundle is a
    /// silent subset of the slice the sample chose.
    #[test]
    fn incomplete_export_covers_both_a_size_cut_and_dropped_candidates() {
        let base = SampleManifest {
            generated_at: chrono::Utc::now(),
            status: SampleStatus::Complete,
            states: vec!["RUNNING".into()],
            per_workflow: vec![],
            sampled_total: 10,
            in_flight_total: 10,
            unavailable_shards: vec![],
            inspected_shards: vec![0],
            truncated_by_size: false,
            export_failures: 0,
        };
        assert!(
            !base.is_incomplete_export(),
            "a clean export must not be flagged"
        );

        let size_cut = SampleManifest {
            truncated_by_size: true,
            ..base.clone()
        };
        assert!(size_cut.is_incomplete_export());

        let dropped = SampleManifest {
            export_failures: 1,
            ..base.clone()
        };
        assert!(dropped.is_incomplete_export());

        // Orthogonal to ordinary (intended) truncation: sampling a slice of a
        // larger population is the gate's normal mode and is NOT a shortfall.
        let intended_slice = SampleManifest {
            sampled_total: 10,
            in_flight_total: 4_000,
            ..base
        };
        assert!(
            intended_slice.is_truncated(),
            "10 of 4000 is truncated coverage"
        );
        assert!(
            !intended_slice.is_incomplete_export(),
            "intended truncation must never be confused with an export shortfall"
        );
    }

    /// The aggregate response budget must actually bound the count cap.
    ///
    /// `MAX_SAMPLE_TOTAL` alone leaves the retained bytes unbounded, because
    /// `max_bytes` is per-document and caller-raisable. This pins the budget as
    /// the load-bearing bound rather than a decorative constant: it has to be
    /// smaller than what the count cap permits at the default per-document
    /// ceiling, or it never binds.
    #[test]
    fn response_byte_budget_binds_below_the_count_cap() {
        let unbounded_worst_case =
            MAX_SAMPLE_TOTAL * crate::history_export::DEFAULT_HISTORY_EXPORT_MAX_BYTES;
        assert!(
            MAX_SAMPLE_RESPONSE_BYTES < unbounded_worst_case,
            "the byte budget must bind before the count cap does: {MAX_SAMPLE_RESPONSE_BYTES} \
             vs {unbounded_worst_case}"
        );
        // And it must still admit a realistic bundle rather than cutting normal
        // exports short: the default sample is 50 per type, and a typical
        // in-flight history is well under 1 MiB.
        let realistic_bundle = DEFAULT_PER_WORKFLOW_SAMPLE * 1024 * 1024;
        assert!(
            MAX_SAMPLE_RESPONSE_BYTES >= realistic_bundle,
            "the budget must not cut an ordinary default-sized export short: \
             {MAX_SAMPLE_RESPONSE_BYTES} vs {realistic_bundle}"
        );
    }
}
