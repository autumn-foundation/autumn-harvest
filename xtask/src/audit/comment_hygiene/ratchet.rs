//! The Tier B ratchet: fingerprints, the merge base, and the comparison.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use smop::prelude::*;

use super::rules::{Finding, findings_for_source};
use super::{TIER_B, rule_title};

/// Fingerprints per rule, then per path, each list sorted.
pub type Tally = BTreeMap<String, BTreeMap<String, Vec<String>>>;

/// Findings per rule and path, then per fingerprint, in scan order.
pub type FindingIndex<'f> = HashMap<(&'static str, &'f str), HashMap<String, Vec<&'f Finding>>>;

/// Groups fingerprints per rule and per file.
///
/// These are fingerprints, not counts. A count only answers "how many".
/// Take a change that removes one legacy violation and adds a different one
/// in the same file. It nets to zero and passes. But it adds exactly the
/// defect that the rule exists to stop. Identities answer "which", so the
/// new one is a regression even though the total never moved.
pub fn tally<'f>(findings: impl IntoIterator<Item = &'f Finding>) -> Tally {
    let mut grouped = Tally::new();
    for finding in findings {
        grouped
            .entry(finding.rule.to_string())
            .or_default()
            .entry(finding.path.clone())
            .or_default()
            .push(finding.fingerprint());
    }
    for files in grouped.values_mut() {
        for fingerprints in files.values_mut() {
            fingerprints.sort();
        }
    }
    grouped
}

/// The output of `git <args>`, with the newline translation of Python text
/// mode.
///
/// It is `None` when git cannot run, when it fails, or when its output is
/// not UTF-8. The Python original treated each of those as "no answer".
fn git(root: &Path, args: &[&str]) -> Option<String> {
    let output = sh::cmd("git").args(args).dir(root).output_raw().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// The Tier B findings at the merge base, for the changed files.
///
/// They come out of git, not from a baseline file in the tree. That
/// removes each problem that a stored baseline has. It cannot go stale when
/// the base branch moves. It needs no regeneration step, so nobody can
/// regenerate it to hide a new violation. It is stable across a rename,
/// because a moved file is compared against its own previous path. And it
/// keeps half a megabyte of generated fingerprints out of the tree.
pub fn baseline_from_merge_base(
    root: &Path,
    merge_base: &str,
    scope: &HashSet<String>,
    renames: &HashMap<String, String>,
) -> Tally {
    let mut paths: Vec<&String> = scope.iter().filter(|path| path.ends_with(".rs")).collect();
    paths.sort();
    let mut baseline: Vec<Finding> = Vec::new();
    for path in paths {
        let was_path = renames.get(path).unwrap_or(path);
        if !was_path.ends_with(".rs") {
            // Renamed into the Rust corpus. It has no audited history, so it
            // gets no allowance, and each finding in it belongs to this change.
            continue;
        }
        let Some(source) = git(root, &["show", &format!("{merge_base}:{was_path}")]) else {
            // Absent at the base: a file that this change adds. Nothing is
            // allowed, so each Tier B finding in it is from this change.
            continue;
        };
        baseline.extend(
            findings_for_source(path, &source)
                .into_iter()
                .filter(|finding| TIER_B.contains(&finding.rule)),
        );
    }
    tally(&baseline)
}

/// What this branch changed since its merge base with `base_ref`.
///
/// It returns the merge-base sha, the changed paths, and a map from the new
/// path to the old path for renames. It is `None` when the answer cannot be
/// trusted. That is the case with no git, with an unknown ref, or with a
/// shallow clone whose history does not reach the merge base. Callers treat `None` as "cannot scope", never
/// as "nothing changed".
///
/// The sha matters. A comparison against the tip of the branch would blame
/// this change for comments that the base branch added after the fork
/// point. Renames are tracked, because a moved file is compared against its
/// own previous path. Without that, each legacy finding in it looks new,
/// and an ordinary module rename fails CI over debt it did not add.
pub fn diff_context(
    root: &Path,
    base_ref: &str,
) -> Option<(String, HashSet<String>, HashMap<String, String>)> {
    let merge_base = git(root, &["merge-base", base_ref, "HEAD"])?;
    let merge_base = super::pyre::strip(&merge_base).to_string();
    if merge_base.is_empty() {
        return None;
    }
    // The NUL terminators that `-z` writes stay in place. A strip is for a
    // sha, not for a record separator.
    let listing = git(
        root,
        &[
            "diff",
            "-z",
            "--name-status",
            "--find-renames",
            &merge_base,
            "HEAD",
        ],
    )?;
    let (changed, renames) = parse_name_status(&listing);
    Some((merge_base, changed, renames))
}

/// The paths in a `git diff -z --name-status` listing, and its renames.
///
/// The listing uses `-z`, not the default. Git quotes a path that it cannot
/// write plainly. A name with a character outside ASCII, a tab or a newline
/// comes back as `"src/\\303\\251.rs"`, quotes and octal escapes included.
/// That string does not end in `.rs`. So the file left the Tier B scope in
/// silence, and the run reported "gating 0 changed .rs file(s)" and passed.
/// A ratchet that stops watching a file in silence is worse than one that
/// fails.
///
/// `-z` writes each field raw and ends it with a NUL, so there is nothing
/// to unquote. The records are `STATUS\\0path\\0`, and
/// `R100\\0old\\0new\\0` for a rename or a copy: the status first, then one
/// path or two.
pub fn parse_name_status(listing: &str) -> (HashSet<String>, HashMap<String, String>) {
    let fields: Vec<&str> = listing.split('\0').collect();
    let mut changed = HashSet::new();
    let mut renames = HashMap::new();
    let mut index = 0;
    while index < fields.len() {
        let status = fields[index];
        if status.is_empty() {
            index += 1;
            continue;
        }
        if status.starts_with(['R', 'C']) && index + 2 < fields.len() {
            changed.insert(fields[index + 2].to_string());
            renames.insert(fields[index + 2].to_string(), fields[index + 1].to_string());
            index += 3;
        } else if index + 1 < fields.len() {
            changed.insert(fields[index + 1].to_string());
            index += 2;
        } else {
            break;
        }
    }
    (changed, renames)
}

/// Indexes findings by rule and path, then by fingerprint, so a regression
/// can be located.
///
/// A count tells a contributor that something moved. It does not say what
/// to fix. In a file with 30 legacy findings, "30 total vs 29" means a
/// manual bisection. To keep the findings lets the failure name the line.
pub fn index_findings(findings: &[Finding]) -> FindingIndex<'_> {
    let mut index: FindingIndex<'_> = HashMap::new();
    for finding in findings {
        index
            .entry((finding.rule, finding.path.as_str()))
            .or_default()
            .entry(finding.fingerprint())
            .or_default()
            .push(finding);
    }
    index
}

/// Counts each fingerprint in `items`.
fn counter(items: &[String]) -> BTreeMap<&str, usize> {
    let mut counts = BTreeMap::new();
    for item in items {
        *counts.entry(item.as_str()).or_insert(0) += 1;
    }
    counts
}

/// The regressions only: a count that rose, or a file newly in violation.
///
/// `scope` limits the gate to the files that a change touches. That is not
/// a softer gate. It is what makes the ratchet usable. A count over the
/// whole corpus is a shared mutable number. So one merge that adds a long
/// comment anywhere turns each open PR red for a file its author never
/// opened. The predictable response is to regenerate the baseline, which
/// defeats the ratchet. A scope of changed files makes each PR answer only
/// for its own work. It leaves the baseline a stable record of legacy debt,
/// not a contended counter.
pub fn compare_tier_b(
    current: &Tally,
    baseline: &Tally,
    scope: Option<&HashSet<String>>,
    renames: &HashMap<String, String>,
    index: &FindingIndex<'_>,
) -> Vec<String> {
    let empty = BTreeMap::new();
    let mut regressions = Vec::new();
    for rule in TIER_B {
        let now = current.get(rule).unwrap_or(&empty);
        let was = baseline.get(rule).unwrap_or(&empty);
        for (path, fingerprints) in now {
            if scope.is_some_and(|scope| !scope.contains(path)) {
                continue;
            }
            let previous = was
                .get(path)
                .filter(|list| !list.is_empty())
                .or_else(|| was.get(renames.get(path).unwrap_or(path)));
            let allowed = previous.map(|list| counter(list)).unwrap_or_default();
            let mut added: BTreeMap<&str, usize> = BTreeMap::new();
            for (fingerprint, count) in counter(fingerprints) {
                let left = count.saturating_sub(allowed.get(fingerprint).copied().unwrap_or(0));
                if left > 0 {
                    added.insert(fingerprint, left);
                }
            }
            let total: usize = added.values().sum();
            if total == 0 {
                continue;
            }
            let allowed_total: usize = allowed.values().sum();
            regressions.push(format!(
                "{rule} {path}: {total} new finding(s), {} total vs {allowed_total} at the merge base [{}]",
                fingerprints.len(),
                rule_title(rule)
            ));
            // Name the actual lines. Without this the contributor learns
            // that a count moved, and must find which comment did it.
            let located = index.get(&(rule, path.as_str()));
            for (fingerprint, count) in added {
                let sites: &[&Finding] = located
                    .and_then(|by_print| by_print.get(fingerprint))
                    .map_or(&[], Vec::as_slice);
                // When the merge base already had this exact text, the
                // occurrences cannot be told apart, because the fingerprint
                // is the text. To name the first one names the legacy line,
                // and that sends the contributor to edit a comment that they
                // never wrote. So each candidate is named, with how many of
                // them are new.
                if sites.len() > count {
                    regressions.push(format!(
                        "    {count} of these {} identical comments {} new:",
                        sites.len(),
                        if count == 1 { "is" } else { "are" }
                    ));
                }
                for finding in sites {
                    regressions.push(format!(
                        "    {path}:{}: {}",
                        finding.line,
                        super::pyre::truncate(&finding.text, 100)
                    ));
                }
            }
        }
    }
    regressions
}

/// The Tier B regressions, or none when only the absolute gates apply.
///
/// `--tier-a-only` promises to check only the absolute gates. The text
/// report has kept that promise from the start, because it returns before
/// it reaches Tier B. The JSON path once ran the comparison anyway, and it
/// put a Tier B regression into its exit status. So the same two flags gave
/// two answers, and the answer for machines was the wrong one.
pub fn tier_b_gate(
    current: &Tally,
    baseline: &Tally,
    scope: Option<&HashSet<String>>,
    renames: &HashMap<String, String>,
    findings: &[Finding],
    tier_a_only: bool,
) -> Vec<String> {
    if tier_a_only {
        return Vec::new();
    }
    compare_tier_b(current, baseline, scope, renames, &index_findings(findings))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nul_listing_keeps_a_path_outside_ascii() {
        let listing = "A\0src/\u{e9}.rs\0R100\0src/a.rs\0src/\u{f1}.rs\0M\0src/b.rs\0";
        let (changed, renames) = parse_name_status(listing);
        let mut changed: Vec<_> = changed.into_iter().collect();
        changed.sort();
        assert_eq!(changed, vec!["src/b.rs", "src/\u{e9}.rs", "src/\u{f1}.rs"]);
        assert_eq!(
            renames.get("src/\u{f1}.rs").map(String::as_str),
            Some("src/a.rs")
        );
    }

    #[test]
    fn a_truncated_listing_stops() {
        let (changed, renames) = parse_name_status("M");
        assert!(changed.is_empty());
        assert!(renames.is_empty());
    }
}
