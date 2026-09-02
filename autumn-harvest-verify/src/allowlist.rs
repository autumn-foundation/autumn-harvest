//! Checked-in allowlist (`harvest-verify.allow.toml`) — the AC5 escape hatch.
//! A file rather than an attribute because AC7 forbids macro-path changes.
//!
//! The rules exist so the hatch cannot be used silently: every entry must carry
//! a non-blank justification, an entry may not be listed twice, and an entry
//! that no longer matches an analyzed workflow is reported (a warning by
//! default, a failure under `--strict`). Every diagnostic names the offending
//! workflow, because "allowlist error" without a subject is unactionable.

use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllowEntry {
    /// Fully-qualified workflow fn path (`crate::module::name`).
    pub workflow: String,
    /// Required, non-blank.
    pub justification: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Allowlist {
    #[serde(default)]
    pub allow: Vec<AllowEntry>,
}

impl Allowlist {
    /// Load and validate. A blank justification is an error, not a warning.
    ///
    /// # Errors
    /// On i/o failure ([`crate::Error::Io`]), malformed TOML, duplicate
    /// workflows or a blank justification ([`crate::Error::Allowlist`]).
    pub fn load(path: &Path) -> crate::Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| crate::Error::Io {
            path: path.display().to_string(),
            source: e,
        })?;
        let list: Self = toml::from_str(&text)
            .map_err(|e| crate::Error::Allowlist(format!("{}: {e}", path.display())))?;
        list.validate()?;
        Ok(list)
    }

    /// Validate an in-memory allowlist (same rules as [`Self::load`]).
    ///
    /// # Errors
    /// On a blank workflow path, a blank justification or a duplicate workflow;
    /// the message always names the offending workflow.
    pub fn validate(&self) -> crate::Result<()> {
        let mut seen: HashSet<&str> = HashSet::new();
        for entry in &self.allow {
            let workflow = entry.workflow.trim();
            if workflow.is_empty() {
                return Err(crate::Error::Allowlist(
                    "an [[allow]] entry has an empty `workflow`".to_string(),
                ));
            }
            if entry.justification.trim().is_empty() {
                return Err(crate::Error::Allowlist(format!(
                    "{workflow}: `justification` must not be blank — an allowlist \
                     entry without a reason is how a known bug becomes permanent"
                )));
            }
            if !seen.insert(workflow) {
                return Err(crate::Error::Allowlist(format!(
                    "{workflow}: listed twice; keep one entry with one justification"
                )));
            }
        }
        Ok(())
    }

    /// Justification for `workflow`, if allowlisted. Matched on the full path.
    #[must_use]
    pub fn justification(&self, workflow: &str) -> Option<&str> {
        if workflow.is_empty() {
            return None;
        }
        self.allow
            .iter()
            .find(|e| e.workflow == workflow)
            .map(|e| e.justification.as_str())
    }

    /// Entries whose workflow is not in `used` (reported as warnings; errors under `--strict`).
    #[must_use]
    pub fn unused(&self, used: &BTreeSet<String>) -> Vec<&AllowEntry> {
        self.allow
            .iter()
            .filter(|e| !used.contains(&e.workflow))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(workflow: &str, justification: &str) -> AllowEntry {
        AllowEntry {
            workflow: workflow.to_string(),
            justification: justification.to_string(),
        }
    }

    #[test]
    fn an_empty_workflow_path_is_rejected() {
        let list = Allowlist {
            allow: vec![entry("  ", "a reason")],
        };
        assert!(list.validate().is_err());
    }

    #[test]
    fn validation_names_the_offending_workflow() {
        let list = Allowlist {
            allow: vec![entry("seeded::wf_x", " \t ")],
        };
        let message = match list.validate() {
            Ok(()) => String::new(),
            Err(e) => e.to_string(),
        };
        assert!(message.contains("seeded::wf_x"), "{message}");
    }

    #[test]
    fn unused_preserves_file_order() {
        let list = Allowlist {
            allow: vec![
                entry("a::x", "one"),
                entry("b::y", "two"),
                entry("c::z", "three"),
            ],
        };
        let used: BTreeSet<String> = std::iter::once("b::y".to_string()).collect();
        let unused: Vec<&str> = list
            .unused(&used)
            .into_iter()
            .map(|e| e.workflow.as_str())
            .collect();
        assert_eq!(unused, vec!["a::x", "c::z"]);
    }
}
