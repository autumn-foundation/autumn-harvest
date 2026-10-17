//! A file inbox that carries commands to a running session.
//!
//! The SQLite backend has one writer. While `autoresearch run` or
//! `autoresearch serve` holds it, another process cannot send a signal. So
//! `hint`, `stop`, `policy`, `approve` and `deny` write one JSON file each
//! into the inbox. The runner turns each file into a keyed signal, then
//! deletes it. The file name is the idempotency key, so a crash between the
//! signal and the delete cannot deliver a command twice.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::rules::KeepPolicy;

/// One command for a session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    /// Send an idea to the session with workflow id `session`.
    Hint {
        /// The session workflow id.
        session: String,
        /// The idea.
        text: String,
    },
    /// End the session after the experiment in flight.
    Stop {
        /// The session workflow id.
        session: String,
        /// Why.
        reason: String,
    },
    /// Replace the keep policy.
    Policy {
        /// The session workflow id.
        session: String,
        /// The new policy.
        policy: KeepPolicy,
    },
    /// Release the gated call that `token` names.
    Approve {
        /// `<execution id>/<signal name>`, as `status` prints it.
        token: String,
    },
    /// Refuse the gated call that `token` names.
    Deny {
        /// See [`Command::Approve`].
        token: String,
        /// Why. The model reads it.
        reason: String,
    },
}

/// The inbox directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inbox {
    dir: PathBuf,
}

impl Inbox {
    /// The inbox under `state_dir`, created on first use.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created.
    pub fn new(state_dir: &Path) -> Result<Self, String> {
        let _ = state_dir;
        unimplemented!("green phase")
    }

    /// Post a command. Returns its id: the file name and the signal key.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be written.
    pub fn post(&self, command: &Command) -> Result<String, String> {
        let _ = (command, &self.dir);
        unimplemented!("green phase")
    }

    /// Every posted command, oldest first. A file that does not parse is
    /// moved to `inbox/rejected` and left out.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be read.
    pub fn pending(&self) -> Result<Vec<(String, Command)>, String> {
        unimplemented!("green phase")
    }

    /// Remove a command that the runner delivered.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and cannot be removed.
    pub fn done(&self, id: &str) -> Result<(), String> {
        let _ = id;
        unimplemented!("green phase")
    }
}

/// The approval token of one wait: `<execution id>/<signal name>`.
#[must_use]
pub fn token(exec: &str, signal: &str) -> String {
    format!("{exec}/{signal}")
}

/// Split a token into the execution id and the signal name.
///
/// # Errors
///
/// Returns an error when the token has no `/` or an empty part.
pub fn parse_token(token: &str) -> Result<(String, String), String> {
    let _ = token;
    unimplemented!("green phase")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn hint(text: &str) -> Command {
        Command::Hint {
            session: "autoresearch/oct17".into(),
            text: text.into(),
        }
    }

    #[test]
    fn commands_come_back_in_order_and_go_once_done() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Inbox::new(dir.path()).unwrap();
        let first = inbox.post(&hint("a")).unwrap();
        let second = inbox.post(&hint("b")).unwrap();
        assert_ne!(first, second);
        let pending = inbox.pending().unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0], (first.clone(), hint("a")));
        assert_eq!(pending[1].1, hint("b"));
        inbox.done(&first).unwrap();
        inbox.done(&first).unwrap();
        assert_eq!(inbox.pending().unwrap().len(), 1);
    }

    #[test]
    fn a_file_that_does_not_parse_is_set_aside() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = Inbox::new(dir.path()).unwrap();
        std::fs::write(dir.path().join("inbox").join("0000-bad.json"), "{nope").unwrap();
        inbox.post(&hint("ok")).unwrap();
        let pending = inbox.pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert!(dir.path().join("inbox/rejected/0000-bad.json").is_file());
    }

    #[test]
    fn a_token_splits_at_the_first_slash() {
        let token = token("0192-abc", "tool_approval:0:1:call/x");
        assert_eq!(
            parse_token(&token).unwrap(),
            ("0192-abc".into(), "tool_approval:0:1:call/x".into())
        );
        assert!(parse_token("no-slash").is_err());
        assert!(parse_token("/signal").is_err());
        assert!(parse_token("exec/").is_err());
    }
}
