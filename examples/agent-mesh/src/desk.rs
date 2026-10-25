//! The files of a support desk, and the tools that read and write them.
//!
//! A desk is a folder:
//!
//! - `inbox/<id>.json` holds one ticket.
//! - `notes/<name>.md` holds one note of the knowledge base.
//! - `replies/<id>.md` holds the filed reply to a ticket.
//! - `filed.log` has one line for each run of the `file_reply` tool. A test
//!   reads it to prove that each reply was filed once.
//! - `reports.log` has one line for each report of the triage agent.
//! - `desk.db` is the SQLite file of the mesh.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use autumn_harvest_agent::{AgentError, ErrorKind, FnTool, Tool, ToolEffect};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// One support ticket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    /// The ticket id, such as `T-1`.
    pub id: String,
    /// Who wrote it.
    pub from: String,
    /// The subject line.
    pub subject: String,
    /// The text.
    pub body: String,
}

/// The tickets and notes that `init` writes.
const SAMPLE_TICKETS: [(&str, &str, &str, &str); 2] = [
    (
        "T-1",
        "ada@example.com",
        "Export fails after the upgrade",
        "Since the upgrade to 2.4, the CSV export stops at 10,000 rows.",
    ),
    (
        "T-2",
        "bo@example.com",
        "How do I rotate my API key?",
        "I need to rotate the API key of our staging project.",
    ),
];

const SAMPLE_NOTES: [(&str, &str); 2] = [
    (
        "export",
        "# CSV export\n\nRelease 2.4 caps one export at 10,000 rows. Set \
         `export.max_rows` in the project settings to raise the cap, up to \
         1,000,000 rows.\n",
    ),
    (
        "api-keys",
        "# API keys\n\nRotate a key in Settings > API keys > Rotate. The old \
         key works for 24 hours after a rotation.\n",
    ),
];

/// A desk folder.
#[derive(Debug, Clone)]
pub struct Desk {
    root: PathBuf,
}

fn io(err: &std::io::Error) -> AgentError {
    AgentError::new(
        ErrorKind::Unavailable,
        format!("the desk folder failed: {err}"),
    )
}

impl Desk {
    /// The desk in `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The folder.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The SQLite file of the mesh.
    #[must_use]
    pub fn db(&self) -> PathBuf {
        self.root.join("desk.db")
    }

    fn dir(&self, name: &str) -> PathBuf {
        self.root.join(name)
    }

    /// Make the folders, and write the sample tickets and notes into an
    /// empty desk.
    ///
    /// # Errors
    ///
    /// Returns an error when a write fails.
    pub fn init(&self) -> std::io::Result<()> {
        for name in ["inbox", "notes", "replies"] {
            std::fs::create_dir_all(self.dir(name))?;
        }
        for (id, from, subject, body) in SAMPLE_TICKETS {
            let path = self.dir("inbox").join(format!("{id}.json"));
            if !path.exists() {
                let ticket = Ticket {
                    id: id.to_owned(),
                    from: from.to_owned(),
                    subject: subject.to_owned(),
                    body: body.to_owned(),
                };
                let text = serde_json::to_string_pretty(&ticket).map_err(std::io::Error::other)?;
                std::fs::write(path, text)?;
            }
        }
        for (name, text) in SAMPLE_NOTES {
            let path = self.dir("notes").join(format!("{name}.md"));
            if !path.exists() {
                std::fs::write(path, text)?;
            }
        }
        Ok(())
    }

    /// Every ticket, by id.
    ///
    /// # Errors
    ///
    /// Returns an error when the inbox cannot be read or a ticket does not
    /// decode.
    pub fn tickets(&self) -> std::io::Result<Vec<Ticket>> {
        let mut tickets = Vec::new();
        for entry in std::fs::read_dir(self.dir("inbox"))? {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let text = std::fs::read_to_string(&path)?;
                tickets.push(serde_json::from_str(&text).map_err(std::io::Error::other)?);
            }
        }
        tickets.sort_by(|a: &Ticket, b| a.id.cmp(&b.id));
        Ok(tickets)
    }

    /// The ticket `id`.
    ///
    /// # Errors
    ///
    /// As [`Self::tickets`].
    pub fn ticket(&self, id: &str) -> std::io::Result<Option<Ticket>> {
        Ok(self.tickets()?.into_iter().find(|ticket| ticket.id == id))
    }

    /// The tickets with no filed reply.
    ///
    /// # Errors
    ///
    /// As [`Self::tickets`].
    pub fn open_tickets(&self) -> std::io::Result<Vec<Ticket>> {
        Ok(self
            .tickets()?
            .into_iter()
            .filter(|ticket| self.reply(&ticket.id).is_none())
            .collect())
    }

    /// The notes whose text holds a word of `query` with four or more
    /// letters, as `(name, text)`, the best match first.
    ///
    /// A note scores one point for each time that a word of the query
    /// occurs in it. A common word such as "after" can match a note on
    /// another topic. The score puts the note on the topic first. Notes
    /// with the same score go by name.
    ///
    /// # Errors
    ///
    /// Returns an error when the notes cannot be read.
    pub fn search_notes(&self, query: &str) -> std::io::Result<Vec<(String, String)>> {
        let words: BTreeSet<String> = query
            .split(|c: char| !c.is_alphanumeric())
            .filter(|word| word.len() >= 4)
            .map(str::to_lowercase)
            .collect();
        let mut found = Vec::new();
        for entry in std::fs::read_dir(self.dir("notes"))? {
            let path = entry?.path();
            let text = std::fs::read_to_string(&path)?;
            let lower = text.to_lowercase();
            let score: usize = words
                .iter()
                .map(|word| lower.matches(word.as_str()).count())
                .sum();
            if score > 0 {
                let name = path
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
                    .unwrap_or_default();
                found.push((score, name, text));
            }
        }
        found.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        Ok(found
            .into_iter()
            .map(|(_, name, text)| (name, text))
            .collect())
    }

    /// The filed reply to ticket `id`.
    #[must_use]
    pub fn reply(&self, id: &str) -> Option<String> {
        std::fs::read_to_string(self.dir("replies").join(format!("{id}.md"))).ok()
    }

    /// File `text` as the reply to ticket `id`, and log the run. A second
    /// run with the same text writes nothing, so a retry is safe.
    ///
    /// # Errors
    ///
    /// Returns an error when a write fails.
    pub fn file_reply(&self, id: &str, text: &str) -> std::io::Result<bool> {
        use std::io::Write as _;
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("filed.log"))?;
        writeln!(log, "{id}")?;
        if self.reply(id).as_deref() == Some(text) {
            return Ok(false);
        }
        std::fs::create_dir_all(self.dir("replies"))?;
        std::fs::write(self.dir("replies").join(format!("{id}.md")), text)?;
        Ok(true)
    }

    /// The ticket id of each run of `file_reply`, in order.
    #[must_use]
    pub fn filed_log(&self) -> Vec<String> {
        std::fs::read_to_string(self.root.join("filed.log"))
            .map(|text| text.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// Add one line to `reports.log`.
    ///
    /// # Errors
    ///
    /// Returns an error when the write fails.
    pub fn log_report(&self, line: &str) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("reports.log"))?;
        writeln!(log, "{}", line.replace('\n', " "))
    }

    /// The lines of `reports.log`.
    #[must_use]
    pub fn reports(&self) -> Vec<String> {
        std::fs::read_to_string(self.root.join("reports.log"))
            .map(|text| text.lines().map(str::to_owned).collect())
            .unwrap_or_default()
    }

    /// `check_inbox`: the open tickets. It reads only.
    #[must_use]
    pub fn check_inbox_tool(&self) -> Arc<dyn Tool> {
        let desk = self.clone();
        FnTool::new(
            "check_inbox",
            "List the tickets that have no reply yet.",
            json!({"type": "object", "properties": {}, "additionalProperties": false}),
            move |_input| {
                let desk = desk.clone();
                async move {
                    let open = desk.open_tickets().map_err(|e| io(&e))?;
                    Ok(json!(
                        open.iter()
                            .map(|t| json!({"id": t.id, "subject": t.subject, "body": t.body}))
                            .collect::<Vec<Value>>()
                    ))
                }
            },
        )
        .effect(ToolEffect::ReadOnly)
        .shared()
    }

    /// `search_notes`: the notes that match a query. It reads only.
    #[must_use]
    pub fn search_notes_tool(&self) -> Arc<dyn Tool> {
        let desk = self.clone();
        FnTool::new(
            "search_notes",
            "Search the knowledge base. Returns each note that matches a word of the query, \
             the best match first.",
            json!({
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": false
            }),
            move |input| {
                let desk = desk.clone();
                async move {
                    let query = input["query"].as_str().unwrap_or_default().to_owned();
                    let found = desk.search_notes(&query).map_err(|e| io(&e))?;
                    Ok(json!(
                        found
                            .into_iter()
                            .map(|(name, text)| json!({"note": name, "text": text}))
                            .collect::<Vec<Value>>()
                    ))
                }
            },
        )
        .effect(ToolEffect::ReadOnly)
        .shared()
    }

    /// `file_reply`: file the reply to a ticket. It writes, so the policy of
    /// the desk asks a person first.
    #[must_use]
    pub fn file_reply_tool(&self) -> Arc<dyn Tool> {
        let desk = self.clone();
        FnTool::new(
            "file_reply",
            "File the reply to a ticket. A person approves each reply first.",
            json!({
                "type": "object",
                "properties": {"ticket": {"type": "string"}, "text": {"type": "string"}},
                "required": ["ticket", "text"],
                "additionalProperties": false
            }),
            move |input| {
                let desk = desk.clone();
                async move {
                    let ticket = input["ticket"].as_str().unwrap_or_default().to_owned();
                    let text = input["text"].as_str().unwrap_or_default().to_owned();
                    if desk.ticket(&ticket).map_err(|e| io(&e))?.is_none() {
                        return Err(AgentError::new(
                            ErrorKind::Tool,
                            format!("no ticket {ticket:?}"),
                        ));
                    }
                    let written = desk.file_reply(&ticket, &text).map_err(|e| io(&e))?;
                    Ok(json!({"filed": ticket, "written": written}))
                }
            },
        )
        .effect(ToolEffect::Write)
        .shared()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_search_puts_the_note_on_the_topic_first() {
        let dir = tempfile::tempdir().unwrap();
        let desk = Desk::new(dir.path());
        desk.init().unwrap();
        // "after" also occurs in the note on API keys.
        let found = desk
            .search_notes("Find what our notes say about: Export fails after the upgrade")
            .unwrap();
        let names: Vec<&str> = found.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(names, ["export", "api-keys"]);
        let found = desk.search_notes("How do I rotate my API key?").unwrap();
        assert_eq!(found[0].0, "api-keys");
    }
}
