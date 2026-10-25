//! An always-on support desk run by a durable agent mesh: the capstone of
//! epic #2262.
//!
//! Three durable agents share one mesh, `desk`:
//!
//! - `triage/desk` wakes on a heartbeat and reads the inbox. A person asks
//!   it to answer a ticket. It asks its peers, streams its answer, and waits
//!   for an approval before it files the reply.
//! - `researcher/kb` searches the notes of the desk.
//! - `critic/qa` checks a draft reply.
//!
//! The agents run on the embedded SQLite backend, in one file. A restart,
//! a crash between steps, or a duplicate message changes nothing that the
//! mesh did. `README.md` walks through a session.

pub mod desk;
pub mod mesh;
pub mod scripted;
