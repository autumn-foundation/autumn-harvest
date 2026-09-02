//! Parser entry point.

use super::ast::MirDoc;

/// Parse one `.mir` dump. Never panics; unparseable items land in
/// [`MirDoc::parse_failures`].
#[must_use]
pub fn parse(crate_name: &str, path: &str, text: &str) -> MirDoc {
    let _ = (crate_name, path, text);
    todo!("RED phase: implemented in GREEN")
}
