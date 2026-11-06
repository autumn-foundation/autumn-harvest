//! CH002: a TODO, FIXME, XXX or HACK marker with no tracking reference.

use super::patterns::{ADJACENT_REF_RE, SENTENCE_END_RE, TODO_RE, TODO_REF_RE, TRAILING_REF_RE};

/// Where the sentence that starts at `lower` ends, if it ends at all.
///
/// A sentence ends at terminal punctuation, and then after a bracketed
/// citation attached to it. "add retries. (#123)" is one sentence with a
/// reference, not a sentence and then a fragment. `limit` bounds the
/// answer, so a sentence never reaches past what the caller owns.
fn sentence_end(text: &str, lower: usize, limit: usize) -> Option<usize> {
    let (_, mut end) = SENTENCE_END_RE.find_range(text, lower, limit)?;
    if let Some((_, citation)) = TRAILING_REF_RE.match_range(text, end, limit) {
        end = citation;
    }
    Some(end.min(limit))
}

/// The text that one marker owns: from its own start to the end of its
/// own sentence.
///
/// This is one rule in one place, because reviews found four edges of it
/// one at a time. A marker owns its text up to the end of its sentence, or
/// up to the next marker, whichever comes first. `sentence_end` says where
/// the sentence ends. The next marker stops the claim in any case, because
/// the reference of that marker is its own.
fn marker_span(text: &str, marks: &[(usize, usize)], index: usize) -> (usize, usize) {
    let start = marks[index].0;
    let limit = marks.get(index + 1).map_or(text.len(), |next| next.0);
    let end = sentence_end(text, marks[index].1, limit);
    (start, end.unwrap_or(limit))
}

/// Each marker match in `text`, as Python `finditer` gives them.
fn markers(text: &str) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    let mut pos = 0;
    while let Some((start, end)) = TODO_RE.find_from(text, pos) {
        found.push((start, end));
        pos = if end > start { end } else { end + 1 };
        if pos > text.len() {
            break;
        }
    }
    found
}

/// Does `text` hold a marker with no reference of its own?
///
/// The test is per marker, not per line. A search of the whole line let
/// one reference cover each marker beside it, in two shapes. One is an
/// unrelated reference already on the line, as in
/// `See #123 for the parser. TODO: add retries`. The other is a second
/// marker with no reference after one with a reference, as in
/// `TODO(#123): parser; TODO: add retries`. The gate passed both untracked
/// commitments.
///
/// A marker owns the text from itself to the next marker, or to the end of
/// the line. That admits each form this tree writes: `TODO(#123):`, `TODO:
/// ... (#123)" and a bare URL. It refuses only a reference that belongs to
/// something else.
pub fn untracked_marker(text: &str, before: &str) -> bool {
    let marks = markers(text);
    let mut claimed: Option<usize> = None;
    for index in 0..marks.len() {
        let (start, end) = marker_span(text, &marks, index);
        if let Some((forward, _)) = TODO_REF_RE.find_range(text, start, end) {
            // This is what this marker consumes, so the next marker cannot
            // use it again.
            claimed = Some(forward);
            continue;
        }
        // The search goes backward as well as forward.
        // `#123 - TODO: remove the fallback` is a tracked commitment written
        // the other way round. A forward search alone failed the build on it. The
        // reference must touch the marker: only separators may sit between
        // the two. So a reference from an earlier clause still does not
        // stand in for one.
        if index == 0 {
            // `before` is what the comment says on the lines above. That is
            // to the left of the first marker, as its own line is. It comes
            // as a separate argument, not joined to `text`. A join moves the
            // marker off the start of the string, and `TODO_RE` reads an
            // unpunctuated "TODO fix this" only there. No earlier marker can
            // have claimed anything yet, so there is nothing to compare.
            let left = format!("{before}{}", &text[..start]);
            if ADJACENT_REF_RE.is_match(&left) {
                continue;
            }
            return true;
        }
        let lower = marks[index - 1].1;
        let between = &text[lower..start];
        // One reference tracks one marker. The forward search of the
        // previous marker reaches into this same text. So
        // `TODO: #123; TODO: x` let the second marker borrow the reference
        // of the first and pass an absolute gate. The borrowed reference is
        // refused. A different reference in the same span still counts, as
        // in `TODO(#1): a; #2 - TODO: b`.
        if let Some(adjacent) = ADJACENT_REF_RE.search(between) {
            let reference = adjacent.span(1).map_or(0, |span| span.0);
            if Some(lower + reference) != claimed {
                continue;
            }
        }
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_tracks_its_marker() {
        assert!(!untracked_marker("TODO(#123): fix", ""));
        assert!(!untracked_marker("TODO: fix. (#123)", ""));
        assert!(!untracked_marker("#123 - TODO: fix", ""));
    }

    #[test]
    fn a_reference_does_not_track_another_marker() {
        assert!(untracked_marker("TODO: fix", ""));
        assert!(untracked_marker(
            "See #123 for the parser. TODO: add retries",
            ""
        ));
        assert!(untracked_marker(
            "TODO(#123): parser; TODO: add retries",
            ""
        ));
        assert!(untracked_marker("TODO: #123; TODO: x", ""));
    }

    #[test]
    fn a_placeholder_is_not_a_reference() {
        assert!(untracked_marker("TODO(#0): fix", ""));
        assert!(untracked_marker("TODO: see https://).", ""));
    }

    #[test]
    fn the_line_above_can_carry_the_reference() {
        assert!(!untracked_marker("TODO: remove the fallback", "#123 "));
    }
}
