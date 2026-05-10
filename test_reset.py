import sys

with open("./autumn-harvest/src/reset.rs", "r") as f:
    content = f.read()

search_str = """    let source_next_event_id = rows.last().map_or(0, |row| row.event_id.saturating_add(1));"""
replace_str = """    let source_next_event_id = rows.last().map_or(Ok(0), |row| row.event_id.checked_add(1).ok_or_else(|| crate::error::database_error("event ID overflow")))?;"""

content = content.replace(search_str, replace_str)

search_str2 = """            .take(start.saturating_add(1))"""
replace_str2 = """            .take(start.checked_add(1).unwrap_or(usize::MAX))"""

content = content.replace(search_str2, replace_str2)

search_str3 = """    Ok(usize::try_from(queued.saturating_add(external)).unwrap_or(usize::MAX))"""
replace_str3 = """    Ok(usize::try_from(queued.checked_add(external).unwrap_or(i64::MAX)).unwrap_or(usize::MAX))"""

content = content.replace(search_str3, replace_str3)

with open("./autumn-harvest/src/reset.rs", "w") as f:
    f.write(content)
