import sys

with open("./autumn-harvest/src/store.rs", "r") as f:
    content = f.read()

search_str = """    let next_id = max_id.map_or(0, |id| id.saturating_add(1));"""
replace_str = """    let next_id = max_id.map_or(Ok(0), |id| id.checked_add(1).ok_or_else(|| crate::error::database_error("event ID overflow")))?;"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/store.rs", "w") as f:
    f.write(content)
