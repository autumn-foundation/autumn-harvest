import sys

with open("./autumn-harvest/src/scheduler.rs", "r") as f:
    content = f.read()

search_str = """        attempt = attempt.saturating_add(1);"""
replace_str = """        attempt = attempt.checked_add(1).unwrap_or(u32::MAX);"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/scheduler.rs", "w") as f:
    f.write(content)
