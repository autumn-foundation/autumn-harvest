import sys

with open("./autumn-harvest/src/det_check.rs", "r") as f:
    content = f.read()

search_str = """                *depth = depth.saturating_add(1);"""
replace_str = """                *depth = depth.checked_add(1)?;"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/det_check.rs", "w") as f:
    f.write(content)
