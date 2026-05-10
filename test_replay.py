import sys

with open("./autumn-harvest/src/replay.rs", "r") as f:
    content = f.read()

search_str = """                    self.cursor = scan_cursor.checked_add(1).expect("event ID overflow during replay match");"""
replace_str = """                    self.cursor = scan_cursor.checked_add(1).unwrap_or(usize::MAX);"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/replay.rs", "w") as f:
    f.write(content)
