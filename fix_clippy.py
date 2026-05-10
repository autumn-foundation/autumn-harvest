import sys

with open("./autumn-harvest/src/reset.rs", "r") as f:
    content = f.read()

search_str2 = """            .take(start.checked_add(1).unwrap_or(usize::MAX))"""
replace_str2 = """            .take(start.saturating_add(1))"""

content = content.replace(search_str2, replace_str2)

with open("./autumn-harvest/src/reset.rs", "w") as f:
    f.write(content)

with open("./autumn-harvest/src/scheduler.rs", "r") as f:
    content = f.read()

search_str = """        attempt = attempt.checked_add(1).unwrap_or(u32::MAX);"""
replace_str = """        attempt = attempt.saturating_add(1);"""

content = content.replace(search_str, replace_str)

with open("./autumn-harvest/src/scheduler.rs", "w") as f:
    f.write(content)

with open("./autumn-harvest/src/replay.rs", "r") as f:
    content = f.read()

search_str3 = """                    self.cursor = scan_cursor.checked_add(1).unwrap_or(usize::MAX);"""
replace_str3 = """                    self.cursor = scan_cursor.saturating_add(1);"""

content = content.replace(search_str3, replace_str3)

with open("./autumn-harvest/src/replay.rs", "w") as f:
    f.write(content)
