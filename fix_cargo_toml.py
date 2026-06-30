import re

with open("autumn-harvest-cli/Cargo.toml", "r") as f:
    content = f.read()

new_bin = """
[[bin]]
name = "harvest-analyze"
path = "src/bin/harvest_analyze.rs"
"""

if "harvest-analyze" not in content:
    content = content.replace("[dev-dependencies]", new_bin + "\n[dev-dependencies]")

with open("autumn-harvest-cli/Cargo.toml", "w") as f:
    f.write(content)
