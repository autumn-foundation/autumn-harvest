import re

with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

# Modify history_output_file to include Analyze
content = re.sub(
    r"(HistoryCommand::Export \{\s*output_file,\s*\.\.\s*\})",
    r"HistoryCommand::Analyze { .. } => None,\n                \g<1>",
    content
)

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
