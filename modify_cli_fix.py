import re

with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

# Fix history_output_file
content = re.sub(
    r"(HistoryCommand::Analyze \{ \.\. \} => None,\n                HistoryCommand::Export \{ output_file, \.\. \}\n                \| HistoryCommand::ExportBatch \{ output_file, \.\. \},\n        \} => output_file\.as_deref\(\),)",
    r"HistoryCommand::Export { output_file, .. } | HistoryCommand::ExportBatch { output_file, .. } => output_file.as_deref(),\n            HistoryCommand::Analyze { .. } => None,\n        },\n        _ => None,",
    content
)

# And earlier, we messed up the pattern in history_output_file by using `=> None` inside the `match &cli.command` which was expecting a pattern, not a match arm
content = re.sub(
    r"Commands::History \{\n\s*command:\n\s*HistoryCommand::Analyze \{ \.\. \} => None,\n\s*HistoryCommand::Export \{ output_file, \.\. \}\n\s*\| HistoryCommand::ExportBatch \{ output_file, \.\. \},\n\s*\} => output_file\.as_deref\(\),",
    r"Commands::History { command } => match command {\n            HistoryCommand::Export { output_file, .. } | HistoryCommand::ExportBatch { output_file, .. } => output_file.as_deref(),\n            HistoryCommand::Analyze { .. } => None,\n        },",
    content
)

# Fix ApiRequest::Get to ApiRequest::get
content = content.replace("ApiRequest::Get(", "ApiRequest::get(")

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
