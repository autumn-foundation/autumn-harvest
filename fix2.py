with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

correct_code = """fn history_output_file(cli: &Cli) -> Option<&Path> {
    match &cli.command {
        Commands::History { command } => match command {
            HistoryCommand::Export { output_file, .. } | HistoryCommand::ExportBatch { output_file, .. } => output_file.as_deref(),
            HistoryCommand::Analyze { .. } => None,
        },
        _ => None,
    }
}"""

import re
content = re.sub(
    r"fn history_output_file\(cli: &Cli\) -> Option<&Path> \{\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n.*?\n\}",
    correct_code,
    content,
    flags=re.DOTALL
)

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
