import re

with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

render_match = r'fn render_response\(cli: &Cli, value: &Value\) -> Result<String, CliError> \{\n'
render_add = """fn render_response(cli: &Cli, value: &Value) -> Result<String, CliError> {
    if let Commands::History {
        command: HistoryCommand::PlantumlSequence { .. },
    } = &cli.command
    {
        if let Some(events_val) = value.get("events") {
            let events: Vec<autumn_harvest::event::WorkflowEvent> = serde_json::from_value(events_val.clone()).map_err(|e| CliError::ReadJson {
                label: "parse history events",
                path: "history.events".into(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            })?;
            return autumn_harvest::history_export::export_plantuml_sequence(&events).map_err(|e| CliError::ReadJson {
                label: "export plantuml sequence",
                path: "history.events".into(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
            });
        }
        return Err(CliError::ReadJson {
            label: "parse history export",
            path: "events".into(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, "missing events array"),
        });
    }
"""

content = re.sub(render_match, render_add, content)

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
print("Done")
