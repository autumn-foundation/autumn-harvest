import re

def fix_other_rs():
    with open('autumn-harvest/src/diagnostic.rs', 'r') as f:
        content = f.read()
    content = content.replace("pub struct ReplayDiagnostic {\n    pub final_output: Result<Value, String>,\n    pub event_count: usize,\n    pub analyzer_warnings: Vec<AnalyzerWarning>,\n    pub mermaid_sequence: String,\n}", "pub struct ReplayDiagnostic {\n    /// The final output of the workflow.\n    pub final_output: Result<Value, String>,\n    /// The number of events replayed.\n    pub event_count: usize,\n    /// Any analyzer warnings.\n    pub analyzer_warnings: Vec<AnalyzerWarning>,\n    /// The mermaid sequence string.\n    pub mermaid_sequence: String,\n}")
    with open('autumn-harvest/src/diagnostic.rs', 'w') as f:
        f.write(content)

    with open('autumn-harvest/src/history_export.rs', 'r') as f:
        content = f.read()
    content = content.replace("    pub const fn as_wire(self) -> &'static str {", "    /// Returns the wire format string.\n    pub const fn as_wire(self) -> &'static str {")
    with open('autumn-harvest/src/history_export.rs', 'w') as f:
        f.write(content)

    with open('autumn-harvest/src/replay.rs', 'r') as f:
        content = f.read()
    content = content.replace("    NoMatch,", "    /// No match was found.\n    NoMatch,")
    with open('autumn-harvest/src/replay.rs', 'w') as f:
        f.write(content)

fix_other_rs()
