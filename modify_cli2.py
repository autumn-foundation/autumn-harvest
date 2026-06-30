import re

with open("autumn-harvest-cli/src/lib.rs", "r") as f:
    content = f.read()

# Add Analyze branch to history_request
analyze_req = """        HistoryCommand::Analyze { execution_id, rules } => {
            let mut params = vec![];
            if let Some(r) = rules {
                params.push(("rules", r.clone()));
            }
            ApiRequest::Get(format!(
                "/workflows/{}/history/analyze?{}",
                path_segment(execution_id),
                encode_query_params(&params)
            ))
        }
"""

content = re.sub(
    r"(fn history_request\(command: &HistoryCommand\) -> ApiRequest \{\n\s*match command \{\n)",
    r"\g<1>" + analyze_req,
    content
)

with open("autumn-harvest-cli/src/lib.rs", "w") as f:
    f.write(content)
