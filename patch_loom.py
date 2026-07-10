import sys

with open("autumn-harvest/tests/loom_models.rs", "r") as f:
    lines = f.readlines()

new_lines = []
in_test = False
for line in lines:
    if line.strip() == "fn admission_gate_cache_concurrent_refresh_and_check() {":
        in_test = True
        new_lines.append(line)
        new_lines.append("    use autumn_harvest::admission_gate::{AdmissionGateCache, AdmissionGate, GateScope};\n")
        new_lines.append("    use uuid::Uuid;\n")
        new_lines.append("    use chrono::Utc;\n")
        continue

    if in_test and line.strip().startswith("use autumn_harvest::"):
        if "AdmissionGateCache" in line or "loom_sync" in line:
            pass # skip these from old implementation
        else:
            new_lines.append(line)
    elif in_test and line.strip() == "id: autumn_harvest::admission_gate::AdmissionGateId(Uuid::nil()),":
        new_lines.append(line)
    elif in_test and line.strip() == "created_by: \"test\".to_string(),":
        new_lines.append(line)
        new_lines.append("            message: \"test\".to_string(),\n")
    elif in_test and "loom_sync::Arc" in line:
        pass # Handle Arc manually later
    elif in_test and "let c1 = Arc::clone" in line:
        new_lines.append("        let c1 = std::sync::Arc::clone(&cache);\n")
    elif in_test and "let c2 = Arc::clone" in line:
        new_lines.append("        let c2 = std::sync::Arc::clone(&cache);\n")
    elif in_test and "let cache = Arc::new" in line:
        new_lines.append("        let cache = std::sync::Arc::new(AdmissionGateCache::default());\n")
    else:
        new_lines.append(line)
        if in_test and line.strip() == "}":
            in_test = False

with open("autumn-harvest/tests/loom_models.rs", "w") as f:
    f.writelines(new_lines)
