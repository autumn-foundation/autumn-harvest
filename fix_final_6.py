import re

reps = [
    (
        "autumn-harvest/src/admission_gate.rs",
        "    TooManyGates { limit: usize },",
        "    #[allow(missing_docs)]\n    TooManyGates { limit: usize },"
    ),
    (
        "autumn-harvest/src/admission_gate.rs",
        "    pub reason: String,",
        "    /// Reason\n    pub reason: String,"
    )
]

def apply_reps(filepath, old, new):
    try:
        with open(filepath, "r") as f:
            content = f.read()

        content = content.replace(old, new)

        with open(filepath, "w") as f:
            f.write(content)
    except Exception as e:
        print(f"Failed {filepath}: {e}")

for f, old, new in reps:
    apply_reps(f, old, new)
