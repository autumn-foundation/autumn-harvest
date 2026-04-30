with open("autumn-harvest/src/simulator.rs", "r") as f:
    content = f.read()

# Add the # Panics documentation block back to the `run` method
if "# Panics" not in content:
    doc_block = """    /// Run the workflow to completion using the provided input.
    ///
    /// # Panics
    ///
    /// Panics if the workflow deadlocks (e.g., suspends without emitting any
    /// progressable commands like mocked activities, child workflows, or awaited signals).
    #[allow(clippy::too_many_lines)]
    pub async fn run(self, input: Value) -> SimulatorResult {"""

    content = content.replace("    #[allow(clippy::too_many_lines)]\n    pub async fn run(self, input: Value) -> SimulatorResult {", doc_block)

with open("autumn-harvest/src/simulator.rs", "w") as f:
    f.write(content)
