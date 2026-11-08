//! Run one task under four inference strategies and print each result.
//!
//! ```sh
//! cargo run -p agent-strategies
//! ```

use agent_strategies::{Error, TASK, render, run};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    let dir = tempfile::tempdir()?;
    let outcomes = run(&dir.path().join("strategies.db")).await?;
    println!("Task: {TASK}");
    println!();
    for outcome in &outcomes {
        for line in render(outcome) {
            println!("{line}");
        }
        println!();
    }
    Ok(())
}
