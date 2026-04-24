use std::io;
use std::time::Duration;

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Style},
    text::Span,
    widgets::{Block, Borders, Row, Table},
};
use serde_json::Value;

use crate::Cli;
use crate::CliError;

/// Run the interactive TUI dashboard.
///
/// # Errors
/// Returns `CliError` if terminal initialization fails, if the API client fails
/// to fetch data, or if there is an error drawing to the terminal.
pub async fn run_tui(cli: &Cli) -> Result<(), CliError> {
    // Setup terminal
    enable_raw_mode().map_err(|e| CliError::ReadJson {
        label: "setup raw mode",
        path: "terminal".to_string(),
        source: e,
    })?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).map_err(|e| CliError::ReadJson {
        label: "enter alternate screen",
        path: "terminal".to_string(),
        source: e,
    })?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| CliError::ReadJson {
        label: "create terminal",
        path: "terminal".to_string(),
        source: e,
    })?;

    let client = reqwest::Client::new();
    let url = format!("{}/workflows?limit=50", cli.base_url.trim_end_matches('/'));

    let res = run_app(&mut terminal, &client, &url, cli).await;

    // Restore terminal
    disable_raw_mode().unwrap_or_default();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).unwrap_or_default();
    terminal.show_cursor().unwrap_or_default();

    res
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: &reqwest::Client,
    url: &str,
    cli: &Cli,
) -> Result<(), CliError> {
    let tick_rate = Duration::from_millis(1000);
    let mut last_tick = std::time::Instant::now();
    let mut workflows: Vec<Value> = Vec::new();

    loop {
        if last_tick.elapsed() >= tick_rate {
            let mut req = client.get(url);
            if let Some(token) = &cli.token {
                req = req.bearer_auth(token);
            }
            if let Ok(res) = req.send().await {
                if let Ok(data) = res.json::<Vec<Value>>().await {
                    workflows = data;
                }
            }
            last_tick = std::time::Instant::now();
        }

        terminal
            .draw(|f| {
                let rects = Layout::default()
                    .constraints([Constraint::Percentage(100)].as_ref())
                    .split(f.area());

                let normal_style = Style::default().bg(Color::Blue);

                let header_cells = ["ID", "Name", "Status", "Created At"].iter().map(|h| {
                    ratatui::widgets::Cell::from(*h).style(Style::default().fg(Color::Yellow))
                });
                let header = Row::new(header_cells)
                    .style(normal_style)
                    .height(1)
                    .bottom_margin(1);

                let rows = format_workflows_to_rows(&workflows);

                let t = Table::new(
                    rows,
                    [
                        Constraint::Length(38),
                        Constraint::Length(30),
                        Constraint::Length(15),
                        Constraint::Min(25),
                    ],
                )
                .header(header)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Autumn Harvest Workflows (Press 'q' to quit)"),
                );

                f.render_widget(t, rects[0]);
            })
            .map_err(|e| CliError::ReadJson {
                label: "draw terminal",
                path: "terminal".to_string(),
                source: e,
            })?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));

        if event::poll(timeout).unwrap_or(false) {
            if let Event::Key(key) = event::read().unwrap() {
                if key.code == KeyCode::Char('q')
                    || (key.modifiers.contains(KeyModifiers::CONTROL)
                        && key.code == KeyCode::Char('c'))
                {
                    return Ok(());
                }
            }
        }
    }
}

pub(crate) fn format_workflows_to_rows(workflows: &[Value]) -> Vec<Row<'_>> {
    workflows
        .iter()
        .map(|w| {
            let id = w.get("id").and_then(Value::as_str).unwrap_or("?");
            let name = w
                .get("workflow_name")
                .and_then(Value::as_str)
                .unwrap_or("?");
            let status = w.get("status").and_then(Value::as_str).unwrap_or("?");
            let created_at = w.get("created_at").and_then(Value::as_str).unwrap_or("?");

            let status_color = match status {
                "completed" => Color::Green,
                "failed" => Color::Red,
                "cancelled" => Color::Yellow,
                "running" => Color::Blue,
                _ => Color::White,
            };

            Row::new(vec![
                ratatui::widgets::Cell::from(id),
                ratatui::widgets::Cell::from(name),
                ratatui::widgets::Cell::from(Span::styled(
                    status,
                    Style::default().fg(status_color),
                )),
                ratatui::widgets::Cell::from(created_at),
            ])
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_workflows_to_rows() {
        let dummy_data = vec![
            json!({
                "id": "123",
                "workflow_name": "test_wf",
                "status": "running",
                "created_at": "2023-01-01T00:00:00Z"
            }),
            json!({
                "id": "456",
                "workflow_name": "another_wf",
                "status": "failed",
                "created_at": "2023-01-02T00:00:00Z"
            }),
        ];

        let rows = format_workflows_to_rows(&dummy_data);
        assert_eq!(rows.len(), 2);
    }
}
