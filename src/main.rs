use std::{
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    prelude::*,
    widgets::{
        Block, BorderType, Borders, Gauge, List, ListItem, Padding, Paragraph, Sparkline, Wrap,
    },
};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const HISTORY_LEN: usize = 48;

#[derive(Debug, Deserialize, Default, Clone)]
struct Ready {
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    stalled: bool,
    #[serde(default)]
    wedged: bool,
    #[serde(default)]
    lag_blocks: u64,
    #[serde(default)]
    last_block: u64,
    #[serde(default)]
    sealed_through: u64,
    #[serde(default)]
    tip: u64,
    #[serde(default)]
    seconds_since_poll: u64,
}

#[derive(Debug, Deserialize, Default)]
struct Tables {
    #[serde(default)]
    count: usize,
    #[serde(default)]
    tables: Vec<EventTable>,
}

#[derive(Debug, Deserialize, Clone)]
struct EventTable {
    table: String,
}

#[derive(Debug, Deserialize, Default)]
struct SqlResponse {
    #[serde(default)]
    rows: Vec<Value>,
    #[serde(default)]
    degraded: bool,
}

#[derive(Default)]
struct Snapshot {
    ready: Ready,
    metrics: BTreeMap<String, f64>,
    tables: Tables,
    selected_table: Option<String>,
    selected_rows: Option<u64>,
    selected_latest_block: Option<u64>,
    recent_events: Vec<Value>,
    degraded: bool,
}

#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    decoded_rows: u64,
    rpc_requests: u64,
}

struct App {
    url: String,
    snapshot: Snapshot,
    samples: Vec<Sample>,
    selected_table: usize,
    refresh_time: Option<Duration>,
    last_refresh: Instant,
    status: String,
    should_quit: bool,
}

impl App {
    fn new(url: String) -> Self {
        Self {
            url,
            snapshot: Snapshot::default(),
            samples: Vec::new(),
            selected_table: 0,
            refresh_time: None,
            last_refresh: Instant::now() - POLL_INTERVAL,
            status: "Connecting to nest…".into(),
            should_quit: false,
        }
    }

    fn refresh(&mut self, client: &Client) {
        let started = Instant::now();
        let selected = self
            .snapshot
            .tables
            .tables
            .get(self.selected_table)
            .map(|table| table.table.as_str());
        match fetch_snapshot(client, &self.url, selected) {
            Ok(snapshot) => {
                if let Some(index) = snapshot.tables.tables.iter().position(|table| {
                    snapshot.selected_table.as_deref() == Some(table.table.as_str())
                }) {
                    self.selected_table = index;
                }
                self.samples.push(Sample {
                    at: Instant::now(),
                    decoded_rows: metric_u64(&snapshot.metrics, "nuthatch_rows_decoded_total"),
                    rpc_requests: metric_u64(&snapshot.metrics, "nuthatch_rpc_requests_total"),
                });
                if self.samples.len() > HISTORY_LEN {
                    self.samples.remove(0);
                }
                self.snapshot = snapshot;
                self.refresh_time = Some(started.elapsed());
                self.status = "Live data received".into();
            }
            Err(error) => self.status = format!("Connection problem: {error:#}"),
        }
        self.last_refresh = Instant::now();
    }

    fn select_next(&mut self) {
        let count = self.snapshot.tables.tables.len();
        if count > 0 {
            self.selected_table = (self.selected_table + 1) % count;
            self.last_refresh = Instant::now() - POLL_INTERVAL;
        }
    }

    fn select_previous(&mut self) {
        let count = self.snapshot.tables.tables.len();
        if count > 0 {
            self.selected_table = (self.selected_table + count - 1) % count;
            self.last_refresh = Instant::now() - POLL_INTERVAL;
        }
    }

    fn rate(&self, field: impl Fn(Sample) -> u64) -> f64 {
        let Some([before, after]) = self.samples.as_slice().windows(2).last() else {
            return 0.0;
        };
        let elapsed = after.at.duration_since(before.at).as_secs_f64();
        if elapsed == 0.0 {
            0.0
        } else {
            field(*after).saturating_sub(field(*before)) as f64 / elapsed
        }
    }
}

fn main() -> Result<()> {
    let url = parse_url()?;
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .context("building HTTP client")?;
    let mut app = App::new(url);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let result = run(&mut terminal, &client, &mut app);
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn parse_url() -> Result<String> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => Ok("http://127.0.0.1:8288".into()),
        Some("--url") => args
            .next()
            .map(normalize_url)
            .context("--url needs a Nuthatch base URL"),
        Some("-h") | Some("--help") => {
            println!("nuthatch-tui-client [--url http://127.0.0.1:8288]");
            std::process::exit(0);
        }
        Some(value) => anyhow::bail!("unknown argument '{value}'; try --help"),
    }
}

fn normalize_url(value: String) -> String {
    value.trim_end_matches('/').to_owned()
}

fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: &Client,
    app: &mut App,
) -> Result<()> {
    loop {
        if app.last_refresh.elapsed() >= POLL_INTERVAL {
            app.refresh(client);
        }
        terminal.draw(|frame| draw(frame, app))?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
                KeyCode::Char('r') => app.refresh(client),
                KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                _ => {}
            }
        }
        if app.should_quit {
            return Ok(());
        }
    }
}

fn fetch_snapshot(client: &Client, base: &str, selected: Option<&str>) -> Result<Snapshot> {
    let ready = client
        .get(format!("{base}/ready"))
        .send()
        .context("GET /ready")?
        .error_for_status()
        .context("/ready returned an error")?
        .json()
        .context("decoding /ready")?;
    let metrics = parse_prometheus(
        &client
            .get(format!("{base}/metrics"))
            .send()
            .context("GET /metrics")?
            .error_for_status()
            .context("/metrics returned an error")?
            .text()
            .context("reading /metrics")?,
    );
    let tables: Tables = client
        .get(format!("{base}/tables"))
        .send()
        .context("GET /tables")?
        .error_for_status()
        .context("/tables returned an error")?
        .json()
        .context("decoding /tables")?;
    let selected_table = selected
        .filter(|name| tables.tables.iter().any(|table| table.table == *name))
        .map(str::to_owned)
        .or_else(|| tables.tables.first().map(|table| table.table.clone()));

    let mut snapshot = Snapshot {
        ready,
        metrics,
        tables,
        selected_table,
        ..Default::default()
    };
    if let Some(table) = snapshot.selected_table.as_deref() {
        let query =
            format!("SELECT count(*) AS rows, max(block_number) AS latest_block FROM {table}");
        let sql: SqlResponse = client
            .get(format!("{base}/sql"))
            .query(&[("q", query)])
            .send()
            .context("GET /sql")?
            .error_for_status()
            .context("/sql returned an error")?
            .json()
            .context("decoding /sql")?;
        snapshot.degraded = sql.degraded;
        if let Some(Value::Object(row)) = sql.rows.first() {
            snapshot.selected_rows = row.get("rows").and_then(Value::as_u64);
            snapshot.selected_latest_block = row.get("latest_block").and_then(Value::as_u64);
        }
        let events: SqlResponse = client
            .get(format!("{base}/sql"))
            .query(&[(
                "q",
                format!("SELECT * FROM {table} ORDER BY block_number DESC, log_index DESC LIMIT 6"),
            )])
            .send()
            .context("GET /sql for recent events")?
            .error_for_status()
            .context("recent-events /sql returned an error")?
            .json()
            .context("decoding recent-events /sql")?;
        snapshot.degraded |= events.degraded;
        snapshot.recent_events = events.rows;
    }
    Ok(snapshot)
}

fn parse_prometheus(text: &str) -> BTreeMap<String, f64> {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            if name.contains('{') {
                return None;
            }
            Some((name.to_string(), value.parse().ok()?))
        })
        .collect()
}

fn metric_u64(metrics: &BTreeMap<String, f64>, name: &str) -> u64 {
    metrics.get(name).copied().unwrap_or_default() as u64
}

fn format_rate(value: f64, unit: &str) -> String {
    if value < 0.05 {
        format!("0 {unit}")
    } else if value < 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.0} {unit}")
    }
}

fn format_bytes(value: u64) -> String {
    match value {
        0 => "unavailable".into(),
        value if value < 1024 * 1024 => format!("{} KiB", value / 1024),
        value => format!("{:.1} MiB", value as f64 / (1024.0 * 1024.0)),
    }
}

fn shorten(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.into()
    } else {
        format!("{}…", &value[..width.saturating_sub(1)])
    }
}

fn event_line(row: &Value) -> String {
    let Some(row) = row.as_object() else {
        return "unreadable event row".into();
    };
    let block = row
        .get("block_number")
        .and_then(Value::as_u64)
        .map_or("?".into(), |value| value.to_string());
    let details = row
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "block_number" | "block_hash" | "tx_hash" | "log_index" | "address" | "_seq"
            )
        })
        .take(2)
        .map(|(key, value)| format!("{key}={}", shorten(value.to_string().trim_matches('"'), 22)))
        .collect::<Vec<_>>()
        .join("  ");
    format!("#{block:<10} {details}")
}

fn panel(title: &str) -> Block<'_> {
    Block::default()
        .title(Line::from(title).style(Style::default().fg(Color::Cyan).bold()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))
        .padding(Padding::horizontal(1))
}

fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(Color::Rgb(11, 14, 20))),
        area,
    );
    let vertical = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(9),
        Constraint::Min(10),
        Constraint::Length(3),
    ])
    .split(area);

    let state =
        if app.snapshot.ready.ready && !app.snapshot.ready.stalled && !app.snapshot.ready.wedged {
            ("● LIVE", Color::Green)
        } else {
            ("● ATTENTION", Color::Yellow)
        };
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " NUTHATCH ",
            Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
        ),
        Span::styled(" LIVE VIEW", Style::default().fg(Color::White).bold()),
        Span::styled(format!("   {}", app.url), Style::default().fg(Color::Gray)),
        Span::styled(
            format!("   {} ", state.0),
            Style::default().fg(state.1).bold(),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    frame.render_widget(title, vertical[0]);

    let top = Layout::horizontal([
        Constraint::Percentage(34),
        Constraint::Percentage(33),
        Constraint::Percentage(33),
    ])
    .split(vertical[1]);
    let ready = &app.snapshot.ready;
    let health = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("STATUS  ", Style::default().fg(Color::Gray)),
            Span::styled(
                if ready.ready { "READY" } else { "WAITING" },
                Style::default().fg(state.1).bold(),
            ),
        ]),
        Line::from(format!("Tip             {}", ready.tip)),
        Line::from(format!("Indexed         {}", ready.last_block)),
        Line::from(format!("Finalised       {}", ready.sealed_through)),
    ])
    .block(panel("NEST HEALTH"));
    frame.render_widget(health, top[0]);

    let lag_ratio = if ready.tip == 0 {
        0.0
    } else {
        (1.0 - ready.lag_blocks as f64 / ready.tip as f64).clamp(0.0, 1.0)
    };
    let data = Paragraph::new(vec![
        Line::from(format!("Tables          {}", app.snapshot.tables.count)),
        Line::from(format!(
            "Decoded rows    {}",
            metric_u64(&app.snapshot.metrics, "nuthatch_rows_decoded_total")
        )),
        Line::from(format!(
            "Sealed rows     {}",
            metric_u64(&app.snapshot.metrics, "nuthatch_rows_sealed_total")
        )),
        Line::from(format!("Lag             {} blocks", ready.lag_blocks)),
    ])
    .block(panel("DATA COLLECTED"));
    frame.render_widget(data, top[1]);
    frame.render_widget(
        Gauge::default()
            .block(panel("SYNC POSITION"))
            .gauge_style(Style::default().fg(Color::Magenta))
            .ratio(lag_ratio)
            .label(format!("{} / {}", ready.last_block, ready.tip)),
        top[2],
    );

    let bottom = Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(vertical[2]);
    let rows: Vec<ListItem> = app
        .snapshot
        .tables
        .tables
        .iter()
        .enumerate()
        .map(|(index, table)| {
            ListItem::new(Line::from(Span::styled(
                &table.table,
                Style::default()
                    .fg(if index == app.selected_table {
                        Color::Black
                    } else {
                        Color::White
                    })
                    .bg(if index == app.selected_table {
                        Color::Cyan
                    } else {
                        Color::Reset
                    }),
            )))
        })
        .collect();
    frame.render_widget(
        List::new(rows).block(panel("INDEXED TABLES  ↑↓ / j k to inspect")),
        bottom[0],
    );

    let right =
        Layout::vertical([Constraint::Percentage(60), Constraint::Percentage(40)]).split(bottom[1]);
    let show_sparkline = right[0].height >= 13;
    let performance_area = if show_sparkline {
        Layout::vertical([Constraint::Min(8), Constraint::Length(4)]).split(right[0])
    } else {
        Layout::vertical([Constraint::Percentage(100)]).split(right[0])
    };
    let performance = Layout::horizontal([Constraint::Percentage(65), Constraint::Percentage(35)])
        .split(performance_area[0]);
    let rpc = metric_u64(&app.snapshot.metrics, "nuthatch_rpc_requests_total");
    let reorgs = metric_u64(&app.snapshot.metrics, "nuthatch_reorgs_total");
    let selected = app
        .snapshot
        .selected_table
        .as_deref()
        .unwrap_or("no event tables");
    let rpc_per_second = app.rate(|sample| sample.rpc_requests);
    let decode_per_second = app.rate(|sample| sample.decoded_rows);
    let activity = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("RPC TOTAL  ", Style::default().fg(Color::Gray)),
            Span::styled(rpc.to_string(), Style::default().fg(Color::Yellow).bold()),
        ]),
        Line::from(format!(
            "RPC rate        {}",
            format_rate(rpc_per_second, "req/s")
        )),
        Line::from(format!(
            "RPC rate        {}",
            format_rate(rpc_per_second * 60.0, "req/min")
        )),
        Line::from(format!(
            "Decode rate     {}",
            format_rate(decode_per_second, "rows/s")
        )),
        Line::from(format!(
            "API refresh     {} ms",
            app.refresh_time.map_or(0, |time| time.as_millis())
        )),
        Line::from(format!("Source poll age {} s", ready.seconds_since_poll)),
        Line::from(format!(
            "RSS             {}",
            format_bytes(metric_u64(&app.snapshot.metrics, "nuthatch_rss_bytes"))
        )),
        Line::from(format!("Reorgs          {reorgs}")),
    ])
    .block(panel("PERFORMANCE"));
    frame.render_widget(activity, performance[0]);

    let summary = Paragraph::new(vec![
        Line::from(Span::styled(
            selected,
            Style::default().fg(Color::Cyan).bold(),
        )),
        Line::from(format!(
            "Rows    {}",
            app.snapshot
                .selected_rows
                .map_or("—".into(), |n| n.to_string())
        )),
        Line::from(format!(
            "Latest  {}",
            app.snapshot
                .selected_latest_block
                .map_or("—".into(), |n| n.to_string())
        )),
        Line::from(Span::styled(
            if app.snapshot.degraded {
                "Warning: a sealed segment is degraded"
            } else {
                "Storage integrity: healthy"
            },
            Style::default().fg(if app.snapshot.degraded {
                Color::Yellow
            } else {
                Color::Green
            }),
        )),
    ])
    .block(panel("SELECTED TABLE"))
    .wrap(Wrap { trim: true });
    frame.render_widget(summary, performance[1]);
    let rpc_samples: Vec<u64> = app
        .samples
        .windows(2)
        .map(|pair| pair[1].rpc_requests.saturating_sub(pair[0].rpc_requests))
        .collect();
    let feed_rows: Vec<ListItem> = app
        .snapshot
        .recent_events
        .iter()
        .map(|row| ListItem::new(Line::from(event_line(row))))
        .collect();
    frame.render_widget(
        List::new(feed_rows).block(panel("LIVE EVENT FEED")),
        right[1],
    );
    if show_sparkline {
        frame.render_widget(
            Sparkline::default()
                .block(panel("RPC ACTIVITY / REFRESH"))
                .data(&rpc_samples)
                .style(Style::default().fg(Color::Yellow)),
            performance_area[1],
        );
    }

    let footer = Paragraph::new(Line::from(vec![
        Span::styled(
            " q ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" quit   "),
        Span::styled(
            " r ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" refresh   "),
        Span::styled(
            " ↑↓ ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" tables   "),
        Span::styled(&app.status, Style::default().fg(Color::DarkGray)),
    ]));
    frame.render_widget(footer, vertical[3]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_parser_keeps_plain_metrics_only() {
        let metrics = parse_prometheus(
            "# HELP ignored\nnuthatch_rows_decoded_total 42\nnuthatch_nest_rows_decoded_total{nest=\"x\"} 41\n",
        );
        assert_eq!(metrics.get("nuthatch_rows_decoded_total"), Some(&42.0));
        assert!(!metrics.contains_key("nuthatch_nest_rows_decoded_total{nest=\"x\"}"));
    }

    #[test]
    fn url_has_no_trailing_slash() {
        assert_eq!(
            normalize_url("http://localhost:8288/".into()),
            "http://localhost:8288"
        );
    }
}
