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
/// Labelled metric lines in the performance panel. The panel is laid out at exactly this height so
/// that none of them is silently cropped; raise it with the panel.
const PERFORMANCE_LINES: u16 = 9;
const RATE_WINDOWS: [Duration; 3] = [
    Duration::from_secs(15),
    Duration::from_secs(60),
    Duration::from_secs(90),
];

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

#[derive(Debug, Deserialize, Default)]
struct NestInfo {
    #[serde(default)]
    name: String,
}

#[derive(Default)]
struct Snapshot {
    nest_name: Option<String>,
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
    rpc_methods: u64,
    indexed_block: u64,
    cpu_seconds: Option<f64>,
}

struct App {
    url: String,
    snapshot: Snapshot,
    samples: Vec<Sample>,
    rate_window: usize,
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
            rate_window: 1,
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
                    rpc_methods: metric_u64(&snapshot.metrics, "nuthatch_rpc_methods_total"),
                    indexed_block: snapshot.ready.last_block,
                    cpu_seconds: snapshot
                        .metrics
                        .get("nuthatch_process_cpu_seconds_total")
                        .copied(),
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
        let Some(after) = self.samples.last().copied() else {
            return 0.0;
        };
        let window = RATE_WINDOWS[self.rate_window];
        let before = self
            .samples
            .iter()
            .rev()
            .copied()
            .find(|sample| after.at.duration_since(sample.at) >= window)
            .or_else(|| self.samples.first().copied());
        let Some(before) = before else {
            return 0.0;
        };
        let elapsed = after.at.duration_since(before.at).as_secs_f64();
        if elapsed == 0.0 {
            0.0
        } else {
            field(after).saturating_sub(field(before)) as f64 / elapsed
        }
    }

    /// `Some(None)` distinguishes "the metric is published but we're still warming up a window"
    /// from `None`, "this Nuthatch does not publish `nuthatch_process_cpu_seconds_total` at all".
    /// There is a third case the client cannot see. Nuthatch's sampler read `/proc/self/stat` and
    /// nothing else until nightswatchhq/nuthatch#844, so a nest on any released Nuthatch up to
    /// v2.7.2, hosted off Linux, publishes the counter pinned at exactly 0.0 forever. That arrives
    /// here as `Some(Some(0.0))`, indistinguishable from a genuinely idle process. Nuthatch main
    /// has the `ps -o time=` fallback; no tag carries it yet.
    fn cpu_percent(&self) -> Option<Option<f64>> {
        let after = self.samples.last().copied()?;
        let after_cpu = after.cpu_seconds?;
        let window = RATE_WINDOWS[self.rate_window];
        let before = self
            .samples
            .iter()
            .rev()
            .copied()
            .find(|sample| after.at.duration_since(sample.at) >= window)
            .or_else(|| self.samples.first().copied())?;
        let Some(before_cpu) = before.cpu_seconds else {
            return Some(None);
        };
        let elapsed = after.at.duration_since(before.at).as_secs_f64();
        if elapsed <= 0.0 {
            Some(None)
        } else {
            Some(Some(((after_cpu - before_cpu).max(0.0) / elapsed) * 100.0))
        }
    }

    fn rate_window_label(&self) -> String {
        let target = RATE_WINDOWS[self.rate_window].as_secs();
        let observed = self
            .samples
            .first()
            .zip(self.samples.last())
            .map(|(first, last)| last.at.duration_since(first.at).as_secs())
            .unwrap_or_default();
        if observed < target {
            format!("warming {observed}/{target}s")
        } else {
            format!("last {target}s")
        }
    }

    fn cycle_rate_window(&mut self) {
        self.rate_window = (self.rate_window + 1) % RATE_WINDOWS.len();
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
                KeyCode::Char('w') => app.cycle_rate_window(),
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
    let info: NestInfo = client
        .get(format!("{base}/"))
        .send()
        .context("GET /")?
        .error_for_status()
        .context("/ returned an error")?
        .json()
        .context("decoding /")?;
    let schema = client
        .get(format!("{base}/schema"))
        .send()
        .context("GET /schema")?
        .error_for_status()
        .context("/schema returned an error")?
        .text()
        .context("reading /schema")?;
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
        nest_name: nest_name_from_schema(&schema)
            .or_else(|| (!info.name.is_empty() && info.name != "nuthatch").then_some(info.name)),
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

/// `/schema` carries the authored nest name, whereas the compact root document historically
/// identifies the runtime itself as `nuthatch`.
fn nest_name_from_schema(schema: &str) -> Option<String> {
    let line = schema.lines().find(|line| line.starts_with("The `"))?;
    let rest = line.strip_prefix("The `")?;
    let (name, _) = rest.split_once("` nest on ")?;
    (!name.is_empty()).then(|| name.to_owned())
}

fn parse_prometheus(text: &str) -> BTreeMap<String, f64> {
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            let name = name.split_once('{').map_or(name, |(name, _)| name);
            Some((name.to_string(), value.parse::<f64>().ok()?))
        })
        .fold(BTreeMap::new(), |mut metrics, (name, value)| {
            *metrics.entry(name).or_default() += value;
            metrics
        })
}

fn metric_u64(metrics: &BTreeMap<String, f64>, name: &str) -> u64 {
    metric_opt_u64(metrics, name).unwrap_or_default()
}

fn metric_opt_u64(metrics: &BTreeMap<String, f64>, name: &str) -> Option<u64> {
    metrics.get(name).copied().map(|value| value as u64)
}

fn format_optional_count(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), |value| value.to_string())
}

/// `cpu_percent` carries the same `Option<Option<f64>>` distinction as `App::cpu_percent`.
fn format_cpu_percent(cpu_percent: Option<Option<f64>>) -> String {
    match cpu_percent {
        None => "unavailable (older Nuthatch)".into(),
        Some(None) => "warming up".into(),
        Some(Some(percent)) => format!("{percent:.1}%"),
    }
}

/// `None` = Nuthatch does not publish the histogram at all; `Some(None)` = published but no RPC
/// call has been observed yet; `Some(Some(ms))` = average round-trip in milliseconds, summed
/// across every RPC endpoint the way the rest of this client already aggregates labelled series.
fn rpc_latency_ms(metrics: &BTreeMap<String, f64>) -> Option<Option<f64>> {
    let sum = metrics.get("nuthatch_rpc_request_duration_seconds_sum")?;
    let count = metrics.get("nuthatch_rpc_request_duration_seconds_count")?;
    Some((*count > 0.0).then(|| sum / count * 1000.0))
}

fn format_rpc_latency(latency: Option<Option<f64>>) -> String {
    match latency {
        None => "unavailable".into(),
        Some(None) => "no calls yet".into(),
        Some(Some(ms)) => format!("{ms:.0} ms avg"),
    }
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

/// A zero here is a fact, not a gap: a nest that has sealed nothing yet genuinely occupies no
/// bytes, and saying `unavailable` would be the same misreport in the other direction. Absence is
/// `format_optional_bytes`'s job.
fn format_bytes(value: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    match value {
        value if value < KIB => format!("{value} B"),
        value if value < MIB => format!("{} KiB", value / KIB),
        value if value < GIB => format!("{:.1} MiB", value as f64 / MIB as f64),
        value => format!("{:.1} GiB", value as f64 / GIB as f64),
    }
}

fn format_optional_bytes(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), format_bytes)
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

fn panel<'a>(title: &str) -> Block<'a> {
    Block::default()
        .title(Line::from(format!(" {title} ")).style(Style::default().fg(Color::Cyan).bold()))
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
        Constraint::Length(7),
        Constraint::Min(10),
        Constraint::Length(3),
    ])
    .split(area);

    let backfill_active = metric_u64(&app.snapshot.metrics, "nuthatch_direct_backfill_active") != 0;
    let backfill_from = metric_u64(&app.snapshot.metrics, "nuthatch_direct_backfill_from_block");
    let backfill_current = metric_u64(
        &app.snapshot.metrics,
        "nuthatch_direct_backfill_current_block",
    );
    let backfill_target = metric_u64(
        &app.snapshot.metrics,
        "nuthatch_direct_backfill_target_block",
    );
    let state = if backfill_active {
        ("● BACKFILL", Color::Magenta)
    } else if app.snapshot.ready.ready && !app.snapshot.ready.stalled && !app.snapshot.ready.wedged
    {
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
        Span::styled(
            format!(
                "   {}",
                app.snapshot
                    .nest_name
                    .as_deref()
                    .unwrap_or("discovering nest…")
            ),
            Style::default().fg(Color::Cyan).bold(),
        ),
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
                if backfill_active {
                    "BACKFILL"
                } else if ready.ready {
                    "READY"
                } else {
                    "WAITING"
                },
                Style::default().fg(state.1).bold(),
            ),
        ]),
        Line::from(format!("Tip             {}", ready.tip)),
        Line::from(format!("Indexed         {}", ready.last_block)),
        Line::from(format!("Finalised       {}", ready.sealed_through)),
        Line::from(if backfill_active {
            format!("Range           {backfill_from}..={backfill_target}")
        } else {
            "Range           —".into()
        }),
    ])
    .block(panel("NEST HEALTH"));
    frame.render_widget(health, top[0]);

    let lag_ratio = if backfill_active {
        let span = backfill_target.saturating_sub(backfill_from).max(1);
        backfill_current.saturating_sub(backfill_from).min(span) as f64 / span as f64
    } else if ready.tip == 0 {
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
            .label(if backfill_active {
                format!("{backfill_current} / {backfill_target}")
            } else {
                format!("{} / {}", ready.last_block, ready.tip)
            }),
        top[2],
    );

    let bottom = Layout::horizontal([Constraint::Percentage(38), Constraint::Percentage(62)])
        .split(vertical[2]);
    // Several performance lines are wider than half the right-hand column, and at 100 columns the
    // panel used to lose both the disk and the RPC-health line off the bottom. It now takes the
    // right column whole, at a height fixed to its line count, and the selected-table summary
    // moves under the table list where four short lines sit comfortably at 38%.
    let left = Layout::vertical([Constraint::Min(4), Constraint::Length(6)]).split(bottom[0]);
    // The feed is the first thing to go when the terminal is short: a cropped metric line is a
    // misreport, whereas a missing feed is visibly missing.
    let panel_height = PERFORMANCE_LINES + 2;
    let show_feed = bottom[1].height >= panel_height + 3;
    let right = if show_feed {
        Layout::vertical([Constraint::Length(panel_height), Constraint::Min(3)]).split(bottom[1])
    } else {
        Layout::vertical([Constraint::Percentage(100)]).split(bottom[1])
    };
    let show_sparkline = show_feed && right[1].height >= 9;
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
        left[0],
    );

    let rpc = metric_u64(&app.snapshot.metrics, "nuthatch_rpc_requests_total");
    let rpc_methods = metric_u64(&app.snapshot.metrics, "nuthatch_rpc_methods_total");
    let reorgs = metric_u64(&app.snapshot.metrics, "nuthatch_reorgs_total");
    let selected = app
        .snapshot
        .selected_table
        .as_deref()
        .unwrap_or("no event tables");
    let rpc_per_second = app.rate(|sample| sample.rpc_requests);
    let method_per_second = app.rate(|sample| sample.rpc_methods);
    let decode_per_second = app.rate(|sample| sample.decoded_rows);
    let blocks_per_second = app.rate(|sample| sample.indexed_block);
    let activity = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("RPC REQUESTS  ", Style::default().fg(Color::Gray)),
            Span::styled(rpc.to_string(), Style::default().fg(Color::Yellow).bold()),
            Span::styled(
                format!(
                    " since start  {}  {}",
                    format_rate(rpc_per_second, "req/s"),
                    format_rate(rpc_per_second * 60.0, "req/min")
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        Line::from(format!(
            "RPC METHODS     {rpc_methods} since start  {}",
            format_rate(method_per_second, "calls/s")
        )),
        Line::from(format!(
            "DECODED ROWS    {} since start  {}  {}",
            metric_u64(&app.snapshot.metrics, "nuthatch_rows_decoded_total"),
            format_rate(decode_per_second, "rows/s"),
            format_rate(decode_per_second * 60.0, "rows/min")
        )),
        Line::from(format!(
            "INDEXED BLOCKS  {}  {}",
            format_rate(blocks_per_second, "blocks/s"),
            format_rate(blocks_per_second * 60.0, "blocks/min")
        )),
        Line::from(format!(
            "MEMORY RSS      {}",
            format_optional_bytes(metric_opt_u64(&app.snapshot.metrics, "nuthatch_rss_bytes"))
        )),
        Line::from(format!(
            "API REFRESH     {} ms  source poll age {} s",
            app.refresh_time.map_or(0, |time| time.as_millis()),
            ready.seconds_since_poll
        )),
        Line::from(format!(
            "REORGS  {reorgs} since start   CPU  {}",
            format_cpu_percent(app.cpu_percent())
        )),
        Line::from(format!(
            "DISK            hot {}  sealed {}",
            format_optional_bytes(metric_opt_u64(
                &app.snapshot.metrics,
                "nuthatch_hot_store_bytes"
            )),
            format_optional_bytes(metric_opt_u64(
                &app.snapshot.metrics,
                "nuthatch_sealed_segments_bytes"
            )),
        )),
        Line::from(format!(
            "RPC HEALTH      fail {}  retry {}  latency {}",
            format_optional_count(metric_opt_u64(
                &app.snapshot.metrics,
                "nuthatch_rpc_endpoint_failures_total"
            )),
            format_optional_count(metric_opt_u64(
                &app.snapshot.metrics,
                "nuthatch_rpc_endpoint_retries_total"
            )),
            format_rpc_latency(rpc_latency_ms(&app.snapshot.metrics)),
        )),
    ])
    .block(panel(&format!(
        "PERFORMANCE  rates {}  (w)",
        app.rate_window_label()
    )));
    frame.render_widget(activity, right[0]);

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
    frame.render_widget(summary, left[1]);
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
    if show_feed {
        let feed_area = if show_sparkline {
            Layout::vertical([Constraint::Min(3), Constraint::Length(4)]).split(right[1])
        } else {
            Layout::vertical([Constraint::Percentage(100)]).split(right[1])
        };
        frame.render_widget(
            List::new(feed_rows).block(panel("LIVE EVENT FEED")),
            feed_area[0],
        );
        if show_sparkline {
            frame.render_widget(
                Sparkline::default()
                    .block(panel("RPC ACTIVITY / REFRESH"))
                    .data(&rpc_samples)
                    .style(Style::default().fg(Color::Yellow)),
                feed_area[1],
            );
        }
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
        Span::styled(
            " w ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" rate window   "),
        Span::styled(&app.status, Style::default().fg(Color::DarkGray)),
    ]));
    frame.render_widget(footer, vertical[3]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    /// A dashboard populated the way a healthy mainnet nest populates it, for the layout tests.
    fn rendered(width: u16, height: u16) -> String {
        let mut app = App::new("http://127.0.0.1:8288".into());
        app.snapshot.nest_name = Some("graph-staking-nest".into());
        app.snapshot.ready = Ready {
            ready: true,
            lag_blocks: 0,
            last_block: 25_766_811,
            sealed_through: 25_766_747,
            tip: 25_766_811,
            seconds_since_poll: 1,
            ..Ready::default()
        };
        app.snapshot.metrics = parse_prometheus(
            "nuthatch_rows_decoded_total 2275\n\
             nuthatch_rows_sealed_total 2453\n\
             nuthatch_rpc_requests_total 367\n\
             nuthatch_rpc_methods_total 412\n\
             nuthatch_reorgs_total 0\n\
             nuthatch_rss_bytes 63963136\n\
             nuthatch_process_cpu_seconds_total 4.5\n\
             nuthatch_hot_store_bytes 2113536\n\
             nuthatch_sealed_segments_bytes 48731\n\
             nuthatch_rpc_endpoint_failures_total 69\n\
             nuthatch_rpc_endpoint_retries_total 35\n\
             nuthatch_rpc_request_duration_seconds_sum 15.3\n\
             nuthatch_rpc_request_duration_seconds_count 221\n",
        );
        app.snapshot.tables = Tables {
            count: 2,
            tables: vec![
                EventTable {
                    table: "usdc__approval".into(),
                },
                EventTable {
                    table: "usdc__transfer".into(),
                },
            ],
        };
        app.snapshot.selected_table = Some("usdc__approval".into());
        app.snapshot.selected_rows = Some(2275);
        app.snapshot.selected_latest_block = Some(25_766_811);
        app.refresh_time = Some(Duration::from_millis(12));
        // Two samples a full window apart, so the rolling rates render at a realistic width
        // rather than the flattering "0 req/s" a single sample would give.
        let now = Instant::now();
        let earlier = now
            .checked_sub(Duration::from_secs(60))
            .expect("a host that has been up for a minute");
        app.samples = vec![
            Sample {
                at: earlier,
                decoded_rows: 1000,
                rpc_requests: 300,
                rpc_methods: 340,
                indexed_block: 25_766_741,
                cpu_seconds: Some(2.4),
            },
            Sample {
                at: now,
                decoded_rows: 2275,
                rpc_requests: 367,
                rpc_methods: 412,
                indexed_block: 25_766_811,
                cpu_seconds: Some(4.5),
            },
        ];

        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("test terminal");
        terminal.draw(|frame| draw(frame, &app)).expect("draw");
        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The README advertises 100 columns as the pleasant setting, so 100 columns is where the
    /// panel has to hold every line it claims to show. It used to crop the last two entirely.
    #[test]
    fn performance_panel_shows_every_metric_at_one_hundred_columns() {
        let screen = rendered(100, 30);
        for expected in [
            "PERFORMANCE  rates last 60s  (w)",
            "RPC REQUESTS  367 since start  1.1 req/s  67 req/min",
            "RPC METHODS     412 since start  1.2 calls/s",
            "DECODED ROWS    2275 since start  21 rows/s  1275 rows/min",
            "INDEXED BLOCKS  1.2 blocks/s  70 blocks/min",
            "MEMORY RSS      61.0 MiB",
            "API REFRESH     12 ms  source poll age 1 s",
            "REORGS  0 since start   CPU  ",
            "DISK            hot 2.0 MiB  sealed 47 KiB",
            "RPC HEALTH      fail 69  retry 35  latency 69 ms avg",
        ] {
            assert!(
                screen.contains(expected),
                "performance panel dropped or truncated {expected:?} at 100x30:\n{screen}"
            );
        }
    }

    /// The selected-table summary and the event feed have to survive the same squeeze.
    #[test]
    fn table_summary_and_feed_survive_at_one_hundred_columns() {
        let screen = rendered(100, 30);
        for expected in [
            "SELECTED TABLE",
            "usdc__approval",
            "Rows    2275",
            "Latest  25766811",
            "Storage integrity: healthy",
            "LIVE EVENT FEED",
        ] {
            assert!(
                screen.contains(expected),
                "{expected:?} missing at 100x30:\n{screen}"
            );
        }
    }

    /// The sparkline is the last thing to arrive, and the README quotes the height at which it
    /// does. Asserting the boundary keeps that sentence honest.
    #[test]
    fn the_sparkline_arrives_at_thirty_three_rows() {
        assert!(!rendered(100, 32).contains("RPC ACTIVITY"));
        assert!(rendered(100, 33).contains("RPC ACTIVITY"));
    }

    /// At 80x24 there is no room for both the panel and the feed. The feed is what gives way: a
    /// missing panel is visibly missing, whereas a cropped metric line reads as a smaller number.
    #[test]
    fn a_short_terminal_drops_the_feed_rather_than_a_metric_line() {
        let screen = rendered(80, 24);
        for expected in ["MEMORY RSS", "REORGS", "DISK", "RPC HEALTH"] {
            assert!(
                screen.contains(expected),
                "{expected:?} cropped at 80x24:\n{screen}"
            );
        }
        assert!(
            !screen.contains("LIVE EVENT FEED"),
            "the feed should have given way at 80x24:\n{screen}"
        );
    }

    #[test]
    fn prometheus_parser_keeps_plain_metrics_only() {
        let metrics = parse_prometheus(
            "# HELP ignored\nnuthatch_rows_decoded_total 42\nnuthatch_nest_rows_decoded_total{nest=\"x\"} 41\n",
        );
        assert_eq!(metrics.get("nuthatch_rows_decoded_total"), Some(&42.0));
        assert_eq!(metrics.get("nuthatch_nest_rows_decoded_total"), Some(&41.0));
    }

    #[test]
    fn prometheus_parser_sums_labelled_counter_series() {
        let metrics = parse_prometheus(
            "nuthatch_rpc_methods_total{method=\"eth_getLogs\"} 4\n\
             nuthatch_rpc_methods_total{method=\"eth_getBlockByNumber\"} 9\n",
        );
        assert_eq!(metrics.get("nuthatch_rpc_methods_total"), Some(&13.0));
    }

    #[test]
    fn url_has_no_trailing_slash() {
        assert_eq!(
            normalize_url("http://localhost:8288/".into()),
            "http://localhost:8288"
        );
    }

    #[test]
    fn cpu_percent_is_unavailable_when_metric_absent() {
        assert_eq!(format_cpu_percent(None), "unavailable (older Nuthatch)");
    }

    #[test]
    fn cpu_percent_warms_up_before_a_second_sample() {
        assert_eq!(format_cpu_percent(Some(None)), "warming up");
    }

    #[test]
    fn cpu_percent_formats_one_decimal() {
        assert_eq!(format_cpu_percent(Some(Some(12.34))), "12.3%");
    }

    /// A live staking nest on arbitrum-one reported 1261.9 MiB of resident memory, which is one
    /// rung above where this ladder used to stop.
    #[test]
    fn bytes_climb_past_a_gibibyte() {
        assert_eq!(format_bytes(1_020 * 1024 * 1024), "1020.0 MiB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GiB");
        assert_eq!(format_bytes(1_323_205_427), "1.2 GiB");
    }

    /// A nest that has sealed nothing occupies no bytes. That is a measurement, and saying
    /// `unavailable` instead would be the same misreport the panel exists to avoid.
    #[test]
    fn zero_bytes_is_a_measurement_and_absence_is_not() {
        assert_eq!(format_optional_bytes(Some(0)), "0 B");
        assert_eq!(format_optional_bytes(Some(512)), "512 B");
        assert_eq!(format_optional_bytes(None), "unavailable");
    }

    #[test]
    fn rpc_latency_is_unavailable_without_the_histogram() {
        let metrics = BTreeMap::new();
        assert_eq!(rpc_latency_ms(&metrics), None);
    }

    #[test]
    fn rpc_latency_is_no_calls_yet_with_zero_count() {
        let mut metrics = BTreeMap::new();
        metrics.insert("nuthatch_rpc_request_duration_seconds_sum".into(), 0.0);
        metrics.insert("nuthatch_rpc_request_duration_seconds_count".into(), 0.0);
        assert_eq!(rpc_latency_ms(&metrics), Some(None));
    }

    #[test]
    fn rpc_latency_averages_sum_over_count_in_milliseconds() {
        let mut metrics = BTreeMap::new();
        metrics.insert("nuthatch_rpc_request_duration_seconds_sum".into(), 2.0);
        metrics.insert("nuthatch_rpc_request_duration_seconds_count".into(), 4.0);
        assert_eq!(rpc_latency_ms(&metrics), Some(Some(500.0)));
    }

    /// A real `/metrics` snippet captured from a running `nuthatch dev` (v2.7.1) against two RPC
    /// endpoints, macOS host. Guards against silent drift in Nuthatch's exposition format.
    #[test]
    fn live_metrics_snippet_parses_and_formats() {
        let metrics = parse_prometheus(
            "nuthatch_rss_bytes 63963136\n\
             nuthatch_process_cpu_seconds_total 0.000000\n\
             nuthatch_hot_store_bytes 2113536\n\
             nuthatch_sealed_segments_bytes 48731\n\
             nuthatch_rpc_endpoint_requests_total{endpoint=\"eth-pokt.nodies.app\"} 35\n\
             nuthatch_rpc_endpoint_failures_total{endpoint=\"eth-pokt.nodies.app\"} 35\n\
             nuthatch_rpc_endpoint_retries_total{endpoint=\"eth-pokt.nodies.app\"} 34\n\
             nuthatch_rpc_request_duration_seconds_sum{endpoint=\"eth-pokt.nodies.app\"} 1.5274509169999997\n\
             nuthatch_rpc_request_duration_seconds_count{endpoint=\"eth-pokt.nodies.app\"} 35\n\
             nuthatch_rpc_endpoint_requests_total{endpoint=\"eth.drpc.org\"} 186\n\
             nuthatch_rpc_endpoint_failures_total{endpoint=\"eth.drpc.org\"} 34\n\
             nuthatch_rpc_endpoint_retries_total{endpoint=\"eth.drpc.org\"} 1\n\
             nuthatch_rpc_request_duration_seconds_sum{endpoint=\"eth.drpc.org\"} 13.819300575999996\n\
             nuthatch_rpc_request_duration_seconds_count{endpoint=\"eth.drpc.org\"} 186\n",
        );

        assert_eq!(
            format_optional_bytes(metric_opt_u64(&metrics, "nuthatch_hot_store_bytes")),
            "2.0 MiB"
        );
        assert_eq!(
            format_optional_bytes(metric_opt_u64(&metrics, "nuthatch_sealed_segments_bytes")),
            "47 KiB"
        );
        // Failures/retries sum across both labelled endpoints, matching how RPC methods already sum.
        assert_eq!(
            format_optional_count(metric_opt_u64(
                &metrics,
                "nuthatch_rpc_endpoint_failures_total"
            )),
            "69"
        );
        assert_eq!(
            format_optional_count(metric_opt_u64(
                &metrics,
                "nuthatch_rpc_endpoint_retries_total"
            )),
            "35"
        );
        // (1.5274509169999997 + 13.819300575999996) / (35 + 186) * 1000 ≈ 69.5 ms
        assert_eq!(format_rpc_latency(rpc_latency_ms(&metrics)), "69 ms avg");
        // v2.7.1's CPU sampler was Linux-only (nightswatchhq/nuthatch#844), so on this macOS
        // capture the counter is present but pinned at 0.0: a real value, not a missing one.
        assert_eq!(
            metrics.get("nuthatch_process_cpu_seconds_total"),
            Some(&0.0)
        );
    }

    #[test]
    fn extracts_authored_nest_name_from_schema() {
        assert_eq!(
            nest_name_from_schema(
                "nuthatch data model\n\nThe `graph-staking-nest` nest on arbitrum-one.\n"
            ),
            Some("graph-staking-nest".into())
        );
    }
}
