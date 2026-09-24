use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use crossterm::{
    cursor::Show,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    prelude::*,
    widgets::{
        Block, BorderType, Borders, Gauge, List, ListItem, ListState, Padding, Paragraph,
        Sparkline, Wrap,
    },
};
use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Bounds on an interval taken from the nest's own `freshness.poll_interval_secs`. A five-minute
/// cursor still gets a dashboard that notices a crash within half a minute.
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(30);
const HISTORY_LEN: usize = 48;
const TABLE_PAGE: usize = 10;
const ACTIVITY_LEN: usize = 64;
/// How long an observed restart keeps the restart line lit.
const RECENT_RESTART: Duration = Duration::from_secs(600);
/// Labelled metric lines in the performance panel. The panel is laid out at exactly this height so
/// that none of them is silently cropped; raise it with the panel.
const PERFORMANCE_LINES: u16 = 9;
const RATE_WINDOWS: [Duration; 3] = [
    Duration::from_secs(15),
    Duration::from_secs(60),
    Duration::from_secs(90),
];
const CANVAS: Color = Color::Rgb(11, 14, 20);

#[derive(Debug, Deserialize, Default, Clone)]
struct Ready {
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    stalled: bool,
    #[serde(default)]
    wedged: bool,
    #[serde(default)]
    initial_poll_failed: bool,
    #[serde(default)]
    seal_direct_stalled: bool,
    #[serde(default)]
    entities_stalled: bool,
    #[serde(default)]
    quarantined: bool,
    /// Null for a cursorless role, which has no tip to lag behind. Zero would claim "at tip".
    tip: Option<u64>,
    lag_blocks: Option<u64>,
    #[serde(default)]
    last_block: u64,
    #[serde(default)]
    sealed_through: u64,
    #[serde(default)]
    seconds_since_poll: u64,
    freshness: Option<Freshness>,
    #[serde(default)]
    seal_direct_active: bool,
    seal_direct_origin: Option<u64>,
    seal_direct_completed: Option<u64>,
    seal_direct_target: Option<u64>,
    /// Published from Nuthatch 3.9.0.
    version: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct Freshness {
    poll_interval_secs: Option<u64>,
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
struct NestDocument {
    name: Option<String>,
    chain: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct RootDocument {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct QueriesDocument {
    #[serde(default)]
    sql: String,
    #[serde(default = "yes")]
    free_form: bool,
    #[serde(default)]
    queries: Vec<NamedQuery>,
}

#[derive(Debug, Deserialize)]
struct NamedQuery {
    name: String,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq)]
enum SqlAccess {
    Open,
    Closed { mode: String, named: Vec<String> },
}

/// What the nest is, as opposed to how it is doing. Nuthatch builds all of it at startup and never
/// changes it, so it is fetched once and again only after a restart.
struct Identity {
    nest_name: Option<String>,
    chain: Option<String>,
    tables: Tables,
    sql: SqlAccess,
}

#[derive(Default)]
struct Selection {
    table: String,
    rows: Option<u64>,
    latest_block: Option<u64>,
    events: Vec<Value>,
    degraded: bool,
}

#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    decoded_rows: Option<u64>,
    rpc_requests: Option<u64>,
    rpc_methods: Option<u64>,
    indexed_block: Option<u64>,
    cpu_seconds: Option<f64>,
}

impl Sample {
    /// Every counter here is monotonic for the life of a Nuthatch process, so one going backwards
    /// means a new process. `indexed_block` is left out because a reorg legitimately rewinds it.
    fn follows_restart_of(&self, before: &Sample) -> bool {
        let fell = |before: Option<u64>, after: Option<u64>| matches!((before, after), (Some(b), Some(a)) if a < b);
        fell(before.rpc_requests, self.rpc_requests)
            || fell(before.decoded_rows, self.decoded_rows)
            || matches!((before.cpu_seconds, self.cpu_seconds), (Some(b), Some(a)) if a < b)
    }
}

#[derive(Clone, Copy)]
struct Bucket {
    start: Instant,
    rpc_requests: u64,
    peak_refresh_ms: u64,
}

/// Fixed-width time buckets for the activity sparklines. The width follows the nest's poll
/// interval, so a bar is one nest poll's worth of work however often the client happens to ask.
#[derive(Default)]
struct Activity {
    width: Duration,
    buckets: VecDeque<Bucket>,
}

impl Activity {
    fn record(&mut self, at: Instant, width: Duration, rpc_requests: u64, refresh_ms: u64) {
        if width != self.width {
            self.buckets.clear();
            self.width = width;
        }
        match self.buckets.back_mut() {
            Some(bucket) if at.duration_since(bucket.start) < width => {
                bucket.rpc_requests += rpc_requests;
                bucket.peak_refresh_ms = bucket.peak_refresh_ms.max(refresh_ms);
            }
            _ => {
                self.buckets.push_back(Bucket {
                    start: at,
                    rpc_requests,
                    peak_refresh_ms: refresh_ms,
                });
                if self.buckets.len() > ACTIVITY_LEN {
                    self.buckets.pop_front();
                }
            }
        }
    }
}

struct Backfill {
    origin: u64,
    current: u64,
    target: u64,
}

type Problem = (&'static str, String);

/// A poll names the selected table rather than indexing it, so a catalogue refetched after a
/// restart keeps the operator's place in it.
struct PollRequest {
    identity: bool,
    table: Option<String>,
    sql_open: bool,
}

struct PollResult {
    identity: Option<Result<Identity, Problem>>,
    ready: Result<Ready, String>,
    metrics: Result<BTreeMap<String, f64>, String>,
    table: Option<String>,
    selection: Option<Result<Selection, String>>,
    elapsed: Duration,
}

enum Request {
    Poll(PollRequest),
    Selection(String),
}

enum Reply {
    Poll(Box<PollResult>),
    Selection(Result<Selection, String>),
}

fn poll(client: &Client, base: &str, request: &PollRequest) -> PollResult {
    let started = Instant::now();
    let identity = request.identity.then(|| fetch_identity(client, base));
    let ready = fetch_ready(client, base);
    let metrics =
        fetch_ok(client, &format!("{base}/metrics"), &[]).map(|text| parse_prometheus(&text));
    let (table, sql_open) = match &identity {
        Some(Ok(identity)) => {
            let tables = &identity.tables.tables;
            let table = request
                .table
                .clone()
                .filter(|name| tables.iter().any(|table| table.table == *name))
                .or_else(|| tables.first().map(|table| table.table.clone()));
            (table, identity.sql == SqlAccess::Open)
        }
        _ => (request.table.clone(), request.sql_open),
    };
    let selection = table
        .as_deref()
        .filter(|_| sql_open)
        .map(|table| fetch_selection(client, base, table));
    PollResult {
        identity,
        ready,
        metrics,
        table,
        selection,
        elapsed: started.elapsed(),
    }
}

/// Requests run here so that a slow `/sql` delays the numbers rather than the keyboard.
fn spawn_worker(client: Client, base: String) -> (Sender<Request>, Receiver<Reply>) {
    let (requests, inbox) = mpsc::channel();
    let (outbox, replies) = mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(first) = inbox.recv() {
            // Holding `j` queues a selection per keypress; only the last one is worth asking for.
            let (mut next_poll, mut next_selection) = (None, None);
            for request in std::iter::once(first).chain(inbox.try_iter()) {
                match request {
                    Request::Poll(request) => next_poll = Some(request),
                    Request::Selection(table) => next_selection = Some(table),
                }
            }
            let replies = next_poll
                .map(|request| Reply::Poll(Box::new(poll(&client, &base, &request))))
                .into_iter()
                .chain(
                    next_selection
                        .map(|table| Reply::Selection(fetch_selection(&client, &base, &table))),
                );
            for reply in replies {
                if outbox.send(reply).is_err() {
                    return;
                }
            }
        }
    });
    (requests, replies)
}

struct App {
    url: String,
    interval_override: Option<Duration>,
    no_color: bool,
    identity: Option<Identity>,
    /// Set by an observed restart: the next poll fetches the catalogue again.
    refetch_identity: bool,
    ready: Option<Ready>,
    /// The last `/ready` failed, so `ready` is the previous answer and everything is stale.
    ready_failed: bool,
    metrics: Option<BTreeMap<String, f64>>,
    selection: Option<Selection>,
    problems: Vec<Problem>,
    samples: Vec<Sample>,
    activity: Activity,
    restarts: u32,
    last_restart: Option<Instant>,
    rate_window: usize,
    selected_table: usize,
    /// The table list's scroll position, kept between frames so moving up does not jerk the view.
    table_offset: Cell<usize>,
    refresh_time: Option<Duration>,
    last_refresh: Option<Instant>,
    poll_in_flight: bool,
    should_quit: bool,
}

impl App {
    fn new(url: String) -> Self {
        Self {
            url,
            interval_override: None,
            no_color: false,
            identity: None,
            refetch_identity: false,
            ready: None,
            ready_failed: false,
            metrics: None,
            selection: None,
            problems: Vec::new(),
            samples: Vec::new(),
            activity: Activity::default(),
            restarts: 0,
            last_restart: None,
            rate_window: 1,
            selected_table: 0,
            table_offset: Cell::new(0),
            refresh_time: None,
            last_refresh: None,
            poll_in_flight: false,
            should_quit: false,
        }
    }

    fn refresh_due(&self) -> bool {
        !self.poll_in_flight
            && self
                .last_refresh
                .is_none_or(|at| at.elapsed() >= self.poll_interval())
    }

    fn poll_request(&self) -> PollRequest {
        PollRequest {
            identity: self.identity.is_none() || self.refetch_identity,
            table: self.selected_table_name().map(str::to_owned),
            sql_open: self
                .identity
                .as_ref()
                .is_some_and(|identity| identity.sql == SqlAccess::Open),
        }
    }

    fn apply(&mut self, result: PollResult) {
        self.poll_in_flight = false;
        let mut problems = Vec::new();
        match result.identity {
            Some(Ok(identity)) => {
                self.selected_table = result
                    .table
                    .as_deref()
                    .and_then(|name| {
                        identity
                            .tables
                            .tables
                            .iter()
                            .position(|table| table.table == name)
                    })
                    .unwrap_or_default();
                self.identity = Some(identity);
                self.refetch_identity = false;
            }
            Some(Err(problem)) => problems.push(problem),
            None => {}
        }
        match result.ready {
            Ok(ready) => {
                self.ready = Some(ready);
                self.ready_failed = false;
            }
            Err(error) => {
                problems.push(("/ready", error));
                self.ready_failed = true;
            }
        }
        self.metrics = match result.metrics {
            Ok(metrics) => Some(metrics),
            Err(error) => {
                problems.push(("/metrics", error));
                None
            }
        };
        match result.selection {
            Some(Ok(selection)) => self.selection = Some(selection),
            Some(Err(error)) => {
                self.selection = None;
                problems.push(("/sql", error));
            }
            None => {}
        }
        self.refresh_time = Some(result.elapsed);
        self.problems = problems;
        if !self.ready_failed {
            self.record_sample(result.elapsed);
        }
        self.last_refresh = Some(Instant::now());
    }

    fn apply_selection(&mut self, result: Result<Selection, String>) {
        self.problems.retain(|(endpoint, _)| *endpoint != "/sql");
        match result {
            Ok(selection) => self.selection = Some(selection),
            Err(error) => self.problems.push(("/sql", error)),
        }
    }

    fn record_sample(&mut self, refresh_time: Duration) {
        let metrics = self.metrics.as_ref();
        let sample = Sample {
            at: Instant::now(),
            decoded_rows: metrics.and_then(|m| metric_opt_u64(m, "nuthatch_rows_decoded_total")),
            rpc_requests: metrics.and_then(|m| metric_opt_u64(m, "nuthatch_rpc_requests_total")),
            rpc_methods: metrics.and_then(|m| metric_opt_u64(m, "nuthatch_rpc_methods_total")),
            indexed_block: self.ready.as_ref().map(|ready| ready.last_block),
            cpu_seconds: metrics
                .and_then(|m| m.get("nuthatch_process_cpu_seconds_total"))
                .copied(),
        };
        if let Some(previous) = self.samples.last()
            && sample.follows_restart_of(previous)
        {
            self.restarts += 1;
            self.last_restart = Some(sample.at);
            self.samples.clear();
            self.activity.buckets.clear();
            // A restart may have come with a new configuration, and so a new catalogue.
            self.refetch_identity = true;
        }
        let rpc_delta = self
            .samples
            .last()
            .and_then(|previous| Some(sample.rpc_requests?.saturating_sub(previous.rpc_requests?)))
            .unwrap_or_default();
        self.activity.record(
            sample.at,
            self.activity_width(),
            rpc_delta,
            refresh_time.as_millis() as u64,
        );
        self.samples.push(sample);
        if self.samples.len() > HISTORY_LEN {
            self.samples.remove(0);
        }
    }

    fn nest_poll_interval(&self) -> Option<Duration> {
        self.ready
            .as_ref()?
            .freshness
            .as_ref()?
            .poll_interval_secs
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs)
    }

    fn poll_interval(&self) -> Duration {
        self.interval_override.unwrap_or_else(|| {
            self.nest_poll_interval()
                .map_or(DEFAULT_POLL_INTERVAL, |interval| {
                    interval.clamp(MIN_POLL_INTERVAL, MAX_POLL_INTERVAL)
                })
        })
    }

    fn activity_width(&self) -> Duration {
        self.nest_poll_interval()
            .unwrap_or_default()
            .max(self.poll_interval())
    }

    /// Moves the selection, clamped to the list, and returns the table to query if it changed and
    /// the nest allows asking.
    fn select(&mut self, index: usize) -> Option<String> {
        let last = self.table_count().checked_sub(1)?;
        let index = index.min(last);
        if index == self.selected_table {
            return None;
        }
        self.selected_table = index;
        self.identity
            .as_ref()
            .filter(|identity| identity.sql == SqlAccess::Open)?;
        self.selected_table_name().map(str::to_owned)
    }

    fn select_next(&mut self) -> Option<String> {
        let count = self.table_count().max(1);
        self.select((self.selected_table + 1) % count)
    }

    fn select_previous(&mut self) -> Option<String> {
        let count = self.table_count().max(1);
        self.select((self.selected_table + count - 1) % count)
    }

    fn table_count(&self) -> usize {
        self.identity
            .as_ref()
            .map_or(0, |identity| identity.tables.tables.len())
    }

    fn selected_table_name(&self) -> Option<&str> {
        self.identity
            .as_ref()?
            .tables
            .tables
            .get(self.selected_table)
            .map(|table| table.table.as_str())
    }

    fn backfill(&self) -> Option<Backfill> {
        let ready = self.ready.as_ref()?;
        if ready.seal_direct_active {
            return Some(Backfill {
                origin: ready.seal_direct_origin.unwrap_or_default(),
                current: ready.seal_direct_completed.unwrap_or_default(),
                target: ready.seal_direct_target.unwrap_or_default(),
            });
        }
        // Nuthatch before 3.4 published the pass as gauges rather than on `/ready`.
        let metrics = self.metrics.as_ref()?;
        (metric_u64(metrics, "nuthatch_direct_backfill_active") != 0).then(|| Backfill {
            origin: metric_u64(metrics, "nuthatch_direct_backfill_from_block"),
            current: metric_u64(metrics, "nuthatch_direct_backfill_current_block"),
            target: metric_u64(metrics, "nuthatch_direct_backfill_target_block"),
        })
    }

    /// The header marker. `(partial)` means the nest answered but some of what the screen shows
    /// could not be fetched, so a green marker cannot sit over a panel full of `unavailable`.
    fn state(&self) -> (String, Color) {
        let Some(ready) = self.ready.as_ref() else {
            return ("● CONNECTING".into(), Color::Gray);
        };
        if self.ready_failed {
            return ("● STALE".into(), Color::Red);
        }
        let (label, color) = if ready.quarantined {
            ("● QUARANTINED", Color::Red)
        } else if self.backfill().is_some() {
            ("● BACKFILL", Color::Magenta)
        } else if ready.ready
            && !ready.stalled
            && !ready.wedged
            && !ready.initial_poll_failed
            && !ready.seal_direct_stalled
            && !ready.entities_stalled
        {
            ("● LIVE", Color::Green)
        } else {
            ("● ATTENTION", Color::Yellow)
        };
        if self.problems.is_empty() {
            (label.into(), color)
        } else {
            (format!("{label} (partial)"), Color::Yellow)
        }
    }

    fn status(&self) -> String {
        if self.last_refresh.is_none() {
            "Connecting to nest…".into()
        } else if self.problems.is_empty() {
            "Live data received".into()
        } else {
            self.problems
                .iter()
                .map(|(endpoint, error)| format!("{endpoint}: {error}"))
                .collect::<Vec<_>>()
                .join("  ·  ")
        }
    }

    fn window_pair(&self) -> Option<(Sample, Sample)> {
        let after = self.samples.last().copied()?;
        let window = RATE_WINDOWS[self.rate_window];
        let before = self
            .samples
            .iter()
            .rev()
            .copied()
            .find(|sample| after.at.duration_since(sample.at) >= window)
            .or_else(|| self.samples.first().copied())?;
        Some((before, after))
    }

    fn rate(&self, field: impl Fn(Sample) -> Option<u64>) -> f64 {
        let Some((before, after)) = self.window_pair() else {
            return 0.0;
        };
        let elapsed = after.at.duration_since(before.at).as_secs_f64();
        match (field(before), field(after)) {
            (Some(before), Some(after)) if elapsed > 0.0 => {
                after.saturating_sub(before) as f64 / elapsed
            }
            _ => 0.0,
        }
    }

    /// `Some(None)` distinguishes "the metric is published but we're still warming up a window"
    /// from `None`, "this Nuthatch does not publish `nuthatch_process_cpu_seconds_total` at all".
    /// There is a third case the client cannot see. Nuthatch before 3.0.0 read `/proc/self/stat`
    /// and nothing else (nightswatchhq/nuthatch#844), so such a nest hosted off Linux publishes the
    /// counter pinned at 0.0, which arrives here as `Some(Some(0.0))`.
    fn cpu_percent(&self) -> Option<Option<f64>> {
        let after = self.samples.last().copied()?;
        let after_cpu = after.cpu_seconds?;
        let (before, _) = self.window_pair()?;
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

struct Args {
    url: String,
    interval: Option<Duration>,
}

fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .context("building HTTP client")?;
    let mut app = App::new(args.url);
    app.interval_override = args.interval;
    app.no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());

    let quit = Arc::new(AtomicBool::new(false));
    #[cfg(unix)]
    for signal in [
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGHUP,
        signal_hook::consts::SIGINT,
    ] {
        signal_hook::flag::register(signal, Arc::clone(&quit))
            .context("installing signal handler")?;
    }
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore_terminal();
        default_hook(info);
    }));

    let _screen = Screen::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    run(&mut terminal, client, &mut app, &quit)
}

/// Raw mode and the alternate screen, undone on drop. A signal is turned into an ordinary return
/// from `run` so that this drop happens; without it `kill` left the shell in raw mode.
struct Screen;

impl Screen {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        execute!(io::stdout(), EnterAlternateScreen)?;
        Ok(Self)
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), LeaveAlternateScreen, Show);
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args> {
    let mut parsed = Args {
        url: "http://127.0.0.1:8288".into(),
        interval: None,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => {
                parsed.url = args
                    .next()
                    .map(normalize_url)
                    .context("--url needs a Nuthatch base URL")?;
            }
            "--interval" => {
                let value = args
                    .next()
                    .context("--interval needs a duration, e.g. 5s")?;
                parsed.interval = Some(parse_interval(&value)?);
            }
            "-h" | "--help" => {
                println!("nuthatch-tui-client [--url http://127.0.0.1:8288] [--interval 5s]");
                std::process::exit(0);
            }
            value => anyhow::bail!("unknown argument '{value}'; try --help"),
        }
    }
    Ok(parsed)
}

fn parse_interval(value: &str) -> Result<Duration> {
    let (digits, scale) = match value.strip_suffix('m') {
        Some(minutes) => (minutes, 60),
        None => (value.strip_suffix('s').unwrap_or(value), 1),
    };
    let amount: u64 = digits
        .parse()
        .with_context(|| format!("--interval '{value}' is not a duration like 5s or 2m"))?;
    anyhow::ensure!(amount > 0, "--interval must be longer than zero");
    Ok(Duration::from_secs(amount * scale))
}

fn normalize_url(value: String) -> String {
    value.trim_end_matches('/').to_owned()
}

fn run<B: Backend>(
    terminal: &mut Terminal<B>,
    client: Client,
    app: &mut App,
    quit: &AtomicBool,
) -> Result<()> {
    let (requests, replies) = spawn_worker(client, app.url.clone());
    let send = |request| {
        requests
            .send(request)
            .map_err(|_| anyhow::anyhow!("the fetch thread has stopped"))
    };
    loop {
        for reply in replies.try_iter() {
            match reply {
                Reply::Poll(result) => app.apply(*result),
                Reply::Selection(result) => app.apply_selection(result),
            }
        }
        if app.refresh_due() {
            app.poll_in_flight = true;
            send(Request::Poll(app.poll_request()))?;
        }
        terminal.draw(|frame| draw(frame, app))?;

        if event::poll(Duration::from_millis(50))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            let query = match key.code {
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    app.should_quit = true;
                    None
                }
                KeyCode::Char('q') | KeyCode::Esc => {
                    app.should_quit = true;
                    None
                }
                KeyCode::Char('r') => {
                    app.last_refresh = None;
                    None
                }
                KeyCode::Char('w') => {
                    app.cycle_rate_window();
                    None
                }
                KeyCode::Down | KeyCode::Char('j') => app.select_next(),
                KeyCode::Up | KeyCode::Char('k') => app.select_previous(),
                KeyCode::PageDown => app.select(app.selected_table.saturating_add(TABLE_PAGE)),
                KeyCode::PageUp => app.select(app.selected_table.saturating_sub(TABLE_PAGE)),
                KeyCode::Home | KeyCode::Char('g') => app.select(0),
                KeyCode::End | KeyCode::Char('G') => app.select(usize::MAX),
                _ => None,
            };
            if let Some(table) = query {
                send(Request::Selection(table))?;
            }
        }
        if app.should_quit || quit.load(Ordering::Relaxed) {
            return Ok(());
        }
    }
}

/// The endpoint is named by the caller; this says only what went wrong with it.
fn describe(error: &reqwest::Error) -> String {
    if let Some(status) = error.status() {
        format!("HTTP {status}")
    } else if error.is_timeout() {
        "timed out".into()
    } else if error.is_connect() {
        "cannot connect".into()
    } else if error.is_decode() {
        "unreadable response".into()
    } else {
        "request failed".into()
    }
}

fn fetch(
    client: &Client,
    url: &str,
    query: &[(&str, &str)],
) -> Result<(StatusCode, String), String> {
    let response = client
        .get(url)
        .query(query)
        .send()
        .map_err(|error| describe(&error))?;
    let status = response.status();
    let body = response.text().map_err(|error| describe(&error))?;
    Ok((status, body))
}

fn fetch_ok(client: &Client, url: &str, query: &[(&str, &str)]) -> Result<String, String> {
    let (status, body) = fetch(client, url, query)?;
    if status.is_success() {
        Ok(body)
    } else {
        Err(format!("HTTP {status}"))
    }
}

fn fetch_json<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    query: &[(&str, &str)],
) -> Result<T, String> {
    serde_json::from_str(&fetch_ok(client, url, query)?)
        .map_err(|_| "unreadable response".to_owned())
}

/// A stalled or quarantined nest answers 503 with the full body, and that body is exactly what
/// the operator needs to see. Treating the status as a failure hid every unhealthy state.
fn fetch_ready(client: &Client, base: &str) -> Result<Ready, String> {
    let (status, body) = fetch(client, &format!("{base}/ready"), &[])?;
    if !status.is_success() && status != StatusCode::SERVICE_UNAVAILABLE {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_str(&body).map_err(|_| "unreadable response".to_owned())
}

fn fetch_identity(client: &Client, base: &str) -> Result<Identity, Problem> {
    let tables: Tables =
        fetch_json(client, &format!("{base}/tables"), &[]).map_err(|error| ("/tables", error))?;
    let (nest_name, chain) = match fetch_json::<NestDocument>(client, &format!("{base}/nest"), &[])
    {
        Ok(nest) => (nest.name.filter(|name| !name.is_empty()), nest.chain),
        Err(_) => (fallback_nest_name(client, base), None),
    };
    // Absent on a nest too old to serve it, which is also a nest too old to close SQL.
    let sql = fetch_json::<QueriesDocument>(client, &format!("{base}/queries"), &[]).map_or(
        SqlAccess::Open,
        |queries| {
            if queries.free_form && matches!(queries.sql.as_str(), "open" | "") {
                SqlAccess::Open
            } else {
                SqlAccess::Closed {
                    mode: queries.sql,
                    named: queries
                        .queries
                        .into_iter()
                        .map(|query| query.name)
                        .collect(),
                }
            }
        },
    );
    Ok(Identity {
        nest_name,
        chain,
        tables,
        sql,
    })
}

/// `/schema` carries the authored nest name, whereas the compact root document historically
/// identifies the runtime itself as `nuthatch`.
fn fallback_nest_name(client: &Client, base: &str) -> Option<String> {
    fetch_ok(client, &format!("{base}/schema"), &[])
        .ok()
        .and_then(|schema| nest_name_from_schema(&schema))
        .or_else(|| {
            fetch_json::<RootDocument>(client, &format!("{base}/"), &[])
                .ok()
                .map(|root| root.name)
                .filter(|name| !name.is_empty() && name != "nuthatch")
        })
}

fn fetch_selection(client: &Client, base: &str, table: &str) -> Result<Selection, String> {
    let url = format!("{base}/sql");
    let quoted = quote_identifier(table);
    let counts: SqlResponse = fetch_json(
        client,
        &url,
        &[(
            "q",
            &format!("SELECT count(*) AS rows, max(block_number) AS latest_block FROM {quoted}"),
        )],
    )?;
    let events: SqlResponse = fetch_json(
        client,
        &url,
        &[(
            "q",
            &format!("SELECT * FROM {quoted} ORDER BY block_number DESC, log_index DESC LIMIT 6"),
        )],
    )?;
    let row = counts.rows.first().and_then(Value::as_object);
    Ok(Selection {
        table: table.to_owned(),
        rows: row.and_then(|row| row.get("rows")).and_then(Value::as_u64),
        latest_block: row
            .and_then(|row| row.get("latest_block"))
            .and_then(Value::as_u64),
        events: events.rows,
        degraded: counts.degraded || events.degraded,
    })
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

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

fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// Lifetime counters in the performance panel, which is laid out to the width of its longest line.
/// Past a million the exact figure has stopped being the point and starts costing the line its tail.
fn format_counter(value: u64) -> String {
    match value {
        value if value < 1_000_000 => group_digits(value),
        value if value < 1_000_000_000 => format!("{:.1}M", value as f64 / 1e6),
        value => format!("{:.1}B", value as f64 / 1e9),
    }
}

fn format_optional_count(value: Option<u64>) -> String {
    value.map_or_else(|| "unavailable".into(), format_counter)
}

fn format_span(span: Duration) -> String {
    match span.as_secs() {
        secs if secs < 120 => format!("{secs}s"),
        secs if secs < 7200 => format!("{}m", secs / 60),
        secs => format!("{}h", secs / 3600),
    }
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
        format!("{} {unit}", group_digits(value.round() as u64))
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
        .map_or("?".into(), group_digits);
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
    format!("#{block:<11} {details}")
}

fn panel<'a>(title: &str) -> Block<'a> {
    Block::default()
        .title(Line::from(format!(" {title} ")).style(Style::default().fg(Color::Cyan).bold()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))
        .padding(Padding::horizontal(1))
}

/// NO_COLOR (no-color.org): drop every colour after drawing, and turn each filled background into
/// reverse video so the selection, the key badges and the gauge stay visible without one.
fn strip_colour(buffer: &mut Buffer) {
    for cell in &mut buffer.content {
        if cell.bg != Color::Reset && cell.bg != CANVAS {
            cell.modifier.insert(Modifier::REVERSED);
        }
        cell.fg = Color::Reset;
        cell.bg = Color::Reset;
    }
}

fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(CANVAS)), area);
    let vertical = Layout::vertical([
        Constraint::Length(3),
        Constraint::Length(7),
        Constraint::Min(10),
        Constraint::Length(3),
    ])
    .split(area);

    let fallback = Ready::default();
    let ready = app.ready.as_ref().unwrap_or(&fallback);
    let metrics = app.metrics.as_ref();
    let identity = app.identity.as_ref();
    let backfill = app.backfill();
    let state = app.state();
    // The marker leads: a long URL at 100 columns used to push it off the right-hand edge.
    let mut header = vec![
        Span::styled(
            " NUTHATCH ",
            Style::default().fg(Color::Black).bg(Color::Cyan).bold(),
        ),
        Span::styled(
            format!("  {}", state.0),
            Style::default().fg(state.1).bold(),
        ),
        Span::styled(
            format!(
                "   {}",
                identity
                    .and_then(|identity| identity.nest_name.as_deref())
                    .unwrap_or("discovering nest…")
            ),
            Style::default().fg(Color::Cyan).bold(),
        ),
    ];
    if let Some(chain) = identity.and_then(|identity| identity.chain.as_deref()) {
        header.push(Span::styled(
            format!("  {chain}"),
            Style::default().fg(Color::Gray),
        ));
    }
    if let Some(version) = ready.version.as_deref() {
        header.push(Span::styled(
            format!("  v{version}"),
            Style::default().fg(Color::Gray),
        ));
    }
    header.push(Span::styled(
        format!("   {}", app.url),
        Style::default().fg(Color::DarkGray),
    ));
    let title = Paragraph::new(Line::from(header)).block(
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
    let tip = ready
        .tip
        .map_or_else(|| "— (cursorless)".into(), group_digits);
    let health = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("STATUS  ", Style::default().fg(Color::Gray)),
            Span::styled(
                if ready.quarantined {
                    "QUARANTINED"
                } else if backfill.is_some() {
                    "BACKFILL"
                } else if ready.ready {
                    "READY"
                } else {
                    "WAITING"
                },
                Style::default().fg(state.1).bold(),
            ),
        ]),
        Line::from(format!("Tip             {tip}")),
        Line::from(format!(
            "Indexed         {}",
            group_digits(ready.last_block)
        )),
        Line::from(format!(
            "Sealed          {}",
            group_digits(ready.sealed_through)
        )),
        Line::from(match &backfill {
            Some(backfill) => format!("Origin          {}", group_digits(backfill.origin)),
            None if ready.sealed_through == 0 => "Seal gap        nothing sealed".into(),
            None => format!(
                "Seal gap        {}",
                group_digits(ready.last_block.saturating_sub(ready.sealed_through))
            ),
        }),
    ])
    .block(panel("NEST HEALTH"));
    frame.render_widget(health, top[0]);

    let lag_ratio = match (&backfill, ready.tip, ready.lag_blocks) {
        (Some(backfill), _, _) => {
            let span = backfill.target.saturating_sub(backfill.origin).max(1);
            backfill.current.saturating_sub(backfill.origin).min(span) as f64 / span as f64
        }
        (None, Some(tip), Some(lag)) if tip > 0 => (1.0 - lag as f64 / tip as f64).clamp(0.0, 1.0),
        _ => 0.0,
    };
    let lifetime = |name| metrics.and_then(|metrics| metric_opt_u64(metrics, name));
    let restarts = match app.last_restart {
        None => "none seen".into(),
        Some(at) => format!("{}, {} ago", app.restarts, format_span(at.elapsed())),
    };
    let recent_restart = app
        .last_restart
        .is_some_and(|at| at.elapsed() < RECENT_RESTART);
    let data = Paragraph::new(vec![
        Line::from(format!(
            "Tables          {}",
            identity.map_or_else(
                || "—".into(),
                |identity| group_digits(identity.tables.count as u64)
            )
        )),
        Line::from(format!(
            "Decoded rows    {}",
            lifetime("nuthatch_rows_decoded_total")
                .map_or_else(|| "unavailable".into(), group_digits)
        )),
        Line::from(format!(
            "Sealed rows     {}",
            lifetime("nuthatch_rows_sealed_total")
                .map_or_else(|| "unavailable".into(), group_digits)
        )),
        Line::from(format!(
            "Lag             {}",
            ready
                .lag_blocks
                .map_or_else(|| "—".into(), |lag| format!("{} blocks", group_digits(lag)))
        )),
        Line::from(Span::styled(
            format!("Restarts        {restarts}"),
            Style::default().fg(if recent_restart {
                Color::Red
            } else {
                Color::Reset
            }),
        )),
    ])
    .block(panel("DATA COLLECTED"));
    frame.render_widget(data, top[1]);
    frame.render_widget(
        Gauge::default()
            .block(panel("SYNC POSITION"))
            .gauge_style(Style::default().fg(Color::Magenta))
            .ratio(lag_ratio)
            .label(match (&backfill, ready.tip) {
                (Some(backfill), _) => format!(
                    "{} / {}",
                    group_digits(backfill.current),
                    group_digits(backfill.target)
                ),
                (None, Some(tip)) => {
                    format!("{} / {}", group_digits(ready.last_block), group_digits(tip))
                }
                (None, None) => group_digits(ready.last_block),
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
    let tables = identity
        .map(|identity| identity.tables.tables.as_slice())
        .unwrap_or_default();
    let rows: Vec<ListItem> = tables
        .iter()
        .map(|table| ListItem::new(table.table.as_str()).style(Style::default().fg(Color::White)))
        .collect();
    let mut list_state = ListState::default()
        .with_offset(app.table_offset.get())
        .with_selected((!tables.is_empty()).then_some(app.selected_table));
    frame.render_stateful_widget(
        List::new(rows)
            .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
            .block(panel(&if tables.is_empty() {
                "INDEXED TABLES".to_owned()
            } else {
                format!(
                    "INDEXED TABLES  {}/{}  ↑↓ j k",
                    app.selected_table + 1,
                    tables.len()
                )
            })),
        left[0],
        &mut list_state,
    );
    app.table_offset.set(list_state.offset());

    let rpc_per_second = app.rate(|sample| sample.rpc_requests);
    let method_per_second = app.rate(|sample| sample.rpc_methods);
    let decode_per_second = app.rate(|sample| sample.decoded_rows);
    let blocks_per_second = app.rate(|sample| sample.indexed_block);
    let poll = match app.nest_poll_interval() {
        Some(interval) => format!(
            "poll {} ago, every {}",
            format_span(Duration::from_secs(ready.seconds_since_poll)),
            format_span(interval)
        ),
        None => format!(
            "poll {} ago",
            format_span(Duration::from_secs(ready.seconds_since_poll))
        ),
    };
    let rpc_line = match lifetime("nuthatch_rpc_requests_total") {
        Some(rpc) => Line::from(vec![
            Span::styled("RPC REQUESTS  ", Style::default().fg(Color::Gray)),
            Span::styled(
                format_counter(rpc),
                Style::default().fg(Color::Yellow).bold(),
            ),
            Span::styled(
                format!(
                    " since start  {}  {}",
                    format_rate(rpc_per_second, "req/s"),
                    format_rate(rpc_per_second * 60.0, "req/min")
                ),
                Style::default().fg(Color::DarkGray),
            ),
        ]),
        None => Line::from("RPC REQUESTS  unavailable"),
    };
    let activity = Paragraph::new(vec![
        rpc_line,
        Line::from(match lifetime("nuthatch_rpc_methods_total") {
            Some(methods) => format!(
                "RPC METHODS     {} since start  {}",
                format_counter(methods),
                format_rate(method_per_second, "calls/s")
            ),
            None => "RPC METHODS     unavailable".into(),
        }),
        Line::from(match lifetime("nuthatch_rows_decoded_total") {
            Some(decoded) => format!(
                "DECODED ROWS    {} since start  {}",
                format_counter(decoded),
                format_rate(decode_per_second, "rows/s"),
            ),
            None => "DECODED ROWS    unavailable".into(),
        }),
        Line::from(format!(
            "INDEXED BLOCKS  {}  {}",
            format_rate(blocks_per_second, "blocks/s"),
            format_rate(blocks_per_second * 60.0, "blocks/min")
        )),
        Line::from(format!(
            "MEMORY RSS      {}",
            format_optional_bytes(lifetime("nuthatch_rss_bytes"))
        )),
        Line::from(format!(
            "API REFRESH     {} ms  {poll}",
            app.refresh_time.map_or(0, |time| time.as_millis()),
        )),
        Line::from(format!(
            "REORGS  {}   CPU  {}",
            lifetime("nuthatch_reorgs_total").map_or_else(
                || "unavailable".into(),
                |reorgs| format!("{} since start", format_counter(reorgs))
            ),
            format_cpu_percent(app.cpu_percent())
        )),
        Line::from(format!(
            "DISK            hot {}  sealed {}",
            format_optional_bytes(lifetime("nuthatch_hot_store_bytes")),
            format_optional_bytes(lifetime("nuthatch_sealed_segments_bytes")),
        )),
        Line::from(format!(
            "RPC HEALTH      fail {}  retry {}  latency {}",
            format_optional_count(lifetime("nuthatch_rpc_endpoint_failures_total")),
            format_optional_count(lifetime("nuthatch_rpc_endpoint_retries_total")),
            format_rpc_latency(metrics.and_then(rpc_latency_ms)),
        )),
    ])
    .block(panel(&format!(
        "PERFORMANCE  rates {}  (w)",
        app.rate_window_label()
    )));
    frame.render_widget(activity, right[0]);

    let selection = app
        .selection
        .as_ref()
        .filter(|selection| Some(selection.table.as_str()) == app.selected_table_name());
    let heading = Line::from(Span::styled(
        app.selected_table_name().unwrap_or("no event tables"),
        Style::default().fg(Color::Cyan).bold(),
    ));
    let summary_lines = match identity.map(|identity| &identity.sql) {
        Some(SqlAccess::Closed { mode, .. }) => vec![
            heading,
            Line::from(Span::styled(
                format!("SQL is closed on this nest ({mode})"),
                Style::default().fg(Color::Yellow),
            )),
            Line::from("Row counts need free-form SQL"),
        ],
        _ => vec![
            heading,
            Line::from(format!(
                "Rows    {}",
                selection
                    .and_then(|selection| selection.rows)
                    .map_or("—".into(), group_digits)
            )),
            Line::from(format!(
                "Latest  {}",
                selection
                    .and_then(|selection| selection.latest_block)
                    .map_or("—".into(), group_digits)
            )),
            match selection.map(|selection| selection.degraded) {
                Some(true) => Line::from(Span::styled(
                    "Warning: a sealed segment is degraded",
                    Style::default().fg(Color::Yellow),
                )),
                Some(false) => Line::from(Span::styled(
                    "Storage integrity: healthy",
                    Style::default().fg(Color::Green),
                )),
                None => Line::from("Storage integrity: unknown"),
            },
        ],
    };
    let summary = Paragraph::new(summary_lines)
        .block(panel("SELECTED TABLE"))
        .wrap(Wrap { trim: true });
    frame.render_widget(summary, left[1]);

    let feed_rows: Vec<ListItem> = match identity.map(|identity| &identity.sql) {
        Some(SqlAccess::Closed { named, .. }) if named.is_empty() => {
            vec![ListItem::new("This nest serves no named queries.")]
        }
        Some(SqlAccess::Closed { named, .. }) => {
            std::iter::once(ListItem::new("Named queries, served at /q/{name}:"))
                .chain(named.iter().map(|name| ListItem::new(format!("  {name}"))))
                .collect()
        }
        _ => selection
            .map(|selection| selection.events.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|row| ListItem::new(Line::from(event_line(row))))
            .collect(),
    };
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
            draw_activity(frame, app, feed_area[1]);
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
        Span::styled(
            app.status(),
            Style::default().fg(if app.problems.is_empty() {
                Color::DarkGray
            } else {
                Color::Yellow
            }),
        ),
    ]));
    frame.render_widget(footer, vertical[3]);

    if app.no_color {
        strip_colour(frame.buffer_mut());
    }
}

/// Two sparklines over the same buckets. Each autoscales to its own peak, so the title carries
/// that peak: an unlabelled bar says something happened, not how much.
fn draw_activity(frame: &mut Frame, app: &App, area: Rect) {
    let halves =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).split(area);
    let span = format_span(app.activity.width);
    let buckets = &app.activity.buckets;
    let rpc: Vec<u64> = buckets.iter().map(|bucket| bucket.rpc_requests).collect();
    let refresh: Vec<u64> = buckets
        .iter()
        .map(|bucket| bucket.peak_refresh_ms)
        .collect();
    let rpc_title = if app.metrics.is_some() {
        format!(
            "RPC / {span}  peak {}",
            group_digits(rpc.iter().copied().max().unwrap_or_default())
        )
    } else {
        "RPC  unavailable".into()
    };
    frame.render_widget(
        Sparkline::default()
            .block(panel(&rpc_title))
            .data(&rpc)
            .style(Style::default().fg(Color::Yellow)),
        halves[0],
    );
    frame.render_widget(
        Sparkline::default()
            .block(panel(&format!(
                "API REFRESH / {span}  peak {} ms",
                group_digits(refresh.iter().copied().max().unwrap_or_default())
            )))
            .data(&refresh)
            .style(Style::default().fg(Color::Cyan)),
        halves[1],
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use std::{
        io::{BufRead, BufReader, Write},
        net::TcpListener,
        sync::{Mutex, atomic::AtomicUsize},
    };

    fn render(app: &App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("test terminal");
        terminal.draw(|frame| draw(frame, app)).expect("draw");
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

    /// A dashboard populated the way a healthy mainnet nest populates it, for the layout tests.
    fn populated() -> App {
        let mut app = App::new("http://127.0.0.1:8288".into());
        app.identity = Some(Identity {
            nest_name: Some("graph-staking-nest".into()),
            chain: Some("mainnet".into()),
            tables: Tables {
                count: 2,
                tables: vec![
                    EventTable {
                        table: "usdc__approval".into(),
                    },
                    EventTable {
                        table: "usdc__transfer".into(),
                    },
                ],
            },
            sql: SqlAccess::Open,
        });
        app.ready = Some(Ready {
            ready: true,
            lag_blocks: Some(0),
            last_block: 25_766_811,
            sealed_through: 25_766_747,
            tip: Some(25_766_811),
            seconds_since_poll: 1,
            freshness: Some(Freshness {
                poll_interval_secs: Some(2),
            }),
            version: Some("3.10.0".into()),
            ..Ready::default()
        });
        app.metrics = Some(parse_prometheus(
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
        ));
        app.selection = Some(Selection {
            table: "usdc__approval".into(),
            rows: Some(2275),
            latest_block: Some(25_766_811),
            events: Vec::new(),
            degraded: false,
        });
        app.refresh_time = Some(Duration::from_millis(12));
        app.last_refresh = Some(Instant::now());
        // Two samples a full window apart, so the rolling rates render at a realistic width
        // rather than the flattering "0 req/s" a single sample would give.
        let now = Instant::now();
        let earlier = now
            .checked_sub(Duration::from_secs(60))
            .expect("a host that has been up for a minute");
        app.samples = vec![
            Sample {
                at: earlier,
                decoded_rows: Some(1000),
                rpc_requests: Some(300),
                rpc_methods: Some(340),
                indexed_block: Some(25_766_741),
                cpu_seconds: Some(2.4),
            },
            Sample {
                at: now,
                decoded_rows: Some(2275),
                rpc_requests: Some(367),
                rpc_methods: Some(412),
                indexed_block: Some(25_766_811),
                cpu_seconds: Some(4.5),
            },
        ];
        app
    }

    fn rendered(width: u16, height: u16) -> String {
        render(&populated(), width, height)
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
            "DECODED ROWS    2,275 since start  21 rows/s",
            "INDEXED BLOCKS  1.2 blocks/s  70 blocks/min",
            "MEMORY RSS      61.0 MiB",
            "API REFRESH     12 ms  poll 1s ago, every 2s",
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

    /// The widest lines, at the counter sizes a long-running arbitrum nest actually reaches.
    #[test]
    fn large_counters_still_fit_at_one_hundred_columns() {
        let mut app = populated();
        app.metrics = Some(parse_prometheus(
            "nuthatch_rows_decoded_total 912345678\n\
             nuthatch_rpc_requests_total 999999\n\
             nuthatch_rpc_methods_total 45678901\n",
        ));
        let screen = render(&app, 100, 30);
        for expected in [
            "RPC REQUESTS  999,999 since start",
            "DECODED ROWS    912.3M since start",
            "RPC METHODS     45.7M since start",
        ] {
            assert!(screen.contains(expected), "{expected:?} missing:\n{screen}");
        }
        assert!(screen.contains("req/min"), "req/min cropped:\n{screen}");
    }

    #[test]
    fn health_panel_groups_heights_and_names_the_seal_gap() {
        let screen = rendered(100, 30);
        for expected in [
            "Tip             25,766,811",
            "Sealed          25,766,747",
            "Seal gap        64",
            "Restarts        none seen",
        ] {
            assert!(screen.contains(expected), "{expected:?} missing:\n{screen}");
        }
    }

    /// The selected-table summary and the event feed have to survive the same squeeze.
    #[test]
    fn table_summary_and_feed_survive_at_one_hundred_columns() {
        let screen = rendered(100, 30);
        for expected in [
            "SELECTED TABLE",
            "usdc__approval",
            "Rows    2,275",
            "Latest  25,766,811",
            "Storage integrity: healthy",
            "LIVE EVENT FEED",
        ] {
            assert!(
                screen.contains(expected),
                "{expected:?} missing at 100x30:\n{screen}"
            );
        }
    }

    #[test]
    fn the_marker_survives_a_long_url_at_one_hundred_columns() {
        let mut app = populated();
        app.url = "http://allocations-nest.internal.example.com:18288/some/prefix".into();
        let screen = render(&app, 100, 30);
        assert!(
            screen.lines().next().unwrap().contains("● LIVE"),
            "{screen}"
        );
    }

    /// The sparkline is the last thing to arrive, and the README quotes the height at which it
    /// does. Asserting the boundary keeps that sentence honest.
    #[test]
    fn the_sparkline_arrives_at_thirty_three_rows() {
        assert!(!rendered(100, 32).contains("API REFRESH /"));
        assert!(rendered(100, 33).contains("API REFRESH /"));
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
    fn no_color_leaves_no_colour_and_keeps_the_selection_visible() {
        let mut app = populated();
        app.no_color = true;
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).expect("test terminal");
        terminal.draw(|frame| draw(frame, &app)).expect("draw");
        let buffer = terminal.backend().buffer();
        assert!(
            buffer
                .content
                .iter()
                .all(|cell| cell.fg == Color::Reset && cell.bg == Color::Reset)
        );
        let reversed = |text: &str| {
            (0..buffer.area.height).any(|y| {
                let row: String = (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect();
                row.find(text).is_some_and(|start| {
                    let x = row[..start].chars().count() as u16;
                    buffer[(x, y)].modifier.contains(Modifier::REVERSED)
                })
            })
        };
        assert!(
            reversed("usdc__approval"),
            "the selected table lost its highlight"
        );
        assert!(!reversed("usdc__transfer"));
    }

    fn with_many_tables(count: usize) -> App {
        let mut app = populated();
        let identity = app.identity.as_mut().unwrap();
        identity.tables = Tables {
            count,
            tables: (0..count)
                .map(|index| EventTable {
                    table: format!("graph__table_{index:03}"),
                })
                .collect(),
        };
        app
    }

    /// On an 81-table nest the selection used to walk off the bottom of an unscrolled list.
    #[test]
    fn the_table_list_scrolls_to_the_selection_and_holds_its_place() {
        let mut app = with_many_tables(81);
        app.selected_table = 70;
        let screen = render(&app, 100, 30);
        assert!(screen.contains("graph__table_070"), "{screen}");
        assert!(!screen.contains("graph__table_000"), "{screen}");
        assert!(screen.contains("INDEXED TABLES  71/81"), "{screen}");
        let offset = app.table_offset.get();
        app.selected_table = 68;
        render(&app, 100, 30);
        assert_eq!(
            app.table_offset.get(),
            offset,
            "moving up inside the view should not scroll it"
        );
    }

    #[test]
    fn paging_and_the_ends_clamp_to_the_list() {
        let mut app = with_many_tables(81);
        assert_eq!(app.select(usize::MAX).as_deref(), Some("graph__table_080"));
        assert_eq!(app.select(app.selected_table + TABLE_PAGE), None);
        assert_eq!(app.select(0).as_deref(), Some("graph__table_000"));
        assert_eq!(app.select_previous().as_deref(), Some("graph__table_080"));
        assert_eq!(app.select_next().as_deref(), Some("graph__table_000"));
        app.identity = None;
        assert_eq!(app.select_next(), None);
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
    fn arguments_take_a_url_and_an_interval_in_either_order() {
        let args = |list: &[&str]| parse_args(list.iter().map(|arg| arg.to_string()));
        let parsed = args(&["--interval", "2m", "--url", "http://h:1/"]).unwrap();
        assert_eq!(parsed.url, "http://h:1");
        assert_eq!(parsed.interval, Some(Duration::from_secs(120)));
        assert_eq!(
            args(&["--interval", "5"]).unwrap().interval,
            Some(Duration::from_secs(5))
        );
        assert!(args(&["--interval", "0s"]).is_err());
        assert!(args(&["--interval", "soon"]).is_err());
        assert!(args(&["--bogus"]).is_err());
    }

    #[test]
    fn identifiers_are_quoted_for_sql() {
        assert_eq!(quote_identifier("usdc__transfer"), "\"usdc__transfer\"");
        assert_eq!(quote_identifier("odd \"name\""), "\"odd \"\"name\"\"\"");
    }

    #[test]
    fn digits_are_grouped_and_large_counters_compacted() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(999), "999");
        assert_eq!(group_digits(1000), "1,000");
        assert_eq!(group_digits(502_325_155), "502,325,155");
        assert_eq!(format_counter(999_999), "999,999");
        assert_eq!(format_counter(12_345_678), "12.3M");
        assert_eq!(format_counter(4_200_000_000), "4.2B");
        assert_eq!(format_rate(1275.4, "rows/min"), "1,275 rows/min");
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

    #[test]
    fn a_cursorless_role_decodes_with_null_tip_and_lag() {
        let ready: Ready = serde_json::from_str(
            r#"{"ready":true,"tip":null,"lag_blocks":null,"cursorless":true,"last_block":0}"#,
        )
        .unwrap();
        assert_eq!((ready.tip, ready.lag_blocks), (None, None));
        let mut app = populated();
        app.ready = Some(ready);
        assert!(render(&app, 100, 30).contains("Tip             — (cursorless)"));
    }

    #[test]
    fn the_poll_interval_follows_the_nest_within_bounds() {
        let mut app = populated();
        let with_nest_interval = |app: &mut App, secs| {
            app.ready.as_mut().unwrap().freshness = Some(Freshness {
                poll_interval_secs: Some(secs),
            });
        };
        with_nest_interval(&mut app, 300);
        assert_eq!(app.poll_interval(), MAX_POLL_INTERVAL);
        assert_eq!(app.activity_width(), Duration::from_secs(300));
        with_nest_interval(&mut app, 1);
        assert_eq!(app.poll_interval(), MIN_POLL_INTERVAL);
        with_nest_interval(&mut app, 12);
        assert_eq!(app.poll_interval(), Duration::from_secs(12));
        app.interval_override = Some(Duration::from_secs(5));
        assert_eq!(app.poll_interval(), Duration::from_secs(5));
        app.ready.as_mut().unwrap().freshness = None;
        app.interval_override = None;
        assert_eq!(app.poll_interval(), DEFAULT_POLL_INTERVAL);
    }

    #[test]
    fn activity_buckets_are_fixed_width_and_reset_when_the_width_changes() {
        let mut activity = Activity::default();
        let start = Instant::now();
        let width = Duration::from_secs(300);
        for (offset, rpc) in [(0, 5), (30, 1), (60, 2), (299, 1), (300, 7), (330, 1)] {
            activity.record(start + Duration::from_secs(offset), width, rpc, offset);
        }
        let buckets: Vec<(u64, u64)> = activity
            .buckets
            .iter()
            .map(|bucket| (bucket.rpc_requests, bucket.peak_refresh_ms))
            .collect();
        assert_eq!(buckets, [(9, 299), (8, 330)]);
        activity.record(
            start + Duration::from_secs(340),
            Duration::from_secs(2),
            3,
            1,
        );
        assert_eq!(activity.buckets.len(), 1);
    }

    /// Prints the dashboard as drawn against a real nest, which is how the README's sample screen
    /// is made: `NUTHATCH_URL=http://127.0.0.1:18288 cargo test live -- --ignored --nocapture`.
    #[test]
    #[ignore = "needs a running nest at NUTHATCH_URL"]
    fn live() {
        let url = std::env::var("NUTHATCH_URL").expect("NUTHATCH_URL");
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut app = App::new(normalize_url(url));
        for _ in 0..6 {
            app.refresh(&client);
            std::thread::sleep(app.poll_interval());
        }
        println!("{}", render(&app, 100, 34));
    }

    /// Just enough of an HTTP server to answer the client's GETs from canned bodies, recording the
    /// path of every request so a test can count what the client actually asked for.
    struct TestNest {
        base: String,
        hits: Arc<Mutex<Vec<String>>>,
    }

    /// The worker's two halves, run in line so a test can drive the app without a thread.
    impl App {
        fn refresh(&mut self, client: &Client) {
            let request = self.poll_request();
            self.poll_in_flight = true;
            self.apply(poll(client, &self.url, &request));
        }

        fn query(&mut self, client: &Client, table: Option<String>) {
            if let Some(table) = table {
                self.apply_selection(fetch_selection(client, &self.url, &table));
            }
        }
    }

    impl TestNest {
        fn serve(routes: impl Fn(&str) -> (u16, String) + Send + 'static) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            let base = format!("http://{}", listener.local_addr().expect("address"));
            let hits = Arc::new(Mutex::new(Vec::new()));
            let log = Arc::clone(&hits);
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(mut stream) = stream else { continue };
                    let mut reader = BufReader::new(stream.try_clone().expect("clone"));
                    let mut request = String::new();
                    let _ = reader.read_line(&mut request);
                    loop {
                        let mut header = String::new();
                        if reader.read_line(&mut header).unwrap_or(0) <= 2 {
                            break;
                        }
                    }
                    let target = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    log.lock()
                        .unwrap()
                        .push(target.split('?').next().unwrap_or("/").to_owned());
                    let (status, body) = routes(&target);
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                }
            });
            Self { base, hits }
        }

        fn hits(&self, path: &str) -> usize {
            self.hits
                .lock()
                .unwrap()
                .iter()
                .filter(|hit| *hit == path)
                .count()
        }

        fn app(&self) -> (App, Client) {
            (App::new(self.base.clone()), Client::new())
        }
    }

    /// Trimmed from a live `nuthatch dev` 3.10.0 USDC nest on mainnet, 2026-09-24.
    const READY: &str = r#"{"cursorless":false,"entities_stalled":false,"freshness":{"mode":"tip","poll_interval_secs":2},"initial_poll_failed":false,"lag_blocks":0,"last_block":26048483,"ready":true,"seal_direct_active":false,"seal_direct_completed":0,"seal_direct_origin":0,"seal_direct_stalled":false,"seal_direct_target":0,"seal_lag_blocks":null,"sealed_through":0,"seconds_since_poll":2,"stalled":false,"tip":26048483,"version":"3.10.0","wedged":false}"#;
    const METRICS: &str = "nuthatch_rows_decoded_total 10511\nnuthatch_rpc_requests_total 57\n";
    const TABLES: &str =
        r#"{"count":2,"tables":[{"table":"usdc__approval"},{"table":"usdc__transfer"}]}"#;
    const NEST: &str = r#"{"chain":"mainnet","chain_id":1,"name":"demo-usdc","table_count":2}"#;
    const QUERIES_OPEN: &str = r#"{"free_form":true,"queries":[],"sql":"open"}"#;
    const COUNTS: &str = r#"{"rows":[{"rows":2275,"latest_block":26048483}],"degraded":false}"#;
    const EVENTS: &str = r#"{"rows":[],"degraded":false}"#;

    fn healthy(target: &str) -> (u16, String) {
        let path = target.split('?').next().unwrap_or(target);
        let body = match path {
            "/ready" => READY,
            "/metrics" => METRICS,
            "/tables" => TABLES,
            "/nest" => NEST,
            "/queries" => QUERIES_OPEN,
            "/sql" if target.contains("count") => COUNTS,
            "/sql" => EVENTS,
            _ => return (404, "not found".into()),
        };
        (200, body.into())
    }

    #[test]
    fn the_catalogue_is_fetched_once_and_the_state_is_live() {
        let nest = TestNest::serve(healthy);
        let (mut app, client) = nest.app();
        for _ in 0..3 {
            app.refresh(&client);
        }
        assert_eq!(nest.hits("/tables"), 1);
        assert_eq!(nest.hits("/nest"), 1);
        assert_eq!(nest.hits("/queries"), 1);
        assert_eq!(
            nest.hits("/schema"),
            0,
            "/nest answered, so /schema is not needed"
        );
        assert_eq!(nest.hits("/ready"), 3);
        assert_eq!(app.state().0, "● LIVE");
        assert_eq!(app.status(), "Live data received");
        let identity = app.identity.as_ref().unwrap();
        assert_eq!(identity.nest_name.as_deref(), Some("demo-usdc"));
        assert_eq!(identity.chain.as_deref(), Some("mainnet"));
        assert_eq!(app.selection.as_ref().unwrap().rows, Some(2275));
    }

    #[test]
    fn moving_the_selection_reruns_only_the_sql() {
        let nest = TestNest::serve(healthy);
        let (mut app, client) = nest.app();
        app.refresh(&client);
        let table = app.select_next();
        app.query(&client, table);
        assert_eq!(nest.hits("/ready"), 1);
        assert_eq!(nest.hits("/sql"), 4);
        assert_eq!(app.selection.as_ref().unwrap().table, "usdc__transfer");
    }

    #[test]
    fn the_table_name_reaches_the_nest_quoted() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&queries);
        let nest = TestNest::serve(move |target| {
            if target.starts_with("/sql") {
                log.lock().unwrap().push(target.to_owned());
            }
            healthy(target)
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        let queries = queries.lock().unwrap();
        assert!(
            queries
                .iter()
                .all(|q| q.contains("FROM+%22usdc__approval%22")),
            "{queries:?}"
        );
    }

    /// A stalled nest answers 503. It used to be read as a transport failure, which discarded the
    /// snapshot and made `ATTENTION` unreachable.
    #[test]
    fn a_stalled_nest_is_read_not_discarded() {
        let nest = TestNest::serve(|target| match target {
            "/ready" => (
                503,
                READY
                    .replace(r#""ready":true"#, r#""ready":false"#)
                    .replace(r#""stalled":false"#, r#""stalled":true"#),
            ),
            _ => healthy(target),
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        assert!(app.ready.as_ref().unwrap().stalled);
        assert_eq!(app.state().0, "● ATTENTION");
        assert!(app.problems.is_empty(), "{:?}", app.problems);
    }

    #[test]
    fn a_missing_metrics_endpoint_is_partial_and_named_once() {
        let nest = TestNest::serve(|target| match target {
            "/metrics" => (404, "not found".into()),
            _ => healthy(target),
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        assert_eq!(app.state().0, "● LIVE (partial)");
        assert_eq!(app.status(), "/metrics: HTTP 404 Not Found");
        let screen = render(&app, 100, 30);
        assert!(screen.contains("RPC REQUESTS  unavailable"), "{screen}");
        assert!(screen.contains("Decoded rows    unavailable"), "{screen}");
    }

    #[test]
    fn a_nest_that_stops_answering_is_stale_not_live() {
        let down = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&down);
        let nest = TestNest::serve(move |target| {
            if flag.load(Ordering::Relaxed) && target == "/ready" {
                (502, "bad gateway".into())
            } else {
                healthy(target)
            }
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        down.store(true, Ordering::Relaxed);
        app.refresh(&client);
        assert_eq!(app.state().0, "● STALE");
        assert_eq!(app.ready.as_ref().unwrap().last_block, 26_048_483);
    }

    #[test]
    fn schema_is_the_fallback_when_nest_is_not_served() {
        let nest = TestNest::serve(|target| match target {
            "/nest" => (404, "not found".into()),
            "/schema" => (
                200,
                "The `graph-allocations-nest` nest on arbitrum-one.\n".into(),
            ),
            _ => healthy(target),
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        assert_eq!(
            app.identity.as_ref().unwrap().nest_name.as_deref(),
            Some("graph-allocations-nest")
        );
    }

    #[test]
    fn closed_sql_is_not_asked_and_says_why() {
        let nest = TestNest::serve(|target| {
            match target {
            "/queries" => (
                200,
                r#"{"free_form":false,"queries":[{"name":"top_holders","params":[],"path":"/q/top_holders"}],"sql":"allowlist"}"#.into(),
            ),
            _ => healthy(target),
        }
        });
        let (mut app, client) = nest.app();
        app.refresh(&client);
        assert_eq!(app.select_next(), None);
        assert_eq!(nest.hits("/sql"), 0);
        assert!(app.problems.is_empty(), "{:?}", app.problems);
        let screen = render(&app, 100, 33);
        assert!(screen.contains("SQL is closed on this nest"), "{screen}");
        assert!(screen.contains("top_holders"), "{screen}");
    }

    /// Holding `j` must not queue a round trip per keypress behind a slow nest.
    #[test]
    fn the_worker_asks_only_for_the_last_of_a_burst_of_selections() {
        let nest = TestNest::serve(|target| {
            if target == "/ready" {
                std::thread::sleep(Duration::from_millis(200));
            }
            healthy(target)
        });
        let (requests, replies) = spawn_worker(Client::new(), nest.base.clone());
        requests
            .send(Request::Poll(PollRequest {
                identity: true,
                table: None,
                sql_open: true,
            }))
            .unwrap();
        for table in ["usdc__transfer", "usdc__approval", "usdc__transfer"] {
            requests.send(Request::Selection(table.into())).unwrap();
        }
        let first = replies.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(first, Reply::Poll(_)));
        let Reply::Selection(Ok(selection)) = replies.recv_timeout(Duration::from_secs(5)).unwrap()
        else {
            panic!("expected a selection reply");
        };
        assert_eq!(selection.table, "usdc__transfer");
        assert!(replies.recv_timeout(Duration::from_millis(300)).is_err());
        assert_eq!(
            nest.hits("/sql"),
            4,
            "two for the poll, two for the last selection"
        );
    }

    #[test]
    fn a_refetched_catalogue_keeps_the_selected_table() {
        let nest = TestNest::serve(healthy);
        let (mut app, client) = nest.app();
        app.refresh(&client);
        let table = app.select_next();
        app.query(&client, table);
        app.refetch_identity = true;
        app.refresh(&client);
        assert_eq!(nest.hits("/tables"), 2);
        assert_eq!(app.selected_table_name(), Some("usdc__transfer"));
        assert_eq!(app.selection.as_ref().unwrap().table, "usdc__transfer");
    }

    #[test]
    fn a_restart_is_counted_and_refetches_the_catalogue() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&polls);
        let nest = TestNest::serve(move |target| match target {
            "/metrics" => {
                let rpc = [500, 510, 3, 9][counter.fetch_add(1, Ordering::Relaxed).min(3)];
                (200, format!("nuthatch_rpc_requests_total {rpc}\n"))
            }
            _ => healthy(target),
        });
        let (mut app, client) = nest.app();
        for _ in 0..4 {
            app.refresh(&client);
        }
        assert_eq!(app.restarts, 1);
        assert!(app.last_restart.is_some());
        assert_eq!(nest.hits("/tables"), 2);
        assert_eq!(
            app.samples.len(),
            2,
            "history before the restart is discarded"
        );
        assert!(render(&app, 100, 30).contains("Restarts        1, 0s ago"));
    }
}
