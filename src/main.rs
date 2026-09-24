use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    io::{self, Read},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
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
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
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
const DEFAULT_FEED_ROWS: usize = 6;
const MAX_FEED_ROWS: usize = 50;
const ACTIVITY_LEN: usize = 64;
/// How long an observed restart keeps the restart line lit.
const RECENT_RESTART: Duration = Duration::from_secs(600);
/// Labelled metric lines in the performance panel. The panel is laid out at exactly this height so
/// that none of them is silently cropped; raise it with the panel.
const PERFORMANCE_LINES: u16 = 10;
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
    #[serde(default)]
    columns: Vec<Column>,
}

#[derive(Debug, Deserialize, Clone)]
struct Column {
    name: String,
    #[serde(default)]
    sol_type: String,
}

/// Columns every row carries that say where it came from rather than what happened.
const IMPLICIT_COLUMNS: [&str; 7] = [
    "block_number",
    "block_hash",
    "tx_hash",
    "log_index",
    "address",
    "_seq",
    "block_timestamp",
];

/// The feed and summary queries for one table. The catalogue lists only decoded columns, so
/// selecting them by name also leaves out the `_dec` and `_overflow` companions Nuthatch adds to
/// every big integer for arithmetic; the plain column already holds the exact decimal text.
#[derive(Debug, Clone)]
struct SelectionQuery {
    table: String,
    columns: Vec<String>,
    has_log_index: bool,
    limit: usize,
}

impl SelectionQuery {
    fn new(table: &EventTable, limit: usize) -> Self {
        Self {
            table: table.table.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| column.sol_type != "implicit")
                .map(|column| column.name.clone())
                .collect(),
            has_log_index: table.columns.is_empty()
                || table
                    .columns
                    .iter()
                    .any(|column| column.name == "log_index"),
            limit,
        }
    }

    fn counts_sql(&self) -> String {
        format!(
            "SELECT count(*) AS rows, max(block_number) AS latest_block FROM {}",
            quote_identifier(&self.table)
        )
    }

    fn events_sql(&self) -> String {
        let columns = if self.columns.is_empty() {
            "*".to_owned()
        } else {
            std::iter::once("block_number")
                .chain(self.columns.iter().map(String::as_str))
                .map(quote_identifier)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let order = if self.has_log_index {
            "block_number DESC, log_index DESC"
        } else {
            "block_number DESC"
        };
        format!(
            "SELECT {columns} FROM {} ORDER BY {order} LIMIT {}",
            quote_identifier(&self.table),
            self.limit
        )
    }
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
    columns: Vec<String>,
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
    tip: Option<u64>,
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
    /// The nest this poll is for. A reply for a nest the operator has since left is dropped.
    base: String,
    /// A runtime's root, whose roster is refreshed with every poll so another nest's quarantine shows.
    roster: Option<String>,
    identity: bool,
    selection: Option<SelectionQuery>,
    sql_open: bool,
    feed_limit: usize,
}

struct PollResult {
    base: String,
    /// Set when `base` turned out to be a runtime's root rather than a nest.
    discovered: Option<Roster>,
    roster: Option<Result<Roster, String>>,
    identity: Option<Result<Identity, Problem>>,
    ready: Result<Ready, String>,
    metrics: Result<BTreeMap<String, f64>, String>,
    table: Option<String>,
    selection: Option<Result<Selection, String>>,
    elapsed: Duration,
}

enum Request {
    Poll(PollRequest),
    Selection(String, SelectionQuery),
}

enum Reply {
    Poll(Box<PollResult>),
    Selection(String, Result<Selection, String>),
}

/// `GET /nests` on a runtime: the nests it mounts, each serving its own API under `base_path`.
#[derive(Debug, Deserialize, Clone, Default)]
struct Roster {
    #[serde(default)]
    runtime: String,
    nests: Vec<RosterNest>,
}

#[derive(Debug, Deserialize, Clone)]
struct RosterNest {
    name: String,
    #[serde(default)]
    base_path: String,
    #[serde(default)]
    health: String,
}

impl RosterNest {
    fn path(&self) -> String {
        if self.base_path.is_empty() {
            format!("/{}", self.name)
        } else {
            self.base_path.clone()
        }
    }
}

struct Runtime {
    root: String,
    roster: Roster,
    current: usize,
}

fn poll(client: &Client, request: &PollRequest) -> PollResult {
    let started = Instant::now();
    let base = request.base.as_str();
    let identity = request.identity.then(|| fetch_identity(client, base));
    // A runtime's root has no catalogue of its own, only a roster of the nests it mounts, and a
    // `/ready` with no heights in it that would decode as a ready nest at block zero.
    if let Some(Err(("/tables", _))) = &identity
        && let Ok(roster) = fetch_json::<Roster>(client, &format!("{base}/nests"), &[])
    {
        return PollResult {
            base: base.to_owned(),
            discovered: Some(roster),
            roster: None,
            identity: None,
            ready: Err("a runtime root".into()),
            metrics: Err("a runtime root".into()),
            table: None,
            selection: None,
            elapsed: started.elapsed(),
        };
    }
    let roster = request
        .roster
        .as_ref()
        .map(|root| fetch_json::<Roster>(client, &format!("{root}/nests"), &[]));
    let ready = fetch_ready(client, base);
    let metrics =
        fetch_ok(client, &format!("{base}/metrics"), &[]).map(|text| parse_prometheus(&text));
    let (query, sql_open) = match &identity {
        Some(Ok(identity)) => {
            let tables = &identity.tables.tables;
            let wanted = request.selection.as_ref().map(|query| query.table.as_str());
            let table = tables
                .iter()
                .find(|table| Some(table.table.as_str()) == wanted)
                .or(tables.first());
            (
                table.map(|table| SelectionQuery::new(table, request.feed_limit)),
                identity.sql == SqlAccess::Open,
            )
        }
        _ => (request.selection.clone(), request.sql_open),
    };
    let selection = query
        .as_ref()
        .filter(|_| sql_open)
        .map(|query| fetch_selection(client, base, query));
    let table = query.map(|query| query.table);
    PollResult {
        base: base.to_owned(),
        discovered: None,
        roster,
        identity,
        ready,
        metrics,
        table,
        selection,
        elapsed: started.elapsed(),
    }
}

/// Requests run here so that a slow `/sql` delays the numbers rather than the keyboard.
fn spawn_worker(client: Client) -> (Sender<Request>, Receiver<Reply>) {
    let (requests, inbox) = mpsc::channel();
    let (outbox, replies) = mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(first) = inbox.recv() {
            // Holding `j` queues a selection per keypress; only the last one is worth asking for.
            let (mut next_poll, mut next_selection) = (None, None);
            for request in std::iter::once(first).chain(inbox.try_iter()) {
                match request {
                    Request::Poll(request) => next_poll = Some(request),
                    Request::Selection(base, query) => next_selection = Some((base, query)),
                }
            }
            let replies = next_poll
                .map(|request| Reply::Poll(Box::new(poll(&client, &request))))
                .into_iter()
                .chain(next_selection.map(|(base, query)| {
                    let result = fetch_selection(&client, &base, &query);
                    Reply::Selection(base, result)
                }));
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
    /// What the header names: the nest's own URL, and the host when it is reached through ssh.
    target: String,
    /// Set while the ssh forward is down, and says when it will be reopened.
    tunnel_problem: Option<String>,
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
    /// Rows the feed panel had room for when last drawn, which sizes the next feed query.
    feed_limit: Cell<usize>,
    refresh_time: Option<Duration>,
    last_refresh: Option<Instant>,
    poll_in_flight: bool,
    /// Set when the URL given was a runtime's root; `url` is then the mounted nest being shown.
    runtime: Option<Runtime>,
    /// Narrows the table list to names containing it, case-insensitively.
    filter: String,
    /// Keys are going into the filter rather than driving the dashboard.
    filtering: bool,
    should_quit: bool,
}

impl App {
    fn new(url: String) -> Self {
        Self {
            target: url.clone(),
            url,
            tunnel_problem: None,
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
            feed_limit: Cell::new(DEFAULT_FEED_ROWS),
            refresh_time: None,
            last_refresh: None,
            poll_in_flight: false,
            runtime: None,
            filter: String::new(),
            filtering: false,
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
            base: self.url.clone(),
            roster: self.runtime.as_ref().map(|runtime| runtime.root.clone()),
            identity: self.identity.is_none() || self.refetch_identity,
            selection: self.selection_query(),
            sql_open: self
                .identity
                .as_ref()
                .is_some_and(|identity| identity.sql == SqlAccess::Open),
            feed_limit: self.feed_limit.get(),
        }
    }

    fn selection_query(&self) -> Option<SelectionQuery> {
        let table = self
            .identity
            .as_ref()?
            .tables
            .tables
            .get(self.selected_table)?;
        Some(SelectionQuery::new(table, self.feed_limit.get()))
    }

    fn apply(&mut self, result: PollResult) {
        self.poll_in_flight = false;
        if result.base != self.url {
            return;
        }
        if let Some(roster) = result.discovered {
            self.runtime = Some(Runtime {
                root: self.url.clone(),
                roster,
                current: 0,
            });
            self.switch_nest(0);
            return;
        }
        let mut problems = Vec::new();
        match (result.roster, self.runtime.as_mut()) {
            (Some(Ok(roster)), Some(runtime)) => runtime.roster = roster,
            (Some(Err(error)), Some(_)) => problems.push(("/nests", error)),
            _ => {}
        }
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

    fn apply_selection(&mut self, base: &str, result: Result<Selection, String>) {
        if base != self.url {
            return;
        }
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
            tip: self.ready.as_ref().and_then(|ready| ready.tip),
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
    fn select(&mut self, index: usize) -> Option<SelectionQuery> {
        let last = self.table_count().checked_sub(1)?;
        let index = index.min(last);
        if index == self.selected_table {
            return None;
        }
        self.selected_table = index;
        self.identity
            .as_ref()
            .filter(|identity| identity.sql == SqlAccess::Open)?;
        self.selection_query()
    }

    /// Shows another of a runtime's nests. Everything the dashboard holds belonged to the nest being
    /// left, so it starts again from nothing rather than blending two nests' counters.
    fn switch_nest(&mut self, index: usize) {
        let Some(runtime) = self.runtime.as_mut() else {
            return;
        };
        let Some(nest) = runtime.roster.nests.get(index) else {
            return;
        };
        runtime.current = index;
        self.url = format!("{}{}", runtime.root, nest.path());
        self.identity = None;
        self.refetch_identity = false;
        self.ready = None;
        self.ready_failed = false;
        self.metrics = None;
        self.selection = None;
        self.problems.clear();
        self.samples.clear();
        self.activity = Activity::default();
        self.restarts = 0;
        self.last_restart = None;
        self.selected_table = 0;
        self.table_offset.set(0);
        self.filter.clear();
        self.filtering = false;
        self.refresh_time = None;
        self.last_refresh = None;
    }

    fn cycle_nest(&mut self, forward: bool) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        let count = runtime.roster.nests.len();
        if count < 2 {
            return;
        }
        let next = if forward {
            (runtime.current + 1) % count
        } else {
            (runtime.current + count - 1) % count
        };
        self.switch_nest(next);
    }

    /// Indices into the full table list of the tables the filter lets through.
    fn visible_tables(&self) -> Vec<usize> {
        let filter = self.filter.to_lowercase();
        self.identity
            .as_ref()
            .map(|identity| identity.tables.tables.as_slice())
            .unwrap_or_default()
            .iter()
            .enumerate()
            .filter(|(_, table)| table.table.to_lowercase().contains(&filter))
            .map(|(index, _)| index)
            .collect()
    }

    fn visible_position(&self) -> Option<usize> {
        self.visible_tables()
            .iter()
            .position(|index| *index == self.selected_table)
    }

    fn select_visible(&mut self, position: usize) -> Option<SelectionQuery> {
        let visible = self.visible_tables();
        let index = *visible.get(position.min(visible.len().checked_sub(1)?))?;
        self.select(index)
    }

    fn select_next(&mut self) -> Option<SelectionQuery> {
        let count = self.visible_tables().len().max(1);
        let next = self
            .visible_position()
            .map_or(0, |position| (position + 1) % count);
        self.select_visible(next)
    }

    fn select_previous(&mut self) -> Option<SelectionQuery> {
        let count = self.visible_tables().len().max(1);
        let previous = self
            .visible_position()
            .map_or(0, |position| (position + count - 1) % count);
        self.select_visible(previous)
    }

    fn select_page(&mut self, forward: bool) -> Option<SelectionQuery> {
        let position = self.visible_position().unwrap_or_default();
        self.select_visible(if forward {
            position.saturating_add(TABLE_PAGE)
        } else {
            position.saturating_sub(TABLE_PAGE)
        })
    }

    fn set_filter(&mut self, filter: String) -> Option<SelectionQuery> {
        self.filter = filter;
        self.table_offset.set(0);
        if self.visible_position().is_some() {
            None
        } else {
            self.select_visible(0)
        }
    }

    /// Applies one keypress and returns the table to query if the selection moved.
    fn handle_key(&mut self, key: KeyEvent) -> Option<SelectionQuery> {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return None;
        }
        match key.code {
            KeyCode::Down => return self.select_next(),
            KeyCode::Up => return self.select_previous(),
            KeyCode::PageDown => return self.select_page(true),
            KeyCode::PageUp => return self.select_page(false),
            _ => {}
        }
        if self.filtering {
            return match key.code {
                KeyCode::Enter => {
                    self.filtering = false;
                    None
                }
                KeyCode::Esc => {
                    self.filtering = false;
                    self.set_filter(String::new())
                }
                KeyCode::Backspace => {
                    let mut filter = self.filter.clone();
                    filter.pop();
                    self.set_filter(filter)
                }
                KeyCode::Char(typed) => self.set_filter(format!("{}{typed}", self.filter)),
                _ => None,
            };
        }
        match key.code {
            KeyCode::Char('/') => {
                self.filtering = true;
                None
            }
            KeyCode::Esc if !self.filter.is_empty() => self.set_filter(String::new()),
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
                None
            }
            KeyCode::Char('r') => {
                self.last_refresh = None;
                None
            }
            KeyCode::Char('w') => {
                self.cycle_rate_window();
                None
            }
            KeyCode::Char('n') => {
                self.cycle_nest(true);
                None
            }
            KeyCode::Char('N') => {
                self.cycle_nest(false);
                None
            }
            KeyCode::Char('j') => self.select_next(),
            KeyCode::Char('k') => self.select_previous(),
            KeyCode::Home | KeyCode::Char('g') => self.select_visible(0),
            KeyCode::End | KeyCode::Char('G') => self.select_visible(usize::MAX),
            _ => None,
        }
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

    /// Blocks per second, from how far the tip moved across the sample history.
    fn chain_block_rate(&self) -> Option<f64> {
        let (first, last) = (self.samples.first()?, self.samples.last()?);
        let blocks = last.tip?.checked_sub(first.tip?)?;
        let seconds = last.at.duration_since(first.at).as_secs_f64();
        (blocks > 0 && seconds > 0.0).then(|| blocks as f64 / seconds)
    }

    /// The gauge's fill and label. Lag is measured against the least the nest can be expected to
    /// trail by, one poll interval's worth of blocks or one block, whichever is more: full within
    /// that, half at twice it. `1 - lag / tip` read as full for any lag an arbitrum nest could have.
    fn sync(&self) -> (f64, String) {
        let Some(ready) = self.ready.as_ref() else {
            return (0.0, "waiting for /ready".into());
        };
        if let Some(backfill) = self.backfill() {
            let span = backfill.target.saturating_sub(backfill.origin).max(1);
            let done = backfill.current.saturating_sub(backfill.origin).min(span);
            return (
                done as f64 / span as f64,
                format!(
                    "{} / {}",
                    group_digits(backfill.current),
                    group_digits(backfill.target)
                ),
            );
        }
        let (Some(_), Some(lag)) = (ready.tip, ready.lag_blocks) else {
            return (0.0, "cursorless".into());
        };
        if lag == 0 {
            return (1.0, "at tip".into());
        }
        let rate = self.chain_block_rate();
        let step = rate.map_or(1.0, |rate| {
            (rate * self.activity_width().as_secs_f64()).max(1.0)
        });
        let label = match rate {
            Some(rate) => format!(
                "{} blocks · {} behind",
                group_digits(lag),
                format_span(Duration::from_secs_f64(lag as f64 / rate))
            ),
            None => format!("{} blocks behind", group_digits(lag)),
        };
        ((step / lag as f64).min(1.0), label)
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
        if let Some(problem) = &self.tunnel_problem {
            problem.clone()
        } else if self.last_refresh.is_none() {
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

const DEFAULT_URL: &str = "http://127.0.0.1:8288";

#[derive(Debug, Default)]
struct Args {
    url: Option<String>,
    ssh: Option<String>,
    nest: Option<String>,
    interval: Option<Duration>,
}

/// One entry in `nests.toml`: where the nest listens, and the ssh host to reach it through when
/// that is only on the host's loopback.
#[derive(Debug, Deserialize, Clone, Default, PartialEq)]
#[serde(deny_unknown_fields)]
struct NestTarget {
    url: Option<String>,
    ssh: Option<String>,
}

fn config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    Some(base.join("nuthatch-tui").join("nests.toml"))
}

fn parse_nests(text: &str) -> Result<BTreeMap<String, NestTarget>> {
    Ok(toml::from_str(text)?)
}

/// Flags win over the named entry, which wins over the default listener.
fn resolve(args: &Args, nests: &BTreeMap<String, NestTarget>) -> Result<NestTarget> {
    let named = match &args.nest {
        Some(name) => nests.get(name).cloned().with_context(|| {
            if nests.is_empty() {
                format!("no nest called '{name}': no nests are configured")
            } else {
                let known = nests.keys().cloned().collect::<Vec<_>>().join(", ");
                format!("no nest called '{name}'; configured: {known}")
            }
        })?,
        None => NestTarget::default(),
    };
    Ok(NestTarget {
        url: Some(normalize_url(
            args.url
                .clone()
                .or(named.url)
                .unwrap_or_else(|| DEFAULT_URL.into()),
        )),
        ssh: args.ssh.clone().or(named.ssh),
    })
}

fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
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

    let nests = match config_path() {
        Some(path) if path.exists() => parse_nests(
            &std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?,
        )
        .with_context(|| format!("parsing {}", path.display()))?,
        _ => BTreeMap::new(),
    };
    let mut args = args;
    if args.url.is_none() && args.ssh.is_none() && args.nest.is_none() && !nests.is_empty() {
        match pick_nest(&nests, &quit)? {
            Some(name) => args.nest = Some(name),
            None => return Ok(()),
        }
    }
    let target = resolve(&args, &nests)?;
    let nest_url = target.url.expect("resolve always sets a url");
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .context("building HTTP client")?;
    let tunnel = match &target.ssh {
        Some(host) => {
            eprintln!("opening an ssh forward to {nest_url} on {host}…");
            Some(Tunnel::open("ssh", host, &nest_url, &quit)?)
        }
        None => None,
    };
    let mut app = App::new(
        tunnel
            .as_ref()
            .map_or_else(|| nest_url.clone(), |tunnel| tunnel.local_url.clone()),
    );
    if let Some(host) = &target.ssh {
        app.target = format!("{nest_url} via {host}");
    }
    app.interval_override = args.interval;
    app.no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());

    let _screen = Screen::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    run(&mut terminal, client, &mut app, tunnel, &quit)
}

/// The list shown when `nests.toml` names nests and the command line names none.
struct Picker {
    names: Vec<String>,
    lines: Vec<String>,
    selected: usize,
}

enum Picked {
    Nest(String),
    Quit,
}

impl Picker {
    fn new(nests: &BTreeMap<String, NestTarget>) -> Self {
        let width = nests.keys().map(String::len).max().unwrap_or_default();
        Self {
            names: nests.keys().cloned().collect(),
            lines: nests
                .iter()
                .map(|(name, nest)| {
                    let url = nest.url.as_deref().unwrap_or(DEFAULT_URL);
                    match &nest.ssh {
                        Some(host) => format!("{name:<width$}  {url} via {host}"),
                        None => format!("{name:<width$}  {url}"),
                    }
                })
                .collect(),
            selected: 0,
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<Picked> {
        let last = self.names.len().saturating_sub(1);
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Some(Picked::Quit);
            }
            KeyCode::Char('q') | KeyCode::Esc => return Some(Picked::Quit),
            KeyCode::Enter => return self.names.get(self.selected).cloned().map(Picked::Nest),
            KeyCode::Down | KeyCode::Char('j') => self.selected = (self.selected + 1).min(last),
            KeyCode::Up | KeyCode::Char('k') => self.selected = self.selected.saturating_sub(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => self.selected = last,
            _ => {}
        }
        None
    }

    fn draw(&self, frame: &mut Frame, no_color: bool) {
        let area = frame.area();
        frame.render_widget(Block::default().style(Style::default().bg(CANVAS)), area);
        let rows: Vec<ListItem> = self
            .lines
            .iter()
            .map(|line| ListItem::new(line.as_str()).style(Style::default().fg(Color::White)))
            .collect();
        let mut state = ListState::default().with_selected(Some(self.selected));
        let height = (self.lines.len() as u16 + 2).min(area.height);
        let [list] = Layout::vertical([Constraint::Length(height)])
            .flex(layout::Flex::Center)
            .areas(area);
        frame.render_stateful_widget(
            List::new(rows)
                .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
                .block(panel("CHOOSE A NEST  ↑↓  enter  q")),
            list,
            &mut state,
        );
        if no_color {
            strip_colour(frame.buffer_mut());
        }
    }
}

/// Runs the picker in a screen session of its own, released before the chosen nest's tunnel is
/// opened, so anything ssh prints lands on the ordinary terminal rather than over the dashboard.
fn pick_nest(nests: &BTreeMap<String, NestTarget>, quit: &AtomicBool) -> Result<Option<String>> {
    let mut picker = Picker::new(nests);
    let no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());
    let _screen = Screen::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    loop {
        terminal.draw(|frame| picker.draw(frame, no_color))?;
        if quit.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match picker.handle_key(key) {
                Some(Picked::Nest(name)) => return Ok(Some(name)),
                Some(Picked::Quit) => return Ok(None),
                None => {}
            }
        }
    }
}

/// An `ssh -N -L` forward to a nest that listens only on another host's loopback. `BatchMode`
/// because a password prompt would land in the middle of the dashboard, and
/// `ExitOnForwardFailure` so that a forward which cannot bind is an exit rather than a quiet ssh
/// session forwarding nothing.
struct Tunnel {
    program: String,
    host: String,
    forward: String,
    local_url: String,
    local_port: u16,
    child: Child,
    opened_at: Instant,
    failures: u32,
    retry_at: Option<Instant>,
    last_error: String,
}

impl Tunnel {
    fn open(program: &str, host: &str, nest_url: &str, quit: &AtomicBool) -> Result<Self> {
        let mut url =
            reqwest::Url::parse(nest_url).with_context(|| format!("'{nest_url}' is not a URL"))?;
        let remote_host = url
            .host_str()
            .with_context(|| format!("'{nest_url}' names no host"))?
            .to_owned();
        let remote_port = url
            .port_or_known_default()
            .with_context(|| format!("'{nest_url}' names no port"))?;
        let local_port = TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        url.set_host(Some("127.0.0.1"))?;
        url.set_port(Some(local_port))
            .map_err(|_| anyhow::anyhow!("cannot set a port on '{nest_url}'"))?;
        let forward = format!("127.0.0.1:{local_port}:{remote_host}:{remote_port}");
        let child =
            spawn_ssh(program, host, &forward).with_context(|| format!("starting {program}"))?;
        let mut tunnel = Self {
            program: program.to_owned(),
            host: host.to_owned(),
            forward,
            local_url: normalize_url(url.to_string()),
            local_port,
            child,
            opened_at: Instant::now(),
            failures: 0,
            retry_at: None,
            last_error: String::new(),
        };
        tunnel.wait_until_listening(Duration::from_secs(15), quit)?;
        Ok(tunnel)
    }

    fn wait_until_listening(&mut self, limit: Duration, quit: &AtomicBool) -> Result<()> {
        let started = Instant::now();
        let address = ([127, 0, 0, 1], self.local_port).into();
        loop {
            anyhow::ensure!(!quit.load(Ordering::Relaxed), "interrupted");
            if let Some(status) = self.child.try_wait()? {
                let reason = self.stderr();
                anyhow::bail!("ssh to {} exited ({status}): {reason}", self.host);
            }
            if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            anyhow::ensure!(
                started.elapsed() < limit,
                "ssh to {} had not opened the forward after {}",
                self.host,
                format_span(limit)
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn stderr(&mut self) -> String {
        let mut text = String::new();
        if let Some(mut stderr) = self.child.stderr.take() {
            let _ = stderr.read_to_string(&mut text);
        }
        let text = text.trim();
        if text.is_empty() {
            "no message".into()
        } else {
            text.lines().last().unwrap_or(text).to_owned()
        }
    }

    /// Called every loop: notices ssh exiting and reopens the forward with a doubling backoff.
    /// Returns what the footer should say while the forward is down.
    fn supervise(&mut self) -> Option<String> {
        if let Some(at) = self.retry_at {
            let now = Instant::now();
            if now < at {
                return Some(format!(
                    "ssh to {} exited: {}. Reopening in {}",
                    self.host,
                    self.last_error,
                    // A countdown rounds up, or the first second reads "in 0s".
                    format_span(Duration::from_secs((at - now).as_secs_f64().ceil() as u64))
                ));
            }
            self.retry_at = None;
            match spawn_ssh(&self.program, &self.host, &self.forward) {
                Ok(child) => {
                    self.child = child;
                    self.opened_at = now;
                }
                Err(error) => {
                    self.last_error = error.to_string();
                    self.schedule_retry();
                }
            }
            return Some(format!("ssh to {}: reopening the forward", self.host));
        }
        match self.child.try_wait() {
            Ok(None) => {
                if self.opened_at.elapsed() > TUNNEL_SETTLED {
                    self.failures = 0;
                }
                None
            }
            Ok(Some(_)) => {
                self.last_error = self.stderr();
                self.schedule_retry();
                self.supervise()
            }
            Err(error) => Some(format!("ssh to {}: {error}", self.host)),
        }
    }

    fn schedule_retry(&mut self) {
        self.retry_at = Some(Instant::now() + tunnel_backoff(self.failures));
        self.failures += 1;
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A forward that has stayed up this long is healthy, and its next failure starts the backoff over.
const TUNNEL_SETTLED: Duration = Duration::from_secs(60);

fn tunnel_backoff(failures: u32) -> Duration {
    Duration::from_secs((1u64 << failures.min(5)).min(30))
}

fn ssh_args(host: &str, forward: &str) -> Vec<String> {
    [
        "-N",
        "-o",
        "BatchMode=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=2",
        "-L",
        forward,
        host,
    ]
    .map(String::from)
    .to_vec()
}

fn spawn_ssh(program: &str, host: &str, forward: &str) -> io::Result<Child> {
    Command::new(program)
        .args(ssh_args(host, forward))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
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

const USAGE: &str = "\
nuthatch-tui-client [--url URL] [--ssh HOST] [--nest NAME] [--interval 5s]

  --url URL       the nest's API, as seen from where it runs (default http://127.0.0.1:8288)
  --ssh HOST      reach it through an ssh forward to HOST, for a nest bound to loopback there
  --nest NAME     take url and ssh from NAME in ~/.config/nuthatch-tui/nests.toml
  --interval DUR  poll this often instead of as often as the nest polls";

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args> {
    let mut parsed = Args::default();
    while let Some(arg) = args.next() {
        let mut value = |what: &str| args.next().with_context(|| format!("{arg} needs {what}"));
        match arg.as_str() {
            "--url" => parsed.url = Some(normalize_url(value("a Nuthatch base URL")?)),
            "--ssh" => parsed.ssh = Some(value("an ssh host")?),
            "--nest" => parsed.nest = Some(value("a name from nests.toml")?),
            "--interval" => parsed.interval = Some(parse_interval(&value("a duration, e.g. 5s")?)?),
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument '{other}'; try --help"),
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
    mut tunnel: Option<Tunnel>,
    quit: &AtomicBool,
) -> Result<()> {
    let (requests, replies) = spawn_worker(client);
    let send = |request| {
        requests
            .send(request)
            .map_err(|_| anyhow::anyhow!("the fetch thread has stopped"))
    };
    loop {
        if let Some(tunnel) = tunnel.as_mut() {
            app.tunnel_problem = tunnel.supervise();
        }
        for reply in replies.try_iter() {
            match reply {
                Reply::Poll(result) => app.apply(*result),
                Reply::Selection(base, result) => app.apply_selection(&base, result),
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
            let query = app.handle_key(key);
            if let Some(table) = query {
                send(Request::Selection(app.url.clone(), table))?;
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

fn fetch_selection(
    client: &Client,
    base: &str,
    query: &SelectionQuery,
) -> Result<Selection, String> {
    let url = format!("{base}/sql");
    let counts: SqlResponse = fetch_json(client, &url, &[("q", &query.counts_sql())])?;
    let events: SqlResponse = fetch_json(client, &url, &[("q", &query.events_sql())])?;
    let row = counts.rows.first().and_then(Value::as_object);
    Ok(Selection {
        table: query.table.clone(),
        columns: query.columns.clone(),
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

/// Labelled series of one family are summed, as RPC methods are across endpoints. A family that
/// also publishes an unlabelled series is giving its total there and only breaking it down in the
/// labelled ones, so adding the two would count everything twice.
fn parse_prometheus(text: &str) -> BTreeMap<String, f64> {
    let mut totals = BTreeMap::new();
    let mut summed: BTreeMap<String, f64> = BTreeMap::new();
    for line in text
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let Some((series, value)) = line.split_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        match series.split_once('{') {
            Some((name, _)) => *summed.entry(name.to_owned()).or_default() += value,
            None => {
                totals.insert(series.to_owned(), value);
            }
        }
    }
    summed.extend(totals);
    summed
}

fn metric_u64(metrics: &BTreeMap<String, f64>, name: &str) -> u64 {
    metric_opt_u64(metrics, name).unwrap_or_default()
}

fn metric_opt_u64(metrics: &BTreeMap<String, f64>, name: &str) -> Option<u64> {
    metrics.get(name).copied().map(|value| value as u64)
}

fn group_digits(value: u64) -> String {
    group_decimal(&value.to_string())
}

fn group_decimal(digits: &str) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |rest| ("-", rest));
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 + 1);
    grouped.push_str(sign);
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

fn format_value(value: &Value) -> String {
    match value {
        Value::String(text) if text.starts_with("0x") && text.len() > 14 => {
            format!("{}…{}", &text[..6], &text[text.len() - 4..])
        }
        Value::String(text)
            if !text.is_empty()
                && text
                    .trim_start_matches('-')
                    .bytes()
                    .all(|b| b.is_ascii_digit()) =>
        {
            format_decimal(text)
        }
        Value::String(text) => shorten(text, 24),
        other => other.to_string(),
    }
}

/// Big integers arrive as exact decimal text. Past fifteen digits the exact figure no longer fits a
/// feed line, and an unlimited approval (2^256 - 1) is seventy-eight of them.
fn format_decimal(text: &str) -> String {
    let (sign, digits) = text
        .strip_prefix('-')
        .map_or(("", text), |rest| ("-", rest));
    if digits.len() <= 15 {
        return group_decimal(text);
    }
    format!(
        "{sign}{}.{}e{}",
        &digits[..1],
        &digits[1..3],
        digits.len() - 1
    )
}

/// The feed as a small table: column names once in a header, then a line per row, taking columns
/// in declared order while they fit. A nest that published no column list gets the first row's
/// own keys, less the implicit ones and the big-integer companions.
fn feed_lines(rows: &[Value], columns: &[String], width: usize) -> Vec<String> {
    let rows: Vec<_> = rows.iter().filter_map(Value::as_object).collect();
    let Some(first) = rows.first() else {
        return Vec::new();
    };
    let names: Vec<&str> = if columns.is_empty() {
        first
            .keys()
            .map(String::as_str)
            .filter(|key| {
                !IMPLICIT_COLUMNS.contains(key)
                    && *key != "table"
                    && !key.ends_with("_dec")
                    && !key.ends_with("_overflow")
            })
            .collect()
    } else {
        columns.iter().map(String::as_str).collect()
    };
    let blocks: Vec<String> = rows
        .iter()
        .map(|row| {
            row.get("block_number")
                .and_then(Value::as_u64)
                .map_or("?".into(), group_digits)
        })
        .collect();
    let mut chosen: Vec<FeedColumn> = Vec::new();
    let mut used = 0;
    let named = names.into_iter().map(|name| {
        let cells: Vec<String> = rows
            .iter()
            .map(|row| row.get(name).map_or("—".into(), format_value))
            .collect();
        (name, cells)
    });
    for (name, cells) in std::iter::once(("block", blocks)).chain(named) {
        let width_needed = cells
            .iter()
            .map(|cell| cell.chars().count())
            .chain(std::iter::once(name.len()))
            .max()
            .unwrap_or_default();
        let gap = if chosen.is_empty() { 0 } else { 2 };
        if used + gap + width_needed > width {
            break;
        }
        used += gap + width_needed;
        chosen.push(FeedColumn {
            name,
            cells,
            width: width_needed,
        });
    }
    // Line 0 is the header; line n is row n - 1.
    (0..=rows.len())
        .map(|line| {
            chosen
                .iter()
                .map(|column| {
                    let cell = match line {
                        0 => column.name,
                        row => column.cells[row - 1].as_str(),
                    };
                    format!("{cell:<width$}", width = column.width)
                })
                .collect::<Vec<_>>()
                .join("  ")
                .trim_end()
                .to_owned()
        })
        .collect()
}

struct FeedColumn<'a> {
    name: &'a str,
    cells: Vec<String>,
    width: usize,
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
        Constraint::Length(2),
        Constraint::Length(7),
        Constraint::Min(10),
        Constraint::Length(1),
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
    if let Some(runtime) = &app.runtime {
        header.push(Span::styled(
            format!(
                "  {} {}/{}",
                runtime.roster.runtime,
                runtime.current + 1,
                runtime.roster.nests.len()
            ),
            Style::default().fg(Color::Gray),
        ));
        let quarantined = runtime
            .roster
            .nests
            .iter()
            .filter(|nest| nest.health == "quarantined")
            .count();
        if quarantined > 0 {
            header.push(Span::styled(
                format!("  {quarantined} quarantined"),
                Style::default().fg(Color::Red).bold(),
            ));
        }
    }
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
        format!("   {}", app.target),
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

    let (sync_ratio, sync_label) = app.sync();
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
            .ratio(sync_ratio)
            .label(sync_label),
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
    let visible = app.visible_tables();
    let rows: Vec<ListItem> = visible
        .iter()
        .map(|index| {
            ListItem::new(tables[*index].table.as_str()).style(Style::default().fg(Color::White))
        })
        .collect();
    let position = app.visible_position();
    let mut list_state = ListState::default()
        .with_offset(app.table_offset.get())
        .with_selected(position);
    let filter = match (app.filtering, app.filter.is_empty()) {
        (true, _) => format!("  /{}▏", app.filter),
        (false, false) => format!("  /{}", app.filter),
        (false, true) => String::new(),
    };
    let title = match (tables.is_empty(), position, visible.is_empty()) {
        (true, _, _) => "INDEXED TABLES".to_owned(),
        (false, _, true) => format!("INDEXED TABLES{filter}  no match"),
        (false, Some(position), false) => {
            format!("INDEXED TABLES{filter}  {}/{}", position + 1, visible.len())
        }
        (false, None, false) => format!("INDEXED TABLES{filter}  {}", visible.len()),
    };
    frame.render_stateful_widget(
        List::new(rows)
            .highlight_style(Style::default().fg(Color::Black).bg(Color::Cyan))
            .block(panel(&if filter.is_empty() && !tables.is_empty() {
                format!("{title}  ↑↓ j k")
            } else {
                title
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
    let rejections = lifetime("nuthatch_sql_rejections_total");
    let sql_line = Line::from(vec![
        Span::raw(format!(
            "SQL QUERIES     {}  ",
            format_optional_count(lifetime("nuthatch_sql_queries_total"))
        )),
        Span::styled(
            format!("rejected {}", format_optional_count(rejections)),
            Style::default().fg(if rejections.is_some_and(|count| count > 0) {
                Color::Yellow
            } else {
                Color::Reset
            }),
        ),
        Span::raw(format!(
            "   OUTBOX {}",
            format_optional_count(lifetime("nuthatch_alert_outbox_depth"))
        )),
    ]);
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
        sql_line,
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
            .map(|selection| {
                feed_lines(
                    &selection.events,
                    &selection.columns,
                    bottom[1].width.saturating_sub(4) as usize,
                )
            })
            .unwrap_or_default()
            .into_iter()
            .enumerate()
            .map(|(index, line)| {
                let style = if index == 0 {
                    Style::default().fg(Color::Gray)
                } else {
                    Style::default()
                };
                ListItem::new(line).style(style)
            })
            .collect(),
    };
    if show_feed {
        let feed_area = if show_sparkline {
            Layout::vertical([Constraint::Min(3), Constraint::Length(4)]).split(right[1])
        } else {
            Layout::vertical([Constraint::Percentage(100)]).split(right[1])
        };
        // Borders and the header line.
        app.feed_limit
            .set((feed_area[0].height.saturating_sub(3) as usize).clamp(1, MAX_FEED_ROWS));
        frame.render_widget(
            List::new(feed_rows).block(panel("LIVE EVENT FEED")),
            feed_area[0],
        );
        if show_sparkline {
            draw_activity(frame, app, feed_area[1]);
        }
    }

    let mut footer = vec![
        Span::styled(
            " q ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" quit  "),
        Span::styled(
            " r ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" refresh  "),
        Span::styled(
            " ↑↓ ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" tables  "),
        Span::styled(
            " w ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" window  "),
        Span::styled(
            " / ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ),
        Span::raw(" filter  "),
    ];
    if app.runtime.is_some() {
        footer.push(Span::styled(
            " n ",
            Style::default().fg(Color::Black).bg(Color::Gray).bold(),
        ));
        footer.push(Span::raw(" nest  "));
    }
    footer.push(Span::styled(
        app.status(),
        Style::default().fg(if app.problems.is_empty() {
            Color::DarkGray
        } else {
            Color::Yellow
        }),
    ));
    frame.render_widget(Paragraph::new(Line::from(footer)), vertical[3]);

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
                "REFRESH / {span}  peak {} ms",
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
                        columns: Vec::new(),
                    },
                    EventTable {
                        table: "usdc__transfer".into(),
                        columns: Vec::new(),
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
             nuthatch_rpc_request_duration_seconds_count 221\n\
             nuthatch_sql_queries_total 1204\n\
             nuthatch_sql_rejections_total 6\n\
             nuthatch_sql_rejections_total{reason=\"busy\"} 6\n\
             nuthatch_alert_outbox_depth 0\n",
        ));
        app.selection = Some(Selection {
            table: "usdc__approval".into(),
            columns: ["owner", "spender", "value"].map(String::from).to_vec(),
            rows: Some(2275),
            latest_block: Some(25_766_811),
            events: serde_json::from_str(
                r#"[{"block_number":25766811,"owner":"0x9fad00000000000000000000000000000000043a9","spender":"0x4cd00000000000000000000000000000000000bc31","value":"115792089237316195423570985008687907853269984665640564039457584007913129639935"},
                    {"block_number":25766810,"owner":"0x3e8100000000000000000000000000000000bd36","spender":"0xee3900000000000000000000000000000000063b5","value":"1500000000"}]"#,
            )
            .expect("fixture rows"),
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
                tip: Some(25_766_806),
                cpu_seconds: Some(2.4),
            },
            Sample {
                at: now,
                decoded_rows: Some(2275),
                rpc_requests: Some(367),
                rpc_methods: Some(412),
                indexed_block: Some(25_766_811),
                tip: Some(25_766_811),
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
            "SQL QUERIES     1,204  rejected 6   OUTBOX 0",
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
            "block       owner        spender      value",
            "25,766,811  0x9fad…43a9  0x4cd0…bc31  1.15e77",
            "25,766,810  0x3e81…bd36  0xee39…63b5  1,500,000,000",
        ] {
            assert!(
                screen.contains(expected),
                "{expected:?} missing at 100x30:\n{screen}"
            );
        }
    }

    /// The status is the part of the footer that changes, so a failure has to be readable whole.
    #[test]
    fn the_footer_leaves_room_for_a_failure_at_one_hundred_columns() {
        let mut app = populated();
        app.problems = vec![("/metrics", "HTTP 404 Not Found".into())];
        let screen = render(&app, 100, 30);
        assert!(
            screen
                .lines()
                .find(|line| line.contains("q  quit"))
                .is_some_and(|footer| footer.contains("/metrics: HTTP 404 Not Found")),
            "{screen}"
        );
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
    fn the_sparkline_arrives_at_thirty_one_rows() {
        assert!(!rendered(100, 30).contains("REFRESH /"));
        assert!(rendered(100, 31).contains("REFRESH /"));
    }

    /// At 80x24 there is no room for both the panel and the feed. The feed is what gives way: a
    /// missing panel is visibly missing, whereas a cropped metric line reads as a smaller number.
    #[test]
    fn a_short_terminal_drops_the_feed_rather_than_a_metric_line() {
        let screen = rendered(80, 24);
        for expected in ["MEMORY RSS", "REORGS", "DISK", "RPC HEALTH", "SQL QUERIES"] {
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
                    columns: Vec::new(),
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

    /// Types `keys` and returns the last selection they moved to, if any did.
    fn press(app: &mut App, keys: &str) -> Option<SelectionQuery> {
        keys.chars()
            .map(|key| {
                app.handle_key(KeyEvent::from(match key {
                    '\n' => KeyCode::Enter,
                    '\x1b' => KeyCode::Esc,
                    '\x08' => KeyCode::Backspace,
                    other => KeyCode::Char(other),
                }))
            })
            .fold(None, |moved, query| query.or(moved))
    }

    fn allocations_nest() -> App {
        let mut app = with_many_tables(0);
        let names = [
            "curation__burned",
            "staking__allocation_closed",
            "staking__stake_deposited",
            "subgraph_service__allocation_closed",
            "subgraph_service__allocation_created",
            "total_supply",
        ];
        app.identity.as_mut().unwrap().tables = Tables {
            count: names.len(),
            tables: names
                .iter()
                .map(|name| EventTable {
                    table: (*name).into(),
                    columns: Vec::new(),
                })
                .collect(),
        };
        app
    }

    #[test]
    fn typing_a_filter_narrows_the_list_and_moves_the_selection_into_it() {
        let mut app = allocations_nest();
        let query = press(&mut app, "/ALLOC");
        assert_eq!(
            query.map(|query| query.table).as_deref(),
            Some("staking__allocation_closed")
        );
        assert_eq!(app.visible_tables(), [1, 3, 4]);
        // Letters that are also commands are text while filtering.
        press(&mut app, "q");
        assert!(!app.should_quit);
        assert!(app.visible_tables().is_empty());
        let screen = render(&app, 100, 30);
        assert!(
            screen.contains("INDEXED TABLES  /ALLOCq▏  no match"),
            "{screen}"
        );
        press(&mut app, "\x08\n");
        assert!(!app.filtering);
        assert_eq!(
            press(&mut app, "j").map(|query| query.table).as_deref(),
            Some("subgraph_service__allocation_closed")
        );
        assert_eq!(
            press(&mut app, "jj").map(|query| query.table).as_deref(),
            Some("staking__allocation_closed"),
            "j wraps within the filtered tables"
        );
        let screen = render(&app, 100, 30);
        assert!(screen.contains("INDEXED TABLES  /ALLOC  1/3"), "{screen}");
        assert!(!screen.contains("curation__burned"), "{screen}");
        // Esc clears a standing filter first, and only then quits.
        press(&mut app, "\x1b");
        assert_eq!(app.visible_tables().len(), 6);
        assert!(!app.should_quit);
        press(&mut app, "\x1b");
        assert!(app.should_quit);
    }

    #[test]
    fn esc_while_typing_abandons_the_filter() {
        let mut app = allocations_nest();
        press(&mut app, "/total\x1b");
        assert!(!app.filtering);
        assert!(app.filter.is_empty());
        assert_eq!(app.selected_table_name(), Some("total_supply"));
        assert!(!app.should_quit);
    }

    #[test]
    fn paging_and_the_ends_clamp_to_the_list() {
        let mut app = with_many_tables(81);
        assert_eq!(
            app.select(usize::MAX).map(|query| query.table).as_deref(),
            Some("graph__table_080")
        );
        assert!(app.select(app.selected_table + TABLE_PAGE).is_none());
        assert_eq!(
            app.select(0).map(|query| query.table).as_deref(),
            Some("graph__table_000")
        );
        assert_eq!(
            app.select_previous().map(|query| query.table).as_deref(),
            Some("graph__table_080")
        );
        assert_eq!(
            app.select_next().map(|query| query.table).as_deref(),
            Some("graph__table_000")
        );
        app.identity = None;
        assert!(app.select_next().is_none());
    }

    /// Samples whose tip advances at `blocks_per_second`, a minute apart.
    fn at_block_rate(app: &mut App, blocks_per_second: u64, lag: u64) {
        let now = Instant::now();
        let tip = 500_000_000;
        for sample in &mut app.samples {
            sample.tip = Some(tip - blocks_per_second * now.duration_since(sample.at).as_secs());
        }
        let ready = app.ready.as_mut().unwrap();
        ready.tip = Some(tip);
        ready.lag_blocks = Some(lag);
        ready.last_block = tip - lag;
    }

    #[test]
    fn the_gauge_measures_lag_against_a_poll_on_a_slow_chain() {
        let mut app = populated();
        assert_eq!(app.sync(), (1.0, "at tip".into()));
        // Mainnet-ish: five blocks in the minute between samples, so a 2 s poll trails by one.
        at_block_rate(&mut app, 0, 2);
        app.samples[0].tip = app.samples[1].tip.map(|tip| tip - 5);
        assert_eq!(app.sync(), (0.5, "2 blocks · 24s behind".into()));
    }

    #[test]
    fn the_gauge_measures_lag_against_a_poll_on_a_fast_chain() {
        let mut app = populated();
        app.ready.as_mut().unwrap().freshness = Some(Freshness {
            poll_interval_secs: Some(300),
        });
        // Arbitrum-ish: four blocks a second, and a five-minute cursor trails by ~1,200 by design.
        at_block_rate(&mut app, 4, 1_100);
        assert_eq!(app.sync().0, 1.0);
        at_block_rate(&mut app, 4, 331_434);
        let (ratio, label) = app.sync();
        assert!(ratio < 0.01, "{ratio}");
        assert_eq!(label, "331,434 blocks · 23h behind");
    }

    #[test]
    fn the_gauge_without_a_measured_rate_counts_blocks() {
        let mut app = populated();
        app.samples.truncate(1);
        let ready = app.ready.as_mut().unwrap();
        ready.lag_blocks = Some(4);
        assert_eq!(app.sync(), (0.25, "4 blocks behind".into()));
    }

    #[test]
    fn the_feed_is_a_table_in_schema_order_that_fits_the_width() {
        // Real rows from `usdc__transfer` on the 3.10.0 demo nest, the second an unlimited amount.
        let rows: Vec<Value> = serde_json::from_str(
            r#"[{"_seq":27314306940942,"address":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48","block_number":26048953,"from":"0x000000000004444c5dc75cb358380d2e3de08a90","log_index":14,"table":"usdc__transfer","to":"0x4313c378cc91ea583c91387b9216e2c03096b27f","value":"486153178","value_dec":"486153178","value_overflow":false},
                {"block_number":26048952,"from":"0x9fad0000000000000000000000000000000043a9","to":"0x4cd0000000000000000000000000000000000bc31","value":"115792089237316195423570985008687907853269984665640564039457584007913129639935","value_dec":null,"value_overflow":true}]"#,
        )
        .unwrap();
        let columns = ["from", "to", "value"].map(String::from);
        assert_eq!(
            feed_lines(&rows, &columns, 58),
            [
                "block       from         to           value",
                "26,048,953  0x0000…8a90  0x4313…b27f  486,153,178",
                "26,048,952  0x9fad…43a9  0x4cd0…bc31  1.15e77",
            ]
        );
        assert_eq!(
            feed_lines(&rows, &columns, 30)[1],
            "26,048,953  0x0000…8a90"
        );
        // Without a column list the first row's own keys are used, less implicit ones and companions.
        assert_eq!(feed_lines(&rows, &[], 58), feed_lines(&rows, &columns, 58));
        assert!(feed_lines(&[], &columns, 58).is_empty());
    }

    #[test]
    fn long_decimals_turn_scientific_rather_than_vanish() {
        assert_eq!(format_decimal("486153178"), "486,153,178");
        assert_eq!(format_decimal("-1000"), "-1,000");
        assert_eq!(format_decimal("999999999999999"), "999,999,999,999,999");
        assert_eq!(
            format_decimal(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
            ),
            "1.15e77"
        );
    }

    #[test]
    fn the_feed_query_names_its_columns_and_quotes_them() {
        let table: EventTable = serde_json::from_str(
            r#"{"table":"usdc__transfer","columns":[
                {"name":"block_number","sol_type":"implicit"},{"name":"log_index","sol_type":"implicit"},
                {"name":"from","sol_type":"address"},{"name":"to","sol_type":"address"},
                {"name":"value","sol_type":"uint256"}]}"#,
        )
        .unwrap();
        assert_eq!(
            SelectionQuery::new(&table, 9).events_sql(),
            "SELECT \"block_number\", \"from\", \"to\", \"value\" FROM \"usdc__transfer\" \
             ORDER BY block_number DESC, log_index DESC LIMIT 9"
        );
        let bare: EventTable = serde_json::from_str(
            r#"{"table":"t","columns":[{"name":"block_number","sol_type":"implicit"},{"name":"result","sol_type":"bytes"}]}"#,
        )
        .unwrap();
        assert!(
            SelectionQuery::new(&bare, 6)
                .events_sql()
                .ends_with("ORDER BY block_number DESC LIMIT 6")
        );
    }

    #[test]
    fn the_feed_asks_for_as_many_rows_as_the_panel_shows() {
        let app = populated();
        render(&app, 100, 30);
        let short = app.feed_limit.get();
        render(&app, 100, 50);
        assert!(
            app.feed_limit.get() > short,
            "{short} -> {}",
            app.feed_limit.get()
        );
        assert_eq!(app.poll_request().feed_limit, app.feed_limit.get());
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

    /// Nuthatch 3.10 publishes SQL rejections as a total and again by reason.
    #[test]
    fn a_published_total_is_not_added_to_its_own_breakdown() {
        let metrics = parse_prometheus(
            "nuthatch_sql_rejections_total 6\n\
             nuthatch_sql_rejections_total{reason=\"busy\"} 4\n\
             nuthatch_sql_rejections_total{reason=\"too_large\"} 2\n\
             nuthatch_rpc_methods_total{method=\"eth_getLogs\"} 4\n\
             nuthatch_rpc_methods_total{method=\"eth_blockNumber\"} 9\n",
        );
        assert_eq!(metrics.get("nuthatch_sql_rejections_total"), Some(&6.0));
        assert_eq!(metrics.get("nuthatch_rpc_methods_total"), Some(&13.0));
    }

    /// Twenty-two rows is the least that holds the performance panel whole.
    #[test]
    fn every_metric_line_survives_at_twenty_two_rows() {
        let screen = rendered(80, 22);
        for expected in ["RPC REQUESTS", "RPC HEALTH", "SQL QUERIES"] {
            assert!(
                screen.contains(expected),
                "{expected:?} cropped at 80x22:\n{screen}"
            );
        }
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
        assert_eq!(parsed.url.as_deref(), Some("http://h:1"));
        assert_eq!(parsed.interval, Some(Duration::from_secs(120)));
        assert_eq!(
            args(&["--interval", "5"]).unwrap().interval,
            Some(Duration::from_secs(5))
        );
        assert!(args(&["--interval", "0s"]).is_err());
        assert!(args(&["--interval", "soon"]).is_err());
        assert!(args(&["--bogus"]).is_err());
        assert!(args(&["--ssh"]).is_err());
        let parsed = args(&["--nest", "allocations", "--ssh", "hel1"]).unwrap();
        assert_eq!(parsed.nest.as_deref(), Some("allocations"));
        assert_eq!(parsed.ssh.as_deref(), Some("hel1"));
    }

    const NESTS: &str = r#"
        [allocations]
        url = "http://127.0.0.1:8107"
        ssh = "89.167.109.4"

        [local]
        url = "http://127.0.0.1:18288/"
    "#;

    #[test]
    fn a_named_nest_supplies_url_and_host_and_flags_override_it() {
        let nests = parse_nests(NESTS).unwrap();
        let args = |nest: Option<&str>, url: Option<&str>, ssh: Option<&str>| Args {
            nest: nest.map(String::from),
            url: url.map(String::from),
            ssh: ssh.map(String::from),
            interval: None,
        };
        assert_eq!(
            resolve(&args(Some("allocations"), None, None), &nests).unwrap(),
            NestTarget {
                url: Some("http://127.0.0.1:8107".into()),
                ssh: Some("89.167.109.4".into()),
            }
        );
        assert_eq!(
            resolve(
                &args(
                    Some("allocations"),
                    Some("http://127.0.0.1:8095"),
                    Some("nbg1")
                ),
                &nests
            )
            .unwrap(),
            NestTarget {
                url: Some("http://127.0.0.1:8095".into()),
                ssh: Some("nbg1".into()),
            }
        );
        assert_eq!(
            resolve(&args(Some("local"), None, None), &nests)
                .unwrap()
                .url
                .as_deref(),
            Some("http://127.0.0.1:18288")
        );
        assert_eq!(
            resolve(&args(None, None, None), &nests)
                .unwrap()
                .url
                .as_deref(),
            Some(DEFAULT_URL)
        );
        let unknown = resolve(&args(Some("staking"), None, None), &nests).unwrap_err();
        assert_eq!(
            unknown.to_string(),
            "no nest called 'staking'; configured: allocations, local"
        );
        assert!(
            parse_nests("[x]\nurl = \"u\"\nport = 1\n").is_err(),
            "typos are refused"
        );
    }

    #[test]
    fn the_picker_lists_the_configured_nests_and_returns_the_chosen_one() {
        let nests = parse_nests(NESTS).unwrap();
        let mut picker = Picker::new(&nests);
        let mut terminal = Terminal::new(TestBackend::new(80, 12)).unwrap();
        terminal.draw(|frame| picker.draw(frame, false)).unwrap();
        let screen: String = format!("{:?}", terminal.backend().buffer());
        for expected in [
            "CHOOSE A NEST",
            "allocations  http://127.0.0.1:8107 via 89.167.109.4",
            "local        http://127.0.0.1:18288",
        ] {
            assert!(screen.contains(expected), "{expected:?} missing:\n{screen}");
        }
        let key = |code| KeyEvent::from(code);
        assert!(picker.handle_key(key(KeyCode::Char('j'))).is_none());
        assert!(
            picker.handle_key(key(KeyCode::Char('j'))).is_none(),
            "clamps at the end"
        );
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            Some(Picked::Nest(name)) if name == "local"
        ));
        assert!(matches!(
            picker.handle_key(key(KeyCode::Char('q'))),
            Some(Picked::Quit)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn an_interrupt_stops_the_wait_for_a_tunnel() {
        let error = Tunnel::open(
            &fake_ssh(false),
            "hel1",
            "http://127.0.0.1:8107",
            &AtomicBool::new(true),
        )
        .err()
        .expect("an interrupted open fails");
        assert_eq!(error.to_string(), "interrupted");
    }

    #[test]
    fn ssh_is_asked_for_a_batch_mode_forward_that_fails_loudly() {
        let args = ssh_args("hel1", "127.0.0.1:40000:127.0.0.1:8107");
        assert_eq!(args.last().map(String::as_str), Some("hel1"));
        for expected in [
            "-N",
            "BatchMode=yes",
            "ExitOnForwardFailure=yes",
            "127.0.0.1:40000:127.0.0.1:8107",
        ] {
            assert!(
                args.iter().any(|arg| arg == expected),
                "{expected} missing from {args:?}"
            );
        }
    }

    #[test]
    fn the_tunnel_backs_off_doubling_to_half_a_minute() {
        let delays: Vec<u64> = (0..8).map(|n| tunnel_backoff(n).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30]);
    }

    /// A stand-in for ssh: listens on the local end of `-L` as a real forward would, or with
    /// `fail` set, says what ssh says when the key is refused and exits the way it does.
    #[cfg(unix)]
    fn fake_ssh(fail: bool) -> String {
        use std::{os::unix::fs::PermissionsExt, sync::atomic::AtomicUsize};
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "nuthatch-tui-fake-ssh-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let body = if fail {
            "echo 'hel1: Permission denied (publickey).' >&2; exit 255".to_owned()
        } else {
            "while [ $# -gt 0 ]; do [ \"$1\" = -L ] && forward=$2; shift; done\n\
             port=${forward#127.0.0.1:}; port=${port%%:*}\n\
             exec python3 -c \"import socket, time; s = socket.socket(); \
             s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); \
             s.bind(('127.0.0.1', $port)); s.listen(); time.sleep(60)\""
                .to_owned()
        };
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    #[test]
    fn the_tunnel_rewrites_the_url_onto_its_local_end() {
        let tunnel = Tunnel::open(
            &fake_ssh(false),
            "hel1",
            "http://127.0.0.1:8107/allocations",
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(
            tunnel.local_url,
            format!("http://127.0.0.1:{}/allocations", tunnel.local_port)
        );
        assert!(tunnel.forward.ends_with(":127.0.0.1:8107"));
    }

    #[cfg(unix)]
    #[test]
    fn a_refused_key_is_reported_before_the_dashboard_opens() {
        let error = Tunnel::open(
            &fake_ssh(true),
            "hel1",
            "http://127.0.0.1:8107",
            &AtomicBool::new(false),
        )
        .err()
        .expect("a refused key must not open");
        let message = error.to_string();
        assert!(
            message.contains("Permission denied (publickey)."),
            "{message}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_dead_tunnel_is_reported_and_reopened() {
        let mut tunnel = Tunnel::open(
            &fake_ssh(false),
            "hel1",
            "http://127.0.0.1:8107",
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(tunnel.supervise(), None);
        tunnel.child.kill().unwrap();
        tunnel.child.wait().unwrap();
        let down = tunnel.supervise().expect("a dead forward is reported");
        assert!(down.starts_with("ssh to hel1 exited"), "{down}");
        assert!(down.contains("Reopening in 1s"), "{down}");
        tunnel.retry_at = Some(Instant::now());
        assert_eq!(
            tunnel.supervise().as_deref(),
            Some("ssh to hel1: reopening the forward")
        );
        tunnel
            .wait_until_listening(Duration::from_secs(10), &AtomicBool::new(false))
            .unwrap();
        assert_eq!(tunnel.supervise(), None);
        assert_eq!(
            tunnel.failures, 1,
            "the backoff only resets once the forward has settled"
        );
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
    /// With `NUTHATCH_SSH=host` it goes through a forward, as `--ssh` does.
    #[test]
    #[ignore = "needs a running nest at NUTHATCH_URL"]
    fn live() {
        let url = normalize_url(std::env::var("NUTHATCH_URL").expect("NUTHATCH_URL"));
        let tunnel = std::env::var("NUTHATCH_SSH").ok().map(|host| {
            Tunnel::open("ssh", &host, &url, &AtomicBool::new(false)).expect("ssh forward")
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut app = App::new(
            tunnel
                .as_ref()
                .map_or_else(|| url.clone(), |tunnel| tunnel.local_url.clone()),
        );
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
            self.apply(poll(client, &request));
        }

        fn query(&mut self, client: &Client, query: Option<SelectionQuery>) {
            if let Some(query) = query {
                let base = self.url.clone();
                self.apply_selection(&base, fetch_selection(client, &base, &query));
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
        assert!(app.select_next().is_none());
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
        let (requests, replies) = spawn_worker(Client::new());
        requests
            .send(Request::Poll(PollRequest {
                base: nest.base.clone(),
                roster: None,
                identity: true,
                selection: None,
                sql_open: true,
                feed_limit: DEFAULT_FEED_ROWS,
            }))
            .unwrap();
        for table in ["usdc__transfer", "usdc__approval", "usdc__transfer"] {
            let table = EventTable {
                table: table.into(),
                columns: Vec::new(),
            };
            requests
                .send(Request::Selection(
                    nest.base.clone(),
                    SelectionQuery::new(&table, 6),
                ))
                .unwrap();
        }
        let first = replies.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(first, Reply::Poll(_)));
        let Reply::Selection(_, Ok(selection)) =
            replies.recv_timeout(Duration::from_secs(5)).unwrap()
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

    /// A runtime's root, answering the way `nuthatch dev` over a `mounts.toml` does in 3.10.0.
    fn runtime(target: &str) -> (u16, String) {
        match target {
            "/nests" => (
                200,
                r#"{"runtime":"demo-runtime","nests":[
                    {"name":"usdc","base_path":"/usdc","health":"indexing"},
                    {"name":"weth","base_path":"/weth","health":"quarantined"}]}"#
                    .into(),
            ),
            "/ready" => (
                200,
                r#"{"quarantined":[],"ready":true,"stalled":[],"version":"3.10.0"}"#.into(),
            ),
            "/weth/queries" => (
                200,
                r#"{"free_form":false,"queries":[],"sql":"deny"}"#.into(),
            ),
            _ => match target
                .split_once('/')
                .and_then(|(_, rest)| rest.split_once('/'))
            {
                Some(("usdc" | "weth", rest)) => healthy(&format!("/{rest}")),
                _ => (404, "not found".into()),
            },
        }
    }

    #[test]
    fn a_runtime_root_is_recognised_and_its_nests_can_be_walked() {
        let nest = TestNest::serve(runtime);
        let (mut app, client) = nest.app();
        app.refresh(&client);
        let runtime = app.runtime.as_ref().expect("the roster was found");
        assert_eq!(runtime.roster.runtime, "demo-runtime");
        assert_eq!(app.url, format!("{}/usdc", nest.base));
        assert!(app.ready.is_none(), "the root's /ready is not a nest's");
        app.refresh(&client);
        assert_eq!(app.state().0, "● LIVE");
        assert_eq!(app.selection.as_ref().unwrap().rows, Some(2275));
        let screen = render(&app, 100, 30);
        assert!(
            screen.contains("demo-runtime 1/2  1 quarantined"),
            "{screen}"
        );
        assert!(screen.contains(" n  nest"), "{screen}");

        press(&mut app, "n");
        assert_eq!(app.url, format!("{}/weth", nest.base));
        assert!(app.identity.is_none() && app.samples.is_empty() && app.selection.is_none());
        app.refresh(&client);
        assert!(matches!(
            app.identity.as_ref().unwrap().sql,
            SqlAccess::Closed { .. }
        ));
        press(&mut app, "N");
        assert_eq!(app.url, format!("{}/usdc", nest.base));
    }

    /// A poll that was already in flight when the operator switched nests must not be drawn over the
    /// nest they switched to.
    #[test]
    fn a_reply_for_a_nest_already_left_is_dropped() {
        let nest = TestNest::serve(runtime);
        let (mut app, client) = nest.app();
        app.refresh(&client);
        let stale = poll(&client, &app.poll_request());
        press(&mut app, "n");
        app.apply(stale);
        assert!(app.ready.is_none() && app.identity.is_none());
        let query = SelectionQuery::new(
            &EventTable {
                table: "usdc__approval".into(),
                columns: Vec::new(),
            },
            6,
        );
        let usdc = format!("{}/usdc", nest.base);
        app.apply_selection(&usdc, fetch_selection(&client, &usdc, &query));
        assert!(app.selection.is_none());
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
