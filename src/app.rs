use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::prelude::*;

use crate::{api::*, format::*, worker::*};

pub(crate) const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Bounds on an interval taken from the nest's own `freshness.poll_interval_secs`. A five-minute
/// cursor still gets a dashboard that notices a crash within half a minute.
pub(crate) const MIN_POLL_INTERVAL: Duration = Duration::from_secs(2);
pub(crate) const MAX_POLL_INTERVAL: Duration = Duration::from_secs(30);
const HISTORY_LEN: usize = 48;
pub(crate) const TABLE_PAGE: usize = 10;
pub(crate) const DEFAULT_FEED_ROWS: usize = 6;
pub(crate) const MAX_FEED_ROWS: usize = 50;
const ACTIVITY_LEN: usize = 64;
/// How long an observed restart keeps the restart line lit.
pub(crate) const RECENT_RESTART: Duration = Duration::from_secs(600);
const RATE_WINDOWS: [Duration; 3] = [
    Duration::from_secs(15),
    Duration::from_secs(60),
    Duration::from_secs(90),
];
#[derive(Clone, Copy)]
pub(crate) struct Sample {
    pub(crate) at: Instant,
    pub(crate) decoded_rows: Option<u64>,
    pub(crate) rpc_requests: Option<u64>,
    pub(crate) rpc_methods: Option<u64>,
    pub(crate) indexed_block: Option<u64>,
    pub(crate) tip: Option<u64>,
    pub(crate) cpu_seconds: Option<f64>,
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
pub(crate) struct Bucket {
    start: Instant,
    pub(crate) rpc_requests: u64,
    pub(crate) peak_refresh_ms: u64,
}

/// Fixed-width time buckets for the activity sparklines. The width follows the nest's poll
/// interval, so a bar is one nest poll's worth of work however often the client happens to ask.
#[derive(Default)]
pub(crate) struct Activity {
    pub(crate) width: Duration,
    pub(crate) buckets: VecDeque<Bucket>,
}

impl Activity {
    pub(crate) fn record(
        &mut self,
        at: Instant,
        width: Duration,
        rpc_requests: u64,
        refresh_ms: u64,
    ) {
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

pub(crate) struct Backfill {
    pub(crate) origin: u64,
    current: u64,
    target: u64,
}

pub(crate) struct Runtime {
    root: String,
    pub(crate) roster: Roster,
    pub(crate) current: usize,
}

pub(crate) struct App {
    pub(crate) url: String,
    /// What the header names: the nest's own URL, and the host when it is reached through ssh.
    pub(crate) target: String,
    /// Set while the ssh forward is down, and says when it will be reopened.
    pub(crate) tunnel_problem: Option<String>,
    pub(crate) interval_override: Option<Duration>,
    pub(crate) no_color: bool,
    pub(crate) identity: Option<Identity>,
    /// Set by an observed restart: the next poll fetches the catalogue again.
    pub(crate) refetch_identity: bool,
    pub(crate) ready: Option<Ready>,
    /// The last `/ready` failed, so `ready` is the previous answer and everything is stale.
    ready_failed: bool,
    pub(crate) metrics: Option<BTreeMap<String, f64>>,
    pub(crate) hot_rows: Option<u64>,
    pub(crate) selection: Option<Selection>,
    pub(crate) problems: Vec<Problem>,
    pub(crate) samples: Vec<Sample>,
    pub(crate) activity: Activity,
    pub(crate) restarts: u32,
    pub(crate) last_restart: Option<Instant>,
    rate_window: usize,
    pub(crate) selected_table: usize,
    /// The table list's scroll position, kept between frames so moving up does not jerk the view.
    pub(crate) table_offset: Cell<usize>,
    /// Rows the feed panel had room for when last drawn, which sizes the next feed query.
    pub(crate) feed_limit: Cell<usize>,
    pub(crate) refresh_time: Option<Duration>,
    pub(crate) last_refresh: Option<Instant>,
    pub(crate) poll_in_flight: bool,
    /// Set when the URL given was a runtime's root; `url` is then the mounted nest being shown.
    pub(crate) runtime: Option<Runtime>,
    pub(crate) decimals: BTreeMap<String, u32>,
    /// Narrows the table list to names containing it, case-insensitively.
    pub(crate) filter: String,
    /// Keys are going into the filter rather than driving the dashboard.
    pub(crate) filtering: bool,
    pub(crate) should_quit: bool,
}

impl App {
    pub(crate) fn new(url: String) -> Self {
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
            hot_rows: None,
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
            decimals: BTreeMap::new(),
            filter: String::new(),
            filtering: false,
            should_quit: false,
        }
    }

    pub(crate) fn refresh_due(&self) -> bool {
        !self.poll_in_flight
            && self
                .last_refresh
                .is_none_or(|at| at.elapsed() >= self.poll_interval())
    }

    pub(crate) fn poll_request(&self) -> PollRequest {
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

    pub(crate) fn apply(&mut self, result: PollResult) {
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
        self.hot_rows = match result.hot_rows {
            Ok(rows) => rows,
            Err(error) => {
                problems.push(("/", error));
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

    pub(crate) fn apply_selection(&mut self, base: &str, result: Result<Selection, String>) {
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

    pub(crate) fn nest_poll_interval(&self) -> Option<Duration> {
        self.ready
            .as_ref()?
            .freshness
            .as_ref()?
            .poll_interval_secs
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs)
    }

    pub(crate) fn poll_interval(&self) -> Duration {
        self.interval_override.unwrap_or_else(|| {
            self.nest_poll_interval()
                .map_or(DEFAULT_POLL_INTERVAL, |interval| {
                    interval.clamp(MIN_POLL_INTERVAL, MAX_POLL_INTERVAL)
                })
        })
    }

    pub(crate) fn activity_width(&self) -> Duration {
        self.nest_poll_interval()
            .unwrap_or_default()
            .max(self.poll_interval())
    }

    /// Moves the selection, clamped to the list, and returns the table to query if it changed and
    /// the nest allows asking.
    pub(crate) fn select(&mut self, index: usize) -> Option<SelectionQuery> {
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
        self.hot_rows = None;
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
    /// Indices into the full table list of the tables the filter lets through, grouped under
    /// their headings in the order each heading first appears.
    pub(crate) fn visible_tables(&self) -> Vec<usize> {
        let filter = self.filter.to_lowercase();
        let tables = self
            .identity
            .as_ref()
            .map(|identity| identity.tables.tables.as_slice())
            .unwrap_or_default();
        let mut groups: Vec<&str> = Vec::new();
        for table in tables {
            if !groups.contains(&table.group()) {
                groups.push(table.group());
            }
        }
        let mut visible: Vec<usize> = (0..tables.len())
            .filter(|index| tables[*index].table.to_lowercase().contains(&filter))
            .collect();
        visible.sort_by_key(|index| {
            groups
                .iter()
                .position(|group| *group == tables[*index].group())
        });
        visible
    }

    pub(crate) fn visible_position(&self) -> Option<usize> {
        self.visible_tables()
            .iter()
            .position(|index| *index == self.selected_table)
    }

    fn select_visible(&mut self, position: usize) -> Option<SelectionQuery> {
        let visible = self.visible_tables();
        let index = *visible.get(position.min(visible.len().checked_sub(1)?))?;
        self.select(index)
    }

    pub(crate) fn select_next(&mut self) -> Option<SelectionQuery> {
        let count = self.visible_tables().len().max(1);
        let next = self
            .visible_position()
            .map_or(0, |position| (position + 1) % count);
        self.select_visible(next)
    }

    pub(crate) fn select_previous(&mut self) -> Option<SelectionQuery> {
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
    pub(crate) fn handle_key(&mut self, key: KeyEvent) -> Option<SelectionQuery> {
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

    pub(crate) fn selected_table_name(&self) -> Option<&str> {
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
    pub(crate) fn sync(&self) -> (f64, String) {
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
                "{} · {} behind",
                count_blocks(lag),
                format_span(Duration::from_secs_f64(lag as f64 / rate))
            ),
            None => format!("{} behind", count_blocks(lag)),
        };
        ((step / lag as f64).min(1.0), label)
    }

    pub(crate) fn backfill(&self) -> Option<Backfill> {
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
    pub(crate) fn state(&self) -> (String, Color) {
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

    pub(crate) fn status(&self) -> String {
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

    pub(crate) fn rate(&self, field: impl Fn(Sample) -> Option<u64>) -> f64 {
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
    pub(crate) fn cpu_percent(&self) -> Option<Option<f64>> {
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

    pub(crate) fn rate_window_label(&self) -> String {
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
