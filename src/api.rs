use std::collections::BTreeMap;

use anyhow::Result;
use reqwest::{StatusCode, blocking::Client};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct Ready {
    #[serde(default)]
    pub(crate) ready: bool,
    #[serde(default)]
    pub(crate) stalled: bool,
    #[serde(default)]
    pub(crate) wedged: bool,
    #[serde(default)]
    pub(crate) initial_poll_failed: bool,
    #[serde(default)]
    pub(crate) seal_direct_stalled: bool,
    #[serde(default)]
    pub(crate) entities_stalled: bool,
    #[serde(default)]
    pub(crate) quarantined: bool,
    /// Null for a cursorless role, which has no tip to lag behind. Zero would claim "at tip".
    pub(crate) tip: Option<u64>,
    pub(crate) lag_blocks: Option<u64>,
    #[serde(default)]
    pub(crate) last_block: u64,
    #[serde(default)]
    pub(crate) sealed_through: u64,
    #[serde(default)]
    pub(crate) seconds_since_poll: u64,
    pub(crate) freshness: Option<Freshness>,
    #[serde(default)]
    pub(crate) seal_direct_active: bool,
    pub(crate) seal_direct_origin: Option<u64>,
    pub(crate) seal_direct_completed: Option<u64>,
    pub(crate) seal_direct_target: Option<u64>,
    /// Published from Nuthatch 3.9.0.
    pub(crate) version: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct Freshness {
    pub(crate) poll_interval_secs: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct Tables {
    #[serde(default)]
    pub(crate) count: usize,
    #[serde(default)]
    pub(crate) tables: Vec<EventTable>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct EventTable {
    pub(crate) table: String,
    #[serde(default)]
    pub(crate) columns: Vec<Column>,
    #[serde(default)]
    pub(crate) alias: String,
    /// `call` for a table of `eth_call` results rather than decoded events.
    #[serde(default)]
    pub(crate) kind: String,
    pub(crate) selector: Option<String>,
}

impl EventTable {
    pub(crate) fn is_call(&self) -> bool {
        self.kind == "call"
    }

    /// The heading the table is listed under. State calls each have an alias of their own, so they
    /// go together under one heading rather than a heading apiece.
    pub(crate) fn group(&self) -> &str {
        if self.is_call() {
            "calls"
        } else if !self.alias.is_empty() {
            &self.alias
        } else {
            self.table.split_once("__").map_or("", |(alias, _)| alias)
        }
    }

    /// The name as listed under its heading, which already says the alias.
    pub(crate) fn short_name(&self) -> &str {
        self.table
            .strip_prefix(self.group())
            .and_then(|rest| rest.strip_prefix("__"))
            .unwrap_or(&self.table)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Column {
    name: String,
    #[serde(default)]
    sol_type: String,
}

/// Columns every row carries that say where it came from rather than what happened.
pub(crate) const IMPLICIT_COLUMNS: [&str; 7] = [
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
pub(crate) struct SelectionQuery {
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
    has_log_index: bool,
    limit: usize,
}

impl SelectionQuery {
    pub(crate) fn new(table: &EventTable, limit: usize) -> Self {
        let mut columns: Vec<String> = table
            .columns
            .iter()
            .filter(|column| column.sol_type != "implicit")
            .map(|column| column.name.clone())
            .collect();
        if table.is_call() {
            // What the call returned is the point; the calldata is the same on every row.
            columns.sort_by_key(|name| name != "result");
        }
        Self {
            table: table.table.clone(),
            columns,
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

    pub(crate) fn events_sql(&self) -> String {
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
pub(crate) struct RootDocument {
    #[serde(default)]
    name: String,
    /// Rows in the nest's hot store: the `ENTITIES` table of its redb, not sealed history.
    pub(crate) entities: Option<u64>,
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
pub(crate) enum SqlAccess {
    Open,
    Closed { mode: String, named: Vec<String> },
}

/// What the nest is, as opposed to how it is doing. Nuthatch builds all of it at startup and never
/// changes it, so it is fetched once and again only after a restart.
pub(crate) struct Identity {
    pub(crate) nest_name: Option<String>,
    pub(crate) chain: Option<String>,
    pub(crate) tables: Tables,
    pub(crate) sql: SqlAccess,
}

#[derive(Default)]
pub(crate) struct Selection {
    pub(crate) table: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Option<u64>,
    pub(crate) latest_block: Option<u64>,
    pub(crate) events: Vec<Value>,
    pub(crate) degraded: bool,
}

pub(crate) type Problem = (&'static str, String);

/// `GET /nests` on a runtime: the nests it mounts, each serving its own API under `base_path`.
#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct Roster {
    #[serde(default)]
    pub(crate) runtime: String,
    pub(crate) nests: Vec<RosterNest>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct RosterNest {
    name: String,
    #[serde(default)]
    base_path: String,
    #[serde(default)]
    pub(crate) health: String,
}

impl RosterNest {
    pub(crate) fn path(&self) -> String {
        if self.base_path.is_empty() {
            format!("/{}", self.name)
        } else {
            self.base_path.clone()
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

pub(crate) fn fetch_ok(
    client: &Client,
    url: &str,
    query: &[(&str, &str)],
) -> Result<String, String> {
    let (status, body) = fetch(client, url, query)?;
    if status.is_success() {
        Ok(body)
    } else {
        Err(format!("HTTP {status}"))
    }
}

pub(crate) fn fetch_json<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    query: &[(&str, &str)],
) -> Result<T, String> {
    serde_json::from_str(&fetch_ok(client, url, query)?)
        .map_err(|_| "unreadable response".to_owned())
}

/// A stalled or quarantined nest answers 503 with the full body, and that body is exactly what
/// the operator needs to see. Treating the status as a failure hid every unhealthy state.
pub(crate) fn fetch_ready(client: &Client, base: &str) -> Result<Ready, String> {
    let (status, body) = fetch(client, &format!("{base}/ready"), &[])?;
    if !status.is_success() && status != StatusCode::SERVICE_UNAVAILABLE {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_str(&body).map_err(|_| "unreadable response".to_owned())
}

pub(crate) fn fetch_identity(client: &Client, base: &str) -> Result<Identity, Problem> {
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

pub(crate) fn fetch_selection(
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

pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) fn nest_name_from_schema(schema: &str) -> Option<String> {
    let line = schema.lines().find(|line| line.starts_with("The `"))?;
    let rest = line.strip_prefix("The `")?;
    let (name, _) = rest.split_once("` nest on ")?;
    (!name.is_empty()).then(|| name.to_owned())
}

/// Labelled series of one family are summed, as RPC methods are across endpoints. A family that
/// also publishes an unlabelled series is giving its total there and only breaking it down in the
/// labelled ones, so adding the two would count everything twice.
pub(crate) fn parse_prometheus(text: &str) -> BTreeMap<String, f64> {
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

pub(crate) fn metric_u64(metrics: &BTreeMap<String, f64>, name: &str) -> u64 {
    metric_opt_u64(metrics, name).unwrap_or_default()
}

pub(crate) fn metric_opt_u64(metrics: &BTreeMap<String, f64>, name: &str) -> Option<u64> {
    metrics.get(name).copied().map(|value| value as u64)
}
