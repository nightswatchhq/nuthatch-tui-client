use std::{
    collections::BTreeMap,
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, Instant},
};

use anyhow::Result;
use reqwest::blocking::Client;

use crate::api::*;

/// A poll names the selected table rather than indexing it, so a catalogue refetched after a
/// restart keeps the operator's place in it.
pub(crate) struct PollRequest {
    /// The nest this poll is for. A reply for a nest the operator has since left is dropped.
    pub(crate) base: String,
    /// A runtime's root, whose roster is refreshed with every poll so another nest's quarantine shows.
    pub(crate) roster: Option<String>,
    pub(crate) identity: bool,
    pub(crate) selection: Option<SelectionQuery>,
    pub(crate) sql_open: bool,
    pub(crate) feed_limit: usize,
}

pub(crate) struct PollResult {
    pub(crate) base: String,
    /// Set when `base` turned out to be a runtime's root rather than a nest.
    pub(crate) discovered: Option<Roster>,
    pub(crate) roster: Option<Result<Roster, String>>,
    pub(crate) identity: Option<Result<Identity, Problem>>,
    pub(crate) ready: Result<Ready, String>,
    pub(crate) metrics: Result<BTreeMap<String, f64>, String>,
    pub(crate) hot_rows: Result<Option<u64>, String>,
    pub(crate) table: Option<String>,
    pub(crate) selection: Option<Result<Selection, String>>,
    pub(crate) elapsed: Duration,
}

pub(crate) enum Request {
    Poll(PollRequest),
    Selection(String, SelectionQuery),
}

pub(crate) enum Reply {
    Poll(Box<PollResult>),
    Selection(String, Result<Selection, String>),
}

pub(crate) fn poll(client: &Client, request: &PollRequest) -> PollResult {
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
            hot_rows: Err("a runtime root".into()),
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
    let hot_rows =
        fetch_json::<RootDocument>(client, &format!("{base}/"), &[]).map(|root| root.entities);
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
        hot_rows,
        elapsed: started.elapsed(),
    }
}

/// Requests run here so that a slow `/sql` delays the numbers rather than the keyboard.
pub(crate) fn spawn_worker(client: Client) -> (Sender<Request>, Receiver<Reply>) {
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
