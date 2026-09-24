mod api;
mod app;
mod config;
mod format;
mod picker;
#[cfg(test)]
mod tests;
mod tunnel;
mod ui;
mod worker;

use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use crossterm::{
    cursor::Show,
    event::{self, Event, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::prelude::*;
use reqwest::blocking::Client;

use crate::{app::*, config::*, picker::*, tunnel::*, ui::*, worker::*};

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
    app.decimals = target.decimals;
    app.no_color = std::env::var_os("NO_COLOR").is_some_and(|value| !value.is_empty());

    let _screen = Screen::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    run(&mut terminal, client, &mut app, tunnel, &quit)
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
