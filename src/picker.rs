use std::{
    collections::BTreeMap,
    io::{self},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::{
    prelude::*,
    widgets::{Block, List, ListItem, ListState},
};

use crate::{Screen, config::*, ui::*};

/// The list shown when `nests.toml` names nests and the command line names none.
pub(crate) struct Picker {
    names: Vec<String>,
    lines: Vec<String>,
    selected: usize,
}

pub(crate) enum Picked {
    Nest(String),
    Quit,
}

impl Picker {
    pub(crate) fn new(nests: &BTreeMap<String, NestTarget>) -> Self {
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

    pub(crate) fn handle_key(&mut self, key: KeyEvent) -> Option<Picked> {
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

    pub(crate) fn draw(&self, frame: &mut Frame, no_color: bool) {
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
pub(crate) fn pick_nest(
    nests: &BTreeMap<String, NestTarget>,
    quit: &AtomicBool,
) -> Result<Option<String>> {
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
