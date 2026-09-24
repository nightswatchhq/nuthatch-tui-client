use std::{collections::BTreeMap, time::Duration};

use ratatui::{
    prelude::*,
    widgets::{
        Block, BorderType, Borders, Gauge, List, ListItem, ListState, Padding, Paragraph,
        Sparkline, Wrap,
    },
};
use serde_json::Value;

use crate::{api::*, app::*, format::*};

/// Labelled metric lines in the performance panel. The panel is laid out at exactly this height so
/// that none of them is silently cropped; raise it with the panel.
const PERFORMANCE_LINES: u16 = 10;
pub(crate) const CANVAS: Color = Color::Rgb(11, 14, 20);

/// The feed as a small table: column names once in a header, then a line per row, taking columns
/// in declared order while they fit. A nest that published no column list gets the first row's
/// own keys, less the implicit ones and the big-integer companions.
pub(crate) fn feed_lines(
    rows: &[Value],
    table: &str,
    columns: &[String],
    decimals: &BTreeMap<String, u32>,
    width: usize,
) -> Vec<String> {
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
        let scale = decimals
            .get(&format!("{table}.{name}"))
            .or_else(|| decimals.get(name))
            .copied();
        let cells: Vec<String> = rows
            .iter()
            .map(|row| match (row.get(name), scale) {
                (Some(Value::String(word)), scale) if name == "result" && is_abi_word(word) => {
                    let value = word_to_decimal(word);
                    scale.map_or_else(
                        || format_decimal(&value),
                        |scale| format_scaled(&value, scale),
                    )
                }
                (Some(Value::String(text)), Some(scale)) if is_decimal(text) => {
                    format_scaled(text, scale)
                }
                (Some(value), _) => format_value(value),
                (None, _) => "—".into(),
            })
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

pub(crate) fn panel<'a>(title: &str) -> Block<'a> {
    Block::default()
        .title(Line::from(format!(" {title} ")).style(Style::default().fg(Color::Cyan).bold()))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray))
        .padding(Padding::horizontal(1))
}

/// NO_COLOR (no-color.org): drop every colour after drawing, and turn each filled background into
/// reverse video so the selection, the key badges and the gauge stay visible without one.
pub(crate) fn strip_colour(buffer: &mut Buffer) {
    for cell in &mut buffer.content {
        if cell.bg != Color::Reset && cell.bg != CANVAS {
            cell.modifier.insert(Modifier::REVERSED);
        }
        cell.fg = Color::Reset;
        cell.bg = Color::Reset;
    }
}

pub(crate) fn draw(frame: &mut Frame, app: &App) {
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
            "Hot rows        {}",
            app.hot_rows
                .map_or_else(|| "unavailable".into(), group_digits)
        )),
        Line::from(format!(
            "Sealed rows     {}",
            lifetime("nuthatch_rows_sealed_total")
                .map_or_else(|| "unavailable".into(), group_digits)
        )),
        Line::from(format!(
            "Lag             {}",
            ready.lag_blocks.map_or_else(|| "—".into(), count_blocks)
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
    let mut rows: Vec<ListItem> = Vec::new();
    let mut selected_row = None;
    for (position, index) in visible.iter().enumerate() {
        let group = tables[*index].group();
        if position == 0 || tables[visible[position - 1]].group() != group {
            let count = visible
                .iter()
                .filter(|index| tables[**index].group() == group)
                .count();
            rows.push(
                ListItem::new(format!("{group} ({count})"))
                    .style(Style::default().fg(Color::Gray).bold()),
            );
        }
        if *index == app.selected_table {
            selected_row = Some(rows.len());
        }
        rows.push(
            ListItem::new(format!("  {}", tables[*index].short_name()))
                .style(Style::default().fg(Color::White)),
        );
    }
    let position = app.visible_position();
    let mut list_state = ListState::default()
        .with_offset(app.table_offset.get())
        .with_selected(selected_row);
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
        .block(panel(&match identity
            .and_then(|identity| identity.tables.tables.get(app.selected_table))
            .filter(|table| table.is_call())
        {
            Some(table) => format!(
                "SELECTED TABLE  eth_call {}",
                table.selector.as_deref().unwrap_or("")
            ),
            None => "SELECTED TABLE".to_owned(),
        }))
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
                    &selection.table,
                    &selection.columns,
                    &app.decimals,
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
