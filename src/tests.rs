use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{backend::TestBackend, prelude::*};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::{api::*, app::*, config::*, format::*, picker::*, tunnel::*, ui::*, worker::*};

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
                    ..EventTable::default()
                },
                EventTable {
                    table: "usdc__transfer".into(),
                    ..EventTable::default()
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
        reversed("  approval"),
        "the selected table lost its highlight"
    );
    assert!(!reversed("  transfer"));
}

fn with_many_tables(count: usize) -> App {
    let mut app = populated();
    let identity = app.identity.as_mut().unwrap();
    identity.tables = Tables {
        count,
        tables: (0..count)
            .map(|index| EventTable {
                table: format!("graph__table_{index:03}"),
                ..EventTable::default()
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
                ..EventTable::default()
            })
            .collect(),
    };
    app
}

fn grouped_nest() -> App {
    let mut app = with_many_tables(0);
    let tables: Vec<EventTable> = serde_json::from_str(
        r#"[{"table":"curation__burned","alias":"curation"},
            {"table":"staking__stake_deposited","alias":"staking"},
            {"table":"total_supply","alias":"total_supply","kind":"call","selector":"0x18160ddd"},
            {"table":"curation__signalled","alias":"curation"},
            {"table":"issuance_per_block","alias":"issuance_per_block","kind":"call","selector":"0x0c0b9f9c"}]"#,
    )
    .unwrap();
    app.identity.as_mut().unwrap().tables = Tables {
        count: tables.len(),
        tables,
    };
    app
}

#[test]
fn tables_are_listed_under_their_alias_and_calls_together() {
    let mut app = grouped_nest();
    assert_eq!(app.visible_tables(), [0, 3, 1, 2, 4]);
    let screen = render(&app, 100, 30);
    for expected in [
        "curation (2)",
        "  burned",
        "  signalled",
        "staking (1)",
        "calls (2)",
        "  total_supply",
    ] {
        assert!(screen.contains(expected), "{expected:?} missing:\n{screen}");
    }
    // Navigation walks the grouped order and never lands on a heading.
    let walked: Vec<String> = (0..5)
        .filter_map(|_| app.select_next().map(|query| query.table))
        .collect();
    assert_eq!(
        walked,
        [
            "curation__signalled",
            "staking__stake_deposited",
            "total_supply",
            "issuance_per_block",
            "curation__burned"
        ]
    );
    app.selected_table = 2;
    let screen = render(&app, 100, 30);
    assert!(
        screen.contains("SELECTED TABLE  eth_call 0x18160ddd"),
        "{screen}"
    );
    press(&mut app, "/supply\n");
    let screen = render(&app, 100, 30);
    assert!(
        screen.contains("calls (1)") && !screen.contains("curation (2)"),
        "{screen}"
    );
}

#[test]
fn a_heading_takes_the_alias_off_the_names_under_it() {
    let table = |json| serde_json::from_str::<EventTable>(json).unwrap();
    assert_eq!(
        table(r#"{"table":"subgraph_service__allocation_closed","alias":"subgraph_service"}"#)
            .short_name(),
        "allocation_closed"
    );
    assert_eq!(
        table(r#"{"table":"total_supply","alias":"total_supply","kind":"call"}"#).short_name(),
        "total_supply"
    );
    assert_eq!(table(r#"{"table":"graph__x"}"#).short_name(), "x");
}

#[test]
fn a_call_result_is_read_as_the_number_it_encodes() {
    let table: EventTable = serde_json::from_str(
        r#"{"table":"total_supply","kind":"call","columns":[
            {"name":"block_number","sol_type":"implicit"},{"name":"calldata","sol_type":"bytes"},
            {"name":"result","sol_type":"bytes"},{"name":"reverted","sol_type":"bool"}]}"#,
    )
    .unwrap();
    let query = SelectionQuery::new(&table, 6);
    assert_eq!(query.columns, ["result", "calldata", "reverted"]);
    assert_eq!(
        word_to_decimal("0x00000000000000000000000000000000000000000052b7d2dcc80cd2e4000000"),
        "100000000000000000000000000"
    );
    assert_eq!(word_to_decimal(&format!("0x{}", "0".repeat(64))), "0");
    assert_eq!(
        word_to_decimal(&format!("0x{}", "f".repeat(64))),
        "115792089237316195423570985008687907853269984665640564039457584007913129639935"
    );
    let rows: Vec<Value> = serde_json::from_str(
        r#"[{"block_number":508500000,"calldata":"0x18160ddd","reverted":false,
             "result":"0x00000000000000000000000000000000000000000052b7d2dcc80cd2e4000000"},
            {"block_number":508400000,"calldata":"0x18160ddd","reverted":true,"result":"0x"}]"#,
    )
    .unwrap();
    let decimals = BTreeMap::from([("total_supply.result".to_owned(), 18)]);
    let lines = feed_lines(&rows, "total_supply", &query.columns, &decimals, 58);
    assert_eq!(lines[0], "block        result       calldata    reverted");
    assert_eq!(lines[1], "508,500,000  100,000,000  0x18160ddd  false");
    assert_eq!(lines[2], "508,400,000  0x           0x18160ddd  true");
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
fn one_block_is_singular() {
    assert_eq!(count_blocks(1), "1 block");
    assert_eq!(count_blocks(0), "0 blocks");
    assert_eq!(count_blocks(331_434), "331,434 blocks");
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
        feed_lines(&rows, "usdc__transfer", &columns, &BTreeMap::new(), 58),
        [
            "block       from         to           value",
            "26,048,953  0x0000…8a90  0x4313…b27f  486,153,178",
            "26,048,952  0x9fad…43a9  0x4cd0…bc31  1.15e77",
        ]
    );
    assert_eq!(
        feed_lines(&rows, "usdc__transfer", &columns, &BTreeMap::new(), 30)[1],
        "26,048,953  0x0000…8a90"
    );
    // Without a column list the first row's own keys are used, less implicit ones and companions.
    assert_eq!(
        feed_lines(&rows, "usdc__transfer", &[], &BTreeMap::new(), 58),
        feed_lines(&rows, "usdc__transfer", &columns, &BTreeMap::new(), 58)
    );
    assert!(feed_lines(&[], "usdc__transfer", &columns, &BTreeMap::new(), 58).is_empty());
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
fn amounts_scale_by_declared_decimals() {
    assert_eq!(format_scaled("40700000000000000000", 18), "40.7");
    assert_eq!(format_scaled("2970000000000000000000", 18), "2,970");
    assert_eq!(
        format_scaled("486153178", 6),
        "486.1531",
        "truncated, not rounded"
    );
    assert_eq!(format_scaled("5", 6), "<0.0001");
    assert_eq!(format_scaled("0", 18), "0");
    assert_eq!(format_scaled("-1500000", 6), "-1.5");
    assert_eq!(format_scaled("1500000", 0), "1,500,000");
    assert_eq!(
        format_scaled(
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            6
        ),
        "1.15e71"
    );
}

#[test]
fn decimals_apply_by_table_and_column_or_by_column_alone() {
    let rows: Vec<Value> = serde_json::from_str(
        r#"[{"block_number":508518993,"curator":"0xec9a00000000000000000000000000000003bec","tokens":"40700000000000000000","signal":"12000000000000000000"}]"#,
    )
    .unwrap();
    let columns = ["curator", "tokens", "signal"].map(String::from);
    let decimals = BTreeMap::from([
        ("curation__burned.tokens".to_owned(), 18),
        ("signal".to_owned(), 18),
    ]);
    assert_eq!(
        feed_lines(&rows, "curation__burned", &columns, &decimals, 80)[1],
        "508,518,993  0xec9a…3bec  40.7    12"
    );
    assert_eq!(
        feed_lines(&rows, "curation__collected", &columns, &decimals, 80)[1],
        "508,518,993  0xec9a…3bec  4.07e19  12",
        "a table-qualified key applies to that table only"
    );
    let nests = parse_nests(
        "[allocations]\nurl = \"http://127.0.0.1:8107\"\n\n[allocations.decimals]\n\"curation__burned.tokens\" = 18\n",
    )
    .unwrap();
    assert_eq!(nests["allocations"].decimals["curation__burned.tokens"], 18);
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
            ..NestTarget::default()
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
            ..NestTarget::default()
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
/// `NUTHATCH_SSH=host` goes through a forward, as `--ssh` does, and `NUTHATCH_NEST=name` takes
/// everything from `nests.toml`, as `--nest` does. `NUTHATCH_TABLE` selects a table by name.
#[test]
#[ignore = "needs a running nest at NUTHATCH_URL or NUTHATCH_NEST"]
fn live() {
    let args = Args {
        url: std::env::var("NUTHATCH_URL").ok(),
        ssh: std::env::var("NUTHATCH_SSH").ok(),
        nest: std::env::var("NUTHATCH_NEST").ok(),
        interval: None,
    };
    let nests = config_path()
        .filter(|path| path.exists())
        .map(|path| parse_nests(&std::fs::read_to_string(path).unwrap()).unwrap())
        .unwrap_or_default();
    let target = resolve(&args, &nests).unwrap();
    let url = target.url.clone().unwrap();
    let tunnel = target
        .ssh
        .as_ref()
        .map(|host| Tunnel::open("ssh", host, &url, &AtomicBool::new(false)).expect("ssh forward"));
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let mut app = App::new(
        tunnel
            .as_ref()
            .map_or_else(|| url.clone(), |tunnel| tunnel.local_url.clone()),
    );
    app.decimals = target.decimals;
    let table = std::env::var("NUTHATCH_TABLE").ok();
    for _ in 0..6 {
        if let Some(index) = table.as_ref().and_then(|name| {
            app.identity
                .as_ref()?
                .tables
                .tables
                .iter()
                .position(|t| t.table == *name)
        }) {
            app.selected_table = index;
        }
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
const ROOT: &str =
    r#"{"name":"nuthatch","chain":"mainnet","entities":5705,"last_block":"26048483","tables":2}"#;
const TABLES: &str =
    r#"{"count":2,"tables":[{"table":"usdc__approval"},{"table":"usdc__transfer"}]}"#;
const NEST: &str = r#"{"chain":"mainnet","chain_id":1,"name":"demo-usdc","table_count":2}"#;
const QUERIES_OPEN: &str = r#"{"free_form":true,"queries":[],"sql":"open"}"#;
const COUNTS: &str = r#"{"rows":[{"rows":2275,"latest_block":26048483}],"degraded":false}"#;
const EVENTS: &str = r#"{"rows":[],"degraded":false}"#;

fn healthy(target: &str) -> (u16, String) {
    let path = target.split('?').next().unwrap_or(target);
    let body = match path {
        "/" => ROOT,
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
    assert_eq!(app.hot_rows, Some(5705));
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
    assert!(screen.contains("DECODED ROWS    unavailable"), "{screen}");
    assert!(
        screen.contains("Hot rows        5,705"),
        "the root document still answered:\n{screen}"
    );
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
            ..EventTable::default()
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
    let Reply::Selection(_, Ok(selection)) = replies.recv_timeout(Duration::from_secs(5)).unwrap()
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
            ..EventTable::default()
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
