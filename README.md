# Nuthatch TUI Client

[![CI](https://github.com/nightswatchhq/nuthatch-tui-client/actions/workflows/ci.yml/badge.svg)](https://github.com/nightswatchhq/nuthatch-tui-client/actions/workflows/ci.yml)

A fast, read-only terminal dashboard for a running [Nuthatch](https://github.com/nightswatchhq/nuthatch) nest.

`nuthatch-tui-client` turns the Nuthatch HTTP API into an operator view: whether the nest is live, how far it is behind, which data it has collected, what has been sealed, and how many outbound RPC requests the indexer has made since it started.

It is a client, not an indexer. It does not need an RPC key, open a store, alter nest configuration, or write data. Point it at an already-running Nuthatch API and it observes.

```text
 NUTHATCH   ● LIVE   demo-usdc  mainnet  v3.10.0   http://127.0.0.1:18288
────────────────────────────────────────────────────────────────────────────────────────────────────
╭ NEST HEALTH ───────────────────╮╭ DATA COLLECTED ───────────────╮╭ SYNC POSITION ────────────────╮
│ STATUS  READY                  ││ Tables          17            ││ █████████████████████████████ │
│ Tip             26,048,984     ││ Decoded rows    7,100         ││ █████████████████████████████ │
│ Indexed         26,048,984     ││ Sealed rows     0             ││ ███████████at tip ███████████ │
│ Sealed          26,048,764     ││ Lag             0 blocks      ││ █████████████████████████████ │
│ Seal gap        220            ││ Restarts        none seen     ││ █████████████████████████████ │
╰────────────────────────────────╯╰───────────────────────────────╯╰───────────────────────────────╯
╭ INDEXED TABLES  1/17  ↑↓ j k ──────╮╭ PERFORMANCE  rates warming 10/60s  (w) ────────────────────╮
│ usdc__approval                     ││ RPC REQUESTS  495 since start  0.8 req/s  48 req/min       │
│ usdc__authorization_canceled       ││ RPC METHODS     495 since start  0.8 calls/s               │
│ usdc__authorization_used           ││ DECODED ROWS    7,100 since start  0 rows/s                │
│ usdc__blacklisted                  ││ INDEXED BLOCKS  0 blocks/s  0 blocks/min                   │
│ usdc__blacklister_changed          ││ MEMORY RSS      149.2 MiB                                  │
│ usdc__burn                         ││ API REFRESH     16 ms  poll 2s ago, every 2s               │
│ usdc__master_minter_changed        ││ REORGS  0 since start   CPU  0.8%                          │
│ usdc__mint                         ││ DISK            hot 32.6 MiB  sealed 1.9 MiB               │
│ usdc__minter_configured            ││ RPC HEALTH      fail 0  retry 0  latency 81 ms avg         │
│ usdc__minter_removed               │╰────────────────────────────────────────────────────────────╯
│ usdc__ownership_transferred        │╭ LIVE EVENT FEED ───────────────────────────────────────────╮
│ usdc__pause                        ││ block       owner        spender      value                │
│ usdc__pauser_changed               ││ 26,048,983  0xf083…d1d7  0x8787…a4e2  26,734,150           │
╰────────────────────────────────────╯│ 26,048,983  0xb5e1…b0e0  0x1111…2a65  8,317,550            │
╭ SELECTED TABLE ────────────────────╮│ 26,048,983  0xb92f…ff4f  0x4cd0…bc31  176,605,151          │
│ usdc__approval                     │╰────────────────────────────────────────────────────────────╯
│ Rows    7,890                      │╭ RPC / 2s  peak 2 ───────────╮╭ REFRESH / 2s  peak 79 ms ───╮
│ Latest  26,048,983                 ││  ███ █                      ││ █                           │
│ Storage integrity: healthy         ││  ███ █                      ││ █▁▁▁▃▃                      │
╰────────────────────────────────────╯╰─────────────────────────────╯╰─────────────────────────────╯
 q  quit    r  refresh    ↑↓  tables    w  rate window   Live data received
```

## What it shows

| Panel | What it answers |
|---|---|
| **Nest health** | Is the indexer ready? What are the tip, indexed, and sealed block heights, and how far does the seal trail the index? During a seal-direct backfill, where the pass started. |
| **Data collected** | How many event tables exist, rows decoded, rows sealed, blocks of lag, and how many Nuthatch restarts the client has seen while watching. |
| **Sync position** | How far the cursor trails the tip, in blocks and in time. The fill measures that lag against the least the nest can be expected to trail by, one poll interval's worth of blocks or one block, whichever is more: full within it, half at twice it. During a backfill, the pass's progress instead. |
| **Indexed tables** | The event tables exposed by the nest's schema, scrolled to keep the selection in view, with its position in the title. |
| **Performance** | Rolling RPC request, RPC method, decoded-row, and indexed-block rates; lifetime counters; API refresh time; source-poll age; RSS; CPU utilisation; hot-store and sealed-segment disk footprint; RPC endpoint failures, retries, and average latency; and reorg count. The panel title carries the active rolling window; press `w` to choose 15, 60, or 90 seconds. |
| **Selected table** | Row count and latest block for the selected event table, or a note that the nest has closed free-form SQL. |
| **Live event feed** | The newest decoded rows for the selected table, as many as the panel has room for, as a table in the schema's column order. Columns are taken while they fit; addresses are shortened in the middle, and integers past fifteen digits turn scientific (`1.15e77`). Shows the nest's named queries instead when SQL is closed. Yields its space to the performance panel on a short terminal. |
| **RPC / API refresh** | Two sparklines: RPC requests and peak API refresh time, one bar per nest poll interval. Each title carries its own peak, since each bar is scaled to it. |

The header leads with the nest's state. `● LIVE`, `● ATTENTION`, `● BACKFILL` and `● QUARANTINED` come from `/ready`, including the 503 body a stalled nest answers with. `(partial)` is added when the nest answered but some endpoint behind a panel did not; the footer names which. `● STALE` means `/ready` itself stopped answering and every number on screen is the last one received.

The RPC counter is Nuthatch's own `nuthatch_rpc_requests_total` metric. It resets when the indexer restarts and measures JSON-RPC requests, not provider billing units. Alchemy and other providers use their own compute-unit accounting, so their dashboard remains the authority for spend.

## Requirements

- Rust 1.88 or newer. Edition 2024 alone would settle for 1.85, but the locked dependency tree does not: `darling`, `instability` and the `icu_*` crates each want 1.88. CI builds on the declared floor so that sentence stays true.
- A running Nuthatch API, normally started with `nuthatch dev`.
- A terminal with colour and Unicode support. 100 columns by 30 rows shows every panel at once; see [Terminal size](#terminal-size) for what gives way below that.

The default target is `http://127.0.0.1:8288`, Nuthatch's default local listener.

## Install and run

From a checkout:

```sh
git clone git@github.com:nightswatchhq/nuthatch-tui-client.git
cd nuthatch-tui-client
cargo run
```

Point it at a different listener with `--url`:

```sh
cargo run -- --url http://127.0.0.1:18288
```

The dashboard polls as often as the nest does, taken from `freshness.poll_interval_secs` on `/ready` and held between 2 and 30 seconds, so a nest with a five-minute cursor is not asked 150 times per update. `--interval 10s` (or `2m`) overrides that. A Nuthatch older than 3.5 does not publish the interval, and gets 2 seconds.

Set `NO_COLOR` to drop every colour. The selection, key badges and gauge switch to reverse video so that they remain visible.

### Remote nests

A nest on a server is normally bound to its loopback. `--ssh` opens the forward itself, with `--url` giving the nest's address as seen from that host:

```sh
cargo run -- --ssh 89.167.109.4 --url http://127.0.0.1:8107
```

The client runs `ssh -N -L` with `BatchMode`, so the host needs key or agent authentication; a password prompt would land in the middle of the dashboard, and a refused key is reported before it opens. If ssh exits mid-session, the footer says why and the forward is reopened after 1, 2, 4 and up to 30 seconds. The ssh process goes when the client does, including on `SIGTERM`.

Nests used often can be named in `~/.config/nuthatch-tui/nests.toml` (or under `$XDG_CONFIG_HOME`):

```toml
[allocations]
url = "http://127.0.0.1:8107"
ssh = "89.167.109.4"

[local]
url = "http://127.0.0.1:18288"
```

and opened with `--nest allocations`. `--url` and `--ssh` given alongside it take precedence over the entry. Started with none of `--url`, `--ssh` or `--nest`, the client lists the configured nests and opens the one chosen with `Enter`.

### Runtimes

Pointed at the root of a runtime (`nuthatch dev` over a `mounts.toml`), the client finds the roster at `/nests` and opens the first mounted nest under its prefix. `n` and `N` step through the others; each switch starts the dashboard afresh, because counters and rates belong to one nest. The header names the runtime, which nest is showing, and how many of its nests are quarantined. A runtime's root `/ready` carries no heights, so it is never read as a nest's.

For an optimised build:

```sh
cargo run --release -- --url http://127.0.0.1:8288
```

## Controls

| Key | Action |
|---|---|
| `r` | Refresh immediately. |
| `Up` / `Down` or `k` / `j` | Select an event table and refresh its summary and event feed. |
| `PageUp` / `PageDown` | Move the selection ten tables. |
| `Home` / `End` or `g` / `G` | Jump to the first or last table. |
| `n` / `N` | On a runtime, show the next or previous mounted nest. |
| `/` | Filter the table list by name, case-insensitively. `Enter` keeps the filter, `Esc` drops it; while typing, letters are text rather than commands. |
| `w` | Cycle the rolling-rate window: 15, 60, or 90 seconds. |
| `q`, `Esc` or `Ctrl-C` | Exit cleanly. With a filter standing, the first `Esc` clears it instead. |

The terminal is restored on `SIGTERM`, `SIGHUP` and `SIGINT` as well as on a normal exit or a panic.

## A local USDC example

Create and run a small Nuthatch nest first:

```sh
nuthatch init 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 \
  --alias usdc --chain mainnet --dir demo-usdc --no-timestamps

nuthatch dev --dir demo-usdc --backfill 100 --listen 127.0.0.1:18288 --no-admin \
  --rpc https://your-mainnet-rpc.example
```

Then open the operator view:

```sh
cargo run -- --url http://127.0.0.1:18288
```

The RPC endpoint belongs to Nuthatch, not this client. Do not put a provider key in the TUI command or configuration because the TUI does not make chain RPC calls.

## HTTP contract

The client uses only public, read-only Nuthatch endpoints:

| Endpoint | Use |
|---|---|
| `GET /ready` | Every refresh. Readiness, head positions, lag, stall state, poll interval, backfill, and version. A 503 is read, not discarded: it is how a stalled nest answers. |
| `GET /metrics` | Every refresh. Prometheus counters and gauges for rows, RPC activity, process RSS, reorgs, and positions. |
| `GET /sql?q=…` | Every refresh, and on changing the selection. A count, and the newest rows for the selected table by its decoded columns, all quoted as identifiers. Naming the columns leaves out the `_dec` and `_overflow` companions Nuthatch adds to every big integer for arithmetic; the plain column already holds the exact decimal text. Skipped entirely on a nest whose `/queries` says SQL is closed. |
| `GET /tables`, `GET /nest`, `GET /queries` | Once, and again after a restart. The table catalogue, the nest's name and chain, and whether free-form SQL is open. Nuthatch builds these at startup and never changes them. Fetching them once took a refresh on the 17-table USDC demo from 38 KB to 18 KB; on an 81-table nest, `/tables` and `/schema` were 139 KB of every refresh. |
| `GET /schema`, `GET /` | Only when `/nest` is not served, to find the nest name. |
| `GET /nests` | Only on a runtime's root, found when the root has no `/tables`: the mounted nests and their health, refreshed with each poll. |

Requests are made on a thread of their own, so a slow nest delays the numbers and never the keyboard. Selections made while a query is running are collapsed into one query for the last of them.

It makes no HTTP mutation request and never touches the nest's redb or Parquet files. It can watch a nest on another machine through `--ssh`, or directly if the operator has deliberately exposed the API behind their normal network controls.

## Performance measurements

The performance panel separates values reported since the Nuthatch process started from rolling rates calculated locally from successive `/metrics` samples. Process-lifetime counters reset when Nuthatch restarts; rolling rates naturally settle again after the selected window. The client labels the two forms separately.

| Measurement | Source | Scope |
|---|---|---|
| RPC requests | `nuthatch_rpc_requests_total` | Outbound JSON-RPC HTTP request or batch envelopes since process start. Includes failover retries. |
| RPC methods | `nuthatch_rpc_methods_total` | Sum of labelled method-counter series since process start. A batch can contain many methods. |
| Decoded rows and reorgs | `nuthatch_rows_decoded_total`, `nuthatch_reorgs_total` | Process-lifetime counters. Past a million they are shortened to `12.3M` so that the line keeps its tail. |
| Indexed blocks | `last_block` from `/ready` | Difference over the selected rolling window, not a process counter. |
| Resident memory | `nuthatch_rss_bytes` | Current process RSS as reported by Nuthatch. Shown as `unavailable` when that metric is absent. |
| CPU utilisation | `nuthatch_process_cpu_seconds_total` | A cumulative CPU-seconds counter; the client derives a rolling percentage over the selected window, distinct from the lifetime counters above. Shown as `unavailable` on a Nuthatch that does not publish the series, `warming up` before the first full window of samples. Nuthatch before 3.0.0 read `/proc/self/stat` and nothing else ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)), so such a nest hosted off Linux publishes the counter pinned at 0.0 and the client shows a flat `0.0%`. From 3.9.0 the header shows the nest's version, which settles the question at a glance. |
| Disk footprint | `nuthatch_hot_store_bytes`, `nuthatch_sealed_segments_bytes` | Current on-disk bytes of the mutable hot store and sealed Parquet segments, summed across mounted nests. Shown as `unavailable` when the metric is absent, and as `0 B` when it is present and genuinely zero, which is what a nest that has sealed nothing yet reports. |
| RPC endpoint health | `nuthatch_rpc_endpoint_failures_total`, `nuthatch_rpc_endpoint_retries_total`, `nuthatch_rpc_request_duration_seconds_{sum,count}` | Failure and retry counts, and average round-trip latency, summed across every configured RPC endpoint the same way this client already aggregates labelled series like RPC methods. Latency reads `no calls yet` if the histogram is present but empty, `unavailable` if Nuthatch does not publish it. |
| API refresh and source-poll age | Client timing and `/ready` | Current client request time, and time since Nuthatch's last successful source poll beside the interval it polls at, so `63s ago` on a five-minute cursor does not read as a fault. |
| Restarts | Counters going backwards | Nuthatch publishes no start time. The client counts a restart when a lifetime counter falls between two samples, clears its rate history across the boundary, fetches the catalogue again, and shows the count and the time since the last one. |

The dashboard degrades each of these independently rather than displaying a misleading zero: a metric a given Nuthatch version does not publish reads `unavailable`, not `0`. The converse holds too, and matters just as much: a metric that is present and genuinely zero reads as a zero, because a nest that has sealed nothing really does occupy no bytes. It does not inspect the local process, filesystem, or RPC provider to fill gaps itself, because that would make remote operation and the read-only boundary rather less clear than advertised — every number here comes from Nuthatch's own `/metrics`.

## Current limits

- The client is a dashboard, not a general SQL workbench. It presents a summary and a live feed for the selected event table.
- The sync gauge needs the tip to move between two samples before it knows the chain's block rate. Until then it treats one block as the expected lag.
- It reports request count, not exact provider cost. Billing models differ by provider and method.
- On Nuthatch before 3.0.0, CPU utilisation is only accurate when the nest itself is Linux-hosted; a Mac-hosted nest reports a flat `0.0%` rather than `unavailable` ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)).
- A restart is only seen if it happens while the client is watching, and only if the counters have not climbed past their old values by the next sample. A restart before the client started is invisible to it.
- `--ssh` needs non-interactive authentication to the host.
- The screen wants 100 columns by 30 rows for everything at once. It stays usable smaller, in the order set out under [Terminal size](#terminal-size), but the performance panel's longest lines truncate below 100 columns.

## Terminal size

The performance panel is the widest thing on the screen and the one that must not lie, so the layout
is arranged around it. It occupies the right-hand column whole, at a height fixed to its line count,
which means nothing it shows can be cropped by a neighbouring widget.

| Size | What you get |
|---|---|
| 100x30 or larger | Every panel, including the live event feed. At 33 rows the two activity sparklines join them. |
| Shorter than 30 rows | The live event feed gives way first, so the metric lines stay whole. A missing panel is visibly missing; a cropped metric line just reads as a smaller number. |
| Narrower than 100 columns | The longest performance lines start to truncate on the right. The panel is laid out to fit its widest line at 100 columns, and `cargo test` asserts that. |

Lifetime counters are shortened past a million (`912.3M`), and the per-minute decoded-row rate is
gone, so the widest lines fit at 100 columns at the counter sizes a long-running arbitrum nest
reaches. `cargo test` renders those sizes and asserts it.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

These are the three tasks `yatr ci` runs, and the three GitHub Actions runs on every push and pull
request. A second CI job builds on the MSRV declared in `Cargo.toml`.

The test suite covers Prometheus parsing, metric formatting, and the panel layout, the last by rendering the whole dashboard into a `TestBackend` at several terminal sizes and asserting that no metric line has been cropped. The HTTP contract is tested against a small in-process server answering with bodies trimmed from a live 3.10.0 nest: a stalled 503, a missing `/metrics`, closed SQL, a restart, and how often each endpoint is actually asked. The terminal lifecycle is not covered by tests.

To see the dashboard as drawn against a real nest, which is how the sample screen above was made:

```sh
NUTHATCH_URL=http://127.0.0.1:18288 cargo test live -- --ignored --nocapture
```

## Licence

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option, matching Nuthatch.
