# Nuthatch TUI Client

[![CI](https://github.com/nightswatchhq/nuthatch-tui-client/actions/workflows/ci.yml/badge.svg)](https://github.com/nightswatchhq/nuthatch-tui-client/actions/workflows/ci.yml)

A fast, read-only terminal dashboard for a running [Nuthatch](https://github.com/nightswatchhq/nuthatch) nest.

`nuthatch-tui-client` turns the Nuthatch HTTP API into an operator view: whether the nest is live, how far it is behind, which data it has collected, what has been sealed, and how many outbound RPC requests the indexer has made since it started.

It is a client, not an indexer. It does not need an RPC key, open a store, alter nest configuration, or write data. Point it at an already-running Nuthatch API and it observes.

```text
 NUTHATCH  LIVE VIEW   graph-staking-nest   http://127.0.0.1:8288   ● LIVE
────────────────────────────────────────────────────────────────────────────────────────────────────
╭ NEST HEALTH ───────────────────╮╭ DATA COLLECTED ───────────────╮╭ SYNC POSITION ────────────────╮
│ STATUS  READY                  ││ Tables          17            ││ █████████████████████████████ │
│ Tip             25766811       ││ Decoded rows    2275          ││ █████████████████████████████ │
│ Indexed         25766811       ││ Sealed rows     2453          ││ █████25766811 / 25766811 ████ │
│ Finalised       25766747       ││ Lag             0 blocks      ││ █████████████████████████████ │
│ Range           —              ││                               ││ █████████████████████████████ │
╰────────────────────────────────╯╰───────────────────────────────╯╰───────────────────────────────╯
╭ INDEXED TABLES  ↑↓ / j k to inspect╮╭ PERFORMANCE  rates last 60s  (w) ──────────────────────────╮
│ usdc__approval                     ││ RPC REQUESTS  367 since start  1.1 req/s  67 req/min       │
│ usdc__transfer                     ││ RPC METHODS     412 since start  1.2 calls/s               │
│                                    ││ DECODED ROWS    2275 since start  21 rows/s  1275 rows/min │
│                                    ││ INDEXED BLOCKS  1.2 blocks/s  70 blocks/min                │
│                                    ││ MEMORY RSS      61.0 MiB                                   │
│                                    ││ API REFRESH     12 ms  source poll age 1 s                 │
│                                    ││ REORGS  0 since start   CPU  3.5%                          │
│                                    ││ DISK            hot 2.0 MiB  sealed 47 KiB                 │
│                                    ││ RPC HEALTH      fail 69  retry 35  latency 69 ms avg       │
╰────────────────────────────────────╯╰────────────────────────────────────────────────────────────╯
╭ SELECTED TABLE ────────────────────╮╭ LIVE EVENT FEED ───────────────────────────────────────────╮
│ usdc__transfer                     ││ #25766811  from=0x2f8dfa1c9b3e5d7a04c…  value=1500000000   │
│ Rows    2275                       ││ #25766811  from=0x9a1c04b7e6f2d8135ac…  value=42000000     │
│ Latest  25766811                   ││ #25766810  from=0x71b3e0d95c4a2f68d17…  value=980000000    │
│ Storage integrity: healthy         ││ #25766810  from=0x0c4de71a83b95f26e40…  value=6000000      │
╰────────────────────────────────────╯╰────────────────────────────────────────────────────────────╯
 q  quit    r  refresh    ↑↓  tables    w  rate window   Live data received
```

## What it shows

| Panel | What it answers |
|---|---|
| **Nest health** | Is the indexer ready? What are the tip, indexed, and finalised block heights? |
| **Data collected** | How many event tables exist, rows decoded, rows sealed, and blocks of lag? |
| **Sync position** | How closely the committed cursor follows the chain tip. |
| **Indexed tables** | The event tables exposed by the nest's schema. |
| **Performance** | Rolling RPC request, RPC method, decoded-row, and indexed-block rates; lifetime counters; API refresh time; source-poll age; RSS; CPU utilisation; hot-store and sealed-segment disk footprint; RPC endpoint failures, retries, and average latency; and reorg count. The panel title carries the active rolling window; press `w` to choose 15, 60, or 90 seconds. |
| **Selected table** | Row count and latest block for the selected event table. |
| **Live event feed** | The six newest decoded rows for the selected table. Yields its space to the performance panel on a short terminal. |
| **RPC activity** | Recent changes in the RPC request counter, sampled once per dashboard refresh. |

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

For an optimised build:

```sh
cargo run --release -- --url http://127.0.0.1:8288
```

## Controls

| Key | Action |
|---|---|
| `r` | Refresh immediately. |
| `Up` / `Down` or `k` / `j` | Select an event table and refresh its summary and event feed. |
| `w` | Cycle the rolling-rate window: 15, 60, or 90 seconds. |
| `q` or `Esc` | Exit cleanly. |

The dashboard otherwise refreshes every two seconds.

## A local USDC example

Create and run a small Nuthatch nest first:

```sh
nuthatch init 0xA0b86991c6218b36c1D4a2e9Eb0cE3606eB48 \
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
| `GET /ready` | Readiness, head positions, lag, and stall state. |
| `GET /` and `GET /schema` | Runtime and authored nest identity. |
| `GET /metrics` | Prometheus counters and gauges for rows, RPC activity, process RSS, reorgs, and positions. |
| `GET /tables` | The event-table catalogue. |
| `GET /sql?q=…` | A small summary query for the first available event table. |

It makes no HTTP mutation request and never touches the nest's redb or Parquet files. This also means it can run from another machine if the Nuthatch API is intentionally exposed and protected by the operator's normal network controls.

## Performance measurements

The performance panel separates values reported since the Nuthatch process started from rolling rates calculated locally from successive `/metrics` samples. Process-lifetime counters reset when Nuthatch restarts; rolling rates naturally settle again after the selected window. The client labels the two forms separately.

| Measurement | Source | Scope |
|---|---|---|
| RPC requests | `nuthatch_rpc_requests_total` | Outbound JSON-RPC HTTP request or batch envelopes since process start. Includes failover retries. |
| RPC methods | `nuthatch_rpc_methods_total` | Sum of labelled method-counter series since process start. A batch can contain many methods. |
| Decoded rows and reorgs | `nuthatch_rows_decoded_total`, `nuthatch_reorgs_total` | Process-lifetime counters. |
| Indexed blocks | `last_block` from `/ready` | Difference over the selected rolling window, not a process counter. |
| Resident memory | `nuthatch_rss_bytes` | Current process RSS as reported by Nuthatch. Shown as `unavailable` when that metric is absent. |
| CPU utilisation | `nuthatch_process_cpu_seconds_total` | A cumulative CPU-seconds counter; the client derives a rolling percentage over the selected window, distinct from the lifetime counters above. Shown as `unavailable` on a Nuthatch that does not publish the series, `warming up` before the first full window of samples. On a nest running a **released** Nuthatch up to v2.7.2 and hosted off Linux, the sampler read `/proc/self/stat` and nothing else ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)), so the counter is published but pinned at 0.0 and the client shows a permanent, honestly-reported-but-misleading `0.0%` rather than `unavailable`. Fixed on Nuthatch `main` by a `ps -o time=` fallback; no tag carries it yet. |
| Disk footprint | `nuthatch_hot_store_bytes`, `nuthatch_sealed_segments_bytes` | Current on-disk bytes of the mutable hot store and sealed Parquet segments, summed across mounted nests. Shown as `unavailable` when the metric is absent, and as `0 B` when it is present and genuinely zero, which is what a nest that has sealed nothing yet reports. |
| RPC endpoint health | `nuthatch_rpc_endpoint_failures_total`, `nuthatch_rpc_endpoint_retries_total`, `nuthatch_rpc_request_duration_seconds_{sum,count}` | Failure and retry counts, and average round-trip latency, summed across every configured RPC endpoint the same way this client already aggregates labelled series like RPC methods. Latency reads `no calls yet` if the histogram is present but empty, `unavailable` if Nuthatch does not publish it. |
| API refresh and source-poll age | Client timing and `/ready` | Current client request time and seconds since Nuthatch's last successful source poll. |

The dashboard degrades each of these independently rather than displaying a misleading zero: a metric a given Nuthatch version does not publish reads `unavailable`, not `0`. The converse holds too, and matters just as much: a metric that is present and genuinely zero reads as a zero, because a nest that has sealed nothing really does occupy no bytes. It does not inspect the local process, filesystem, or RPC provider to fill gaps itself, because that would make remote operation and the read-only boundary rather less clear than advertised — every number here comes from Nuthatch's own `/metrics`.

## Current limits

- The client is a dashboard, not a general SQL workbench. It presents a summary and a six-row live feed for the selected event table.
- It reports request count, not exact provider cost. Billing models differ by provider and method.
- On any released Nuthatch up to v2.7.2, CPU utilisation is only accurate when the nest itself is Linux-hosted. That sampler had no macOS fallback, so a Mac-hosted nest reports a flat `0.0%` rather than `unavailable`. Fixed on Nuthatch `main`, unreleased ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)).
- A remote Nuthatch endpoint must be deliberately exposed by its operator. The default assumes a localhost service.
- The screen wants 100 columns by 30 rows for everything at once. It stays usable smaller, in the order set out under [Terminal size](#terminal-size), but the performance panel's longest lines truncate below 100 columns.

## Terminal size

The performance panel is the widest thing on the screen and the one that must not lie, so the layout
is arranged around it. It occupies the right-hand column whole, at a height fixed to its line count,
which means nothing it shows can be cropped by a neighbouring widget.

| Size | What you get |
|---|---|
| 100x30 or larger | Every panel, including the live event feed. At 33 rows the RPC activity sparkline joins them. |
| Shorter than 30 rows | The live event feed gives way first, so the metric lines stay whole. A missing panel is visibly missing; a cropped metric line just reads as a smaller number. |
| Narrower than 100 columns | The longest performance lines start to truncate on the right. The panel is laid out to fit its widest line at 100 columns, and `cargo test` asserts that. |

One honest limit: the widest line, decoded rows with both rates, fits its 100-column column exactly
with nothing to spare. A nest whose lifetime counters reach eight digits will push the tail of that
line off the right-hand edge until the terminal is wider.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
```

These are the three tasks `yatr ci` runs, and the three GitHub Actions runs on every push and pull
request. A second CI job builds on the MSRV declared in `Cargo.toml`.

The test suite covers Prometheus parsing, metric formatting, and the panel layout, the last by rendering the whole dashboard into a `TestBackend` at several terminal sizes and asserting that no metric line has been cropped. The HTTP contract and the terminal lifecycle are not covered by tests: to exercise those, start a local nest and run the client against its listener URL.

## Licence

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option, matching Nuthatch.
