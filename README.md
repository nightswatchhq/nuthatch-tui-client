# Nuthatch TUI Client

A fast, read-only terminal dashboard for a running [Nuthatch](https://github.com/nightswatchhq/nuthatch) nest.

`nuthatch-tui-client` turns the Nuthatch HTTP API into an operator view: whether the nest is live, how far it is behind, which data it has collected, what has been sealed, and how many outbound RPC requests the indexer has made since it started.

It is a client, not an indexer. It does not need an RPC key, open a store, alter nest configuration, or write data. Point it at an already-running Nuthatch API and it observes.

```text
 NUTHATCH  LIVE VIEW   http://127.0.0.1:8288   ● LIVE

 ╭ NEST HEALTH ────────────╮  ╭ DATA COLLECTED ───────╮  ╭ SYNC POSITION ───────╮
 │ STATUS     READY        │  │ Tables          17     │  │ ████████████████████ │
 │ Tip        25,766,811   │  │ Decoded rows    2,275  │  │ 25766811 / 25766811  │
 │ Indexed    25,766,811   │  │ Sealed rows     2,453  │  ╰──────────────────────╯
 │ Finalised  25,766,747   │  │ Lag             0      │
 ╰─────────────────────────╯  ╰────────────────────────╯

 ╭ INDEXED TABLES ─────────╮  ╭ OPERATIONS ────────────╮
 │ usdc__approval           │  │ RPC REQUESTS     367   │
 │ usdc__transfer           │  │ Reorgs             0   │
 │ ...                      │  │ Storage integrity: OK  │
 ╰─────────────────────────╯  ╰─────────────────────────╯
```

## What it shows

| Panel | What it answers |
|---|---|
| **Nest health** | Is the indexer ready? What are the tip, indexed, and finalised block heights? |
| **Data collected** | How many event tables exist, rows decoded, rows sealed, and blocks of lag? |
| **Sync position** | How closely the committed cursor follows the chain tip. |
| **Indexed tables** | The event tables exposed by the nest's schema. |
| **Performance** | Rolling RPC request, RPC method, decoded-row, and indexed-block rates; lifetime counters; API refresh time; source-poll age; RSS; CPU utilisation; hot-store and sealed-segment disk footprint; RPC endpoint failures, retries, and average latency; and reorg count. Press `w` to choose a 15-, 60-, or 90-second rolling window. |
| **Selected table** | Row count and latest block for the selected event table. |
| **Live event feed** | The six newest decoded rows for the selected table. |
| **RPC activity** | Recent changes in the RPC request counter, sampled once per dashboard refresh. |

The RPC counter is Nuthatch's own `nuthatch_rpc_requests_total` metric. It resets when the indexer restarts and measures JSON-RPC requests, not provider billing units. Alchemy and other providers use their own compute-unit accounting, so their dashboard remains the authority for spend.

## Requirements

- Rust 1.85 or newer.
- A running Nuthatch API, normally started with `nuthatch dev`.
- A terminal with colour and Unicode support. A modest 100-column terminal is the pleasant setting.

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
| CPU utilisation | `nuthatch_process_cpu_seconds_total` | A cumulative CPU-seconds counter; the client derives a rolling percentage over the selected window, distinct from the lifetime counters above. Shown as `unavailable` on a Nuthatch that does not publish the series, `warming up` before the first full window of samples. **Nuthatch's sampler currently reads `/proc/self/stat` only** ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)) — on a macOS-hosted nest the counter is published but pinned at 0.0, so the client will show a permanent, honestly-reported-but-misleading `0.0%` rather than `unavailable` until that's fixed upstream. |
| Disk footprint | `nuthatch_hot_store_bytes`, `nuthatch_sealed_segments_bytes` | Current on-disk bytes of the mutable hot store and sealed Parquet segments, summed across mounted nests. Shown as `unavailable` when absent. |
| RPC endpoint health | `nuthatch_rpc_endpoint_failures_total`, `nuthatch_rpc_endpoint_retries_total`, `nuthatch_rpc_request_duration_seconds_{sum,count}` | Failure and retry counts, and average round-trip latency, summed across every configured RPC endpoint the same way this client already aggregates labelled series like RPC methods. Latency reads `no calls yet` if the histogram is present but empty, `unavailable` if Nuthatch does not publish it. |
| API refresh and source-poll age | Client timing and `/ready` | Current client request time and seconds since Nuthatch's last successful source poll. |

The dashboard degrades each of these independently rather than displaying a misleading zero: a metric a given Nuthatch version does not publish reads `unavailable`, not `0`. It does not inspect the local process, filesystem, or RPC provider to fill gaps itself, because that would make remote operation and the read-only boundary rather less clear than advertised — every number here comes from Nuthatch's own `/metrics`.

## Current limits

- The client is a dashboard, not a general SQL workbench. It presents a summary and a six-row live feed for the selected event table.
- It reports request count, not exact provider cost. Billing models differ by provider and method.
- CPU utilisation is only accurate on a Linux-hosted Nuthatch. On macOS the upstream sampler has no fallback yet and the counter reads a flat `0.0%` rather than `unavailable` ([nightswatchhq/nuthatch#844](https://github.com/nightswatchhq/nuthatch/issues/844)).
- A remote Nuthatch endpoint must be deliberately exposed by its operator. The default assumes a localhost service.
- The screen is currently designed for an 80-column or wider terminal. It remains usable narrower, but tables will necessarily have less room to breathe.

## Development

```sh
cargo fmt --check
cargo test
cargo clippy -- -D warnings
```

To exercise the real API path, start a local nest and run the client with its listener URL. The test suite covers Prometheus parsing and URL normalisation; the live smoke test confirms the endpoint contract and terminal lifecycle.

## Licence

Dual-licensed under MIT or Apache-2.0, matching Nuthatch.
