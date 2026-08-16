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
| **Performance** | RPC requests per second and minute, decode throughput, API refresh time, source-poll age, process RSS, and reorg count. |
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
| `GET /metrics` | Prometheus counters for decoded rows, sealed rows, reorgs, and RPC requests. |
| `GET /tables` | The event-table catalogue. |
| `GET /sql?q=…` | A small summary query for the first available event table. |

It makes no HTTP mutation request and never touches the nest's redb or Parquet files. This also means it can run from another machine if the Nuthatch API is intentionally exposed and protected by the operator's normal network controls.

## Current limits

- The client is a dashboard, not a general SQL workbench. It presents a summary and a six-row live feed for the selected event table.
- It reports request count, not exact provider cost. Billing models differ by provider and method.
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
