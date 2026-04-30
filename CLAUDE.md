# btc-kalshi — Claude Code Project Guide

## What This Is

Low-latency Bitcoin prediction market trading system. We arbitrage Kalshi's BTC price markets
by reconstructing the CF Benchmarks BRTI index in real-time from constituent exchange feeds,
faster than Kalshi's orderbook reprices.

**Project sunset April 2026.** This file was the build guide used during active development. See [README.md](README.md) for architecture overview and post-mortem.

---

## Repo Structure

```
btc-kalshi/
├── CLAUDE.md
├── Cargo.toml                  # Workspace root — all Rust crates listed here
├── .env.example                # All required env vars with descriptions
├── shared/
│   ├── types/                  # Trade, Exchange, Signal, OrderBook structs
│   └── ipc/                    # Ring buffer transport, Unix socket helpers
├── services/
│   ├── ingest/                 # Multi-exchange WebSocket feed normalizer
│   ├── reconstructor/          # Live BRTI estimation engine
│   ├── signal/                 # Orderbook analysis + signal generation
│   ├── executor/               # Kalshi order management
│   └── ml-sidecar/             # Python — TimesFM / LightGBM inference
├── data/
│   ├── historian/              # Async Parquet writer for all tick data
│   └── backtest/               # Replay engine + strategy harness (Python)
├── dashboard/                  # Next.js — P&L, positions, feed health
└── infra/
    ├── docker/
    └── scripts/
```

---

## Build Phases — Work Only on the Active Phase

| Phase | Service                  | Status                |
| ----- | ------------------------ | --------------------- |
| 1     | `services/ingest`        | ✅ Complete           |
| 2     | `services/reconstructor` | ✅ Complete           |
| 3     | `services/signal`        | ✅ Complete           |
| 4     | `services/executor`      | ✅ Complete           |
| 5     | `services/ml-sidecar`    | Never built (project sunset) |
| 6     | `data/historian`         | ✅ Complete           |
| 7     | `data/backtest`          | ✅ Complete           |
| 8     | `dashboard`              | Never built (project sunset) |

---

## Architecture Rules — Never Violate These

### Data Flow (unidirectional, no cycles)

```
[Exchange WSS x8] → [ingest] → [reconstructor] → [signal] → [executor] → Kalshi API
                                                       ↕
                                                 [ml-sidecar]
                        All services → [historian] (async, non-blocking)
```

### IPC Transport

- `ingest → reconstructor`: lock-free SPSC ring buffer (`ringbuf` crate), shared memory
- `reconstructor → signal`: lock-free SPSC ring buffer
- `signal → executor`: lock-free SPSC ring buffer — **this leg must be fastest**
- `signal → ml-sidecar`: Unix domain socket (ZeroMQ `zmq` crate, PUSH/PULL)
- All services → `historian`: `tokio::sync::mpsc` channel, non-blocking send, drop on full

### Latency Budget

- ingest receipt → reconstructor update: **< 1ms**
- reconstructor update → signal fire: **< 2ms**
- signal fire → executor order submission: **< 5ms**
- ml-sidecar inference: **< 100ms** (runs async, never blocks hot path)

### Error Handling Philosophy

- Each exchange WebSocket handler is an independent `tokio::task`
- One feed dying must never panic or block another feed
- All reconnections use exponential backoff: start 100ms, max 30s, jitter ±20%
- Services log errors via `tracing` and recover — they do not crash the process

---

## Shared Types (source of truth — never duplicate these)

Defined in `shared/types/src/lib.rs`. Import via `use shared_types::*`.

```rust
pub enum Exchange {
    Coinbase,
    Kraken,
    Bitstamp,
    Gemini,
    ItBit,
    Lmax,
    Bullish,
    CryptoCom,
}

pub struct Trade {
    pub exchange: Exchange,
    pub price: f64,
    pub size: f64,
    pub exchange_ts: u64,   // exchange-reported unix milliseconds
    pub local_ts: u64,      // SystemTime at receipt, unix milliseconds
}

pub struct BrtiEstimate {
    pub value: f64,
    pub timestamp: u64,
    pub exchange_count: u8,  // how many feeds contributed
    pub confidence: f64,     // 0.0–1.0, drops if feeds are missing
}

pub struct Signal {
    pub direction: Direction,      // Up, Down, Neutral
    pub confidence: f64,           // 0.0–1.0
    pub brti_est: BrtiEstimate,
    pub generated_at: u64,
}

pub enum Direction { Up, Down, Neutral }
```

---

## Tech Stack

### Rust Services

| Concern            | Crate                                                            |
| ------------------ | ---------------------------------------------------------------- |
| Async runtime      | `tokio` (full features)                                          |
| WebSocket client   | `tokio-tungstenite` + `tungstenite`                              |
| JSON               | `serde` + `serde_json`                                           |
| Ring buffer IPC    | `ringbuf`                                                        |
| HTTP client        | `reqwest` (TLS enabled)                                          |
| HMAC auth (Kalshi) | `hmac` + `sha2`                                                  |
| Logging            | `tracing` + `tracing-subscriber` (JSON format in prod)           |
| JSONL writes       | `serde_json` — append-only, one JSON line per record             |
| Config/env         | `dotenvy` + `config` crate                                       |
| Time               | `std::time::SystemTime` for local_ts; parse exchange ts manually |

### Python ML Sidecar (`services/ml-sidecar/`)

| Concern            | Library                                  |
| ------------------ | ---------------------------------------- |
| Package management | `uv` with `pyproject.toml`               |
| IPC with Rust      | `pyzmq` (ZeroMQ PULL socket)             |
| Time series model  | `timesfm` (Google)                       |
| Fast tabular model | `lightgbm`                               |
| Numerics           | `numpy`, `pandas`, `polars`              |
| Serialization      | `msgpack` (faster than JSON over socket) |

### Dashboard (`dashboard/`)

- Next.js 14 App Router, TypeScript
- `recharts` for time series charts
- Polling Kalshi REST API (not WebSocket — dashboard is not latency-critical)
- No SSR needed, pure client-side

---

## Environment Variables

All secrets via `.env`. Never hardcode. Never commit `.env`.

```bash
# Kalshi
KALSHI_API_KEY=
KALSHI_API_SECRET=
KALSHI_USE_DEMO=true          # flip to false for live trading
KALSHI_BASE_URL=https://api.elections.kalshi.com/trade-api/v2

# CF Benchmarks (when licensed)
CF_BENCHMARKS_API_KEY=
CF_BENCHMARKS_BASE_URL=

# Risk limits
MAX_POSITION_SIZE_CENTS=500   # max per trade in cents
MAX_DAILY_LOSS_CENTS=5000     # halt trading if crossed
KELLY_FRACTION=0.25           # quarter-Kelly sizing

# Logging
RUST_LOG=info                 # trace | debug | info | warn | error
LOG_FORMAT=pretty             # pretty (dev) | json (prod)
```

---

## Constituent Exchanges — WebSocket Endpoints

| Exchange     | URL                                                         | Pair      |
| ------------ | ----------------------------------------------------------- | --------- |
| Coinbase     | `wss://advanced-trade-ws.coinbase.com`                      | `BTC-USD` |
| Kraken       | `wss://ws.kraken.com`                                       | `XBT/USD` |
| Bitstamp     | `wss://ws.bitstamp.net`                                     | `btcusd`  |
| Gemini       | `wss://api.gemini.com/v1/marketdata/BTCUSD`                 | —         |
| itBit        | REST polling fallback (no public WS)                        | `XBTUSD`  |
| LMAX Digital | `wss://fix.lmaxdigital.com` (FIX/WS)                        | `BTC/USD` |
| Bullish      | `wss://api.exchange.bullish.com/trading-api/v1/market-data` | `BTC-USD` |
| Crypto.com   | `wss://stream.crypto.com/exchange/v1/market`                | `BTC_USD` |

Start Phase 1 with Coinbase, Kraken, Bitstamp, Gemini only. Add remaining 4 in Phase 1.5.

---

## BRTI Reconstruction Logic (for `services/reconstructor/`)

Do not implement until Phase 2. Reference only.

1. Maintain a rolling 60-second trade window per exchange
2. Per exchange, compute **Volume-Weighted Median Price (VWMP)**:
   - Sort trades by price
   - Walk sorted list accumulating volume until cumulative >= total_volume / 2
   - That price is the VWMP
3. `BRTI_est = mean(VWMP per exchange)` over all live exchanges
4. `confidence = live_exchange_count / 8.0`
5. Publish a new `BrtiEstimate` on every incoming trade (not on a timer)

---

## Kalshi Market Resolution Reference

- **15-minute markets** (`KXBTC15M`): resolve against BRTI at the close of the 15m window
- **1-hour markets** (`KXBTC1H`): resolve against BRTI at the top of the hour
- **Daily close markets**: resolve against BRRNY (4pm New York time)

Your signal must know which benchmark resolves which market. Don't conflate them.

---

## Code Style

- **Rust**: `rustfmt` default settings. `clippy` with `#![deny(clippy::unwrap_used)]` — use `?` or explicit error handling everywhere. No `unwrap()` in production paths.
- **Python**: `ruff` for linting + formatting. Type hints on all function signatures.
- **Naming**: Services are snake_case directories. Structs are PascalCase. No abbreviations except established ones (`ts` for timestamp, `ws` for WebSocket, `ipc`).
- **Comments**: Comment the _why_, not the _what_. BRTI reconstruction math must be commented with the CF methodology source.

---

## Testing Requirements

- Each exchange normalizer must have a unit test that feeds a raw WebSocket JSON payload and asserts the correct `Trade` struct output
- Reconnection logic must be tested with a mock WebSocket server that drops connections
- BRTI reconstruction must be tested against known historical BRTI values (stored in `data/backtest/fixtures/`)
- No integration tests that hit live exchange APIs in CI

---

## What Claude Code Should Never Do

- Add dependencies not listed in this file without asking
- Build ahead of the current active phase
- Use `unwrap()` or `expect()` in non-test code
- Share mutable state between exchange handlers — each handler owns its data
- Write to the ring buffer from multiple tasks — ingest has one writer, reconstructor has one reader (SPSC)
- Skip the `local_ts` timestamp — it's critical for latency measurement
- Use `async_std` — this repo uses `tokio` exclusively
- Create a new `.env` file — modify `.env.example` only

---

## Historian Log Format

Files: `data/logs/{type}_{YYYY-MM-DD}.jsonl` — one JSON object per line.

Read in Python:
```python
import pandas as pd
df = pd.read_json('data/logs/brti_2026-04-07.jsonl', lines=True)
```

Types: `brti`, `trades`, `signals`, `opportunities`, `fills`, `risk_violations`.

---

## Signal Design Learnings

- The 20-sample rolling delta detector is too slow for 15-minute markets. Real edge is in 5-30 second spike detection, not multi-minute trend following.
- Observed: BTC moved $44 down then $31 up within 90 seconds on a 15-min market. Kalshi repriced from 52¢ → 41¢ → 64¢ in the same window.
- Target detection window: BRTI moves >$20 in <30 seconds = actionable signal. Current system would miss this entirely.
- Phase 5 ML model should focus on short-window microstructure features, not trend features.
- Add a second detector alongside `BrtiDeltaDetector`: `SpikeDetector` — rolling 30-second window, fires when absolute price change exceeds configurable threshold (default $15).
