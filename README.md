# btc-kalshi

Low-latency Bitcoin prediction market trading system that arbitrages Kalshi's BTC price markets by reconstructing the CF Benchmarks BRTI index in real-time from constituent exchange feeds.

**Status: Sunset (April 2026)** — The engineering worked; the strategy didn't. See [Post-Mortem](#post-mortem).

---

## Architecture

```
                    +------------------+
                    |  Exchange WSS x5 |
                    | Coinbase, Kraken |
                    | Bitstamp, Gemini |
                    |    Crypto.com    |
                    +--------+---------+
                             |
                     tokio::mpsc (merge)
                             |
                    +--------v---------+
                    |      ingest      |
                    |  normalize trades |
                    +--------+---------+
                             |
                   SPSC ring buffer (65K)
                             |
                    +--------v---------+
                    |  reconstructor   |
                    | VWMP per exchange |
                    |  mean -> BRTI est |
                    +--------+---------+
                             |
                   SPSC ring buffer (16K)
                             |
                    +--------v---------+
                    |      signal      |
                    |  spike + delta   |
                    |  consensus gate  |
                    |  logistic model  |
                    +--------+---------+
                             |
                   SPSC ring buffer (1K)
                             |
                    +--------v---------+
                    |     executor     |
                    |   risk gate      |
                    |   Kelly sizing   |
                    |   Kalshi API     |
                    +--------+---------+
                             |
                       Kalshi REST API

    All services -----> [historian] (tokio::mpsc, non-blocking)
                        daily JSONL audit files
```

---

## The Thesis

Kalshi offers prediction markets on BTC price — 15-minute, hourly, and daily contracts that resolve against the [CF Benchmarks BRTI](https://www.cfbenchmarks.com/data/indices/BRTI) index. BRTI is computed from trades on 8 constituent exchanges (Coinbase, Kraken, Bitstamp, Gemini, itBit, LMAX, Bullish, Crypto.com) using a volume-weighted median methodology.

The idea: if you reconstruct BRTI in real-time from the same exchange feeds, you can detect when BRTI is moving before Kalshi's orderbook reprices. A $20 BTC move that hasn't yet been reflected in contract prices represents a mispriced contract — buy or sell before the market catches up.

This requires sub-10ms latency from exchange trade receipt to order submission, which is why the system is written in Rust with lock-free ring buffer IPC between services.

---

## System Design

### Crate Structure

| Crate | LOC | Purpose |
|-------|-----|---------|
| `shared/types` | 96 | Canonical types: `Trade`, `BrtiEstimate`, `Signal`, `KalshiMarket`, `TradeOpportunity` |
| `services/ingest` | 1,351 | 8 exchange WebSocket normalizers (5 active), merged into SPSC ring buffer |
| `services/reconstructor` | 417 | Rolling 60s trade windows, VWMP per exchange, mean across live feeds |
| `services/signal` | 930 | Dual detectors (spike + delta), consensus gate, logistic probability model |
| `services/executor` | 933 | Risk gate, Kelly sizing, Kalshi RSA-PSS auth, position tracking, 5 exit conditions |
| `data/historian` | 220 | Append-only JSONL logger, one file per type per day, non-blocking |
| `data/backtest` | 855 (Python) | Replay engine + logistic regression model fitter |

**Total: ~5,800 lines Rust + 855 lines Python**

### IPC: Lock-Free Ring Buffers

The hot path uses single-producer single-consumer (SPSC) ring buffers from the `ringbuf` crate — no mutexes, no allocations, no syscalls on the critical path.

- **ingest -> reconstructor**: 65,536-slot ring buffer. 8 exchange tasks merge into one `tokio::mpsc` channel, then a single writer pushes into the ring buffer. This preserves the SPSC invariant while accepting trades from multiple sources.
- **reconstructor -> signal**: 16,384-slot ring buffer.
- **signal -> executor**: 1,024-slot ring buffer. This leg is the fastest — a new signal must reach the executor before the opportunity window closes.

The historian runs on a separate `tokio::mpsc` channel with `try_send` — if the channel is full, the log entry is dropped rather than blocking the hot path.

### Signal Detection

Two independent momentum detectors feed into a consensus gate:

**SpikeDetector** — 30-second rolling window, fires when price moves exceed a threshold. Asymmetric: $15 up threshold, $25 down threshold (downward spikes mean-revert more aggressively, so we require a bigger signal to trade the NO side).

**BrtiDeltaDetector** — 20-sample rolling buffer, fires on percent-change thresholds (default 1%). Outputs delta, velocity ($/sec), and direction. Captures sustained trends that the spike detector might miss.

**Consensus gate**: only trades when both detectors agree or one is neutral. Spike=Up + Delta=Down = no trade (conflicting signals). A direction persistence gate requires 2 consecutive non-neutral confirmations before emitting opportunities — this eliminates whipsaw from flickering signals.

### Probability Model

Empirical logistic regression fitted on n=97 historical observations:

```
P(YES) = logistic(1.261 + 0.083*(BRTI - strike) - 0.002*seconds_to_close + 0.099*velocity)
         * (0.7 + brti_confidence * 0.3)
```

Trained on observations within +/-$5 of strike to avoid extrapolation. Cross-validated AUC: **0.61** — effectively a coin flip. This was the clearest signal that the strategy lacked edge.

### Position Management

**Kelly criterion sizing** with quarter-Kelly default (0.25x). Position caps: 2,000 cents ($20) per trade, 100 contracts max.

**Five exit conditions** (checked every 60 seconds via async reconciliation loop):

1. **Stop-loss**: price drops below 40% of entry (60% loss)
2. **Profit target**: price rises to 145% of entry (45% gain), floored at 10 cents, capped at 95 cents
3. **Trailing stop**: price falls 8 cents from peak after gaining 10+ cents
4. **BRTI reversal**: underlying moved >$30 against the position since entry
5. **Near expiry**: <90 seconds remaining and position is underwater

Additional protections: thesis locks (no opposite-side entry while a position is open), loss suppression (10-cent loss blocks same-ticker re-entry for 120 seconds), daily loss halt at $50.

### Kalshi Integration

RSA-PSS signature authentication (PKCS#8 private key, SHA-256). The client places limit orders at market_price + 1 cent for aggressive fill. Balance is re-fetched before every order for capital-aware sizing. A background reconciliation loop syncs local position state with Kalshi's unsettled positions every 60 seconds.

---

## Exchange Connectors

| Exchange | Protocol | Pair | Status |
|----------|----------|------|--------|
| Coinbase | WebSocket | `BTC-USD` | Active |
| Kraken | WebSocket | `XBT/USD` | Active |
| Bitstamp | WebSocket | `btcusd` | Active |
| Gemini | WebSocket | `BTCUSD` | Active |
| Crypto.com | WebSocket | `BTC_USD` | Active |
| itBit | REST | `XBTUSD` | Inactive (API shut down) |
| LMAX Digital | WebSocket/FIX | `BTC/USD` | Inactive (private venue) |
| Bullish | WebSocket | `BTC-USD` | Inactive (returns 404) |

Each connector runs as an independent `tokio::task` with exponential backoff reconnection (100ms start, 30s max, +/-20% jitter). One feed dying never affects another.

---

## Post-Mortem

### What Happened

Started with ~$140 in Kalshi. Lost $102 over several live trading sessions. Backtesting confirmed the strategy was no better than a coin flip.

### Why It Didn't Work

**Network latency was the real bottleneck.** The internal pipeline met its latency targets — under 8ms from ingest to order submission. But the round trip to Kalshi's API from a local machine added 50-200ms of network latency. By the time our order hit the exchange, the opportunity had already been priced in. Slippage ate any theoretical margin. Rust was the right language for the hot path, but the bottleneck was never in our code — it was on the wire.

**The features were wrong for prediction markets.** "Distance and velocity" — how far and how fast BRTI is moving — turned out to be poor predictors for binary outcome markets. Prediction markets price in *expected outcomes at resolution time*, not spot price momentum. A $20 BTC move that's already visible in the orderbook has zero predictive value for where BRTI will be when the market closes. The model needed to predict whether BRTI would be above or below the strike at close — momentum says almost nothing about that.

**Asymmetric information problem.** Market makers on Kalshi have better models, more data, and co-located infrastructure. Retail participants start at a structural disadvantage on fees and latency. Finding genuine edge requires either better information or better infrastructure — we had neither.

### What Worked

- Lock-free SPSC ring buffers kept the hot path under 5ms end-to-end
- BRTI reconstruction was accurate (validated against CF Benchmarks public data)
- Position management with 5 exit conditions prevented catastrophic losses — we lost $102, not $140
- The historian produced clean JSONL audit trails that made debugging and backtesting straightforward
- Rust was the right choice: zero-cost abstractions, no GC pauses, fearless concurrency
- `#![deny(clippy::unwrap_used)]` caught real bugs during development

### Lessons Learned

1. **Test the thesis before building the system.** A simple Python script polling Kalshi and logging BRTI correlation data would have revealed the lack of edge in a weekend, before writing 5,800 lines of Rust.

2. **Latency only matters if you have edge.** Sub-millisecond IPC is meaningless when the round trip to the exchange is 100ms and the opportunity window is 50ms.

3. **Prediction markets are not spot markets.** The same signals that work for spot BTC trading (momentum, mean-reversion) do not translate to binary outcome markets. The resolution mechanism changes everything.

4. **Small sample sizes kill quant strategies.** n=97 training samples with AUC=0.61 is not a model — it's noise. You need hundreds of labeled observations before you can trust any signal.

5. **If a strategy is obvious enough for a solo developer to implement in a month, it's probably already priced in.**

---

## What Was Never Built

- **`services/ml-sidecar`**: TimesFM/LightGBM inference sidecar. Was going to consume features over ZeroMQ and return probability updates. Blocked on insufficient training data — with AUC=0.61 on the simple model, there was no reason to believe a more complex model would do better.

- **`dashboard`**: Next.js real-time P&L and feed health dashboard. Deprioritized after the strategy proved unprofitable. The JSONL historian logs turned out to be sufficient for analysis.

---

## Running Locally

```bash
# Clone and configure
git clone https://github.com/<your-username>/btc-kalshi.git
cd btc-kalshi
cp .env.example .env
# Fill in KALSHI_API_KEY, KALSHI_API_SECRET, and KALSHI_PRIVATE_KEY in .env

# Build
cargo build --release

# Run in observation mode (no trading, just logging)
# Set TRADING_ENABLED=false in .env
cargo run --release -p ingest

# Run backtesting on collected data
python3 data/backtest/backtest.py --dates 2026-04-28
```

Note: requires Rust 1.75+ and Python 3.10+. Exchange WebSocket connections need outbound internet access.

---

## Tech Stack

| Concern | Crate/Library |
|---------|---------------|
| Async runtime | `tokio` |
| WebSocket | `tokio-tungstenite` |
| Ring buffer IPC | `ringbuf` |
| HTTP client | `reqwest` |
| RSA auth | `rsa` + `sha2` |
| Logging | `tracing` + `tracing-subscriber` |
| Serialization | `serde` + `serde_json` |
| Config | `dotenvy` |
| Backtesting | Python (`pandas`, `numpy`, `scikit-learn`) |

---

## License

MIT — see [LICENSE](LICENSE).
