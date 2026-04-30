//! Record types written to JSONL by the historian.
//! Each type maps 1-to-1 to a daily `.jsonl` file.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BrtiRecord {
    pub timestamp_ms: u64,
    pub value: f64,
    pub confidence: f64,
    pub exchange_count: u8,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeRecord {
    /// unix-ms at local receipt (local_ts)
    pub timestamp_ms: u64,
    pub exchange: String,
    pub price: f64,
    pub size: f64,
    pub exchange_ts: u64,
    pub local_ts: u64,
    /// local_ts - exchange_ts; negative means clock skew
    pub latency_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SignalRecord {
    pub timestamp_ms: u64,
    pub direction: String,
    pub confidence: f64,
    pub brti_est: f64,
    pub delta_pct: f64,
    pub velocity: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpportunityRecord {
    pub timestamp_ms: u64,
    pub ticker: String,
    pub side: String,
    pub edge: f64,
    pub kelly_fraction: f64,
    pub market_yes_price: f64,
    pub market_no_price: f64,
    pub strike: f64,
    pub closes_at: u64,
    pub brti_est: f64,
    pub signal_confidence: f64,
    /// true when market prices were synthesised (no live orderbook). Backtest should
    /// filter these out — implied_prob was 0 so edge is meaningless.
    pub synthetic: bool,
}

/// One record per market snapshot emitted by CompareTracker (observation mode only).
/// Written to `compare_YYYY-MM-DD.jsonl` when TRADING_ENABLED=false.
#[derive(Debug, Clone, Serialize)]
pub struct CompareRecord {
    pub timestamp_ms: u64,
    pub brti_est: f64,
    pub confidence: f64,
    pub ticker: String,
    pub strike: f64,
    pub yes_price: f64,
    pub no_price: f64,
    /// false = live orderbook price; true = synthetic (no real market)
    pub synthetic: bool,
    /// brti_est - strike (positive = BRTI above strike, favours YES)
    pub delta_from_strike: f64,
    pub direction: String,
    pub velocity: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FillRecord {
    pub timestamp_ms: u64,
    pub order_id: String,
    pub ticker: String,
    pub side: String,
    pub contracts: u64,
    pub price_cents: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RealizedPnlRecord {
    pub timestamp_ms: u64,
    pub ticker: String,
    pub side: String,
    pub contracts: u64,
    pub entry_price_cents: u64,
    pub exit_price_cents: u64,
    pub pnl_cents: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskViolationRecord {
    pub timestamp_ms: u64,
    pub violation_type: String,
    pub detail: String,
}
