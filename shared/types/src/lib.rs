use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub exchange: Exchange,
    pub price: f64,
    pub size: f64,
    /// Exchange-reported unix milliseconds
    pub exchange_ts: u64,
    /// SystemTime at receipt, unix milliseconds
    pub local_ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconstructorConfig {
    /// Rolling trade window in seconds (default: 60)
    pub window_secs: u64,
    /// Minimum number of live feeds required to publish an estimate (default: 2)
    pub min_exchanges: u8,
    /// Drop an exchange feed if silent longer than this many milliseconds (default: 5000)
    pub staleness_threshold_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrtiEstimate {
    pub value: f64,
    pub timestamp: u64,
    /// How many feeds contributed
    pub exchange_count: u8,
    /// 0.0–1.0, drops if feeds are missing
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Direction {
    Up,
    Down,
    Neutral,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub direction: Direction,
    /// 0.0–1.0, product of model confidence × brti confidence
    pub confidence: f64,
    pub brti_est: BrtiEstimate,
    /// unix ms
    pub generated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiMarket {
    pub ticker: String,
    /// cents, 0–100
    pub yes_price: f64,
    /// cents, 0–100
    pub no_price: f64,
    /// BTC price the market resolves around
    pub strike: f64,
    /// unix ms when market opened
    pub open_time: u64,
    /// unix ms
    pub closes_at: u64,
    /// true when yes_price/no_price were computed synthetically (no live orderbook bid)
    pub synthetic: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeSide {
    Yes,
    No,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeOpportunity {
    pub market: KalshiMarket,
    pub signal: Signal,
    pub side: TradeSide,
    /// our probability - market implied probability (0.0–1.0)
    pub edge: f64,
    /// suggested position size as fraction of bankroll
    pub kelly_fraction: f64,
}
