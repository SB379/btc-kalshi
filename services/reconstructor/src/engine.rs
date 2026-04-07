use std::collections::HashMap;
use shared_types::{BrtiEstimate, Exchange, ReconstructorConfig, Trade};

use crate::window::TradeWindow;

pub struct ReconstructorEngine {
    windows: HashMap<Exchange, TradeWindow>,
    config: ReconstructorConfig,
}

impl ReconstructorEngine {
    /// Create engine with one TradeWindow pre-allocated per Exchange variant.
    pub fn new(config: ReconstructorConfig) -> Self {
        let mut windows = HashMap::new();
        for exchange in [
            Exchange::Coinbase,
            Exchange::Kraken,
            Exchange::Bitstamp,
            Exchange::Gemini,
            Exchange::ItBit,
            Exchange::Lmax,
            Exchange::Bullish,
            Exchange::CryptoCom,
        ] {
            windows.insert(exchange, TradeWindow::new());
        }
        ReconstructorEngine { windows, config }
    }

    /// Route a trade into its exchange window, then attempt to publish a BRTI estimate.
    pub fn ingest(&mut self, trade: Trade) -> Option<BrtiEstimate> {
        let ts = trade.local_ts;
        let exchange = trade.exchange.clone();
        let window = self.windows.entry(exchange).or_default();
        window.push(trade, self.config.window_secs);
        self.publish(ts)
    }

    /// Compute a BrtiEstimate from all non-stale exchange windows.
    ///
    /// Returns None if fewer than `config.min_exchanges` windows have live data.
    /// value = arithmetic mean of per-exchange VWMPs
    /// confidence = live_exchange_count / 8.0
    fn publish(&self, now_ms: u64) -> Option<BrtiEstimate> {
        let live: Vec<f64> = self
            .windows
            .values()
            .filter(|w| !w.is_stale(now_ms, self.config.staleness_threshold_ms))
            .filter_map(|w| w.vwmp())
            .collect();

        let live_count = live.len() as u8;
        if live_count < self.config.min_exchanges {
            return None;
        }

        let value = (live.iter().sum::<f64>() / live.len() as f64 * 100.0).round() / 100.0;
        let confidence = live_count as f64 / 8.0;

        Some(BrtiEstimate {
            value,
            timestamp: now_ms,
            exchange_count: live_count,
            confidence,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ReconstructorEngine;
    use shared_types::{Exchange, ReconstructorConfig, Trade};

    fn cfg(min_exchanges: u8) -> ReconstructorConfig {
        ReconstructorConfig {
            window_secs: 60,
            min_exchanges,
            staleness_threshold_ms: 5000,
        }
    }

    fn trade(exchange: Exchange, price: f64, ts: u64) -> Trade {
        Trade {
            exchange,
            price,
            size: 1.0,
            exchange_ts: ts,
            local_ts: ts,
        }
    }

    #[test]
    fn estimate_within_one_dollar_of_mean_vwmp() {
        // Coinbase: 3 trades at 100, 200, 300 (size 1.0 each)
        //   VWMP: total=3.0, target=1.5 → cumulative at 200 = 2.0 >= 1.5 → VWMP=200
        // Kraken:  3 trades at 400, 500, 600 (size 1.0 each)
        //   VWMP: total=3.0, target=1.5 → cumulative at 500 = 2.0 >= 1.5 → VWMP=500
        // Expected mean = (200 + 500) / 2 = 350.0
        let mut engine = ReconstructorEngine::new(cfg(2));
        let ts = 1_000_000_u64;

        for price in [100.0, 200.0, 300.0] {
            engine.ingest(trade(Exchange::Coinbase, price, ts));
        }
        // Inserting the first Kraken trade gives us 2 live exchanges → estimate published
        engine
            .ingest(trade(Exchange::Kraken, 400.0, ts))
            .expect("should have estimate with 2 live exchanges");
        engine.ingest(trade(Exchange::Kraken, 500.0, ts));
        let est = engine
            .ingest(trade(Exchange::Kraken, 600.0, ts))
            .expect("should produce final estimate");

        let expected = 350.0_f64;
        assert!(
            (est.value - expected).abs() < 1.0,
            "estimate {:.2} deviates from expected {:.2} by more than $1",
            est.value,
            expected
        );
        assert_eq!(est.exchange_count, 2);
        assert!((est.confidence - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn returns_none_below_min_exchanges() {
        let mut engine = ReconstructorEngine::new(cfg(2));
        // Only one exchange has data — should not publish
        let result = engine.ingest(trade(Exchange::Coinbase, 100.0, 1_000_000));
        assert!(result.is_none());
    }

    #[test]
    fn stale_exchange_excluded_from_estimate() {
        let mut engine = ReconstructorEngine::new(cfg(2));
        // Push a Coinbase trade at ts=0 (will be stale when now_ms is large)
        engine.ingest(trade(Exchange::Coinbase, 100.0, 0));
        // Push a Kraken trade at ts=1_000_000 (fresh)
        let result = engine.ingest(trade(Exchange::Kraken, 200.0, 1_000_000));
        // Coinbase last_ts=0, now_ms=1_000_000, delta=1_000_000 > staleness=5000 → stale
        // Only Kraken is live → below min_exchanges=2 → None
        assert!(result.is_none());
    }
}
