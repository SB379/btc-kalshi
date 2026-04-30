use shared_types::Trade;
use std::collections::VecDeque;

pub struct TradeWindow {
    trades: VecDeque<Trade>,
}

impl TradeWindow {
    pub fn new() -> Self {
        TradeWindow {
            trades: VecDeque::new(),
        }
    }

    /// Append a trade and evict entries outside the rolling window.
    pub fn push(&mut self, trade: Trade, window_secs: u64) {
        let window_ms = window_secs * 1000;
        let cutoff = trade.local_ts.saturating_sub(window_ms);
        while let Some(front) = self.trades.front() {
            if front.local_ts < cutoff {
                self.trades.pop_front();
            } else {
                break;
            }
        }
        self.trades.push_back(trade);
    }

    /// Volume-Weighted Median Price per CF Benchmarks BRTI methodology.
    ///
    /// Trades are sorted by price, then we walk accumulating volume until
    /// cumulative >= total_volume / 2. That price is the VWMP.
    pub fn vwmp(&self) -> Option<f64> {
        if self.trades.is_empty() {
            return None;
        }
        let mut sorted: Vec<&Trade> = self.trades.iter().collect();
        sorted.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_size: f64 = sorted.iter().map(|t| t.size).sum();
        let target = total_size / 2.0;

        let mut cumulative = 0.0_f64;
        for trade in &sorted {
            cumulative += trade.size;
            if cumulative >= target {
                return Some(trade.price);
            }
        }
        // Unreachable given non-empty input and positive sizes, but safe fallback
        sorted.last().map(|t| t.price)
    }

    /// Returns the local_ts of the most recently pushed trade.
    pub fn last_trade_ts(&self) -> Option<u64> {
        self.trades.back().map(|t| t.local_ts)
    }

    /// True if no trade has been received, or if the last trade is older than threshold_ms.
    pub fn is_stale(&self, now_ms: u64, threshold_ms: u64) -> bool {
        match self.last_trade_ts() {
            None => true,
            Some(ts) => now_ms.saturating_sub(ts) > threshold_ms,
        }
    }
}

impl Default for TradeWindow {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::TradeWindow;
    use shared_types::{Exchange, Trade};

    fn make_trade(price: f64, size: f64) -> Trade {
        Trade {
            exchange: Exchange::Coinbase,
            price,
            size,
            exchange_ts: 1000,
            local_ts: 1000,
        }
    }

    #[test]
    fn vwmp_five_trade_case() {
        // total_size = 5.0, target = 2.5
        // Sorted by price: 100(1.0), 200(1.0), 300(2.0), 400(0.5), 500(0.5)
        // cumulative after 100 = 1.0 < 2.5
        // cumulative after 200 = 2.0 < 2.5
        // cumulative after 300 = 4.0 >= 2.5 → VWMP = 300
        let mut w = TradeWindow::new();
        let window_secs = 60;
        w.push(make_trade(100.0, 1.0), window_secs);
        w.push(make_trade(200.0, 1.0), window_secs);
        w.push(make_trade(300.0, 2.0), window_secs);
        w.push(make_trade(400.0, 0.5), window_secs);
        w.push(make_trade(500.0, 0.5), window_secs);

        assert_eq!(w.vwmp(), Some(300.0));
    }

    #[test]
    fn vwmp_empty_returns_none() {
        let w = TradeWindow::new();
        assert_eq!(w.vwmp(), None);
    }

    #[test]
    fn is_stale_when_empty() {
        let w = TradeWindow::new();
        assert!(w.is_stale(1_000_000, 5000));
    }

    #[test]
    fn is_stale_after_threshold() {
        let mut w = TradeWindow::new();
        w.push(make_trade(100.0, 1.0), 60);
        // last_ts = 1000; now_ms = 7000; delta = 6000 > 5000
        assert!(w.is_stale(7000, 5000));
    }

    #[test]
    fn not_stale_within_threshold() {
        let mut w = TradeWindow::new();
        w.push(make_trade(100.0, 1.0), 60);
        // last_ts = 1000; now_ms = 4000; delta = 3000 < 5000
        assert!(!w.is_stale(4000, 5000));
    }

    #[test]
    fn push_evicts_old_trades() {
        let mut w = TradeWindow::new();
        // Push a trade with local_ts = 0 (very old)
        let old = Trade {
            exchange: Exchange::Coinbase,
            price: 999.0,
            size: 1.0,
            exchange_ts: 0,
            local_ts: 0,
        };
        w.push(old, 60);
        assert_eq!(w.vwmp(), Some(999.0));

        // Push a new trade 2 minutes later — old trade falls outside 60s window
        let new = Trade {
            exchange: Exchange::Coinbase,
            price: 100.0,
            size: 1.0,
            exchange_ts: 120_000,
            local_ts: 120_000,
        };
        w.push(new, 60);
        // Window: cutoff = 120_000 - 60_000 = 60_000; old.local_ts=0 < 60_000 → evicted
        assert_eq!(w.vwmp(), Some(100.0));
    }
}
