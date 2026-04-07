use shared_types::{BrtiEstimate, Direction, KalshiMarket, Signal, TradeOpportunity, TradeSide};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

use crate::delta::BrtiDeltaDetector;
use crate::probability::{implied_probability, kelly_fraction, model_probability};

pub struct SignalEngine {
    delta: BrtiDeltaDetector,
    markets: watch::Receiver<Vec<KalshiMarket>>,
    /// Minimum BRTI percent move required to act
    delta_threshold_pct: f64,
    /// Minimum probability edge to generate a trade opportunity
    min_edge: f64,
    /// Position sizing multiplier (quarter-Kelly = 0.25)
    kelly_multiplier: f64,
}

impl SignalEngine {
    pub fn new(markets: watch::Receiver<Vec<KalshiMarket>>) -> Self {
        SignalEngine {
            delta: BrtiDeltaDetector::new(),
            markets,
            delta_threshold_pct: 0.05,
            min_edge: 0.08,
            kelly_multiplier: 0.25,
        }
    }

    /// Ingest a new BRTI estimate, update the momentum detector, and return any trade
    /// opportunities whose edge clears the minimum threshold.
    pub fn ingest(&mut self, estimate: BrtiEstimate) -> Vec<TradeOpportunity> {
        self.delta.push(estimate.clone());

        let direction = self.delta.direction(self.delta_threshold_pct);
        if matches!(direction, Direction::Neutral) {
            return vec![];
        }

        let velocity = self.delta.velocity().unwrap_or(0.0);
        let now_ms = local_ts_ms();

        // Snapshot current markets without holding the watch borrow across the loop
        let markets: Vec<KalshiMarket> = self.markets.borrow().clone();

        let model_prob = model_probability(&direction, estimate.confidence, velocity);

        let mut opportunities = Vec::new();
        for market in &markets {
            // Skip markets closing within the next 60 seconds — not enough time to trade
            if market.closes_at < now_ms.saturating_add(60_000) {
                continue;
            }

            let (side, implied_prob) = match &direction {
                Direction::Up => (TradeSide::Yes, implied_probability(market.yes_price)),
                Direction::Down => (TradeSide::No, implied_probability(market.no_price)),
                Direction::Neutral => continue, // unreachable given guard above
            };

            let edge = model_prob - implied_prob;
            if edge < self.min_edge {
                continue;
            }

            let kelly = kelly_fraction(edge, 1.0, self.kelly_multiplier);
            let signal = Signal {
                direction: direction.clone(),
                confidence: model_prob,
                brti_est: estimate.clone(),
                generated_at: now_ms,
            };

            opportunities.push(TradeOpportunity {
                market: market.clone(),
                signal,
                side,
                edge,
                kelly_fraction: kelly,
            });
        }
        opportunities
    }
}

fn local_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::SignalEngine;
    use shared_types::{BrtiEstimate, KalshiMarket};
    use tokio::sync::watch;

    fn est(value: f64, ts: u64) -> BrtiEstimate {
        BrtiEstimate { value, timestamp: ts, exchange_count: 4, confidence: 1.0 }
    }

    fn far_future_ms() -> u64 {
        // Year 2100 in ms — guaranteed to be in the future
        4_102_444_800_000
    }

    fn make_market(ticker: &str, yes_price: f64, no_price: f64) -> KalshiMarket {
        KalshiMarket {
            ticker: ticker.to_string(),
            yes_price,
            no_price,
            strike: 70_000.0,
            closes_at: far_future_ms(),
        }
    }

    #[test]
    fn no_opportunities_when_direction_neutral() {
        let (tx, rx) = watch::channel(vec![make_market("KXBTC15M-TEST", 50.0, 50.0)]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);
        // Only one sample — delta detector has no direction yet
        let opps = engine.ingest(est(70_000.0, 1_000_000));
        assert!(opps.is_empty());
    }

    #[test]
    fn opportunity_generated_when_edge_cleared() {
        let (tx, rx) = watch::channel(vec![
            // yes_price=30 cents → implied_prob=0.30; model_prob(Up,1.0,0)=0.65 → edge=0.35
            make_market("KXBTC15M-A", 30.0, 70.0),
        ]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);

        // Pump 10 increasing estimates to establish a strong Up direction
        for i in 0..10u64 {
            engine.ingest(est(60_000.0 + i as f64 * 100.0, i * 1_000));
        }
        let opps = engine.ingest(est(61_000.0, 10_000));
        assert!(!opps.is_empty(), "expected at least one opportunity");
        let opp = &opps[0];
        assert!(opp.edge >= 0.08, "edge {:.3} should be >= min_edge 0.08", opp.edge);
        assert!(opp.kelly_fraction >= 0.0);
    }

    #[test]
    fn market_expiring_within_60s_is_skipped() {
        let now_ms = super::local_ts_ms();
        let (tx, rx) = watch::channel(vec![KalshiMarket {
            ticker: "EXPIRING".to_string(),
            yes_price: 10.0,
            no_price: 90.0,
            strike: 70_000.0,
            closes_at: now_ms + 30_000, // closes in 30s — should be skipped
        }]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);
        for i in 0..11u64 {
            engine.ingest(est(60_000.0 + i as f64 * 100.0, i * 1_000));
        }
        let opps = engine.ingest(est(61_100.0, 11_000));
        assert!(opps.is_empty(), "expiring market should be skipped");
    }
}
