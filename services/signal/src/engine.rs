use shared_types::{BrtiEstimate, Direction, KalshiMarket, Signal, TradeOpportunity, TradeSide};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::debug;

use crate::delta::BrtiDeltaDetector;
use crate::probability::{implied_probability, kelly_fraction, model_probability};
use crate::spike::SpikeDetector;

pub struct SignalEngine {
    delta: BrtiDeltaDetector,
    /// Short-window spike detector: Up fires at >$15, Down fires at >$25 (asymmetric)
    spike: SpikeDetector,
    markets: watch::Receiver<Vec<KalshiMarket>>,
    /// Minimum BRTI percent move required to act
    delta_threshold_pct: f64,
    /// Minimum probability edge to generate a trade opportunity
    min_edge: f64,
    /// Position sizing multiplier (quarter-Kelly = 0.25)
    kelly_multiplier: f64,
    /// Per-ticker cooldown: unix-ms of the last opportunity emitted for each ticker
    ticker_last_opp_ms: HashMap<String, u64>,
    // Cached stats from the most recent ingest() call — for historian access
    last_delta_pct: f64,
    last_velocity: f64,
    last_model_prob: f64,
    last_direction: Direction,
    /// Number of consecutive non-neutral confirmations required before trading.
    direction_confirms_required: u8,
    /// Last observed non-neutral direction for persistence gating.
    last_non_neutral_direction: Option<Direction>,
    /// Consecutive count for the current non-neutral direction.
    direction_confirm_count: u8,
}

impl SignalEngine {
    pub fn new(markets: watch::Receiver<Vec<KalshiMarket>>) -> Self {
        SignalEngine {
            delta: BrtiDeltaDetector::new(),
            spike: SpikeDetector::new(30, 15.0, 25.0),
            markets,
            delta_threshold_pct: 0.01,
            min_edge: 0.05,
            kelly_multiplier: 0.25,
            ticker_last_opp_ms: HashMap::new(),
            last_delta_pct: 0.0,
            last_velocity: 0.0,
            last_model_prob: 0.0,
            last_direction: Direction::Neutral,
            direction_confirms_required: 2,
            last_non_neutral_direction: None,
            direction_confirm_count: 0,
        }
    }

    pub fn last_delta_pct(&self) -> f64 {
        self.last_delta_pct
    }
    pub fn last_velocity(&self) -> f64 {
        self.last_velocity
    }
    pub fn last_model_prob(&self) -> f64 {
        self.last_model_prob
    }
    pub fn last_direction(&self) -> &Direction {
        &self.last_direction
    }

    /// Ingest a new BRTI estimate, update the momentum detector, and return any trade
    /// opportunities whose edge clears the minimum threshold.
    pub fn ingest(&mut self, estimate: BrtiEstimate) -> Vec<TradeOpportunity> {
        self.delta.push(estimate.clone());
        self.spike.push(estimate.clone());

        let delta_direction = self.delta.direction(self.delta_threshold_pct);
        let spike_direction = self.spike.direction();

        // Only trade when detectors agree or one is neutral.
        // Conflicting signals (e.g. spike=Up while delta=Down) → Neutral — no trade.
        // Rationale: conflicting detectors indicate ambiguous momentum; the original
        // OR logic caused Up to always win on conflicts due to match arm ordering.
        let direction = match (&spike_direction, &delta_direction) {
            (Direction::Up, Direction::Up)
            | (Direction::Up, Direction::Neutral)
            | (Direction::Neutral, Direction::Up) => Direction::Up,
            (Direction::Down, Direction::Down)
            | (Direction::Down, Direction::Neutral)
            | (Direction::Neutral, Direction::Down) => Direction::Down,
            _ => Direction::Neutral,
        };

        let spike_delta = self.spike.delta();
        let delta_pct = self.delta.delta_pct().unwrap_or(0.0);

        // Update cached stats — model_prob is computed per-market inside the loop;
        // reset to 0.0 here so the historian sees 0 if no markets pass the distance filter.
        self.last_delta_pct = delta_pct;
        self.last_velocity = spike_delta;
        self.last_model_prob = 0.0;
        self.last_direction = direction.clone();

        if matches!(direction, Direction::Neutral) {
            self.last_non_neutral_direction = None;
            self.direction_confirm_count = 0;
            return vec![];
        }
        match self.last_non_neutral_direction.as_ref() {
            Some(prev) if same_direction(prev, &direction) => {
                self.direction_confirm_count = self.direction_confirm_count.saturating_add(1);
            }
            _ => {
                self.last_non_neutral_direction = Some(direction.clone());
                self.direction_confirm_count = 1;
            }
        }
        if self.direction_confirm_count < self.direction_confirms_required {
            debug!(
                direction = ?direction,
                confirms = self.direction_confirm_count,
                required = self.direction_confirms_required,
                "direction persistence gate not yet satisfied"
            );
            return vec![];
        }

        debug!(
            spike_delta = spike_delta,
            delta_pct = delta_pct,
            direction = ?direction,
            "non-neutral signal"
        );
        let now_ms = local_ts_ms();

        // Snapshot current markets without holding the watch borrow across the loop
        let markets: Vec<KalshiMarket> = self.markets.borrow().clone();

        let mut opportunities = Vec::new();
        for market in &markets {
            // Skip markets closing within the next 5 minutes — too close to resolution,
            // price is already fair and there's no time for our edge to play out.
            if market.closes_at.saturating_sub(now_ms) < 300_000 {
                continue;
            }

            if market.yes_price == 0.0 || market.no_price == 0.0 {
                debug!(ticker = %market.ticker, "skipping market with zero prices");
                continue;
            }

            // Distance filter: only trade when BRTI is within $30 of the strike.
            // Beyond $30, the logistic model is extrapolating outside its training range
            // and edge estimates are unreliable. The min_edge threshold does additional
            // filtering — this is a hard cap on model extrapolation.
            let distance_from_strike = (estimate.value - market.strike).abs();
            if distance_from_strike > 30.0 {
                debug!(
                    ticker = %market.ticker,
                    brti = estimate.value,
                    strike = market.strike,
                    distance = distance_from_strike,
                    "skipping — BRTI too far from strike"
                );
                continue;
            }

            // Compute time remaining for this specific market (used in model_probability).
            let seconds_to_close = market.closes_at.saturating_sub(now_ms) as f64 / 1000.0;

            // P(YES resolves) for this market, given current BRTI position vs strike.
            let yes_prob = model_probability(
                &direction,
                estimate.value,
                market.strike,
                seconds_to_close,
                spike_delta,
                estimate.confidence,
            );

            // model_prob = probability that our specific bet wins.
            // For YES bets: model_prob = yes_prob.
            // For NO bets: model_prob = P(NO) = 1 - yes_prob.
            let (side, implied_prob, model_prob) = match &direction {
                Direction::Up => (
                    TradeSide::Yes,
                    implied_probability(market.yes_price),
                    yes_prob,
                ),
                Direction::Down => (
                    TradeSide::No,
                    implied_probability(market.no_price),
                    1.0 - yes_prob,
                ),
                Direction::Neutral => continue, // unreachable given persistence gate above
            };

            // Update cached model_prob for historian (last market wins if multiple fire).
            self.last_model_prob = model_prob;

            debug!(
                ticker = %market.ticker,
                yes_price_cents = market.yes_price,
                no_price_cents = market.no_price,
                direction = ?direction,
                yes_prob = yes_prob,
                model_prob = model_prob,
                "market prices"
            );

            let edge = model_prob - implied_prob;
            if edge < self.min_edge {
                continue;
            }

            // Suppress duplicate opportunities for the same ticker within 30 seconds.
            // Uses estimate.timestamp (receipt time) so tests can control timing via
            // synthetic timestamps without depending on wall-clock speed.
            let opp_ts = estimate.timestamp;
            if let Some(&last_ms) = self.ticker_last_opp_ms.get(&market.ticker) {
                if opp_ts.saturating_sub(last_ms) < 30_000 {
                    debug!(ticker = %market.ticker, "suppressing opportunity — 30s cooldown active");
                    continue;
                }
            }
            self.ticker_last_opp_ms
                .insert(market.ticker.clone(), opp_ts);

            // Real Kelly for binary markets.
            // At price p cents: win (100-p) cents, lose p cents → odds = (100-p)/p.
            // Use the price of the side being traded.
            let price_cents = match &direction {
                Direction::Up => market.yes_price,
                Direction::Down => market.no_price,
                Direction::Neutral => continue,
            };
            if price_cents <= 0.0 || price_cents >= 100.0 {
                continue;
            }
            let odds = (100.0 - price_cents) / price_cents;
            let kelly = kelly_fraction(model_prob, odds, self.kelly_multiplier);
            tracing::debug!(
                edge = edge,
                price_cents = price_cents,
                odds = odds,
                kelly_multiplier = self.kelly_multiplier,
                kelly = kelly,
                "kelly computation"
            );
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

fn same_direction(a: &Direction, b: &Direction) -> bool {
    matches!(
        (a, b),
        (Direction::Up, Direction::Up)
            | (Direction::Down, Direction::Down)
            | (Direction::Neutral, Direction::Neutral)
    )
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
        BrtiEstimate {
            value,
            timestamp: ts,
            exchange_count: 4,
            confidence: 1.0,
        }
    }

    fn far_future_ms() -> u64 {
        // Year 2100 in ms — guaranteed to be in the future
        4_102_444_800_000
    }

    fn make_market(ticker: &str, yes_price: f64, no_price: f64, strike: f64) -> KalshiMarket {
        let now_ms = super::local_ts_ms();
        KalshiMarket {
            ticker: ticker.to_string(),
            yes_price,
            no_price,
            strike,
            open_time: now_ms.saturating_sub(60_000),
            closes_at: far_future_ms(),
            synthetic: false,
        }
    }

    #[test]
    fn no_opportunities_when_direction_neutral() {
        let (tx, rx) = watch::channel(vec![make_market("KXBTC15M-TEST", 50.0, 50.0, 70_000.0)]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);
        // Only one sample — delta detector has no direction yet
        let opps = engine.ingest(est(70_000.0, 1_000_000));
        assert!(opps.is_empty());
    }

    #[test]
    fn requires_two_consecutive_confirms_before_emitting() {
        // Market near 70_000 strike; BRTI climbs from 70_000 → 70_030 (within $30 of strike).
        let (tx, rx) = watch::channel(vec![make_market("KXBTC15M-A", 30.0, 70.0, 70_000.0)]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);

        // Prime with first sample (still neutral).
        let first = engine.ingest(est(70_000.0, 0));
        assert!(first.is_empty());

        // First non-neutral tick: confirm count = 1, should still be blocked.
        let second = engine.ingest(est(70_020.0, 1_000));
        assert!(
            second.is_empty(),
            "first non-neutral confirm should be blocked"
        );

        // Second same-direction tick: confirm count = 2, should now emit.
        let third = engine.ingest(est(70_025.0, 2_000));
        assert!(
            !third.is_empty(),
            "expected opportunity after persistence gate"
        );
    }

    #[test]
    fn opportunity_generated_when_edge_cleared() {
        let (tx, rx) = watch::channel(vec![
            // yes_price=15¢ → implied_prob=0.15; logistic model gives ~0.25 at distance=$10,
            // vel=0 (spike window has no data 50s after the pump) → edge ≈ 0.10 > min_edge=0.05
            make_market("KXBTC15M-A", 15.0, 85.0, 70_000.0),
        ]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);

        // Pump 10 estimates near 70_000 strike to establish Up direction.
        // BRTI moves $2/s from 69_990 → 70_008 (all within $30 of strike=70_000).
        for i in 0..10u64 {
            engine.ingest(est(69_990.0 + i as f64 * 2.0, i * 1_000));
        }
        // Final call at 50s (>30s past any cooldown set during pump): BRTI at 70_010.
        let opps = engine.ingest(est(70_010.0, 50_000));
        assert!(!opps.is_empty(), "expected at least one opportunity");
        let opp = &opps[0];
        assert!(
            opp.edge >= 0.05,
            "edge {:.3} should be >= min_edge 0.05",
            opp.edge
        );
        assert!(opp.kelly_fraction >= 0.0);
    }

    #[test]
    fn second_opportunity_for_same_ticker_is_suppressed() {
        let (tx, rx) = watch::channel(vec![make_market("KXBTC15M-A", 15.0, 85.0, 70_000.0)]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);

        // Pump establishes direction near strike.
        for i in 0..10u64 {
            engine.ingest(est(69_990.0 + i as f64 * 2.0, i * 1_000));
        }

        // First explicit call at 50s — past the pump cooldown window, should fire.
        let opps1 = engine.ingest(est(70_010.0, 50_000));
        assert!(
            !opps1.is_empty(),
            "first call should produce an opportunity"
        );

        // Second call 1ms later — within the 30s cooldown, should be suppressed.
        let opps2 = engine.ingest(est(70_012.0, 50_001));
        assert!(
            opps2.is_empty(),
            "second call within cooldown window should be suppressed"
        );
    }

    #[test]
    fn market_expiring_within_5_minutes_is_skipped() {
        let now_ms = super::local_ts_ms();
        let (tx, rx) = watch::channel(vec![KalshiMarket {
            ticker: "EXPIRING".to_string(),
            yes_price: 10.0,
            no_price: 90.0,
            strike: 70_000.0,
            open_time: now_ms.saturating_sub(60_000),
            closes_at: now_ms + 30_000, // closes in 30s — should be skipped
            synthetic: false,
        }]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);
        for i in 0..11u64 {
            engine.ingest(est(69_990.0 + i as f64 * 2.0, i * 1_000));
        }
        let opps = engine.ingest(est(70_012.0, 11_000));
        assert!(opps.is_empty(), "expiring market should be skipped");
    }

    #[test]
    fn conflicting_signals_produce_no_opportunity() {
        // spike=Up + delta=Down → Neutral → no trade (previously this fired Up due to OR bias)
        // Use a market near the strike so the distance filter isn't the reason for no trade.
        let (tx, rx) = watch::channel(vec![make_market("KXBTC15M-B", 30.0, 70.0, 70_000.0)]);
        drop(tx);
        let mut engine = SignalEngine::new(rx);

        // Pump down hard (-$3/step over 20 steps at 60s intervals) so the 20-sample
        // delta buffer shows a strong downward trend that survives a single Up spike.
        // 70_060 → 70_003 = -$57 total, delta_pct ≈ -0.081% >> threshold.
        for i in 0..20u64 {
            engine.ingest(est(70_060.0 - i as f64 * 3.0, i * 60_000));
        }

        // At base_ts (large gap — all pump samples evicted from 30s spike window).
        // Push one stable sample then a +$20 Up spike in 5s.
        // spike: +$20 > up_threshold=$15 → Up
        // delta buffer: oldest≈70_057 newest≈70_020 → still strongly Down
        // Conflicting → Neutral → no opportunities
        let base_ts = 20 * 60_000;
        engine.ingest(est(70_003.0, base_ts));
        let opps = engine.ingest(est(70_023.0, base_ts + 5_000));
        assert!(
            opps.is_empty(),
            "conflicting spike/delta signals should produce no opportunity"
        );
    }
}
