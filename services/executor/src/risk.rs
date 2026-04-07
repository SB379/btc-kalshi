use shared_types::TradeOpportunity;

pub struct RiskConfig {
    /// Maximum cost per position in cents (env: MAX_POSITION_SIZE_CENTS, default 500)
    pub max_position_cents: u64,
    /// Halt trading if cumulative daily loss exceeds this (env: MAX_DAILY_LOSS_CENTS, default 5000)
    pub max_daily_loss_cents: u64,
    /// Minimum required signal confidence (default 0.45)
    pub min_signal_confidence: f64,
    /// Minimum required edge over market implied probability (default 0.08)
    pub min_edge: f64,
    /// Maximum number of concurrently open positions (default 5)
    pub max_open_positions: u8,
}

impl RiskConfig {
    pub fn from_env() -> Self {
        RiskConfig {
            max_position_cents: std::env::var("MAX_POSITION_SIZE_CENTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(500),
            max_daily_loss_cents: std::env::var("MAX_DAILY_LOSS_CENTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5000),
            min_signal_confidence: 0.45,
            min_edge: 0.08,
            max_open_positions: 5,
        }
    }
}

pub struct RiskGate {
    config: RiskConfig,
    /// Running daily P&L in cents (negative = loss)
    daily_loss_cents: i64,
    /// Number of currently open positions
    open_positions: u8,
    /// Unix ms when the daily counters were last reset
    last_reset: u64,
}

#[derive(Debug)]
pub enum RiskViolation {
    ConfidenceTooLow { actual: f64, required: f64 },
    EdgeTooSmall { actual: f64, required: f64 },
    TooManyOpenPositions { current: u8, max: u8 },
    DailyLossLimitHit { loss_cents: i64, limit_cents: u64 },
}

impl RiskGate {
    pub fn new(config: RiskConfig) -> Self {
        RiskGate {
            config,
            daily_loss_cents: 0,
            open_positions: 0,
            last_reset: now_ms(),
        }
    }

    /// Returns Ok(()) only if all risk gates pass; otherwise the first failing gate.
    pub fn check(&self, opp: &TradeOpportunity) -> Result<(), RiskViolation> {
        if opp.signal.confidence < self.config.min_signal_confidence {
            return Err(RiskViolation::ConfidenceTooLow {
                actual: opp.signal.confidence,
                required: self.config.min_signal_confidence,
            });
        }
        if opp.edge < self.config.min_edge {
            return Err(RiskViolation::EdgeTooSmall {
                actual: opp.edge,
                required: self.config.min_edge,
            });
        }
        if self.open_positions >= self.config.max_open_positions {
            return Err(RiskViolation::TooManyOpenPositions {
                current: self.open_positions,
                max: self.config.max_open_positions,
            });
        }
        if self.daily_loss_cents <= -(self.config.max_daily_loss_cents as i64) {
            return Err(RiskViolation::DailyLossLimitHit {
                loss_cents: self.daily_loss_cents,
                limit_cents: self.config.max_daily_loss_cents,
            });
        }
        Ok(())
    }

    /// Record that a new position was filled at the given cost.
    pub fn record_fill(&mut self, _cost_cents: u64) {
        self.open_positions = self.open_positions.saturating_add(1);
    }

    /// Record a P&L update (positive = profit, negative = loss).
    pub fn record_pnl(&mut self, pnl_cents: i64) {
        self.daily_loss_cents = self.daily_loss_cents.saturating_add(pnl_cents);
    }

    /// Reset daily counters if now_ms has crossed midnight UTC since the last reset.
    pub fn maybe_reset_daily(&mut self, now_ms: u64) {
        // Compare calendar days since Unix epoch (each day = 86_400_000 ms)
        let last_day = self.last_reset / 86_400_000;
        let today = now_ms / 86_400_000;
        if today > last_day {
            self.daily_loss_cents = 0;
            self.last_reset = now_ms;
        }
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{RiskConfig, RiskGate, RiskViolation};
    use shared_types::{
        BrtiEstimate, Direction, KalshiMarket, Signal, TradeOpportunity, TradeSide,
    };

    fn make_opp(confidence: f64, edge: f64) -> TradeOpportunity {
        TradeOpportunity {
            market: KalshiMarket {
                ticker: "TEST".to_string(),
                yes_price: 45.0,
                no_price: 55.0,
                strike: 70_000.0,
                closes_at: 9_999_999_999_999,
            },
            signal: Signal {
                direction: Direction::Up,
                confidence,
                brti_est: BrtiEstimate {
                    value: 70_000.0,
                    timestamp: 0,
                    exchange_count: 4,
                    confidence,
                },
                generated_at: 0,
            },
            side: TradeSide::Yes,
            edge,
            kelly_fraction: 0.1,
        }
    }

    fn default_config() -> RiskConfig {
        RiskConfig {
            max_position_cents: 500,
            max_daily_loss_cents: 5000,
            min_signal_confidence: 0.45,
            min_edge: 0.08,
            max_open_positions: 5,
        }
    }

    #[test]
    fn confidence_too_low_trips_gate() {
        let gate = RiskGate::new(default_config());
        let opp = make_opp(0.40, 0.15); // confidence below 0.45
        let err = gate.check(&opp).expect_err("should trip confidence gate");
        assert!(matches!(err, RiskViolation::ConfidenceTooLow { .. }));
    }

    #[test]
    fn edge_too_small_trips_gate() {
        let gate = RiskGate::new(default_config());
        let opp = make_opp(0.70, 0.03); // edge below 0.08
        let err = gate.check(&opp).expect_err("should trip edge gate");
        assert!(matches!(err, RiskViolation::EdgeTooSmall { .. }));
    }

    #[test]
    fn too_many_positions_trips_gate() {
        let mut gate = RiskGate::new(default_config());
        // Fill 5 positions (the maximum)
        for _ in 0..5 {
            gate.record_fill(100);
        }
        let opp = make_opp(0.70, 0.15);
        let err = gate.check(&opp).expect_err("should trip position count gate");
        assert!(matches!(err, RiskViolation::TooManyOpenPositions { .. }));
    }

    #[test]
    fn daily_loss_limit_trips_gate() {
        let mut gate = RiskGate::new(default_config());
        // Record a loss equal to the daily limit
        gate.record_pnl(-5000);
        let opp = make_opp(0.70, 0.15);
        let err = gate.check(&opp).expect_err("should trip daily loss gate");
        assert!(matches!(err, RiskViolation::DailyLossLimitHit { .. }));
    }

    #[test]
    fn all_gates_pass_when_within_limits() {
        let gate = RiskGate::new(default_config());
        let opp = make_opp(0.70, 0.15);
        assert!(gate.check(&opp).is_ok());
    }

    #[test]
    fn daily_reset_clears_loss() {
        let mut gate = RiskGate::new(default_config());
        gate.record_pnl(-5000);

        // Simulate crossing midnight: advance by 24h + 1ms
        let tomorrow = gate.last_reset + 86_400_001;
        gate.maybe_reset_daily(tomorrow);

        let opp = make_opp(0.70, 0.15);
        assert!(gate.check(&opp).is_ok(), "loss should be reset after midnight");
    }
}
