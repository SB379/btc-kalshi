use shared_types::TradeSide;
use std::collections::HashMap;

#[derive(Clone)]
pub struct OpenPosition {
    pub order_id: String,
    pub ticker: String,
    pub side: TradeSide,
    pub contracts: u64,
    pub entry_price_cents: f64,
    /// BRTI value at time of entry — used for reversal detection
    pub entry_brti: f64,
    pub opened_at_ms: u64,
    /// Market expiry from KalshiMarket.closes_at
    pub closes_at_ms: u64,
    /// Highest bid price seen since entry. Updated by the reconciliation loop each cycle.
    /// Used to arm and evaluate the trailing stop.
    pub peak_price_cents: f64,
}

#[derive(Debug)]
pub enum ExitReason {
    StopLoss {
        entry_cents: f64,
        current_cents: f64,
    },
    ProfitTarget {
        entry_cents: f64,
        current_cents: f64,
    },
    TrailingStop {
        peak_cents: f64,
        current_cents: f64,
    },
    BrtiReversal {
        entry_brti: f64,
        current_brti: f64,
        delta: f64,
    },
    NearExpiry {
        seconds_remaining: u64,
    },
}

pub struct PositionTracker {
    positions: HashMap<String, OpenPosition>,
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl PositionTracker {
    pub fn new() -> Self {
        PositionTracker {
            positions: HashMap::new(),
        }
    }

    pub fn add(&mut self, pos: OpenPosition) {
        self.positions.insert(pos.ticker.clone(), pos);
    }

    pub fn remove(&mut self, ticker: &str) -> Option<OpenPosition> {
        self.positions.remove(ticker)
    }

    pub fn get_all(&self) -> &HashMap<String, OpenPosition> {
        &self.positions
    }

    pub fn count(&self) -> u8 {
        self.positions.len() as u8
    }

    /// Update peak_price_cents if current_price is higher, then return the updated position.
    /// Returns None if the ticker is not tracked (position was already removed).
    /// Call this in the reconciliation loop after fetching bids, before check_exit,
    /// so check_exit always sees an up-to-date peak.
    pub fn update_peak(&mut self, ticker: &str, current_price_cents: f64) -> Option<OpenPosition> {
        let pos = self.positions.get_mut(ticker)?;
        if current_price_cents > pos.peak_price_cents {
            pos.peak_price_cents = current_price_cents;
        }
        Some(pos.clone())
    }
}

/// Check a position for exit conditions given current market state.
/// Conditions are checked in priority order:
///   stop-loss → profit-target → trailing-stop → BRTI reversal → near expiry
pub fn check_exit(
    pos: &OpenPosition,
    current_price_cents: f64,
    current_brti: f64,
    now_ms: u64,
) -> Option<ExitReason> {
    // 1. Stop-loss: position has lost ≥60% of entry value
    if current_price_cents < pos.entry_price_cents * 0.4 {
        return Some(ExitReason::StopLoss {
            entry_cents: pos.entry_price_cents,
            current_cents: current_price_cents,
        });
    }

    // 2. Profit target: position has gained ≥45% of entry value.
    //    Floor: must be at least 10¢ of profit (avoids exiting cheap contracts on noise).
    //    Ceiling: 95¢ (avoids waiting for an impossible 100¢+ target on expensive entries).
    let profit_target = (pos.entry_price_cents * 1.45)
        .min(95.0)
        .max(pos.entry_price_cents + 10.0);
    if current_price_cents >= profit_target {
        return Some(ExitReason::ProfitTarget {
            entry_cents: pos.entry_price_cents,
            current_cents: current_price_cents,
        });
    }

    // 3. Trailing stop: once we've been up ≥10¢ from entry, exit if we give back ≥8¢ from peak.
    //    The 10¢ arm threshold prevents triggering on normal entry-level noise.
    if pos.peak_price_cents >= pos.entry_price_cents + 10.0
        && current_price_cents < pos.peak_price_cents - 8.0
    {
        return Some(ExitReason::TrailingStop {
            peak_cents: pos.peak_price_cents,
            current_cents: current_price_cents,
        });
    }

    // 4. BRTI reversal: underlying moved against our position by >$30 since entry
    let brti_delta = current_brti - pos.entry_brti;
    let reversed = match pos.side {
        TradeSide::Yes => brti_delta < -30.0, // bought YES, BRTI dropped hard
        TradeSide::No => brti_delta > 30.0,   // bought NO, BRTI rose hard
    };
    if reversed {
        return Some(ExitReason::BrtiReversal {
            entry_brti: pos.entry_brti,
            current_brti,
            delta: brti_delta,
        });
    }

    // 5. Near expiry: <90s remaining AND we are underwater — cut losses
    if pos.closes_at_ms > now_ms {
        let remaining_ms = pos.closes_at_ms - now_ms;
        if remaining_ms < 90_000 && current_price_cents < pos.entry_price_cents {
            return Some(ExitReason::NearExpiry {
                seconds_remaining: remaining_ms / 1000,
            });
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_types::TradeSide;

    fn make_pos(
        side: TradeSide,
        entry_price: f64,
        entry_brti: f64,
        closes_at_ms: u64,
    ) -> OpenPosition {
        OpenPosition {
            order_id: "ord-1".to_string(),
            ticker: "KXBTC15M-TEST".to_string(),
            side,
            contracts: 5,
            entry_price_cents: entry_price,
            peak_price_cents: entry_price,
            entry_brti,
            opened_at_ms: 0,
            closes_at_ms,
        }
    }

    fn make_pos_with_peak(
        side: TradeSide,
        entry_price: f64,
        peak_price: f64,
        entry_brti: f64,
        closes_at_ms: u64,
    ) -> OpenPosition {
        OpenPosition {
            order_id: "ord-1".to_string(),
            ticker: "KXBTC15M-TEST".to_string(),
            side,
            contracts: 5,
            entry_price_cents: entry_price,
            peak_price_cents: peak_price,
            entry_brti,
            opened_at_ms: 0,
            closes_at_ms,
        }
    }

    // --- stop-loss ---

    #[test]
    fn stop_loss_fires_at_60_pct_loss() {
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, u64::MAX);
        // 50 * 0.4 = 20 → current < 20 triggers stop-loss
        let result = check_exit(&pos, 19.0, 70_000.0, 0);
        assert!(matches!(result, Some(ExitReason::StopLoss { .. })));
    }

    #[test]
    fn stop_loss_does_not_fire_above_threshold() {
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, u64::MAX);
        // 20 = exactly 60% loss — not below threshold
        let result = check_exit(&pos, 20.0, 70_000.0, 0);
        assert!(result.is_none());
    }

    // --- profit target ---

    #[test]
    fn profit_target_fires_at_45_pct_gain() {
        // entry 45¢ → target = 45 * 1.45 = 65.25¢
        let pos = make_pos(TradeSide::Yes, 45.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 66.0, 70_000.0, 0);
        assert!(matches!(result, Some(ExitReason::ProfitTarget { .. })));
    }

    #[test]
    fn profit_target_does_not_fire_below_threshold() {
        let pos = make_pos(TradeSide::Yes, 45.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 64.0, 70_000.0, 0);
        assert!(result.is_none());
    }

    #[test]
    fn profit_target_enforces_minimum_10_cent_profit() {
        // entry 15¢ → 1.45× = 21.75¢, but floor is entry + 10 = 25¢
        let pos = make_pos(TradeSide::Yes, 15.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 24.0, 70_000.0, 0);
        assert!(
            result.is_none(),
            "should not fire below 10¢ minimum profit floor"
        );
        let result = check_exit(&pos, 25.0, 70_000.0, 0);
        assert!(matches!(result, Some(ExitReason::ProfitTarget { .. })));
    }

    #[test]
    fn profit_target_capped_at_95_cents() {
        // entry 75¢ → 1.45× = 108.75¢, capped to 95¢
        let pos = make_pos(TradeSide::Yes, 75.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 94.0, 70_000.0, 0);
        assert!(result.is_none(), "should not fire below 95¢ ceiling");
        let result = check_exit(&pos, 95.0, 70_000.0, 0);
        assert!(matches!(result, Some(ExitReason::ProfitTarget { .. })));
    }

    // --- trailing stop ---

    #[test]
    fn trailing_stop_fires_when_peak_gives_back_8_cents() {
        // Peak 68¢ (23¢ above entry 45¢ → arm threshold of 55¢ cleared).
        // Current 59¢ = gave back 9¢ from peak → fire.
        let pos = make_pos_with_peak(TradeSide::Yes, 45.0, 68.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 59.0, 70_000.0, 0);
        assert!(matches!(result, Some(ExitReason::TrailingStop { .. })));
    }

    #[test]
    fn trailing_stop_does_not_fire_without_10_cent_peak_gain() {
        // Peak 52¢ — only 7¢ above entry 45¢, arm threshold (55¢) not cleared.
        // Current 43¢ would normally trigger trail math, but stop is not armed.
        let pos = make_pos_with_peak(TradeSide::Yes, 45.0, 52.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 43.0, 70_000.0, 0);
        assert!(result.is_none());
    }

    #[test]
    fn trailing_stop_does_not_fire_within_trail() {
        // Peak 68¢, current 61¢ → gave back 7¢, under the 8¢ trail → hold.
        let pos = make_pos_with_peak(TradeSide::Yes, 45.0, 68.0, 70_000.0, u64::MAX);
        let result = check_exit(&pos, 61.0, 70_000.0, 0);
        assert!(result.is_none());
    }

    // --- BRTI reversal ---

    #[test]
    fn brti_reversal_fires_for_yes_position_on_drop() {
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, u64::MAX);
        // BRTI dropped $31 from entry → should exit YES
        let result = check_exit(&pos, 45.0, 69_969.0, 0);
        assert!(matches!(result, Some(ExitReason::BrtiReversal { .. })));
    }

    #[test]
    fn brti_reversal_fires_for_no_position_on_rise() {
        let pos = make_pos(TradeSide::No, 50.0, 70_000.0, u64::MAX);
        // BRTI rose $31 from entry → should exit NO
        let result = check_exit(&pos, 45.0, 70_031.0, 0);
        assert!(matches!(result, Some(ExitReason::BrtiReversal { .. })));
    }

    #[test]
    fn brti_reversal_does_not_fire_within_threshold() {
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, u64::MAX);
        // Only $29 drop — below $30 threshold
        let result = check_exit(&pos, 45.0, 69_971.0, 0);
        assert!(result.is_none());
    }

    // --- near expiry ---

    #[test]
    fn near_expiry_fires_when_underwater() {
        let now_ms = 1_000_000_u64;
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, now_ms + 60_000);
        let result = check_exit(&pos, 40.0, 70_000.0, now_ms);
        assert!(matches!(
            result,
            Some(ExitReason::NearExpiry {
                seconds_remaining: 60
            })
        ));
    }

    #[test]
    fn near_expiry_does_not_fire_when_profitable() {
        let now_ms = 1_000_000_u64;
        let pos = make_pos(TradeSide::Yes, 50.0, 70_000.0, now_ms + 60_000);
        // Current price above entry — hold to settlement
        let result = check_exit(&pos, 70.0, 70_000.0, now_ms);
        assert!(result.is_none());
    }

    // --- tracker ---

    #[test]
    fn tracker_add_remove_count() {
        let mut tracker = PositionTracker::new();
        assert_eq!(tracker.count(), 0);
        tracker.add(make_pos(TradeSide::Yes, 50.0, 70_000.0, u64::MAX));
        assert_eq!(tracker.count(), 1);
        let removed = tracker.remove("KXBTC15M-TEST");
        assert!(removed.is_some());
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn update_peak_raises_and_returns_position() {
        let mut tracker = PositionTracker::new();
        tracker.add(make_pos(TradeSide::Yes, 45.0, 70_000.0, u64::MAX));

        // Higher price updates peak
        let updated = tracker.update_peak("KXBTC15M-TEST", 60.0).unwrap();
        assert_eq!(updated.peak_price_cents, 60.0);

        // Lower price does not lower peak
        let updated2 = tracker.update_peak("KXBTC15M-TEST", 55.0).unwrap();
        assert_eq!(updated2.peak_price_cents, 60.0);
    }

    #[test]
    fn update_peak_returns_none_for_unknown_ticker() {
        let mut tracker = PositionTracker::new();
        assert!(tracker.update_peak("NONEXISTENT", 50.0).is_none());
    }
}
