use shared_types::{BrtiEstimate, Direction};
use std::collections::VecDeque;

pub struct BrtiDeltaDetector {
    buffer: VecDeque<BrtiEstimate>,
}

impl BrtiDeltaDetector {
    pub fn new() -> Self {
        BrtiDeltaDetector {
            buffer: VecDeque::new(),
        }
    }

    /// Append an estimate; evict oldest if buffer exceeds 20 samples.
    pub fn push(&mut self, estimate: BrtiEstimate) {
        self.buffer.push_back(estimate);
        if self.buffer.len() > 20 {
            self.buffer.pop_front();
        }
    }

    /// Percent change from oldest to newest sample: (newest - oldest) / oldest * 100.
    /// Returns None if fewer than 2 samples or oldest price is zero.
    pub fn delta_pct(&self) -> Option<f64> {
        if self.buffer.len() < 2 {
            return None;
        }
        let oldest = self.buffer.front()?;
        let newest = self.buffer.back()?;
        if oldest.value == 0.0 {
            return None;
        }
        Some((newest.value - oldest.value) / oldest.value * 100.0)
    }

    /// Rate of change in $/second between oldest and newest sample.
    /// Returns None if fewer than 2 samples or timestamps are identical.
    pub fn velocity(&self) -> Option<f64> {
        if self.buffer.len() < 2 {
            return None;
        }
        let oldest = self.buffer.front()?;
        let newest = self.buffer.back()?;
        let elapsed_ms = newest.timestamp.saturating_sub(oldest.timestamp);
        if elapsed_ms == 0 {
            return None;
        }
        let delta = newest.value - oldest.value;
        let time_secs = elapsed_ms as f64 / 1000.0;
        let velocity = delta / time_secs;
        debug_assert!(
            velocity.abs() < 10_000.0,
            "velocity out of range: {}",
            velocity
        );
        Some(velocity)
    }

    /// Classify the current momentum relative to threshold_pct.
    pub fn direction(&self, threshold_pct: f64) -> Direction {
        match self.delta_pct() {
            Some(d) if d > threshold_pct => Direction::Up,
            Some(d) if d < -threshold_pct => Direction::Down,
            _ => Direction::Neutral,
        }
    }
}

impl Default for BrtiDeltaDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::BrtiDeltaDetector;
    use shared_types::BrtiEstimate;

    fn est(value: f64, ts_ms: u64) -> BrtiEstimate {
        BrtiEstimate {
            value,
            timestamp: ts_ms,
            exchange_count: 2,
            confidence: 1.0,
        }
    }

    #[test]
    fn direction_up_on_increasing_prices() {
        let mut d = BrtiDeltaDetector::new();
        for i in 0..5u64 {
            d.push(est(60_000.0 + i as f64 * 50.0, 1000 * i));
        }
        assert!(matches!(d.direction(0.01), shared_types::Direction::Up));
    }

    #[test]
    fn direction_down_on_decreasing_prices() {
        let mut d = BrtiDeltaDetector::new();
        for i in 0..5u64 {
            d.push(est(60_000.0 - i as f64 * 50.0, 1000 * i));
        }
        assert!(matches!(d.direction(0.01), shared_types::Direction::Down));
    }

    #[test]
    fn direction_neutral_on_flat_prices() {
        let mut d = BrtiDeltaDetector::new();
        for i in 0..5u64 {
            d.push(est(60_000.0, 1000 * i));
        }
        assert!(matches!(
            d.direction(0.01),
            shared_types::Direction::Neutral
        ));
    }

    #[test]
    fn delta_pct_none_with_single_sample() {
        let mut d = BrtiDeltaDetector::new();
        d.push(est(60_000.0, 0));
        assert!(d.delta_pct().is_none());
    }

    #[test]
    fn buffer_capped_at_20() {
        let mut d = BrtiDeltaDetector::new();
        for i in 0..25u64 {
            d.push(est(60_000.0 + i as f64, 1000 * i));
        }
        assert_eq!(d.buffer.len(), 20);
    }

    #[test]
    fn velocity_in_dollars_per_second() {
        let mut d = BrtiDeltaDetector::new();
        // Price rises $100 over 2 seconds → velocity = 50 $/s
        d.push(est(60_000.0, 0));
        d.push(est(60_100.0, 2_000));
        let v = d.velocity().expect("should have velocity");
        assert!((v - 50.0).abs() < 0.01);
    }
}
