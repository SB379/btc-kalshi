use shared_types::{BrtiEstimate, Direction};
use std::collections::VecDeque;

pub struct SpikeDetector {
    window_secs: u64,
    /// Minimum upward move in dollars to fire Direction::Up
    up_threshold_dollars: f64,
    /// Minimum downward move in dollars to fire Direction::Down (stricter than up)
    down_threshold_dollars: f64,
    samples: VecDeque<BrtiEstimate>,
}

impl SpikeDetector {
    pub fn new(window_secs: u64, up_threshold_dollars: f64, down_threshold_dollars: f64) -> Self {
        Self {
            window_secs,
            up_threshold_dollars,
            down_threshold_dollars,
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, estimate: BrtiEstimate) {
        self.samples.push_back(estimate);
        // Evict samples older than window_secs from the front
        let cutoff = self
            .samples
            .back()
            .map(|s| s.timestamp.saturating_sub(self.window_secs * 1000))
            .unwrap_or(0);
        while self.samples.front().is_some_and(|s| s.timestamp < cutoff) {
            self.samples.pop_front();
        }
    }

    /// Direction based on net price change across the rolling window.
    ///
    /// Asymmetric thresholds: Down requires a larger move than Up because downward
    /// spikes mean-revert more aggressively in the data (backtest: NO accuracy 47%).
    /// Up fires at >$15; Down fires at >$25.
    pub fn direction(&self) -> Direction {
        if self.samples.len() < 2 {
            return Direction::Neutral;
        }
        let oldest = match self.samples.front() {
            Some(s) => s.value,
            None => return Direction::Neutral,
        };
        let newest = match self.samples.back() {
            Some(s) => s.value,
            None => return Direction::Neutral,
        };
        let delta = newest - oldest;
        if delta > self.up_threshold_dollars {
            Direction::Up
        } else if delta < -self.down_threshold_dollars {
            Direction::Down
        } else {
            Direction::Neutral
        }
    }

    /// Net dollar move across the rolling window (positive = up, negative = down).
    pub fn delta(&self) -> f64 {
        let oldest = self.samples.front().map(|s| s.value).unwrap_or(0.0);
        let newest = self.samples.back().map(|s| s.value).unwrap_or(0.0);
        newest - oldest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn est(value: f64, ts: u64) -> BrtiEstimate {
        BrtiEstimate {
            value,
            timestamp: ts,
            exchange_count: 4,
            confidence: 0.5,
        }
    }

    #[test]
    fn detects_upward_spike() {
        // $20 move exceeds up_threshold=15
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(70_020.0, 5_000));
        assert!(matches!(d.direction(), Direction::Up));
    }

    #[test]
    fn detects_downward_spike() {
        // $30 drop exceeds down_threshold=25
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(69_970.0, 5_000));
        assert!(matches!(d.direction(), Direction::Down));
    }

    #[test]
    fn downward_spike_below_threshold_is_neutral() {
        // $20 drop does NOT exceed down_threshold=25 — asymmetric filter in action
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(69_980.0, 5_000));
        assert!(matches!(d.direction(), Direction::Neutral));
    }

    #[test]
    fn neutral_below_threshold() {
        // $10 move does not exceed up_threshold=15
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(70_010.0, 5_000));
        assert!(matches!(d.direction(), Direction::Neutral));
    }

    #[test]
    fn evicts_old_samples() {
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(70_020.0, 5_000));
        // Sample 40s later — the first two samples are evicted, delta drops to ~$1
        d.push(est(70_021.0, 40_000));
        assert!(matches!(d.direction(), Direction::Neutral));
    }

    #[test]
    fn delta_returns_net_move() {
        let mut d = SpikeDetector::new(30, 15.0, 25.0);
        d.push(est(70_000.0, 0));
        d.push(est(70_030.0, 5_000));
        assert!((d.delta() - 30.0).abs() < f64::EPSILON);
    }
}
