use std::collections::VecDeque;

pub struct BrtiValidator {
    errors: VecDeque<f64>,
}

impl BrtiValidator {
    pub fn new() -> Self {
        BrtiValidator {
            errors: VecDeque::new(),
        }
    }

    /// Record a prediction/actual pair. When actual is Some, stores (actual - estimate).
    /// Buffer is capped at 100 samples (oldest evicted first).
    pub fn record(&mut self, estimate: f64, actual: Option<f64>) {
        if let Some(actual_val) = actual {
            self.errors.push_back(actual_val - estimate);
            if self.errors.len() > 100 {
                self.errors.pop_front();
            }
        }
    }

    /// Mean absolute error over the rolling sample buffer. Returns 0.0 if no samples.
    pub fn mean_absolute_error(&self) -> f64 {
        if self.errors.is_empty() {
            return 0.0;
        }
        self.errors.iter().map(|e| e.abs()).sum::<f64>() / self.errors.len() as f64
    }
}

impl Default for BrtiValidator {
    fn default() -> Self {
        Self::new()
    }
}
