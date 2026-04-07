use shared_types::Direction;

/// Convert a Kalshi YES price in cents to an implied probability.
pub fn implied_probability(yes_price_cents: f64) -> f64 {
    yes_price_cents / 100.0
}

/// Estimate the model's probability given the BRTI direction, feed confidence, and velocity.
///
/// Base probabilities: Up=0.65, Down=0.35, Neutral=0.50.
/// Velocity adjustment: +/- 0.03 per 10 $/s magnitude, applied toward predicted direction.
/// Clamped to [0.10, 0.90] before confidence discount.
/// Confidence discount: result * (0.7 + brti_confidence * 0.3), so full confidence = no discount.
pub fn model_probability(direction: &Direction, brti_confidence: f64, velocity: f64) -> f64 {
    let base: f64 = match direction {
        Direction::Up => 0.65,
        Direction::Down => 0.35,
        Direction::Neutral => 0.50,
    };

    // For every 10 $/s of velocity magnitude, shift probability toward the predicted extreme.
    let velocity_adj = (velocity.abs() / 10.0) * 0.03;
    let adjusted = match direction {
        Direction::Up => base + velocity_adj,
        Direction::Down => base - velocity_adj,
        Direction::Neutral => base,
    };
    let clamped = adjusted.clamp(0.10, 0.90);

    // Discount by BRTI confidence: at confidence=1.0 multiplier=1.0, at confidence=0.0 multiplier=0.7
    clamped * (0.7 + brti_confidence * 0.3)
}

/// Fractional Kelly position size, floored at 0.
///
/// Standard Kelly criterion: (edge * odds - (1 - edge)) / odds, scaled by kelly_multiplier.
pub fn kelly_fraction(edge: f64, odds: f64, kelly_multiplier: f64) -> f64 {
    let kelly = (edge * odds - (1.0 - edge)) / odds;
    (kelly * kelly_multiplier).max(0.0)
}

#[cfg(test)]
mod tests {
    use super::{implied_probability, kelly_fraction, model_probability};
    use shared_types::Direction;

    #[test]
    fn implied_probability_maps_cents_to_fraction() {
        assert!((implied_probability(50.0) - 0.50).abs() < f64::EPSILON);
        assert!((implied_probability(0.0) - 0.0).abs() < f64::EPSILON);
        assert!((implied_probability(100.0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn model_probability_base_cases_no_velocity() {
        // With zero velocity and full confidence, result = base
        assert!((model_probability(&Direction::Up, 1.0, 0.0) - 0.65).abs() < 1e-9);
        assert!((model_probability(&Direction::Down, 1.0, 0.0) - 0.35).abs() < 1e-9);
        assert!((model_probability(&Direction::Neutral, 1.0, 0.0) - 0.50).abs() < 1e-9);
    }

    #[test]
    fn model_probability_velocity_adjusts_upward_for_up_direction() {
        // 50 $/s velocity → adj = (50/10) * 0.03 = 0.15 → 0.65 + 0.15 = 0.80
        let p = model_probability(&Direction::Up, 1.0, 50.0);
        assert!((p - 0.80).abs() < 1e-9);
    }

    #[test]
    fn model_probability_velocity_adjusts_downward_for_down_direction() {
        // 50 $/s velocity → 0.35 - 0.15 = 0.20
        let p = model_probability(&Direction::Down, 1.0, 50.0);
        assert!((p - 0.20).abs() < 1e-9);
    }

    #[test]
    fn model_probability_capped_at_0_90() {
        // 1000 $/s velocity → adj = 3.0 → 0.65 + 3.0 = 3.65 → clamped to 0.90
        let p = model_probability(&Direction::Up, 1.0, 1000.0);
        assert!((p - 0.90).abs() < 1e-9);
    }

    #[test]
    fn model_probability_floored_at_0_10() {
        // 1000 $/s, Down direction → 0.35 - 3.0 = -2.65 → clamped to 0.10
        let p = model_probability(&Direction::Down, 1.0, 1000.0);
        assert!((p - 0.10).abs() < 1e-9);
    }

    #[test]
    fn model_probability_confidence_discount_at_zero() {
        // brti_confidence=0.0 → multiplier = 0.7 → 0.65 * 0.7 = 0.455
        let p = model_probability(&Direction::Up, 0.0, 0.0);
        assert!((p - 0.455).abs() < 1e-9);
    }

    #[test]
    fn kelly_fraction_positive_edge() {
        // edge=0.6, odds=2.0 → kelly = (0.6*2.0 - 0.4) / 2.0 = (1.2 - 0.4) / 2.0 = 0.4 → scaled: 0.4*0.25 = 0.1
        let k = kelly_fraction(0.6, 2.0, 0.25);
        assert!((k - 0.1).abs() < 1e-9);
    }

    #[test]
    fn kelly_fraction_floored_at_zero_when_negative() {
        // edge=0.0, odds=1.0 → kelly = (0 - 1)/1 = -1 → scaled: -0.25 → floor: 0.0
        let k = kelly_fraction(0.0, 1.0, 0.25);
        assert!((k - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn kelly_fraction_zero_edge_gives_zero() {
        let k = kelly_fraction(0.0, 2.0, 0.25);
        assert_eq!(k, 0.0);
    }
}
