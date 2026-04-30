use shared_types::Direction;

/// Convert a Kalshi YES price in cents to an implied probability.
pub fn implied_probability(yes_price_cents: f64) -> f64 {
    yes_price_cents / 100.0
}

/// Estimate the model's probability that YES resolves (BRTI ≥ strike at close).
///
/// Empirical logistic regression fitted on historical BRTI + opportunity logs.
/// See `data/backtest/fit_probability_model.py` to refit after collecting more data.
///
/// Features:
///   distance          = brti_est - strike  (positive = BRTI above strike)
///   seconds_to_close  = time until market expiry
///   realized_vol_30s  = velocity.abs() (proxy for BRTI stddev over the last 30s)
///
/// Coefficients last fitted: 2026-04-08, n=97, CV AUC=0.61, strike_filter=±$5.
/// Re-fit after each new observation session with ≥ 50 new ±$5 samples.
///
/// NOTE: model was fitted only on the ±$5 band. Outputs beyond that range are
/// extrapolations and may be counterintuitive. The engine's max_distance filter
/// prevents far-from-strike opportunities from reaching this function in practice.
///
/// Confidence discount: result × (0.7 + brti_confidence × 0.3).
/// Returns P(YES resolves). The caller flips this for NO bets: P(NO) = 1 - P(YES).
pub fn model_probability(
    _direction: &Direction, // no longer used in formula; kept for API compat
    brti_est: f64,
    strike: f64,
    seconds_to_close: f64,
    velocity: f64,     // abs value used as realized_vol proxy
    brti_confidence: f64,
) -> f64 {
    // Logistic regression coefficients — update after each re-fit.
    // Run: python3 data/backtest/fit_probability_model.py --dates YYYY-MM-DD --strike-filter 5
    const INTERCEPT: f64 = 1.261386;
    const COEF_DISTANCE: f64 = 0.082579; // sign-corrected: above strike → higher P(YES)
    const COEF_SECONDS: f64 = -0.001716;
    const COEF_VOL: f64 = 0.098562;

    let distance = brti_est - strike;
    let realized_vol = velocity.abs();
    // Cap to the training range (KXBTC15M markets are ≤ 15 min = 900s).
    // Prevents extrapolation when tests or edge cases pass unrealistic close times.
    let seconds_capped = seconds_to_close.clamp(0.0, 900.0);
    let z = INTERCEPT
        + COEF_DISTANCE * distance
        + COEF_SECONDS * seconds_capped
        + COEF_VOL * realized_vol;
    let yes_prob = 1.0 / (1.0 + (-z).exp());

    // Confidence discount: fewer contributing exchanges → lower reliability.
    let result = yes_prob * (0.7 + brti_confidence * 0.3);
    result.clamp(0.05, 0.95)
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
    fn model_probability_above_strike_gives_high_yes_prob() {
        // BRTI $5 above strike, 60s remaining → P(YES) > 0.65
        // Model was fitted on ±$5 band only; use near-money inputs for meaningful assertions.
        let p = model_probability(&Direction::Up, 70_005.0, 70_000.0, 60.0, 0.0, 1.0);
        assert!(p > 0.65, "got {p:.3}, expected > 0.65");
    }

    #[test]
    fn model_probability_below_strike_far_gives_low_yes_prob() {
        // BRTI $30 below strike, low velocity → P(YES) is low (distance penalty dominates).
        // A large upward spike would raise it; with velocity=0 it should be well below 50%.
        let p = model_probability(&Direction::Down, 69_970.0, 70_000.0, 600.0, 0.0, 1.0);
        assert!(p < 0.40, "got {p:.3}, expected < 0.40");
    }

    #[test]
    fn model_probability_at_strike_is_near_50() {
        // BRTI exactly at strike, 10 min remaining → P(YES) near 0.50–0.60
        // Intercept + time effect gives ~0.56 at 600s; within a reasonable band.
        let p = model_probability(&Direction::Neutral, 70_000.0, 70_000.0, 600.0, 0.0, 1.0);
        assert!(
            (p - 0.5).abs() < 0.15,
            "got {p:.3}, expected within 0.15 of 0.50"
        );
    }

    #[test]
    fn model_probability_time_compression_above_strike_near_expiry() {
        // BRTI $5 above strike, 60s remaining → shorter time → higher certainty → P(YES) > 0.65
        let p = model_probability(&Direction::Up, 70_005.0, 70_000.0, 60.0, 0.0, 1.0);
        assert!(
            p > 0.65,
            "got {p:.3}, expected > 0.65 near expiry above strike"
        );
    }

    #[test]
    fn model_probability_time_compression_more_time_lowers_prob() {
        // More time remaining → more uncertainty → P(YES) lower (seconds coef is negative).
        let p_short = model_probability(&Direction::Up, 70_005.0, 70_000.0, 60.0, 0.0, 1.0);
        let p_long = model_probability(&Direction::Up, 70_005.0, 70_000.0, 600.0, 0.0, 1.0);
        assert!(
            p_short > p_long,
            "60s ({p_short:.3}) should give higher P(YES) than 600s ({p_long:.3})"
        );
    }

    #[test]
    fn model_probability_confidence_discount_reduces_result() {
        // Low BRTI confidence (fewer exchanges) → lower result than full confidence.
        let full = model_probability(&Direction::Up, 70_005.0, 70_000.0, 60.0, 0.0, 1.0);
        let low = model_probability(&Direction::Up, 70_005.0, 70_000.0, 60.0, 0.0, 0.0);
        assert!(
            low < full,
            "low confidence ({low:.3}) should be < full ({full:.3})"
        );
    }

    #[test]
    fn kelly_fraction_positive_edge() {
        // edge=0.6, odds=2.0 → kelly = (0.6*2.0 - 0.4) / 2.0 = 0.4 → scaled: 0.4*0.25 = 0.1
        let k = kelly_fraction(0.6, 2.0, 0.25);
        assert!((k - 0.1).abs() < 1e-9);
    }

    #[test]
    fn kelly_fraction_floored_at_zero_when_negative() {
        // edge=0.0, odds=1.0 → kelly = -1 → floor: 0.0
        let k = kelly_fraction(0.0, 1.0, 0.25);
        assert!((k - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn kelly_fraction_zero_edge_gives_zero() {
        let k = kelly_fraction(0.0, 2.0, 0.25);
        assert_eq!(k, 0.0);
    }
}
