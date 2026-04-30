/// Fractional Kelly criterion for Kalshi binary markets.
///
/// - `edge`: model_prob − implied_prob (the probability edge over the market price)
/// - `odds`: 100.0 / price_cents — gross payout (e.g. 23¢ YES → 4.35x)
/// - `kelly_multiplier`: fractional Kelly scale (e.g. 0.25 for quarter-Kelly)
///
/// Derives model_prob = implied_prob + edge, then applies standard Kelly:
///   f* = (model_prob × net_odds − (1 − model_prob)) / net_odds
/// where net_odds = odds − 1 = (100 − price_cents) / price_cents.
/// Result is clamped to [0, 1] before applying kelly_multiplier.
pub fn kelly_fraction(edge: f64, odds: f64, kelly_multiplier: f64) -> f64 {
    if odds <= 1.0 {
        return 0.0; // degenerate: price >= 100¢
    }
    let implied_prob = 1.0 / odds; // price_cents / 100
    let model_prob = implied_prob + edge;
    let net_odds = odds - 1.0; // (100 - price_cents) / price_cents
    let kelly = (model_prob * net_odds - (1.0 - model_prob)) / net_odds;
    (kelly * kelly_multiplier).max(0.0)
}

/// Calculate the number of contracts to buy given Kelly sizing constraints.
///
/// Steps:
/// 1. raw_amount = kelly_fraction * bankroll_cents
/// 2. contracts = floor(raw_amount / yes_price_cents)
/// 3. Cap so total cost (contracts * yes_price_cents) does not exceed max_position_cents
/// 4. Cap at max_contracts (Kalshi per-market position limit, env: MAX_CONTRACTS)
/// 5. Returns 0 if yes_price_cents is 0 or Kelly sizing produces no contracts
///
/// No floor-at-1 minimum: if Kelly says don't trade, we don't trade.
pub fn compute_contracts(
    kelly_fraction: f64,
    bankroll_cents: u64,
    yes_price_cents: f64,
    max_position_cents: u64,
    max_contracts: u64,
) -> u64 {
    if yes_price_cents <= 0.0 {
        return 0;
    }
    let price_cents_u64 = yes_price_cents as u64;
    if price_cents_u64 == 0 {
        return 0;
    }
    let raw_amount = kelly_fraction * bankroll_cents as f64;
    let mut contracts = (raw_amount / yes_price_cents).floor() as u64;
    // Cap total cost to max position size.
    let max_contracts_by_cost = max_position_cents / price_cents_u64;
    contracts = contracts.min(max_contracts_by_cost);
    contracts = contracts.min(max_contracts);
    debug_assert!(contracts * price_cents_u64 <= max_position_cents);
    contracts
}

#[cfg(test)]
mod tests {
    use super::{compute_contracts, kelly_fraction};

    #[test]
    fn kelly_positive_for_cheap_market_with_edge() {
        // edge=0.10 on a 23¢ market: model_prob=0.33, net_odds≈3.35 → Kelly>0 before scaling
        let k = kelly_fraction(0.10, 100.0 / 23.0, 0.25);
        assert!(
            k > 0.0,
            "kelly={k} should be >0 for positive-edge trade on 23¢ market"
        );
    }

    #[test]
    fn kelly_positive_for_even_money_with_edge() {
        // 50¢ market (1:1 net odds), edge=0.05 → model_prob=0.55 → positive Kelly
        let k = kelly_fraction(0.05, 100.0 / 50.0, 0.25);
        assert!(
            k > 0.0,
            "kelly={k} should be >0 for positive-edge trade at even money"
        );
    }

    #[test]
    fn kelly_floors_to_zero_for_negative_edge() {
        // Negative edge → unfavorable trade → Kelly must be 0
        let k = kelly_fraction(-0.05, 100.0 / 50.0, 0.25);
        assert_eq!(k, 0.0, "kelly={k} should be 0 for negative-edge trade");
    }

    #[test]
    fn returns_zero_for_zero_price() {
        assert_eq!(compute_contracts(0.25, 10_000, 0.0, 500, 100), 0);
    }

    #[test]
    fn returns_zero_for_negative_price() {
        assert_eq!(compute_contracts(0.25, 10_000, -1.0, 500, 100), 0);
    }

    #[test]
    fn basic_sizing() {
        // kelly=0.1, bankroll=10_000 → raw=1000 cents
        // price=45 → contracts=floor(1000/45)=22 → min(22, floor(500/45)=11) = 11
        assert_eq!(compute_contracts(0.1, 10_000, 45.0, 500, 100), 11);
    }

    #[test]
    fn returns_zero_when_kelly_is_tiny() {
        // kelly=0.0001, bankroll=100 → raw=0.01 → contracts=0 — no floor, returns 0
        assert_eq!(compute_contracts(0.0001, 100, 50.0, 500, 100), 0);
    }

    #[test]
    fn returns_zero_for_zero_bankroll() {
        // raw=0 → contracts=0 — no floor, returns 0
        assert_eq!(compute_contracts(0.25, 0, 50.0, 500, 100), 0);
    }

    #[test]
    fn capped_by_max_position() {
        // kelly=1.0, bankroll=100_000 → raw=100_000 → contracts=floor(100000/10)=10_000
        // max_from_limit = floor(500/10)=50 → min(10_000, 50) = 50, under max_contracts=100
        assert_eq!(compute_contracts(1.0, 100_000, 10.0, 500, 100), 50);
    }

    #[test]
    fn capped_by_max_contracts() {
        // kelly=1.0, bankroll=100_000, price=10¢, max_pos=5000 → floor(5000/10)=500
        // max_contracts=100 → min(500, 100) = 100
        assert_eq!(compute_contracts(1.0, 100_000, 10.0, 5_000, 100), 100);
    }

    #[test]
    fn returns_zero_when_price_exceeds_max_position() {
        // price=600 > max_position=500 → max_from_limit=floor(500/600)=0 → 0
        assert_eq!(compute_contracts(0.25, 10_000, 600.0, 500, 100), 0);
    }
}
