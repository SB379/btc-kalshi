/// Calculate the number of contracts to buy given Kelly sizing constraints.
///
/// Steps:
/// 1. raw_amount = kelly_fraction * bankroll_cents
/// 2. contracts = floor(raw_amount / yes_price_cents), then floor at 1
/// 3. Cap so total cost (contracts * yes_price_cents) does not exceed max_position_cents
/// 4. Returns 0 if yes_price_cents is 0 (avoid division by zero)
pub fn compute_contracts(
    kelly_fraction: f64,
    bankroll_cents: u64,
    yes_price_cents: f64,
    max_position_cents: u64,
) -> u64 {
    if yes_price_cents <= 0.0 {
        return 0;
    }
    let raw_amount = kelly_fraction * bankroll_cents as f64;
    let contracts = (raw_amount / yes_price_cents).floor() as u64;
    // Floor at 1 contract (always try to place at least one)
    let contracts = contracts.max(1);
    // Cap so total cost stays within the per-position limit
    let max_from_limit = (max_position_cents as f64 / yes_price_cents).floor() as u64;
    contracts.min(max_from_limit)
}

#[cfg(test)]
mod tests {
    use super::compute_contracts;

    #[test]
    fn returns_zero_for_zero_price() {
        assert_eq!(compute_contracts(0.25, 10_000, 0.0, 500), 0);
    }

    #[test]
    fn returns_zero_for_negative_price() {
        assert_eq!(compute_contracts(0.25, 10_000, -1.0, 500), 0);
    }

    #[test]
    fn basic_sizing() {
        // kelly=0.1, bankroll=10_000 → raw=1000 cents
        // price=45 → contracts=floor(1000/45)=22 → min(22, floor(500/45)=11) = 11
        assert_eq!(compute_contracts(0.1, 10_000, 45.0, 500), 11);
    }

    #[test]
    fn floors_at_one_when_kelly_is_tiny() {
        // kelly=0.0001, bankroll=100 → raw=0.01 → contracts=0 → floored to 1
        // max_from_limit = floor(500/50) = 10 → min(1,10) = 1
        assert_eq!(compute_contracts(0.0001, 100, 50.0, 500), 1);
    }

    #[test]
    fn capped_by_max_position() {
        // kelly=1.0, bankroll=100_000 → raw=100_000 → contracts=floor(100000/10)=10_000
        // max_from_limit = floor(500/10)=50 → min(10_000, 50) = 50
        assert_eq!(compute_contracts(1.0, 100_000, 10.0, 500), 50);
    }

    #[test]
    fn returns_zero_when_price_exceeds_max_position() {
        // price=600 > max_position=500 → max_from_limit=floor(500/600)=0 → 0
        assert_eq!(compute_contracts(0.25, 10_000, 600.0, 500), 0);
    }

    #[test]
    fn zero_bankroll_floors_to_one_if_affordable() {
        // raw=0 → contracts=0 → floored to 1
        // max_from_limit = floor(500/50)=10 → min(1,10) = 1
        assert_eq!(compute_contracts(0.25, 0, 50.0, 500), 1);
    }
}
