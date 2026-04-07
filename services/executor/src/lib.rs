#![deny(clippy::unwrap_used)]

pub mod auth;
pub mod client;
pub mod risk;
pub mod sizing;

use client::KalshiClient;
use ringbuf::traits::Consumer;
use ringbuf::HeapCons;
use risk::{RiskConfig, RiskGate};
use shared_types::{TradeOpportunity, TradeSide};
use sizing::compute_contracts;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

/// Consume TradeOpportunity structs from the signal ring buffer, gate each through risk checks,
/// size the position, and place a limit order on Kalshi.
///
/// Hard-fails at startup if KALSHI_USE_DEMO is not set in the environment.
pub async fn run(mut consumer: HeapCons<TradeOpportunity>) -> Result<(), anyhow::Error> {
    let client = KalshiClient::from_env()?;

    if client.use_demo {
        info!("executor starting in DEMO mode");
    } else {
        warn!("executor starting in LIVE mode — real money");
    }

    let balance = match client.get_balance().await {
        Ok(b) => {
            info!(balance_cents = b, "initial balance fetched");
            b
        }
        Err(e) => {
            warn!(error = %e, "failed to fetch initial balance, proceeding with 0");
            0
        }
    };
    let _ = balance; // Initial log only; balance is re-fetched before each order

    let risk_config = RiskConfig::from_env();
    let max_position_cents = risk_config.max_position_cents;
    let mut risk_gate = RiskGate::new(risk_config);

    loop {
        while let Some(opp) = consumer.try_pop() {
            let now = local_ts_ms();
            risk_gate.maybe_reset_daily(now);

            if let Err(violation) = risk_gate.check(&opp) {
                warn!(
                    violation = ?violation,
                    ticker = %opp.market.ticker,
                    "risk gate tripped, skipping opportunity"
                );
                continue;
            }

            // Re-fetch balance immediately before sizing to use up-to-date capital
            let balance = match client.get_balance().await {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "failed to fetch balance before order, skipping");
                    continue;
                }
            };

            let market_price = match &opp.side {
                TradeSide::Yes => opp.market.yes_price,
                TradeSide::No => opp.market.no_price,
            };

            let contracts =
                compute_contracts(opp.kelly_fraction, balance, market_price, max_position_cents);

            if contracts == 0 {
                debug!(
                    ticker = %opp.market.ticker,
                    market_price = market_price,
                    balance_cents = balance,
                    "zero contracts after Kelly sizing, skipping"
                );
                continue;
            }

            // Limit price = market price + 1 cent to take liquidity aggressively
            let limit_price = market_price as u64 + 1;

            match client
                .place_order(&opp.market.ticker, &opp.side, contracts, limit_price)
                .await
            {
                Ok(order) => {
                    let cost_cents = contracts * limit_price;
                    risk_gate.record_fill(cost_cents);
                    info!(
                        order_id = %order.order_id,
                        status = %order.status,
                        filled = order.filled_count,
                        contracts = contracts,
                        cost_cents = cost_cents,
                        ticker = %opp.market.ticker,
                        "order filled"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        ticker = %opp.market.ticker,
                        side = ?opp.side,
                        contracts = contracts,
                        "order placement failed"
                    );
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn local_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
