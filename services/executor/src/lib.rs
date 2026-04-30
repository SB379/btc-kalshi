#![deny(clippy::unwrap_used)]

pub mod auth;
pub mod client;
pub mod position_tracker;
pub mod risk;
pub mod sizing;

use client::KalshiClient;
use historian::{FillRecord, Historian, RealizedPnlRecord, RiskViolationRecord};
use position_tracker::{check_exit, ExitReason, OpenPosition, PositionTracker};
use ringbuf::traits::Consumer;
use ringbuf::HeapCons;
use risk::{RiskConfig, RiskGate, RiskViolation};
use shared_types::{TradeOpportunity, TradeSide};
use sizing::{compute_contracts, kelly_fraction};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

const THESIS_COOLDOWN_MS: u64 = 120_000;
const MIN_TRADABLE_PRICE_CENTS: f64 = 5.0;
const MAX_TRADABLE_PRICE_CENTS: f64 = 95.0;

#[derive(Clone)]
struct ThesisLock {
    side: TradeSide,
    cooldown_until_ms: u64,
}

#[derive(Clone)]
struct ExitPolicyCtx {
    historian: Arc<Historian>,
    thesis_locks: Arc<Mutex<HashMap<String, ThesisLock>>>,
    thesis_loss_suppressions: Arc<Mutex<HashMap<String, u64>>>,
    reentry_loss_threshold_cents: i64,
}

fn side_str(side: &TradeSide) -> &'static str {
    match side {
        TradeSide::Yes => "Yes",
        TradeSide::No => "No",
    }
}

fn thesis_key(ticker: &str, side: &TradeSide) -> String {
    format!("{}:{}", ticker, side_str(side))
}

/// Consume TradeOpportunity structs from the signal ring buffer, gate each through risk checks,
/// size the position, and place a limit order on Kalshi.
///
/// Hard-fails at startup if KALSHI_USE_DEMO is not set in the environment.
pub async fn run(
    mut consumer: HeapCons<TradeOpportunity>,
    historian: Arc<Historian>,
) -> Result<(), anyhow::Error> {
    let client = Arc::new(KalshiClient::from_env()?);

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
    let kelly_multiplier: f64 = std::env::var("KELLY_FRACTION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.25);
    let max_contracts: u64 = std::env::var("MAX_CONTRACTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let risk_gate = Arc::new(Mutex::new(RiskGate::new(risk_config)));
    let position_tracker = Arc::new(Mutex::new(PositionTracker::new()));
    let thesis_locks = Arc::new(Mutex::new(HashMap::<String, ThesisLock>::new()));
    let thesis_loss_suppressions = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let reentry_loss_threshold_cents: i64 = std::env::var("REENTRY_LOSS_THRESHOLD_CENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let exit_policy = ExitPolicyCtx {
        historian: Arc::clone(&historian),
        thesis_locks: Arc::clone(&thesis_locks),
        thesis_loss_suppressions: Arc::clone(&thesis_loss_suppressions),
        reentry_loss_threshold_cents,
    };
    // Latest BRTI value shared with the reconciliation task (stored as f64 bits)
    let latest_brti = Arc::new(AtomicU64::new(0));

    // Spawn the 60-second reconciliation loop as an independent background task
    {
        let client = Arc::clone(&client);
        let tracker = Arc::clone(&position_tracker);
        let gate = Arc::clone(&risk_gate);
        let brti = Arc::clone(&latest_brti);
        let exit_policy = exit_policy.clone();
        tokio::spawn(async move {
            reconciliation_loop(client, tracker, gate, brti, exit_policy).await;
        });
    }

    let mut recently_traded: HashMap<String, u64> = HashMap::new();

    loop {
        while let Some(opp) = consumer.try_pop() {
            let now = local_ts_ms();
            let current_brti = opp.signal.brti_est.value;

            // Keep the reconciliation task's BRTI view up to date
            latest_brti.store(current_brti.to_bits(), Ordering::Relaxed);

            // Fast BRTI reversal check on every incoming estimate (no market API calls)
            check_brti_reversals(
                &client,
                &position_tracker,
                &risk_gate,
                current_brti,
                &exit_policy,
            )
            .await;

            {
                let mut gate = risk_gate.lock().await;
                gate.maybe_reset_daily(now);
            }

            // Never enter a ticker where we already hold an open position.
            // Prevents averaging down on losing trades and corrupting the position tracker
            // (PositionTracker keyed by ticker — a second add() would overwrite the first).
            {
                let tracker = position_tracker.lock().await;
                if tracker.get_all().contains_key(&opp.market.ticker) {
                    debug!(ticker = %opp.market.ticker, "skipping — position already open");
                    continue;
                }
            }

            // One order per market per 30-second window
            if let Some(&last_traded) = recently_traded.get(&opp.market.ticker) {
                if now - last_traded < 30_000 {
                    debug!(ticker = %opp.market.ticker, "skipping — cooldown active");
                    continue;
                }
            }
            recently_traded.insert(opp.market.ticker.clone(), now);

            let violation = {
                let gate = risk_gate.lock().await;
                gate.check(&opp).err()
            };
            if let Some(v) = violation {
                warn!(
                    violation = ?v,
                    ticker = %opp.market.ticker,
                    "risk gate tripped, skipping opportunity"
                );
                historian.record_risk_violation(RiskViolationRecord {
                    timestamp_ms: now,
                    violation_type: violation_type_str(&v).to_string(),
                    detail: format!("{v:?}"),
                });
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
            if !(MIN_TRADABLE_PRICE_CENTS..=MAX_TRADABLE_PRICE_CENTS).contains(&market_price) {
                let detail = format!(
                    "ticker={} side={} price_cents={}",
                    opp.market.ticker,
                    side_str(&opp.side),
                    market_price
                );
                warn!(%detail, "price band gate tripped, skipping opportunity");
                historian.record_risk_violation(RiskViolationRecord {
                    timestamp_ms: now,
                    violation_type: "PriceBandOutOfRange".to_string(),
                    detail,
                });
                continue;
            }

            // Thesis lock: block opposite-side re-entry until cooldown expires.
            {
                let locks = thesis_locks.lock().await;
                if let Some(lock) = locks.get(&opp.market.ticker) {
                    let in_cooldown = now < lock.cooldown_until_ms;
                    let opposite_side = side_str(&lock.side) != side_str(&opp.side);
                    if opposite_side && in_cooldown {
                        debug!(
                            ticker = %opp.market.ticker,
                            locked_side = %side_str(&lock.side),
                            incoming_side = %side_str(&opp.side),
                            "thesis lock active, skipping opposite-side opportunity"
                        );
                        continue;
                    }
                }
            }

            // Suppress recent losing thesis re-entry for same ticker+side.
            {
                let key = thesis_key(&opp.market.ticker, &opp.side);
                let suppressions = thesis_loss_suppressions.lock().await;
                if let Some(&until_ms) = suppressions.get(&key) {
                    if now < until_ms {
                        debug!(ticker = %opp.market.ticker, side = %side_str(&opp.side), "loss suppression active");
                        continue;
                    }
                }
            }

            // Recompute Kelly with real binary-market odds
            let odds = 100.0 / market_price;
            let kelly = kelly_fraction(opp.edge, odds, kelly_multiplier);

            debug!(
                ticker = %opp.market.ticker,
                side = ?opp.side,
                yes_price = opp.market.yes_price,
                no_price = opp.market.no_price,
                edge = opp.edge,
                kelly,
                "evaluating opportunity"
            );

            if kelly <= 0.0 {
                debug!(
                    ticker = %opp.market.ticker,
                    edge = opp.edge,
                    odds,
                    "zero kelly after odds adjustment, skipping"
                );
                continue;
            }

            let contracts = compute_contracts(
                kelly,
                balance,
                market_price,
                max_position_cents,
                max_contracts,
            );
            if contracts == 0 {
                debug!(
                    ticker = %opp.market.ticker,
                    market_price,
                    balance_cents = balance,
                    max_contracts,
                    "zero contracts after sizing (Kelly too small or caps hit), skipping"
                );
                continue;
            }

            // Limit price = market price + 1¢ to take liquidity aggressively
            let limit_price = market_price as u64 + 1;

            match client
                .place_order(&opp.market.ticker, &opp.side, contracts, limit_price)
                .await
            {
                Ok(order) => {
                    let filled_contracts = order.filled_count;
                    if filled_contracts > 0 {
                        let cost_cents = filled_contracts * limit_price;
                        {
                            let mut gate = risk_gate.lock().await;
                            gate.record_fill(cost_cents);
                        }
                        info!(
                            order_id = %order.order_id,
                            status = %order.status,
                            filled = filled_contracts,
                            requested_contracts = contracts,
                            cost_cents,
                            ticker = %opp.market.ticker,
                            "order filled (strict fill promotion)"
                        );

                        // Register in position tracker only for confirmed filled quantity.
                        {
                            let mut tracker = position_tracker.lock().await;
                            tracker.add(OpenPosition {
                                order_id: order.order_id.clone(),
                                ticker: opp.market.ticker.clone(),
                                side: opp.side.clone(),
                                contracts: filled_contracts,
                                entry_price_cents: limit_price as f64,
                                peak_price_cents: limit_price as f64,
                                entry_brti: current_brti,
                                opened_at_ms: now,
                                closes_at_ms: opp.market.closes_at,
                            });
                        }
                        // Lock thesis side while position is active.
                        {
                            let mut locks = thesis_locks.lock().await;
                            locks.insert(
                                opp.market.ticker.clone(),
                                ThesisLock {
                                    side: opp.side.clone(),
                                    cooldown_until_ms: u64::MAX,
                                },
                            );
                        }
                    } else {
                        info!(
                            order_id = %order.order_id,
                            status = %order.status,
                            ticker = %opp.market.ticker,
                            "order acknowledged with zero fills, not promoting to open position"
                        );
                    }

                    historian.record_fill(FillRecord {
                        timestamp_ms: local_ts_ms(),
                        order_id: order.order_id.clone(),
                        ticker: opp.market.ticker.clone(),
                        side: format!("{:?}", opp.side),
                        contracts: filled_contracts,
                        price_cents: limit_price,
                        status: order.status.clone(),
                    });
                    info!(ticker = %opp.market.ticker, "fill written to historian");
                }
                Err(e) => {
                    error!(
                        error = %e,
                        ticker = %opp.market.ticker,
                        side = ?opp.side,
                        contracts,
                        "order placement failed"
                    );
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Background task: runs every 60 seconds.
/// Detects settled positions (no longer in Kalshi's unsettled list) and checks
/// stop-loss / near-expiry exit conditions for all remaining open positions.
async fn reconciliation_loop(
    client: Arc<KalshiClient>,
    tracker: Arc<Mutex<PositionTracker>>,
    risk_gate: Arc<Mutex<RiskGate>>,
    latest_brti: Arc<AtomicU64>,
    exit_policy: ExitPolicyCtx,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    // Skip the first immediate tick so we don't run before any positions exist
    interval.tick().await;

    loop {
        interval.tick().await;

        let current_brti = f64::from_bits(latest_brti.load(Ordering::Relaxed));
        let now_ms = local_ts_ms();

        // 1. Fetch currently unsettled positions from Kalshi
        let kalshi_positions = match client.get_open_positions().await {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "reconciliation: failed to fetch Kalshi positions");
                continue;
            }
        };

        let kalshi_tickers: std::collections::HashSet<String> =
            kalshi_positions.iter().map(|p| p.ticker.clone()).collect();

        // 2. Collect all tracked tickers before mutating (avoids holding lock across awaits)
        let tracked: Vec<String> = {
            let t = tracker.lock().await;
            t.get_all().keys().cloned().collect()
        };

        // 3. Settle positions no longer in Kalshi's unsettled list
        for ticker in &tracked {
            if !kalshi_tickers.contains(ticker) {
                let removed = {
                    let mut t = tracker.lock().await;
                    t.remove(ticker)
                };
                if removed.is_some() {
                    info!(ticker = %ticker, "position settled — removing from tracker");
                    let mut gate = risk_gate.lock().await;
                    gate.record_close();
                    // After explicit settlement/close, enforce a cooldown on opposite-side entries.
                    if let Some(pos) = removed {
                        let mut locks = exit_policy.thesis_locks.lock().await;
                        locks.insert(
                            ticker.clone(),
                            ThesisLock {
                                side: pos.side,
                                cooldown_until_ms: now_ms + THESIS_COOLDOWN_MS,
                            },
                        );
                    }
                }
            }
        }

        // 4. Check stop-loss and near-expiry for positions still open
        let snapshot: Vec<OpenPosition> = {
            let t = tracker.lock().await;
            t.get_all().values().cloned().collect()
        };

        for pos in snapshot {
            let bids = match client.get_market_bids(&pos.ticker).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, ticker = %pos.ticker, "failed to fetch bids for exit check");
                    continue;
                }
            };

            let current_price_cents = match pos.side {
                TradeSide::Yes => bids.yes_bid_cents,
                TradeSide::No => bids.no_bid_cents,
            };

            // Update peak price and get a fresh snapshot with the updated value.
            // The lock is held only for this mutation — no await between lock and unlock.
            let updated = {
                let mut t = tracker.lock().await;
                t.update_peak(&pos.ticker, current_price_cents)
            };
            let pos = match updated {
                Some(p) => p,
                None => continue, // position was concurrently removed (settled)
            };

            if let Some(reason) = check_exit(&pos, current_price_cents, current_brti, now_ms) {
                execute_exit(&client, &tracker, &risk_gate, &pos, reason, &exit_policy).await;
            }
        }
    }
}

/// Fast BRTI-reversal-only check run on every incoming opportunity (no market API calls).
async fn check_brti_reversals(
    client: &KalshiClient,
    tracker: &Mutex<PositionTracker>,
    risk_gate: &Mutex<RiskGate>,
    current_brti: f64,
    exit_policy: &ExitPolicyCtx,
) {
    let snapshot: Vec<OpenPosition> = {
        let t = tracker.lock().await;
        t.get_all().values().cloned().collect()
    };

    for pos in snapshot {
        let brti_delta = current_brti - pos.entry_brti;
        let reversed = match pos.side {
            TradeSide::Yes => brti_delta < -30.0,
            TradeSide::No => brti_delta > 30.0,
        };
        if reversed {
            let reason = ExitReason::BrtiReversal {
                entry_brti: pos.entry_brti,
                current_brti,
                delta: brti_delta,
            };
            execute_exit(client, tracker, risk_gate, &pos, reason, exit_policy).await;
        }
    }
}

/// Place a sell order and, on success, remove the position from the tracker and decrement the
/// risk gate. On failure, leaves the position in the tracker so the next cycle retries.
async fn execute_exit(
    client: &KalshiClient,
    tracker: &Mutex<PositionTracker>,
    risk_gate: &Mutex<RiskGate>,
    pos: &OpenPosition,
    reason: ExitReason,
    exit_policy: &ExitPolicyCtx,
) {
    info!(ticker = %pos.ticker, reason = ?reason, "exiting position");
    match client
        .exit_position(&pos.ticker, &pos.side, pos.contracts)
        .await
    {
        Ok(exit) => {
            if exit.filled_count == 0 {
                warn!(ticker = %pos.ticker, "exit accepted with zero fills — keeping position");
                return;
            }
            {
                let mut t = tracker.lock().await;
                t.remove(&pos.ticker);
            }
            {
                let mut gate = risk_gate.lock().await;
                gate.record_close();
                let pnl_cents = ((exit.fill_price_cents as f64 - pos.entry_price_cents)
                    * exit.filled_count as f64)
                    .round() as i64;
                gate.record_pnl(pnl_cents);
                if pnl_cents < 0 {
                    let loss_abs = pnl_cents.saturating_abs();
                    if loss_abs >= exit_policy.reentry_loss_threshold_cents {
                        let key = thesis_key(&pos.ticker, &pos.side);
                        let mut suppressions = exit_policy.thesis_loss_suppressions.lock().await;
                        suppressions.insert(key, local_ts_ms() + THESIS_COOLDOWN_MS);
                    }
                }
                exit_policy
                    .historian
                    .record_realized_pnl(RealizedPnlRecord {
                        timestamp_ms: local_ts_ms(),
                        ticker: pos.ticker.clone(),
                        side: side_str(&pos.side).to_string(),
                        contracts: exit.filled_count,
                        entry_price_cents: pos.entry_price_cents.round() as u64,
                        exit_price_cents: exit.fill_price_cents,
                        pnl_cents,
                        reason: format!("{reason:?}"),
                    });
            }
            {
                let mut locks = exit_policy.thesis_locks.lock().await;
                locks.insert(
                    pos.ticker.clone(),
                    ThesisLock {
                        side: pos.side.clone(),
                        cooldown_until_ms: local_ts_ms() + THESIS_COOLDOWN_MS,
                    },
                );
            }
        }
        Err(e) => {
            error!(
                error = %e,
                ticker = %pos.ticker,
                "exit order failed — keeping position, will retry next cycle"
            );
        }
    }
}

fn local_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn violation_type_str(v: &RiskViolation) -> &'static str {
    match v {
        RiskViolation::ConfidenceTooLow { .. } => "ConfidenceTooLow",
        RiskViolation::EdgeTooSmall { .. } => "EdgeTooSmall",
        RiskViolation::TooManyOpenPositions { .. } => "TooManyOpenPositions",
        RiskViolation::DailyLossLimitHit { .. } => "DailyLossLimitHit",
    }
}
