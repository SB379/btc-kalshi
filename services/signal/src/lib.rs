#![deny(clippy::unwrap_used)]

pub mod delta;
pub mod engine;
pub mod kalshi_poller;
pub mod probability;
pub mod spike;

use engine::SignalEngine;
use historian::{CompareRecord, Historian, OpportunityRecord, SignalRecord};
use kalshi_poller::KalshiPoller;
use ringbuf::traits::{Consumer, Producer};
use ringbuf::{HeapCons, HeapProd};
use shared_types::{BrtiEstimate, Direction, TradeOpportunity};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::{debug, info, warn};
use chrono::{DateTime, Local};

/// Warn if Kalshi poller has not returned markets within this window.
const MARKET_STALE_MS: u64 = 30_000;

/// Consume BrtiEstimates from the reconstructor ring buffer, run the signal engine,
/// and write TradeOpportunity values into the executor ring buffer.
///
/// Blocks forever; call from a dedicated tokio task.
pub async fn run(
    mut consumer: HeapCons<BrtiEstimate>,
    mut opp_producer: HeapProd<TradeOpportunity>,
    base_url: String,
    historian: Arc<Historian>,
) {
    let (markets_tx, markets_rx) = watch::channel::<Vec<shared_types::KalshiMarket>>(vec![]);
    let last_fetch_ms = Arc::new(AtomicU64::new(0));
    let brti_shared = Arc::new(AtomicU64::new(0));

    let poller = KalshiPoller::new(base_url, Arc::clone(&brti_shared));
    poller.start_polling(markets_tx, Arc::clone(&last_fetch_ms));

    // Clone before passing to engine so CompareTracker can read markets independently.
    let compare_markets_rx = markets_rx.clone();
    let mut engine = SignalEngine::new(markets_rx);

    let trading_enabled = std::env::var("TRADING_ENABLED")
        .map(|v| v.to_lowercase() != "false")
        .unwrap_or(true);
    if !trading_enabled {
        info!("TRADING_ENABLED=false — observation mode active, no orders will be placed");
    }
    let mut tracker: Option<CompareTracker> = if trading_enabled {
        None
    } else {
        Some(CompareTracker::new(Arc::clone(&historian)))
    };

    loop {
        while let Some(estimate) = consumer.try_pop() {
            // Share latest BRTI with the Kalshi poller for synthetic price computation
            poller.update_brti(estimate.value);

            // Warn if poller hasn't successfully fetched markets in the last 30s
            let last = last_fetch_ms.load(Ordering::Relaxed);
            if last > 0 {
                let now = local_ts_ms();
                if now.saturating_sub(last) > MARKET_STALE_MS {
                    warn!("Kalshi poller hasn't returned markets in 30s");
                }
            }

            let opps = engine.ingest(estimate.clone());

            // Record signal stats for every estimate (Neutral included)
            historian.record_signal(SignalRecord {
                timestamp_ms: estimate.timestamp,
                direction: match engine.last_direction() {
                    Direction::Up => "Up",
                    Direction::Down => "Down",
                    Direction::Neutral => "Neutral",
                }
                .to_string(),
                confidence: estimate.confidence,
                brti_est: estimate.value,
                delta_pct: engine.last_delta_pct(),
                velocity: engine.last_velocity(),
            });

            if opps.is_empty() {
                debug!("no opportunities on this estimate");
            } else {
                for opp in opps {
                    info!(
                        ticker = %opp.market.ticker,
                        side = ?opp.side,
                        edge = opp.edge,
                        kelly = opp.kelly_fraction,
                        brti_est = opp.signal.brti_est.value,
                        "trade opportunity"
                    );
                    historian.record_opportunity(OpportunityRecord {
                        timestamp_ms: local_ts_ms(),
                        ticker: opp.market.ticker.clone(),
                        side: format!("{:?}", opp.side),
                        edge: opp.edge,
                        kelly_fraction: opp.kelly_fraction,
                        market_yes_price: opp.market.yes_price,
                        market_no_price: opp.market.no_price,
                        strike: opp.market.strike,
                        closes_at: opp.market.closes_at,
                        brti_est: opp.signal.brti_est.value,
                        signal_confidence: opp.signal.confidence,
                        synthetic: opp.market.synthetic,
                    });
                    // Only forward to executor when trading is live.
                    // Observation mode (TRADING_ENABLED=false) records opportunities
                    // but never places orders.
                    if trading_enabled {
                        if opp_producer.try_push(opp).is_err() {
                            warn!("opportunity ring buffer full, dropping opportunity");
                        }
                    }
                }
            }

            if let Some(ref mut cmp) = tracker {
                let markets_snapshot = compare_markets_rx.borrow().clone();
                cmp.update(
                    &estimate,
                    &markets_snapshot,
                    engine.last_direction(),
                    engine.last_velocity(),
                    engine.last_delta_pct(),
                );
            }
        }
        // Yield to avoid busy-spinning; Phase 4 may switch to a blocking pop.
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn local_ts_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// CompareTracker — prints BRTI vs Kalshi prices and detects lead/lag.
// Only active when TRADING_ENABLED=false.
// ---------------------------------------------------------------------------

struct CompareTracker {
    /// BRTI value at the last [COMPARE] print — used for $5 throttle.
    last_printed_brti: f64,
    /// Wall-clock ms at the last [COMPARE] print — used for 5s throttle.
    last_print_ms: u64,
    /// Per-ticker yes_price at the last poll — diff detects reprices.
    last_kalshi_yes: HashMap<String, f64>,
    /// Timestamp (ms) of the last significant BRTI move (>$10).
    last_brti_move_ms: Option<u64>,
    /// Signed dollar magnitude of that move (positive = up).
    last_brti_move_delta: f64,
    /// BRTI value on the previous estimate — used to compute tick delta.
    prev_brti: f64,
    /// Historian for writing compare_YYYY-MM-DD.jsonl
    historian: Arc<Historian>,
}

impl CompareTracker {
    fn new(historian: Arc<Historian>) -> Self {
        CompareTracker {
            last_printed_brti: 0.0,
            last_print_ms: 0,
            last_kalshi_yes: HashMap::new(),
            last_brti_move_ms: None,
            last_brti_move_delta: 0.0,
            prev_brti: 0.0,
            historian,
        }
    }

    fn update(
        &mut self,
        estimate: &shared_types::BrtiEstimate,
        markets: &[shared_types::KalshiMarket],
        dir: &shared_types::Direction,
        vel: f64,
        delta_pct: f64,
    ) {
        let now_ms = local_ts_ms();

        // Track significant BRTI moves (>$10 per tick) for lead/lag detection.
        let tick_delta = estimate.value - self.prev_brti;
        if self.prev_brti != 0.0 && tick_delta.abs() > 10.0 {
            self.last_brti_move_ms = Some(now_ms);
            self.last_brti_move_delta = tick_delta;
        }
        self.prev_brti = estimate.value;

        // Check for Kalshi reprices (>1¢ change) and emit [LEAD/LAG] lines.
        for market in markets {
            let prev_yes = self
                .last_kalshi_yes
                .get(&market.ticker)
                .copied()
                .unwrap_or(market.yes_price);
            if (market.yes_price - prev_yes).abs() > 1.0 {
                self.emit_lead_lag(market, prev_yes, market.yes_price, now_ms);
            }
            self.last_kalshi_yes
                .insert(market.ticker.clone(), market.yes_price);
        }

        // Throttle [COMPARE] lines: print on >$5 BRTI move or every 5 seconds.
        let price_moved = (estimate.value - self.last_printed_brti).abs() > 5.0;
        let time_elapsed = now_ms.saturating_sub(self.last_print_ms) >= 5_000;
        if price_moved || time_elapsed {
            self.emit_compare(estimate, markets, dir, vel, delta_pct, now_ms);
            self.last_printed_brti = estimate.value;
            self.last_print_ms = now_ms;
        }
    }

    fn emit_compare(
        &self,
        estimate: &shared_types::BrtiEstimate,
        markets: &[shared_types::KalshiMarket],
        dir: &shared_types::Direction,
        vel: f64,
        _delta_pct: f64,
        now_ms: u64,
    ) {
        let ts = format_ts_ms(now_ms);
        let dir_str = match dir {
            shared_types::Direction::Up => "Up",
            shared_types::Direction::Down => "Down",
            shared_types::Direction::Neutral => "Neutral",
        };

        if markets.is_empty() {
            println!(
                "[COMPARE] {} | BRTI=${:.2} ({} feeds conf={:.2}) | no active markets | dir={} vel={:+.1}",
                ts, estimate.value, estimate.exchange_count, estimate.confidence, dir_str, vel
            );
            return;
        }

        let lead_tag = match self.last_brti_move_ms {
            Some(ms) if now_ms.saturating_sub(ms) <= 30_000 => "BRTI MOVE RECENT",
            _ => "no recent BRTI move",
        };

        for market in markets {
            let delta = estimate.value - market.strike;
            let sign = if delta >= 0.0 { "+" } else { "-" };
            let price_label = if market.synthetic { "synth" } else { "live" };
            println!(
                "[COMPARE] {} | BRTI=${:.2} ({} feeds conf={:.2}) | {} strike=${:.0} yes={:.0}¢ no={:.0}¢ ({}) | Δ={}${:.2} | dir={} vel={:+.1} | {}",
                ts,
                estimate.value,
                estimate.exchange_count,
                estimate.confidence,
                market.ticker,
                market.strike,
                market.yes_price,
                market.no_price,
                price_label,
                sign,
                delta.abs(),
                dir_str,
                vel,
                lead_tag,
            );
            // Persist to JSONL for post-hoc backtest analysis
            self.historian.record_compare(CompareRecord {
                timestamp_ms: now_ms,
                brti_est: estimate.value,
                confidence: estimate.confidence,
                ticker: market.ticker.clone(),
                strike: market.strike,
                yes_price: market.yes_price,
                no_price: market.no_price,
                synthetic: market.synthetic,
                delta_from_strike: delta,
                direction: dir_str.to_string(),
                velocity: vel,
            });
        }
    }

    fn emit_lead_lag(
        &self,
        market: &shared_types::KalshiMarket,
        prev_yes: f64,
        new_yes: f64,
        kalshi_ms: u64,
    ) {
        // Direction consistent means Kalshi repriced in the same direction as the BRTI move.
        let direction_matches = (new_yes > prev_yes) == (self.last_brti_move_delta > 0.0);

        match self.last_brti_move_ms {
            None => println!(
                "[LEAD/LAG] {} repriced {:.0}¢→{:.0}¢ | no BRTI move recorded yet",
                market.ticker, prev_yes, new_yes
            ),
            Some(brti_ms) => {
                let age_ms = kalshi_ms.saturating_sub(brti_ms);
                if age_ms > 30_000 {
                    println!(
                        "[LEAD/LAG] {} repriced {:.0}¢→{:.0}¢ | last BRTI move >30s ago — no signal",
                        market.ticker, prev_yes, new_yes
                    );
                } else if direction_matches && brti_ms <= kalshi_ms {
                    println!(
                        "[LEAD/LAG] {} repriced {:.0}¢→{:.0}¢ | BRTI moved {:+.1} was {:.1}s ago → BRTI LEADS kalshi by ~{:.1}s",
                        market.ticker,
                        prev_yes,
                        new_yes,
                        self.last_brti_move_delta,
                        age_ms as f64 / 1000.0,
                        age_ms as f64 / 1000.0,
                    );
                } else {
                    let behind_ms = brti_ms.saturating_sub(kalshi_ms);
                    println!(
                        "[LEAD/LAG] {} repriced {:.0}¢→{:.0}¢ | KALSHI LEADS brti by ~{:.1}s (BRTI moved {:+.1} after)",
                        market.ticker,
                        prev_yes,
                        new_yes,
                        behind_ms as f64 / 1000.0,
                        self.last_brti_move_delta,
                    );
                }
            }
        }
    }
}

fn format_ts_ms(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    let nanos = ((ms % 1000) as u32) * 1_000_000;
    match DateTime::from_timestamp(secs, nanos) {
        Some(dt) => {
            let local: DateTime<Local> = dt.into();
            local.format("%H:%M:%S%.3f").to_string()
        }
        None => format!("{}ms", ms),
    }
}
