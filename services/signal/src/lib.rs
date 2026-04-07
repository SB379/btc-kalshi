#![deny(clippy::unwrap_used)]

pub mod delta;
pub mod engine;
pub mod kalshi_poller;
pub mod probability;

use engine::SignalEngine;
use kalshi_poller::KalshiPoller;
use ringbuf::traits::{Consumer, Producer};
use ringbuf::{HeapCons, HeapProd};
use shared_types::{BrtiEstimate, TradeOpportunity};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tracing::{debug, info, warn};

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
) {
    let (markets_tx, markets_rx) = watch::channel::<Vec<shared_types::KalshiMarket>>(vec![]);
    let last_fetch_ms = Arc::new(AtomicU64::new(0));

    KalshiPoller::new(base_url).start_polling(markets_tx, Arc::clone(&last_fetch_ms));

    let mut engine = SignalEngine::new(markets_rx);

    loop {
        while let Some(estimate) = consumer.try_pop() {
            // Warn if poller hasn't successfully fetched markets in the last 30s
            let last = last_fetch_ms.load(Ordering::Relaxed);
            if last > 0 {
                let now = local_ts_ms();
                if now.saturating_sub(last) > MARKET_STALE_MS {
                    warn!("Kalshi poller hasn't returned markets in 30s");
                }
            }

            let opps = engine.ingest(estimate);
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
                    if opp_producer.try_push(opp).is_err() {
                        warn!("opportunity ring buffer full, dropping opportunity");
                    }
                }
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
