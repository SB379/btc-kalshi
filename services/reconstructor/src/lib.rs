#![deny(clippy::unwrap_used)]

pub mod engine;
pub mod validator;
pub mod window;

use engine::ReconstructorEngine;
use historian::{BrtiRecord, Historian};
use ringbuf::traits::{Consumer, Producer};
use ringbuf::{HeapCons, HeapProd};
use shared_types::{BrtiEstimate, ReconstructorConfig, Trade};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

const LOG_INTERVAL: Duration = Duration::from_millis(100);

/// Consume trades from the ingest ring buffer, maintain per-exchange rolling windows,
/// and publish BrtiEstimate values into the provided signal ring buffer producer.
///
/// Blocks forever; call from a dedicated tokio task.
pub async fn run(
    mut consumer: HeapCons<Trade>,
    mut signal_producer: HeapProd<BrtiEstimate>,
    historian: Arc<Historian>,
) {
    let config = ReconstructorConfig {
        window_secs: 60,
        min_exchanges: 2,
        staleness_threshold_ms: 5000,
    };
    let mut engine = ReconstructorEngine::new(config);

    let mut last_logged = Instant::now();
    let mut last_exchange_count: u8 = 0;

    loop {
        while let Some(trade) = consumer.try_pop() {
            if let Some(estimate) = engine.ingest(trade) {
                let count_changed = estimate.exchange_count != last_exchange_count;
                let should_log = count_changed || last_logged.elapsed() >= LOG_INTERVAL;

                if should_log {
                    info!(
                        brti_est = estimate.value,
                        confidence = estimate.confidence,
                        exchanges = estimate.exchange_count,
                        "brti estimate"
                    );
                    if estimate.confidence < 0.6 {
                        warn!("low confidence - fewer than 3 of 5 exchanges live");
                    }
                    last_logged = Instant::now();
                    last_exchange_count = estimate.exchange_count;
                }

                historian.record_brti(BrtiRecord {
                    timestamp_ms: estimate.timestamp,
                    value: estimate.value,
                    confidence: estimate.confidence,
                    exchange_count: estimate.exchange_count,
                });

                if signal_producer.try_push(estimate).is_err() {
                    warn!("signal ring buffer full, dropping estimate");
                }
            }
        }
        // Yield to avoid busy-spinning; Phase 3 may switch to a blocking pop.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}
