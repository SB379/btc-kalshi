#![deny(clippy::unwrap_used)]

mod exchanges;
mod normalize;

use ringbuf::traits::{Producer, Split};
use ringbuf::HeapRb;
use shared_types::{BrtiEstimate, Trade, TradeOpportunity};
use tracing::{error, info};

/// Internal mpsc capacity — handles bursts from all 4 exchange feeds
const MERGE_CHANNEL_CAPACITY: usize = 4_096;
/// ingest → reconstructor ring buffer capacity
const INGEST_RING_CAPACITY: usize = 65_536;
/// reconstructor → signal ring buffer capacity
const SIGNAL_RING_CAPACITY: usize = 16_384;
/// signal → executor ring buffer capacity
const OPP_RING_CAPACITY: usize = 1_024;

#[tokio::main]
async fn main() {
    // Load .env if present (non-fatal if missing)
    let _ = dotenvy::dotenv();

    // Tracing: pretty in dev, JSON in prod
    let log_format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());
    if log_format == "json" {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .init();
    }

    info!("btc-kalshi ingest starting");

    let kalshi_base_url = std::env::var("KALSHI_BASE_URL")
        .unwrap_or_else(|_| "https://api.elections.kalshi.com/trade-api/v2".to_string());

    // Internal mpsc: each exchange task sends here; merger task drains into ring buffer.
    // This preserves the SPSC contract on the ring buffer (single producer = merger task).
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Trade>(MERGE_CHANNEL_CAPACITY);

    // SPSC ring buffers: created here so each producer/consumer end goes to the right task.
    let (mut ingest_prod, ingest_cons) = HeapRb::<Trade>::new(INGEST_RING_CAPACITY).split();
    let (signal_prod, signal_cons) = HeapRb::<BrtiEstimate>::new(SIGNAL_RING_CAPACITY).split();
    let (opp_prod, opp_cons) = HeapRb::<TradeOpportunity>::new(OPP_RING_CAPACITY).split();

    // Spawn exchange tasks — each gets its own clone of the mpsc sender.
    let cb_tx = tx.clone();
    tokio::spawn(async move { exchanges::coinbase::run(cb_tx).await });

    let kr_tx = tx.clone();
    tokio::spawn(async move { exchanges::kraken::run(kr_tx).await });

    let bs_tx = tx.clone();
    tokio::spawn(async move { exchanges::bitstamp::run(bs_tx).await });

    let gm_tx = tx.clone();
    tokio::spawn(async move { exchanges::gemini::run(gm_tx).await });

    // Drop the original sender so the channel closes if all exchange tasks exit
    drop(tx);

    // Merger task: single writer to the SPSC ring buffer (preserves SPSC contract)
    tokio::spawn(async move {
        while let Some(trade) = rx.recv().await {
            if ingest_prod.try_push(trade).is_err() {
                tracing::warn!("ring buffer full, dropping trade in merger");
            }
        }
    });

    // Reconstructor task: reads trades, maintains BRTI estimate, writes to signal buffer
    tokio::spawn(async move {
        reconstructor::run(ingest_cons, signal_prod).await;
    });

    // Signal task: reads BRTI estimates, runs signal engine, writes opportunities to executor
    tokio::spawn(async move {
        signal::run(signal_cons, opp_prod, kalshi_base_url).await;
    });

    // Executor task: reads opportunities, gates through risk checks, places Kalshi orders.
    // Runs independently — a slow Kalshi API call never blocks the upstream pipeline.
    tokio::spawn(async move {
        if let Err(e) = executor::run(opp_cons).await {
            error!(error = %e, "executor failed to initialize — check KALSHI_USE_DEMO and auth env vars");
            std::process::exit(1);
        }
    });

    // Keep main task alive; all work runs in spawned tasks
    std::future::pending::<()>().await;
}
