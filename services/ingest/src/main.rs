#![deny(clippy::unwrap_used)]

mod exchanges;
mod normalize;

use historian::Historian;
use ringbuf::traits::{Consumer, Producer, Split};
use ringbuf::HeapRb;
use shared_types::{BrtiEstimate, Trade, TradeOpportunity};
use std::sync::Arc;
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

    // Initialise the historian (opens daily JSONL files; fails fast if log dir is unusable).
    let historian = match Historian::new() {
        Ok(h) => Arc::new(h),
        Err(e) => {
            eprintln!("historian: failed to initialise: {e:#}");
            std::process::exit(1);
        }
    };

    let kalshi_base_url = std::env::var("KALSHI_BASE_URL")
        .unwrap_or_else(|_| "https://api.elections.kalshi.com/trade-api/v2".to_string());

    // Internal mpsc: each exchange task sends here; merger task drains into ring buffer.
    // This preserves the SPSC contract on the ring buffer (single producer = merger task).
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Trade>(MERGE_CHANNEL_CAPACITY);

    // SPSC ring buffers: created here so each producer/consumer end goes to the right task.
    let (mut ingest_prod, ingest_cons) = HeapRb::<Trade>::new(INGEST_RING_CAPACITY).split();
    let (signal_prod, signal_cons) = HeapRb::<BrtiEstimate>::new(SIGNAL_RING_CAPACITY).split();
    let (opp_prod, mut opp_cons) = HeapRb::<TradeOpportunity>::new(OPP_RING_CAPACITY).split();

    // Spawn exchange tasks — each gets its own clone of the mpsc sender.
    let cb_tx = tx.clone();
    tokio::spawn(async move { exchanges::coinbase::run(cb_tx).await });

    let kr_tx = tx.clone();
    tokio::spawn(async move { exchanges::kraken::run(kr_tx).await });

    let bs_tx = tx.clone();
    tokio::spawn(async move { exchanges::bitstamp::run(bs_tx).await });

    let gm_tx = tx.clone();
    tokio::spawn(async move { exchanges::gemini::run(gm_tx).await });

    let cc_tx = tx.clone();
    tokio::spawn(async move { exchanges::cryptocom::run(cc_tx).await });
    // itbit (REST API shut down), lmax (private venue), bullish (404): not spawned

    // Drop the original sender so the channel closes if all exchange tasks exit
    drop(tx);

    // Merger task: single writer to the SPSC ring buffer (preserves SPSC contract)
    let hist_merger = Arc::clone(&historian);
    tokio::spawn(async move {
        while let Some(trade) = rx.recv().await {
            hist_merger.record_trade(historian::TradeRecord {
                timestamp_ms: trade.local_ts,
                exchange: format!("{:?}", trade.exchange),
                price: trade.price,
                size: trade.size,
                exchange_ts: trade.exchange_ts,
                local_ts: trade.local_ts,
                latency_ms: trade.local_ts as i64 - trade.exchange_ts as i64,
            });
            if ingest_prod.try_push(trade).is_err() {
                tracing::warn!("ring buffer full, dropping trade in merger");
            }
        }
    });

    // Reconstructor task: reads trades, maintains BRTI estimate, writes to signal buffer
    let hist_recon = Arc::clone(&historian);
    tokio::spawn(async move {
        reconstructor::run(ingest_cons, signal_prod, hist_recon).await;
    });

    // Signal task: reads BRTI estimates, runs signal engine, writes opportunities to executor
    let hist_signal = Arc::clone(&historian);
    tokio::spawn(async move {
        signal::run(signal_cons, opp_prod, kalshi_base_url, hist_signal).await;
    });

    let trading_enabled = std::env::var("TRADING_ENABLED")
        .map(|v| v.to_lowercase() != "false")
        .unwrap_or(true);

    if trading_enabled {
        // Executor task: reads opportunities, gates through risk checks, places Kalshi orders.
        // Runs independently — a slow Kalshi API call never blocks the upstream pipeline.
        let hist_exec = Arc::clone(&historian);
        tokio::spawn(async move {
            if let Err(e) = executor::run(opp_cons, hist_exec).await {
                error!(error = %e, "executor failed to initialize — check KALSHI_USE_DEMO and auth env vars");
                std::process::exit(1);
            }
        });
    } else {
        info!("TRADING DISABLED — compare mode active, no orders will be placed");
        // Drain task: prevents the opp ring buffer from filling and producing log spam.
        tokio::spawn(async move {
            loop {
                while opp_cons.try_pop().is_some() {}
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });
    }

    // Keep main task alive; all work runs in spawned tasks
    std::future::pending::<()>().await;
}
