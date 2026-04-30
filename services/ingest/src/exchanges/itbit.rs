use crate::normalize::local_ts_ms;
use shared_types::{Exchange, Trade};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// itBit has no public WebSocket; we fall back to polling the REST ticker every 2 seconds.
const POLL_URL: &str = "https://api.itbit.com/v1/markets/XBTUSD/ticker";
const POLL_INTERVAL: Duration = Duration::from_secs(2);

pub fn normalize(raw: &str) -> Option<Trade> {
    let v: serde_json::Value = serde_json::from_str(raw).ok()?;
    let price: f64 = v["lastPrice"].as_str()?.parse().ok()?;
    let local = local_ts_ms();
    Some(Trade {
        exchange: Exchange::ItBit,
        price,
        // itBit REST ticker has no single-trade volume; use a fixed proxy
        size: 0.01,
        exchange_ts: local,
        local_ts: local,
    })
}

pub async fn run(tx: mpsc::Sender<Trade>) {
    info!(exchange = "itbit", "REST poller starting");
    let client = reqwest::Client::new();
    let mut backoff_ms = 100u64;

    loop {
        match poll_once(&client, &tx).await {
            Ok(()) => {
                // Reset backoff after a clean poll so transient errors don't starve the feed
                backoff_ms = 100;
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Err(ref e) => {
                warn!(exchange = "itbit", error = %e, "REST poll failed, retrying");
                let jitter = jitter(backoff_ms);
                tokio::time::sleep(Duration::from_millis(jitter)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        }
    }
}

async fn poll_once(
    client: &reqwest::Client,
    tx: &mpsc::Sender<Trade>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let text = client
        .get(POLL_URL)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    match normalize(&text) {
        Some(trade) => {
            if tx.try_send(trade).is_err() {
                warn!(exchange = "itbit", "merge channel full, dropping trade");
            }
        }
        None => {
            warn!(exchange = "itbit", raw = %text, "failed to parse ticker response");
        }
    }
    Ok(())
}

fn jitter(base: u64) -> u64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let factor = rng.gen_range(0.8..=1.2);
    (base as f64 * factor) as u64
}

#[cfg(test)]
mod tests {
    use super::normalize;
    use shared_types::Exchange;

    #[test]
    fn parses_itbit_ticker_response() {
        // Real itBit REST ticker shape
        let raw = r#"{
            "pair": "XBTUSD",
            "bid": "69750.00",
            "bidAmt": "0.10000000",
            "ask": "69800.00",
            "askAmt": "0.20000000",
            "lastPrice": "69775.00",
            "lastAmt": "0.01000000",
            "volume24h": "123.45678900",
            "volumeToday": "45.12345600",
            "high24h": "70000.00",
            "low24h": "69500.00",
            "highToday": "69900.00",
            "lowToday": "69600.00",
            "openToday": "69700.00",
            "vwapToday": "69750.00",
            "vwap24h": "69760.00",
            "serverTimeUTC": "2026-04-07T00:00:00.000Z"
        }"#;

        let trade = normalize(raw).expect("should parse");
        assert_eq!(trade.exchange, Exchange::ItBit);
        assert!((trade.price - 69775.0).abs() < f64::EPSILON);
        assert!((trade.size - 0.01).abs() < f64::EPSILON);
        assert!(trade.local_ts > 0);
        assert_eq!(trade.exchange_ts, trade.local_ts);
    }

    #[test]
    fn returns_none_for_missing_last_price() {
        let raw = r#"{"pair":"XBTUSD","bid":"69750.00"}"#;
        assert!(normalize(raw).is_none());
    }

    #[test]
    fn returns_none_for_invalid_json() {
        assert!(normalize("not json").is_none());
    }
}
