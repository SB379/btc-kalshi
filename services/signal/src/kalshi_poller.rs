use anyhow::Context;
use reqwest::Client;
use shared_types::KalshiMarket;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::watch;
use tracing::{debug, warn};

pub struct KalshiPoller {
    base_url: String,
    client: Client,
}

impl KalshiPoller {
    pub fn new(base_url: String) -> Self {
        KalshiPoller {
            base_url,
            client: Client::new(),
        }
    }

    /// Fetch all open KXBTC15M markets from the Kalshi REST API.
    /// Returns an empty vec (not an error) if no markets are found.
    pub async fn fetch_active_btc_markets(&self) -> Result<Vec<KalshiMarket>, anyhow::Error> {
        let url = format!(
            "{}/markets?series_ticker=KXBTC15M&status=open",
            self.base_url
        );
        debug!(url = %url, "GET /markets");

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("GET /markets request failed")?;
        let text = response.text().await.context("failed to read markets response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse markets response");
            anyhow::anyhow!("failed to parse markets response: {}", e)
        })?;

        let markets_arr = match body.get("markets").and_then(|m| m.as_array()) {
            Some(arr) => arr,
            None => return Ok(vec![]),
        };

        let mut result = Vec::new();
        for m in markets_arr {
            let ticker = match m.get("ticker").and_then(|v| v.as_str()) {
                Some(t) => t.to_string(),
                None => continue,
            };
            let yes_price = m
                .get("yes_bid")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let no_price = m
                .get("no_bid")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let strike = m
                .get("floor_strike")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let closes_at = m
                .get("close_time")
                .and_then(|v| v.as_str())
                .and_then(parse_rfc3339_ms)
                .unwrap_or(0);

            debug!(ticker = %ticker, yes = yes_price, no = no_price, "fetched kalshi market");
            result.push(KalshiMarket { ticker, yes_price, no_price, strike, closes_at });
        }
        Ok(result)
    }

    /// Spawn a background task that polls every 5s and pushes results to the watch channel.
    /// `last_fetch_ms` is updated with the unix-ms timestamp of each successful fetch.
    pub fn start_polling(
        self,
        tx: watch::Sender<Vec<KalshiMarket>>,
        last_fetch_ms: Arc<AtomicU64>,
    ) {
        tokio::spawn(async move {
            loop {
                match self.fetch_active_btc_markets().await {
                    Ok(markets) => {
                        last_fetch_ms.store(local_ts_ms(), Ordering::Relaxed);
                        // send() only fails if all receivers are dropped — log and continue
                        if tx.send(markets).is_err() {
                            warn!("kalshi market watch channel closed");
                        }
                    }
                    Err(e) => warn!(error = %e, "failed to fetch Kalshi markets"),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
}

fn parse_rfc3339_ms(s: &str) -> Option<u64> {
    let dt =
        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339).ok()?;
    Some((dt.unix_timestamp() as u64) * 1000 + (dt.nanosecond() as u64) / 1_000_000)
}

fn local_ts_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
