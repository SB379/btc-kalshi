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

#[derive(Clone)]
pub struct KalshiPoller {
    base_url: String,
    client: Client,
    /// Latest BRTI estimate stored as f64 bits. Zero = not yet initialised.
    brti_est: Arc<AtomicU64>,
}

impl KalshiPoller {
    pub fn new(base_url: String, brti_est: Arc<AtomicU64>) -> Self {
        KalshiPoller {
            base_url,
            client: Client::new(),
            brti_est,
        }
    }

    /// Update the shared BRTI estimate used for synthetic price computation.
    pub fn update_brti(&self, value: f64) {
        self.brti_est.store(value.to_bits(), Ordering::Relaxed);
    }

    /// Fetch all open KXBTC15M markets from the Kalshi REST API.
    /// Returns an empty vec (not an error) if no markets are found.
    /// When a market has zero bid prices, synthetic prices derived from the
    /// current BRTI estimate and time-to-close are substituted.
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
        let text = response
            .text()
            .await
            .context("failed to read markets response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse markets response");
            anyhow::anyhow!("failed to parse markets response: {}", e)
        })?;

        let markets_arr = match body.get("markets").and_then(|m| m.as_array()) {
            Some(arr) => arr,
            None => return Ok(vec![]),
        };

        let now_ms = local_ts_ms();
        let brti = f64::from_bits(self.brti_est.load(Ordering::Relaxed));

        let mut result = Vec::new();
        for m in markets_arr {
            let ticker = match m["ticker"].as_str() {
                Some(t) => t.to_string(),
                None => continue,
            };
            // Kalshi returns yes_bid / no_bid as integers in cents (0–99).
            // Fall back to yes_bid_dollars string form for forward-compat with API changes.
            let mut yes_price = m["yes_bid"]
                .as_f64()
                .or_else(|| {
                    m["yes_bid_dollars"]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|d| d * 100.0)
                })
                .unwrap_or(0.0);
            let mut no_price = m["no_bid"]
                .as_f64()
                .or_else(|| {
                    m["no_bid_dollars"]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|d| d * 100.0)
                })
                .unwrap_or(0.0);
            let strike = m["floor_strike"].as_f64().unwrap_or(0.0);
            let open_time = m["open_time"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis() as u64)
                .unwrap_or(0);
            let closes_at = m["close_time"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis() as u64)
                .unwrap_or(0);

            // Substitute synthetic prices when the orderbook is empty and we have
            // enough information (live BRTI + valid close time + known strike).
            let synthetic = yes_price == 0.0 && brti != 0.0 && strike != 0.0 && closes_at > now_ms;
            if synthetic {
                let seconds_to_close = closes_at.saturating_sub(now_ms) as f64 / 1000.0;
                let (syn_yes, syn_no) = synthetic_prices(brti, strike, seconds_to_close);
                debug!(
                    ticker = %ticker,
                    yes_price = syn_yes,
                    no_price = syn_no,
                    synthetic = true,
                    brti = brti,
                    strike = strike,
                    secs_to_close = seconds_to_close,
                    "using synthetic prices"
                );
                yes_price = syn_yes;
                no_price = syn_no;
            } else {
                debug!(ticker = %ticker, yes = yes_price, no = no_price, "fetched kalshi market");
            }

            result.push(KalshiMarket {
                ticker,
                yes_price,
                no_price,
                strike,
                open_time,
                closes_at,
                synthetic,
            });
        }
        Ok(result)
    }

    /// Spawn a background task that polls every 5s and pushes results to the watch channel.
    /// `last_fetch_ms` is updated with the unix-ms timestamp of each successful fetch.
    /// Takes `&self` (clones internally) so the caller retains the poller for `update_brti`.
    pub fn start_polling(
        &self,
        tx: watch::Sender<Vec<KalshiMarket>>,
        last_fetch_ms: Arc<AtomicU64>,
    ) {
        let poller = self.clone();
        tokio::spawn(async move {
            loop {
                match poller.fetch_active_btc_markets().await {
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

/// Compute synthetic YES/NO prices (in cents) from BRTI relative to strike and time remaining.
///
/// Distance from strike drives the base probability; time decay compresses prices toward
/// certainty as expiry approaches.  Output is clamped to [1, 99] cents.
fn synthetic_prices(brti_est: f64, strike: f64, seconds_to_close: f64) -> (f64, f64) {
    // Distance from strike as a fraction
    let distance = (brti_est - strike) / strike;

    // Time decay — closer to expiry = more extreme prices
    let time_factor = (seconds_to_close / 900.0).min(1.0); // 900s = 15 min

    // Base probability from distance
    // +0.5% above strike ≈ 65% yes, -0.5% below ≈ 35% yes
    let base_prob = 0.5 + (distance * 100.0).tanh() * 0.4;

    // Compress toward certainty as time runs out
    let yes_prob = if brti_est > strike {
        base_prob + (1.0 - base_prob) * (1.0 - time_factor) * 0.5
    } else {
        base_prob * time_factor
    };

    let yes_price = (yes_prob * 100.0).clamp(1.0, 99.0);
    let no_price = (100.0 - yes_price).clamp(1.0, 99.0);
    (yes_price, no_price)
}

fn local_ts_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::synthetic_prices;

    #[test]
    fn synthetic_yes_above_50_when_brti_above_strike() {
        let (yes, no) = synthetic_prices(71_000.0, 70_000.0, 450.0);
        assert!(yes > 50.0, "yes={yes} should be >50 when BRTI above strike");
        assert!((yes + no - 100.0).abs() < 0.01, "yes+no should ≈100");
    }

    #[test]
    fn synthetic_yes_below_50_when_brti_below_strike() {
        let (yes, no) = synthetic_prices(69_000.0, 70_000.0, 450.0);
        assert!(yes < 50.0, "yes={yes} should be <50 when BRTI below strike");
        assert!((yes + no - 100.0).abs() < 0.01, "yes+no should ≈100");
    }

    #[test]
    fn synthetic_prices_clamped_to_valid_range() {
        // Extreme distance should clamp to [1, 99]
        let (yes, no) = synthetic_prices(100_000.0, 70_000.0, 1.0);
        assert!(yes >= 1.0 && yes <= 99.0, "yes={yes} out of [1,99]");
        assert!(no >= 1.0 && no <= 99.0, "no={no} out of [1,99]");
    }

    #[test]
    fn synthetic_yes_50_at_strike_midpoint() {
        // Exactly at strike with plenty of time → should be close to 50
        let (yes, _) = synthetic_prices(70_000.0, 70_000.0, 900.0);
        assert!(
            (yes - 50.0).abs() < 1.0,
            "yes={yes} should be ≈50 at strike"
        );
    }
}
