use anyhow::Context;
use reqwest::Client;
use shared_types::TradeSide;
use tracing::{debug, info};

use crate::auth::KalshiAuth;

pub struct KalshiClient {
    pub http: Client,
    pub auth: KalshiAuth,
    pub base_url: String,
    pub use_demo: bool,
}

pub struct OrderResponse {
    pub order_id: String,
    /// "resting", "filled", "cancelled", etc.
    pub status: String,
    pub filled_count: u64,
    pub remaining_count: u64,
}

pub struct ExitResponse {
    pub filled_count: u64,
    pub fill_price_cents: u64,
}

pub struct Position {
    pub ticker: String,
    pub side: TradeSide,
    pub contracts: u64,
    pub entry_price_cents: u64,
}

/// Current best bid prices for a market, in cents.
pub struct MarketBids {
    pub yes_bid_cents: f64,
    pub no_bid_cents: f64,
}

impl KalshiClient {
    /// Build the full URL path for RSA-PSS signature construction.
    /// Kalshi signs over the complete path (e.g. /trade-api/v2/portfolio/balance),
    /// not just the endpoint suffix (/portfolio/balance).
    fn sign_path(&self, endpoint: &str) -> String {
        // Strip "https://host" from base_url to get e.g. "/trade-api/v2"
        let path_prefix = self
            .base_url
            .split("://")
            .nth(1)
            .and_then(|s| s.find('/').map(|i| &s[i..]))
            .unwrap_or("");
        format!("{}{}", path_prefix, endpoint)
    }

    /// Construct from environment variables.
    /// Hard-fails if `KALSHI_USE_DEMO` is not set — the executor refuses to start without it.
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let use_demo_str = std::env::var("KALSHI_USE_DEMO").map_err(|_| {
            anyhow::anyhow!(
                "KALSHI_USE_DEMO env var is required but not set — refusing to start executor. \
                 Set KALSHI_USE_DEMO=true for demo trading or KALSHI_USE_DEMO=false for live."
            )
        })?;
        let use_demo = use_demo_str.trim().to_lowercase() == "true";

        let base_url = std::env::var("KALSHI_BASE_URL")
            .unwrap_or_else(|_| "https://api.elections.kalshi.com/trade-api/v2".to_string());

        let auth = KalshiAuth::from_env().context("failed to initialize Kalshi auth")?;

        Ok(KalshiClient {
            http: Client::new(),
            auth,
            base_url,
            use_demo,
        })
    }

    /// Fetch the account's available balance in cents.
    pub async fn get_balance(&self) -> Result<u64, anyhow::Error> {
        let path = "/portfolio/balance";
        let headers = self.auth.sign_request("GET", &self.sign_path(path))?;
        let url = format!("{}{}", self.base_url, path);

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(url = %url, kalshi_key_id = %key_id, signature = "<redacted>", "GET /portfolio/balance");

        let response = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /portfolio/balance request failed")?;
        let text = response
            .text()
            .await
            .context("failed to read balance response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse balance response");
            anyhow::anyhow!("failed to parse balance response: {}", e)
        })?;

        debug!(response = %body, "balance response");

        // Kalshi returns available_balance_cents; fall back to balance for compatibility
        let cents = body
            .get("available_balance_cents")
            .or_else(|| body.get("balance"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(cents)
    }

    /// Place a limit order on Kalshi.
    /// Logs the full request and response at DEBUG, and a summary at INFO on success.
    pub async fn place_order(
        &self,
        ticker: &str,
        side: &TradeSide,
        contracts: u64,
        limit_price_cents: u64,
    ) -> Result<OrderResponse, anyhow::Error> {
        let path = "/portfolio/orders";
        let headers = self.auth.sign_request("POST", &self.sign_path(path))?;
        let url = format!("{}{}", self.base_url, path);

        let side_str = match side {
            TradeSide::Yes => "yes",
            TradeSide::No => "no",
        };

        let mut body = serde_json::json!({
            "ticker": ticker,
            "action": "buy",
            "side": side_str,
            "count": contracts,
            "type": "limit",
        });
        match side {
            TradeSide::Yes => body["yes_price"] = serde_json::Value::from(limit_price_cents),
            TradeSide::No => body["no_price"] = serde_json::Value::from(limit_price_cents),
        }

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(
            url = %url,
            kalshi_key_id = %key_id,
            signature = "<redacted>",
            ticker = ticker,
            side = side_str,
            contracts = contracts,
            limit_cents = limit_price_cents,
            "POST /portfolio/orders"
        );

        let response = self
            .http
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .context("POST /portfolio/orders request failed")?;
        let text = response
            .text()
            .await
            .context("failed to read order response body")?;
        let resp: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse order response");
            anyhow::anyhow!("failed to parse order response: {}", e)
        })?;

        debug!(raw_response = %text, response = %resp, "order response");

        // Reject API-level errors before trying to parse order fields
        if let Some(err) = resp.get("error") {
            tracing::error!(
                api_error = %err,
                raw_response = %text,
                ticker = ticker,
                "Kalshi API returned an error"
            );
            return Err(anyhow::anyhow!("Kalshi API error: {}", err));
        }

        // Kalshi wraps the order in an "order" key
        let order = resp.get("order").unwrap_or(&resp);

        let order_id = order
            .get("id")
            .or_else(|| order.get("order_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if order_id.is_empty() {
            tracing::warn!(raw_order_response = %text, "order_id missing from response — check field mapping");
        }
        let status = order
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let filled_count = order
            .get("fill_count")
            .or_else(|| order.get("filled_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let remaining_count = order
            .get("remaining_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(contracts.saturating_sub(filled_count));

        info!(
            order_id = %order_id,
            status = %status,
            filled = filled_count,
            remaining = remaining_count,
            ticker = ticker,
            "order placed"
        );

        Ok(OrderResponse {
            order_id,
            status,
            filled_count,
            remaining_count,
        })
    }

    /// Fetch all unsettled (open) positions.
    pub async fn get_open_positions(&self) -> Result<Vec<Position>, anyhow::Error> {
        let path = "/portfolio/positions?settlement_status=unsettled";
        let headers = self.auth.sign_request("GET", &self.sign_path(path))?;
        let url = format!("{}{}", self.base_url, path);

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(url = %url, kalshi_key_id = %key_id, signature = "<redacted>", "GET /portfolio/positions");

        let response = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /portfolio/positions request failed")?;
        let text = response
            .text()
            .await
            .context("failed to read positions response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse positions response");
            anyhow::anyhow!("failed to parse positions response: {}", e)
        })?;

        debug!(response = %body, "positions response");

        let positions_arr = match body.get("market_positions").and_then(|v| v.as_array()) {
            Some(arr) => arr,
            None => return Ok(vec![]),
        };

        let mut result = Vec::new();
        for p in positions_arr {
            let ticker = match p.get("ticker").and_then(|v| v.as_str()) {
                Some(t) => t.to_string(),
                None => continue,
            };
            // Positive position = YES contracts, negative = NO contracts
            let raw_position = p.get("position").and_then(|v| v.as_i64()).unwrap_or(0);
            if raw_position == 0 {
                continue;
            }
            let (side, contracts) = if raw_position > 0 {
                (TradeSide::Yes, raw_position as u64)
            } else {
                (TradeSide::No, raw_position.unsigned_abs())
            };
            let entry_price_cents = p
                .get("average_yes_price")
                .or_else(|| p.get("average_no_price"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            result.push(Position {
                ticker,
                side,
                contracts,
                entry_price_cents,
            });
        }
        Ok(result)
    }

    /// Fetch the current best bid prices for a single market.
    pub async fn get_market_bids(&self, ticker: &str) -> Result<MarketBids, anyhow::Error> {
        let path = format!("/markets/{ticker}");
        let headers = self.auth.sign_request("GET", &self.sign_path(&path))?;
        let url = format!("{}{}", self.base_url, path);

        let response = self
            .http
            .get(&url)
            .headers(headers)
            .send()
            .await
            .context("GET /markets/{ticker} request failed")?;
        let text = response
            .text()
            .await
            .context("failed to read market response body")?;
        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse market response");
            anyhow::anyhow!("failed to parse market response: {}", e)
        })?;

        // Kalshi wraps the market under a "market" key
        let m = body.get("market").unwrap_or(&body);

        let yes_bid_cents = m["yes_bid_dollars"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|d| d * 100.0)
            .unwrap_or(0.0);
        let no_bid_cents = m["no_bid_dollars"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|d| d * 100.0)
            .unwrap_or(0.0);

        debug!(ticker, yes_bid_cents, no_bid_cents, "market bids fetched");
        Ok(MarketBids {
            yes_bid_cents,
            no_bid_cents,
        })
    }

    /// Exit an open position by selling at the current bid.
    ///
    /// Fetches the live bid, then places a sell limit order on the same side as entry.
    /// Returns Ok(()) if the order was accepted; the caller handles tracker/gate updates.
    pub async fn exit_position(
        &self,
        ticker: &str,
        side: &TradeSide,
        contracts: u64,
    ) -> Result<ExitResponse, anyhow::Error> {
        let bids = self
            .get_market_bids(ticker)
            .await
            .context("failed to fetch bids before exit")?;

        let (side_str, bid_cents) = match side {
            TradeSide::Yes => ("yes", bids.yes_bid_cents),
            TradeSide::No => ("no", bids.no_bid_cents),
        };

        if bid_cents <= 0.0 {
            anyhow::bail!("bid is 0¢ for {ticker} {side_str} — market may be closed, cannot exit");
        }

        let path = "/portfolio/orders";
        let headers = self.auth.sign_request("POST", &self.sign_path(path))?;
        let url = format!("{}{}", self.base_url, path);

        let limit_price_cents = bid_cents as u64;
        let mut body = serde_json::json!({
            "ticker": ticker,
            "action": "sell",
            "side": side_str,
            "count": contracts,
            "type": "limit",
        });
        match side {
            TradeSide::Yes => body["yes_price"] = serde_json::Value::from(limit_price_cents),
            TradeSide::No => body["no_price"] = serde_json::Value::from(limit_price_cents),
        }

        let key_id = headers
            .get("kalshi-access-key")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>")
            .to_string();
        debug!(
            url = %url,
            kalshi_key_id = %key_id,
            ticker,
            side = side_str,
            contracts,
            limit_price_cents,
            "POST /portfolio/orders (sell)"
        );

        let response = self
            .http
            .post(&url)
            .headers(headers)
            .json(&body)
            .send()
            .await
            .context("POST /portfolio/orders (sell) request failed")?;
        let text = response
            .text()
            .await
            .context("failed to read sell order response body")?;
        let resp: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            tracing::error!(raw_response = %text, "failed to parse sell order response");
            anyhow::anyhow!("failed to parse sell order response: {}", e)
        })?;
        debug!(raw_response = %text, response = %resp, "sell order response");

        if let Some(err) = resp.get("error") {
            tracing::error!(api_error = %err, ticker, "Kalshi API error on exit order");
            return Err(anyhow::anyhow!("Kalshi API exit error: {}", err));
        }

        let order = resp.get("order").unwrap_or(&resp);
        let filled_count = order
            .get("fill_count")
            .or_else(|| order.get("filled_count"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        info!(
            ticker,
            side = side_str,
            contracts,
            limit_price_cents,
            filled_count,
            "exit order placed"
        );
        Ok(ExitResponse {
            filled_count,
            fill_price_cents: limit_price_cents,
        })
    }
}
